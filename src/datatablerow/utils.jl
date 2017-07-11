# Rows grouping.
# Maps row contents to the indices of all the equal rows.
# Used by groupby(), join(), nonunique()
immutable RowGroupDict{T<:AbstractDataTable}
    "source data table"
    dt::T
    "number of groups"
    ngroups::Int
    "row hashes"
    rhashes::Vector{UInt}
    "hashindex -> index of group-representative row"
    gslots::Vector{Int}
    "group index for each row"
    groups::Vector{Int}
    "permutation of row indices that sorts them by groups"
    rperm::Vector{Int}
    "starts of ranges in rperm for each group"
    starts::Vector{Int}
    "stops of ranges in rperm for each group"
    stops::Vector{Int}
end

# "kernel" functions for hashrows()
# adjust row hashes by the hashes of column elements
function hashrows_col!(h::Vector{UInt}, v::AbstractVector)
    @inbounds for i in eachindex(h)
        h[i] = hash(v[i], h[i])
    end
    h
end

if !isdefined(Base, :unsafe_get)
    unsafe_get(x::Nullable) = x.value
    unsafe_get(x::Any) = x
end

function hashrows_col!{T<:Nullable}(h::Vector{UInt}, v::AbstractVector{T})
    @inbounds for i in eachindex(h)
        h[i] = isnull(v[i]) ?
               h[i] + Base.nullablehash_seed :
               hash(unsafe_get(v[i]), h[i])
    end
    h
end

# should give the same hash as AbstractVector{T}
function hashrows_col!{T}(h::Vector{UInt}, v::AbstractCategoricalVector{T})
    # TODO is it possible to optimize by hashing the pool values once?
    @inbounds for (i, ref) in enumerate(v.refs)
        h[i] = hash(CategoricalArrays.index(v.pool)[ref], h[i])
    end
    h
end

# should give the same hash as AbstractNullableVector{T}
# enables efficient sequential memory access pattern
function hashrows_col!{T}(h::Vector{UInt}, v::AbstractNullableCategoricalVector{T})
    # TODO is it possible to optimize by hashing the pool values once?
    @inbounds for (i, ref) in enumerate(v.refs)
        h[i] = ref == 0 ?
               h[i] + Base.nullablehash_seed :
               hash(CategoricalArrays.index(v.pool)[ref], h[i])
    end
    h
end

# Calculate the vector of `dt` rows hash values.
function hashrows(dt::AbstractDataTable)
    res = zeros(UInt, nrow(dt))
    for col in columns(dt)
        hashrows_col!(res, col)
    end
    return res
end

# Helper function for RowGroupDict.
# Returns a tuple:
# 1) the number of row groups in a data table
# 2) vector of row hashes
# 3) slot array for a hash map, non-zero values are
#    the indices of the first row in a group
# Optional group vector is set to the group indices of each row
row_group_slots(dt::AbstractDataTable, groups::Union{Vector{Int}, Void} = nothing) =
    row_group_slots(ntuple(i -> dt[i], ncol(dt)), hashrows(dt), groups)

function row_group_slots(cols::Tuple{Vararg{AbstractVector}},
                         rhashes::AbstractVector{UInt},
                         groups::Union{Vector{Int}, Void} = nothing)
    @assert groups === nothing || length(groups) == length(cols[1])
    # inspired by Dict code from base cf. https://github.com/JuliaData/DataTables.jl/pull/17#discussion_r102481481
    sz = Base._tablesz(length(rhashes))
    @assert sz >= length(rhashes)
    szm1 = sz-1
    gslots = zeros(Int, sz)
    ngroups = 0
    @inbounds for i in eachindex(rhashes)
        # find the slot and group index for a row
        slotix = rhashes[i] & szm1 + 1
        gix = 0
        probe = 0
        while true
            g_row = gslots[slotix]
            if g_row == 0 # unoccupied slot, current row starts a new group
                gslots[slotix] = i
                gix = ngroups += 1
                break
            elseif rhashes[i] == rhashes[g_row] # occupied slot, check if miss or hit
                if isequal_row(cols, i, g_row) # hit
                    gix = groups !== nothing ? groups[g_row] : 0
                end
                break
            end
            slotix = slotix & szm1 + 1 # check the next slot
            probe += 1
            @assert probe < sz
        end
        if groups !== nothing
            groups[i] = gix
        end
    end
    return ngroups, rhashes, gslots
end

# Builds RowGroupDict for a given datatable.
# Partly uses the code of Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
function group_rows(dt::AbstractDataTable)
    groups = Vector{Int}(nrow(dt))
    ngroups, rhashes, gslots = row_group_slots(dt, groups)

    # count elements in each group
    stops = zeros(Int, ngroups)
    @inbounds for g_ix in groups
        stops[g_ix] += 1
    end

    # group start positions in a sorted table
    starts = Vector{Int}(ngroups)
    if !isempty(starts)
        starts[1] = 1
        @inbounds for i in 1:(ngroups-1)
            starts[i+1] = starts[i] + stops[i]
        end
    end

    # define row permutation that sorts them into groups
    rperm = Vector{Int}(length(groups))
    copy!(stops, starts)
    @inbounds for (i, gix) in enumerate(groups)
        rperm[stops[gix]] = i
        stops[gix] += 1
    end
    stops .-= 1
    return RowGroupDict(dt, ngroups, rhashes, gslots, groups, rperm, starts, stops)
end

# Find index of a row in gd that matches given row by content, 0 if not found
function findrow(gd::RowGroupDict,
                 dt::DataTable,
                 gd_cols::Tuple{Vararg{AbstractVector}},
                 dt_cols::Tuple{Vararg{AbstractVector}},
                 row::Int)
    (gd.dt === dt) && return row # same table, return itself
    # different tables, content matching required
    rhash = rowhash(dt_cols, row)
    szm1 = length(gd.gslots)-1
    slotix = ini_slotix = rhash & szm1 + 1
    while true
        g_row = gd.gslots[slotix]
        if g_row == 0 || # not found
            (rhash == gd.rhashes[g_row] &&
            isequal_row(gd_cols, g_row, dt_cols, row)) # found
            return g_row
        end
        slotix = (slotix & szm1) + 1 # miss, try the next slot
        (slotix == ini_slotix) && break
    end
    return 0 # not found
end

# Find indices of rows in 'gd' that match given row by content.
# return empty set if no row matches
function findrows(gd::RowGroupDict,
                  dt::DataTable,
                  gd_cols::Tuple{Vararg{AbstractVector}},
                  dt_cols::Tuple{Vararg{AbstractVector}},
                  row::Int)
    g_row = findrow(gd, dt, gd_cols, dt_cols, row)
    (g_row == 0) && return view(gd.rperm, 0:-1)
    gix = gd.groups[g_row]
    return view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end

function Base.getindex(gd::RowGroupDict, dtr::DataTableRow)
    g_row = findrow(gd, dtr.dt, ntuple(i -> gd.dt[i], ncol(gd.dt)),
                    ntuple(i -> dtr.dt[i], ncol(dtr.dt)), dtr.row)
    (g_row == 0) && throw(KeyError(dtr))
    gix = gd.groups[g_row]
    return view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end
