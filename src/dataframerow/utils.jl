# Rows grouping.
# Maps row contents to the indices of all the equal rows.
# Used by groupby(), join(), nonunique()
struct RowGroupDict{T<:AbstractDataFrame}
    "source data table"
    df::T
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
function hashrows_col!(h::Vector{UInt},
                       n::Vector{Bool},
                       v::AbstractVector{T}) where T
    @inbounds for i in eachindex(h)
        el = v[i]
        h[i] = hash(el, h[i])
        if T >: Missing && length(n) > 0
            # el isa Missing should be redundant
            # but it gives much more efficient code on Julia 0.6
            n[i] |= (el isa Missing || ismissing(el))
        end
    end
    h
end

# should give the same hash as AbstractVector{T}
function hashrows_col!(h::Vector{UInt},
                       n::Vector{Bool},
                       v::AbstractCategoricalVector{T}) where T
    # TODO is it possible to optimize by hashing the pool values once?
    @inbounds for (i, ref) in enumerate(v.refs)
        h[i] = hash(CategoricalArrays.index(v.pool)[ref], h[i])
    end
    h
end

# should give the same hash as AbstractVector{T}
# enables efficient sequential memory access pattern
function hashrows_col!(h::Vector{UInt},
                       n::Vector{Bool},
                       v::AbstractCategoricalVector{>: Missing})
    # TODO is it possible to optimize by hashing the pool values once?
    @inbounds for (i, ref) in enumerate(v.refs)
        if ref == 0
            h[i] = hash(missing, h[i])
            length(n) > 0 && (n[i] = true)
        else
            h[i] = hash(CategoricalArrays.index(v.pool)[ref], h[i])
        end
    end
    h
end

# Calculate the vector of `df` rows hash values.
function hashrows(df::AbstractDataFrame, skipmissing::Bool)
    rhashes = zeros(UInt, nrow(df))
    missings = fill(false, skipmissing ? nrow(df) : 0)
    for col in columns(df)
        hashrows_col!(rhashes, missings, col)
    end
    return (rhashes, missings)
end

# Helper function for RowGroupDict.
# Returns a tuple:
# 1) the number of row groups in a data table
# 2) vector of row hashes
# 3) slot array for a hash map, non-zero values are
#    the indices of the first row in a group
# Optional group vector is set to the group indices of each row
function row_group_slots(df::AbstractDataFrame,
                         groups::Union{Vector{Int}, Nothing} = nothing,
                         skipmissing::Bool = false)
    rhashes, missings = hashrows(df, skipmissing)
    row_group_slots(ntuple(i -> df[i], ncol(df)), rhashes, missings, groups, skipmissing)
end

function row_group_slots(cols::Tuple{Vararg{AbstractVector}},
                         rhashes::AbstractVector{UInt},
                         missings::AbstractVector{Bool},
                         groups::Union{Vector{Int}, Nothing} = nothing,
                         skipmissing::Bool = false)
    @assert groups === nothing || length(groups) == length(cols[1])
    # inspired by Dict code from base cf. https://github.com/JuliaData/DataFrames.jl/pull/17#discussion_r102481481
    sz = Base._tablesz(length(rhashes))
    @assert sz >= length(rhashes)
    szm1 = sz-1
    gslots = zeros(Int, sz)
    # If missings are to be skipped, they will all go to group 1
    ngroups = skipmissing ? 1 : 0
    @inbounds for i in eachindex(rhashes)
        # find the slot and group index for a row
        slotix = rhashes[i] & szm1 + 1
        # Use 0 for non-missing values to catch bugs if group is not found
        gix = skipmissing && missings[i] ? 1 : 0
        probe = 0
        # Skip rows contaning at least one missing (assigning them to group 0)
        if !skipmissing || !missings[i]
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
        end
        if groups !== nothing
            groups[i] = gix
        end
    end
    return ngroups, rhashes, gslots
end

# Builds RowGroupDict for a given DataFrame.
# Partly uses the code of Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
function group_rows(df::AbstractDataFrame, skipmissing::Bool = false)
    groups = Vector{Int}(undef, nrow(df))
    ngroups, rhashes, gslots = row_group_slots(df, groups, skipmissing)

    # count elements in each group
    stops = zeros(Int, ngroups)
    @inbounds for g_ix in groups
        stops[g_ix] += 1
    end

    # group start positions in a sorted table
    starts = Vector{Int}(undef, ngroups)
    if !isempty(starts)
        starts[1] = 1
        @inbounds for i in 1:(ngroups-1)
            starts[i+1] = starts[i] + stops[i]
        end
    end

    # define row permutation that sorts them into groups
    rperm = Vector{Int}(undef, length(groups))
    copyto!(stops, starts)
    @inbounds for (i, gix) in enumerate(groups)
        rperm[stops[gix]] = i
        stops[gix] += 1
    end
    stops .-= 1

    # drop group 1 which contains rows with missings in grouping columns
    if skipmissing
        splice!(starts, 1)
        splice!(stops, 1)
        ngroups -= 1
    end

    return RowGroupDict(df, ngroups, rhashes, gslots, groups, rperm, starts, stops)
end

# Find index of a row in gd that matches given row by content, 0 if not found
function findrow(gd::RowGroupDict,
                 df::DataFrame,
                 gd_cols::Tuple{Vararg{AbstractVector}},
                 df_cols::Tuple{Vararg{AbstractVector}},
                 row::Int)
    (gd.df === df) && return row # same table, return itself
    # different tables, content matching required
    rhash = rowhash(df_cols, row)
    szm1 = length(gd.gslots)-1
    slotix = ini_slotix = rhash & szm1 + 1
    while true
        g_row = gd.gslots[slotix]
        if g_row == 0 || # not found
            (rhash == gd.rhashes[g_row] &&
            isequal_row(gd_cols, g_row, df_cols, row)) # found
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
                  df::DataFrame,
                  gd_cols::Tuple{Vararg{AbstractVector}},
                  df_cols::Tuple{Vararg{AbstractVector}},
                  row::Int)
    g_row = findrow(gd, df, gd_cols, df_cols, row)
    (g_row == 0) && return view(gd.rperm, 0:-1)
    gix = gd.groups[g_row]
    return view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end

function Base.getindex(gd::RowGroupDict, dfr::DataFrameRow)
    g_row = findrow(gd, parent(dfr), ntuple(i -> gd.df[i], ncol(gd.df)),
                    ntuple(i -> parent(dfr)[i], ncol(parent(dfr))), row(dfr))
    (g_row == 0) && throw(KeyError(dfr))
    gix = gd.groups[g_row]
    return view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end
