# Rows grouping.
# Maps row contents to the indices of all the equal rows.
# Used by groupby(), join(), nonunique()
immutable RowGroupDict{T<:AbstractDataFrame}
    df::T                 # source data frame

    ngroups::Int          # number of groups

    rhashes::Vector{UInt} # row hashes
    gslots::Vector{Int}   # hashindex -> index of group-representative row

    groups::Vector{Int}   # group index for each row
    rperm::Vector{Int}    # permutation of row indices that sorts them by groups

    starts::Vector{Int}   # starts of ranges in rperm for each group
    stops::Vector{Int}    # stops of ranges in rperm for each group
end

# "kernel" functions for hashrows()
# adjust row hashes by the hashes of column elements
function hashrows_col!(h::Vector{UInt}, v::AbstractVector)
    @inbounds for i in eachindex(h)
        h[i] = hash(v[i], h[i])
    end
    h
end

function hashrows_col!{T}(h::Vector{UInt}, v::AbstractNullableVector{T})
    @inbounds for i in eachindex(h)
        h[i] = isnull(v, i) ?
               hash(NULL_MAGIC, h[i]) :
               hash(NullableArrays.unsafe_getvalue_notnull(v, i), h[i])
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
function hashrows_col!{T}(h::Vector{UInt}, v::AbstractNullableCategoricalVector{T})
    # TODO is it possible to optimize by hashing the pool values once?
    @inbounds for (i, ref) in enumerate(v.refs)
        h[i] = ref == 0 ?
               hash(NULL_MAGIC, h[i]) :
               hash(CategoricalArrays.index(v.pool)[ref], h[i])
    end
    h
end

# Calculate hash for each row
# in an efficient column-wise manner
function hashrows(df::AbstractDataFrame)
    res = zeros(UInt, nrow(df))
    for col in columns(df)
        hashrows_col!(res, col)
    end
    return res
end

# Helper function for RowGroupDict.
# Returns a tuple:
# 1) the number of row groups in a data frame
# 2) vector of row hashes
# 3) slot array for a hash map, non-zero values are
#    the indices of the first row in a group
# Optional group vector is set to the group indices of each row
function row_group_slots(df::AbstractDataFrame,
                          groups::Union{Vector{Int}, Void} = nothing)
    @assert groups === nothing || length(groups) == nrow(df)
    rhashes = hashrows(df)
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
                eq = true
                for col in columns(df)
                    if !isequal_colel(col, i, g_row)
                        eq = false # miss
                        break
                    end
                end
                if eq # hit
                    gix = groups !== nothing ? groups[g_row] : 0
                    break
                end
            end
            slotix = slotix & szm1 + 1 # check the next slot
            probe += 1
            probe < sz || error("Cannot find free row slot")
        end
        if groups !== nothing
            groups[i] = gix
        end
    end
    return ngroups, rhashes, gslots
end

# Builds RowGroupDict for a given dataframe.
# Partly uses the code of Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
function group_rows(df::AbstractDataFrame)
    groups = Vector{Int}(nrow(df))
    ngroups, rhashes, gslots = row_group_slots(df, groups)

    # count elements in each group
    stops = zeros(Int, ngroups)
    for g_ix in groups
        stops[g_ix] += 1
    end

    # group start positions in a sorted frame
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
    return RowGroupDict(df, ngroups, rhashes, gslots, groups, rperm, starts, stops)
end

# number of unique row groups
ngroups(gd::RowGroupDict) = gd.ngroups

# Find index of a row in gd that matches given row by content, 0 if not found
function findrow(gd::RowGroupDict, df::DataFrame, row::Int)
    (gd.df === df) && return row # same frame, return itself
    # different frames, content matching required
    rhash = rowhash(df, row)
    szm1 = length(gd.gslots)-1
    slotix = ini_slotix = rhash & szm1 + 1
    while true
        g_row = gd.gslots[slotix]
        if g_row == 0 || # not found
           (rhash == gd.rhashes[g_row] &&
           isequal_row(gd.df, g_row, df, row)) # found
            return g_row
        end
        slotix = (slotix & szm1) + 1 # miss, try the next slot
        (slotix == ini_slotix) && break
    end
    return 0 # not found
end

# Finds indices of rows in 'gd' that match given row by content.
# returns empty set if no row matches
function Base.get(gd::RowGroupDict, df::DataFrame, row::Int)
    g_row = findrow(gd, df, row)
    (g_row == 0) && return Compat.view(gd.rperm, 0:-1)
    gix = gd.groups[g_row]
    return Compat.view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end

function Base.getindex(gd::RowGroupDict, dfr::DataFrameRow)
    g_row = findrow(gd, dfr.df, dfr.row)
    (g_row == 0) && throw(KeyError(dfr))
    gix = gd.groups[g_row]
    return Compat.view(gd.rperm, gd.starts[gix]:gd.stops[gix])
end

# Check if there is matching row in gd
Base.in(gd::RowGroupDict, df::DataFrame, row::Int) = (findrow(gd, df, row) != 0)
