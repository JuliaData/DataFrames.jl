# Rows grouping.
# Maps row contents to the indices of all the equal rows.
# Used by groupby(), join(), nonunique()
struct RowGroupDict{T<:AbstractDataFrame}
    "source data table"
    df::T
    "row hashes (optional, can be empty)"
    rhashes::Vector{UInt}
    "hashindex -> index of group-representative row (optional, can be empty)"
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
                       v::AbstractVector{T},
                       firstcol::Bool) where T
    @inbounds for i in eachindex(h)
        el = v[i]
        h[i] = hash(el, h[i])
        if length(n) > 0
            n[i] |= ismissing(el)
        end
    end
    h
end

# should give the same hash as AbstractVector{T}
function hashrows_col!(h::Vector{UInt},
                       n::Vector{Bool},
                       v::AbstractCategoricalVector,
                       firstcol::Bool)
    index = CategoricalArrays.index(v.pool)
    # When hashing the first column, no need to take into account previous hash,
    # which is always zero
    if firstcol
        hashes = Vector{UInt}(undef, length(levels(v.pool))+1)
        hashes[1] = hash(missing)
        hashes[2:end] .= hash.(index)
        @inbounds for (i, ref) in enumerate(v.refs)
            h[i] = hashes[ref+1]
        end
    else
        @inbounds for (i, ref) in enumerate(v.refs)
            if eltype(v) >: Missing && ref == 0
                h[i] = hash(missing, h[i])
            else
                h[i] = hash(index[ref], h[i])
            end
        end
    end
    # Doing this step separately is faster, as it would disable SIMD above
    if eltype(v) >: Missing && length(n) > 0
        @inbounds for (i, ref) in enumerate(v.refs)
            n[i] |= (ref == 0)
        end
    end
    h
end

# Calculate the vector of `df` rows hash values.
function hashrows(cols::Tuple{Vararg{AbstractVector}}, skipmissing::Bool)
    len = length(cols[1])
    rhashes = zeros(UInt, len)
    missings = fill(false, skipmissing ? len : 0)
    for (i, col) in enumerate(cols)
        hashrows_col!(rhashes, missings, col, i == 1)
    end
    return (rhashes, missings)
end

# table columns are passed as a tuple of vectors to ensure type specialization
isequal_row(cols::Tuple{AbstractVector}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2])
isequal_row(cols::Tuple{Vararg{AbstractVector}}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2]) && isequal_row(Base.tail(cols), r1, r2)

isequal_row(cols1::Tuple{AbstractVector}, r1::Int, cols2::Tuple{AbstractVector}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2])
isequal_row(cols1::Tuple{Vararg{AbstractVector}}, r1::Int,
            cols2::Tuple{Vararg{AbstractVector}}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2]) &&
        isequal_row(Base.tail(cols1), r1, Base.tail(cols2), r2)

# Helper function for RowGroupDict.
# Returns a tuple:
# 1) the highest group index in the `groups` vector
# 2) vector of row hashes (may be empty if hash=Val(false))
# 3) slot array for a hash map, non-zero values are
#    the indices of the first row in a group
# 4) whether groups are already sorted
# Optional `groups` vector is set to the group indices of each row
function row_group_slots(cols::Tuple{Vararg{AbstractVector}},
                         hash::Val = Val(true),
                         groups::Union{Vector{Int}, Nothing} = nothing,
                         skipmissing::Bool = false)::Tuple{Int, Vector{UInt}, Vector{Int}, Bool}
    @assert groups === nothing || length(groups) == length(cols[1])
    rhashes, missings = hashrows(cols, skipmissing)
    # inspired by Dict code from base cf. https://github.com/JuliaData/DataTables.jl/pull/17#discussion_r102481481
    sz = Base._tablesz(length(rhashes))
    @assert sz >= length(rhashes)
    szm1 = sz-1
    gslots = zeros(Int, sz)
    # If missings are to be skipped, they will all go to group 1,
    # which will be removed by group_rows
    ngroups = skipmissing ? 1 : 0
    @inbounds for i in eachindex(rhashes)
        # find the slot and group index for a row
        slotix = rhashes[i] & szm1 + 1
        # Use 0 for non-missing values to catch bugs if group is not found
        gix = skipmissing && missings[i] ? 1 : 0
        probe = 0
        # If skipmissing=true, assign rows containing at least one missing to group 1
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
    return ngroups, rhashes, gslots, false
end

function row_group_slots(cols::Tuple{CategoricalVector},
                         hash::Val{false},
                         groups::Union{Vector{Int}, Nothing} = nothing,
                         skipmissing::Bool = false)::Tuple{Int, Vector{UInt}, Vector{Int}, Bool}
    col = cols[1]
    @assert groups === nothing || length(groups) == length(col)

    # If missings are to be skipped, they will all go to group 1,
    # which will be removed by group_rows
    ngroups = length(levels(col)) + (eltype(col) >: Missing)

    if groups !== nothing
        # When levels are in the same order as the index and there are no missing values,
        # we could simply copy refs to groups, but the performance gain is negligible,
        # so always sort groups in the order of levels
        refmap = [0; CategoricalArrays.order(col.pool)]
        seen = fill(false, length(refmap))
        if skipmissing
            refmap .+= 1
            seen[1] = true
        else
            refmap[1] = ngroups
        end
        @inbounds for i in eachindex(groups)
            j = refmap[col.refs[i]+1]
            groups[i] = j
            seen[j] = true
        end
        if !all(seen)
            if skipmissing # Always keep first group even if empty
                ngroups = 1
                start = 2
            else
                ngroups = 0
                start = 1
            end
            @inbounds for i in start:length(refmap)
                ngroups += seen[i]
                refmap[i] = ngroups
            end
            @inbounds for i in eachindex(groups)
                groups[i] = refmap[groups[i]]
            end
        end
    end
    return ngroups, UInt[], Int[], true
end

# Builds RowGroupDict for a given DataFrame.
# Partly uses the code of Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).
# - hash: whether row hashes should be computed (if false, the rhashes and gslots fields
#   hold empty vectors)
# - sort: whether groups should be sorted
# - skipmissing: whether rows with missing values should be skipped
#   rather than put into a separate group
function group_rows(df::AbstractDataFrame, hash::Bool = true, sort::Bool = false,
                    skipmissing::Bool = false)
    groups = Vector{Int}(undef, nrow(df))
    ngroups, rhashes, gslots, sorted =
        row_group_slots(ntuple(i -> df[i], ncol(df)), Val(hash), groups, skipmissing)

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
        popfirst!(starts)
        popfirst!(stops)
        ngroups -= 1
    end

    # sort groups if row_group_slots hasn't already done that
    if sort && !sorted
        group_perm = sortperm(view(df, rperm[starts], :))
        permute!(starts, group_perm)
        Base.permute!!(stops, group_perm)
    end

    return RowGroupDict(df, rhashes, gslots, groups, rperm, starts, stops)
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
