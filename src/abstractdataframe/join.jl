##
## Join / merge
##

# Like similar, but returns a array that can have nulls and is initialized with nulls
similar_nullable(dv::AbstractArray{T}, dims::Union{Int, Tuple{Vararg{Int}}}) where {T} =
    fill!(similar(dv, Union{T, Null}, dims), null)

# helper structure for DataFrames joining
struct DataFrameJoiner{DF1<:AbstractDataFrame, DF2<:AbstractDataFrame}
    dfl::DF1
    dfr::DF2
    dfl_on::DF1
    dfr_on::DF2
    on_cols::Vector{Symbol}

    function DataFrameJoiner{DF1, DF2}(dfl::DF1, dfr::DF2, on::Union{Symbol,Vector{Symbol}}) where {DF1, DF2}
        on_cols = isa(on, Symbol) ? [on] : on
        new(dfl, dfr, dfl[on_cols], dfr[on_cols], on_cols)
    end
end

DataFrameJoiner(dfl::DF1, dfr::DF2, on::Union{Symbol,Vector{Symbol}}) where {DF1<:AbstractDataFrame, DF2<:AbstractDataFrame} =
    DataFrameJoiner{DF1,DF2}(dfl, dfr, on)

# helper map between the row indices in original and joined table
struct RowIndexMap
    "row indices in the original table"
    orig::Vector{Int}
    "row indices in the resulting joined table"
    join::Vector{Int}
end

Base.length(x::RowIndexMap) = length(x.orig)

# composes the joined data table using the maps between the left and right
# table rows and the indices of rows in the result
function compose_joined_table(joiner::DataFrameJoiner, kind::Symbol,
                              left_ixs::RowIndexMap, leftonly_ixs::RowIndexMap,
                              right_ixs::RowIndexMap, rightonly_ixs::RowIndexMap)
    @assert length(left_ixs) == length(right_ixs)
    # compose left half of the result taking all left columns
    all_orig_left_ixs = vcat(left_ixs.orig, leftonly_ixs.orig)

    ril = length(right_ixs)
    lil = length(left_ixs)
    loil = length(leftonly_ixs)
    roil = length(rightonly_ixs)

    if loil > 0
        # combine the matched (left_ixs.orig) and non-matched (leftonly_ixs.orig) indices of the left table rows
        # preserving the original rows order
        all_orig_left_ixs = similar(left_ixs.orig, lil + loil)
        @inbounds all_orig_left_ixs[left_ixs.join] = left_ixs.orig
        @inbounds all_orig_left_ixs[leftonly_ixs.join] = leftonly_ixs.orig
    else
        # the result contains only the left rows that are matched to right rows (left_ixs)
        all_orig_left_ixs = left_ixs.orig # no need to copy left_ixs.orig as it's not used elsewhere
    end
    # permutation to swap rightonly and leftonly rows
    right_perm = vcat(1:ril, ril+roil+1:ril+roil+loil, ril+1:ril+roil)
    if length(leftonly_ixs) > 0
        # compose right_perm with the permutation that restores left rows order
        right_perm[vcat(right_ixs.join, leftonly_ixs.join)] = right_perm[1:ril+loil]
    end
    all_orig_right_ixs = vcat(right_ixs.orig, rightonly_ixs.orig)

    # compose right half of the result taking all right columns excluding on
    dfr_noon = without(joiner.dfr, joiner.on_cols)

    nrow = length(all_orig_left_ixs) + roil
    @assert nrow == length(all_orig_right_ixs) + loil
    ncleft = ncol(joiner.dfl)
    cols = Vector{Any}(ncleft + ncol(dfr_noon))
    _similar = kind == :inner ? similar : similar_nullable
    for (i, col) in enumerate(columns(joiner.dfl))
        cols[i] = _similar(col, nrow)
        fillcolumn!(cols[i], col, all_orig_left_ixs)
    end
    for (i, col) in enumerate(columns(dfr_noon))
        cols[i+ncleft] = _similar(col, nrow)
        fillcolumn!(cols[i+ncleft], col, all_orig_right_ixs)
        permute!(cols[i+ncleft], right_perm)
    end
    res = DataFrame(cols, vcat(names(joiner.dfl), names(dfr_noon)))

    if length(rightonly_ixs.join) > 0
        # some left rows are nulls, so the values of the "on" columns
        # need to be taken from the right
        for (on_col_ix, on_col) in enumerate(joiner.on_cols)
            # fix the result of the rightjoin by taking the nonnull values from the right table
            offset = nrow - length(rightonly_ixs.orig)
            fillcolumn!(res[on_col], joiner.dfr_on[on_col_ix], rightonly_ixs.orig, offset)
        end
    end
    return res
end

function fillcolumn!(dfcol::AbstractVector{T1}, refcol::AbstractVector{T2},
                     indices::Vector{Int}, offset::Int=0) where {T1, T2}
    @inbounds for (j, k) in enumerate(indices)
        dfcol[j+offset] = refcol[k]
    end
end

# map the indices of the left and right joined tables
# to the indices of the rows in the resulting table
# if `nothing` is given, the corresponding map is not built
function update_row_maps!(left_table::AbstractDataFrame,
                          right_table::AbstractDataFrame,
                          right_dict::RowGroupDict,
                          left_ixs::Union{Void, RowIndexMap},
                          leftonly_ixs::Union{Void, RowIndexMap},
                          right_ixs::Union{Void, RowIndexMap},
                          rightonly_mask::Union{Void, Vector{Bool}})
    # helper functions
    @inline update!(ixs::Void, orig_ix::Int, join_ix::Int, count::Int = 1) = nothing
    @inline function update!(ixs::RowIndexMap, orig_ix::Int, join_ix::Int, count::Int = 1)
        n = length(ixs.orig)
        resize!(ixs.orig, n+count)
        ixs.orig[n+1:end] = orig_ix
        append!(ixs.join, join_ix:(join_ix+count-1))
        ixs
    end
    @inline update!(ixs::Void, orig_ixs::AbstractArray, join_ix::Int) = nothing
    @inline function update!(ixs::RowIndexMap, orig_ixs::AbstractArray, join_ix::Int)
        append!(ixs.orig, orig_ixs)
        append!(ixs.join, join_ix:(join_ix+length(orig_ixs)-1))
        ixs
    end
    @inline update!(ixs::Void, orig_ixs::AbstractArray) = nothing
    @inline update!(mask::Vector{Bool}, orig_ixs::AbstractArray) = (mask[orig_ixs] = false)

    # iterate over left rows and compose the left<->right index map
    right_dict_cols = ntuple(i -> right_dict.df[i], ncol(right_dict.df))
    left_table_cols = ntuple(i -> left_table[i], ncol(left_table))
    next_join_ix = 1
    for l_ix in 1:nrow(left_table)
        r_ixs = findrows(right_dict, left_table, right_dict_cols, left_table_cols, l_ix)
        if isempty(r_ixs)
            update!(leftonly_ixs, l_ix, next_join_ix)
            next_join_ix += 1
        else
            update!(left_ixs, l_ix, next_join_ix, length(r_ixs))
            update!(right_ixs, r_ixs, next_join_ix)
            update!(rightonly_mask, r_ixs)
            next_join_ix += length(r_ixs)
        end
    end
end

# map the row indices of the left and right joined tables
# to the indices of rows in the resulting table
# returns the 4-tuple of row indices maps for
# - matching left rows
# - non-matching left rows
# - matching right rows
# - non-matching right rows
# if false is provided, the corresponding map is not built and the
# tuple element is empty RowIndexMap
function update_row_maps!(left_table::AbstractDataFrame,
                          right_table::AbstractDataFrame,
                          right_dict::RowGroupDict,
                          map_left::Bool, map_leftonly::Bool,
                          map_right::Bool, map_rightonly::Bool)
    init_map(df::AbstractDataFrame, init::Bool) = init ?
        RowIndexMap(sizehint!(Vector{Int}(), nrow(df)),
                    sizehint!(Vector{Int}(), nrow(df))) : nothing
    to_bimap(x::RowIndexMap) = x
    to_bimap(::Void) = RowIndexMap(Vector{Int}(), Vector{Int}())

    # init maps as requested
    left_ixs = init_map(left_table, map_left)
    leftonly_ixs = init_map(left_table, map_leftonly)
    right_ixs = init_map(right_table, map_right)
    rightonly_mask = map_rightonly ? fill(true, nrow(right_table)) : nothing
    update_row_maps!(left_table, right_table, right_dict, left_ixs, leftonly_ixs, right_ixs, rightonly_mask)
    if map_rightonly
        rightonly_orig_ixs = find(rightonly_mask)
        rightonly_ixs = RowIndexMap(rightonly_orig_ixs,
                                    collect(length(right_ixs.orig) +
                                            (leftonly_ixs === nothing ? 0 : length(leftonly_ixs)) +
                                            (1:length(rightonly_orig_ixs))))
    else
        rightonly_ixs = nothing
    end

    return to_bimap(left_ixs), to_bimap(leftonly_ixs), to_bimap(right_ixs), to_bimap(rightonly_ixs)
end

"""
Join two DataFrames

```julia
join(df1::AbstractDataFrame,
     df2::AbstractDataFrame;
     on::Union{Symbol, Vector{Symbol}} = Symbol[],
     kind::Symbol = :inner)
```

### Arguments

* `df1`, `df2` : the two AbstractDataFrames to be joined

### Keyword Arguments

* `on` : a Symbol or Vector{Symbol}, the column(s) used as keys when
  joining; required argument except for `kind = :cross`

* `kind` : the type of join, options include:

  - `:inner` : only include rows with keys that match in both `df1`
    and `df2`, the default
  - `:outer` : include all rows from `df1` and `df2`
  - `:left` : include all rows from `df1`
  - `:right` : include all rows from `df2`
  - `:semi` : return rows of `df1` that match with the keys in `df2`
  - `:anti` : return rows of `df1` that do not match with the keys in `df2`
  - `:cross` : a full Cartesian product of the key combinations; every
    row of `df1` is matched with every row of `df2`

For the three join operations that may introduce missing values (`:outer`, `:left`,
and `:right`), all columns of the returned data table will be nullable.

### Result

* `::DataFrame` : the joined DataFrame

### Examples

```julia
name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

join(name, job, on = :ID)
join(name, job, on = :ID, kind = :outer)
join(name, job, on = :ID, kind = :left)
join(name, job, on = :ID, kind = :right)
join(name, job, on = :ID, kind = :semi)
join(name, job, on = :ID, kind = :anti)
join(name, job, kind = :cross)
```

"""
function Base.join(df1::AbstractDataFrame,
                   df2::AbstractDataFrame;
                   on::Union{Symbol, Vector{Symbol}} = Symbol[],
                   kind::Symbol = :inner)
    if kind == :cross
        (on == Symbol[]) || throw(ArgumentError("Cross joins don't use argument 'on'."))
        return crossjoin(df1, df2)
    elseif on == Symbol[]
        throw(ArgumentError("Missing join argument 'on'."))
    end

    joiner = DataFrameJoiner(df1, df2, on)

    if kind == :inner
        compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                            group_rows(joiner.dfr_on),
                                                            true, false, true, false)...)
    elseif kind == :left
        compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                            group_rows(joiner.dfr_on),
                                                            true, true, true, false)...)
    elseif kind == :right
        compose_joined_table(joiner, kind, update_row_maps!(joiner.dfr_on, joiner.dfl_on,
                                                            group_rows(joiner.dfl_on),
                                                            true, true, true, false)[[3, 4, 1, 2]]...)
    elseif kind == :outer
        compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                            group_rows(joiner.dfr_on),
                                                            true, true, true, true)...)
    elseif kind == :semi
        # hash the right rows
        dfr_on_grp = group_rows(joiner.dfr_on)
        # iterate over left rows and leave those found in right
        left_ixs = Vector{Int}()
        sizehint!(left_ixs, nrow(joiner.dfl))
        dfr_on_grp_cols = ntuple(i -> dfr_on_grp.df[i], ncol(dfr_on_grp.df))
        dfl_on_cols = ntuple(i -> joiner.dfl_on[i], ncol(joiner.dfl_on))
        @inbounds for l_ix in 1:nrow(joiner.dfl_on)
            if findrow(dfr_on_grp, joiner.dfl_on, dfr_on_grp_cols, dfl_on_cols, l_ix) != 0
                push!(left_ixs, l_ix)
            end
        end
        return joiner.dfl[left_ixs, :]
    elseif kind == :anti
        # hash the right rows
        dfr_on_grp = group_rows(joiner.dfr_on)
        # iterate over left rows and leave those not found in right
        leftonly_ixs = Vector{Int}()
        sizehint!(leftonly_ixs, nrow(joiner.dfl))
        dfr_on_grp_cols = ntuple(i -> dfr_on_grp.df[i], ncol(dfr_on_grp.df))
        dfl_on_cols = ntuple(i -> joiner.dfl_on[i], ncol(joiner.dfl_on))
        @inbounds for l_ix in 1:nrow(joiner.dfl_on)
            if findrow(dfr_on_grp, joiner.dfl_on, dfr_on_grp_cols, dfl_on_cols, l_ix) == 0
                push!(leftonly_ixs, l_ix)
            end
        end
        return joiner.dfl[leftonly_ixs, :]
    else
        throw(ArgumentError("Unknown kind of join requested: $kind"))
    end
end

function crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame)
    r1, r2 = size(df1, 1), size(df2, 1)
    cols = Any[[repeat(c, inner=r2) for c in columns(df1)];
               [repeat(c, outer=r1) for c in columns(df2)]]
    colindex = merge(index(df1), index(df2))
    DataFrame(cols, colindex)
end
