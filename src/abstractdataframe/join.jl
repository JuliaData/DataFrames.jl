##
## Join / merge
##

# Like similar, but returns a array that can have missings and is initialized with missings
similar_missing(dv::AbstractArray{T}, dims::Union{Int, Tuple{Vararg{Int}}}) where {T} =
    fill!(similar(dv, Union{T, Missing}, dims), missing)

const OnType = Union{Symbol, NTuple{2,Symbol}, Pair{Symbol,Symbol}}

# helper structure for DataFrames joining
struct DataFrameJoiner{DF1<:AbstractDataFrame, DF2<:AbstractDataFrame}
    dfl::DF1
    dfr::DF2
    dfl_on::DF1
    dfr_on::DF2
    left_on::Vector{Symbol}
    right_on::Vector{Symbol}

    function DataFrameJoiner{DF1, DF2}(dfl::DF1, dfr::DF2,
                                       on::Union{<:OnType, AbstractVector{<:OnType}}) where {DF1, DF2}

        on_cols = isa(on, Vector) ? on : [on]
        if eltype(on_cols) == Symbol
            left_on = on_cols
            right_on = on_cols
        else
            left_on = [first(x) for x in on_cols]
            right_on = [last(x) for x in on_cols]
        end
        new(dfl, dfr, dfl[left_on], dfr[right_on], left_on, right_on)
    end
end

DataFrameJoiner(dfl::DF1, dfr::DF2, on::Union{<:OnType, AbstractVector{<:OnType}}) where
    {DF1<:AbstractDataFrame, DF2<:AbstractDataFrame} =
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
                              right_ixs::RowIndexMap, rightonly_ixs::RowIndexMap;
                              makeunique::Bool=false)
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
    dfr_noon = without(joiner.dfr, joiner.right_on)

    nrow = length(all_orig_left_ixs) + roil
    @assert nrow == length(all_orig_right_ixs) + loil
    ncleft = ncol(joiner.dfl)
    cols = Vector{AbstractVector}(undef, ncleft + ncol(dfr_noon))
    # inner and left joins preserve non-missingness of the left frame
    _similar_left = kind == :inner || kind == :left ? similar : similar_missing
    for (i, col) in enumerate(eachcol(joiner.dfl))
        cols[i] = _similar_left(col, nrow)
        copyto!(cols[i], view(col, all_orig_left_ixs))
    end
    # inner and right joins preserve non-missingness of the right frame
    _similar_right = kind == :inner || kind == :right ? similar : similar_missing
    for (i, col) in enumerate(eachcol(dfr_noon))
        cols[i+ncleft] = _similar_right(col, nrow)
        copyto!(cols[i+ncleft], view(col, all_orig_right_ixs))
        permute!(cols[i+ncleft], right_perm)
    end
    res = DataFrame(cols, vcat(names(joiner.dfl), names(dfr_noon)),
                    makeunique=makeunique, copycols=false)

    if length(rightonly_ixs.join) > 0
        # some left rows are missing, so the values of the "on" columns
        # need to be taken from the right
        for (on_col_ix, on_col) in enumerate(joiner.left_on)
            # fix the result of the rightjoin by taking the nonmissing values from the right table
            offset = nrow - length(rightonly_ixs.orig) + 1
            copyto!(res[on_col], offset, view(joiner.dfr_on[on_col_ix], rightonly_ixs.orig))
        end
    end
    if kind ∈ (:right, :outer) && !isempty(rightonly_ixs.join)
        # At this point on-columns of the result allow missing values, because
        # right-only rows were filled with missing values when processing joiner.dfl
        # However, when the right on-column (plus the left one for the outer join)
        # does not allow missing values, the result should also disallow them.
        for (on_col_ix, on_col) in enumerate(joiner.left_on)
            LT = eltype(joiner.dfl_on[on_col_ix])
            RT = eltype(joiner.dfr_on[on_col_ix])
            if !(RT >: Missing) && (kind == :right || !(LT >: Missing))
                res[on_col] = disallowmissing(res[on_col])
            end
        end
    end
    return res
end

# map the indices of the left and right joined tables
# to the indices of the rows in the resulting table
# if `nothing` is given, the corresponding map is not built
function update_row_maps!(left_table::AbstractDataFrame,
                          right_table::AbstractDataFrame,
                          right_dict::RowGroupDict,
                          left_ixs::Union{Nothing, RowIndexMap},
                          leftonly_ixs::Union{Nothing, RowIndexMap},
                          right_ixs::Union{Nothing, RowIndexMap},
                          rightonly_mask::Union{Nothing, Vector{Bool}})
    # helper functions
    @inline update!(ixs::Nothing, orig_ix::Int, join_ix::Int, count::Int = 1) = nothing
    @inline function update!(ixs::RowIndexMap, orig_ix::Int, join_ix::Int, count::Int = 1)
        n = length(ixs.orig)
        resize!(ixs.orig, n+count)
        ixs.orig[n+1:end] .= orig_ix
        append!(ixs.join, join_ix:(join_ix+count-1))
        ixs
    end
    @inline update!(ixs::Nothing, orig_ixs::AbstractArray, join_ix::Int) = nothing
    @inline function update!(ixs::RowIndexMap, orig_ixs::AbstractArray, join_ix::Int)
        append!(ixs.orig, orig_ixs)
        append!(ixs.join, join_ix:(join_ix+length(orig_ixs)-1))
        ixs
    end
    @inline update!(ixs::Nothing, orig_ixs::AbstractArray) = nothing
    @inline update!(mask::Vector{Bool}, orig_ixs::AbstractArray) = (mask[orig_ixs] .= false)

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
    to_bimap(::Nothing) = RowIndexMap(Vector{Int}(), Vector{Int}())

    # init maps as requested
    left_ixs = init_map(left_table, map_left)
    leftonly_ixs = init_map(left_table, map_leftonly)
    right_ixs = init_map(right_table, map_right)
    rightonly_mask = map_rightonly ? fill(true, nrow(right_table)) : nothing
    update_row_maps!(left_table, right_table, right_dict, left_ixs, leftonly_ixs, right_ixs, rightonly_mask)
    if map_rightonly
        rightonly_orig_ixs = findall(rightonly_mask)
        rightonly_ixs = RowIndexMap(rightonly_orig_ixs,
                                    collect(length(right_ixs.orig) +
                                            (leftonly_ixs === nothing ? 0 : length(leftonly_ixs)) .+
                                            (1:length(rightonly_orig_ixs))))
    else
        rightonly_ixs = nothing
    end

    return to_bimap(left_ixs), to_bimap(leftonly_ixs), to_bimap(right_ixs), to_bimap(rightonly_ixs)
end

"""
    join(df1, df2; on = Symbol[], kind = :inner, makeunique = false,
         indicator = nothing, validate = (false, false))

Join two `DataFrame` objects

### Arguments

* `df1`, `df2` : the two AbstractDataFrames to be joined

### Keyword Arguments

* `on` : A column, or vector of columns to join df1 and df2 on. If the column(s)
    that df1 and df2 will be joined on have different names, then the columns
    should be `(left, right)` tuples or `left => right` pairs, or a vector of
    such tuples or pairs. `on` is a required argument for all joins except for
    `kind = :cross`

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


* `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).

* `indicator` : Default: `nothing`. If a `Symbol`, adds categorical indicator
   column named `Symbol` for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If `Symbol` is already in use,
   the column name will be modified if `makeunique=true`.

* `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to [`isequal`](@ref)).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

For the three join operations that may introduce missing values (`:outer`, `:left`,
and `:right`), all columns of the returned data table will support missing values.

When merging `on` categorical columns that differ in the ordering of their levels, the
ordering of the left `DataFrame` takes precedence over the ordering of the right `DataFrame`

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

job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
join(name, job2, on = (:ID, :identifier))
join(name, job2, on = :ID => :identifier)
```

"""
function Base.join(df1::AbstractDataFrame,
                   df2::AbstractDataFrame;
                   on::Union{<:OnType, AbstractVector{<:OnType}} = Symbol[],
                   kind::Symbol = :inner, makeunique::Bool=false,
                   indicator::Union{Nothing, Symbol} = nothing,
                   validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false))
    if indicator !== nothing
        indicator_cols = ["_left", "_right"]
        for i in 1:2
            while (haskey(index(df1), Symbol(indicator_cols[i])) ||
                   haskey(index(df2), Symbol(indicator_cols[i])) ||
                   Symbol(indicator_cols[i]) == indicator)
                 indicator_cols[i] *= 'X'
            end
        end
        df1 = copy(df1, copycols=false)
        df1[Symbol(indicator_cols[1])] = trues(nrow(df1))
        df2 = copy(df2, copycols=false)
        df2[Symbol(indicator_cols[2])] = trues(nrow(df2))
    end

    if kind == :cross
        (on == Symbol[]) || throw(ArgumentError("Cross joins don't use argument 'on'."))
        return crossjoin(df1, df2, makeunique=makeunique)
    elseif on == Symbol[]
        throw(ArgumentError("Missing join argument 'on'."))
    end

    joiner = DataFrameJoiner(df1, df2, on)

    # Check merge key validity
    left_invalid = validate[1] ? any(nonunique(joiner.dfl, joiner.left_on)) : false
    right_invalid = validate[2] ? any(nonunique(joiner.dfr, joiner.right_on)) : false

    if left_invalid && right_invalid
        first_error_df1 = findfirst(nonunique(joiner.dfl, joiner.left_on))
        first_error_df2 = findfirst(nonunique(joiner.dfr, joiner.right_on))
        throw(ArgumentError("Merge key(s) are not unique in both df1 and df2. " *
                            "First duplicate in df1 at $first_error_df1. " *
                            "First duplicate in df2 at $first_error_df2"))
    elseif left_invalid
        first_error = findfirst(nonunique(joiner.dfl, joiner.left_on))
        throw(ArgumentError("Merge key(s) in df1 are not unique. " *
                            "First duplicate at row $first_error"))
    elseif right_invalid
        first_error = findfirst(nonunique(joiner.dfr, joiner.right_on))
        throw(ArgumentError("Merge key(s) in df2 are not unique. " *
                            "First duplicate at row $first_error"))
    end

    if kind == :inner
        joined = compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                                     group_rows(joiner.dfr_on),
                                                                     true, false, true, false)...,
                                      makeunique=makeunique)
    elseif kind == :left
        joined = compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                            group_rows(joiner.dfr_on),
                                                            true, true, true, false)...,
                                      makeunique=makeunique)
    elseif kind == :right
        joined = compose_joined_table(joiner, kind, update_row_maps!(joiner.dfr_on, joiner.dfl_on,
                                                            group_rows(joiner.dfl_on),
                                                            true, true, true, false)[[3, 4, 1, 2]]...,
                                      makeunique=makeunique)
    elseif kind == :outer
        joined = compose_joined_table(joiner, kind, update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                            group_rows(joiner.dfr_on),
                                                            true, true, true, true)...,
                                      makeunique=makeunique)
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
        joined = joiner.dfl[left_ixs, :]
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
        joined = joiner.dfl[leftonly_ixs, :]
    else
        throw(ArgumentError("Unknown kind of join requested: $kind"))
    end

    if indicator !== nothing
        left = joined[Symbol(indicator_cols[1])]
        right = joined[Symbol(indicator_cols[2])]

        refs = UInt8[coalesce(l, false) + 2 * coalesce(r, false) for (l, r) in zip(left, right)]
        indicatorcol = CategoricalArray{String,1}(refs, CategoricalPool{String,UInt8}(["left_only", "right_only", "both"]))
        unique_indicator = indicator
        try_idx = 0
        while haskey(index(joined), unique_indicator)
            try_idx += 1
            unique_indicator = Symbol(string(indicator, "_", try_idx))
        end
        joined[unique_indicator] = indicatorcol

        deletecols!(joined, Symbol(indicator_cols[1]))
        deletecols!(joined, Symbol(indicator_cols[2]))
    end

    return joined
end

function crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame; makeunique::Bool=false)
    r1, r2 = size(df1, 1), size(df2, 1)
    colindex = merge(index(df1), index(df2), makeunique=makeunique)
    cols = Any[[repeat(c, inner=r2) for c in eachcol(df1)];
               [repeat(c, outer=r1) for c in eachcol(df2)]]
    DataFrame(cols, colindex, copycols=false)
end
