##
## Join / merge
##

# Like similar, but returns a array that can have missings and is initialized with missings
similar_missing(dv::AbstractArray{T}, dims::Union{Int, Tuple{Vararg{Int}}}) where {T} =
    fill!(similar(dv, Union{T, Missing}, dims), missing)

const OnType = Union{SymbolOrString, NTuple{2,Symbol}, Pair{Symbol,Symbol},
                     Pair{<:AbstractString, <:AbstractString}}

# helper structure for DataFrames joining
struct DataFrameJoiner{DF1<:AbstractDataFrame, DF2<:AbstractDataFrame}
    dfl::DF1
    dfr::DF2
    dfl_on::DF1
    dfr_on::DF2
    left_on::Vector{Symbol}
    right_on::Vector{Symbol}

    function DataFrameJoiner{DF1, DF2}(dfl::DF1, dfr::DF2,
                                       on::Union{<:OnType, AbstractVector}) where {DF1, DF2}
        on_cols = isa(on, AbstractVector) ? on : [on]
        left_on = Symbol[]
        right_on = Symbol[]
        for v in on_cols
            if v isa SymbolOrString
                push!(left_on, Symbol(v))
                push!(right_on, Symbol(v))
            elseif v isa Union{Pair{Symbol,Symbol},
                               Pair{<:AbstractString, <:AbstractString},
                               NTuple{2,Symbol}}
                push!(left_on, Symbol(first(v)))
                push!(right_on, Symbol(last(v)))
                if v isa NTuple{2,Symbol}
                    Base.depwarn("Using a `Tuple{Symbol, Symbol}` or a vector containing " *
                                 "such tuples as a value of `on` keyword argument is " *
                                 "deprecated: use `Pair{Symbol,Symbol}` instead.", :join)

                end
            else
                throw(ArgumentError("All elements of `on` argument to `join` must be " *
                                    "Symbol or Pair{Symbol,Symbol}."))
            end
        end
        new(dfl, dfr, dfl[!, left_on], dfr[!, right_on], left_on, right_on)
    end
end

DataFrameJoiner(dfl::DF1, dfr::DF2, on::Union{<:OnType, AbstractVector}) where
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
        # combine the matched (left_ixs.orig) and non-matched (leftonly_ixs.orig)
        # indices of the left table rows, preserving the original rows order
        all_orig_left_ixs = similar(left_ixs.orig, lil + loil)
        @inbounds all_orig_left_ixs[left_ixs.join] = left_ixs.orig
        @inbounds all_orig_left_ixs[leftonly_ixs.join] = leftonly_ixs.orig
    else
        # the result contains only the left rows that are matched to right rows (left_ixs)
        # no need to copy left_ixs.orig as it's not used elsewhere
        all_orig_left_ixs = left_ixs.orig
    end
    # permutation to swap rightonly and leftonly rows
    right_perm = vcat(1:ril, ril+roil+1:ril+roil+loil, ril+1:ril+roil)
    if length(leftonly_ixs) > 0
        # compose right_perm with the permutation that restores left rows order
        right_perm[vcat(right_ixs.join, leftonly_ixs.join)] = right_perm[1:ril+loil]
    end
    all_orig_right_ixs = vcat(right_ixs.orig, rightonly_ixs.orig)

    # compose right half of the result taking all right columns excluding on
    dfr_noon = select(joiner.dfr, Not(joiner.right_on), copycols=false)

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
    res = DataFrame(cols, vcat(_names(joiner.dfl), _names(dfr_noon)),
                    makeunique=makeunique, copycols=false)

    if length(rightonly_ixs.join) > 0
        # some left rows are missing, so the values of the "on" columns
        # need to be taken from the right
        for (on_col_ix, on_col) in enumerate(joiner.left_on)
            # fix the result of the rightjoin by taking the nonmissing values from the right table
            offset = nrow - length(rightonly_ixs.orig) + 1
            copyto!(res[!, on_col], offset,
                    view(joiner.dfr_on[!, on_col_ix], rightonly_ixs.orig))
        end
    end
    if kind ∈ (:right, :outer) && !isempty(rightonly_ixs.join)
        # At this point on-columns of the result allow missing values, because
        # right-only rows were filled with missing values when processing joiner.dfl
        # However, when the right on-column (plus the left one for the outer join)
        # does not allow missing values, the result should also disallow them.
        for (on_col_ix, on_col) in enumerate(joiner.left_on)
            LT = eltype(joiner.dfl_on[!, on_col_ix])
            RT = eltype(joiner.dfr_on[!, on_col_ix])
            if !(RT >: Missing) && (kind == :right || !(LT >: Missing))
                res[!, on_col] = disallowmissing(res[!, on_col])
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
    @inline update!(mask::Vector{Bool}, orig_ixs::AbstractArray) =
        (mask[orig_ixs] .= false)

    # iterate over left rows and compose the left<->right index map
    right_dict_cols = ntuple(i -> right_dict.df[!, i], ncol(right_dict.df))
    left_table_cols = ntuple(i -> left_table[!, i], ncol(left_table))
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
    update_row_maps!(left_table, right_table, right_dict, left_ixs, leftonly_ixs,
                     right_ixs, rightonly_mask)
    if map_rightonly
        rightonly_orig_ixs = findall(rightonly_mask)
        leftonly_ixs_len = leftonly_ixs === nothing ? 0 : length(leftonly_ixs)
        rightonly_ixs = RowIndexMap(rightonly_orig_ixs,
                                    collect(length(right_ixs.orig) .+
                                            leftonly_ixs_len .+
                                            (1:length(rightonly_orig_ixs))))
    else
        rightonly_ixs = nothing
    end

    return to_bimap(left_ixs), to_bimap(leftonly_ixs),
           to_bimap(right_ixs), to_bimap(rightonly_ixs)
end

function _join(df1::AbstractDataFrame, df2::AbstractDataFrame;
               on::Union{<:OnType, AbstractVector}, kind::Symbol, makeunique::Bool,
               indicator::Union{Nothing, Symbol},
               validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}})
    _check_consistency(df1)
    _check_consistency(df2)

    if indicator !== nothing
        indicator_cols = ["_left", "_right"]
        for i in 1:2
            while (hasproperty(df1, Symbol(indicator_cols[i])) ||
                   hasproperty(df2, Symbol(indicator_cols[i])) ||
                   Symbol(indicator_cols[i]) == indicator)
                 indicator_cols[i] *= 'X'
            end
        end
        df1 = copy(df1, copycols=false)
        df1_ind = Symbol(indicator_cols[1])
        df1[!, df1_ind] = trues(nrow(df1))
        df2 = copy(df2, copycols=false)
        df2_ind = Symbol(indicator_cols[2])
        df2[!, df2_ind] = trues(nrow(df2))
    end

    if on == []
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
        joined = compose_joined_table(joiner, kind,
                                      update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                       group_rows(joiner.dfr_on),
                                                       true, false, true, false)...,
                                      makeunique=makeunique)
    elseif kind == :left
        joined = compose_joined_table(joiner, kind,
                                      update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                       group_rows(joiner.dfr_on),
                                                       true, true, true, false)...,
                                      makeunique=makeunique)
    elseif kind == :right
        joined = compose_joined_table(joiner, kind,
                                      update_row_maps!(joiner.dfr_on, joiner.dfl_on,
                                                       group_rows(joiner.dfl_on),
                                                       true, true, true, false)[[3, 4, 1, 2]]...,
                                      makeunique=makeunique)
    elseif kind == :outer
        joined = compose_joined_table(joiner, kind,
                                      update_row_maps!(joiner.dfl_on, joiner.dfr_on,
                                                       group_rows(joiner.dfr_on),
                                                       true, true, true, true)...,
                                      makeunique=makeunique)
    elseif kind == :semi
        # hash the right rows
        dfr_on_grp = group_rows(joiner.dfr_on)
        # iterate over left rows and leave those found in right
        left_ixs = Vector{Int}()
        sizehint!(left_ixs, nrow(joiner.dfl))
        dfr_on_grp_cols = ntuple(i -> dfr_on_grp.df[!, i], ncol(dfr_on_grp.df))
        dfl_on_cols = ntuple(i -> joiner.dfl_on[!, i], ncol(joiner.dfl_on))
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
        dfr_on_grp_cols = ntuple(i -> dfr_on_grp.df[!, i], ncol(dfr_on_grp.df))
        dfl_on_cols = ntuple(i -> joiner.dfl_on[!, i], ncol(joiner.dfl_on))
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
        refs = UInt8.(coalesce.(joined[!, df1_ind], false) .+
                      2 .* coalesce.(joined[!, df2_ind], false))
        pool = CategoricalPool{String,UInt8}(["left_only", "right_only", "both"])
        indicatorcol = CategoricalArray{String,1}(refs, pool)
        unique_indicator = indicator
        try_idx = 0
        while hasproperty(joined, unique_indicator)
            try_idx += 1
            unique_indicator = Symbol(string(indicator, "_", try_idx))
        end
        joined[!, unique_indicator] = indicatorcol
        select!(joined, Not([df1_ind, df2_ind]))
    end

    return joined
end

"""
    innerjoin(df1, df2; on, makeunique = false,
              validate = (false, false))
    innerjoin(df1, df2, dfs...; on, makeunique = false,
              validate = (false, false))

Perform an inner join of two or more data frame objects and return a `DataFrame`
containing the result. An inner join includes rows with keys that match in all
passed data frames.

# Arguments
- `df1`, `df2`, `dfs...`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed). If more than two data frames are joined
  then only a column name or a vector of column names are allowed.
  `on` is a required argument.
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

If more than two data frames are passed, the join is performed recursively with
left associativity. In this case the `validate` keyword argument is applied
recursively with left associativity.

See also: [`leftjoin`](@ref), [`rightjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> innerjoin(name, job, on = :ID)
2×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String   │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> innerjoin(name, job2, on = :ID => :identifier)
2×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String   │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │

julia> innerjoin(name, job2, on = [:ID => :identifier])
2×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String   │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │
```
"""
innerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          makeunique::Bool=false,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:inner, makeunique=makeunique,
          indicator=nothing, validate=validate)

innerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          makeunique::Bool=false,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    innerjoin(innerjoin(df1, df2, on=on, makeunique=makeunique, validate=validate),
              dfs..., on=on, makeunique=makeunique, validate=validate)

"""
    leftjoin(df1, df2; on, makeunique = false,
             indicator = nothing, validate = (false, false))

Perform a left join of twodata frame objects and return a `DataFrame` containing
the result. A left join includes all rows from `df1`.

# Arguments
- `df1`, `df2`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed).
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `indicator` : Default: `nothing`. If a `Symbol`, adds categorical indicator
   column named `Symbol` for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If `Symbol` is already in use,
   the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

All columns of the returned data table will support missing values.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`rightjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> leftjoin(name, job, on = :ID)
3×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String    │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> leftjoin(name, job2, on = :ID => :identifier)
3×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String    │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │

julia> leftjoin(name, job2, on = [:ID => :identifier])
3×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String    │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │
```
"""
leftjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[],
         makeunique::Bool=false, indicator::Union{Nothing, Symbol} = nothing,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:left, makeunique=makeunique, indicator=indicator,
          validate=validate)

"""
    rightjoin(df1, df2; on, makeunique = false,
              indicator = nothing, validate = (false, false))

Perform a right join on two data frame objects and return a `DataFrame` containing
the result. A right join includes all rows from `df2`.

# Arguments
- `df1`, `df2`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed).
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `indicator` : Default: `nothing`. If a `Symbol`, adds categorical indicator
   column named `Symbol` for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If `Symbol` is already in use,
   the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

All columns of the returned data table will support missing values.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> rightjoin(name, job, on = :ID)
3×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String?  │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │
│ 3   │ 4     │ missing  │ Farmer │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> rightjoin(name, job2, on = :ID => :identifier)
3×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String?  │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │
│ 3   │ 4     │ missing  │ Farmer │

julia> rightjoin(name, job2, on = [:ID => :identifier])
3×3 DataFrame
│ Row │ ID    │ Name     │ Job    │
│     │ Int64 │ String?  │ String │
├─────┼───────┼──────────┼────────┤
│ 1   │ 1     │ John Doe │ Lawyer │
│ 2   │ 2     │ Jane Doe │ Doctor │
│ 3   │ 4     │ missing  │ Farmer │
```
"""
rightjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          makeunique::Bool=false, indicator::Union{Nothing, Symbol} = nothing,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:right, makeunique=makeunique, indicator=indicator,
                 validate=validate)

"""
    outerjoin(df1, df2; on, kind = :inner, makeunique = false,
              indicator = nothing, validate = (false, false))
    outerjoin(df1, df2, dfs...; on, kind = :inner, makeunique = false,
              validate = (false, false))

Perform an outer join of two or more data frame objects and return a `DataFrame`
containing the result. An outer join includes rows with keys that appear in any
of the passed data frames.

# Arguments
- `df1`, `df2`, `dfs...` : the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed). If more than two data frames are joined
  then only a column name or a vector of column names are allowed.
  `on` is a required argument.
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `indicator` : Default: `nothing`. If a `Symbol`, adds categorical indicator
   column named `Symbol` for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If `Symbol` is already in use,
   the column name will be modified if `makeunique=true`.
   This argument is only supported when joining exactly two data frames.
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

All columns of the returned data table will support missing values.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

If more than two data frames are passed, the join is performed
recursively with left associativity.
In this case the `indicator` keyword argument is not supported
and `validate` keyword argument is applied recursively with left associativity.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> outerjoin(name, job, on = :ID)
4×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String?   │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │
│ 4   │ 4     │ missing   │ Farmer  │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> outerjoin(name, job2, on = :ID => :identifier)
4×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String?   │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │
│ 4   │ 4     │ missing   │ Farmer  │

julia> outerjoin(name, job2, on = [:ID => :identifier])
4×3 DataFrame
│ Row │ ID    │ Name      │ Job     │
│     │ Int64 │ String?   │ String? │
├─────┼───────┼───────────┼─────────┤
│ 1   │ 1     │ John Doe  │ Lawyer  │
│ 2   │ 2     │ Jane Doe  │ Doctor  │
│ 3   │ 3     │ Joe Blogs │ missing │
│ 4   │ 4     │ missing   │ Farmer  │
```
"""
outerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          makeunique::Bool=false, indicator::Union{Nothing, Symbol} = nothing,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:outer, makeunique=makeunique, indicator=indicator,
          validate=validate)

outerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    outerjoin(outerjoin(df1, df2, on=on, makeunique=makeunique, validate=validate),
              dfs..., on=on, makeunique=makeunique, validate=validate)

"""
    semijoin(df1, df2; on, makeunique = false, validate = (false, false))

Perform a semi join of two data frame objects and return a `DataFrame`
containing the result. A semi join returns the subset of rows of `df1` that
match with the keys in `df2`.

# Arguments
- `df1`, `df2`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed).
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `indicator` : Default: `nothing`. If a `Symbol`, adds categorical indicator
   column named `Symbol` for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If `Symbol` is already in use,
   the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`outerjoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> semijoin(name, job, on = :ID)
2×2 DataFrame
│ Row │ ID    │ Name     │
│     │ Int64 │ String   │
├─────┼───────┼──────────┤
│ 1   │ 1     │ John Doe │
│ 2   │ 2     │ Jane Doe │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> semijoin(name, job2, on = :ID => :identifier)
2×2 DataFrame
│ Row │ ID    │ Name     │
│     │ Int64 │ String   │
├─────┼───────┼──────────┤
│ 1   │ 1     │ John Doe │
│ 2   │ 2     │ Jane Doe │

julia> semijoin(name, job2, on = [:ID => :identifier])
2×2 DataFrame
│ Row │ ID    │ Name     │
│     │ Int64 │ String   │
├─────┼───────┼──────────┤
│ 1   │ 1     │ John Doe │
│ 2   │ 2     │ Jane Doe │
```
"""
semijoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:semi, makeunique=makeunique,
          indicator=nothing, validate=validate)

"""
    antijoin(df1, df2; on, makeunique = false, validate = (false, false))

Perform an anti join of two data frame objects and return a `DataFrame`
containing the result. An anti join returns the subset of rows of `df1` that do
not match with the keys in `df2`.

# Arguments
- `df1`, `df2`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed).
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`outerjoin`](@ref), [`semijoin`](@ref), [`crossjoin`](@ref).

# Examples
```julia
julia> name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 1     │ John Doe  │
│ 2   │ 2     │ Jane Doe  │
│ 3   │ 3     │ Joe Blogs │

julia> job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ ID    │ Job    │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ Lawyer │
│ 2   │ 2     │ Doctor │
│ 3   │ 4     │ Farmer │

julia> antijoin(name, job, on = :ID)
1×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 3     │ Joe Blogs │

julia> job2 = DataFrame(identifier = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
│ Row │ identifier │ Job    │
│     │ Int64      │ String │
├─────┼────────────┼────────┤
│ 1   │ 1          │ Lawyer │
│ 2   │ 2          │ Doctor │
│ 3   │ 4          │ Farmer │

julia> antijoin(name, job2, on = :ID => :identifier)
1×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 3     │ Joe Blogs │

julia> antijoin(name, job2, on = [:ID => :identifier])
1×2 DataFrame
│ Row │ ID    │ Name      │
│     │ Int64 │ String    │
├─────┼───────┼───────────┤
│ 1   │ 3     │ Joe Blogs │
```
"""
antijoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    _join(df1, df2, on=on, kind=:anti, makeunique=makeunique,
          indicator=nothing, validate=validate)

"""
    crossjoin(df1, df2, dfs...; makeunique = false)

Perform a cross join of two or more data frame objects and return a `DataFrame`
containing the result. A cross join returns the cartesian product of rows from
all passed data frames.

# Arguments
- `df1`, `df2`, `dfs...` : the `AbstractDataFrames` to be joined

# Keyword Arguments
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).

If more than two data frames are passed, the join is performed
recursively with left associativity.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`outerjoin`](@ref), [`semijoin`](@ref), [`antijoin`](@ref).

# Examples
```julia
julia> df1 = DataFrame(X=1:3)
3×1 DataFrame
│ Row │ X     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> df2 = DataFrame(Y=["a", "b"])
2×1 DataFrame
│ Row │ Y      │
│     │ String │
├─────┼────────┤
│ 1   │ a      │
│ 2   │ b      │

julia> crossjoin(df1, df2)
6×2 DataFrame
│ Row │ X     │ Y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ a      │
│ 2   │ 1     │ b      │
│ 3   │ 2     │ a      │
│ 4   │ 2     │ b      │
│ 5   │ 3     │ a      │
│ 6   │ 3     │ b      │
```
"""
function crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame; makeunique::Bool=false)
    _check_consistency(df1)
    _check_consistency(df2)
    r1, r2 = size(df1, 1), size(df2, 1)
    colindex = merge(index(df1), index(df2), makeunique=makeunique)
    cols = Any[[repeat(c, inner=r2) for c in eachcol(df1)];
               [repeat(c, outer=r1) for c in eachcol(df2)]]
    return DataFrame(cols, colindex, copycols=false)
end

crossjoin(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          makeunique::Bool=false) =
    crossjoin(crossjoin(df1, df2, makeunique=makeunique), dfs..., makeunique=makeunique)
