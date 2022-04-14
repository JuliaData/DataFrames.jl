##
## Join / merge
##
## Implements methods for join functions defined in DataAPI.jl
##

# Like similar, but returns a array that can have missings and is initialized with missings
similar_missing(dv::AbstractArray{T}, dims::Union{Int, Tuple{Vararg{Int}}}) where {T} =
    fill!(similar(dv, Union{T, Missing}, dims), missing)

similar_outer(leftcol::AbstractVector, rightcol::AbstractVector, n::Int) =
    Tables.allocatecolumn(promote_type(eltype(leftcol), eltype(rightcol)), n)

const OnType = Union{SymbolOrString, NTuple{2, Symbol}, Pair{Symbol, Symbol},
                     Pair{<:AbstractString, <:AbstractString}}

# helper structure for DataFrames joining
struct DataFrameJoiner
    dfl::AbstractDataFrame
    dfr::AbstractDataFrame
    dfl_on::AbstractDataFrame
    dfr_on::AbstractDataFrame
    left_on::Vector{Symbol}
    right_on::Vector{Symbol}

    function DataFrameJoiner(dfl::AbstractDataFrame, dfr::AbstractDataFrame,
                             on::Union{<:OnType, AbstractVector},
                             matchmissing::Symbol,
                             kind::Symbol)
        on_cols = isa(on, AbstractVector) ? on : [on]
        left_on = Symbol[]
        right_on = Symbol[]
        for v in on_cols
            if v isa SymbolOrString
                push!(left_on, Symbol(v))
                push!(right_on, Symbol(v))
            elseif v isa Union{Pair{Symbol, Symbol},
                               Pair{<:AbstractString, <:AbstractString}}
                push!(left_on, Symbol(first(v)))
                push!(right_on, Symbol(last(v)))
            elseif v isa NTuple{2, Symbol}
                # an explicit error is thrown as Tuple{Symbol, Symbol} was supported in the past
                throw(ArgumentError("Using a `Tuple{Symbol, Symbol}` or a vector containing " *
                                    "such tuples as a value of `on` keyword argument is " *
                                    "not supported: use `Pair{Symbol, Symbol}` instead."))
            else
                throw(ArgumentError("All elements of `on` argument to `join` must be " *
                                    "Symbol or Pair{Symbol, Symbol}."))
            end
        end

        if matchmissing === :notequal
            if kind in (:left, :semi, :anti)
                dfr = dropmissing(dfr, right_on, view=true)
            elseif kind === :right
                dfl = dropmissing(dfl, left_on, view=true)
            elseif kind === :inner
                # it possible to drop only left or right df
                # to gain some performance but needs more testing, see #2724
                dfl = dropmissing(dfl, left_on, view=true)
                dfr = dropmissing(dfr, right_on, view=true)
            elseif kind === :outer
                throw(ArgumentError("matchmissing == :notequal for `outerjoin` is not allowed"))
            else
                throw(ArgumentError("matchmissing == :notequal not implemented for kind == :$kind"))
            end
        end
        dfl_on = select(dfl, left_on, copycols=false)
        dfr_on = select(dfr, right_on, copycols=false)
        if matchmissing === :error
            for df in (dfl_on, dfr_on), col in eachcol(df)
                if any(ismissing, col)
                    throw(ArgumentError("missing values in key columns are not allowed " *
                                        "when matchmissing == :error"))
                end
            end
        elseif !(matchmissing in (:equal, :notequal))
            throw(ArgumentError("matchmissing allows only :error, :equal, or :notequal"))
        end
        for df in (dfl_on, dfr_on), col in eachcol(df)
            if any(x -> (x isa Union{Complex, Real}) &&
                        (isnan(x) || isequal(real(x), -0.0) || isequal(imag(x), -0.0)), col)
                throw(ArgumentError("currently for numeric values NaN and `-0.0` " *
                                    "in their real or imaginary components are not " *
                                    "allowed. Use CategoricalArrays.jl to wrap " *
                                    "these values in a CategoricalVector to perform " *
                                    "the requested join."))
            end
        end

        new(dfl, dfr, dfl_on, dfr_on, left_on, right_on)
    end
end

_rename_cols(old_names::AbstractVector{Symbol},
             renamecols::Union{Function, Symbol, AbstractString},
             exclude::AbstractVector{Symbol} = Symbol[]) =
    Symbol[n in exclude ? n :
           (renamecols isa Function ? Symbol(renamecols(string(n))) : Symbol(n, renamecols))
           for n in old_names]

function compose_inner_table(joiner::DataFrameJoiner,
                             makeunique::Bool,
                             left_rename::Union{Function, AbstractString, Symbol},
                             right_rename::Union{Function, AbstractString, Symbol})
    left_ixs, right_ixs = find_inner_rows(joiner)

    @static if VERSION >= v"1.4"
        if Threads.nthreads() > 1 && length(left_ixs) >= 1_000_000
            dfl_task = Threads.@spawn joiner.dfl[left_ixs, :]
            dfr_noon_task = Threads.@spawn joiner.dfr[right_ixs, Not(joiner.right_on)]
            dfl = fetch(dfl_task)
            dfr_noon = fetch(dfr_noon_task)
        else
            dfl = joiner.dfl[left_ixs, :]
            dfr_noon = joiner.dfr[right_ixs, Not(joiner.right_on)]
        end
    else
        dfl = joiner.dfl[left_ixs, :]
        dfr_noon = joiner.dfr[right_ixs, Not(joiner.right_on)]
    end

    ncleft = ncol(dfl)
    cols = Vector{AbstractVector}(undef, ncleft + ncol(dfr_noon))

    for (i, col) in enumerate(eachcol(dfl))
        cols[i] = col
    end
    for (i, col) in enumerate(eachcol(dfr_noon))
        cols[i+ncleft] = col
    end

    new_names = vcat(_rename_cols(_names(joiner.dfl), left_rename, joiner.left_on),
                     _rename_cols(_names(dfr_noon), right_rename))
    res = DataFrame(cols, new_names, makeunique=makeunique, copycols=false)

    return res
end

function find_missing_idxs(present::Vector{Int}, target_len::Int)
    not_seen = trues(target_len)
    @inbounds for v in present
        not_seen[v] = false
    end
    return _findall(not_seen)
end

function compose_joined_table(joiner::DataFrameJoiner, kind::Symbol, makeunique::Bool,
                              left_rename::Union{Function, AbstractString, Symbol},
                              right_rename::Union{Function, AbstractString, Symbol},
                              indicator::Union{Nothing, Symbol, AbstractString})
    @assert kind == :left || kind == :right || kind == :outer
    left_ixs, right_ixs = find_inner_rows(joiner)

    if kind == :left || kind == :outer
        leftonly_ixs = find_missing_idxs(left_ixs, nrow(joiner.dfl))
    else
        leftonly_ixs = 1:0
    end

    if kind == :right || kind == :outer
        rightonly_ixs = find_missing_idxs(right_ixs, nrow(joiner.dfr))
    else
        rightonly_ixs = 1:0
    end
    return _compose_joined_table(joiner, kind, makeunique, left_rename, right_rename,
                                 indicator, left_ixs, right_ixs, leftonly_ixs, rightonly_ixs)
end

function _compose_joined_table(joiner::DataFrameJoiner, kind::Symbol, makeunique::Bool,
                               left_rename::Union{Function, AbstractString, Symbol},
                               right_rename::Union{Function, AbstractString, Symbol},
                               indicator::Union{Nothing, Symbol, AbstractString},
                               left_ixs::AbstractVector, right_ixs::AbstractVector,
                               leftonly_ixs::AbstractVector, rightonly_ixs::AbstractVector)
    lil = length(left_ixs)
    ril = length(right_ixs)
    loil = length(leftonly_ixs)
    roil = length(rightonly_ixs)

    @assert lil == ril

    dfl_noon = select(joiner.dfl, Not(joiner.left_on), copycols=false)
    dfr_noon = select(joiner.dfr, Not(joiner.right_on), copycols=false)

    target_nrow = lil + loil + roil

    _similar_left = kind == :left ? similar : similar_missing
    _similar_right = kind == :right ? similar : similar_missing

    if isnothing(indicator)
        src_indicator = nothing
    else
        src_indicator = Vector{UInt32}(undef, target_nrow)
        src_indicator[1:lil] .= 3
        src_indicator[lil + 1:lil + loil] .= 1
        src_indicator[lil + loil + 1:target_nrow] .= 2
    end

    cols = Vector{AbstractVector}(undef, ncol(joiner.dfl) + ncol(dfr_noon))

    col_idx = 1

    left_idxs = [columnindex(joiner.dfl, n) for n in joiner.left_on]
    append!(left_idxs, setdiff(1:ncol(joiner.dfl), left_idxs))

    if kind == :left
        @assert loil + lil == target_nrow
        for col in eachcol(joiner.dfl_on)
            cols_i = left_idxs[col_idx]
            cols[cols_i] = similar(col, target_nrow)
            copyto!(cols[cols_i], view(col, left_ixs))
            copyto!(cols[cols_i], lil + 1, view(col, leftonly_ixs), 1, loil)
            col_idx += 1
        end
    elseif kind == :right
        @assert roil + ril == target_nrow
        for col in eachcol(joiner.dfr_on)
            cols_i = left_idxs[col_idx]
            cols[cols_i] = similar(col, target_nrow)
            copyto!(cols[cols_i], view(col, right_ixs))
            copyto!(cols[cols_i], lil + 1, view(col, rightonly_ixs), 1, roil)
            col_idx += 1
        end
    else
        @assert kind == :outer
        @assert loil + roil + ril == target_nrow
        for (lcol, rcol) in zip(eachcol(joiner.dfl_on), eachcol(joiner.dfr_on))
            cols_i = left_idxs[col_idx]
            cols[cols_i] = similar_outer(lcol, rcol, target_nrow)
            copyto!(cols[cols_i], view(lcol, left_ixs))
            copyto!(cols[cols_i], lil + 1, view(lcol, leftonly_ixs), 1, loil)
            copyto!(cols[cols_i], lil + loil + 1, view(rcol, rightonly_ixs), 1, roil)
            col_idx += 1
        end
    end

    @assert col_idx == ncol(joiner.dfl_on) + 1

    @static if VERSION >= v"1.4"
        if Threads.nthreads() > 1 && target_nrow >= 1_000_000 && length(cols) > col_idx
            @sync begin
                for col in eachcol(dfl_noon)
                    cols_i = left_idxs[col_idx]
                    Threads.@spawn _noon_compose_helper!(cols, _similar_left, cols_i,
                                                         col, target_nrow, left_ixs, lil + 1, leftonly_ixs, loil)
                    col_idx += 1
                end
                @assert col_idx == ncol(joiner.dfl) + 1
                for col in eachcol(dfr_noon)
                    cols_i = col_idx
                    Threads.@spawn _noon_compose_helper!(cols, _similar_right, cols_i, col, target_nrow,
                                                         right_ixs, lil + loil + 1, rightonly_ixs, roil)
                    col_idx += 1
                end
            end
        else
            for col in eachcol(dfl_noon)
                _noon_compose_helper!(cols, _similar_left, left_idxs[col_idx],
                                      col, target_nrow, left_ixs, lil + 1, leftonly_ixs, loil)
                col_idx += 1
            end
            @assert col_idx == ncol(joiner.dfl) + 1
            for col in eachcol(dfr_noon)
                _noon_compose_helper!(cols, _similar_right, col_idx, col, target_nrow,
                                      right_ixs, lil + loil + 1, rightonly_ixs, roil)
                col_idx += 1
            end
        end
    else
        for col in eachcol(dfl_noon)
            _noon_compose_helper!(cols, _similar_left, left_idxs[col_idx],
                                  col, target_nrow, left_ixs, lil + 1, leftonly_ixs, loil)
            col_idx += 1
        end
        @assert col_idx == ncol(joiner.dfl) + 1
        for col in eachcol(dfr_noon)
            _noon_compose_helper!(cols, _similar_right, col_idx, col, target_nrow,
                                  right_ixs, lil + loil + 1, rightonly_ixs, roil)
            col_idx += 1
        end
    end

    @assert col_idx == length(cols) + 1

    new_names = vcat(_rename_cols(_names(joiner.dfl), left_rename, joiner.left_on),
                     _rename_cols(_names(dfr_noon), right_rename))
    res = DataFrame(cols, new_names, makeunique=makeunique, copycols=false)

    return res, src_indicator
end

function _noon_compose_helper!(cols::Vector{AbstractVector}, # target container to populate
                               similar_col::Function, # function to use to materialize new column
                               cols_i::Integer, # index in cols to populate
                               col::AbstractVector, # source column
                               target_nrow::Integer, # target number of rows in new column
                               side_ixs::AbstractVector, # indices in col that were matched
                               offset::Integer, # offset to put non matched indices
                               sideonly_ixs::AbstractVector, # indices in col that were not
                               tocopy::Integer) # number on non-matched rows to copy
    @assert tocopy == length(sideonly_ixs)
    cols[cols_i] = similar_col(col, target_nrow)
    copyto!(cols[cols_i], view(col, side_ixs))
    copyto!(cols[cols_i], offset, view(col, sideonly_ixs), 1, tocopy)
end

function _join(df1::AbstractDataFrame, df2::AbstractDataFrame;
               on::Union{<:OnType, AbstractVector}, kind::Symbol, makeunique::Bool,
               indicator::Union{Nothing, Symbol, AbstractString},
               validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}},
               left_rename::Union{Function, AbstractString, Symbol},
               right_rename::Union{Function, AbstractString, Symbol},
               matchmissing::Symbol)
    _check_consistency(df1)
    _check_consistency(df2)

    if on == []
        throw(ArgumentError("Missing join argument 'on'."))
    end

    joiner = DataFrameJoiner(df1, df2, on, matchmissing, kind)

    # Check merge key validity
    if validate[1]
        non_unique_left = nonunique(joiner.dfl, joiner.left_on)
        if any(non_unique_left)
            left_invalid = true
            non_unique_left_df = unique(joiner.dfl[non_unique_left, joiner.left_on])
            nrow_nul_df = nrow(non_unique_left_df)
            @assert nrow_nul_df > 0
            if nrow_nul_df == 1
                left_invalid_msg = "df1 contains 1 duplicate key: " *
                                   "$(NamedTuple(non_unique_left_df[1, :])). "
            elseif nrow_nul_df == 2
                left_invalid_msg = "df1 contains 2 duplicate keys: " *
                                   "$(NamedTuple(non_unique_left_df[1, :])) and " *
                                   "$(NamedTuple(non_unique_left_df[2, :])). "
            else
                left_invalid_msg = "df1 contains $(nrow_nul_df) duplicate keys: " *
                                   "$(NamedTuple(non_unique_left_df[1, :])), ..., " *
                                   "$(NamedTuple(non_unique_left_df[end, :])). "
            end

        else
            left_invalid = false
            left_invalid_msg = ""
        end
    else
        left_invalid = false
        left_invalid_msg = ""
    end

    if validate[2]
        non_unique_right = nonunique(joiner.dfr, joiner.right_on)
        if any(non_unique_right)
            right_invalid = true
            non_unique_right_df = unique(joiner.dfr[non_unique_right, joiner.right_on])
            nrow_nur_df = nrow(non_unique_right_df)
            @assert nrow_nur_df > 0
            if nrow_nur_df == 1
                right_invalid_msg = "df2 contains 1 duplicate key: " *
                                    "$(NamedTuple(non_unique_right_df[1, :]))."
            elseif nrow_nur_df == 2
                right_invalid_msg = "df2 contains 2 duplicate keys: " *
                                    "$(NamedTuple(non_unique_right_df[1, :])) and " *
                                    "$(NamedTuple(non_unique_right_df[2, :]))."
            else
                right_invalid_msg = "df2 contains $(nrow_nur_df) duplicate keys: " *
                                    "$(NamedTuple(non_unique_right_df[1, :])), ..., " *
                                    "$(NamedTuple(non_unique_right_df[end, :]))."
            end

        else
            right_invalid = false
            right_invalid_msg = ""
        end
    else
        right_invalid = false
        right_invalid_msg = ""
    end

    if left_invalid && right_invalid
        first_error_df1 = findfirst(nonunique(joiner.dfl, joiner.left_on))
        first_error_df2 = findfirst(nonunique(joiner.dfr, joiner.right_on))
        throw(ArgumentError("Merge key(s) are not unique in both df1 and df2. " *
                            left_invalid_msg * right_invalid_msg))
    elseif left_invalid
        first_error = findfirst(nonunique(joiner.dfl, joiner.left_on))
        throw(ArgumentError("Merge key(s) in df1 are not unique. " *
                            left_invalid_msg))
    elseif right_invalid
        first_error = findfirst(nonunique(joiner.dfr, joiner.right_on))
        throw(ArgumentError("Merge key(s) in df2 are not unique. " *
                            right_invalid_msg))
    end

    src_indicator = nothing
    if kind == :inner
        joined = compose_inner_table(joiner, makeunique, left_rename, right_rename)
    elseif kind == :left
        joined, src_indicator =
            compose_joined_table(joiner, kind, makeunique, left_rename, right_rename, indicator)
    elseif kind == :right
        joined, src_indicator =
            compose_joined_table(joiner, kind, makeunique, left_rename, right_rename, indicator)
    elseif kind == :outer
        joined, src_indicator =
            compose_joined_table(joiner, kind, makeunique, left_rename, right_rename, indicator)
    elseif kind == :semi
        joined = joiner.dfl[find_semi_rows(joiner), :]
    elseif kind == :anti
        joined = joiner.dfl[.!find_semi_rows(joiner), :]
    else
        throw(ArgumentError("Unknown kind of join requested: $kind"))
    end

    if indicator !== nothing
        pool = ["left_only", "right_only", "both"]
        invpool = Dict{String, UInt32}("left_only" => 1,
                                       "right_only" => 2,
                                       "both" => 3)
        indicatorcol = PooledArray(PooledArrays.RefArray(src_indicator),
                                   invpool, pool)

        unique_indicator = indicator
        if makeunique
            try_idx = 0
            while hasproperty(joined, unique_indicator)
                try_idx += 1
                unique_indicator = Symbol(indicator, "_", try_idx)
            end
        end

        if hasproperty(joined, unique_indicator)
            throw(ArgumentError("joined data frame already has column " *
                                ":$unique_indicator. Pass makeunique=true to " *
                                "make it unique using a suffix automatically."))
        end
        joined[!, unique_indicator] = indicatorcol
    else
        @assert isnothing(src_indicator)
    end

    return joined
end

"""
    innerjoin(df1, df2; on, makeunique=false, validate=(false, false),
              renamecols=(identity => identity), matchmissing=:error)
    innerjoin(df1, df2, dfs...; on, makeunique=false,
              validate=(false, false), matchmissing=:error)

Perform an inner join of two or more data frame objects and return a `DataFrame`
containing the result. An inner join includes rows with keys that match in all
passed data frames.

The order of rows in the result is undefined and may change in the future releases.

In the returned data frame the type of the columns on which the data frames are
joined is determined by the type of these columns in `df1`. This behavior may
change in future releases.

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
- `renamecols` : a `Pair` specifying how columns of left and right data frames should
  be renamed in the resulting data frame. Each element of the pair can be a
  string or a `Symbol` can be passed in which case it is appended to the original
  column name; alternatively a function can be passed in which case it is applied
  to each column name, which is passed to it as a `String`. Note that `renamecols`
  does not affect `on` columns, whose names are always taken from the left
  data frame and left unchanged.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df1` and `df2`
  `on` columns; `isequal` is used for comparisons of rows for equality

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

If more than two data frames are passed, the join is performed recursively with
left associativity. In this case the `validate` keyword argument is applied
recursively with left associativity.

See also: [`leftjoin`](@ref), [`rightjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> innerjoin(name, job, on = :ID)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String
─────┼─────────────────────────
   1 │     1  John Doe  Lawyer
   2 │     2  Jane Doe  Doctor

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> innerjoin(name, job2, on = :ID => :identifier, renamecols = "_left" => "_right")
2×3 DataFrame
 Row │ ID     Name_left  Job_right
     │ Int64  String     String
─────┼─────────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor

julia> innerjoin(name, job2, on = [:ID => :identifier], renamecols = uppercase => lowercase)
2×3 DataFrame
 Row │ ID     NAME      job
     │ Int64  String    String
─────┼─────────────────────────
   1 │     1  John Doe  Lawyer
   2 │     2  Jane Doe  Doctor
```
"""
function innerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
                   on::Union{<:OnType, AbstractVector} = Symbol[],
                   makeunique::Bool=false,
                   validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
                   renamecols::Pair=identity => identity,
                   matchmissing::Symbol=:error)
    if !all(x -> x isa Union{Function, AbstractString, Symbol}, renamecols)
        throw(ArgumentError("renamecols keyword argument must be a `Pair` " *
                            "containing functions, strings, or `Symbol`s"))
    end
    return _join(df1, df2, on=on, kind=:inner, makeunique=makeunique,
                 indicator=nothing, validate=validate,
                 left_rename=first(renamecols), right_rename=last(renamecols),
                 matchmissing=matchmissing)
end

innerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          makeunique::Bool=false,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
          matchmissing::Symbol=:error) =
    innerjoin(innerjoin(df1, df2, on=on, makeunique=makeunique, validate=validate,
                        matchmissing=matchmissing),
              dfs..., on=on, makeunique=makeunique, validate=validate,
              matchmissing=matchmissing)

"""
    leftjoin(df1, df2; on, makeunique=false, source=nothing, validate=(false, false),
             renamecols=(identity => identity), matchmissing=:error)

Perform a left join of two data frame objects and return a `DataFrame` containing
the result. A left join includes all rows from `df1`.

The order of rows in the result is undefined and may change in the future releases.

In the returned data frame the type of the columns on which the data frames are
joined is determined by the type of these columns in `df1`. This behavior may
change in future releases.

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
- `source` : Default: `nothing`. If a `Symbol` or string, adds indicator
  column with the given name, for whether a row appeared in only `df1` (`"left_only"`)
  or in both (`"both"`). If the name is already in use,
  the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
  define unique keys in each input data frame (according to `isequal`).
  Can be a tuple or a pair, with the first element indicating whether to
  run check for `df1` and the second element for `df2`.
  By default no check is performed.
- `renamecols` : a `Pair` specifying how columns of left and right data frames should
  be renamed in the resulting data frame. Each element of the pair can be a
  string or a `Symbol` can be passed in which case it is appended to the original
  column name; alternatively a function can be passed in which case it is applied
  to each column name, which is passed to it as a `String`. Note that `renamecols`
  does not affect `on` columns, whose names are always taken from the left
  data frame and left unchanged.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df2` `on` columns;
  `isequal` is used for comparisons of rows for equality

All columns of the returned data frame will support missing values.

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`rightjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> leftjoin(name, job, on = :ID)
3×3 DataFrame
 Row │ ID     Name       Job
     │ Int64  String     String?
─────┼───────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     3  Joe Blogs  missing

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> leftjoin(name, job2, on = :ID => :identifier, renamecols = "_left" => "_right")
3×3 DataFrame
 Row │ ID     Name_left  Job_right
     │ Int64  String     String?
─────┼─────────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     3  Joe Blogs  missing

julia> leftjoin(name, job2, on = [:ID => :identifier], renamecols = uppercase => lowercase)
3×3 DataFrame
 Row │ ID     NAME       job
     │ Int64  String     String?
─────┼───────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     3  Joe Blogs  missing
```
"""
function leftjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
         source::Union{Nothing, Symbol, AbstractString}=nothing,
         indicator::Union{Nothing, Symbol, AbstractString}=nothing,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
         renamecols::Pair=identity => identity, matchmissing::Symbol=:error)
    if !all(x -> x isa Union{Function, AbstractString, Symbol}, renamecols)
        throw(ArgumentError("renamecols keyword argument must be a `Pair` " *
                            "containing functions, strings, or `Symbol`s"))
    end
    if source === nothing
        if indicator !== nothing
            source = indicator
            Base.depwarn("`indicator` keyword argument is deprecated and " *
                         "will be removed in 2.0 release of DataFrames.jl. " *
                         "Use `source` keyword argument instead.", :leftjoin)
        end
    elseif indicator !== nothing
        throw(ArgumentError("`indicator` keyword argument is deprecated. " *
                            "It is not allowed to pass both `indicator` and `source` " *
                            "keyword arguments at the same time."))
    end
    return _join(df1, df2, on=on, kind=:left, makeunique=makeunique,
                 indicator=source, validate=validate,
                 left_rename=first(renamecols), right_rename=last(renamecols),
                 matchmissing=matchmissing)
end

"""
    rightjoin(df1, df2; on, makeunique=false, source=nothing,
              validate=(false, false), renamecols=(identity => identity),
              matchmissing=:error)

Perform a right join on two data frame objects and return a `DataFrame` containing
the result. A right join includes all rows from `df2`.

The order of rows in the result is undefined and may change in the future releases.

In the returned data frame the type of the columns on which the data frames are
joined is determined by the type of these columns in `df2`. This behavior may
change in future releases.

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
- `source` : Default: `nothing`. If a `Symbol` or string, adds indicator
  column with the given name for whether a row appeared in only `df2` (`"right_only"`)
  or in both (`"both"`). If the name is already in use,
  the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
  define unique keys in each input data frame (according to `isequal`).
  Can be a tuple or a pair, with the first element indicating whether to
  run check for `df1` and the second element for `df2`.
  By default no check is performed.
- `renamecols` : a `Pair` specifying how columns of left and right data frames should
  be renamed in the resulting data frame. Each element of the pair can be a
  string or a `Symbol` can be passed in which case it is appended to the original
  column name; alternatively a function can be passed in which case it is applied
  to each column name, which is passed to it as a `String`. Note that `renamecols`
  does not affect `on` columns, whose names are always taken from the left
  data frame and left unchanged.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df1` `on` columns;
  `isequal` is used for comparisons of rows for equality

All columns of the returned data frame will support missing values.

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`outerjoin`](@ref),
          [`semijoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> rightjoin(name, job, on = :ID)
3×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String?   String
─────┼─────────────────────────
   1 │     1  John Doe  Lawyer
   2 │     2  Jane Doe  Doctor
   3 │     4  missing   Farmer

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> rightjoin(name, job2, on = :ID => :identifier, renamecols = "_left" => "_right")
3×3 DataFrame
 Row │ ID     Name_left  Job_right
     │ Int64  String?    String
─────┼─────────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     4  missing    Farmer

julia> rightjoin(name, job2, on = [:ID => :identifier], renamecols = uppercase => lowercase)
3×3 DataFrame
 Row │ ID     NAME      job
     │ Int64  String?   String
─────┼─────────────────────────
   1 │     1  John Doe  Lawyer
   2 │     2  Jane Doe  Doctor
   3 │     4  missing   Farmer
```
"""
function rightjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
          on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
          source::Union{Nothing, Symbol, AbstractString}=nothing,
          indicator::Union{Nothing, Symbol, AbstractString}=nothing,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
          renamecols::Pair=identity => identity, matchmissing::Symbol=:error)
    if !all(x -> x isa Union{Function, AbstractString, Symbol}, renamecols)
        throw(ArgumentError("renamecols keyword argument must be a `Pair` " *
                            "containing functions, strings, or `Symbol`s"))
    end
    if source === nothing
        if indicator !== nothing
            source = indicator
            Base.depwarn("`indicator` keyword argument is deprecated and " *
                         "will be removed in 2.0 release of DataFrames.jl. " *
                         "Use `source` keyword argument instead.", :rightjoin)
        end
    elseif indicator !== nothing
        throw(ArgumentError("`indicator` keyword argument is deprecated. " *
                            "It is not allowed to pass both `indicator` and `source` " *
                            "keyword arguments at the same time."))
    end
    return _join(df1, df2, on=on, kind=:right, makeunique=makeunique,
                 indicator=source, validate=validate,
                 left_rename=first(renamecols), right_rename=last(renamecols),
                 matchmissing=matchmissing)
end

"""
    outerjoin(df1, df2; on, makeunique=false, source=nothing, validate=(false, false),
              renamecols=(identity => identity), matchmissing=:error)
    outerjoin(df1, df2, dfs...; on, makeunique = false,
              validate = (false, false), matchmissing=:error)

Perform an outer join of two or more data frame objects and return a `DataFrame`
containing the result. An outer join includes rows with keys that appear in any
of the passed data frames.

The order of rows in the result is undefined and may change in the future releases.

In the returned data frame the type of the columns on which the data frames are
joined is determined by the element type of these columns both `df1` and `df2`.
This behavior may change in future releases.

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
- `source` : Default: `nothing`. If a `Symbol` or string, adds indicator
  column with the given name for whether a row appeared in only `df1` (`"left_only"`),
  only `df2` (`"right_only"`) or in both (`"both"`). If the name is already in use,
  the column name will be modified if `makeunique=true`.
  This argument is only supported when joining exactly two data frames.
- `validate` : whether to check that columns passed as the `on` argument
  define unique keys in each input data frame (according to `isequal`).
  Can be a tuple or a pair, with the first element indicating whether to
  run check for `df1` and the second element for `df2`.
  By default no check is performed.
- `renamecols` : a `Pair` specifying how columns of left and right data frames should
  be renamed in the resulting data frame. Each element of the pair can be a
  string or a `Symbol` can be passed in which case it is appended to the original
  column name; alternatively a function can be passed in which case it is applied
  to each column name, which is passed to it as a `String`. Note that `renamecols`
  does not affect `on` columns, whose names are always taken from the left
  data frame and left unchanged.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; `isequal` is used for comparisons of rows for equality

All columns of the returned data frame will support missing values.

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

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
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> outerjoin(name, job, on = :ID)
4×3 DataFrame
 Row │ ID     Name       Job
     │ Int64  String?    String?
─────┼───────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     3  Joe Blogs  missing
   4 │     4  missing    Farmer

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> rightjoin(name, job2, on = :ID => :identifier, renamecols = "_left" => "_right")
3×3 DataFrame
 Row │ ID     Name_left  Job_right
     │ Int64  String?    String
─────┼─────────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     4  missing    Farmer

julia> rightjoin(name, job2, on = [:ID => :identifier], renamecols = uppercase => lowercase)
3×3 DataFrame
 Row │ ID     NAME      job
     │ Int64  String?   String
─────┼─────────────────────────
   1 │     1  John Doe  Lawyer
   2 │     2  Jane Doe  Doctor
   3 │     4  missing   Farmer
```
"""
function outerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
          on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
          source::Union{Nothing, Symbol, AbstractString}=nothing,
          indicator::Union{Nothing, Symbol, AbstractString}=nothing,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
          renamecols::Pair=identity => identity, matchmissing::Symbol=:error)
    if !all(x -> x isa Union{Function, AbstractString, Symbol}, renamecols)
        throw(ArgumentError("renamecols keyword argument must be a `Pair` " *
                            "containing functions, strings, or `Symbol`s"))
    end
    if source === nothing
        if indicator !== nothing
            source = indicator
            Base.depwarn("`indicator` keyword argument is deprecated and " *
                         "will be removed in 2.0 release of DataFrames.jl. " *
                         "Use `source` keyword argument instead.", :outerjoin)
        end
    elseif indicator !== nothing
        throw(ArgumentError("`indicator` keyword argument is deprecated. " *
                            "It is not allowed to pass both `indicator` and `source` " *
                            "keyword arguments at the same time."))
    end
    return _join(df1, df2, on=on, kind=:outer, makeunique=makeunique,
                 indicator=source, validate=validate,
                 left_rename=first(renamecols), right_rename=last(renamecols),
                 matchmissing=matchmissing)
end

outerjoin(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
          matchmissing::Symbol=:error) =
    outerjoin(outerjoin(df1, df2, on=on, makeunique=makeunique, validate=validate,
                        matchmissing=matchmissing),
              dfs..., on=on, makeunique=makeunique, validate=validate,
              matchmissing=matchmissing)

"""
    semijoin(df1, df2; on, makeunique=false, validate=(false, false), matchmissing=:error)

Perform a semi join of two data frame objects and return a `DataFrame`
containing the result. A semi join returns the subset of rows of `df1` that
match with the keys in `df2`.

The order of rows in the result is undefined and may change in the future releases.

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
- `indicator` : Default: `nothing`. If a `Symbol` or string, adds categorical indicator
   column with the given name for whether a row appeared in only `df1` (`"left_only"`),
   only `df2` (`"right_only"`) or in both (`"both"`). If the name is already in use,
   the column name will be modified if `makeunique=true`.
- `validate` : whether to check that columns passed as the `on` argument
   define unique keys in each input data frame (according to `isequal`).
   Can be a tuple or a pair, with the first element indicating whether to
   run check for `df1` and the second element for `df2`.
   By default no check is performed.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df2` `on` columns;
  `isequal` is used for comparisons of rows for equality

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`outerjoin`](@ref), [`antijoin`](@ref), [`crossjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> semijoin(name, job, on = :ID)
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> semijoin(name, job2, on = :ID => :identifier)
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe

julia> semijoin(name, job2, on = [:ID => :identifier])
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
```
"""
semijoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
         matchmissing::Symbol=:error) =
    _join(df1, df2, on=on, kind=:semi, makeunique=makeunique,
          indicator=nothing, validate=validate,
          left_rename=identity, right_rename=identity, matchmissing=matchmissing)

"""
    antijoin(df1, df2; on, makeunique=false, validate=(false, false), matchmissing=:error)

Perform an anti join of two data frame objects and return a `DataFrame`
containing the result. An anti join returns the subset of rows of `df1` that do
not match with the keys in `df2`.

The order of rows in the result is undefined and may change in the future releases.

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
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df2` `on` columns;
  `isequal` is used for comparisons of rows for equality

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

When merging `on` categorical columns that differ in the ordering of their
levels, the ordering of the left data frame takes precedence over the ordering
of the right data frame.

See also: [`innerjoin`](@ref), [`leftjoin`](@ref), [`rightjoin`](@ref),
          [`outerjoin`](@ref), [`semijoin`](@ref), [`crossjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> antijoin(name, job, on = :ID)
1×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     3  Joe Blogs

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> antijoin(name, job2, on = :ID => :identifier)
1×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     3  Joe Blogs

julia> antijoin(name, job2, on = [:ID => :identifier])
1×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     3  Joe Blogs
```
"""
antijoin(df1::AbstractDataFrame, df2::AbstractDataFrame;
         on::Union{<:OnType, AbstractVector} = Symbol[], makeunique::Bool=false,
         validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false),
         matchmissing::Symbol=:error) =
    _join(df1, df2, on=on, kind=:anti, makeunique=makeunique,
          indicator=nothing, validate=validate,
          left_rename=identity, right_rename=identity,
          matchmissing=matchmissing)

"""
    crossjoin(df1, df2, dfs...; makeunique = false)

Perform a cross join of two or more data frame objects and return a `DataFrame`
containing the result. A cross join returns the cartesian product of rows from
all passed data frames, where the first passed data frame is assigned to the
dimension that changes the slowest and the last data frame is assigned to the
dimension that changes the fastest.

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
```jldoctest
julia> df1 = DataFrame(X=1:3)
3×1 DataFrame
 Row │ X
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> df2 = DataFrame(Y=["a", "b"])
2×1 DataFrame
 Row │ Y
     │ String
─────┼────────
   1 │ a
   2 │ b

julia> crossjoin(df1, df2)
6×2 DataFrame
 Row │ X      Y
     │ Int64  String
─────┼───────────────
   1 │     1  a
   2 │     1  b
   3 │     2  a
   4 │     2  b
   5 │     3  a
   6 │     3  b
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

# an explicit error is thrown as join was supported in the past
Base.join(df1::AbstractDataFrame, df2::AbstractDataFrame, dfs::AbstractDataFrame...;
          on::Union{<:OnType, AbstractVector} = Symbol[],
          kind::Symbol = :inner, makeunique::Bool=false,
          indicator::Union{Nothing, Symbol} = nothing,
          validate::Union{Pair{Bool, Bool}, Tuple{Bool, Bool}}=(false, false)) =
    throw(ArgumentError("join function for data frames is not supported. Use innerjoin, " *
                        "leftjoin, rightjoin, outerjoin, semijoin, antijoin, or crossjoin"))
