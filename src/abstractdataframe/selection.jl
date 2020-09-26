# TODO:
# * add combine(fun, df) for DataFrame with 0 rows

# normalize_selection function makes sure that whatever input format of idx is it
# will end up in one of four canonical forms
# 1) AbstractVector{Int}
# 2) Pair{Int, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 3) Pair{AbstractVector{Int}, <:Pair{<:Base.Callable, <:Union{Symbol, AbstractVector{Symbol}, Type{AsTable}}}}
# 4) Pair{AsTable, <:Pair{<:Base.Callable, <:Union{Symbol, Vector{Symbol}, Type{AsTable}}}}
# 5) Function

"""
    ByRow

A type used for selection operations to signal that the wrapped function should
be applied to each element (row) of the selection.

Note that `ByRow` always collects values returned by `fun` in a vector.
"""
struct ByRow{T} <: Function
    fun::T
end

(f::ByRow)(cols::AbstractVector...) = f.fun.(cols...)
(f::ByRow)(table::NamedTuple) = f.fun.(Tables.namedtupleiterator(table))

# add a method to funname defined in other/utils.jl
funname(row::ByRow) = funname(row.fun)

normalize_selection(idx::AbstractIndex, sel, renamecols::Bool) =
    try
        idx[sel]
    catch e
        if e isa MethodError && e.f === getindex && e.args === (idx, sel)
            throw(ArgumentError("Unrecognized column selector: $sel"))
        else
            rethrow(e)
        end
    end

normalize_selection(idx::AbstractIndex, sel::Function, renamecols::Bool) = sel
normalize_selection(idx::AbstractIndex, sel::Colon, renamecols::Bool) = idx[:]

normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), Symbol},
                    renamecols::Bool) =
    length(idx) == 0 ? (Int[] => (() -> 0) => last(sel)) : (1 => length => last(sel))
normalize_selection(idx::AbstractIndex, sel::Pair{typeof(nrow), <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols)
normalize_selection(idx::AbstractIndex, sel::typeof(nrow), renamecols::Bool) =
    normalize_selection(idx, nrow => :nrow, renamecols)

function normalize_selection(idx::AbstractIndex, sel::ColumnIndex, renamecols::Bool)
    c = idx[sel]
    return c => identity => _names(idx)[c]
end

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, Symbol},
                             renamecols::Bool)
    c = idx[first(sel)]
    return c => identity => last(sel)
end

normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, <:AbstractString},
                    renamecols::Bool) =
    normalize_selection(idx, first(sel) => Symbol(last(sel)), renamecols::Bool)

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any,<:Pair{<:Base.Callable,
                             <:Union{Symbol, AbstractString, DataType,
                                     AbstractVector{Symbol}, AbstractVector{<:AbstractString}}}},
                             renamecols::Bool)
    lls = last(last(sel))
    if lls isa DataType
        lls === AsTable || throw(ArgumentError("Only DataType supported as target is AsTable"))
    end
    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end
    if length(c) == 0 && first(last(sel)) isa ByRow
        throw(ArgumentError("at least one column must be passed to a " *
                            "`ByRow` transformation function"))
    end
    if lls isa AbstractString
        r = Symbol(lls)
    elseif lls isa AbstractVector{<:AbstractString}
        r = Symbol.(lls)
    else
        r = lls
    end
    if r isa AbstractVector{Symbol}
        allunique(r) || throw(ArgumentError("target column names must be unique"))
    end
    return (wanttable ? AsTable(c) : c) => first(last(sel)) => r
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:ColumnIndex,<:Base.Callable}, renamecols::Bool)
    c = idx[first(sel)]
    fun = last(sel)
    if renamecols
        newcol = Symbol(_names(idx)[c], "_", funname(fun))
    else
        newcol = _names(idx)[c]
    end
    return c => fun => newcol
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any, <:Base.Callable}, renamecols::Bool)
    if first(sel) isa AsTable
        rawc = first(sel).cols
        wanttable = true
    else
        rawc = first(sel)
        wanttable = false
    end
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa Union{AbstractVector{Symbol}, AbstractVector{<:AbstractString}}
        c = [idx[n] for n in rawc]
    else
        c = try
                idx[rawc]
            catch e
                if e isa MethodError && e.f === getindex && e.args === (idx, rawc)
                    throw(ArgumentError("Unrecognized column selector: $rawc"))
                else
                    rethrow(e)
                end
            end
    end
    if length(c) == 0 && last(sel) isa ByRow
        throw(ArgumentError("at least one column must be passed to a " *
                            "`ByRow` transformation function"))
    end
    fun = last(sel)
    if length(c) > 3
        prefix = join(@views(_names(idx)[c[1:2]]), '_')
        if renamecols
            newcol = Symbol(prefix, "_etc_", funname(fun))
        else
            newcol = Symbol(prefix, "_etc")
        end
    elseif isempty(c)
        renamecols || throw(ArgumentError("when renamecols=false target column name " *
                                          "must be passed if there are no input columns"))
        newcol = Symbol(funname(fun))
    else
        prefix = join(view(_names(idx), c), '_')
        if renamecols
            newcol = Symbol(prefix, '_', funname(fun))
        else
            newcol = Symbol(prefix)
        end
    end
    return (wanttable ? AsTable(c) : c) => fun => newcol
end

function select_transform!(nc::Union{Function, Pair{<:Union{Int, AbstractVector{Int}, AsTable},
                                                    <:Pair{<:Base.Callable,
                                                           <:Union{Symbol, AbstractVector{Symbol}, DataType}}}},
                           df::AbstractDataFrame, newdf::DataFrame,
                           transformed_cols::Set{Symbol}, copycols::Bool,
                           allow_resizing_newdf::Ref{Bool})
    if nc isa Function
        col_idx, fun, newname = nothing, nc, AsTable
    else
        col_idx, (fun, newname) = nc
    end
    if newname isa DataType
        newname === AsTable || throw(ArgumentError("Only DataType supported as target is AsTable"))
    end
    # It is allowed to request a tranformation operation into a newname column
    # only once. This is ensured by the logic related to transformed_cols dictionaly
    # in _manipulate, therefore in select_transform! such a duplicate should not happen
    cdf = eachcol(df)
    if col_idx === nothing
        res = fun(df)
    elseif col_idx isa Int
        res = fun(df[!, col_idx])
    elseif col_idx isa AsTable
        res = fun(Tables.columntable(select(df, col_idx.cols, copycols=false)))
    else
        # it should be fast enough here as we do not expect to do it millions of times
        @assert col_idx isa AbstractVector{Int}
        res = fun(map(c -> cdf[c], col_idx)...)
    end

    if (newname === AsTable || newname isa AbstractVector{Symbol}) &&
        !(res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix})
        if res isa AbstractVector && !isempty(res)
            p = pairs.(res)
            ex = extrema(length, p)
            ex[1] == ex[2] || throw(ArgumentError("returned elements must have the same length"))
            kp1 = keys(p[1])
            all(x -> keys(x) == kp1, p) || throw(ArgumentError("keys of the returned elements must be identical"))
            res = DataFrame()
            for (i, n) in enumerate(kp1)
                res[!, Symbol(n)] = [x[i] for x in p]
            end
        else
            res = Tables.columntable(res)
        end
    end

    if res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}
        if newname isa Symbol
            throw(ArgumentError("Table returned while a single column return value was requested"))
        end
        if res isa AbstractMatrix
            colnames = gennames(size(res, 2))
        else
            colnames = propertynames(res)
        end
        if !(newname === AsTable)
            if length(colnames) != length(newname)
                throw(ArgumentError("Number of returned columns does not match the " *
                                    "length of requested output"))
            end
            colnames = newname
        end
        isempty(colnames) && return # nothing to do

        if any(in(transformed_cols), colnames)
            throw(ArgumentError("Duplicate column name returned"))
        else
            startlen = length(transformed_cols)
            union!(transformed_cols, colnames)
            @assert startlen + length(colnames) == length(transformed_cols)
        end
        if res isa AbstractDataFrame
            lr = nrow(res)
            # allow shortening to 0 rows
            if allow_resizing_newdf[] && nrow(newdf) == 1
                newdfcols = _columns(newdf)
                for (i, col) in enumerate(newdfcols)
                    newdfcols[i] = fill!(similar(col, lr), first(col))
                end
            end

            # !allow_resizing_newdf[] && ncol(newdf) == 0
            # means that we use `select` or `transform` not `combine`
            if !allow_resizing_newdf[] && ncol(newdf) == 0 && lr != nrow(df)
                throw(ArgumentError("length $(lr) of vector returned from " *
                                    "function $fun is different from number of rows " *
                                    "$(nrow(df)) of the source data frame."))
            end
            allow_resizing_newdf[] = false
            @assert length(colnames) == ncol(res)
            for (newname, v) in zip(colnames, eachcol(res))
                vpar = parent(v)
                parent_cols = col_idx isa AsTable ? col_idx.cols : (col_idx === nothing ? (1:ncol(df)) : col_idx)
                if copycols && !(fun isa ByRow) &&
                    (v isa SubArray || any(i -> vpar === parent(cdf[i]), parent_cols))
                    newdf[!, newname] = copy(v)
                else
                    newdf[!, newname] = v
                end
            end
        elseif res isa AbstractMatrix
            lr = size(res, 1)
            # allow shortening to 0 rows
            if allow_resizing_newdf[] && nrow(newdf) == 1
                newdfcols = _columns(newdf)
                for (i, col) in enumerate(newdfcols)
                    newdfcols[i] = fill!(similar(col, lr), first(col))
                end
            end

            # !allow_resizing_newdf[] && ncol(newdf) == 0
            # means that we use `select` or `transform` not `combine`
            if !allow_resizing_newdf[] && ncol(newdf) == 0 && lr != nrow(df)
                throw(ArgumentError("length $(lr) of vector returned from " *
                                    "function $fun is different from number of rows " *
                                    "$(nrow(df)) of the source data frame."))
            end
            allow_resizing_newdf[] = false
            @assert length(colnames) == size(res, 2)
            for (i, newname) in enumerate(colnames)
                newdf[!, newname] = res[:, i]
            end
        elseif res isa NamedTuple
            if all(v -> v isa AbstractVector, x)
                lr = length(res[1])
                # allow shortening to 0 rows
                if allow_resizing_newdf[] && nrow(newdf) == 1
                    newdfcols = _columns(newdf)
                    for (i, col) in enumerate(newdfcols)
                        newdfcols[i] = fill!(similar(col, lr), first(col))
                    end
                end

                # !allow_resizing_newdf[] && ncol(newdf) == 0
                # means that we use `select` or `transform` not `combine`
                if !allow_resizing_newdf[] && ncol(newdf) == 0 && lr != nrow(df)
                    throw(ArgumentError("length $(lr) of vector returned from " *
                                        "function $fun is different from number of rows " *
                                        "$(nrow(df)) of the source data frame."))
                end
                allow_resizing_newdf[] = false
                @assert length(colnames) == length(res)
                for (newname, v) in zip(colnames, res)
                    vpar = parent(v)
                    parent_cols = col_idx isa AsTable ? col_idx.cols : (col_idx === nothing ? (1:ncol(df)) : col_idx)
                    if copycols && !(fun isa ByRow) &&
                        (v isa SubArray || any(i -> vpar === parent(cdf[i]), parent_cols))
                        newdf[!, newname] = copy(v)
                    else
                        newdf[!, newname] = v
                    end
                end
            elseif any(v -> v isa AbstractVector, x)
                throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
            else
                if ncol(newdf) == 0
                    # if allow_resizing_newdf[] is false we know this is select or transform
                    rows = allow_resizing_newdf[] ? 1 : nrow(df)
                else
                    # allow squashing a scalar to 0 rows
                    rows = nrow(newdf)
                end
                @assert length(colnames) == length(res)
                for (newname, v) in zip(colnames, res)
                    # note that newdf potentially can contain c in general
                    newdf[!, newname] = fill!(Tables.allocatecolumn(typeof(v), rows), v)
                end
            end
        elseif res isa DataFrameRow
            if ncol(newdf) == 0
                # if allow_resizing_newdf[] is false we know this is select or transform
                rows = allow_resizing_newdf[] ? 1 : nrow(df)
            else
                # allow squashing a scalar to 0 rows
                rows = nrow(newdf)
            end
            @assert length(colnames) == length(res)
            for (newname, v) in zip(colnames, res)
                # note that newdf potentially can contain c in general
                newdf[!, newname] = fill!(Tables.allocatecolumn(typeof(v), rows), v)
            end
        end
    elseif res isa AbstractVector
        if newname in transformed_cols
            throw(ArgumentError("duplicate name of a transformed column"))
        else
            push!(transformed_cols, newname)
        end
        # allow shortening to 0 rows
        if allow_resizing_newdf[] && nrow(newdf) == 1
            newdfcols = _columns(newdf)
            for (i, col) in enumerate(newdfcols)
                newdfcols[i] = fill!(similar(col, length(res)), first(col))
            end
        end

        # !allow_resizing_newdf[] && ncol(newdf) == 0
        # means that we use `select` or `transform` not `combine`
        if !allow_resizing_newdf[] && ncol(newdf) == 0 && length(res) != nrow(df)
            throw(ArgumentError("length $(length(res)) of vector returned from " *
                                "function $fun is different from number of rows " *
                                "$(nrow(df)) of the source data frame."))
        end
        allow_resizing_newdf[] = false
        respar = parent(res)
        parent_cols = col_idx isa AsTable ? col_idx.cols : (col_idx === nothing ? (1:ncol(df)) : col_idx)
        if copycols && !(fun isa ByRow) &&
            (res isa SubArray || any(i -> respar === parent(cdf[i]), parent_cols))
            newdf[!, newname] = copy(res)
        else
            newdf[!, newname] = res
        end
    else
        if newname in transformed_cols
            throw(ArgumentError("duplicate name of a transformed column"))
        else
            push!(transformed_cols, newname)
        end
        res_unwrap = res isa Union{AbstractArray{<:Any, 0}, Ref} ? res[] : res
        if ncol(newdf) == 0
            # if allow_resizing_newdf[] is false we know this is select or transform
            rows = allow_resizing_newdf[] ? 1 : nrow(df)
        else
            # allow squashing a scalar to 0 rows
            rows = nrow(newdf)
        end
        newdf[!, newname] = fill!(Tables.allocatecolumn(typeof(res_unwrap), rows),
                                  res_unwrap)
    end
end

SELECT_ARG_RULES =
    """
    Arguments passed as `args...` can be:

    * Any index that is allowed for column indexing
      ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
    * Column transformation operations using the `Pair` notation that is
      described below and vectors of such pairs.

    Columns can be renamed using the `old_column => new_column_name` syntax, and
    transformed using the `old_column => fun => new_column_name` syntax.
    `new_column_name` must be a `Symbol` or a string, and `fun` a function or a
    type. If `old_column` is a `Symbol`, a string, or an integer then `fun` is
    applied to the corresponding column vector. Otherwise `old_column` can be
    any column indexing syntax, in which case `fun` will be passed the column
    vectors specified by `old_column` as separate arguments. The only exception
    is when `old_column` is an `AsTable` type wrapping a selector, in which case
    `fun` is passed a `NamedTuple` containing the selected columns.

    If `fun` returns a value of type other than `AbstractVector` then it will be
    broadcasted into a vector matching the target number of rows in the data
    frame, unless its type is one of `AbstractDataFrame`, `NamedTuple`,
    `DataFrameRow`, `AbstractMatrix`, in which case an error is thrown as
    currently these return types are not allowed. As a particular rule, values
    wrapped in a `Ref` or a `0`-dimensional `AbstractArray` are unwrapped and
    then broadcasted.

    To apply `fun` to each row instead of whole columns, it can be wrapped in a
    `ByRow` struct. In this case if `old_column` is a `Symbol`, a string, or an
    integer then `fun` is applied to each element (row) of `old_column` using
    broadcasting. Otherwise `old_column` can be any column indexing syntax, in
    which case `fun` will be passed one argument for each of the columns
    specified by `old_column`. If `ByRow` is used it is not allowed for
    `old_column` to select an empty set of columns nor for `fun` to return a
    `NamedTuple` or a `DataFrameRow`.

    Column transformation can also be specified using the short `old_column =>
    fun` form. In this case, `new_column_name` is automatically generated as
    `\$(old_column)_\$(fun)` if `renamecols=true` and `\$(old_column)` if
    `renamecols=false`. Up to three column names are used for multiple input
    columns and they are joined using `_`; if more than three columns are passed
    then the name consists of the first two names and `etc` suffix then, e.g.
    `[:a,:b,:c,:d] => fun` produces the new column name `:a_b_etc_fun` if
    `renamecols=true` and ``:a_b_etc` if `renamecols=false`.
    It is not allowed to pass `renamecols=false` if `old_column` is empty
    as it would generate an empty column name.

    Column renaming and transformation operations can be passed wrapped in
    vectors (this is useful when combined with broadcasting).

    As a special rule passing `nrow` without specifying `old_column` creates a
    column named `:nrow` containing a number of rows in a source data frame, and
    passing `nrow => new_column_name` stores the number of rows in source data
    frame in `new_column_name` column.

    If a collection of column names is passed to `select!` or `select` then
    requesting duplicate column names in target data frame are accepted (e.g.
    `select!(df, [:a], :, r"a")` is allowed) and only the first occurrence is
    used. In particular a syntax to move column `:col` to the first position in
    the data frame is `select!(df, :col, :)`. On the contrary, output column
    names of renaming, transformation and single column selection operations
    must be unique, so e.g. `select!(df, :a, :a => :a)` or
    `select!(df, :a, :a => ByRow(sin) => :a)` are not allowed.
    """

"""
    select!(df::DataFrame, args...; renamecols::Bool=true)

Mutate `df` in place to retain only columns specified by `args...` and return it.
The result is guaranteed to have the same number of rows as `df`, except when no
columns are selected (in which case the result has zero rows).

$SELECT_ARG_RULES

Note that including the same column several times in the data frame via renaming
or transformations that return the same object without copying will create
column aliases. An example of such a situation is
`select!(df, :a, :a => :b, :a => identity => :c)`.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select!(df, 2)
3×1 DataFrame
│ Row │ b     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 4     │
│ 2   │ 5     │
│ 3   │ 6     │

julia> df = DataFrame(a=1:3, b=4:6);

julia> select!(df, :a => ByRow(sin) => :c, :b)
3×2 DataFrame
│ Row │ c        │ b     │
│     │ Float64  │ Int64 │
├─────┼──────────┼───────┤
│ 1   │ 0.841471 │ 4     │
│ 2   │ 0.909297 │ 5     │
│ 3   │ 0.14112  │ 6     │

julia> select!(df, :, [:c, :b] => (c,b) -> c .+ b .- sum(b)/length(b))
3×3 DataFrame
│ Row │ c        │ b     │ c_b_function │
│     │ Float64  │ Int64 │ Float64      │
├─────┼──────────┼───────┼──────────────┤
│ 1   │ 0.841471 │ 4     │ -0.158529    │
│ 2   │ 0.909297 │ 5     │ 0.909297     │
│ 3   │ 0.14112  │ 6     │ 1.14112      │

julia> df = DataFrame(a=1:3, b=4:6);

julia> select!(df, names(df) .=> sum);

julia> df
3×2 DataFrame
│ Row │ a_sum │ b_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 15    │
│ 2   │ 6     │ 15    │
│ 3   │ 6     │ 15    │

julia> df = DataFrame(a=1:3, b=4:6);

julia> using Statistics

julia> select!(df, AsTable(:) => ByRow(mean), renamecols=false)
3×1 DataFrame
│ Row │ a_b     │
│     │ Float64 │
├─────┼─────────┤
│ 1   │ 2.5     │
│ 2   │ 3.5     │
│ 3   │ 4.5     │
```

"""
select!(df::DataFrame, args...; renamecols::Bool=true) =
    _replace_columns!(df, select(df, args..., copycols=false, renamecols=renamecols))

function select!(arg::Function, df::AbstractDataFrame; renamecols::Bool=true)
    if arg isa Colon
        throw(ArgumentError("Only transformations are allowed when function is a " *
                            "frist argument to select!"))
    end
    return select!(df, arg)
end

"""
    transform!(df::DataFrame, args...; renamecols::Bool=true)

Mutate `df` in place to add columns specified by `args...` and return it.
The result is guaranteed to have the same number of rows as `df`.
Equivalent to `select!(df, :, args...)`.

See [`select!`](@ref) for detailed rules regarding accepted values for `args`.
"""
transform!(df::DataFrame, args...; renamecols::Bool=true) =
    select!(df, :, args..., renamecols=renamecols)

function transform!(arg::Function, df::AbstractDataFrame; renamecols::Bool=true)
    if arg isa Colon
        throw(ArgumentError("Only transformations are allowed when function is a " *
                            "frist argument to transform!"))
    end
    return transform!(df, arg)
end

"""
    select(df::AbstractDataFrame, args...; copycols::Bool=true, renamecols::Bool=true)

Create a new data frame that contains columns from `df` specified by `args` and
return it. The result is guaranteed to have the same number of rows as `df`,
except when no columns are selected (in which case the result has zero rows)..

If `df` is a `DataFrame` or `copycols=true` then column renaming and transformations
are supported.

$SELECT_ARG_RULES

If `df` is a `DataFrame` a new `DataFrame` is returned.
If `copycols=false`, then the returned `DataFrame` shares column vectors with `df`
where possible.
If `copycols=true` (the default), then the returned `DataFrame` will not share
columns with `df`.
The only exception for this rule is the `old_column => fun => new_column`
transformation when `fun` returns a vector that is not allocated by `fun` but is
neither a `SubArray` nor one of the input vectors.
In such a case a new `DataFrame` might contain aliases. Such a situation can
only happen with transformations which returns vectors other than their inputs,
e.g. with `select(df, :a => (x -> c) => :c1, :b => (x -> c) => :c2)`  when `c`
is a vector object or with `select(df, :a => (x -> df.c) => :c2)`.

If `df` is a `SubDataFrame` and `copycols=true` then a `DataFrame` is returned
and the same copying rules apply as for a `DataFrame` input:
this means in particular that selected columns will be copied.
If `copycols=false`, a `SubDataFrame` is returned without copying columns.

Note that including the same column several times in the data frame via renaming
or transformations that return the same object when `copycols=false` will create
column aliases. An example of such a situation is
`select(df, :a, :a => :b, :a => identity => :c, copycols=false)`.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select(df, :b)
3×1 DataFrame
│ Row │ b     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 4     │
│ 2   │ 5     │
│ 3   │ 6     │

julia> select(df, Not(:b)) # drop column :b from df
3×1 DataFrame
│ Row │ a     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> select(df, :a => :c, :b)
3×2 DataFrame
│ Row │ c     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select(df, :a => ByRow(sin) => :c, :b)
3×2 DataFrame
│ Row │ c        │ b     │
│     │ Float64  │ Int64 │
├─────┼──────────┼───────┤
│ 1   │ 0.841471 │ 4     │
│ 2   │ 0.909297 │ 5     │
│ 3   │ 0.14112  │ 6     │

julia> select(df, :, [:a, :b] => (a,b) -> a .+ b .- sum(b)/length(b))
3×3 DataFrame
│ Row │ a     │ b     │ a_b_function │
│     │ Int64 │ Int64 │ Float64      │
├─────┼───────┼───────┼──────────────┤
│ 1   │ 1     │ 4     │ 0.0          │
│ 2   │ 2     │ 5     │ 2.0          │
│ 3   │ 3     │ 6     │ 4.0          │

julia> select(df, names(df) .=> sum)
3×2 DataFrame
│ Row │ a_sum │ b_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 15    │
│ 2   │ 6     │ 15    │
│ 3   │ 6     │ 15    │

julia> select(df, names(df) .=> sum .=> [:A, :B])
3×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 15    │
│ 2   │ 6     │ 15    │
│ 3   │ 6     │ 15    │

julia> select(df, AsTable(:) => ByRow(mean), renamecols=false)
3×1 DataFrame
│ Row │ a_b     │
│     │ Float64 │
├─────┼─────────┤
│ 1   │ 2.5     │
│ 2   │ 3.5     │
│ 3   │ 4.5     │
```

"""
select(df::AbstractDataFrame, args...; copycols::Bool=true, renamecols::Bool=true) =
    manipulate(df, args..., copycols=copycols, keeprows=true, renamecols=renamecols)

function select(arg::Function, df::AbstractDataFrame; renamecols::Bool=true)
    if arg isa Colon
        throw(ArgumentError("Only transformations are allowed when function is a " *
                            "frist argument to select"))
    end
    return select(df, arg)
end

"""
    transform(df::AbstractDataFrame, args...; copycols::Bool=true, renamecols::Bool=true)

Create a new data frame that contains columns from `df` and adds columns
specified by `args` and return it.
The result is guaranteed to have the same number of rows as `df`.
Equivalent to `select(df, :, args..., copycols=copycols)`.

See [`select`](@ref) for detailed rules regarding accepted values for `args`.
"""
transform(df::AbstractDataFrame, args...; copycols::Bool=true, renamecols::Bool=true) =
    select(df, :, args..., copycols=copycols, renamecols=renamecols)

function transform(arg::Function, df::AbstractDataFrame; renamecols::Bool=true)
    if arg isa Colon
        throw(ArgumentError("Only transformations are allowed when function is a " *
                            "frist argument to transform"))
    end
    return transform(df, arg)
end

"""
    combine(df::AbstractDataFrame, args...; renamecols::Bool=true)
    combine(arg, df::AbstractDataFrame; renamecols::Bool=true)

Create a new data frame that contains columns from `df` specified by `args` and
return it. The result can have any number of rows that is determined by the
values returned by passed transformations.

See [`select`](@ref) for detailed rules regarding accepted values for `args` in
`combine(df, args...)` form. For `combine(arg, df)` the same rules as for
`combine` on `GroupedDataFrame` apply except that a `df` with zero rows is
currently not allowed.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> combine(df, :a => sum, nrow, renamecols=false)
1×2 DataFrame
│ Row │ a     │ nrow  │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 3     │
```
"""
combine(df::AbstractDataFrame, args...; renamecols::Bool=true) =
    manipulate(df, args..., copycols=true, keeprows=false, renamecols=renamecols)

function combine(arg::Function, df::AbstractDataFrame; renamecols::Bool=true)
    if arg isa Colon
        throw(ArgumentError("Only transformations are allowed when function is a " *
                            "frist argument to combine"))
    end
    return combine(df, arg)
end

manipulate(df::DataFrame, args::AbstractVector{Int}; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    DataFrame(_columns(df)[args], Index(_names(df)[args]), copycols=copycols)

function manipulate(df::DataFrame, c::MultiColumnIndex; copycols::Bool, keeprows::Bool,
                    renamecols::Bool)
    if c isa AbstractVector{<:Pair}
        return manipulate(df, c..., copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    else
        return manipulate(df, index(df)[c], copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    end
end

manipulate(df::DataFrame, c::ColumnIndex; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    manipulate(df, [c], copycols=copycols, keeprows=keeprows, renamecols=renamecols)

function manipulate(df::DataFrame, cs...; copycols::Bool, keeprows::Bool, renamecols::Bool)
    cs_vec = []
    for v in cs
        if v isa AbstractVector{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    return _manipulate(df, [normalize_selection(index(df), c, renamecols) for c in cs_vec],
                    copycols, keeprows)
end

function _manipulate(df::AbstractDataFrame, normalized_cs, copycols::Bool, keeprows::Bool)
    @assert !(df isa SubDataFrame && copycols==false)
    newdf = DataFrame()
    # the role of transformed_cols is the following
    # * make sure that we do not use the same target column name twice in transformations;
    #   note though that it can appear in no-transformation selection like
    #   `select(df, :, :a => ByRow(sin) => :a), where :a is produced both by `:`
    #   and by `:a => ByRow(sin) => :a`
    # * make sure that if some column is produced by transformation like
    #   `:a => ByRow(sin) => :a` and it appears earlier or later in non-transforming
    #   selection like `:` or `:a` then the transformation is computed and inserted
    #   in to the target data frame once and only once the first time the target column
    #   is requested to be produced.
    #
    # For example in:
    #
    # julia> df = DataFrame(a=1:2, b=3:4)
    # 2×2 DataFrame
    # │ Row │ a     │ b     │
    # │     │ Int64 │ Int64 │
    # ├─────┼───────┼───────┤
    # │ 1   │ 1     │ 3     │
    # │ 2   │ 2     │ 4     │
    #
    # julia> select(df, :, :a => ByRow(sin) => :a, :a, 1)
    # 2×2 DataFrame
    # │ Row │ a        │ b     │
    # │     │ Float64  │ Int64 │
    # ├─────┼──────────┼───────┤
    # │ 1   │ 0.841471 │ 3     │
    # │ 2   │ 0.909297 │ 4     │
    #
    # transformed_cols keeps a set of columns that were generated via a transformation
    # up till the point. Note that single column selection and column renaming is
    # considered to be a transformation
    transformed_cols = Set{Symbol}()
    # we allow resizing newdf only if up to some point only scalars were put
    # in it. The moment we put any vector into newdf its number of rows becomes fixed
    # Also if keeprows is true then we make sure to produce nrow(df) rows so resizing
    # is not allowed
    allow_resizing_newdf = Ref(!keeprows)
    for nc in normalized_cs
        if nc isa AbstractVector{Int} # only this case is NOT considered to be a transformation
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                # as nc is a multiple column selection without transformations
                # we allow duplicate column names with selections applied earlier
                # and ignore them for convinience, to allow for e.g. select(df, :x1, :)
                if !hasproperty(newdf, newname)
                    # allow shortening to 0 rows
                    if allow_resizing_newdf[] && nrow(newdf) == 1
                        newdfcols = _columns(newdf)
                        for (i, col) in enumerate(newdfcols)
                            newdfcols[i] = fill!(similar(col, nrow(df)), first(col))
                        end
                    end
                    # here even if keeprows is true all is OK
                    newdf[!, newname] = copycols ? df[:, i] : df[!, i]
                    allow_resizing_newdf[] = false
                end
            end
        else
            select_transform!(nc, df, newdf, transformed_cols, copycols,
                              allow_resizing_newdf)
        end
    end
    return newdf
end

manipulate(dfv::SubDataFrame, ind::ColumnIndex; copycols::Bool, keeprows::Bool,
           renamecols::Bool) =
    manipulate(dfv, [ind], copycols=copycols, keeprows=keeprows, renamecols=renamecols)

function manipulate(dfv::SubDataFrame, args::MultiColumnIndex;
                 copycols::Bool, keeprows::Bool, renamecols::Bool)
    if args isa AbstractVector{<:Pair}
        return manipulate(dfv, args..., copycols=copycols, keeprows=keeprows,
                          renamecols=renamecols)
    else
        return copycols ? dfv[:, args] : view(dfv, :, args)
    end
end

function manipulate(dfv::SubDataFrame, args...; copycols::Bool, keeprows::Bool,
                    renamecols::Bool)
    if copycols
        cs_vec = []
        for v in args
            if v isa AbstractVector{<:Pair}
                append!(cs_vec, v)
            else
                push!(cs_vec, v)
            end
        end
        return _manipulate(dfv, [normalize_selection(index(dfv), c, renamecols) for c in cs_vec],
                           true, keeprows)
    else
        # we do not support transformations here
        # newinds contains only indexing; making it Vector{Any} avoids some compilation
        newinds = []
        seen_single_column = Set{Int}()
        for ind in args
            if ind isa ColumnIndex
                ind_idx = index(dfv)[ind]
                if ind_idx in seen_single_column
                    throw(ArgumentError("selecting the same column multiple times " *
                                        "using Symbol, string or integer is not allowed " *
                                        "($ind was passed more than once"))
                else
                    push!(seen_single_column, ind_idx)
                end
            else
                newind = normalize_selection(index(dfv), ind, renamecols)
                if newind isa Pair
                    throw(ArgumentError("transforming and renaming columns of a " *
                                        "SubDataFrame is not allowed when `copycols=false`"))
                end
                push!(newinds, newind)
            end
        end
        return view(dfv, :, isempty(newinds) ? [] : All(newinds...))
    end
end
