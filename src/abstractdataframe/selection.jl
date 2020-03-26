# TODO:
# * add NT (or better name) to column selector passing NamedTuple
#   (also in other places: filter, combine)
# * add select/select!/transform/transform! for GroupedDataFrame

# normalize_selection function makes sure that whatever input format of idx is it
# will end up in one of four canonical forms
# 1) Int
# 2) AbstractVector{Int}
# 4) Pair{Int, <:Pair{<:Base.Callable, Symbol}}
# 5) Pair{AbstractVector{Int}, <:Pair{<:Base.Callable, Symbol}}
# 6) Pair{Int, Pair{ByRow, Symbol}}
# 7) Pair{AbstractVector{Int}, Pair{ByRow, Symbol}}

"""
    ByRow

A type used for selection operations to signal that the wrapped function should
be applied to each element (row) of the selection.
"""
struct ByRow{T}
    fun::T
end

Base.broadcastable(x::ByRow) = Ref(x)

(f::ByRow)(cols::AbstractVector...) = f.fun.(cols...)

# add a method to funname defined in other/utils.jl
funname(row::ByRow) = funname(row.fun)

normalize_selection(idx::AbstractIndex, sel) =
    try
        idx[sel]
    catch e
        if e isa MethodError && e.f === getindex && e.args === (idx, sel)
            throw(ArgumentError("Unrecognized column selector: $sel"))
        else
            rethrow(e)
        end
    end

function normalize_selection(idx::AbstractIndex, sel::ColumnIndex)
    c = idx[sel]
    return c => identity => _names(idx)[c]
end

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, Symbol})
    c = idx[first(sel)]
    return c => identity => last(sel)
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any,<:Pair{<:Union{Base.Callable, ByRow}, Symbol}})
    rawc = first(sel)
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa AbstractVector{Symbol}
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
    return c => last(sel)
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:ColumnIndex,<:Union{Base.Callable, ByRow}})
    c = idx[first(sel)]
    fun = last(sel)
    newcol = Symbol(_names(idx)[c], "_", funname(fun))
    return c => fun => newcol
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any, <:Union{Base.Callable,ByRow}})
    rawc = first(sel)
    if rawc isa AbstractVector{Int}
        c = rawc
    elseif rawc isa AbstractVector{Symbol}
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
        newcol = Symbol(join(@views(_names(idx)[c[1:2]]), '_'), "_etc_", funname(fun))
    elseif isempty(c)
        newcol = Symbol(funname(fun))
    else
        newcol = Symbol(join(view(_names(idx), c), '_'), '_', funname(fun))
    end
    return c => fun => newcol
end

function select_transform!(nc::Pair{<:Union{Int, AbstractVector{Int}},
                                    <:Pair{<:Union{Base.Callable, ByRow}, Symbol}},
                           df::AbstractDataFrame, newdf::DataFrame,
                           transformed_cols::Dict{Symbol, Any}, copycols::Bool)
    col_idx, (fun, newname) = nc
    # it is allowed to request column tranformation only once
    @assert !hasproperty(newdf, newname)
    cdf = eachcol(df)
    if col_idx isa Int
        res = fun(df[!, col_idx])
    else
        res = fun((cdf[i] for i in col_idx)...)
    end
    if res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}
        throw(ArgumentError("return value from function $fun " *
                            "of type $(typeof(res)) is currently not allowed."))
    end
    if res isa AbstractVector
        respar = parent(res)
        if copycols && !(fun isa ByRow) &&
            (res isa SubArray || any(i -> respar === parent(cdf[i]), col_idx))
            newdf[!, newname] = copy(res)
        else
            newdf[!, newname] = res
        end
    else
        newdf[!, newname] = [res]
    end
    # mark that column transformation was applied
    # nothing is not possible otherwise as a value in this dict
    transformed_cols[newname] = nothing
end

"""
    select!(df::DataFrame, inds...)

Mutate `df` in place to retain only columns specified by `inds...` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

Columns can be renamed using the `old_column => new_column_name` syntax,
and transformed using the `old_column => fun => new_column_name` syntax.
`new_column_name` must be a `Symbol`, and `fun` a function or a type. If `old_column`
is a `Symbol` or an integer then `fun` is applied to the corresponding column vector.
Otherwise `old_column` can be any column indexing syntax, in which case `fun`
will be passed the column vectors specified by `old_column` as separate arguments.
If `fun` returns a value of type other than `AbstractVector` then it will be wrapped
into a 1-element vector, unless its type is one of `AbstractDataFrame`, `NamedTuple`,
`DataFrameRow`, `AbstractMatrix`, in which case an error is thrown as currently these
return types are not allowed.

To apply `fun` to each row instead of whole columns, it can be wrapped in a `ByRow`
struct. In this case if `old_column` is a `Symbol` or an integer then `fun` is applied
to each element (row) of `old_column` using broadcasting. Otherwise `old_column` can be
any column indexing syntax, in which case `fun` will be passed one argument for each of
the columns specified by `old_column`. If `ByRow` is used it is not allowed for
`old_column` to select an empty set of columns.

Column transformation can also be specified using the short `old_column => fun` form.
In this case, `new_column_name` is automatically generated as `\$(old_column)_\$(fun)`.
Up to three column names are used for multiple input columns and they are joined
using `_`; if more than three columns are passed then the name consists of the
first two names and `etc` suffix then, e.g. `[:a,:b,:c,:d] => fun` produces
the new column name `:a_b_etc_fun`.

Column renaming and transformation operations can be passed wrapped in vectors
(this is useful when combined with broadcasting).

If a collection of column names is passed to `select!` then requesting duplicate column
names in target data frame are accepted (e.g. `select!(df, [:a], :, r"a")` is allowed)
and only the first occurrence is used. In particular a syntax to move column `:col`
to the first position in the data frame is `select!(df, :col, :)`.
On the contrary, output column names of renaming, transformation and single column
selection operations must be unique, so e.g. `select!(df, :a, :a => :a)` or
`select!(df, :a, :a => ByRow(sin) => :a)` are not allowed.

Note that including the same column several times in the data frame via renaming
or transformations that return the same object without copying will create column aliases.
An example of such a situation is `select!(df, :a, :a => :b, :a => identity => :c)`.

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

julia> select!(df, names(df) .=> sum .=> [:A, :B]);

julia> df
1×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 15    │
```

"""
function select!(df::DataFrame, inds::AbstractVector{Int})
    if isempty(inds)
        empty!(_columns(df))
        empty!(index(df))
        return df
    end
    indmin, indmax = extrema(inds)
    if indmin < 1
        throw(ArgumentError("indices must be positive"))
    end
    if indmax > ncol(df)
        throw(ArgumentError("indices must not be greater than number of columns"))
    end
    if !allunique(inds)
        throw(ArgumentError("indices must not contain duplicates"))
    end
    copy!(_columns(df), _columns(df)[inds])
    x = index(df)
    copy!(_names(x), _names(df)[inds])
    empty!(x.lookup)
    for (i, n) in enumerate(x.names)
        x.lookup[n] = i
    end
    return df
end

select!(df::DataFrame, c::Int) = select!(df, [c])
select!(df::DataFrame, c::Union{AbstractVector{<:Integer}, AbstractVector{Symbol},
                                Colon, All, Not, Between, Regex}) =
    select!(df, index(df)[c])

function select!(df::DataFrame, cs...)
    newdf = select(df, cs..., copycols=false)
    copy!(_columns(df), _columns(newdf))
    x = index(df)
    copy!(_names(x), _names(newdf))
    empty!(x.lookup)
    for (i, n) in enumerate(x.names)
        x.lookup[n] = i
    end
    return df
end

"""
    transform!(df::DataFrame, inds...)

Mutate `df` in place to add columns specified by `inds...` and return it.
Equivalent to `select!(df, :, inds...)`.

See [`select!`](@ref) for detailed rules regarding accepted values for `inds`.
"""
transform!(df::DataFrame, inds...) = select!(df, :, inds...)

"""
    select(df::AbstractDataFrame, inds...; copycols::Bool=true)

Create a new data frame that contains columns from `df` specified by `inds` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

Also if `df` is a `DataFrame` or `copycols=true` then column renaming and transformations
are supported.

Columns can be renamed using the `old_column => new_column_name` syntax,
and transformed using the `old_column => fun => new_column_name` syntax.
`new_column_name` must be a `Symbol`, and `fun` a function or a type. If `old_column`
is a `Symbol` or an integer then `fun` is applied to the corresponding column vector.
Otherwise `old_column` can be any column indexing syntax, in which case `fun`
will be passed the column vectors specified by `old_column` as separate arguments.
If `fun` returns a value of type other than `AbstractVector` then it will be wrapped
into a 1-element vector, unless its type is one of `AbstractDataFrame`, `NamedTuple`,
`DataFrameRow`, `AbstractMatrix`, in which case an error is thrown as currently these
return types are not allowed.

To apply `fun` to each row instead of whole columns, it can be wrapped in a `ByRow`
struct. In this case if `old_column` is a `Symbol` or an integer then `fun` is applied
to each element (row) of `old_column` using broadcasting. Otherwise `old_column` can be
any column indexing syntax, in which case `fun` will be passed one argument for each of
the columns specified by `old_column`. If `ByRow` is used it is not allowed for
`old_column` to select an empty set of columns.

Column transformation can also be specified using the short `old_column => fun` form.
In this case, `new_column_name` is automatically generated as `\$(old_column)_\$(fun)`.
Up to three column names are used for multiple input columns and they are joined
using `_`; if more than three columns are passed then the name consists of the
first two names and `etc` suffix then, e.g. `[:a,:b,:c,:d] => fun` produces
the new column name `:a_b_etc_fun`.

Column renaming and transformation operations can be passed wrapped in vectors
(this is useful when combined with broadcasting).

If a collection of column names is passed to `select!` then requesting duplicate column
names in target data frame are accepted (e.g. `select!(df, [:a], :, r"a")` is allowed)
and only the first occurrence is used. In particular a syntax to move column `:col`
to the first position in the data frame is `select!(df, :col, :)`.
On the contrary, output column names of renaming, transformation and single column
selection operations must be unique, so e.g. `select!(df, :a, :a => :a)` or
`select!(df, :a, :a => ByRow(sin) => :a)` are not allowed.

If `df` is a `DataFrame` a new `DataFrame` is returned.
If `copycols=false`, then the returned `DataFrame` shares column vectors with `df` where possible.
If `copycols=true` (the default), then the returned `DataFrame` will not share columns with `df`.
The only exception for this rule is the `old_column => fun => new_column` transformation
when `fun` returns a vector that is not allocated by `fun` but is neither a `SubArray` nor one
of the input vectors.
In such a case a new `DataFrame` might contain aliases. Such a situation can only happen
with transformations which returns vectors other than their inputs, e.g. with
`select(df, :a => (x -> c) => :c1, :b => (x -> c) => :c2)`  when `c` is a vector object
or with `select(df, :a => (x -> df.c) => :c2)`.

If `df` is a `SubDataFrame` and `copycols=true` then a `DataFrame` is returned and
the same copying rules apply as for a `DataFrame` input:
this means in particular that selected columns will be copied.
If `copycols=false`, a `SubDataFrame` is returned without copying columns.

Note that including the same column several times in the data frame via renaming or
transformations that return the same object when `copycols=false` will create column
aliases. An example of such a situation is
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

julia> select(df, names(df) .=> sum .=> [:A, :B])
1×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 6     │ 15    │
```

"""
select(df::DataFrame, inds::AbstractVector{Int}; copycols::Bool=true) =
    DataFrame(_columns(df)[inds], Index(_names(df)[inds]),
              copycols=copycols)
select(df::DataFrame, c::Union{AbstractVector{<:Integer}, AbstractVector{Symbol},
                               Colon, All, Not, Between, Regex}; copycols::Bool=true) =
    select(df, index(df)[c], copycols=copycols)
select(df::DataFrame, c::ColumnIndex; copycols::Bool=true) =
    select(df, [c], copycols=copycols)

function select(df::DataFrame, cs...; copycols::Bool=true)
    cs_vec = []
    for v in cs
        if v isa AbstractVector{<:Pair}
            append!(cs_vec, v)
        else
            push!(cs_vec, v)
        end
    end
    _select(df, [normalize_selection(index(df), c) for c in cs_vec], copycols)
end

function _select(df::AbstractDataFrame, normalized_cs, copycols::Bool)
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
    # we compute column :a immediately when we process `:` although it is specified
    # later by `:a=>sin=>:a` because we know from `transformed_cols` variable that
    # it will be computed later via a transformation
    transformed_cols = Dict{Symbol, Any}()
    for nc in normalized_cs
        if nc isa Pair
            newname = last(last(nc))
            @assert newname isa Symbol
            if haskey(transformed_cols, newname)
                throw(ArgumentError("duplicate target column name $newname passed"))
            end
            transformed_cols[newname] = nc
        end
    end
    for nc in normalized_cs
        if nc isa AbstractVector{Int}
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                # as nc is a multiple column selection without transformations
                # we allow duplicate column names with selections applied earlier
                # and ignore them for convinience, to allow for e.g. select(df, :x1, :)
                if !hasproperty(newdf, newname)
                    if haskey(transformed_cols, newname)
                        # if newdf does not have a column newname
                        # but a column transformation was requested for this column
                        # then apply the transformation immediately
                        # in such a case nct may not be nothing, as if it were
                        # nothing then newname should be preasent in newdf already
                        nct = transformed_cols[newname]
                        @assert nct !== nothing
                        select_transform!(nct, df, newdf, transformed_cols, copycols)
                    else
                        newdf[!, newname] = copycols ? df[:, i] : df[!, i]
                    end
                end
            end
        else
            # nc is normalized so it has a form src_cols => fun => Symbol
            newname = last(last(nc))
            if hasproperty(newdf, newname)
                # it is possible that the transformation has already been applied
                # via multiple column selection, like in select(df, :, :x1 => :y1)
                # but then transformed_cols[newname] must be nothing
                @assert transformed_cols[newname] === nothing
            else
                select_transform!(nc, df, newdf, transformed_cols, copycols)
            end
        end
    end
    return newdf
end

select(dfv::SubDataFrame, ind::ColumnIndex; copycols::Bool=true) =
    select(dfv, [ind], copycols=copycols)
select(dfv::SubDataFrame, inds::Union{AbstractVector{<:Integer}, AbstractVector{Symbol},
                                      Colon, All, Not, Between, Regex}; copycols::Bool=true) =
    copycols ? dfv[:, inds] : view(dfv, :, inds)

function select(dfv::SubDataFrame, inds...; copycols::Bool=true)
    if copycols
        cs_vec = []
        for v in inds
            if v isa AbstractVector{<:Pair}
                append!(cs_vec, v)
            else
                push!(cs_vec, v)
            end
        end
        return _select(dfv, [normalize_selection(index(dfv), c) for c in cs_vec], true)
    else
        # we do not support transformations here
        # newinds should not be large so making it Vector{Any} should be OK
        newinds = []
        seen_single_column = Set{Int}()
        for ind in inds
            if ind isa ColumnIndex
                ind_idx = index(dfv)[ind]
                if ind_idx in seen_single_column
                    throw(ArgumentError("selecting the same column multiple times using" *
                                        " Symbol or integer is not allowed ($ind was " *
                                        "passed more than once"))
                else
                    push!(seen_single_column, ind_idx)
                end
            else
                newind = normalize_selection(index(dfv), ind)
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

"""
    transform(df::AbstractDataFrame, inds...; copycols::Bool=true)

Create a new data frame that contains columns from `df` and adds columns
specified by `inds` and return it.
Equivalent to `select(df, :, inds..., copycols=copycols)`.

See [`select`](@ref) for detailed rules regarding accepted values for `inds`.
"""
transform(df::AbstractDataFrame, inds...; copycols::Bool=true) =
    select(df, :, inds..., copycols=copycols)
