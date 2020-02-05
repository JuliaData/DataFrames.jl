# TODO:
# * add transform and transfom! functions
# * add `Col` wrapper for whole column operations
# * update documentation
# * add tests

# normalize_selection function makes sure that whatever input format of idx is it
# will end up in one of four canonical forms
# 1) Int
# 2) AbstractVector{Int}
# 3) Pair{Int, Pair{ColRename, Symbol}}
# 4) Pair{Int, <:Pair{<:Base.Callable, Symbol}}
# 5) Pair{AbstractVector{Int}, <:Pair{<:Base.Callable, Symbol}}
# 6) Pair{Int, Pair{Row, Symbol}}
# 7) Pair{AbstractVector{Int}, Pair{Row, Symbol}}
# in this way we can easily later decide on the codepath using type signatures

"""
    ColRename

A singleton type indicating that column renaming operation was requested in `select`.
"""
struct ColRename end

"""
    Row

A type used for selection operations to signal that the wrapped function should
be applied to each element (row) of the selection.
"""
struct Row{T}
    fun::T
end

# add a method to funname defined in other/utils.jl
funname(row::Row) = funname(row.fun)

normalize_selection(idx::AbstractIndex, sel) = idx[sel]

function normalize_selection(idx::AbstractIndex, sel::ColumnIndex)
    c = idx[sel]
    return c => ColRename() => _names(idx)[c]
end

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, Symbol})
    c = idx[first(sel)]
    return c => ColRename() => last(sel)
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any,<:Pair{<:Union{Base.Callable, Row}, Symbol}})
    c = first(sel)
    return idx[c] => last(sel)
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:ColumnIndex,<:Union{Base.Callable, Row}})
    c = idx[first(sel)]
    fun = last(sel)
    newcol = Symbol(_names(idx)[c], "_", funname(fun))
    return c => fun => newcol
end

function normalize_selection(idx::AbstractIndex,
                             sel::Pair{<:Any, <:Union{Base.Callable,Row}})
    c = idx[first(sel)]
    fun = last(sel)
    if length(c) > 3
        newcol = Symbol(join(@views _names(idx)[c[1:2]], '_'), "_etc_", funname(fun))
    else
        newcol = Symbol(join(view(_names(idx), c), '_'), '_', funname(fun))
    end
    return c => fun => newcol
end

function select_transform!(nc::Union{Pair{Int, Pair{ColRename, Symbol}},
                                     Pair{<:Union{Int, AbstractVector{Int}},
                                          <:Pair{<:Union{Base.Callable, Row}, Symbol}}},
                           df::DataFrame, newdf::DataFrame,
                           transformed_cols::Dict{Symbol, Any}, copycols::Bool)
    col_idx = first(nc)
    transform_spec = last(nc)
    newname = last(transform_spec)
    if !isnothing(transformed_cols[newname])
        @assert !hasproperty(newdf, newname)
    end
    if nc isa Pair{Int, Pair{ColRename, Symbol}}
        newdf[!, newname] = copycols ? df[:, col_idx] : df[!, col_idx]
    elseif nc isa Pair{Int, <:Pair{<:Row, Symbol}}
        newdf[!, newname] = (first(transform_spec).fun).(df[!, col_idx])
    elseif nc isa Pair{Int, <:Pair{<:Base.Callable, Symbol}}
        res = first(transform_spec)(df[!, col_idx])
        newdf[!, newname] = res isa AbstractVector ? res : [res]
    elseif nc isa Pair{<:AbstractVector{Int}, <:Pair{<:Row, Symbol}}
        if length(col_idx) == 0
            newdf[!, newname] = map(_ -> (first(transform_spec).fun)(), axes(df, 1))
        else
            rowiterator = Tables.rows(Tables.columntable(df[!, col_idx]))
            newdf[!, newname] = map(first(transform_spec).fun,
                                    Tables.namedtupleiterator(eltype(rowiterator), rowiterator))
        end
    elseif nc isa Pair{<:AbstractVector{Int}, <:Pair{<:Base.Callable, Symbol}}
        res = first(transform_spec)(Tables.columntable(df[!, col_idx]))
        newdf[!, newname] = res isa AbstractVector ? res : [res]
    else
        throw(ErrorException("code should never reach this branch"))
    end
    transformed_cols[newname] = nothing
end

"""
    select!(df::DataFrame, inds...)

Mutate `df` in place to retain only columns specified by `inds...` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

Columns can be renamed using the `old_column => new_column_name` syntax,
and transformed using the `old_column => fun => new_column_name` syntax.
`new_column_name` must be a `Symbol`, and `fun` a function or a type.
If `old_column` is a `Symbol` or an integer then `fun` is applied to each element
(row) of `old_column`.
Otherwise `old_column` can be any column indexing syntax, but in this case `fun`
will be passed a `NamedTuple` representing each row, holding only
the columns specified by `old_column`.

Column transformation can also be specified using the short `old_column => fun` form.
In this case, `new_column_name` is automatically generated as `\$(old_column)_\$(fun)`.
Up to three column names are used for multiple input columns and they are joined
using `_`; if more than three columns are passed then the name consists of the
first two names and `etc` suffix then, e.g. `[:a,:b,:c,:d] => fun` produces
the new column name `a_b_etc_fun`.

If a collection of column names is passed to `select!` then requesting duplicate column
names in target data frame are accepted (e.g. `select!(df, [:a], :, r"a")` is allowed)
and only the first occurrence is used. In particular a syntax to move column `:col`
to the first position in the data frame is `select!(df, :col, :)`.
On the contrary, output column names of renaming, transformation and single column
selection operations must be unique, so e.g. `select!(df, :a, :a => :a)` or
`select!(df, :a, :a => sin => :a)` are not allowed.

Note that including the same column several times in the data frame via renaming
when `copycols=false` will create column aliases. An example of such a situation is
`select!(df, :a, :a => :b, :a => :c, copycols=false)`.

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

julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select!(df, :a => Row(sin) => :c, :b)
3×2 DataFrame
│ Row │ c        │ b     │
│     │ Float64  │ Int64 │
├─────┼──────────┼───────┤
│ 1   │ 0.841471 │ 4     │
│ 2   │ 0.909297 │ 5     │
│ 3   │ 0.14112  │ 6     │

julia> select!(df, :, [:c, :b] => x -> x.c + x.b .- sum(x.b) / length(x.b))
3×3 DataFrame
│ Row │ c        │ b     │ c_b_function │
│     │ Float64  │ Int64 │ Float64      │
├─────┼──────────┼───────┼──────────────┤
│ 1   │ 0.841471 │ 4     │ -0.158529    │
│ 2   │ 0.909297 │ 5     │ 0.909297     │
│ 3   │ 0.14112  │ 6     │ 1.14112      │
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
    select(df::AbstractDataFrame, inds...; copycols::Bool=true)

Create a new data frame that contains columns from `df`
specified by `inds` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

Also if `df` is a `DataFrame` or `copycols=true` then column renaming and transformations
are supported.

Columns can be renamed using the `old_column => new_column_name` syntax,
and transformed using the `old_column => fun => new_column_name` syntax.
`new_column_name` must be a `Symbol`, and `fun` a function or a type.
If `old_column` is a `Symbol` or an integer then `fun` is applied to each element
(row) of `old_column`.
Otherwise `old_column` can be any column indexing syntax, but in this case `fun`
will be passed a `NamedTuple` representing each row, holding only
the columns specified by `old_column`.

Column transformation can also be specified using the short `old_column => fun` form.
In this case, `new_column_name` is automatically generated as `\$(old_column)_\$(fun)`.
Up to three column names are used for multiple input columns and they are joined
using `_`; if more than three columns are passed then the name consists of the
first two names and `etc` suffix then, e.g. `[:a,:b,:c,:d] => fun` produces
the new column name `a_b_etc_fun`.

If a collection of column names is passed to `select` then requesting duplicate column
names in target data frame are accepted (e.g. `select(df, [:a], :, r"a")` is allowed)
and only the first occurrence is used. In particular a syntax to move column `:col`
to the first position in the data frame is `select(df, :col, :)`.
On the contrary, output column names of renaming, transformation and single column
selection operations must be unique, so e.g. `select(df, :a, :a => :a)` or
`select(df, :a, :a => sin => :a)` are not allowed.

If `df` is a `DataFrame` a new `DataFrame` is returned.
If `copycols=true` (the default), then returned `DataFrame` is guaranteed not to share columns with `df`.
If `copycols=false`, then returned `DataFrame` shares column vectors with `df` where possible.

If `df` is a `SubDataFrame` then a `SubDataFrame` is returned if `copycols=false`
and a `DataFrame` with freshly allocated columns otherwise.

Note that including the same column several times in the data frame via renaming
when `copycols=false` will create column aliases. An example of such a situation is
`select(df, :a, :a => :b, :a => :c, copycols=false)`.

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

julia> select(df, :a=>:c, :b)
3×2 DataFrame
│ Row │ c     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select(df, :a => Row(sin) => :c, :b)
3×2 DataFrame
│ Row │ c        │ b     │
│     │ Float64  │ Int64 │
├─────┼──────────┼───────┤
│ 1   │ 0.841471 │ 4     │
│ 2   │ 0.909297 │ 5     │
│ 3   │ 0.14112  │ 6     │

julia> select(df, :, [:a, :b] => x -> x.a + x.b .- sum(x.b) / length(x.b))
3×3 DataFrame
│ Row │ a     │ b     │ a_b_function │
│     │ Int64 │ Int64 │ Float64      │
├─────┼───────┼───────┼──────────────┤
│ 1   │ 1     │ 4     │ 0.0          │
│ 2   │ 2     │ 5     │ 2.0          │
│ 3   │ 3     │ 6     │ 4.0          │
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

select(df::DataFrame, cs...; copycols::Bool=true) =
    _select(df, [normalize_selection(index(df), c) for c in cs], copycols)

function _select(df::AbstractDataFrame, normalized_cs, copycols::Bool)
    @assert !(df isa SubDataFrame && copycols==false)
    newdf = DataFrame()
    # the role of transformed_cols is the following
    # * make sure that we do not use the same target column name twice in transformations;
    #   note though that it can appear in no-transformation selection like
    #    `select(df, :, :a => sin => :a), where :a is produced both by `:` and by `:a => sin => :a`
    # * make sure that if some column is produced by transformation like `:a => sin => :a`
    #   and it appears earlier or later in non-transforming selection like `:` or `:a`
    #   then the transformation is computed and inserted in to the target data frame once and only once
    #   the first time the target column is requested to be produced.
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
    # julia> select(df, :, :a=>Row(sin)=>:a, :a, 1)
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
                throw(ArgumentError("duplicate target transformed or renamed " *
                                    "column name $newname passed"))
            end
            transformed_cols[newname] = nc
        end
    end
    for nc in normalized_cs
        if nc isa Union{Int, AbstractVector{Int}}
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                if !hasproperty(newdf, newname)
                    if haskey(transformed_cols, newname)
                        nct = transformed_cols[newname]
                        @assert nct !== nothing
                        select_transform!(nct, df, newdf, transformed_cols, copycols)
                    else
                        newdf[!, newname] = copycols ? df[:, i] : df[!, i]
                    end
                end
            end
        else
            select_transform!(nc, df, newdf, transformed_cols, copycols)
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
        return _select(dfv, [normalize_selection(index(dfv), c) for c in inds], true)
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
                                        "`Symbol` or integer is not allowed ($ind was " *
                                        "passed more than once"))
                else
                    push!(seen_single_column, ind_idx)
                end
                push!(new_column_name, ind_idx)
            else
                newind = normalize_selection(index(dfv), ind)
                if newind isa Pair
                    throw(ArgumentError("transforming and renaming columns of a " *
                                        "`SubDataFrame` is not allowed when `copycols=false`"))
                end
                push!(newinds, newind)
            end
        end
        return view(dfv, :, All(newinds...))
    end
end
