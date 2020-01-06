normalize_selection(idx::AbstractIndex, sel) = idx[sel]
normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex,<:Pair{<:Function,Symbol}}) =
    idx[first(sel)] => last(sel)
normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, <:Symbol}) =
    idx[first(sel)] => last(sel)

function normalize_selection(idx::AbstractIndex, sel::Pair{<:ColumnIndex, <:Function})
    c = idx[sel]
    fun = last(sel)
    newcol = Symbol(_names(idx)[c], "_", funname(fun))
    return c => fun => newcol
end

function select_transform!(nc::Pair{Int, <:Union{Symbol, Pair{<:Function, Symbol}}},
                           df::DataFrame, newdf::DataFrame,
                           transformed_cols::Dict{Any, Any}, copycols::Bool)
    if nc isa Pair{Int, Symbol}
        newname = last(nc)
        if !isnothing(transformed_cols[newname])
            @assert !hasproperty(newdf, newname)
            newdf[!, newname] = copycols ? df[:, first(nc)] : df[!, first(nc)]
        end
    elseif nc isa Pair{Int, <:Pair{<:Function, Symbol}}
        newname = last(last(nc))
        if !isnothing(transformed_cols[newname])
            @assert !hasproperty(newdf, newname)
            newdf[!, newname] = first(last(nc)).(df[!, first(nc)])
        end
    else
        throw(ExceptionError("code should never reach this branch"))
    end
    transformed_cols[newname] = nothing
end

"""
    select!(df::DataFrame, inds...)

Mutate `df` in place to retain only columns specified by `inds...` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing
provided that the columns requested in each of them are unique and present in `df`.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

Column renaming and transformations are supported.
The syntax for column renaming is `old_column=>new_column_name`.
The syntax for column transformations is `old_column=>fun=>new_column_name`.
`old_column` can be a `Symbol` or an integer, ``new_column_name` must be a `Symbol`,
and `fun` must be a `Function` that is applied row by row to the values of `old_column`.

If more than one argument is passed then duplicates are accepted except for
column renaming and transformation operations, where it is not alloweded to rename/transform
into the same column name.
For example if `:col` is present in `df` a call `select!(df, :col, :)` is valid
and moves the column `:col` moved to be the first one in-place.

Note that including the same column several times in the data frame will create aliases.

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

julia> select!(df, :a=><(1.5)=>:c, :b)
3×2 DataFrame
│ Row │ c    │ b     │
│     │ Bool │ Int64 │
├─────┼──────┼───────┤
│ 1   │ 1    │ 4     │
│ 2   │ 0    │ 5     │
│ 3   │ 0    │ 6     │
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
    transform!(df::DataFrame, cs...)

The same as [`select!`](@ref) but retains all columns existing in `df`.

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

julia> transform!(df, :a=><(1.5)=>:c)
3×3 DataFrame
│ Row │ a     │ b     │ c    │
│     │ Int64 │ Int64 │ Bool │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 4     │ 1    │
│ 2   │ 2     │ 5     │ 0    │
│ 3   │ 3     │ 6     │ 0    │

julia> transform!(df, :b=>(x->x^2)=>:b)
3×3 DataFrame
│ Row │ a     │ b     │ c    │
│     │ Int64 │ Int64 │ Bool │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 16    │ 1    │
│ 2   │ 2     │ 25    │ 0    │
│ 3   │ 3     │ 36    │ 0    │
```
"""
transform!(df::DataFrame, cs...) = select!(df, :, cs...)

"""
    select(df::AbstractDataFrame, inds...; copycols::Bool=true)

Create a new data frame that contains columns from `df`
specified by `inds` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing
provided that the columns requested in each of them are unique and present in `df`.
In particular, regular expressions, `All`, `Between`, and `Not` selectors  are supported.

Also if `df` is a `DataFrame` or `copycols=true` then column renaming and transformations
are supported. The syntax for column renaming is `old_column=>new_column_name`.
The syntax for column transformations is `old_column=>fun=>new_column_name`.
`old_column` can be a `Symbol` or an integer, ``new_column_name` must be a `Symbol`,
and `fun` must be a `Function` that is applied row by row to the values of `old_column`.

If more than one argument is passed then duplicates are accepted except for
column renaming and transformation operations, where it is not allowed to rename/transform
into the same column name.
For example if `:col` is present in `df` a call `select(df, :col, :)` is valid
and creates a new data frame with column `:col` moved to be the first.

If `df` is a `DataFrame` a new `DataFrame` is returned.
If `copycols=true` (the default), then returned `DataFrame` is guaranteed not to share columns with `df`.
If `copycols=false`, then returned `DataFrame` shares column vectors with `df` where possible.

If `df` is a `SubDataFrame` then a `SubDataFrame` is returned if `copycols=false`
and a `DataFrame` with freshly allocated columns otherwise.

Note that if `df` is a `DataFrame` and `copycols=false` then including the same column several times
in the resulting data frame will create aliases.

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

julia> select(df, :b, :a=><(1.5)=>:c)
3×2 DataFrame
│ Row │ b     │ c    │
│     │ Int64 │ Bool │
├─────┼───────┼──────┤
│ 1   │ 4     │ 1    │
│ 2   │ 5     │ 0    │
│ 3   │ 6     │ 0    │
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
    newdf = DataFrame()
    # this line ensures that we fail early if passed source column names are missing
    ncs = normalize_selection.(Ref(index(df)), cs)
    # it should be OK to be type unstable here + in this way we aviod having to compile custom Dict
    transformed_cols = Dict()
    for nc in ncs
        if nc isa Pair
            lnc = last(nc)
            newname = lnc isa Symbol ? lnc : last(lnc)
            @assert newname isa Symbol
            if haskey(transformed_cols, newname)
                throw(ArgumentError("duplicate target transformed or renamed column names passed"))
            end
            transformed_cols[newname] = nc
        end
    end
    for nc in ncs
        if nc isa Union{Int, AbstractVector{Int}}
            allunique(nc) || throw(ArgumentError("duplicate column names selected"))
            for i in nc
                newname = _names(df)[i]
                if !hasproperty(newdf, newname)
                    if haskey(transformed_cols, newname)
                        nct = transformed_cols[newname]
                        @assert !isnothing(nct)
                        select_transform!(nct, df, newdf, transformed_cols, copycols)
                    else
                        newdf[!, newname] = copycols ? df[:, i] : df[!, i]
                    end
                end
            end
        else
            @assert nc isa Pair{Int, <:Union{Symbol, Pair{<:Function, Symbol}}}
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
        return select(copy(dfv), inds..., copycols=false)
    else
        # we do not support transformations here
        return view(dfv, :, All(inds...))
    end
end

"""
    transform(df::AbstractDataFrame, cs...; kwargs...)

The same as [`select`](@ref) but retains all columns existing in `df`.

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

julia> transform(df, :a=><(1.5)=>:c)
3×3 DataFrame
│ Row │ a     │ b     │ c    │
│     │ Int64 │ Int64 │ Bool │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 4     │ 1    │
│ 2   │ 2     │ 5     │ 0    │
│ 3   │ 3     │ 6     │ 0    │
```
"""
transform(df::AbstractDataFrame, cs...; kwargs...) = select(df, :, cs...; kwargs...)
