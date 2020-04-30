##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# Iteration by rows
"""
    DataFrameRows{D<:AbstractDataFrame} <: AbstractVector{DataFrameRow{D,S}}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@ref) function.
"""
struct DataFrameRows{D<:AbstractDataFrame,S} <: AbstractVector{DataFrameRow{D,S}}
    df::D
end

Base.summary(dfrs::DataFrameRows) = "$(length(dfrs))-element DataFrameRows"
Base.summary(io::IO, dfrs::DataFrameRows) = print(io, summary(dfrs))

Base.iterate(::AbstractDataFrame) =
    error("AbstractDataFrame is not iterable. Use eachrow(df) to get a row iterator" *
          " or eachcol(df) to get a column iterator")

"""
    eachrow(df::AbstractDataFrame)

Return a `DataFrameRows` that iterates a data frame row by row,
with each row represented as a `DataFrameRow`.

Because `DataFrameRow`s have an `eltype` of `Any`, use `copy(dfr::DataFrameRow)` to obtain 
a named tuple, which supports iteration and property access like a `DataFrameRow`, 
but also passes information on the `eltypes` of the columns of `df`. 

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> eachrow(df)
4-element DataFrameRows:
 DataFrameRow (row 1)
x  1
y  11
 DataFrameRow (row 2)
x  2
y  12
 DataFrameRow (row 3)
x  3
y  13
 DataFrameRow (row 4)
x  4
y  14

julia> copy.(eachrow(df))
4-element Array{NamedTuple{(:x, :y),Tuple{Int64,Int64}},1}:
 (x = 1, y = 11)
 (x = 2, y = 12)
 (x = 3, y = 13)
 (x = 4, y = 14)

julia> eachrow(view(df, [4,3], [2,1]))
2-element DataFrameRows:
 DataFrameRow (row 4)
y  14
x  4
 DataFrameRow (row 3)
y  13
x  3
```
"""
eachrow(df::AbstractDataFrame) = DataFrameRows{typeof(df), typeof(index(df))}(df)

Base.IndexStyle(::Type{<:DataFrameRows}) = Base.IndexLinear()
Base.size(itr::DataFrameRows) = (size(parent(itr), 1), )

Base.@propagate_inbounds function Base.getindex(itr::DataFrameRows, i::Int)
    df = parent(itr)
    return DataFrameRow(df, index(df), i)
end

Base.@propagate_inbounds function Base.getindex(itr::DataFrameRows{<:SubDataFrame}, i::Int)
    sdf = parent(itr)
    return DataFrameRow(parent(sdf), index(sdf), rows(sdf)[i])
end

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DataFrameRows, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DataFrameRows, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DataFrameRows, s::Symbol) = haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DataFrameRows, s::AbstractString) = haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameRows, private::Bool=false) = propertynames(parent(itr))

# Iteration by columns

"""
    DataFrameColumns{<:AbstractDataFrame} <: AbstractVector{AbstractVector}

An `AbstractVector` that allows iteration over columns of an `AbstractDataFrame`.
Indexing into `DataFrameColumns` objects using integer or symbol indices
returns the corresponding column (without copying).
"""
struct DataFrameColumns{T<:AbstractDataFrame} <: AbstractVector{AbstractVector}
    df::T
end

Base.summary(dfcs::DataFrameColumns)= "$(length(dfcs))-element DataFrameColumns"
Base.summary(io::IO, dfcs::DataFrameColumns) = print(io, summary(dfcs))

"""
    eachcol(df::AbstractDataFrame)

Return a `DataFrameColumns` that is an `AbstractVector`
that allows iterating an `AbstractDataFrame` column by column.
Additionally it is allowed to index `DataFrameColumns` using column names.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> collect(eachcol(df))
2-element Array{AbstractArray{T,1} where T,1}:
 [1, 2, 3, 4]
 [11, 12, 13, 14]

julia> map(eachcol(df)) do col
           maximum(col) - minimum(col)
       end
2-element Array{Int64,1}:
 3
 3

julia> sum.(eachcol(df))
2-element Array{Int64,1}:
 10
 50
```
"""
eachcol(df::AbstractDataFrame) = DataFrameColumns(df)

Base.size(itr::DataFrameColumns) = (size(parent(itr), 2),)
Base.IndexStyle(::Type{<:DataFrameColumns}) = Base.IndexLinear()

@inline function Base.getindex(itr::DataFrameColumns, j::Int)
    @boundscheck checkbounds(itr, j)
    @inbounds parent(itr)[!, j]
end

Base.getindex(itr::DataFrameColumns, j::Symbol) = parent(itr)[!, j]

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DataFrameColumns, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DataFrameColumns, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DataFrameColumns, s::Symbol) =
    haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DataFrameColumns, s::AbstractString) =
    haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameColumns, private::Bool=false) =
    propertynames(parent(itr))

"""
    keys(dfc::DataFrameColumns)

Get a vector of column names of `dfc` as `Symbol`s.
"""
Base.keys(itr::DataFrameColumns) = propertynames(itr)

"""
    pairs(dfc::DataFrameColumns)

Return an iterator of pairs associating the name of each column of `dfc`
with the corresponding column vector, i.e. `name => col`
where `name` is the column name of the column `col`.
"""
Base.pairs(itr::DataFrameColumns) = Base.Iterators.Pairs(itr, keys(itr))

Base.parent(itr::Union{DataFrameRows, DataFrameColumns}) = getfield(itr, :df)
Base.names(itr::Union{DataFrameRows, DataFrameColumns}) = names(parent(itr))
Base.names(itr::Union{DataFrameRows, DataFrameColumns}, cols) = names(parent(itr), cols)

function Base.show(io::IO, dfrs::DataFrameRows;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true)
    df = parent(dfrs)
    summary && print(io, "$(nrow(df))×$(ncol(df)) DataFrameRows")
    _show(io, df, allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false, eltypes=eltypes)
end

Base.show(io::IO, mime::MIME"text/plain", dfrs::DataFrameRows;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(io, dfrs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary, eltypes=eltypes)

Base.show(dfrs::DataFrameRows;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(stdout, dfrs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary, eltypes=eltypes)

function Base.show(io::IO, dfcs::DataFrameColumns;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true)
    df = parent(dfcs)
    summary && print(io, "$(nrow(df))×$(ncol(df)) DataFrameColumns")
    _show(io, parent(dfcs), allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false, eltypes=eltypes)
end

Base.show(io::IO, mime::MIME"text/plain", dfcs::DataFrameColumns;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(io, dfcs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary, eltypes=eltypes)

Base.show(dfcs::DataFrameColumns;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(stdout, dfcs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary, eltypes=eltypes)

"""
    mapcols(f::Union{Function,Type}, df::AbstractDataFrame)

Return a `DataFrame` where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

Note that `mapcols` guarantees not to reuse the columns from `df` in the returned
`DataFrame`. If `f` returns its argument then it gets copied before being stored.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> mapcols(x -> x.^2, df)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 121   │
│ 2   │ 4     │ 144   │
│ 3   │ 9     │ 169   │
│ 4   │ 16    │ 196   │
```
"""
function mapcols(f::Union{Function,Type}, df::AbstractDataFrame)
    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for v in eachcol(df)
        fv = f(v)
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv === v ? copy(fv) : fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end
    return DataFrame(vs, _names(df), copycols=false)
end

"""
    mapcols!(f::Union{Function,Type}, df::DataFrame)

Update a `DataFrame` in-place where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

Note that `mapcols!` reuses the columns from `df` if they are returned by `f`.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 11    │
│ 2   │ 2     │ 12    │
│ 3   │ 3     │ 13    │
│ 4   │ 4     │ 14    │

julia> mapcols!(x -> x.^2, df);

julia> df
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 121   │
│ 2   │ 4     │ 144   │
│ 3   │ 9     │ 169   │
│ 4   │ 16    │ 196   │
```
"""
function mapcols!(f::Union{Function,Type}, df::DataFrame)
    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for v in eachcol(df)
        fv = f(v)
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end

    len_min, len_max = extrema(length(v) for v in vs)
    if len_min != len_max
        throw(DimensionMismatch("lengths of returned vectors must be identical"))
    end
    _columns(df) .= vs

    return df
end
