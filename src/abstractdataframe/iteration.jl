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

"""
    eachrow(df::AbstractDataFrame)

Return a `DataFrameRows` that iterates a data frame row by row,
with each row represented as a `DataFrameRow`.

**Examples**

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
    DataFrameRow(df, index(df), i)
end

Base.@propagate_inbounds function Base.getindex(itr::DataFrameRows{<:SubDataFrame}, i::Int)
    sdf = parent(itr)
    DataFrameRow(parent(sdf), index(sdf), rows(sdf)[i])
end

Base.getproperty(itr::DataFrameRows, col_ind::Symbol) = getproperty(parent(itr), col_ind)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameRows, private::Bool=false) = names(parent(itr))

# Iteration by columns
"""
    DataFrameColumns{<:AbstractDataFrame, V} <: AbstractVector{V}

Iterator over columns of an `AbstractDataFrame` constructed using
[`eachcol(df, true)`](@ref) if `V` is a `Pair{Symbol,AbstractVector}`. Then each
returned value is a pair consisting of column name and column vector.
If `V` is an `AbstractVector` (a value returned by [`eachcol(df, false)`](@ref))
then each returned value is a column vector.
"""
struct DataFrameColumns{T<:AbstractDataFrame, V} <: AbstractVector{V}
    df::T
end

"""
    eachcol(df::AbstractDataFrame, names::Bool=false)

Return a `DataFrameColumns` that iterates an `AbstractDataFrame` column by column.
If `names` is equal to `false` (the default) iteration returns column vectors.
If `names` is equal to `true` pairs consisting of column name and column vector
are yielded.

**Examples**

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

julia> collect(eachcol(df, true))
2-element Array{Pair{Symbol,AbstractArray{T,1} where T},1}:
 :x => [1, 2, 3, 4]
 :y => [11, 12, 13, 14]
```
"""
@inline function eachcol(df::T, names::Bool=false) where T<: AbstractDataFrame
    if names
        DataFrameColumns{T, Pair{Symbol, AbstractVector}}(df)
    else
        DataFrameColumns{T, AbstractVector}(df)
    end
end

Base.size(itr::DataFrameColumns) = (size(parent(itr), 2),)
Base.IndexStyle(::Type{<:DataFrameColumns}) = Base.IndexLinear()

@inline function Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame,
                                                     Pair{Symbol, AbstractVector}}, j::Int)
    @boundscheck checkbounds(itr, j)
    @inbounds _names(parent(itr))[j] => parent(itr)[!, j]
end

@inline function Base.getindex(itr::DataFrameColumns{<:AbstractDataFrame, AbstractVector}, j::Int)
    @boundscheck checkbounds(itr, j)
    @inbounds parent(itr)[!, j]
end

Base.getproperty(itr::DataFrameColumns, col_ind::Symbol) = getproperty(parent(itr), col_ind)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameColumns, private::Bool=false) = names(parent(itr))

"""
    mapcols(f::Union{Function,Type}, df::AbstractDataFrame)

Return a `DataFrame` where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars.

Note that `mapcols` guarantees not to reuse the columns from `df` in the returned
`DataFrame`. If `f` returns its argument then it gets copied before being stored.

**Examples**

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
    DataFrame(vs, _names(df), copycols=false)
end

Base.parent(dfrs::DataFrameRows) = getfield(dfrs, :df)
Base.parent(dfcs::DataFrameColumns) = getfield(dfcs, :df)

function Base.show(io::IO, dfrs::DataFrameRows;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true)
    df = parent(dfrs)
    summary && print(io, "$(nrow(df))×$(ncol(df)) DataFrameRows")
    _show(io, df, allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false)
end

Base.show(io::IO, mime::MIME"text/plain", dfrs::DataFrameRows;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true) =
    show(io, dfrs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary)

Base.show(dfrs::DataFrameRows;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true) =
    show(stdout, dfrs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary)

function Base.show(io::IO, dfcs::DataFrameColumns{T,V};
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   splitcols = get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true) where {T, V}
    df = parent(dfcs)
    summary && print(io, "$(nrow(df))×$(ncol(df)) DataFrameColumns (with names=$(V <: Pair))")
    _show(io, parent(dfcs), allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=false)
end

Base.show(io::IO, mime::MIME"text/plain", dfcs::DataFrameColumns{T,V};
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true) where {T, V} =
    show(io, dfcs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary)

Base.show(dfcs::DataFrameColumns;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true) =
    show(stdout, dfcs, allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary)
