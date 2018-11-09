##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
"""
    DFRowIterator{<:AbstractDataFrame}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@link) function.
"""
struct DFRowIterator{T<:AbstractDataFrame}
    df::T
end

"""
    eachrow(df::AbstractDataFrame)

Return a `DFRowIterator` that iterates an `AbstractDataFrame` row by row,
with each row represented as a `DataFrameRow`.
"""
eachrow(df::AbstractDataFrame) = DFRowIterator(df)

Base.size(itr::DFRowIterator) = (size(itr.df, 1), )
Base.size(itr::DFRowIterator, ix) =
    ix == 1 ? length(itr) : throw(ArgumentError("Incorrect dimension"))
Base.length(itr::DFRowIterator) = size(itr.df, 1)
Base.firstindex(itr::DFRowIterator) = 1
Base.lastindex(itr::DFRowIterator) = length(itr)

function Base.iterate(itr::DFRowIterator, i=1)
    i > size(itr.df, 1) && return nothing
    return (DataFrameRow(itr.df, i), i + 1)
end

Base.eltype(::DFRowIterator{T}) where {T} = DataFrameRow{T}
Base.getindex(itr::DFRowIterator, i) = DataFrameRow(itr.df, i)

# Iteration by columns
"""
    DFColumnIterator{<:AbstractDataFrame, C}

Iterator over columns of an `AbstractDataFrame`.
If `C` is `true` (a value returned by the [`eachcol`](@link) function)
then each returned value is a tuple consisting of column name and column vector.
If `C` is `false` (a value returned by the [`columns`](@link) function)
then each returned value is a column vector.
"""
struct DFColumnIterator{T<:AbstractDataFrame, C}
    df::T
end

"""
    eachcol(df::AbstractDataFrame)

Return a `DFColumnIterator` that iterates an `AbstractDataFrame` column by column.
Iteration returns a tuple consisting of column name and column vector.

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
2-element Array{Tuple{Symbol,Any},1}:
 (:x, [1, 2, 3, 4])
 (:y, [11, 12, 13, 14])
```
"""
eachcol(df::T) where T<: AbstractDataFrame = DFColumnIterator{T, true}(df)

"""
    columns(df::AbstractDataFrame)

Return a `DFColumnIterator` that iterates an `AbstractDataFrame` column by column.
Iteration returns a column vector.

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

julia> collect(columns(df))
2-element Array{AbstractArray{T,1} where T,1}:
 [1, 2, 3, 4]
 [11, 12, 13, 14]
```
"""
columns(df::T) where T<: AbstractDataFrame = DFColumnIterator{T, false}(df)

Base.length(itr::DFColumnIterator) = size(itr.df, 2)
Base.size(itr::DFColumnIterator) = (size(itr.df, 2),)
Base.size(itr::DFColumnIterator, ix) =
    ix == 1 ? length(itr) : throw(ArgumentError("Incorrect dimension"))
Base.firstindex(itr::DFColumnIterator) = 1
Base.lastindex(itr::DFColumnIterator) = length(itr)

function Base.iterate(itr::DFColumnIterator{<:AbstractDataFrame,true}, j=1)
    j > size(itr.df, 2) && return nothing
    return ((_names(itr.df)[j], itr.df[j]), j + 1)
end

Base.eltype(::DFColumnIterator{<:AbstractDataFrame,true}) =
    Tuple{Symbol, AbstractVector}

function Base.getindex(itr::DFColumnIterator{<:AbstractDataFrame,true}, j)
    # TODO: change to the way getindex for false is defined below afted deprecation
    Base.depwarn("calling getindex on DFColumnIterator{<:AbstractDataFrame,true} " *
            " object will only accept integer indexing and will return " *
            "a tuple of column name and column value in the future.", :getindex)
    itr.df[j]
end

function Base.iterate(itr::DFColumnIterator{<:AbstractDataFrame,false}, j=1)
    j > size(itr.df, 2) && return nothing
    return (itr.df[j], j + 1)
end

Base.eltype(::DFColumnIterator{<:AbstractDataFrame,false}) = AbstractVector
Base.getindex(itr::DFColumnIterator{<:AbstractDataFrame,false}, j::Integer) =
    itr.df[j]
Base.getindex(itr::DFColumnIterator{<:AbstractDataFrame,false}, j::Bool) =
    throw(ArgumentError("invalid index $j of type Bool"))
