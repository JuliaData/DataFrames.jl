##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
"""
    DFRowIterator{<:AbstractDataFrame}

Iterator over rows of an `AbstractDataFrame`.
Each returned value is represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@link) function.
"""
struct DFRowIterator{T <: AbstractDataFrame}
    df::T
end

"""
    eachrow(df::AbstractDataFrame)

Return `DFRowIterator` that iterates an `AbstractDataFrame` row by row,
with each row represented as a `DataFrameRow`.
"""
eachrow(df::AbstractDataFrame) = DFRowIterator(df)

function Base.iterate(itr::DFRowIterator, i=1)
    i > size(itr.df, 1) && return nothing
    return (DataFrameRow(itr.df, i), i + 1)
end
Base.eltype(::DFRowIterator{T}) where {T} = DataFrameRow{T}
Base.size(itr::DFRowIterator) = (size(itr.df, 1), )
Base.length(itr::DFRowIterator) = size(itr.df, 1)
Base.getindex(itr::DFRowIterator, i) = DataFrameRow(itr.df, i)
Base.map(f::Function, dfri::DFRowIterator) = [f(row) for row in dfri]

# Iteration by columns
"""
    DFColumnIterator{<:AbstractDataFrame}

Iterator over columns of an `AbstractDataFrame`.
Each returned value is a tuple consisting of column name and column vector.

A value of this type is returned by `eachcol` function.
"""
struct DFColumnIterator{T <: AbstractDataFrame}
    df::T
end

"""
    eachcol(df::AbstractDataFrame)

Return a `DFColumnIterator` that iterates an `AbstractDataFrame` column by column.
Iteration returns a tuple consisting of column name and column vector.

`DFColumnIterator` has a custom implementation of the `map` function which
returns a `DataFrame` and assumes that a function argument passed do
the `map` function accepts only column data.

**Examples**

```julia
df = DataFrame(x=1:4, y=11:14)
map(sum, eachcol(df)) == DataFrame(x=10, y=50)
```
"""
eachcol(df::AbstractDataFrame) = DFColumnIterator(df)

function Base.iterate(itr::DFColumnIterator, j=1)
    j > size(itr.df, 2) && return nothing
    return ((_names(itr.df)[j], itr.df[j]), j + 1)
end
Base.eltype(::DFColumnIterator) = Tuple{Symbol, AbstractVector}
Base.size(itr::DFColumnIterator) = (size(itr.df, 2), )
Base.length(itr::DFColumnIterator) = size(itr.df, 2)
Base.getindex(itr::DFColumnIterator, j) = itr.df[j]
function Base.map(f::Function, dfci::DFColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(dfci.df)
        res[n] = f(v)
    end
    res
end
