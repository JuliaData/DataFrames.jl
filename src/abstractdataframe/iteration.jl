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
struct DFRowIterator{T <: AbstractDataFrame}
    df::T
end

"""
    eachrow(df::AbstractDataFrame)

Return a `DFRowIterator` that iterates an `AbstractDataFrame` row by row,
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

# Iteration by columns
"""
    DFColumnIterator{<:AbstractDataFrame}

Iterator over columns of an `AbstractDataFrame`.
Each returned value is a tuple consisting of column name and column vector.

A value of this type is returned by the [`eachcol`](@link) function.
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
the `map` function accepts takes only a column vector.

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

julia> map(sum, eachcol(df))
1×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 10    │ 50    │
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
function Base.map(f::Union{Function,Type}, dfci::DFColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(dfci.df)
        res[n] = f(v)
    end
    res
end
