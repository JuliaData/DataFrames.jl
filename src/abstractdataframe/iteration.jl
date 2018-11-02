##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
struct DFRowIterator{T <: AbstractDataFrame}
    df::T
end
"""
    eachrow(df) => DataFrames.DFRowIterator

Iterate a DataFrame row by row, with each row represented as a `DataFrameRow`,
which is a view that acts like a one-row DataFrame.
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
struct DFColumnIterator{T <: AbstractDataFrame}
    df::T
end
eachcol(df::AbstractDataFrame) = DFColumnIterator(df)

function Base.iterate(itr::DFColumnIterator, j=1)
    j > size(itr.df, 2) && return nothing
    return ((_names(itr.df)[j], itr.df[j]), j + 1)
end
Base.eltype(::DFColumnIterator) = Tuple{Symbol, Any}
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
