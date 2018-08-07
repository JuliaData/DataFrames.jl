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
    if i > size(itr.df, 1)
        nothing
    else
        (DataFrameRow(itr.df, i), i + 1)
    end
end

Base.size(itr::DFRowIterator) = (size(itr.df, 1), )
Base.length(itr::DFRowIterator) = size(itr.df, 1)
Base.getindex(itr::DFRowIterator, i::Any) = DataFrameRow(itr.df, i)
Base.map(f::Function, dfri::DFRowIterator) = [f(row) for row in dfri]

# Iteration by columns
struct DFColumnIterator{T <: AbstractDataFrame}
    df::T
end
eachcol(df::AbstractDataFrame) = DFColumnIterator(df)

function Base.iterate(itr::DFColumnIterator, i=1)
    if i > size(itr.df, 2)
        nothing
    else
        ((_names(itr.df)[i], itr.df[i]), i + 1)
    end
end

Base.size(itr::DFColumnIterator) = (size(itr.df, 2), )
Base.length(itr::DFColumnIterator) = size(itr.df, 2)
Base.getindex(itr::DFColumnIterator, j::Any) = itr.df[:, j]
function Base.map(f::Function, dfci::DFColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(dfci.df)
        res[n] = f(v)
    end
    res
end
