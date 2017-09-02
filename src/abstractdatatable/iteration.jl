##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
immutable DTRowIterator{T <: AbstractDataFrame}
    dt::T
end
eachrow(dt::AbstractDataFrame) = DTRowIterator(dt)

Base.start(itr::DTRowIterator) = 1
Base.done(itr::DTRowIterator, i::Int) = i > size(itr.dt, 1)
Base.next(itr::DTRowIterator, i::Int) = (DataFrameRow(itr.dt, i), i + 1)
Base.size(itr::DTRowIterator) = (size(itr.dt, 1), )
Base.length(itr::DTRowIterator) = size(itr.dt, 1)
Base.getindex(itr::DTRowIterator, i::Any) = DataFrameRow(itr.dt, i)
Base.map(f::Function, dtri::DTRowIterator) = [f(row) for row in dtri]

# Iteration by columns
immutable DTColumnIterator{T <: AbstractDataFrame}
    dt::T
end
eachcol(dt::AbstractDataFrame) = DTColumnIterator(dt)

Base.start(itr::DTColumnIterator) = 1
Base.done(itr::DTColumnIterator, j::Int) = j > size(itr.dt, 2)
Base.next(itr::DTColumnIterator, j::Int) = ((_names(itr.dt)[j], itr.dt[j]), j + 1)
Base.size(itr::DTColumnIterator) = (size(itr.dt, 2), )
Base.length(itr::DTColumnIterator) = size(itr.dt, 2)
Base.getindex(itr::DTColumnIterator, j::Any) = itr.dt[:, j]
function Base.map(f::Function, dtci::DTColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(dtci.dt)
        res[n] = f(v)
    end
    res
end
