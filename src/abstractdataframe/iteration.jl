##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
immutable DTRowIterator{T <: AbstractDataFrame}
    df::T
end
eachrow(df::AbstractDataFrame) = DTRowIterator(df)

Base.start(itr::DTRowIterator) = 1
Base.done(itr::DTRowIterator, i::Int) = i > size(itr.df, 1)
Base.next(itr::DTRowIterator, i::Int) = (DataFrameRow(itr.df, i), i + 1)
Base.size(itr::DTRowIterator) = (size(itr.df, 1), )
Base.length(itr::DTRowIterator) = size(itr.df, 1)
Base.getindex(itr::DTRowIterator, i::Any) = DataFrameRow(itr.df, i)
Base.map(f::Function, dfri::DTRowIterator) = [f(row) for row in dfri]

# Iteration by columns
immutable DTColumnIterator{T <: AbstractDataFrame}
    df::T
end
eachcol(df::AbstractDataFrame) = DTColumnIterator(df)

Base.start(itr::DTColumnIterator) = 1
Base.done(itr::DTColumnIterator, j::Int) = j > size(itr.df, 2)
Base.next(itr::DTColumnIterator, j::Int) = ((_names(itr.df)[j], itr.df[j]), j + 1)
Base.size(itr::DTColumnIterator) = (size(itr.df, 2), )
Base.length(itr::DTColumnIterator) = size(itr.df, 2)
Base.getindex(itr::DTColumnIterator, j::Any) = itr.df[:, j]
function Base.map(f::Function, dfci::DTColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for (n, v) in eachcol(dfci.df)
        res[n] = f(v)
    end
    res
end
