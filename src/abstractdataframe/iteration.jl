##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
immutable DFRowIterator{T <: AbstractDataFrame}
    df::T
    nrow::Int
    @compat (::Type{DFRowIterator}){T}(df::T) = new{T}(df, nrow(df))
end
eachrow(df::AbstractDataFrame) = DFRowIterator(df)

Base.start(itr::DFRowIterator) = 1
Base.done(itr::DFRowIterator, i::Int) = i > itr.nrow
Base.next(itr::DFRowIterator, i::Int) = (DataFrameRow(itr.df, i), i + 1)
Base.size(itr::DFRowIterator) = (itr.nrow, )
Base.length(itr::DFRowIterator) = itr.nrow
Base.getindex(itr::DFRowIterator, i::Any) = DataFrameRow(itr.df, i)
Base.map(f::Function, dfri::DFRowIterator) = [f(row) for row in dfri]

# Iteration by columns
immutable DFColumnIterator{T <: AbstractDataFrame}
    df::T
end
eachcol(df::AbstractDataFrame) = DFColumnIterator(df)

Base.start(itr::DFColumnIterator) = 1
Base.done(itr::DFColumnIterator, j::Int) = j > size(itr.df, 2)
Base.next(itr::DFColumnIterator, j::Int) = ((_names(itr.df)[j], itr.df[j]), j + 1)
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
