##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# Iteration by rows
immutable DFRowIterator
    df::AbstractDataFrame
end
eachrow(df::AbstractDataFrame) = DFRowIterator(df)

Base.start(itr::DFRowIterator) = 1
Base.done(itr::DFRowIterator, i::Int) = i > size(itr.df, 1)
Base.next(itr::DFRowIterator, i::Int) = (DataFrameRow(itr.df, i), i + 1)
Base.size(itr::DFRowIterator) = (size(itr.df, 1), )
Base.length(itr::DFRowIterator) = size(itr.df, 1)
Base.getindex(itr::DFRowIterator, i::Any) = DataFrameRow(itr.df, i)
Base.map(f::Function, dfri::DFRowIterator) = [f(row) for row in dfri]

# Iteration by columns
immutable DFColumnIterator
    df::AbstractDataFrame
end
eachcol(df::AbstractDataFrame) = DFColumnIterator(df)

Base.start(itr::DFColumnIterator) = 1
Base.done(itr::DFColumnIterator, j::Int) = j > size(itr.df, 2)
Base.next(itr::DFColumnIterator, j::Int) = (itr.df[:, j], j + 1)
Base.size(itr::DFColumnIterator) = (size(itr.df, 2), )
Base.length(itr::DFColumnIterator) = size(itr.df, 2)
Base.getindex(itr::DFColumnIterator, j::Any) = itr.df[:, j]
function Base.map(f::Function, dfci::DFColumnIterator)
    # note: `f` must return a consistent length
    res = DataFrame()
    for i = 1:size(dfci.df, 2)
        res[i] = f(dfci[i])
    end
    names!(res, names(dfci.df))
    res
end

# Iteration matches that of Associative types (experimental)
Base.start(df::AbstractDataFrame) = 1
Base.done(df::AbstractDataFrame, i) = i > ncol(df)
Base.next(df::AbstractDataFrame, i) = ((names(df)[i], df[i]), i + 1)
