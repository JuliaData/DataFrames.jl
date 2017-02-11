##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# TODO: Reconsider/redesign eachrow -- ~100% overhead

# Iteration by rows
immutable DFRowIterator{T <: AbstractDataTable}
    df::T
end
eachrow(df::AbstractDataTable) = DFRowIterator(df)

Base.start(itr::DFRowIterator) = 1
Base.done(itr::DFRowIterator, i::Int) = i > size(itr.df, 1)
Base.next(itr::DFRowIterator, i::Int) = (DataTableRow(itr.df, i), i + 1)
Base.size(itr::DFRowIterator) = (size(itr.df, 1), )
Base.length(itr::DFRowIterator) = size(itr.df, 1)
Base.getindex(itr::DFRowIterator, i::Any) = DataTableRow(itr.df, i)
Base.map(f::Function, dfri::DFRowIterator) = [f(row) for row in dfri]

# Iteration by columns
immutable DFColumnIterator{T <: AbstractDataTable}
    df::T
end
eachcol(df::AbstractDataTable) = DFColumnIterator(df)

Base.start(itr::DFColumnIterator) = 1
Base.done(itr::DFColumnIterator, j::Int) = j > size(itr.df, 2)
Base.next(itr::DFColumnIterator, j::Int) = ((_names(itr.df)[j], itr.df[j]), j + 1)
Base.size(itr::DFColumnIterator) = (size(itr.df, 2), )
Base.length(itr::DFColumnIterator) = size(itr.df, 2)
Base.getindex(itr::DFColumnIterator, j::Any) = itr.df[:, j]
function Base.map(f::Function, dfci::DFColumnIterator)
    # note: `f` must return a consistent length
    res = DataTable()
    for (n, v) in eachcol(dfci.df)
        res[n] = f(v)
    end
    res
end
