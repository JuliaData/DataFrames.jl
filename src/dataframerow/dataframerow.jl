# Container for a DataFrame row
immutable DataFrameRow{T <: AbstractDataFrame}
    df::T
    row::Int
end

function Base.getindex(r::DataFrameRow, idx::AbstractArray)
    return DataFrameRow(r.df[[idx]], r.row)
end

function Base.getindex(r::DataFrameRow, idx::Any)
    return r.df[r.row, idx]
end

function Base.setindex!(r::DataFrameRow, value::Any, idx::Any)
    return setindex!(r.df, value, r.row, idx)
end

Base.names(r::DataFrameRow) = names(r.df)
_names(r::DataFrameRow) = _names(r.df)

Base.sub(r::DataFrameRow, c) = DataFrameRow(r.df[[c]], r.row)

index(r::DataFrameRow) = index(r.df)

Base.length(r::DataFrameRow) = size(r.df, 2)

Base.endof(r::DataFrameRow) = size(r.df, 2)

Base.collect(r::DataFrameRow) = (Symbol, Any)[x for x in r]

Base.start(r::DataFrameRow) = 1

Base.next(r::DataFrameRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataFrameRow, s) = s > length(r)

DataArrays.array(r::DataFrameRow) = DataArrays.array(r.df[r.row,:])
