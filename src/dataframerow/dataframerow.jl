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

Base.names(df::DataFrameRow) = names(df.df)

Base.sub(r::DataFrameRow, c) = DataFrameRow(r.df[[c]], r.row)

index(r::DataFrameRow) = index(r.df)

Base.length(r::DataFrameRow) = size(r.df, 2)

Base.endof(r::DataFrameRow) = size(r.df, 2)

Base.collect(r::DataFrameRow) = (String, Any)[x for x in r]

Base.start(r::DataFrameRow) = 1

Base.next(r::DataFrameRow, s) = ((names(r)[s], r[s]), s + 1)

Base.done(r::DataFrameRow, s) = s > length(r)

function DataArrays.array(r::DataFrameRow)
    Base.depwarn(
        """
        array(r::DataFrameRow) is deprecated.
        Use convert(Matrix, r).
        """,
        :array
    )
    return DataArrays.array(r.df[r.row,:])
end

function Base.convert{T}(::Type{Matrix{T}}, r::DataFrameRow)
    return convert(Matrix{T}, r.df[r.row, :])
end

function Base.convert(::Type{Matrix}, r::DataFrameRow)
    return convert(Matrix{typejoin(eltypes(r.df)...)}, r.df[r.row, :])
end

function Base.convert{T}(::Type{Vector{T}}, r::DataFrameRow)
    ncols = size(r.df, 2)
    res = Array(T, ncols)
    for j in 1:ncols
        res[j] = convert(T, r.df[r.row, j])
    end
    return res
end

function Base.convert(::Type{Vector}, r::DataFrameRow)
    return convert(Vector{typejoin(eltypes(r.df)...)}, r)
end

# TODO:
# Convert to DataVector{T}, DataVector, DataMatrix{T}, DataMatrix

# Should this allow NA's?
# Base.convert(Dict, r::DataFrameRow) = Vector{Any}...
