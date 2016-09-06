import Base: @deprecate

@deprecate by(d::AbstractDataFrame, cols, s::Vector{Symbol}) aggregate(d, cols, map(eval, s))
@deprecate by(d::AbstractDataFrame, cols, s::Symbol) aggregate(d, cols, eval(s))
@deprecate nullable!(colnames::Array{Symbol,1}, df::AbstractDataFrame) nullable!(df, colnames)
@deprecate nullable!(colnums::Array{Int,1}, df::AbstractDataFrame) nullable!(df, colnums)
import Base: keys, values, insert!
@deprecate keys(df::AbstractDataFrame) names(df)
@deprecate values(df::AbstractDataFrame) DataFrames.columns(df)
@deprecate insert!(df::DataFrame, df2::AbstractDataFrame) merge!(df, df2)

import DataArrays: array, DataArray
@deprecate array(df::AbstractDataFrame) convert(Array, df)
@deprecate array(r::DataFrameRow) convert(Array, r)
if VERSION < v"0.4.0-"
    @deprecate DataArray(df::AbstractDataFrame) convert(DataArray, df)
end
@deprecate DataArray(df::AbstractDataFrame, T::DataType) convert(DataArray{T}, df)

@deprecate read_rda(args...) FileIO.load(args...)
