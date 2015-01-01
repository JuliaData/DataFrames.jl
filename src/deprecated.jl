using Base.depwarn

Base.@deprecate by(d::AbstractDataFrame, cols, s::Vector{Symbol}) aggregate(d, cols, map(eval, s))
Base.@deprecate by(d::AbstractDataFrame, cols, s::Symbol) aggregate(d, cols, eval(s))
Base.@deprecate nullable!(colnames::Array{Symbol,1}, df::AbstractDataFrame) nullable!(df, colnames)
Base.@deprecate nullable!(colnums::Array{Int,1}, df::AbstractDataFrame) nullable!(df, colnums)
import Base: keys, values, insert!
Base.@deprecate keys(df::AbstractDataFrame) names(df)
Base.@deprecate values(df::AbstractDataFrame) DataFrames.columns(df)
Base.@deprecate insert!(df::DataFrame, df2::AbstractDataFrame) merge!(df, df2)
