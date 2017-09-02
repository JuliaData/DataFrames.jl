import Base: @deprecate

@deprecate by(d::AbstractDataFrame, cols, s::Vector{Symbol}) aggregate(d, cols, map(eval, s))
@deprecate by(d::AbstractDataFrame, cols, s::Symbol) aggregate(d, cols, eval(s))
@deprecate nullable!(colnames::Array{Symbol,1}, df::AbstractDataFrame) nullable!(df, colnames)
@deprecate nullable!(colnums::Array{Int,1}, df::AbstractDataFrame) nullable!(df, colnums)

import Base: keys, values, insert!
@deprecate keys(dt::AbstractDataFrame) names(dt)
@deprecate values(dt::AbstractDataFrame) DataFrames.columns(dt)
@deprecate insert!(dt::DataFrame, dt2::AbstractDataFrame) merge!(dt, dt2)

@deprecate pool categorical
@deprecate pool! categorical!

@deprecate complete_cases! dropnull!
@deprecate complete_cases completecases

@deprecate pool categorical
@deprecate pool! categorical!

@deprecate complete_cases! dropnull!
@deprecate complete_cases completecases

@deprecate sub(df::AbstractDataFrame, rows) view(df, rows)

@deprecate stackdf stackdf
@deprecate meltdf meltdf
