import Base: @deprecate

@deprecate by(d::AbstractDataTable, cols, s::Vector{Symbol}) aggregate(d, cols, map(eval, s))
@deprecate by(d::AbstractDataTable, cols, s::Symbol) aggregate(d, cols, eval(s))
@deprecate nullable!(colnames::Array{Symbol,1}, df::AbstractDataTable) nullable!(df, colnames)
@deprecate nullable!(colnums::Array{Int,1}, df::AbstractDataTable) nullable!(df, colnums)

import Base: keys, values, insert!
@deprecate keys(df::AbstractDataTable) names(df)
@deprecate values(df::AbstractDataTable) DataTables.columns(df)
@deprecate insert!(df::DataTable, df2::AbstractDataTable) merge!(df, df2)

@deprecate read_rda(args...) FileIO.load(args...)

@deprecate pool categorical
@deprecate pool! categorical!
