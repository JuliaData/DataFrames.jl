import Base: @deprecate

@deprecate by(d::AbstractDataTable, cols, s::Vector{Symbol}) aggregate(d, cols, map(eval, s))
@deprecate by(d::AbstractDataTable, cols, s::Symbol) aggregate(d, cols, eval(s))
@deprecate nullable!(colnames::Array{Symbol,1}, dt::AbstractDataTable) nullable!(dt, colnames)
@deprecate nullable!(colnums::Array{Int,1}, dt::AbstractDataTable) nullable!(dt, colnums)

import Base: keys, values, insert!
@deprecate keys(dt::AbstractDataTable) names(dt)
@deprecate values(dt::AbstractDataTable) DataTables.columns(dt)
@deprecate insert!(dt::DataTable, dt2::AbstractDataTable) merge!(dt, dt2)

@deprecate pool categorical
@deprecate pool! categorical!

@deprecate complete_cases! dropnull!
@deprecate complete_cases completecases

@deprecate sub(dt::AbstractDataTable, rows) view(dt, rows)

@deprecate stackdf stackdt
@deprecate meltdf meltdt
