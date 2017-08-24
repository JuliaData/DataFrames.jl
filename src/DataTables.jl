VERSION >= v"0.4.0-dev+6521" && __precompile__(true)

module DataTables

##############################################################################
##
## Dependencies
##
##############################################################################

using Compat
import Compat.String
using Reexport
using StatsBase
import NullableArrays: dropnull, dropnull!
@reexport using NullableArrays
@reexport using CategoricalArrays
using SortingAlgorithms
using Base: Sort, Order
import Base: ==, |>

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

export AbstractDataTable,
       DataTable,
       DataTableRow,
       GroupApplied,
       GroupedDataTable,
       SubDataTable,

       aggregate,
       by,
       categorical!,
       colwise,
       combine,
       completecases,
       deleterows!,
       describe,
       dropnull,
       dropnull!,
       eachcol,
       eachrow,
       eltypes,
       groupby,
       melt,
       meltdt,
       names!,
       ncol,
       nonunique,
       nrow,
       nullable!,
       order,
       printtable,
       rename!,
       rename,
       showcols,
       stack,
       stackdt,
       unique!,
       unstack,
       head,
       tail,

       # Remove after deprecation period
       pool,
       pool!


##############################################################################
##
## Load files
##
##############################################################################

if VERSION < v"0.5.0-dev+2023"
    _displaysize(x...) = Base.tty_size()
else
    const _displaysize = Base.displaysize
end

for (dir, filename) in [
        ("other", "utils.jl"),
        ("other", "index.jl"),

        ("abstractdatatable", "abstractdatatable.jl"),
        ("datatable", "datatable.jl"),
        ("subdatatable", "subdatatable.jl"),
        ("groupeddatatable", "grouping.jl"),
        ("datatablerow", "datatablerow.jl"),
        ("datatablerow", "utils.jl"),

        ("abstractdatatable", "iteration.jl"),
        ("abstractdatatable", "join.jl"),
        ("abstractdatatable", "reshape.jl"),

        ("abstractdatatable", "io.jl"),

        ("abstractdatatable", "show.jl"),
        ("groupeddatatable", "show.jl"),
        ("datatablerow", "show.jl"),

        ("abstractdatatable", "sort.jl"),
        ("datatable", "sort.jl"),

        ("", "deprecated.jl")
    ]

    include(joinpath(dir, filename))
end

end # module DataTables
