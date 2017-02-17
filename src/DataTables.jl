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
@reexport using StatsBase
import NullableArrays: dropnull, dropnull!
@reexport using NullableArrays
@reexport using CategoricalArrays
using GZip
using SortingAlgorithms

using FileIO  # remove after read_rda deprecation period

using Base: Sort, Order
import Base: ==, |>

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

export @~,
       @csv_str,
       @csv2_str,
       @tsv_str,
       @wsv_str,

       AbstractDataTable,
       AbstractContrasts,
       DataTable,
       DataTableRow,
       Formula,
       GroupApplied,
       GroupedDataTable,
       ModelFrame,
       ModelMatrix,
       SubDataTable,
       EffectsCoding,
       DummyCoding,
       HelmertCoding,
       ContrastsCoding,

       aggregate,
       by,
       categorical!,
       coefnames,
       colwise,
       combine,
       completecases,
       setcontrasts!,
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
       readtable,
       rename!,
       rename,
       showcols,
       stack,
       stackdt,
       unique!,
       unstack,
       writetable,
       head,
       tail,

       # Remove after deprecation period
       read_rda,
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

        ("abstractdatatable", "iteration.jl"),
        ("abstractdatatable", "join.jl"),
        ("abstractdatatable", "reshape.jl"),

        ("abstractdatatable", "io.jl"),
        ("datatable", "io.jl"),

        ("abstractdatatable", "show.jl"),
        ("groupeddatatable", "show.jl"),
        ("datatablerow", "show.jl"),

        ("abstractdatatable", "sort.jl"),
        ("datatable", "sort.jl"),

        ("statsmodels", "contrasts.jl"),
        ("statsmodels", "formula.jl"),
        ("statsmodels", "statsmodel.jl"),

        ("", "deprecated.jl")
    ]

    include(joinpath(dir, filename))
end

end # module DataTables
