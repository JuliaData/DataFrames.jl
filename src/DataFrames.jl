VERSION >= v"0.4.0-dev+6521" && __precompile__(true)

module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Compat
using Reexport
@reexport using StatsBase
@reexport using DataArrays
using GZip
using SortingAlgorithms
using Base: Sort, Order
using Docile

@document

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

       AbstractDataFrame,
       DataFrame,
       DataFrameRow,
       Formula,
       GroupApplied,
       GroupedDataFrame,
       ModelFrame,
       ModelMatrix,
       SubDataFrame,

       aggregate,
       by,
       coefnames,
       colwise,
       combine,
       complete_cases,
       complete_cases!,
       deleterows!,
       describe,
       eachcol,
       eachrow,
       eltypes,
       groupby,
       melt,
       meltdf,
       names!,
       ncol,
       nonunique,
       nrow,
       nullable!,
       order,
       pool,
       pool!,
       printtable,
       read_rda,
       readtable,
       rename!,
       rename,
       showcols,
       stack,
       stackdf,
       unique!,
       unstack,
       writetable

##############################################################################
##
## Load files
##
##############################################################################

for (dir, filename) in [
        ("other", "utils.jl"),
        ("other", "index.jl"),

        ("abstractdataframe", "abstractdataframe.jl"),
        ("dataframe", "dataframe.jl"),
        ("subdataframe", "subdataframe.jl"),
        ("groupeddataframe", "grouping.jl"),
        ("dataframerow", "dataframerow.jl"),

        ("abstractdataframe", "iteration.jl"),
        ("abstractdataframe", "join.jl"),
        ("abstractdataframe", "reshape.jl"),

        ("abstractdataframe", "io.jl"),
        ("dataframe", "io.jl"),

        ("abstractdataframe", "show.jl"),
        ("groupeddataframe", "show.jl"),
        ("dataframerow", "show.jl"),

        ("abstractdataframe", "sort.jl"),
        ("dataframe", "sort.jl"),

        ("statsmodels", "formula.jl"),
        ("statsmodels", "statsmodel.jl"),

        ("", "RDA.jl"),
        ("", "deprecated.jl")
    ]

    include(joinpath(dir, filename))
end

end # module DataFrames
