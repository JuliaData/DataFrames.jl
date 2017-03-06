__precompile__()

module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

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
       @formula,
       @tsv_str,
       @wsv_str,

       AbstractDataFrame,
       DataFrame,
       DataFrameRow,
       GroupApplied,
       GroupedDataFrame,
       SubDataFrame,

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
       head,
       melt,
       meltdf,
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
       stackdf,
       tail,
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

for (dir, filename) in [
        ("other", "utils.jl"),
        ("other", "index.jl"),

        ("abstractdataframe", "abstractdataframe.jl"),
        ("dataframe", "dataframe.jl"),
        ("subdataframe", "subdataframe.jl"),
        ("groupeddataframe", "grouping.jl"),
        ("dataframerow", "dataframerow.jl"),
        ("dataframerow", "utils.jl"),

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

        ("", "deprecated.jl")
    ]

    include(joinpath(dir, filename))
end

end # module DataFrames
