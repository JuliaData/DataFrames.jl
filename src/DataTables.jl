__precompile__(true)
module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Compat, Reexport, StatsBase, SortingAlgorithms
@reexport using CategoricalArrays, Nulls

using Base: Sort, Order
import Base: ==, |>

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

export AbstractDataFrame,
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

        ("abstractDataFrame", "abstractDataFrame.jl"),
        ("DataFrame", "DataFrame.jl"),
        ("subDataFrame", "subDataFrame.jl"),
        ("groupedDataFrame", "grouping.jl"),
        ("DataFramerow", "DataFramerow.jl"),
        ("DataFramerow", "utils.jl"),

        ("abstractDataFrame", "iteration.jl"),
        ("abstractDataFrame", "join.jl"),
        ("abstractDataFrame", "reshape.jl"),

        ("abstractDataFrame", "io.jl"),

        ("abstractDataFrame", "show.jl"),
        ("groupedDataFrame", "show.jl"),
        ("DataFramerow", "show.jl"),

        ("abstractDataFrame", "sort.jl"),
        ("DataFrame", "sort.jl"),

        ("", "deprecated.jl")
    ]

    include(joinpath(dir, filename))
end

end # module DataFrames
