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

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

export @~,

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
       loaddf,
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
       save,
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

include(joinpath("other", "utils.jl"))
include(joinpath("other", "index.jl"))

include(joinpath("abstractdataframe", "abstractdataframe.jl"))
include(joinpath("dataframe", "dataframe.jl"))
include(joinpath("subdataframe", "subdataframe.jl"))
include(joinpath("groupeddataframe", "grouping.jl"))
include(joinpath("dataframerow", "dataframerow.jl"))

include(joinpath("abstractdataframe", "io.jl"))
include(joinpath("abstractdataframe", "iteration.jl"))
include(joinpath("abstractdataframe", "join.jl"))
include(joinpath("abstractdataframe", "reshape.jl"))
include(joinpath("abstractdataframe", "show.jl"))
include(joinpath("abstractdataframe", "sort.jl"))
include(joinpath("dataframe", "io.jl"))
include(joinpath("dataframe", "sort.jl"))
include(joinpath("groupeddataframe", "show.jl"))
include(joinpath("dataframerow", "show.jl"))

include(joinpath("statsmodels", "formula.jl"))
include(joinpath("statsmodels", "statsmodel.jl"))
include("RDA.jl")
include("deprecated.jl")

end # module DataFrames
