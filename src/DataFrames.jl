module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Base.Intrinsics
using Reexport
using Compat
@reexport using StatsBase
@reexport using DataArrays
using GZip
using SortingAlgorithms

##############################################################################
##
## Extend methods in Base by default
##
##############################################################################

import Base: Sort, Order
import Base.Sort: sort, sort!, sortperm, Algorithm, defalg, issorted
import Base.Order: Ordering, By, Lt, Perm, Forward, lt, ord
import Base.AsyncStream

##############################################################################
##
## Exported methods and types
##
##############################################################################

export @~,
       AbstractDataFrame,
       aggregate,
       array,
       by,
       cbind,
       coefnames,
       colwise,
       combine,
       complete_cases,
       complete_cases!,
       DataFrame,
       DataFrameRow,
       deleterows!,
       describe,
       eachcol,
       eachrow,
       eltypes,
       flipud!,
       flipud,
       Formula,
       gl,
       GroupApplied,
       groupby,
       GroupedDataFrame,
       interaction_design_matrix,
       loaddf,
       model_response,
       ModelFrame,
       ModelMatrix,
       names!,
       ncol,
       nonunique,
       nrow,
       nullable!,
       order,
       pool,
       pool!,
       printtable,
       rbind,
       readtable,
       rename!,
       rename,
       save,
       showcols,
       stack,
       SubDataFrame,
       subset,
       unique,
       unique!,
       unstack,
       writetable,
       xtab,
       xtabs,
       stackdf,
       melt,
       meltdf,
       pivottable,
       read_rda,
       vecbind,
       push!

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
