module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Base.Intrinsics
using Reexport
@reexport using StatsBase
@reexport using DataArrays
using GZip
using SortingAlgorithms

##############################################################################
##
## Extend methods in Base by default
##
##############################################################################

import Base: Sort, Order, push!
import Base.Sort: sort, sort!, sortperm, sortby, sortby!, Algorithm, defalg, issorted
import Base.Order: Ordering, By, Lt, Perm, Forward, lt, ord
import Base.AsyncStream

##############################################################################
##
## Exported methods and types
##
##############################################################################

export @~,
       AbstractDataFrame,
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
       matrix,
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
       vector,
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
include(joinpath("dataframerow", "dataframerow.jl"))
include(joinpath("dataframe", "sort.jl"))
include(joinpath("dataframe", "iteration.jl"))
include(joinpath("dataframe", "show.jl"))
include(joinpath("dataframe", "join.jl"))
include(joinpath("groupeddataframe", "grouping.jl"))
include(joinpath("dataframe", "reshape.jl"))
include(joinpath("statsmodels", "formula.jl"))
include(joinpath("statsmodels", "statsmodel.jl"))
include(joinpath("dataframe", "io.jl"))
include("RDA.jl")
include("deprecated.jl")

end # module DataFrames
