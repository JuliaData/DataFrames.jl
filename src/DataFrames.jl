# Note that the two calls to using Stats in this file are
# strictly required: one pulls the names into DataFrames
# for easy access within the module, whereas the other call
# pushes those names into Main.
using StatsBase
using DataArrays

module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Base.Intrinsics
using DataArrays
using GZip
using StatsBase
using SortingAlgorithms

##############################################################################
##
## Extend methods in Base by default
##
##############################################################################

importall Base
importall StatsBase
import Base: Sort, Order
import Base.Sort: sort, sort!, Algorithm, defalg, issorted
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
       merge,
       model_response,
       ModelFrame,
       ModelMatrix,
       names!,
       ncol,
       nonunique,
       nrow,
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
       vecbind

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
include(joinpath("formula", "formula.jl"))
include(joinpath("dataframe", "io.jl"))
include("RDA.jl")
include("deprecated.jl")

end # module DataFrames
