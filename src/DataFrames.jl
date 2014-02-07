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
## Global constants
##
##############################################################################

const DEFAULT_COLUMN_TYPE = Float64

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
       crossjoin,
       cut,
       DataFrame,
       DataFrameRow,
       describe,
       drop_duplicates!,
       duplicated,
       eachcol,
       eachrow,
       findat,
       flipud!,
       flipud,
       Formula,
       gl,
       GroupApplied,
       groupby,
       GroupedDataFrame,
       interaction_design_matrix,
       load_df,
       matrix,
       merge,
       model_response,
       ModelFrame,
       ModelMatrix,
       names!,
       ncol,
       nrow,
       order,
       pool,
       pool!,
       printtable,
       range,
       rbind,
       readtable,
       rename!,
       rename,
       save,
       stack,
       SubDataFrame,
       subset,
       types,
       unique,
       unstack,
       vector,
       writetable,
       xtab,
       xtabs,
       stack_df,
       melt,
       melt_df,
       pivot_table,
       RComplex,     # Vector{Complex128}
       RInteger,     # Vector{Int32} plus BitVector of NA indicators
       RLogical,     # BitVector of values and BitVector of NA indicators
       RNumeric,     # Vector{Float64}
       RList,        # Vector{Any}
       RString,      # Vector{ASCIIString}
       RSymbol,      # Symbol stored as an String b/c of embedded '.'

       class,                              # in the S3 sense of "class"
       inherits,
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
