# Note that the two calls to using Stats in this file are
# strictly required: one pulls the names into DataFrames
# for easy access within the module, whereas the other call
# pushes those names into Main.
using Stats
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
using Stats
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
importall Stats
import Base: Sort, Order
import Base.Sort: sort, sort!, Algorithm, defalg, issorted
import Base.Order: Ordering, By, Lt, Perm, Forward, lt, ord
import Base.AsyncStream

##############################################################################
##
## Exported methods and types
##
##############################################################################

export # reconcile_groups,
       @DataFrame,
       @transform,
       AbstractDataFrame,
       AbstractIndex,
       array,
       based_on,
       by,
       cbind,
       clean_colnames!,
       coefnames,
       colffts,
       colmaxs,
       colmeans,
       colmedians,
       colmins,
       colnames!,
       colnames,
       colnorms,
       colprods,
       colranges,
       colstds,
       colsums,
       coltypes,
       colvars,
       colwise,
       combine,
       complete_cases,
       complete_cases!,
       cut,
       DataFrame,
       DataStream,
       describe,
       dict,
       drop_duplicates!,
       duplicated,
       EachCol,
       EachRow,
       findat,
       flipud!,
       flipud,
       Formula,
       get_groups,
       gl,
       GroupApplied,
       groupby,
       GroupedDataFrame,
       Index,
       index,
       IndexedVector,
       Indexer,
       interaction_design_matrix,
       is_group,
       letters,
       LETTERS,
       load_df,
       matrix,
       merge,
       model_response,
       ModelFrame,
       ModelMatrix,
       nafilter,
       naFilter,
       NamedArray,
       names!,
       nareplace,
       naReplace,
       nas,
       ncol,
       nrow,
       order,
       paste,
       percent_change,
       pool,
       pool!,
       PooledDataVecs, # The capitalization and/or name for this is a bit inconsistent (merge_pools, maybe?). Do we want to export?
       printtable,
       range,
       rbind,
       read_minibatch,
       readtable,
       reldiff,
       rename_group!,
       rep,
       rename!,
       rename,
       rowffts,
       rowmaxs,
       rowmeans,
       rowmedians,
       rowmins,
       rownorms,
       rowprods,
       rowranges,
       rowstds,
       rowsums,
       rowvars,
       save,
       set_group,
       set_groups,
       SimpleIndex,
       stack,
       SubDataFrame,
       subset,
       table,
       unique,
       unstack,
       vector,
       with,
       within!,
       within,
       without,
       writetable,
       xtab,
       xtabs,
       stack_df,
       StackedVector,
       RepeatedVector,
       EachRepeatedVector,
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

include("utils.jl")
include("index.jl")
include("namedarray.jl")
include("dataframe.jl")
include("merge.jl")
include("grouping.jl")
include("reshape.jl")
include("formula.jl")
include("io.jl")
include("datastream.jl")
include("operators.jl")
include("indexing.jl")
include("extras.jl")
include("RDA.jl")
include("dataframe_blocks.jl")

##############################################################################
##
## Deprecations
##
##############################################################################

Base.@deprecate read_table readtable
Base.@deprecate print_table printtable
Base.@deprecate write_table writetable
Base.@deprecate merge(df1::AbstractDataFrame, df2::AbstractDataFrame; on::Any = nothing, kind::Symbol = :inner) Base.join

end # module DataFrames
