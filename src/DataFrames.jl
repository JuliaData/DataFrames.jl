require("Options")

module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Base.Intrinsics
using OptionsMod

##############################################################################
##
## Global constants
##
##############################################################################

const DEFAULT_COLUMN_TYPE = Float64
const POOLED_DATA_VEC_REF_TYPE = Uint16
const POOLED_DATA_VEC_REF_CONVERTER = uint16

##############################################################################
##
## Overwritten and/or extended methods
##
##############################################################################

importall Base

##############################################################################
##
## Exported methods and types
##
##############################################################################

export # reconcile_groups,
       @DataFrame,
       @transform,
       AbstractDataArray,
       AbstractDataFrame,
       AbstractDataVector,
       AbstractIndex,
       anyna,
       array,
       based_on,
       between,
       by,
       cbind,
       clean_colnames!,
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
       DataArray,
       databool,
       datadiagm,
       dataeye,
       datafalses,
       datafloat,
       DataFrame,
       dataint,
       DataMatrix,
       dataones,
       DataStream,
       datatrues,
       DataVector,
       datazeros,
       describe,
       drop_duplicates!,
       duplicated,
       each_failNA,
       each_removeNA,
       each_replaceNA,
       EachCol,
       EachRow,
       failNA,
       flipud!,
       flipud,
       Formula,
       get_groups,
       get_indices,
       gl,
       GroupApplied,
       groupby,
       GroupedDataFrame,
       head,
       in,
       Index,
       index,
       index_to_level,
       IndexedVector,
       Indexer,
       interaction_design_matrix,
       is_group,
       isna,
       letters,
       LETTERS,
       level_to_index,
       levels,
       load_df,
       matrix,
       merge,
       model_frame,
       model_matrix,
       ModelFrame,
       ModelMatrix,
       NA,
       NAException,
       nafilter,
       naFilter,
       NamedArray,
       names!,
       nareplace,
       naReplace,
       nas,
       NAtype,
       ncol,
       nrow,
       padNA,
       pdatafalses,
       pdataones,
       pdatatrues,
       pdatazeros,
       percent_change,
       PooledDataVecs, # The capitalization and/or name for this is a bit inconsistent (merge_pools, maybe?). Do we want to export?
       PooledDataArray,
       PooledDataMatrix,
       PooledDataVector,
       print_table,
       range,
       rbind,
       read_minibatch,
       read_table,
       reldiff,
       removeNA,
       rename_group!,
       replace!,
       replace_names!,
       replace_names,
       replaceNA,
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
       tail,
       unique,
       unstack,
       vector,
       with,
       within!,
       within,
       without,
       write_table,
       xtab,
       xtabs

##############################################################################
##
## Load files
##
##############################################################################

include(joinpath(julia_pkgdir(), "DataFrames", "src", "utils.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "natype.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "dataarray.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "datavector.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "datamatrix.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "pooleddataarray.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "index.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "namedarray.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "dataframe.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "grouping.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "formula.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "io.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "datastream.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "linalg.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "operators.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "statistics.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "predicates.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "indexing.jl"))
include(joinpath(julia_pkgdir(), "DataFrames", "src", "extras.jl"))

# TODO: Remove these definitions
nafilter(x...) = error("Function removed. Please use removeNA")
nareplace(x...) = error("Function removed. Please use replaceNA")
naFilter(x...) = error("Function removed. Please use each_removeNA")
naReplace(x...) = error("Function removed. Please use each_replaceNA")

end # module DataFrames
