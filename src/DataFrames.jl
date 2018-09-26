module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Reexport, StatsBase, SortingAlgorithms, Compat, Statistics, Unicode, Printf
@reexport using CategoricalArrays, Missings
using Base.Sort, Base.Order

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

       allowmissing!,
       aggregate,
       by,
       categorical!,
       colwise,
       combine,
       completecases,
       deleterows!,
       describe,
       disallowmissing!,
       dropmissing,
       dropmissing!,
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
       order,
       rename!,
       rename,
       showcols,
       stack,
       stackdf,
       unique!,
       unstack,
       head,
       tail,
       permutecols!,

       # Remove after deprecation period
       pool,
       pool!


##############################################################################
##
## Load files
##
##############################################################################

include("other/utils.jl")
include("other/index.jl")

include("abstractdataframe/abstractdataframe.jl")
include("dataframe/dataframe.jl")
include("subdataframe/subdataframe.jl")
include("groupeddataframe/grouping.jl")
include("dataframerow/dataframerow.jl")
include("dataframerow/utils.jl")

include("abstractdataframe/iteration.jl")
include("abstractdataframe/join.jl")
include("abstractdataframe/reshape.jl")

include("abstractdataframe/io.jl")

include("abstractdataframe/show.jl")
include("groupeddataframe/show.jl")
include("dataframerow/show.jl")

include("abstractdataframe/sort.jl")
include("dataframe/sort.jl")

include("deprecated.jl")

include("other/tables.jl")

end # module DataFrames
