module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Statistics, Printf, REPL
using Reexport, StatsBase, SortingAlgorithms, Compat, Unicode, PooledArrays
@reexport using CategoricalArrays, Missings
using Base.Sort, Base.Order, Base.Iterators

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

export AbstractDataFrame,
       DataFrame,
       DataFrameRow,
       GroupedDataFrame,
       SubDataFrame,

       allowmissing!,
       aggregate,
       by,
       categorical!,
       colwise,
       combine,
       completecases,
       deletecols!,
       deleterows!,
       describe,
       disallowmissing!,
       dropmissing,
       dropmissing!,
       eltypes,
       groupby,
       groupindices,
       groupvars,
       insertcols!,
       mapcols,
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
       permutecols!

if VERSION >= v"1.1.0-DEV.792"
    import Base.eachcol, Base.eachrow
else
    export eachcol, eachrow
end

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
include("dataframerow/dataframerow.jl")
include("groupeddataframe/grouping.jl")
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
