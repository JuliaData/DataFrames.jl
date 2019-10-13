module DataFrames

##############################################################################
##
## Dependencies
##
##############################################################################

using Statistics, Printf, REPL
using Reexport, SortingAlgorithms, Compat, Unicode, PooledArrays, DataAPI
@reexport using CategoricalArrays, Missings, InvertedIndices
using Base.Sort, Base.Order, Base.Iterators
using Tables, TableTraits, IteratorInterfaceExtensions

##############################################################################
##
## Exported methods and types (in addition to everything reexported above)
##
##############################################################################

import DataAPI.All,
       DataAPI.Between,
       DataAPI.describe,
       Tables.columnindex,
       Future.copy!

export AbstractDataFrame,
       All,
       Between,
       DataFrame,
       DataFrame!,
       DataFrameRow,
       GroupedDataFrame,
       SubDataFrame,
       allowmissing!,
       aggregate,
       by,
       categorical!,
       columnindex,
       combine,
       completecases,
       deleterows!,
       describe,
       disallowmissing!,
       dropmissing,
       dropmissing!,
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
       select,
       select!,
       stack,
       stackdf,
       unique!,
       unstack

if VERSION >= v"1.1.0-DEV.792"
    import Base.eachcol, Base.eachrow
else
    export eachcol, eachrow
end

if VERSION < v"1.2"
    export hasproperty
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

include("other/broadcasting.jl")

include("abstractdataframe/iteration.jl")
include("abstractdataframe/join.jl")
include("abstractdataframe/reshape.jl")

include("abstractdataframe/show.jl")
include("groupeddataframe/show.jl")
include("dataframerow/show.jl")
include("abstractdataframe/io.jl")

include("abstractdataframe/sort.jl")
include("dataframe/sort.jl")

include("deprecated.jl")

include("other/tables.jl")

end # module DataFrames
