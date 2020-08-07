module DataFrames

using Statistics, Printf, REPL
using Reexport, SortingAlgorithms, Compat, Unicode, PooledArrays
@reexport using CategoricalArrays, Missings, InvertedIndices
using Base.Sort, Base.Order, Base.Iterators
using TableTraits, IteratorInterfaceExtensions

import DataAPI,
       DataAPI.All,
       DataAPI.Between,
       DataAPI.describe,
       Tables,
       Tables.columnindex,
       Future.copy!

export AbstractDataFrame,
       All,
       AsTable,
       Between,
       ByRow,
       DataFrame,
       DataFrameRow,
       GroupedDataFrame,
       SubDataFrame,
       Tables,
       allowmissing!,
       antijoin,
       by,
       categorical!,
       columnindex,
       combine,
       completecases,
       crossjoin,
       describe,
       disallowmissing!,
       dropmissing!,
       dropmissing,
       flatten,
       groupby,
       groupindices,
       groupcols,
       innerjoin,
       insertcols!,
       leftjoin,
       mapcols,
       mapcols!,
       ncol,
       nonunique,
       nrow,
       order,
       outerjoin,
       rename!,
       rename,
       repeat!,
       rightjoin,
       select!,
       select,
       semijoin,
       stack,
       transform,
       transform!,
       unique!,
       unstack,
       valuecols

# TODO: remove these exports in year 2021
export by, aggregate

if VERSION >= v"1.1.0-DEV.792"
    import Base.eachcol, Base.eachrow
else
    import Compat.eachcol, Compat.eachrow
    export eachcol, eachrow
end

if VERSION < v"1.2"
    export hasproperty
end

include("other/utils.jl")
include("other/index.jl")

include("abstractdataframe/abstractdataframe.jl")
include("dataframe/dataframe.jl")
include("subdataframe/subdataframe.jl")
include("dataframerow/dataframerow.jl")
include("groupeddataframe/groupeddataframe.jl")
include("dataframerow/utils.jl")

include("other/broadcasting.jl")

include("abstractdataframe/selection.jl")
include("abstractdataframe/iteration.jl")
include("abstractdataframe/join.jl")
include("abstractdataframe/reshape.jl")

include("groupeddataframe/splitapplycombine.jl")

include("abstractdataframe/show.jl")
include("groupeddataframe/show.jl")
include("dataframerow/show.jl")
include("abstractdataframe/io.jl")

include("abstractdataframe/sort.jl")
include("dataframe/sort.jl")

include("deprecated.jl")

include("other/tables.jl")

end # module DataFrames
