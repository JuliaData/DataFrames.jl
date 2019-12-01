module DataFrames

using Statistics, Printf, REPL
using SortingAlgorithms, Compat, Unicode, PooledArrays, DataAPI,
      CategoricalArrays, Missings, InvertedIndices
using Base.Sort, Base.Order, Base.Iterators
using Tables, TableTraits, IteratorInterfaceExtensions

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
       Not,
       SubDataFrame,
       allowmissing,
       allowmissing!,
       aggregate,
       by,
       categorical,
       categorical!,
       columnindex,
       combine,
       completecases,
       deleterows!,
       describe,
       disallowmissing,
       disallowmissing!,
       dropmissing,
       dropmissing!,
       flatten,
       groupby,
       groupindices,
       groupvars,
       insertcols!,
       mapcols,
       melt,
       meltdf,
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
