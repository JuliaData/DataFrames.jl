module DataFrames

using Statistics, Printf, REPL
using Reexport, SortingAlgorithms, Compat, Unicode, PooledArrays
@reexport using Missings, InvertedIndices
using Base.Sort, Base.Order, Base.Iterators, Base.Threads
using TableTraits, IteratorInterfaceExtensions
import LinearAlgebra: norm
using Markdown
using PrettyTables

import DataAPI,
       DataAPI.All,
       DataAPI.Between,
       DataAPI.Cols,
       DataAPI.describe,
       DataAPI.innerjoin,
       DataAPI.outerjoin,
       DataAPI.rightjoin,
       DataAPI.leftjoin,
       DataAPI.semijoin,
       DataAPI.antijoin,
       DataAPI.crossjoin,
       DataAPI.nrow,
       DataAPI.ncol,
       Tables,
       Tables.columnindex,
       Future.copy!

export AbstractDataFrame,
       All,
       AsTable,
       Between,
       ByRow,
       Cols,
       DataFrame,
       DataFrameRow,
       GroupedDataFrame,
       SubDataFrame,
       Tables,
       allowmissing!,
       antijoin,
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
       leftjoin!,
       mapcols,
       mapcols!,
       ncol,
       nonunique,
       nrow,
       order,
       outerjoin,
       PrettyTables,
       rename!,
       rename,
       repeat!,
       rightjoin,
       rownumber,
       select!,
       select,
       semijoin,
       stack,
       subset,
       subset!,
       transform,
       transform!,
       unique!,
       unstack,
       valuecols

if VERSION >= v"1.1.0-DEV.792"
    import Base.eachcol, Base.eachrow
else
    import Compat.eachcol, Compat.eachrow
    export eachcol, eachrow
end

if VERSION < v"1.2"
    export hasproperty
end

if isdefined(Base, :only)  # Introduced in 1.4.0
    import Base.only
else
    import Compat.only
    export only
end

if VERSION >= v"1.3"
    using Base.Threads: @spawn
else
    # This is the definition of @async in Base
    macro spawn(expr)
        thunk = esc(:(()->($expr)))
        var = esc(Base.sync_varname)
        quote
            local task = Task($thunk)
            if $(Expr(:isdefined, var))
                push!($var, task)
            end
            schedule(task)
        end
    end
end

if isdefined(Base, :ComposedFunction) # Julia >= 1.6.0-DEV.85
    using Base: ComposedFunction
else
    using Compat: ComposedFunction
end

include("other/utils.jl")
include("other/index.jl")

include("abstractdataframe/abstractdataframe.jl")
include("dataframe/dataframe.jl")
include("subdataframe/subdataframe.jl")
include("dataframerow/dataframerow.jl")
include("groupeddataframe/groupeddataframe.jl")
include("groupeddataframe/utils.jl")

include("other/broadcasting.jl")

include("abstractdataframe/selection.jl")
include("abstractdataframe/selectionfast.jl")
include("abstractdataframe/subset.jl")
include("abstractdataframe/iteration.jl")
include("abstractdataframe/reshape.jl")

include("join/composer.jl")
include("join/core.jl")
include("join/inplace.jl")

include("groupeddataframe/splitapplycombine.jl")
include("groupeddataframe/callprocessing.jl")
include("groupeddataframe/fastaggregates.jl")
include("groupeddataframe/complextransforms.jl")

include("abstractdataframe/prettytables.jl")
include("abstractdataframe/show.jl")
include("groupeddataframe/show.jl")
include("dataframerow/show.jl")
include("abstractdataframe/io.jl")

include("abstractdataframe/sort.jl")

include("other/tables.jl")
include("other/names.jl")

include("deprecated.jl")

include("other/precompile.jl")
precompile()

end # module DataFrames
