module DataFrames

using Statistics, Printf
using Reexport, SortingAlgorithms, Compat, Unicode, PooledArrays
@reexport using Missings, InvertedIndices
using Base.Sort, Base.Order, Base.Iterators, Base.Threads
using TableTraits, IteratorInterfaceExtensions
import LinearAlgebra: norm
using Markdown
using PrettyTables
using Random
using Tables: ByRow
import PrecompileTools
import SentinelArrays
import InlineStrings

import DataAPI,
       DataAPI.allcombinations,
       DataAPI.All,
       DataAPI.Between,
       DataAPI.Cols,
       DataAPI.describe,
       DataAPI.groupby,
       DataAPI.innerjoin,
       DataAPI.outerjoin,
       DataAPI.rightjoin,
       DataAPI.leftjoin,
       DataAPI.semijoin,
       DataAPI.antijoin,
       DataAPI.crossjoin,
       DataAPI.nrow,
       DataAPI.ncol,
       DataAPI.metadata,
       DataAPI.metadatakeys,
       DataAPI.metadata!,
       DataAPI.deletemetadata!,
       DataAPI.emptymetadata!,
       DataAPI.colmetadata,
       DataAPI.colmetadatakeys,
       DataAPI.colmetadata!,
       DataAPI.deletecolmetadata!,
       DataAPI.emptycolmetadata!,
       DataAPI.rownumber,
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
       allcombinations,
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
       fillcombinations,
       flatten,
       groupby,
       groupindices,
       groupcols,
       innerjoin,
       insertcols,
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
       proprow,
       rename!,
       rename,
       repeat!,
       rightjoin,
       rownumber,
       select!,
       select,
       semijoin,
       subset,
       subset!,
       transform,
       transform!,
       unique!,
       unstack,
       valuecols,
       metadata,
       metadatakeys,
       metadata!,
       deletemetadata!,
       emptymetadata!,
       colmetadata,
       colmetadatakeys,
       colmetadata!,
       deletecolmetadata!,
       emptycolmetadata!

using Base.Threads: @spawn
using Base: ComposedFunction

if isdefined(Base, :keepat!)  # Introduced in 1.7.0
    import Base.keepat!
else
    import Compat.keepat!
    export keepat!
end

if VERSION >= v"1.9.0-DEV.1163"
    import Base: stack
else
    import Compat: stack
    export stack
end

const METADATA_FIXED =
    """
    Metadata: this function preserves table-level and column-level `:note`-style metadata.
    """

include("other/utils.jl")
include("other/index.jl")

include("abstractdataframe/abstractdataframe.jl")
include("abstractdataframe/unique.jl")
include("dataframe/dataframe.jl")
include("subdataframe/subdataframe.jl")
include("dataframerow/dataframerow.jl")
include("dataframe/insertion.jl")

include("abstractdataframe/sort.jl")

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

include("other/tables.jl")
include("other/names.jl")
include("other/metadata.jl")

include("deprecated.jl")

include("other/precompile.jl")

end # module DataFrames
