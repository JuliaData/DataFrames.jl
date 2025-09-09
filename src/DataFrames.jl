module DataFrames

using Base.Sort,
      Base.Order,
      Base.Iterators,
      Base.Threads,
      Statistics,
      Printf,
      Reexport,
      SortingAlgorithms,
      Compat,
      Unicode,
      PooledArrays,
      TableTraits,
      IteratorInterfaceExtensions,
      Markdown,
      PrettyTables,
      Random

@reexport using Missings, InvertedIndices

using Tables: ByRow

import Base.keepat!,
       Base.stack,
       LinearAlgebra.norm,
       PrecompileTools,
       SentinelArrays,
       InlineStrings,
       DataAPI,
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

@static if pkgversion(PrettyTables).major == 2
    # When PrettyTables v2 is more widely adopted in the ecosystem, we can remove this file.
    # In this case, we should also update the compat bounds in Project.toml to list only
    # PrettyTables v3.
    include("abstractdataframe/prettytables_v2.jl")
else
    include("abstractdataframe/prettytables.jl")
end

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
