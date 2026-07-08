module DataFramesSentinelArraysExt

using DataFrames
using DataFrames: SymbolOrString
using SentinelArrays: ChainedVector

# definition needed to avoid dispatch ambiguity between the `reduce(vcat, ...)`
# methods defined in DataFrames and the one defined in SentinelArrays for
# `ChainedVector`
Base.reduce(::typeof(vcat),
    dfs::Union{ChainedVector{AbstractDataFrame,<:AbstractVector{AbstractDataFrame}},
        ChainedVector{DataFrame,<:AbstractVector{DataFrame}},
        ChainedVector{SubDataFrame,<:AbstractVector{SubDataFrame}},
        ChainedVector{Union{DataFrame,SubDataFrame},<:AbstractVector{Union{DataFrame,SubDataFrame}}}};
    cols::Union{Symbol,AbstractVector{Symbol},
        AbstractVector{<:AbstractString}}=:setequal,
    source::Union{Nothing,SymbolOrString,
        Pair{<:SymbolOrString,<:AbstractVector}}=nothing,
    init::AbstractDataFrame=DataFrame()) =
    reduce(vcat, collect(AbstractDataFrame, dfs), cols=cols, source=source, init=init)

end # module
