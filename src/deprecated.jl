# TODO: remove these definitions in 1.0 release
by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `combine(groupby(...), ...)` or `combine(f, groupby(...))` instead."))

aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` function instead."))

# TODO: remove these definitions in 1.0 release

import Base: @deprecate
import Base.convert

@deprecate convert(::Type{DataFrame}, d::AbstractDict) DataFrame(d, copycols=false)
@deprecate convert(::Type{Matrix}, df::AbstractDataFrame) Matrix(df)
@deprecate convert(::Type{Matrix{T}}, df::AbstractDataFrame) where T Matrix{T}(df)
@deprecate convert(::Type{Array}, df::AbstractDataFrame) Matrix(df)
@deprecate convert(::Type{Array{T}}, df::AbstractDataFrame) where {T}  Matrix{T}(df)
@deprecate convert(::Type{Vector}, dfr::DataFrameRow) Vector(dfr)
@deprecate convert(::Type{Vector{T}}, dfr::DataFrameRow) where {T} Vector{T}(dfr)
@deprecate convert(::Type{Array}, dfr::DataFrameRow) Vector(dfr)
@deprecate convert(::Type{Array{T}}, dfr::DataFrameRow) where {T} Vector{T}(dfr)
@deprecate convert(::Type{Tuple}, dfr::DataFrameRow) Tuple(dfr)
@deprecate convert(::Type{Vector}, key::GroupKey) Vector(key)
@deprecate convert(::Type{Vector{T}}, key::GroupKey) where T Vector{T}(key)
@deprecate convert(::Type{Array}, key::GroupKey) Vector(key)
@deprecate convert(::Type{Array{T}}, key::GroupKey) where {T} Vector{T}(key)
@deprecate convert(::Type{Tuple}, key::GroupKey) Tuple(key)
