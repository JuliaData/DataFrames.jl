"""
    DataFrameRow{<:AbstractDataFrame, Union{Vector{Int}, UnitRange{Int}}}

A view of one row of an AbstractDataFrame.
"""
struct DataFrameRow{T<:AbstractDataFrame, S<:AbstractVector{Int}}
    df::T
    row::Int
    cols::S
    remap::S # inverse of cols, it is of type S for efficiency in most common cases
end

DataFrameRow(df::AbstractDataFrame, row::Bool, cols::S, remap::S) where {S<:AbstractVector{Int}} =
    throw(ArgumentError("invalid index: $row of type Bool"))

DataFrameRow(df::AbstractDataFrame, row::Integer, cols::S, remap::S) where {S<:AbstractVector{Int}} =
    DataFrameRow{typeof(df),S}(df, row, cols, remap)

@inline function DataFrameRow(df::AbstractDataFrame, row::Integer, cols::Vector{Int})
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row"))
    end
    @boundscheck if !checkindex(Bool, axes(df, 2), cols)
        throw(BoundsError("attempt to access a data frame with $(ncol(df)) " *
                          "columns at indices $cols"))
    end
    # we set non-existing mappings to 0
    remap = zeros(Int, ncol(df))
    for (i, col) in enumerate(cols)
        remap[col] > 0 && throw(ArgumentError("duplicate column $col in cols"))
        remap[col] = i
    end
    DataFrameRow(df, row, cols, remap)
end

@inline function DataFrameRow(df::AbstractDataFrame, row::Integer, cols::UnitRange{Int})
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row"))
    end
    @boundscheck if !checkindex(Bool, axes(df, 2), cols)
        throw(BoundsError("attempt to access a data frame with $(ncol(df)) " *
                          "columns at indices $cols"))
    end
    # non existing mappings are either out range or invalid
    remap = 1:last(cols) .- first(cols) .+ 1
    DataFrameRow(df, row, cols, remap)
end

@inline function DataFrameRow(df::AbstractDataFrame, row::Integer, ::Colon)
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row"))
    end
    cols = axes(df, 2)
    DataFrameRow(df, row, cols, cols)
end

@inline DataFrameRow(df::AbstractDataFrame, row::Integer, cols::AbstractVector{Int}) =
    DataFrameRow{typeof(df),Vector{Int}}(df, row, convert(Vector{Int}, cols))
@inline DataFrameRow(df::AbstractDataFrame, row::Integer, cols::AbstractUnitRange{Int}) =
    DataFrameRow{typeof(df),UnitRange{Int}}(df, row, convert(UnitRange{Int}, cols))
@inline DataFrameRow(df::AbstractDataFrame, row::Integer, cols) =
    DataFrameRow(df, row, index(df)[cols])
@inline DataFrameRow(df::AbstractDataFrame, row::Integer, cols::ColumnIndex) =
    throw(ArgumentError("invalid column vector $cols"))

row(r::DataFrameRow) = getfield(r, :row)
Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), getfield(r, :cols))

Base.view(adf::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(adf, rowind, :)
Base.view(sdf::SubDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind], parentindices(sdf)[2])

Base.view(adf::AbstractDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(adf, rowind, colinds)
Base.view(sdf::SubDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind], index(sdf)[colinds])

# Same here. It is impossible to create a DataFrameRow without columns.
Base.getindex(df::DataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(df, rowind, colinds)
Base.getindex(sdf::SubDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind], index(sdf)[colinds])
Base.getindex(df::DataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(df, rowind, :)
Base.getindex(sdf::SubDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind], parentindices(sdf)[2])

getparentcols(r::DataFrameRow, idx::Union{Integer, AbstractVector{<:Integer}}) =
    getfield(r, :cols)[idx]

function getparentcols(r::DataFrameRow, idx::Symbol)
    parentcols = index(parent(r))[idx]
    getfield(r, :remap)[parentcols] == 0 && throw(KeyError("$idx not found"))
    return parentcols
end

getparentcols(r::DataFrameRow, idx::AbstractVector{Symbol}) =
    [getparentcols(r, i) for i in idx]
getparentcols(r::DataFrameRow, ::Colon) = getfield(r, :cols)

Base.getindex(r::DataFrameRow, idx::ColumnIndex) =
    parent(r)[row(r), getparentcols(r, idx)]
Base.getindex(r::DataFrameRow, idxs::Union{AbstractVector{<:Integer},
                                           AbstractVector{Symbol}}) =
    DataFrameRow(parent(r), row(r), getparentcols(r, idxs))
Base.getindex(r::DataFrameRow, ::Colon) = r

Base.setindex!(r::DataFrameRow, value::Any, idx) =
    setindex!(parent(r), value, row(r), getparentcols(r, idx))

Base.names(r::DataFrameRow) = _names(parent(r))[getparentcols(r, :)]

Base.haskey(r::DataFrameRow, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DataFrameRow, key::Integer) = 1 ≤ key ≤ size(r, 1)
function Base.haskey(r::DataFrameRow, key::Symbol)
    haskey(parent(r), key) || return false
    pos = index(parent(r))[key]
    remap = getfield(r, :remap)
    checkbounds(Bool, remap, pos) || return false
    remap[pos] > 0
end

Base.getproperty(r::DataFrameRow, idx::Symbol) = getindex(r, idx)
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = setindex!(r, x, idx)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = names(r)

Base.view(r::DataFrameRow, col::ColumnIndex) =
    view(parent(r)[getparentcols(r, col)], row(r))
Base.view(r::DataFrameRow, cols::AbstractVector) =
    DataFrameRow(parent(r), row(r), getparentcols(r, cols))
Base.view(r::DataFrameRow, ::Colon) = r

Base.size(r::DataFrameRow) = (length(getfield(r, :cols)),)
Base.size(r::DataFrameRow, i) = size(r)[i]
Base.length(r::DataFrameRow) = size(r, 1)
Base.ndims(r::DataFrameRow) = 1
Base.ndims(::Type{<:DataFrameRow}) = 1

Base.lastindex(r::DataFrameRow) = length(r)

Base.iterate(r::DataFrameRow) = iterate(r, 1)

function Base.iterate(r::DataFrameRow, st)
    st > length(r) && return nothing
    return (r[st], st + 1)
end

# Computing the element type requires going over all columns,
# so better let collect() do it only if necessary (widening)
Base.IteratorEltype(::DataFrameRow) = Base.EltypeUnknown()

function Base.convert(::Type{Vector}, dfr::DataFrameRow)
    T = reduce(promote_type, eltypes(parent(dfr)))
    convert(Vector{T}, dfr)
end
Base.convert(::Type{Vector{T}}, dfr::DataFrameRow) where T =
    T[dfr[i] for i in 1:length(dfr)]
Base.Vector(dfr::DataFrameRow) = convert(Vector, dfr)
Base.Vector{T}(dfr::DataFrameRow) where T = convert(Vector{T}, dfr)

Base.keys(r::DataFrameRow) = names(r)
Base.values(r::DataFrameRow) = ntuple(col -> parent(r)[row(r), getparentcols(r, col)], length(r))

"""
    copy(dfr::DataFrameRow)

Convert a `DataFrameRow` to a `NamedTuple`.
"""
Base.copy(r::DataFrameRow) = NamedTuple{Tuple(keys(r))}(values(r))

# hash column element
Base.@propagate_inbounds hash_colel(v::AbstractArray, i, h::UInt = zero(UInt)) = hash(v[i], h)
Base.@propagate_inbounds function hash_colel(v::AbstractCategoricalArray, i, h::UInt = zero(UInt))
    ref = v.refs[i]
    if eltype(v) >: Missing && ref == 0
        hash(missing, h)
    else
        hash(CategoricalArrays.index(v.pool)[ref], h)
    end
end

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
# table columns are passed as a tuple of vectors to ensure type specialization
rowhash(cols::Tuple{AbstractVector}, r::Int, h::UInt = zero(UInt))::UInt =
    hash_colel(cols[1], r, h)
function rowhash(cols::Tuple{Vararg{AbstractVector}}, r::Int, h::UInt = zero(UInt))::UInt
    h = hash_colel(cols[1], r, h)
    rowhash(Base.tail(cols), r, h)
end

Base.hash(r::DataFrameRow, h::UInt = zero(UInt)) =
    rowhash(ntuple(col -> parent(r)[getparentcols(r, col)], length(r)), row(r), h)

function Base.:(==)(r1::DataFrameRow, r2::DataFrameRow)
    names(r1) == names(r2) || return false
    parent(r1) === parent(r2) && row(r1) == row(r2) && return true
    all(((a, b),) -> a == b, zip(r1, r2))
end

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    names(r1) == names(r2) || return false
    parent(r1) === parent(r2) && row(r1) == row(r2) && return true
    all(((a, b),) -> isequal(a, b), zip(r1, r2))
end

# lexicographic ordering on DataFrame rows, missing > !missing
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
    length(r1) == length(r2) ||
        throw(ArgumentError("compared DataFrameRows must have the same number " *
                            "of columns (got $(length(r1)) and $(length(r2)))"))
    all(((a, b),) -> isless(a, b), zip(r1, r2))
    for (a,b) in zip(r1, r2)
        isequal(a, b) || return isless(a, b)
    end
    return false
end
