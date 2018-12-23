"""
    DataFrameRow{<:AbstractDataFrame, <:SubIndex}

A view of one row of an `AbstractDataFrame`.

A `DataFrameRow` is constructed with `view` or `getindex` when one row and a
selection of columns is requested.
It is also returned when iterating the result of the call to the [`eachrow`](@ref) function.

```julia
view(df::AbstractDataFrame, row, cols)
df[row, cols]
```

### Arguments

* `df` : an `AbstractDataFrame`
* `row` : an `Integer` other than `Bool` indicating requested row number
* `cols` : any indexing type for columns, typically
  `AbstractVector{Int}`, `AbstractVector{Bool}` or `AbstractVector{Symbol}` or a colon

### Notes

A `DataFrameRow` supports iteration interface so you can pass it to functions
that expect a collection as an argument.

Indexing is one-dimensional like specifying a column of a `DataFrame`.

It is possible to create a `DataFrameRow` with duplicate columns, but in such case
an error will be thrown when one tries to access some column by name.
"""
struct DataFrameRow{S<:SubIndex}
    df::DataFrame
    colindex::S
    row::Int
end

DataFrameRow(df::DataFrame, colindex::SubIndex, row::Integer) =
    DataFrameRow{typeof(colindex)}(df, colindex, row)

DataFrameRow(df::DataFrame, colindex::SubIndex, row::Bool) =
    throw(ArgumentError("invalid index: $row of type Bool"))

@inline function DataFrameRow(df::DataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row"))
    end
    DataFrameRow(df, SubIndex(index(df), cols), row)
end

@inline function DataFrameRow(sdf::SubDataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(sdf, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(sdf)) " *
                          "rows at index $row"))
    end
    DataFrameRow(parent(sdf),
                 SubIndex(index(parent(sdf)),
                          parentcols(sdf, cols isa Colon ? cols : index(sdf)[cols])),
                 rows(sdf)[row])
end

row(r::DataFrameRow) = getfield(r, :row)
Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), index(r).cols)

Base.view(adf::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(adf, rowind, :)
Base.view(adf::AbstractDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(adf, rowind, colinds)

Base.getindex(df::AbstractDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(df, rowind, colinds)
Base.getindex(df::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(df, rowind, :)

@inline parentcols(r::DataFrameRow, idx::Union{Integer, AbstractVector{<:Integer}}) =
    index(r).cols[idx]

@inline function parentcols(r::DataFrameRow, idx::Symbol)
    parentcol = index(parent(r))[idx]
    @boundscheck lazyremap!(index(r))[parentcol] == 0 && throw(KeyError("$idx not found"))
    return parentcol
end

@inline parentcols(r::DataFrameRow, idx::AbstractVector{Symbol}) =
    [parentcols(r, i) for i in idx]
@inline parentcols(r::DataFrameRow, ::Colon) = index(r).cols

@inline Base.getindex(r::DataFrameRow, idx::ColumnIndex) =
    parent(r)[row(r), parentcols(r, idx)]
@inline Base.getindex(r::DataFrameRow, idxs::Union{AbstractVector{<:Integer},
                                           AbstractVector{Symbol}}) =
    DataFrameRow(parent(r), row(r), parentcols(r, idxs))
@inline Base.getindex(r::DataFrameRow, ::Colon) = r

@inline Base.setindex!(r::DataFrameRow, value::Any, idx) =
    setindex!(parent(r), value, row(r), parentcols(r, idx))

index(r::DataFrameRow) = getfield(r, :colindex)

Base.names(r::DataFrameRow) = _names(parent(r))[parentcols(r, :)]

Base.haskey(r::DataFrameRow, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DataFrameRow, key::Integer) = 1 ≤ key ≤ size(r, 1)
function Base.haskey(r::DataFrameRow, key::Symbol)
    haskey(parent(r), key) || return false
    pos = index(parent(r))[key]
    remap = lazyremap!(r)
    checkbounds(Bool, remap, pos) || return false
    remap[pos] > 0
end

Base.getproperty(r::DataFrameRow, idx::Symbol) = getindex(r, idx)
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = setindex!(r, x, idx)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = names(r)

Base.view(r::DataFrameRow, col::ColumnIndex) =
    view(parent(r)[parentcols(r, col)], row(r))
Base.view(r::DataFrameRow, cols::AbstractVector) =
    DataFrameRow(parent(r), row(r), parentcols(r, cols))
Base.view(r::DataFrameRow, ::Colon) = r

Base.size(r::DataFrameRow) = (length(index(r)),)
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
Base.values(r::DataFrameRow) = ntuple(col -> parent(r)[row(r), parentcols(r, col)], length(r))

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
    rowhash(ntuple(col -> parent(r)[parentcols(r, col)], length(r)), row(r), h)

function Base.:(==)(r1::DataFrameRow, r2::DataFrameRow)
    if parent(r1) === parent(r2)
        index(r1).cols == index(r2).cols || return false
        row(r1) == row(r2) && return true
    else
        names(r1) == names(r2) || return false
    end
    all(((a, b),) -> a == b, zip(r1, r2))
end

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    if parent(r1) === parent(r2)
        index(r1).cols == index(r2).cols || return false
        row(r1) == row(r2) && return true
    else
        names(r1) == names(r2) || return false
    end
    all(((a, b),) -> isequal(a, b), zip(r1, r2))
end

# lexicographic ordering on DataFrame rows, missing > !missing
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
    length(r1) == length(r2) ||
        throw(ArgumentError("compared DataFrameRows must have the same number " *
                            "of columns (got $(length(r1)) and $(length(r2)))"))
    for (a,b) in zip(r1, r2)
        isequal(a, b) || return isless(a, b)
    end
    return false
end
