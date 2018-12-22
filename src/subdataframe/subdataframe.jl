"""
    SubDataFrame{<:AbstractVector{Int}, <:AbstractVector{Int}} <: AbstractDataFrame

A view of row subsets of an `AbstractDataFrame`.

A `SubDataFrame` is meant to be constructed with `view` when a collection of
rows and columns is selected.
A `SubDataFrame` is used frequently in split/apply sorts of operations.

```julia
view(d::AbstractDataFrame, rows, cols)
view(d::AbstractDataFrame, cols)
```

### Arguments

* `d` : an `AbstractDataFrame`
* `rows` : any indexing type for rows, typically
  `AbstractVector{Int}` or `AbstractVector{Bool}`
* `cols` : any indexing type for columns, typically
  `AbstractVector{Int}`, `AbstractVector{Bool}` or `AbstractVector{Symbol}` or a colon

### Notes

A `SubDataFrame` is an `AbstractDataFrame`, so expect that most
DataFrame functions should work. Such methods include `describe`,
`dump`, `nrow`, `size`, `by`, `stack`, and `join`.

Indexing is just like a `DataFrame`.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdf1 = view(df, 2:3) # column subsetting
sdf2 = view(df, df[:a] .> 1, [1,3])  # row and column subsetting
sdf3 = groupby(df, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame
```
"""
struct SubDataFrame{T<:AbstractVector{Int}, S<:AbstractVector{Int}} <: AbstractDataFrame
    parent::DataFrame
    rows::T # maps from subdf row indexes to parent row indexes
    cols::S
    remap::S # inverse of cols, it is of type S for efficiency in most common cases
end

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector{Int}, cols::Vector{Int})
    @boundscheck checkbounds(axes(parent, 1), rows)
    @boundscheck checkbounds(axes(parent, 2), cols)
    # we set non-existing mappings to 0
    remap = zeros(Int, ncol(parent))
    for (i, col) in enumerate(cols)
        remap[col] > 0 && throw(ArgumentError("duplicate column $col in cols"))
        remap[col] = i
    end
    SubDataFrame(parent, rows, cols, remap)
end

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector{Int}, cols::UnitRange{Int})
    @boundscheck checkbounds(axes(parent, 1), rows)
    @boundscheck checkbounds(axes(parent, 2), cols)
    # non existing mappings are either out range or invalid
    remap = (1:last(cols)) .- first(cols) .+ 1
    SubDataFrame(parent, rows, cols, remap)
end

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector{Int}, ::Colon)
    @boundscheck checkbounds(axes(parent, 1), rows)
    cols = axes(parent, 2)
    SubDataFrame(parent, rows, cols, cols)
end

@inline SubDataFrame(parent::DataFrame, rows::T, cols::AbstractVector{Int}) where {T <: AbstractVector{Int}} =
    SubDataFrame(parent, rows, convert(Vector{Int}, cols))
@inline SubDataFrame(parent::DataFrame, rows::T, cols::AbstractUnitRange{Int}) where {T <: AbstractVector{Int}} =
    SubDataFrame(parent, rows, convert(UnitRange{Int}, cols))
@inline SubDataFrame(parent::DataFrame, rows::T, cols) where {T <: AbstractVector{Int}} =
    SubDataFrame(parent, rows, index(parent)[cols])
@inline SubDataFrame(parent::DataFrame, rows::T, cols::ColumnIndex) where {T <: AbstractVector{Int}} =
    throw(ArgumentError("invalid column vector $cols"))
@inline SubDataFrame(parent::DataFrame, ::Colon, cols) =
    SubDataFrame(parent, axes(parent, 1), cols)
@inline SubDataFrame(parent::DataFrame, row::Integer, cols) =
    throw(ArgumentError("invalid row index: $row of type $(typeof(row))"))

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector{<:Integer}, cols)
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector{Bool}, cols)
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index" *
                            " (got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataFrame(parent, findall(rows), cols)
end

@inline function SubDataFrame(parent::DataFrame, rows::AbstractVector, cols)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

getparentcols(sdf::SubDataFrame, idx::Union{Integer, AbstractVector{<:Integer}}) =
    getfield(sdf, :cols)[idx]

function getparentcols(sdf::SubDataFrame, idx::Symbol)
    parentcols = index(parent(sdf))[idx]
    getfield(sdf, :remap)[parentcols] == 0 && throw(KeyError("$idx not found"))
    return parentcols
end

getparentcols(sdf::SubDataFrame, idx::AbstractVector{Symbol}) =
    [getparentcols(sdf, i) for i in idx]

getparentcols(sdf::SubDataFrame, ::Colon) = getfield(sdf, :cols)

SubDataFrame(sdf::SubDataFrame, rowind, cols) =
    SubDataFrame(parent(sdf), rows(sdf)[rowind], getparentcols(sdf, cols))
SubDataFrame(sdf::SubDataFrame, ::Colon, cols) =
    SubDataFrame(parent(sdf), rows(sdf), getparentcols(sdf, cols))
SubDataFrame(sdf::SubDataFrame, ::Colon, ::Colon) = sdf

rows(sdf::SubDataFrame) = getfield(sdf, :rows)
Base.parent(sdf::SubDataFrame) = getfield(sdf, :parent)
Base.parentindices(sdf::SubDataFrame) = (rows(sdf), getfield(sdf, :cols))

Base.view(adf::AbstractDataFrame, colinds) = view(adf, :, colinds)
Base.view(adf::AbstractDataFrame, rowinds, colind::ColumnIndex) =
    view(adf[colind], rowinds)
Base.view(adf::AbstractDataFrame, rowinds, colind::Bool) =
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
Base.view(adf::AbstractDataFrame, rowinds, colinds) =
    SubDataFrame(adf, rowinds, colinds)

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

struct SubIndex{T,S} <: AbstractIndex
    sdf::SubDataFrame{T,S}
end

index(sdf::SubDataFrame{T,S}) where {T,S} = SubIndex{T,S}(sdf)

Base.length(x::SubIndex) = length(getfield(x.sdf, :cols))
Base.names(x::SubIndex) = copy(_names(x))
_names(x::SubIndex) = view(_names(parent(x.sdf)), getfield(x.sdf, :cols))
Base.isequal(x::AbstractIndex, y::AbstractIndex) = _names(x) == _names(y)
Base.:(==)(x::AbstractIndex, y::AbstractIndex) = isequal(x, y)
function Base.haskey(x::SubIndex, key::Symbol)
    haskey(parent(x.sdf), key) || return false
    pos = index(parent(x.sdf))[key]
    remap = getfield(x.sdf, :remap)
    checkbounds(Bool, remap, pos) || return false
    remap[pos] > 0
end
Base.haskey(x::SubIndex, key::Integer) = 1 <= key <= length(x)
Base.haskey(x::SubIndex, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.keys(x::SubIndex) = names(x)

Base.getindex(x::SubIndex, idx::Symbol) =
    getfield(x.sdf, :remap)[index(parent(x.sdf))[idx]]
Base.getindex(x::SubIndex, idx::AbstractVector{Symbol}) = [x[i] for i in idx]

# TODO: Remove these
nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(rows(sdf))::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

Base.getindex(sdf::SubDataFrame, colind::ColumnIndex) =
    view(parent(sdf), rows(sdf), getparentcols(sdf, colind))
Base.getindex(sdf::SubDataFrame, colinds::AbstractVector) =
    SubDataFrame(parent(sdf), rows(sdf), getparentcols(sdf, colinds))
Base.getindex(sdf::SubDataFrame, ::Colon) = sdf
Base.getindex(sdf::SubDataFrame, rowind::Integer, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowind], getparentcols(sdf, colind)]
Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], getparentcols(sdf, colind)]
Base.getindex(sdf::SubDataFrame, ::Colon, colind::ColumnIndex) =
    parent(sdf)[rows(sdf), getparentcols(sdf, colind)]
Base.getindex(sdf::SubDataFrame, ::Colon, colinds::AbstractVector) =
    parent(sdf)[rows(sdf), getparentcols(sdf, colinds)]
Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, colinds::AbstractVector) =
    parent(sdf)[rows(sdf)[rowinds], getparentcols(sdf, colinds)]
Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, ::Colon) =
    parent(sdf)[rows(sdf)[rowinds], getparentcols(sdf, :)]
Base.getindex(sdf::SubDataFrame, ::Colon, ::Colon) =
    parent(sdf)[rows(sdf), getparentcols(sdf, :)]

function Base.setindex!(sdf::SubDataFrame, val::Any, colinds::Any)
    parent(sdf)[rows(sdf), getparentcols(sdf, colinds)] = val
    return sdf
end

function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    parent(sdf)[rows(sdf)[rowinds], getparentcols(sdf, colinds)] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sdf::SubDataFrame) = parent(sdf)[rows(sdf), getparentcols(sdf, :)]

deleterows!(df::SubDataFrame, ind) =
    throw(ArgumentError("SubDataFrame does not support deleting rows"))
