"""
    SubDataFrame{<:AbstractDataFrame,<:AbstractIndex,<:AbstractVector{Int}} <: AbstractDataFrame

A view of row subsets of an `AbstractDataFrame`.

A `SubDataFrame` is meant to be constructed with `view` when a collection of
rows and columns is selected.
It is also created by some methods of [`by`](@ref) and [`combine`](@ref).

### Arguments

* `d` : an `AbstractDataFrame`
* `rows` : any indexing type for rows, typically
  `AbstractVector{Int}` or `AbstractVector{Bool}`
* `cols` : any indexing type for columns, typically
  a vector of `Int`, `Bool` or `Symbol` or a colon

### Notes

A `SubDataFrame` is an `AbstractDataFrame`, so expect that most
DataFrame functions should work. Such methods include `describe`,
`dump`, `nrow`, `size`, `by`, `stack`, and `join`.

Indexing is just like a `DataFrame` except that it is possible to create a
`SubDataFrame` with duplicate columns. All such columns will have a reference
to the same entry in the parent `DataFrame`.

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
# We allow D to be AbstractDataFrame, to allow for extensions
# In DataFrames.jl D is always DataFrame
struct SubDataFrame{D<:AbstractDataFrame,S<:AbstractIndex,T<:AbstractVector{Int}} <: AbstractDataFrame
    parent::D
    colindex::S
    rows::T # maps from subdf row indexes to parent row indexes
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{Int}, cols)
    @boundscheck if !checkindex(Bool, axes(parent, 1), rows)
        throw(BoundsError("attempt to access a data frame with $(nrow(parent)) " *
                          "rows at indices $rows"))
    end
    SubDataFrame(parent, SubIndex(index(parent), cols), rows)
end
Base.@propagate_inbounds SubDataFrame(parent::DataFrame, ::Colon, cols) =
    SubDataFrame(parent, axes(parent, 1), cols)
@inline SubDataFrame(parent::DataFrame, row::Integer, cols) =
    throw(ArgumentError("invalid row index: $row of type $(typeof(row))"))

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{<:Integer}, cols)
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector{Bool}, cols)
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index" *
                            " (got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataFrame(parent, findall(rows), cols)
end

Base.@propagate_inbounds function SubDataFrame(parent::DataFrame, rows::AbstractVector, cols)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows), cols)
end

Base.@propagate_inbounds parentcols(sdf::SubDataFrame, idx::Union{Integer, AbstractVector{<:Integer}}) =
    parentcols(index(sdf))[idx]

Base.@propagate_inbounds function parentcols(sdf::SubDataFrame, idx::Symbol)
    parentcol = index(parent(sdf))[idx]
    @boundscheck if index(sdf) isa SubIndex
        remap = index(sdf).remap
        length(remap) == 0 && lazyremap!(index(sdf))
        remap[parentcol] == 0 && throw(KeyError("$idx not found"))
    end
    return parentcol
end

Base.@propagate_inbounds parentcols(sdf::SubDataFrame, idx::AbstractVector{Symbol}) =
    [parentcols(sdf, i) for i in idx]

Base.@propagate_inbounds parentcols(sdf::SubDataFrame, ::Colon) = parentcols(index(sdf))

Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind, cols) =
    SubDataFrame(parent(sdf), rows(sdf)[rowind], parentcols(sdf, cols))
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, rowind, ::Colon) =
    SubDataFrame(parent(sdf), rows(sdf)[rowind],
                 index(sdf) isa Index ? Colon() : parentcols(sdf, :))
Base.@propagate_inbounds SubDataFrame(sdf::SubDataFrame, ::Colon, cols) =
    SubDataFrame(parent(sdf), rows(sdf), parentcols(sdf, cols))
@inline SubDataFrame(sdf::SubDataFrame, ::Colon, ::Colon) = sdf

rows(sdf::SubDataFrame) = getfield(sdf, :rows)
Base.parent(sdf::SubDataFrame) = getfield(sdf, :parent)
Base.parentindices(sdf::SubDataFrame) = (rows(sdf), parentcols(index(sdf)))

Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, colinds) = view(adf, :, colinds)
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds, colind::ColumnIndex) =
    view(adf[colind], rowinds)
@inline Base.view(adf::AbstractDataFrame, rowinds, colind::Bool) =
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowinds, colinds) =
    SubDataFrame(adf, rowinds, colinds)

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdf::SubDataFrame) = getfield(sdf, :colindex)

# TODO: Remove these
nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(rows(sdf))::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, colind::ColumnIndex) =
    view(parent(sdf), rows(sdf), parentcols(sdf, colind))
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, colinds::AbstractVector) =
    SubDataFrame(parent(sdf), rows(sdf), parentcols(sdf, colinds))
@inline Base.getindex(sdf::SubDataFrame, ::Colon) = sdf
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowind::Integer, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowind], parentcols(sdf, colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, colind::ColumnIndex) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(sdf, colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::Colon, colind::ColumnIndex) =
    parent(sdf)[rows(sdf), parentcols(sdf, colind)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::Colon, colinds::AbstractVector) =
    parent(sdf)[rows(sdf), parentcols(sdf, colinds)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, colinds::AbstractVector) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(sdf, colinds)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, rowinds::AbstractVector, ::Colon) =
    parent(sdf)[rows(sdf)[rowinds], parentcols(sdf, :)]
Base.@propagate_inbounds Base.getindex(sdf::SubDataFrame, ::Colon, ::Colon) =
    parent(sdf)[rows(sdf), parentcols(sdf, :)]

Base.@propagate_inbounds function Base.setindex!(sdf::SubDataFrame, val::Any, colinds::Any)
    parent(sdf)[rows(sdf), parentcols(sdf, colinds)] = val
    return sdf
end

Base.@propagate_inbounds function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    parent(sdf)[rows(sdf)[rowinds], parentcols(sdf, colinds)] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sdf::SubDataFrame) = parent(sdf)[rows(sdf), parentcols(sdf, :)]

function without(df::SubDataFrame, icols::Vector{<:Integer})
    newcols = setdiff(1:ncol(df), icols)
    view(df, newcols)
end
deleterows!(df::SubDataFrame, ind) =
    throw(ArgumentError("SubDataFrame does not support deleting rows"))
