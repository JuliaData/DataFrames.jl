##############################################################################
##
## We use SubDataFrame's to maintain a reference to a subset of a DataFrame
## without making copies.
##
##############################################################################

struct SubDataFrame{T <: AbstractVector{Int}} <: AbstractDataFrame
    parent::DataFrame
    rows::T # maps from subdf row indexes to parent row indexes

    function SubDataFrame{T}(parent::DataFrame, rows::T) where {T <: AbstractVector{Int}}
        if length(rows) > 0
            rmin, rmax = extrema(rows)
            if rmin < 1 || rmax > size(parent, 1)
                throw(BoundsError())
            end
        end
        new(parent, rows)
    end
end

"""
A view of row subsets of an AbstractDataFrame

A `SubDataFrame` is meant to be constructed with `view`.  A
SubDataFrame is used frequently in split/apply sorts of operations.

```julia
view(d::AbstractDataFrame, rows)
```

### Arguments

* `d` : an AbstractDataFrame
* `rows` : any indexing type for rows, typically an Int,
  AbstractVector{Int}, AbstractVector{Bool}, or a Range

### Notes

A `SubDataFrame` is an AbstractDataFrame, so expect that most
DataFrame functions should work. Such methods include `describe`,
`dump`, `nrow`, `size`, `by`, `stack`, and `join`. Indexing is just
like a DataFrame; copies are returned.

To subset along columns, use standard column indexing as that creates
a view to the columns by default. To subset along rows and columns,
use column-based indexing with `view`.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdf1 = view(df, 1:6)
sdf2 = view(df, df[:a] .> 1)
sdf3 = view(df[[1,3]], df[:a] .> 1)  # row and column subsetting
sdf4 = groupby(df, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame
sdf5 = view(sdf1, 1:3)
sdf1[:,[:a,:b]]
```

"""
SubDataFrame

function SubDataFrame(parent::DataFrame, rows::T) where {T <: AbstractVector{Int}}
    return SubDataFrame{T}(parent, rows)
end

function SubDataFrame(parent::DataFrame, rows::Colon)
    return SubDataFrame(parent, 1:nrow(parent))
end

function SubDataFrame(parent::DataFrame, row::Bool)
    throw(ArgumentError("invalid row index: $row of type `Bool`"))
end

function SubDataFrame(parent::DataFrame, row::Integer)
    Base.depwarn("Creation of `SubDataFrame` with an `Integer` `row` is deprecated. " *
                 "Use `SubDataFrame(parent, [row])` instead.", :SubDataFrame)
    return SubDataFrame(parent, [Int(row)])
end

function SubDataFrame(parent::DataFrame, rows::AbstractVector{<:Integer})
    if any(x -> x isa Bool, rows)
        throw(ArgumentError("invalid row index of type `Bool`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows))
end

function SubDataFrame(parent::DataFrame, rows::AbstractVector{Bool})
    if length(rows) != nrow(parent)
        throw(ArgumentError("invalid length of `AbstractVector{Bool}` row index" *
                            " (got $(length(rows)), expected $(nrow(parent)))"))
    end
    return SubDataFrame(parent, findall(rows))
end

function SubDataFrame(parent::DataFrame, rows::AbstractVector)
    if !all(x -> (x isa Integer) && !(x isa Bool), rows)
        throw(ArgumentError("only `Integer` indices are accepted in `rows`"))
    end
    return SubDataFrame(parent, convert(Vector{Int}, rows))
end

function SubDataFrame(sdf::SubDataFrame, rowinds)
    return SubDataFrame(parent(sdf), rows(sdf)[rowinds])
end

SubDataFrame(sdf::SubDataFrame, rowinds::Colon) = sdf

"""
    parent(sdf::SubDataFrame)

Return the parent data frame of `sdf`.
"""
Base.parent(sdf::SubDataFrame) = getfield(sdf, :parent)

rows(sdf::SubDataFrame) = getfield(sdf, :rows)

# TODO: implement
# `@view df[col]` -> the vector contained in column `col` (this is equivalent to `df[col]`)
# `@view df[cols]` -> a `SubDataFrame` with parent `df` if `cols` is a colon and `df[cols]` otherwise
# `sdf[col]` -> a view of the vector contained in column `col` of `parent(sdf)` with `DataFrames.rows(sdf)` as a selector;
# `sdf[cols]` -> a `SubDataFrame`, with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
# after deprecation period

function Base.view(adf::AbstractDataFrame, rowinds)
    Base.depwarn("`view(adf, x)` will select all rows and columns `x` from `adf` in the future. " *
                 "Use `view(adf, x, :)` to select rows `x` and all columns from `adf` instead.", :view)
    return SubDataFrame(adf, rowinds)
end

function Base.view(adf::AbstractDataFrame, rowind::Integer, colind::ColumnIndex)
    Base.depwarn("`view(adf, rowind, colind)` will create a 0-dimensional view into `adf[colind]` in the future." *
                 " Use `view(adf, [rowind], [colind])` instead.", :view)
    return SubDataFrame(adf[[colind]], [rowind])
end

function Base.view(adf::AbstractDataFrame, rowind::Integer, ::Colon)
    Base.depwarn("`view(adf, rowind, :)` will create a `DataFrameRow` in the future." *
                 " Use `view(adf, [rowind], :)` to create a `SubDataFrame`", :view)
    return SubDataFrame(adf, [rowind])
end

function Base.view(adf::AbstractDataFrame, rowind::Integer, colinds)
    Base.depwarn("`view(adf, rowind, colinds)` will create a `DataFrameRow` in the future." *
                 " Use `view(adf, [rowind], colinds)` to create a `SubDataFrame`", :view)
    return SubDataFrame(adf[colinds], [rowind])
end

function Base.view(adf::AbstractDataFrame, rowinds, colinds)
    return SubDataFrame(adf[colinds], rowinds)
end

function Base.view(adf::AbstractDataFrame, rowinds, ::Colon)
    return SubDataFrame(adf, rowinds)
end

function Base.view(adf::AbstractDataFrame, rowinds, colind::ColumnIndex)
    Base.depwarn("`view(adf, rowinds, col::ColumnIndex)` will create `view(df[col], rowinds)` in the future." *
                 " Use `view(adf, rowinds, [col])` instead.", :view)
    return SubDataFrame(adf[[colind]], rowinds)
end

function Base.view(adf::AbstractDataFrame, rowinds, colind::Bool)
    throw(ArgumentError("invalid column index $colind of type `Bool`"))
end

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdf::SubDataFrame) = index(parent(sdf))

# TODO: Remove these
nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(rows(sdf))::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

function Base.getindex(sdf::SubDataFrame, colind::ColumnIndex)
    Base.depwarn("`sdf[colind]` will create a view of `parent(sdf)[colind]` in the future." *
                 " Use sdf[:, [colind]]` to get a `DataFrame`.", :getindex)
    return parent(sdf)[rows(sdf), colind]
end

function Base.getindex(sdf::SubDataFrame, colinds)
    Base.depwarn("`sdf[colinds]` will create a `SubDataFrame` in the future." *
                 " Use `sdf[:, colinds]` to get a `DataFrame`.", :getindex)
    return parent(sdf)[rows(sdf), colinds]
end

function Base.getindex(sdf::SubDataFrame, rowind::Integer, colind::ColumnIndex)
    return parent(sdf)[rows(sdf)[rowind], colind]
end

function Base.getindex(sdf::SubDataFrame, rowind::Integer, colinds)
    Base.depwarn("Selecting a single row from a `SubDataFrame` will return a `NamedTuple` in the future.", :getindex)
    return parent(sdf)[rows(sdf)[rowind], colinds]
end

function Base.getindex(sdf::SubDataFrame, rowinds, colinds)
    return parent(sdf)[rows(sdf)[rowinds], colinds]
end

function Base.setindex!(sdf::SubDataFrame, val::Any, colinds::Any)
    parent(sdf)[rows(sdf), colinds] = val
    return sdf
end

function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    parent(sdf)[rows(sdf)[rowinds], colinds] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.copy(sdf::SubDataFrame) = sdf[:]

Base.map(f::Function, sdf::SubDataFrame) = f(sdf) # TODO: deprecate

without(sdf::SubDataFrame, c) = view(without(parent(sdf), c), rows(sdf), :)
# Resolve a method ambiguity
without(sdf::SubDataFrame, c::Vector{<:Integer}) = view(without(parent(sdf), c), rows(sdf), :)
