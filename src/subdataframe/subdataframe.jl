##############################################################################
##
## We use SubDataFrame's to maintain a reference to a subset of a DataFrame
## without making copies.
##
##############################################################################

include_string("""
        immutable SubDataFrame{T <: AbstractVector{Int}} <: AbstractDataFrame
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
    """)

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

function SubDataFrame(parent::DataFrame, row::Integer)
    return SubDataFrame(parent, [Int(row)])
end

function SubDataFrame(parent::DataFrame, rows::AbstractVector{<:Integer})
    return SubDataFrame(parent, convert(Vector{Int}, rows))
end

function SubDataFrame(parent::DataFrame, rows::AbstractVector{Bool})
    return SubDataFrame(parent, find(rows))
end

function SubDataFrame(sdf::SubDataFrame, rowinds::Union{T, AbstractVector{T}}) where {T <: Integer}
    return SubDataFrame(sdf.parent, sdf.rows[rowinds])
end

function Base.view(adf::AbstractDataFrame, rowinds::AbstractVector{T}) where {T >: Null}
    # Vector{>:Null} need to be checked for nulls
    any(isnull, rowinds) && throw(NullException())
    return SubDataFrame(adf, convert(Vector{Nulls.T(T)}, rowinds))
end

function Base.view(adf::AbstractDataFrame, rowinds::Any)
    return SubDataFrame(adf, rowinds)
end

function Base.view(adf::AbstractDataFrame, rowinds::Any, colinds::AbstractVector)
    return SubDataFrame(adf[colinds], rowinds)
end

function Base.view(adf::AbstractDataFrame, rowinds::Any, colinds::Any)
    return SubDataFrame(adf[[colinds]], rowinds)
end

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdf::SubDataFrame) = index(sdf.parent)

# TODO: Remove these
nrow(sdf::SubDataFrame) = ncol(sdf) > 0 ? length(sdf.rows)::Int : 0
ncol(sdf::SubDataFrame) = length(index(sdf))

function Base.getindex(sdf::SubDataFrame, colinds::Any)
    return sdf.parent[sdf.rows, colinds]
end

function Base.getindex(sdf::SubDataFrame, rowinds::Any, colinds::Any)
    return sdf.parent[sdf.rows[rowinds], colinds]
end

function Base.setindex!(sdf::SubDataFrame, val::Any, colinds::Any)
    sdf.parent[sdf.rows, colinds] = val
    return sdf
end

function Base.setindex!(sdf::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    sdf.parent[sdf.rows[rowinds], colinds] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.map(f::Function, sdf::SubDataFrame) = f(sdf) # TODO: deprecate

without(sdf::SubDataFrame, c) = view(without(sdf.parent, c), sdf.rows)
