##############################################################################
##
## We use SubDataFrame's to maintain a reference to a subset of a DataFrame
## without making copies.
##
##############################################################################

if VERSION >= v"0.6.0-dev.2643"
    include_string("""
        immutable SubDataFrame{T <: AbstractVector{Int}} <: AbstractDataFrame
            parent::DataFrame
            rows::T # maps from subdt row indexes to parent row indexes

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
else
    @eval begin
        immutable SubDataFrame{T <: AbstractVector{Int}} <: AbstractDataFrame
            parent::DataFrame
            rows::T # maps from subdt row indexes to parent row indexes

            function SubDataFrame(parent::DataFrame, rows::T)
                if length(rows) > 0
                    rmin, rmax = extrema(rows)
                    if rmin < 1 || rmax > size(parent, 1)
                        throw(BoundsError())
                    end
                end
                new(parent, rows)
            end
        end
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
dt = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdt1 = view(dt, 1:6)
sdt2 = view(dt, dt[:a] .> 1)
sdt3 = view(dt[[1,3]], dt[:a] .> 1)  # row and column subsetting
sdt4 = groupby(dt, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame
sdt5 = view(sdt1, 1:3)
sdt1[:,[:a,:b]]
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

function SubDataFrame(sdt::SubDataFrame, rowinds::Union{T, AbstractVector{T}}) where {T <: Integer}
    return SubDataFrame(sdt.parent, sdt.rows[rowinds])
end

function Base.view(adt::AbstractDataFrame, rowinds::AbstractVector{T}) where {T >: Null}
    # Vector{>:Null} need to be checked for nulls
    any(isnull, rowinds) && throw(NullException())
    return SubDataFrame(adt, convert(Vector{Nulls.T(T)}, rowinds))
end

function Base.view(adt::AbstractDataFrame, rowinds::Any)
    return SubDataFrame(adt, rowinds)
end

function Base.view(adt::AbstractDataFrame, rowinds::Any, colinds::AbstractVector)
    return SubDataFrame(adt[colinds], rowinds)
end

function Base.view(adt::AbstractDataFrame, rowinds::Any, colinds::Any)
    return SubDataFrame(adt[[colinds]], rowinds)
end

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(sdt::SubDataFrame) = index(sdt.parent)

# TODO: Remove these
nrow(sdt::SubDataFrame) = ncol(sdt) > 0 ? length(sdt.rows)::Int : 0
ncol(sdt::SubDataFrame) = length(index(sdt))

function Base.getindex(sdt::SubDataFrame, colinds::Any)
    return sdt.parent[sdt.rows, colinds]
end

function Base.getindex(sdt::SubDataFrame, rowinds::Any, colinds::Any)
    return sdt.parent[sdt.rows[rowinds], colinds]
end

function Base.setindex!(sdt::SubDataFrame, val::Any, colinds::Any)
    sdt.parent[sdt.rows, colinds] = val
    return sdt
end

function Base.setindex!(sdt::SubDataFrame, val::Any, rowinds::Any, colinds::Any)
    sdt.parent[sdt.rows[rowinds], colinds] = val
    return sdt
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.map(f::Function, sdt::SubDataFrame) = f(sdt) # TODO: deprecate

without(sdt::SubDataFrame, c) = view(without(sdt.parent, c), sdt.rows)
