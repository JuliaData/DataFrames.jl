##############################################################################
##
## We use SubDataTable's to maintain a reference to a subset of a DataTable
## without making copies.
##
##############################################################################

if VERSION >= v"0.6.0-dev.2643"
    include_string("""
        immutable SubDataTable{T <: AbstractVector{Int}} <: AbstractDataTable
            parent::DataTable
            rows::T # maps from subdt row indexes to parent row indexes

            function SubDataTable{T}(parent::DataTable, rows::T) where {T <: AbstractVector{Int}}
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
        immutable SubDataTable{T <: AbstractVector{Int}} <: AbstractDataTable
            parent::DataTable
            rows::T # maps from subdt row indexes to parent row indexes

            function SubDataTable(parent::DataTable, rows::T)
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
A view of row subsets of an AbstractDataTable

A `SubDataTable` is meant to be constructed with `view`.  A
SubDataTable is used frequently in split/apply sorts of operations.

```julia
view(d::AbstractDataTable, rows)
```

### Arguments

* `d` : an AbstractDataTable
* `rows` : any indexing type for rows, typically an Int,
  AbstractVector{Int}, AbstractVector{Bool}, or a Range

### Notes

A `SubDataTable` is an AbstractDataTable, so expect that most
DataTable functions should work. Such methods include `describe`,
`dump`, `nrow`, `size`, `by`, `stack`, and `join`. Indexing is just
like a DataTable; copies are returned.

To subset along columns, use standard column indexing as that creates
a view to the columns by default. To subset along rows and columns,
use column-based indexing with `view`.

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdt1 = view(dt, 1:6)
sdt2 = view(dt, dt[:a] .> 1)
sdt3 = view(dt[[1,3]], dt[:a] .> 1)  # row and column subsetting
sdt4 = groupby(dt, :a)[1]  # indexing a GroupedDataTable returns a SubDataTable
sdt5 = view(sdt1, 1:3)
sdt1[:,[:a,:b]]
```

"""
SubDataTable

function SubDataTable{T <: AbstractVector{Int}}(parent::DataTable, rows::T)
    return SubDataTable{T}(parent, rows)
end

function SubDataTable(parent::DataTable, row::Integer)
    return SubDataTable(parent, [Int(row)])
end

function SubDataTable{S <: Integer}(parent::DataTable, rows::AbstractVector{S})
    return SubDataTable(parent, convert(Vector{Int}, rows))
end

function SubDataTable(parent::DataTable, rows::AbstractVector{Bool})
    return SubDataTable(parent, find(rows))
end

function SubDataTable{T<:Integer}(sdt::SubDataTable, rowinds::Union{T, AbstractVector{T}})
    return SubDataTable(sdt.parent, sdt.rows[rowinds])
end

function Base.view{T<:Nullable}(adt::AbstractDataTable, rowinds::AbstractVector{T})
    # Vector{<:Nullable} need to be checked for nulls and the values lifted
    any(isnull, rowinds) && throw(NullException())
    return SubDataTable(adt, get.(rowinds))
end

function Base.view(adt::AbstractDataTable, rowinds::NullableVector)
    # convert for NullableVectors will throw NullException if nulls present
    return SubDataTable(adt, convert(Vector, rowinds))
end

function Base.view(adt::AbstractDataTable, rowinds::Any)
    return SubDataTable(adt, rowinds)
end

function Base.view(adt::AbstractDataTable, rowinds::Any, colinds::AbstractVector)
    return SubDataTable(adt[colinds], rowinds)
end

function Base.view(adt::AbstractDataTable, rowinds::Any, colinds::Any)
    return SubDataTable(adt[[colinds]], rowinds)
end

##############################################################################
##
## AbstractDataTable interface
##
##############################################################################

index(sdt::SubDataTable) = index(sdt.parent)

# TODO: Remove these
nrow(sdt::SubDataTable) = ncol(sdt) > 0 ? length(sdt.rows)::Int : 0
ncol(sdt::SubDataTable) = length(index(sdt))

function Base.getindex(sdt::SubDataTable, colinds::Any)
    return sdt.parent[sdt.rows, colinds]
end

function Base.getindex(sdt::SubDataTable, rowinds::Any, colinds::Any)
    return sdt.parent[sdt.rows[rowinds], colinds]
end

function Base.setindex!(sdt::SubDataTable, val::Any, colinds::Any)
    sdt.parent[sdt.rows, colinds] = val
    return sdt
end

function Base.setindex!(sdt::SubDataTable, val::Any, rowinds::Any, colinds::Any)
    sdt.parent[sdt.rows[rowinds], colinds] = val
    return sdt
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.map(f::Function, sdt::SubDataTable) = f(sdt) # TODO: deprecate

without(sdt::SubDataTable, c) = view(without(sdt.parent, c), sdt.rows)
