##############################################################################
##
## We use SubDataTable's to maintain a reference to a subset of a DataTable
## without making copies.
##
##############################################################################

"""
A view of row subsets of an AbstractDataTable

A `SubDataTable` is meant to be constructed with `sub`.  A
SubDataTable is used frequently in split/apply sorts of operations.

```julia
sub(d::AbstractDataTable, rows)
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
use column-based indexing with `sub`.

### Examples

```julia
df = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdf1 = sub(df, 1:6)
sdf2 = sub(df, df[:a] .> 1)
sdf3 = sub(df[[1,3]], df[:a] .> 1)  # row and column subsetting
sdf4 = groupby(df, :a)[1]  # indexing a GroupedDataTable returns a SubDataTable
sdf5 = sub(sdf1, 1:3)
sdf1[:,[:a,:b]]
```

"""
immutable SubDataTable{T <: AbstractVector{Int}} <: AbstractDataTable
    parent::DataTable
    rows::T # maps from subdf row indexes to parent row indexes

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

function SubDataTable{T <: AbstractVector{Int}}(parent::DataTable, rows::T)
    return SubDataTable{T}(parent, rows)
end

function SubDataTable(parent::DataTable, row::Integer)
    return SubDataTable(parent, [row])
end

function SubDataTable{S <: Integer}(parent::DataTable, rows::AbstractVector{S})
    return sub(parent, Int(rows))
end


function Base.sub{S <: Real}(df::DataTable, rowinds::AbstractVector{S})
    return SubDataTable(df, rowinds)
end

function Base.sub{S <: Real}(sdf::SubDataTable, rowinds::AbstractVector{S})
    return SubDataTable(sdf.parent, sdf.rows[rowinds])
end

function Base.sub(df::DataTable, rowinds::AbstractVector{Bool})
    return sub(df, getindex(SimpleIndex(size(df, 1)), rowinds))
end

function Base.sub(sdf::SubDataTable, rowinds::AbstractVector{Bool})
    return sub(sdf, getindex(SimpleIndex(size(sdf, 1)), rowinds))
end

function Base.sub(adf::AbstractDataTable, rowinds::Integer)
    return SubDataTable(adf, Int[rowinds])
end

function Base.sub(adf::AbstractDataTable, rowinds::Any)
    return sub(adf, getindex(SimpleIndex(size(adf, 1)), rowinds))
end

function Base.sub(adf::AbstractDataTable, rowinds::Any, colinds::Any)
    return sub(adf[[colinds]], rowinds)
end

##############################################################################
##
## AbstractDataTable interface
##
##############################################################################

index(sdf::SubDataTable) = index(sdf.parent)

# TODO: Remove these
nrow(sdf::SubDataTable) = ncol(sdf) > 0 ? length(sdf.rows)::Int : 0
ncol(sdf::SubDataTable) = length(index(sdf))

function Base.getindex(sdf::SubDataTable, colinds::Any)
    return sdf.parent[sdf.rows, colinds]
end

function Base.getindex(sdf::SubDataTable, rowinds::Any, colinds::Any)
    return sdf.parent[sdf.rows[rowinds], colinds]
end

function Base.setindex!(sdf::SubDataTable, val::Any, colinds::Any)
    sdf.parent[sdf.rows, colinds] = val
    return sdf
end

function Base.setindex!(sdf::SubDataTable, val::Any, rowinds::Any, colinds::Any)
    sdf.parent[sdf.rows[rowinds], colinds] = val
    return sdf
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

Base.map(f::Function, sdf::SubDataTable) = f(sdf) # TODO: deprecate

function Base.delete!(sdf::SubDataTable, c::Any) # TODO: deprecate?
    return SubDataTable(delete!(sdf.parent, c), sdf.rows)
end

without(sdf::SubDataTable, c::Vector{Int}) = sub(without(sdf.parent, c), sdf.rows)
without(sdf::SubDataTable, c::Int) = sub(without(sdf.parent, c), sdf.rows)
without(sdf::SubDataTable, c::Any) = sub(without(sdf.parent, c), sdf.rows)
