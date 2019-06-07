"""
    DataFrameRow{<:AbstractDataFrame,<:AbstractIndex}

A view of one row of an `AbstractDataFrame`.

A `DataFrameRow` is returned by `getindex` or `view` functions when one row and a
selection of columns are requested, or when iterating the result
of the call to the [`eachrow`](@ref) function.

The `DataFrameRow` constructor can also be called directly:

```
DataFrameRow(parent::AbstractDataFrame, row::Integer, cols=:)
```

A `DataFrameRow` supports the iteration interface and can therefore be passed to
functions that expect a collection as an argument.

Indexing is one-dimensional like specifying a column of a `DataFrame`.
You can also access the data in a `DataFrameRow` using the `getproperty` and
`setproperty!` functions and convert it to a `NamedTuple` using the `copy` function.

It is possible to create a `DataFrameRow` with duplicate columns.
All such columns will have a reference to the same entry in the parent `DataFrame`.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `DataFrameRow` will always have all columns from the parent,
even if they are added or removed after its creation.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
sdf1 = view(df, 2, :)
sdf2 = @view df[end, [:a]]
sdf3 = eachrow(df)[1]
sdf4 = DataFrameRow(df, 2, 1:2)
sdf5 = DataFrameRow(df, 1)
```
"""
struct DataFrameRow{D<:AbstractDataFrame,S<:AbstractIndex}
    df::D
    colindex::S
    row::Int

    @inline DataFrameRow(df::D, colindex::S, row::Union{Signed, Unsigned}) where
        {D<:AbstractDataFrame,S<:AbstractIndex} = new{D,S}(df, colindex, row)
end

Base.@propagate_inbounds function DataFrameRow(df::DataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row"))
    end
    DataFrameRow(df, SubIndex(index(df), cols), row)
end

Base.@propagate_inbounds function DataFrameRow(sdf::SubDataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(sdf, 1), row)
        throw(BoundsError("attempt to access a data frame with $(nrow(sdf)) " *
                          "rows at index $row"))
    end
    colindex = SubIndex(index(parent(sdf)), parentcols(index(sdf), cols))
    @inbounds DataFrameRow(parent(sdf), colindex, rows(sdf)[row])
end

Base.@propagate_inbounds DataFrameRow(df::AbstractDataFrame, row::Integer) =
    DataFrameRow(df, row, :)

row(r::DataFrameRow) = getfield(r, :row)
Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), parentcols(index(r)))

Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(adf, rowind, :)
Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowind::Integer,
                                   colinds::AbstractVector) =
    DataFrameRow(adf, rowind, colinds)

Base.@propagate_inbounds Base.getindex(df::AbstractDataFrame, rowind::Integer,
                                       colinds::AbstractVector) =
    DataFrameRow(df, rowind, colinds)
Base.@propagate_inbounds Base.getindex(df::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(df, rowind, :)

Base.@propagate_inbounds Base.getindex(r::DataFrameRow, idx::ColumnIndex) =
    parent(r)[row(r), parentcols(index(r), idx)]
Base.@propagate_inbounds Base.getindex(r::DataFrameRow, idxs::AbstractVector) =
    DataFrameRow(parent(r), row(r), parentcols(index(r), idxs))
Base.@propagate_inbounds Base.getindex(r::DataFrameRow, ::Colon) = r

Base.@propagate_inbounds Base.setindex!(r::DataFrameRow, value::Any, idx) =
    setindex!(parent(r), value, row(r), parentcols(index(r), idx))

index(r::DataFrameRow) = getfield(r, :colindex)

Base.names(r::DataFrameRow) = _names(parent(r))[parentcols(index(r), :)]
_names(r::DataFrameRow) = view(_names(parent(r)), parentcols(index(r), :))

Base.haskey(r::DataFrameRow, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DataFrameRow, key::Integer) = 1 ≤ key ≤ size(r, 1)
function Base.haskey(r::DataFrameRow, key::Symbol)
    haskey(parent(r), key) || return false
    index(r) isa Index && return true
    # here index(r) is a SubIndex
    pos = index(parent(r))[key]
    remap = index(r).remap
    length(remap) == 0 && lazyremap!(index(r))
    checkbounds(Bool, remap, pos) || return false
    remap[pos] > 0
end

Base.getproperty(r::DataFrameRow, idx::Symbol) = getindex(r, idx)
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = setindex!(r, x, idx)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = names(r)

Base.view(r::DataFrameRow, col::ColumnIndex) =
    view(parent(r)[parentcols(index(r), col)], row(r))
Base.view(r::DataFrameRow, cols::AbstractVector) =
    DataFrameRow(parent(r), row(r), parentcols(index(r), cols))
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
Base.IteratorEltype(::Type{<:DataFrameRow}) = Base.EltypeUnknown()

function Base.convert(::Type{Vector}, dfr::DataFrameRow)
    T = reduce(promote_type, eltypes(parent(dfr)))
    convert(Vector{T}, dfr)
end
Base.convert(::Type{Vector{T}}, dfr::DataFrameRow) where T =
    T[dfr[i] for i in 1:length(dfr)]
Base.Vector(dfr::DataFrameRow) = convert(Vector, dfr)
Base.Vector{T}(dfr::DataFrameRow) where T = convert(Vector{T}, dfr)

Base.keys(r::DataFrameRow) = Tuple(_names(r))
Base.values(r::DataFrameRow) =
    ntuple(col -> parent(r)[row(r), parentcols(index(r), col)], length(r))
Base.map(f, r::DataFrameRow, rs::DataFrameRow...) = map(f, copy(r), copy.(rs)...)
Base.get(dfr::DataFrameRow, key::Union{Integer, Symbol}, default) =
    haskey(dfr, key) ? dfr[key] : default
Base.get(f::Base.Callable, dfr::DataFrameRow, key::Union{Integer, Symbol}) =
    haskey(dfr, key) ? dfr[key] : f()
Base.broadcastable(::DataFrameRow) = throw(ArgumentError("broadcasting over `DataFrameRow`s is reserved"))

"""
    copy(dfr::DataFrameRow)

Convert a [`DataFrameRow`](@ref) to a `NamedTuple`.
"""
Base.copy(r::DataFrameRow) = NamedTuple{Tuple(keys(r))}(values(r))

# hash column element
Base.@propagate_inbounds hash_colel(v::AbstractArray, i, h::UInt = zero(UInt)) =
    hash(v[i], h)
Base.@propagate_inbounds function hash_colel(v::AbstractCategoricalArray, i,
                                             h::UInt = zero(UInt))
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
    rowhash(ntuple(col -> parent(r)[parentcols(index(r), col)], length(r)), row(r), h)

function Base.:(==)(r1::DataFrameRow, r2::DataFrameRow)
    if parent(r1) === parent(r2)
        parentcols(index(r1)) == parentcols(index(r2)) || return false
        row(r1) == row(r2) && return true
    else
        _names(r1) == _names(r2) || return false
    end
    all(((a, b),) -> a == b, zip(r1, r2))
end

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    if parent(r1) === parent(r2)
        parentcols(index(r1)) == parentcols(index(r2)) || return false
        row(r1) == row(r2) && return true
    else
        _names(r1) == _names(r2) || return false
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

function DataFrame(dfr::DataFrameRow)
    row, cols = parentindices(dfr)
    parent(dfr)[row:row, cols]
end

@noinline pushhelper!(x, r) = push!(x, x[r])

function Base.push!(df::DataFrame, dfr::DataFrameRow; columns::Symbol=:equal)
    if !(columns in (:equal, :intersect))
        throw(ArgumentError("`columns` keyword argument must be `:equal` or `:intersect`"))
    end
    if ncol(df) == 0
        for (n, v) in pairs(dfr)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end

    if parent(dfr) === df && index(dfr) isa Index
        # in this case we are sure that all we do is safe
        r = row(dfr)
        for col in _columns(df)
            # use a barrier function to improve performance
            pushhelper!(col, r)
        end
    else
        # DataFrameRow can contain duplicate columns and we disallow this
        # corner case when push!-ing
        # Only check for equal lengths, as an error will be thrown below if some names don't match
        if columns === :equal
            msg = "Number of columns of `row` does not match `DataFrame` column count."
            size(df, 2) == length(dfr) || throw(ArgumentError(msg))
        end
        i = 1
        for nm in _names(df)
            try
                push!(df[i], dfr[nm])
            catch
                #clean up partial row
                for j in 1:(i - 1)
                    pop!(df[j])
                end
                msg = "Error adding value to column :$nm."
                throw(ArgumentError(msg))
            end
            i += 1
        end
    end
    df
end
