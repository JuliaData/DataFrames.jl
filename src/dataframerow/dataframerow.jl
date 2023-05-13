"""
    DataFrameRow{<:AbstractDataFrame, <:AbstractIndex}

A view of one row of an `AbstractDataFrame`.

A `DataFrameRow` is returned by `getindex` or `view` functions when one row and a
selection of columns are requested, or when iterating the result
of the call to the [`eachrow`](@ref) function.

The `DataFrameRow` constructor can also be called directly:

```
DataFrameRow(parent::AbstractDataFrame, row::Integer, cols=:)
```

A `DataFrameRow` supports the iteration interface and can therefore be passed to
functions that expect a collection as an argument. Its element type is always `Any`.

Indexing is one-dimensional like specifying a column of a `DataFrame`.
You can also access the data in a `DataFrameRow` using the `getproperty` and
`setproperty!` functions and convert it to a `Tuple`, `NamedTuple`, or `Vector`
using the corresponding functions.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `DataFrameRow` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
```jldoctest
julia> df = DataFrame(a=repeat([1, 2], outer=[2]),
                      b=repeat(["a", "b"], inner=[2]),
                      c=1:4)
4×3 DataFrame
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1
   2 │     2  a           2
   3 │     1  b           3
   4 │     2  b           4

julia> df[1, :]
DataFrameRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> @view df[end, [:a]]
DataFrameRow
 Row │ a
     │ Int64
─────┼───────
   4 │     2

julia> eachrow(df)[1]
DataFrameRow
 Row │ a      b       c
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  a           1

julia> Tuple(df[1, :])
(1, "a", 1)

julia> NamedTuple(df[1, :])
(a = 1, b = "a", c = 1)

julia> Vector(df[1, :])
3-element Vector{Any}:
 1
  "a"
 1
```
"""
struct DataFrameRow{D<:AbstractDataFrame, S<:AbstractIndex}
    # although we allow D to be AbstractDataFrame to support extensions
    # in DataFrames.jl it will always be a DataFrame unless an inner constructor
    # is used. In this way we have a fast access to the data frame that
    # actually stores the data that DataFrameRow refers to
    df::D
    colindex::S
    dfrow::Int # row number in df
    rownumber::Int # row number in the direct source AbstractDataFrame from which DataFrameRow was created

    @inline DataFrameRow(df::D, colindex::S, row::Union{Signed, Unsigned},
                         rownumber::Union{Signed, Unsigned}) where
        {D<:AbstractDataFrame, S<:AbstractIndex} = new{D, S}(df, colindex, row, rownumber)
end

Base.@propagate_inbounds function DataFrameRow(df::DataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(df, 1), row)
        throw(BoundsError(df, (row, cols)))
    end
    DataFrameRow(df, SubIndex(index(df), cols), row, row)
end

Base.@propagate_inbounds DataFrameRow(df::DataFrame, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds function DataFrameRow(sdf::SubDataFrame, row::Integer, cols)
    @boundscheck if !checkindex(Bool, axes(sdf, 1), row)
        throw(BoundsError(sdf, (row, cols)))
    end
    if index(sdf) isa Index # sdf was created using : as row selector
        colindex = SubIndex(index(sdf), cols)
    else
        colindex = SubIndex(index(parent(sdf)), parentcols(index(sdf), cols))
    end
    @inbounds DataFrameRow(parent(sdf), colindex, rows(sdf)[row], row)
end

Base.@propagate_inbounds DataFrameRow(df::SubDataFrame, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds DataFrameRow(df::AbstractDataFrame, row::Integer) =
    DataFrameRow(df, row, :)

row(r::DataFrameRow) = getfield(r, :dfrow)

"""
    rownumber(dfr::DataFrameRow)

Return a row number in the `AbstractDataFrame` that `dfr` was created from.

Note that this differs from the first element in the tuple returned by
`parentindices`. The latter gives the row number in the `parent(dfr)`, which is
the source `DataFrame` where data that `dfr` gives access to is stored.

# Examples
```jldoctest
julia> df = DataFrame(reshape(1:12, 3, 4), :auto)
3×4 DataFrame
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dfr = df[2, :]
DataFrameRow
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   2 │     2      5      8     11

julia> rownumber(dfr)
2

julia> parentindices(dfr)
(2, Base.OneTo(4))

julia> parent(dfr)
3×4 DataFrame
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12

julia> dfv = @view df[2:3, 1:3]
2×3 SubDataFrame
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      5      8
   2 │     3      6      9

julia> dfrv = dfv[2, :]
DataFrameRow
 Row │ x1     x2     x3
     │ Int64  Int64  Int64
─────┼─────────────────────
   3 │     3      6      9

julia> rownumber(dfrv)
2

julia> parentindices(dfrv)
(3, 1:3)

julia> parent(dfrv)
3×4 DataFrame
 Row │ x1     x2     x3     x4
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      7     10
   2 │     2      5      8     11
   3 │     3      6      9     12
```
"""
rownumber(r::DataFrameRow) = getfield(r, :rownumber)

Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), parentcols(index(r)))

Base.summary(dfr::DataFrameRow) = # -> String
    @sprintf("%d-element %s", length(dfr), nameof(typeof(dfr)))
Base.summary(io::IO, dfr::DataFrameRow) = print(io, summary(dfr))

Base.@propagate_inbounds Base.view(adf::AbstractDataFrame, rowind::Integer,
                                   colinds::MultiColumnIndex) =
    DataFrameRow(adf, rowind, colinds)

Base.@propagate_inbounds Base.getindex(df::AbstractDataFrame, rowind::Integer,
                                       colinds::MultiColumnIndex) =
    DataFrameRow(df, rowind, colinds)
Base.@propagate_inbounds Base.getindex(df::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(df, rowind, :)
Base.@propagate_inbounds Base.getindex(r::DataFrameRow, idx::ColumnIndex) =
    parent(r)[row(r), parentcols(index(r), idx)]

Base.@propagate_inbounds function Base.getindex(r::DataFrameRow, idxs::MultiColumnIndex)
    # we create a temporary DataFrameRow object to compute the SubIndex
    # in the parent(r), but this object has an incorrect rownumber
    # so we later copy rownumber from r
    # the Julia compiler should be able to optimize out this indirection
    # and in this way we avoid duplicating the code that computes the correct SubIndex
    dfr_tmp = DataFrameRow(parent(r), row(r), parentcols(index(r), idxs))
    return DataFrameRow(parent(dfr_tmp), index(dfr_tmp), row(r), rownumber(r))
end

Base.@propagate_inbounds Base.getindex(r::DataFrameRow, ::Colon) = r

for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(df::DataFrame,
                                  v::Union{DataFrameRow, NamedTuple, AbstractDict},
                                  row_ind::Integer,
                                  col_inds::$(T))
        idxs = index(df)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end

        if v isa AbstractDict
            if keytype(v) !== Symbol &&
                (keytype(v) <: AbstractString || all(x -> x isa AbstractString, keys(v)))
                v = (;(Symbol.(keys(v)) .=> values(v))...)
            end
            for n in view(_names(df), idxs)
                if !haskey(v, n)
                    throw(ArgumentError("Column :$n not found in source dictionary"))
                end
            end
        elseif !all(((a, b),) -> a == b, zip(view(_names(df), idxs), keys(v)))
            mismatched = findall(view(_names(df), idxs) .!= collect(keys(v)))
            throw(ArgumentError("Selected column names do not match the names in assigned " *
                                "value in positions $(join(mismatched, ", ", " and "))"))
        end

        for (col, val) in pairs(v)
            df[row_ind, col] = val
        end
        _drop_all_nonnote_metadata!(df)
        return df
    end
end

Base.@propagate_inbounds Base.setindex!(r::DataFrameRow, value, idx) =
    setindex!(parent(r), value, row(r), parentcols(index(r), idx))

index(r::DataFrameRow) = getfield(r, :colindex)

Base.names(r::DataFrameRow, cols::Colon=:) = names(index(r))

function Base.names(r::DataFrameRow, cols)
    nms = _names(index(r))
    idx = index(r)[cols]
    idxs = idx isa Int ? (idx:idx) : idx
    return [string(nms[i]) for i in idxs]
end

Base.names(r::DataFrameRow, T::Type) =
    [String(n) for n in _names(r) if eltype(parent(r)[!, n]) <: T]
Base.names(r::DataFrameRow, fun::Function) = filter!(fun, names(r))

_names(r::DataFrameRow) = view(_names(parent(r)), parentcols(index(r), :))

Base.haskey(::DataFrameRow, key::Any) =
    throw(ArgumentError("invalid key: $key of type $(typeof(key))"))
Base.haskey(::DataFrameRow, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.haskey(r::DataFrameRow, key::Integer) = 1 ≤ key ≤ size(r, 1)

function Base.haskey(r::DataFrameRow, key::Symbol)
    hasproperty(parent(r), key) || return false
    index(r) isa Index && return true
    # here index(r) is a SubIndex
    pos = index(parent(r))[key]
    remap = index(r).remap
    length(remap) == 0 && lazyremap!(index(r))
    checkbounds(Bool, remap, pos) || return false
    return remap[pos] > 0
end

Base.haskey(r::DataFrameRow, key::AbstractString) = haskey(r, Symbol(key))

# separate methods are needed due to dispatch ambiguity
Base.getproperty(r::DataFrameRow, idx::Symbol) = r[idx]
Base.getproperty(r::DataFrameRow, idx::AbstractString) = r[idx]
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = (r[idx] = x)
Base.setproperty!(r::DataFrameRow, idx::AbstractString, x::Any) = (r[idx] = x)
Compat.hasproperty(r::DataFrameRow, s::Symbol) = haskey(index(r), s)
Compat.hasproperty(r::DataFrameRow, s::AbstractString) = haskey(index(r), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = copy(_names(r))

Base.view(r::DataFrameRow, col::ColumnIndex) =
    view(parent(r)[!, parentcols(index(r), col)], row(r))

function Base.view(r::DataFrameRow, cols::MultiColumnIndex)
    # we create a temporary DataFrameRow object to compute the SubIndex
    # in the parent(r), but this object has an incorrect rownumber
    # so we later copy rownumber from r
    # the Julia compiler should be able to optimize out this indirection
    # and in this way we avoid duplicating the code that computes the correct SubIndex
    dfr_tmp = DataFrameRow(parent(r), row(r), parentcols(index(r), cols))
    return DataFrameRow(parent(dfr_tmp), index(dfr_tmp), row(r), rownumber(r))
end

Base.view(r::DataFrameRow, ::Colon) = r

"""
    size(dfr::DataFrameRow[, dim])

Return a 1-tuple containing the number of elements of `dfr`.
If an optional dimension `dim` is specified, it must be `1`, and the number of
elements is returned directly as a number.

See also: [`length`](@ref)

# Examples
```jldoctest
julia> dfr = DataFrame(a=1:3, b='a':'c')[1, :]
DataFrameRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> size(dfr)
(2,)

julia> size(dfr, 1)
2
```
"""
Base.size(r::DataFrameRow) = (length(index(r)),)
Base.size(r::DataFrameRow, i) = size(r)[i]

"""
    length(dfr::DataFrameRow)

Return the number of elements of `dfr`.

See also: [`size`](@ref)

# Examples
```jldoctest
julia> dfr = DataFrame(a=1:3, b='a':'c')[1, :]
DataFrameRow
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a

julia> length(dfr)
2
```
"""
Base.length(r::DataFrameRow) = size(r, 1)

"""
    ndims(::DataFrameRow)
    ndims(::Type{<:DataFrameRow})

Return the number of dimensions of a data frame row, which is always `1`.
"""
Base.ndims(::DataFrameRow) = 1
Base.ndims(::Type{<:DataFrameRow}) = 1

Base.firstindex(r::DataFrameRow) = 1
Base.lastindex(r::DataFrameRow) = length(r)

Base.axes(r::DataFrameRow, i::Integer) = Base.OneTo(size(r, i))

Base.iterate(r::DataFrameRow) = iterate(r, 1)

function Base.iterate(r::DataFrameRow, st)
    st > length(r) && return nothing
    return (r[st], st + 1)
end

# Computing the element type requires going over all columns,
# so better let collect() do it only if necessary (widening)
Base.IteratorEltype(::Type{<:DataFrameRow}) = Base.EltypeUnknown()

function Base.Vector(dfr::DataFrameRow)
    df = parent(dfr)
    T = reduce(promote_type, (eltype(df[!, i]) for i in parentcols(index(dfr))))
    return Vector{T}(dfr)
end
Base.Vector{T}(dfr::DataFrameRow) where T =
    T[dfr[i] for i in 1:length(dfr)]

Base.Array(dfr::DataFrameRow) = Vector(dfr)
Base.Array{T}(dfr::DataFrameRow) where {T} = Vector{T}(dfr)

Base.keys(r::DataFrameRow) = propertynames(r)
Base.values(r::DataFrameRow) =
    ntuple(col -> parent(r)[row(r), parentcols(index(r), col)], length(r))
Base.map(f, r::DataFrameRow, rs::DataFrameRow...) = map(f, copy(r), copy.(rs)...)
Base.get(dfr::DataFrameRow, key::ColumnIndex, default) =
    haskey(dfr, key) ? dfr[key] : default
Base.get(f::Base.Callable, dfr::DataFrameRow, key::ColumnIndex) =
    haskey(dfr, key) ? dfr[key] : f()
Base.broadcastable(::DataFrameRow) =
    throw(ArgumentError("broadcasting over `DataFrameRow`s is reserved"))

function Base.NamedTuple(dfr::DataFrameRow)
    k = Tuple(_names(dfr))
    v = ntuple(i -> dfr[i], length(dfr))
    pc = parentcols(index(dfr))
    cols = _columns(parent(dfr))
    s = ntuple(i -> eltype(cols[pc[i]]), length(dfr))
    NamedTuple{k, Tuple{s...}}(v)
end

"""
    copy(dfr::DataFrameRow)

Construct a `NamedTuple` with the same contents as the [`DataFrameRow`](@ref).
This method returns a `NamedTuple` so that the returned object
is not affected by changes to the parent data frame of which `dfr` is a view.

"""
Base.copy(dfr::DataFrameRow) = NamedTuple(dfr)

Base.convert(::Type{NamedTuple}, dfr::DataFrameRow) = NamedTuple(dfr)

Base.merge(a::DataFrameRow) = NamedTuple(a)
Base.merge(a::DataFrameRow, b::NamedTuple) = merge(NamedTuple(a), b)
Base.merge(a::NamedTuple, b::DataFrameRow) = merge(a, NamedTuple(b))
Base.merge(a::DataFrameRow, b::DataFrameRow) = merge(NamedTuple(a), NamedTuple(b))
Base.merge(a::DataFrameRow, b::Base.Iterators.Pairs) = merge(NamedTuple(a), b)
Base.merge(a::DataFrameRow, itr) = merge(NamedTuple(a), itr)

Base.hash(r::DataFrameRow, h::UInt) = _nt_like_hash(r, h)

for eqfun in (:isequal, :(==)),
    (leftarg, rightarg) in ((:DataFrameRow, :DataFrameRow),
                            (:DataFrameRow, :NamedTuple),
                            (:NamedTuple, :DataFrameRow))
    @eval function Base.$eqfun(r1::$leftarg, r2::$rightarg)
        _equal_names(r1, r2) || return false
        return all(((a, b),) -> $eqfun(a, b), zip(r1, r2))
    end
end

for (eqfun, cmpfun) in ((:isequal, :isless), (:(==), :(<))),
    (leftarg, rightarg) in ((:DataFrameRow, :DataFrameRow),
                            (:DataFrameRow, :NamedTuple),
                            (:NamedTuple, :DataFrameRow))
    @eval function Base.$cmpfun(r1::$leftarg, r2::$rightarg)
        if !_equal_names(r1, r2)
            length(r1) == length(r2) ||
                throw(ArgumentError("compared objects must have the same number " *
                                    "of columns (got $(length(r1)) and $(length(r2)))"))
            mismatch = findfirst(i -> _getnames(r1)[i] != _getnames(r2)[i], 1:length(r1))
            throw(ArgumentError("compared objects must have the same property " *
                                "names but they differ in column number $mismatch " *
                                "where the names are :$(_getnames(r1)[mismatch]) and " *
                                ":$(_getnames(r2)[mismatch]) respectively"))
        end
        for (a, b) in zip(r1, r2)
            eq = $eqfun(a, b)
            if ismissing(eq)
                return missing
            elseif !eq
                return $cmpfun(a, b)
            end
        end
        return false # here we know that r1 and r2 have equal lengths and all values were equal
    end
end

function DataFrame(dfr::DataFrameRow; copycols::Bool=true)
    if !copycols
        throw(ArgumentError("It is not possible to construct a `DataFrame`" *
                            "from DataFrameRow with `copycols=false`"))
    end
    row, cols = parentindices(dfr)
    parent(dfr)[row:row, cols]
end
