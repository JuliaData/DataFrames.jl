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
functions that expect a collection as an argument. Its element type is always `Any`.

Indexing is one-dimensional like specifying a column of a `DataFrame`.
You can also access the data in a `DataFrameRow` using the `getproperty` and
`setproperty!` functions and convert it to a `NamedTuple` using the `copy` function.

It is possible to create a `DataFrameRow` with duplicate columns.
All such columns will have a reference to the same entry in the parent `DataFrame`.

If the selection of columns in a parent data frame is passed as `:` (a colon)
then `DataFrameRow` will always have all columns from the parent,
even if they are added or removed after its creation.

# Examples
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
        throw(BoundsError(df, (row, cols)))
    end
    DataFrameRow(df, SubIndex(index(df), cols), row)
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
    @inbounds DataFrameRow(parent(sdf), colindex, rows(sdf)[row])
end

Base.@propagate_inbounds DataFrameRow(df::SubDataFrame, row::Bool, cols) =
    throw(ArgumentError("invalid row index of type Bool"))

Base.@propagate_inbounds DataFrameRow(df::AbstractDataFrame, row::Integer) =
    DataFrameRow(df, row, :)

row(r::DataFrameRow) = getfield(r, :row)
Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), parentcols(index(r)))

Base.summary(dfr::DataFrameRow) = # -> String
    @sprintf("%d-element %s", length(dfr), typeof(dfr).name)
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
Base.@propagate_inbounds Base.getindex(r::DataFrameRow, idxs::MultiColumnIndex) =
    DataFrameRow(parent(r), row(r), parentcols(index(r), idxs))
Base.@propagate_inbounds Base.getindex(r::DataFrameRow, ::Colon) = r

for T in (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  v::Union{DataFrameRow, NamedTuple, AbstractDict},
                                  row_ind::Integer,
                                  col_inds::$(T))
        idxs = index(df)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned" *
                                    " collection contains $(length(v)) elements"))
        end

        if v isa AbstractDict
            if all(x -> x isa AbstractString, keys(v))
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
        return df
    end
end

Base.@propagate_inbounds Base.setindex!(r::DataFrameRow, value, idx) =
    setindex!(parent(r), value, row(r), parentcols(index(r), idx))

index(r::DataFrameRow) = getfield(r, :colindex)

Base.names(r::DataFrameRow) = names(index(r))

function Base.names(r::DataFrameRow, cols)
    nms = _names(index(r))
    idx = index(r)[cols]
    idxs = idx isa Int ? (idx:idx) : idx
    return [string(nms[i]) for i in idxs]
end

_names(r::DataFrameRow) = view(_names(parent(r)), parentcols(index(r), :))

Base.haskey(r::DataFrameRow, key::Bool) =
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
Base.view(r::DataFrameRow, cols::MultiColumnIndex) =
    DataFrameRow(parent(r), row(r), parentcols(index(r), cols))
Base.view(r::DataFrameRow, ::Colon) = r

"""
    size(dfr::DataFrameRow, [dim])

Return a 1-tuple containing the number of elements of `dfr`.
If an optional dimension `dim` is specified, it must be `1`, and the number of
elements is returned directly as a number.

See also: [`length`](@ref)

# Examples
```julia
julia> dfr = DataFrame(a=1:3, b='a':'c')[1, :];

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
```julia
julia> dfr = DataFrame(a=1:3, b='a':'c')[1, :];

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
    df = parent(dfr)
    T = reduce(promote_type, (eltype(df[!, i]) for i in parentcols(index(dfr))))
    convert(Vector{T}, dfr)
end
Base.convert(::Type{Vector{T}}, dfr::DataFrameRow) where T =
    T[dfr[i] for i in 1:length(dfr)]
Base.Vector(dfr::DataFrameRow) = convert(Vector, dfr)
Base.Vector{T}(dfr::DataFrameRow) where T = convert(Vector{T}, dfr)

Base.convert(::Type{Array}, dfr::DataFrameRow) = Vector(dfr)
Base.convert(::Type{Array{T}}, dfr::DataFrameRow) where {T} = Vector{T}(dfr)
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
Base.convert(::Type{Tuple}, dfr::DataFrameRow) = Tuple(dfr)

Base.merge(a::DataFrameRow) = NamedTuple(a)
Base.merge(a::DataFrameRow, b::NamedTuple) = merge(NamedTuple(a), b)
Base.merge(a::NamedTuple, b::DataFrameRow) = merge(a, NamedTuple(b))
Base.merge(a::DataFrameRow, b::DataFrameRow) = merge(NamedTuple(a), NamedTuple(b))
Base.merge(a::DataFrameRow, b::Base.Iterators.Pairs) = merge(NamedTuple(a), b)
Base.merge(a::DataFrameRow, itr) = merge(NamedTuple(a), itr)

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
# table columns are passed as a tuple of vectors to ensure type specialization
rowhash(cols::Tuple{AbstractVector}, r::Int, h::UInt = zero(UInt))::UInt =
    hash(cols[1][r], h)
function rowhash(cols::Tuple{Vararg{AbstractVector}}, r::Int, h::UInt = zero(UInt))::UInt
    h = hash(cols[1][r], h)
    rowhash(Base.tail(cols), r, h)
end

Base.hash(r::DataFrameRow, h::UInt = zero(UInt)) =
    rowhash(ntuple(col -> parent(r)[!, parentcols(index(r), col)], length(r)), row(r), h)

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

function Base.push!(df::DataFrame, dfr::DataFrameRow; cols::Symbol=:setequal,
                    columns::Union{Nothing,Symbol}=nothing,
                    promote::Bool=(cols in [:union, :subset]))
    if columns !== nothing
        cols = columns
        Base.depwarn("`columns` keyword argument is deprecated. " *
                     "Use `cols` instead.", :push!)
    end

    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows, ncols = size(df)
    targetrows = nrows + 1

    if parent(dfr) === df && index(dfr) isa Index
        # in this case we are sure that all we do is safe
        r = row(dfr)
        for col in _columns(df)
            # use a barrier function to improve performance
            pushhelper!(col, r)
        end
        for (colname, col) in zip(_names(df), _columns(df))
            if length(col) != targetrows
                for col2 in _columns(df)
                    resize!(col2, nrows)
                end
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        return df
    end

    if ncols == 0
        for (n, v) in pairs(dfr)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end

    if cols == :union
        for (i, colname) in enumerate(_names(df))
            col = _columns(df)[i]
            if hasproperty(dfr, colname)
                val = dfr[colname]
            else
                val = missing
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || promote_type(S, T) <: T
                push!(col, val)
            elseif !promote
                try
                    push!(col, val)
                catch err
                    for col in _columns(df)
                        resize!(col, nrows)
                    end
                    @error "Error adding value to column :$colname."
                    rethrow(err)
                end
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                _columns(df)[i] = newcol
            end
        end
        for (colname, col) in zip(_names(df), _columns(df))
            if length(col) != targetrows
                for col2 in _columns(df)
                    resize!(col2, nrows)
                end
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        for colname in setdiff(_names(dfr), _names(df))
            val = dfr[colname]
            S = typeof(val)
            if nrows == 0
                newcol = [val]
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
                newcol[end] = val
            end
            df[!, colname] = newcol
        end
        return df
    end

    current_col = 0
    try
        if cols === :orderequal
            if _names(df) != _names(dfr)
                msg = "when `cols == :orderequal` pushed row must have the same " *
                      "column names and in the same order as the target data frame"
                throw(ArgumentError(msg))
            end
        elseif cols === :setequal || cols === :equal
            if cols === :equal
                Base.depwarn("`cols == :equal` is deprecated." *
                             "Use `:setequal` instead.", :push!)
            end
            msg = "Number of columns of `DataFrameRow` does not match that of " *
                  "target data frame (got $(length(dfr)) and $ncols)."
            ncols == length(dfr) || throw(ArgumentError(msg))
        end
        for (col, nm) in zip(_columns(df), _names(df))
            current_col += 1
            if cols === :subset
                val = get(dfr, nm, missing)
            else
                val = dfr[nm]
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                _columns(df)[columnindex(df, nm)] = newcol
            end
        end
        for col in _columns(df)
            @assert length(col) == targetrows
        end
    catch err
        for col in _columns(df)
            resize!(col, nrows)
        end
        if current_col > 0
            @error "Error adding value to column :$(_names(df)[current_col])."
        end
        rethrow(err)
    end
    return df
end
