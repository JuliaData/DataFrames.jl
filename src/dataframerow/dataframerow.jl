"""
    DataFrameRow{<:AbstractDataFrame}

A view of one row of an AbstractDataFrame.
"""
struct DataFrameRow{T<:AbstractDataFrame}
    df::T
    row::Int

    @inline function DataFrameRow{T}(df::T, row::Integer) where T<:AbstractDataFrame
        if row isa Bool
            throw(ArgumentError("invalid index: $row of type Bool"))
        end
        @boundscheck if !checkindex(Bool, axes(df, 1), row)
            throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                              "rows at index $row"))
        end
        new{T}(df, row)
    end
end

@inline DataFrameRow(df::T, row::Integer) where {T<:AbstractDataFrame} =
    DataFrameRow{T}(df, row)

Base.parent(r::DataFrameRow) = getfield(r, :df)
Base.parentindices(r::DataFrameRow) = (row(r), axes(parent(r), 2))
row(r::DataFrameRow) = getfield(r, :row)

Base.view(adf::AbstractDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(adf, rowind)
Base.view(sdf::SubDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind])

# Here a corner case is when colinds=[] and we pass a valid rowind
# into adf. We will throw an error in this case.
# The consequence is that it is impossible to create a DataFrameRow without columns.
Base.view(adf::AbstractDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(adf[colinds], rowind)
Base.view(sdf::SubDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(parent(sdf)[colinds], rows(sdf)[rowind])

# Same here. It is impossible to create a DataFrameRow without columns.
Base.getindex(df::DataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(df[colinds], rowind)
Base.getindex(sdf::SubDataFrame, rowind::Integer, colinds::AbstractVector) =
    DataFrameRow(parent(sdf)[colinds], rows(sdf)[rowind])
Base.getindex(df::DataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(df, rowind)
Base.getindex(sdf::SubDataFrame, rowind::Integer, ::Colon) =
    DataFrameRow(parent(sdf), rows(sdf)[rowind])

Base.getindex(r::DataFrameRow, idx::ColumnIndex) = parent(r)[row(r), idx]
Base.getindex(r::DataFrameRow, idxs::AbstractVector) =
    DataFrameRow(parent(r)[idxs], row(r))
Base.getindex(r::DataFrameRow, ::Colon) = r

Base.setindex!(r::DataFrameRow, value::Any, idx::Any) =
    setindex!(parent(r), value, row(r), idx)

Base.names(r::DataFrameRow) = names(parent(r))
_names(r::DataFrameRow) = _names(parent(r))

Base.haskey(r::DataFrameRow, key::Any) = haskey(parent(r), key)

Base.getproperty(r::DataFrameRow, idx::Symbol) = getindex(r, idx)
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = setindex!(r, x, idx)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = names(r)

Base.view(r::DataFrameRow, col::ColumnIndex) = view(parent(r)[col], row(r))
Base.view(r::DataFrameRow, cols::AbstractVector) =
    DataFrameRow(parent(r)[cols], row(r))
Base.view(r::DataFrameRow, ::Colon) = r

index(r::DataFrameRow) = index(parent(r))
Base.size(r::DataFrameRow) = (size(parent(r), 2),)
Base.size(r::DataFrameRow, i) = size(r)[i]
Base.length(r::DataFrameRow) = size(parent(r), 2)
Base.ndims(r::DataFrameRow) = 1
Base.ndims(::Type{<:DataFrameRow}) = 1

Base.lastindex(r::DataFrameRow) = size(parent(r), 2)

Base.iterate(r::DataFrameRow) = iterate(r, 1)

function Base.iterate(r::DataFrameRow, st)
    st > length(r) && return nothing
    return (r[st], st + 1)
end

# Computing the element type requires going over all columns,
# so better let collect() do it only if necessary (widening)
Base.IteratorEltype(::DataFrameRow) = Base.EltypeUnknown()

function Base.convert(::Type{Vector}, dfr::DataFrameRow)
    T = reduce(promote_type, eltypes(parent(dfr)))
    convert(Vector{T}, dfr)
end
Base.convert(::Type{Vector{T}}, dfr::DataFrameRow) where T =
    T[dfr[i] for i in 1:length(dfr)]
Base.Vector(dfr::DataFrameRow) = convert(Vector, dfr)
Base.Vector{T}(dfr::DataFrameRow) where T = convert(Vector{T}, dfr)

Base.keys(r::DataFrameRow) = names(parent(r))
Base.values(r::DataFrameRow) = ntuple(col -> parent(r)[col][row(r)], length(r))

"""
    copy(dfr::DataFrameRow)

Convert a `DataFrameRow` to a `NamedTuple`.
"""
Base.copy(r::DataFrameRow) = NamedTuple{Tuple(keys(r))}(values(r))

# hash column element
Base.@propagate_inbounds hash_colel(v::AbstractArray, i, h::UInt = zero(UInt)) = hash(v[i], h)
Base.@propagate_inbounds function hash_colel(v::AbstractCategoricalArray, i, h::UInt = zero(UInt))
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
    rowhash(ntuple(i -> parent(r)[i], ncol(parent(r))), row(r), h)

# comparison of DataFrame rows
# only the rows of the same DataFrame could be compared
# rows are equal if they have the same values (while the row indices could differ)
# if all non-missing values are equal, but there are missings, returns missing
Base.:(==)(r1::DataFrameRow, r2::DataFrameRow) = isequal(r1, r2)

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    isequal_row(parent(r1), row(r1), parent(r2), row(r2))
end

# internal method for comparing the elements of the same data table column
isequal_colel(col::AbstractArray, r1::Int, r2::Int) =
    (r1 == r2) || isequal(Base.unsafe_getindex(col, r1), Base.unsafe_getindex(col, r2))

# table columns are passed as a tuple of vectors to ensure type specialization
isequal_row(cols::Tuple{AbstractVector}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2])
isequal_row(cols::Tuple{Vararg{AbstractVector}}, r1::Int, r2::Int) =
    isequal(cols[1][r1], cols[1][r2]) && isequal_row(Base.tail(cols), r1, r2)

isequal_row(cols1::Tuple{AbstractVector}, r1::Int, cols2::Tuple{AbstractVector}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2])
isequal_row(cols1::Tuple{Vararg{AbstractVector}}, r1::Int,
            cols2::Tuple{Vararg{AbstractVector}}, r2::Int) =
    isequal(cols1[1][r1], cols2[1][r2]) &&
        isequal_row(Base.tail(cols1), r1, Base.tail(cols2), r2)

function isequal_row(df1::AbstractDataFrame, r1::Int, df2::AbstractDataFrame, r2::Int)
    if df1 === df2
        if r1 == r2
            return true
        end
    elseif !(ncol(df1) == ncol(df2))
        throw(ArgumentError("Rows of the tables that have different number of columns cannot be compared. Got $(ncol(df1)) and $(ncol(df2)) columns"))
    end
    @inbounds for (col1, col2) in zip(columns(df1), columns(df2))
        isequal(col1[r1], col2[r2]) || return false
    end
    return true
end

# lexicographic ordering on DataFrame rows, missing > !missing
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
    (ncol(parent(r1)) == ncol(parent(r2))) ||
        throw(ArgumentError("Rows of the data tables that have different number of columns cannot be compared ($(ncol(df1)) and $(ncol(df2)))"))
    @inbounds for i in 1:ncol(parent(r1))
        if !isequal(parent(r1)[i][row(r1)], parent(r2)[i][row(r2)])
            return isless(parent(r1)[i][row(r1)], parent(r2)[i][row(r2)])
        end
    end
    return false
end
