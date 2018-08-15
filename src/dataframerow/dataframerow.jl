# Container for a DataFrame row
struct DataFrameRow{T <: AbstractDataFrame}
    df::T
    row::Int
end


"""
    parent(r::DataFrameRow)

Return the parent data frame of `r`.
"""
Base.parent(r::DataFrameRow) = getfield(r, :df)
row(r::DataFrameRow) = getfield(r, :row)

function Base.getindex(r::DataFrameRow, idx::AbstractArray)
    return DataFrameRow(parent(r)[idx], row(r))
end

function Base.getindex(r::DataFrameRow, idx::Any)
    return parent(r)[row(r), idx]
end

function Base.setindex!(r::DataFrameRow, value::Any, idx::Any)
    return setindex!(parent(r), value, row(r), idx)
end

Base.names(r::DataFrameRow) = names(parent(r))
_names(r::DataFrameRow) = _names(parent(r))

Base.getproperty(r::DataFrameRow, idx::Symbol) = getindex(r, idx)
Base.setproperty!(r::DataFrameRow, idx::Symbol, x::Any) = setindex!(r, x, idx)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(r::DataFrameRow, private::Bool=false) = names(r)

Base.view(r::DataFrameRow, c) = DataFrameRow(parent(r)[[c]], row(r))

index(r::DataFrameRow) = index(parent(r))

Base.length(r::DataFrameRow) = size(parent(r), 2)

Compat.lastindex(r::DataFrameRow) = size(parent(r), 2)

Base.collect(r::DataFrameRow) = Tuple{Symbol, Any}[x for x in r]

function Base.iterate(r::DataFrameRow, st=1)
    st > length(r) && return nothing
    return ((_names(r)[st], r[st]), st + 1)
end

Base.convert(::Type{Array}, r::DataFrameRow) = convert(Array, parent(r)[row(r),:])

# hash column element
Base.@propagate_inbounds hash_colel(v::AbstractArray, i, h::UInt = zero(UInt)) = hash(v[i], h)
Base.@propagate_inbounds hash_colel(v::AbstractCategoricalArray, i, h::UInt = zero(UInt)) =
    hash(CategoricalArrays.index(v.pool)[v.refs[i]], h)
Base.@propagate_inbounds function hash_colel(v::AbstractCategoricalArray{>: Missing}, i, h::UInt = zero(UInt))
    ref = v.refs[i]
    ref == 0 ? hash(missing, h) : hash(CategoricalArrays.index(v.pool)[ref], h)
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
