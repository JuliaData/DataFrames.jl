# Container for a DataFrame row
immutable DataFrameRow{T <: AbstractDataFrame}
    df::T
    row::Int
end

function Base.getindex(r::DataFrameRow, idx::AbstractArray)
    return DataFrameRow(r.df[idx], r.row)
end

function Base.getindex(r::DataFrameRow, idx::Any)
    return r.df[r.row, idx]
end

function Base.setindex!(r::DataFrameRow, value::Any, idx::Any)
    return setindex!(r.df, value, r.row, idx)
end

Base.names(r::DataFrameRow) = names(r.df)
_names(r::DataFrameRow) = _names(r.df)

Base.view(r::DataFrameRow, c) = DataFrameRow(r.df[[c]], r.row)

index(r::DataFrameRow) = index(r.df)

Base.length(r::DataFrameRow) = size(r.df, 2)

Base.endof(r::DataFrameRow) = size(r.df, 2)

Base.collect(r::DataFrameRow) = Tuple{Symbol, Any}[x for x in r]

Base.start(r::DataFrameRow) = 1

Base.next(r::DataFrameRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataFrameRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataFrameRow) = convert(Array, r.df[r.row,:])

# hash column element
Base.@propagate_inbounds hash_colel(v::AbstractArray, i, h::UInt = zero(UInt)) = hash(v[i], h)
Base.@propagate_inbounds hash_colel{T<:Nullable}(v::AbstractArray{T}, i, h::UInt = zero(UInt)) =
    isnull(v[i]) ? h + Base.nullablehash_seed : hash(unsafe_get(v[i]), h)
Base.@propagate_inbounds hash_colel{T}(v::NullableArray{T}, i, h::UInt = zero(UInt)) =
    isnull(v, i) ? h + Base.nullablehash_seed : hash(v.values[i], h)
Base.@propagate_inbounds hash_colel{T}(v::AbstractCategoricalArray{T}, i, h::UInt = zero(UInt)) =
    hash(CategoricalArrays.index(v.pool)[v.refs[i]], h)
Base.@propagate_inbounds function hash_colel{T}(v::AbstractNullableCategoricalArray{T}, i, h::UInt = zero(UInt))
    ref = v.refs[i]
    ref == 0 ? h + Base.nullablehash_seed : hash(CategoricalArrays.index(v.pool)[ref], h)
end

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
function rowhash(df::DataFrame, r::Int, h::UInt = zero(UInt))
    @inbounds for col in columns(df)
        h = hash_colel(col, r, h)
    end
    return h
end

Base.hash(r::DataFrameRow, h::UInt = zero(UInt)) = rowhash(r.df, r.row, h)

# comparison of DataFrame rows
# only the rows of the same DataFrame could be compared
# rows are equal if they have the same values (while the row indices could differ)
# returns Nullable{Bool}
# if all non-null values are equal, but there are nulls, returns null
@compat(Base.:(==))(r1::DataFrameRow, r2::DataFrameRow) = isequal(r1, r2)

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    isequal_row(r1.df, r1.row, r2.df, r2.row)
end

# internal method for comparing the elements of the same data frame column
isequal_colel(col::AbstractArray, r1::Int, r2::Int) =
    (r1 == r2) || isequal(Base.unsafe_getindex(col, r1), Base.unsafe_getindex(col, r2))

isequal_colel(a::Any, b::Any) = isequal(a, b)
isequal_colel(a::Nullable, b::Any) = !isnull(a) & isequal(unsafe_get(a), b)
isequal_colel(a::Any, b::Nullable) = isequal_colel(b, a)
isequal_colel(a::Nullable, b::Nullable) = isequal(a, b)

function isequal_row(df1::AbstractDataFrame, r1::Int, df2::AbstractDataFrame, r2::Int)
    if df1 === df2
        if r1 == r2
            return true
        end
    elseif !(ncol(df1) == ncol(df2))
        throw(ArgumentError("Rows of the tables that have different number of columns cannot be compared. Got $(ncol(df1)) and $(ncol(df2)) columns"))
    end
    @inbounds for (col1, col2) in zip(columns(df1), columns(df2))
        isequal_colel(col1[r1], col2[r2]) || return false
    end
    return true
end

# lexicographic ordering on DataFrame rows, null > !null
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
    (ncol(r1.df) == ncol(r2.df)) ||
        throw(ArgumentError("Rows of the data frames that have different number of columns cannot be compared ($(ncol(df1)) and $(ncol(df2)))"))
    @inbounds for i in 1:ncol(r1.df)
        x = r1.df[i][r1.row]
        y = r2.df[i][r2.row]
        isnullx = _isnull(x)
        isnully = _isnull(y)
        (isnullx != isnully) && return isnully # null > !null
        if !isnullx
            v1 = unsafe_get(x)
            v2 = unsafe_get(y)
            isless(v1, v2) && return true
            !isequal(v1, v2) && return false
        end
    end
    return false
end
