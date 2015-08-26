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

Base.sub(r::DataFrameRow, c) = DataFrameRow(r.df[[c]], r.row)

index(r::DataFrameRow) = index(r.df)

Base.length(r::DataFrameRow) = size(r.df, 2)
Base.endof(r::DataFrameRow) = size(r.df, 2)
Base.start(r::DataFrameRow) = 1
Base.next(r::DataFrameRow, s) = ((_names(r)[s], r[s]), s + 1)
Base.done(r::DataFrameRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataFrameRow) = convert(Array, r.df[r.row,:])
Base.collect(r::DataFrameRow) = Tuple{Symbol, Any}[x for x in r]

# hash array element
# the equal elements of nullable and normal arrays would have the same hashes

const NULL_MAGIC = 0xBADDEED # what to hash if the element is null

unsafe_hashindex(v::AbstractArray, i, h::UInt = zero(UInt)) = hash(Base.unsafe_getindex(v, i), h)
unsafe_hashindex{T}(v::AbstractNullableArray{T}, i, h::UInt = zero(UInt)) =
    _isnull(v, i) ? hash(NULL_MAGIC, h) : hash(NullableArrays.unsafe_getvalue_notnull(v, i), h)
unsafe_hashindex{T}(v::AbstractCategoricalArray{T}, i, h::UInt = zero(UInt)) =
    hash(Base.unsafe_getindex(v.pool, Base.unsafe_getindex(v.refs, i)), h)
function unsafe_hashindex{T}(v::AbstractNullableCategoricalArray{T}, i, h::UInt = zero(UInt))
    ref = Base.unsafe_getindex(v.refs, i)
    ref == 0 ? hash(NULL_MAGIC, h) : hash(Base.unsafe_getindex(v.pool, ref), h)
end

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
function rowhash(df::DataFrame, r::Int, h::UInt = zero(UInt))
    @inbounds for col in columns(df)
        h = unsafe_hashindex(col, r, h)
    end
    return h
end

Base.hash(r::DataFrameRow, h::UInt = zero(UInt)) = rowhash(r.df, r.row, h)

# comparison of DataFrame rows
# rows are equal if they have the same values (while the row indices could differ)
# returns Nullable{Bool}
# if all non-null values are equal, but there are nulls, returns null
function @compat(Base.:(==))(r1::DataFrameRow, r2::DataFrameRow)
    if r1.df !== r2.df
        (ncol(r1.df) != ncol(r2.df)) &&
            throw(ArgumentError("Comparing rows from different frames not supported"))
        eq = Nullable(true)
        @inbounds for (col1, col2) in zip(columns(r1.df), columns(r2.df))
            eq_col = convert(Nullable{Bool}, col1[r1.row] == col2[r2.row])
            # If true or null, need to compare remaining columns
            get(eq_col, true) || return Nullable(false)
            eq &= eq_col
        end
        return eq
    else
    	r1.row == r2.row && return Nullable(true)
        eq = Nullable(true)
        @inbounds for col in columns(r1.df)
            eq_col = convert(Nullable{Bool}, col[r1.row] == col[r2.row])
            # If true or null, need to compare remaining columns
            get(eq_col, true) || return Nullable(false)
            eq &= eq_col
        end
        return eq
    end
end

# internal method for comparing the elements of the same data frame column
isequal_colel(col::AbstractArray, r1::Int, r2::Int) =
    (r1 == r2) || isequal(Base.unsafe_getindex(col, r1), Base.unsafe_getindex(col, r2))

function isequal_colel{T}(col::Union{AbstractNullableArray{T},
                                     AbstractNullableCategoricalArray{T}},
                                     r1::Int, r2::Int)
    (r1 == r2) && return true
    _isnull(col, r1) && return _isnull(col, r2)
    return !_isnull(col, r2) && isequal(NullableArrays.unsafe_getvalue_notnull(col, r1),
                                        NullableArrays.unsafe_getvalue_notnull(col, r2))
end

isequal_colel(a::Any, b::Any) = isequal(a, b)
isequal_colel(a::Nullable, b::Any) = !isnull(a) && isequal(get(a), b)
isequal_colel(a::Any, b::Nullable) = isequal_colel(b, a)
isequal_colel(a::Nullable, b::Nullable) = isnull(a)==isnull(b) && (isnull(a) || isequal(get(a), get(b)))

# comparison of DataFrame rows
function isequal_row(df::AbstractDataFrame, r1::Int, r2::Int)
    (r1 == r2) && return true # same raw
    @inbounds for col in columns(df)
        isequal_colel(col, r1, r2) || return false
    end
    return true
end

function isequal_row(df1::AbstractDataFrame, r1::Int, df2::AbstractDataFrame, r2::Int)
    (df1 === df2) && return isequal_row(df1, r1, r2)
    (ncol(df1) == ncol(df2)) ||
        throw(ArgumentError("Rows of the data frames that have different number of columns cannot be compared ($(ncol(df1)) and $(ncol(df2)))"))
    @inbounds for (col1, col2) in zip(columns(df1), columns(df2))
        isequal_colel(col1[r1], col2[r2]) || return false
    end
    return true
end

# comparison of DataFrame rows
# rows are equal if they have the same values (while the row indices could differ)
Base.isequal(r1::DataFrameRow, r2::DataFrameRow) =
    isequal_row(r1.df, r1.row, r2.df, r2.row)

# lexicographic ordering on DataFrame rows, null > !null
function Base.isless(r1::DataFrameRow, r2::DataFrameRow)
    (ncol(r1.df) == ncol(r2.df)) ||
        throw(ArgumentError("Rows of the data frames that have different number of columns cannot be compared ($(ncol(df1)) and $(ncol(df2)))"))
    @inbounds for i in 1:ncol(r1.df)
        col1 = r1.df[i]
        col2 = r2.df[i]
        isnull1 = _isnull(col1, r1.row)
        isnull2 = _isnull(col2, r2.row)
        (isnull1 != isnull2) && return isnull2 # null > !null
        if !isnull1
            v1 = NullableArrays.unsafe_getvalue_notnull(col1, r1.row)
            v2 = NullableArrays.unsafe_getvalue_notnull(col2, r2.row)
            isless(v1, v2) && return true
            !isequal(v1, v2) && return false
        end
    end
    return false
end
