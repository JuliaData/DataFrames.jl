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

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
function Base.hash(r::DataFrameRow, h::UInt)
    for col in columns(r.df)
        if _isnull(col[r.row])
            h = hash(false, h)
        else
            h = hash(true, hash(col[r.row], h))
        end
    end
    return h
end

# comparison of DataFrame rows
# rows are equal if they have the same values (while the row indices could differ)
# returns Nullable{Bool}
# if all non-null values are equal, but there are nulls, returns null
function @compat(Base.:(==))(r1::DataFrameRow, r2::DataFrameRow)
    if r1.df !== r2.df
        ncol(r1.df) != ncol(r2.df) &&
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

# comparison of DataFrame rows
# rows are equal if they have the same values (while the row indices could differ)
# if both columns have null, they are considered equal
function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    if r1.df !== r2.df
        ncol(r1.df) != ncol(r2.df) &&
            throw(ArgumentError("Comparing rows from different frames not supported"))
        @inbounds for (col1, col2) in zip(columns(r1.df), columns(r2.df))
            isequal(col1[r1.row], col2[r2.row]) || return false
        end
        return true
    else
    	r1.row == r2.row && return true
        @inbounds for col in columns(r1.df)
            isequal(col[r1.row], col[r2.row]) || return false
        end
        return true
    end
end
