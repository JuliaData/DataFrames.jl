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

Base.collect(r::DataFrameRow) = @compat Tuple{Symbol, Any}[x for x in r]

Base.start(r::DataFrameRow) = 1

Base.next(r::DataFrameRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataFrameRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataFrameRow) = convert(Array, r.df[r.row,:])

# hash of DataFrame rows based on its values
# so that duplicate rows would have the same hash
function Base.hash(r::DataFrameRow, h::UInt)
    for col in columns(r.df)
        if isnull(col, r.row)
            h = hash(false, h)
        else
            h = hash(true, hash(col[r.row], h))
        end
    end
    return h
end

# comparison of DataFrame rows
# only the rows of the same DataFrame could be compared
# rows are equal if they have the same values (while the row indices could differ)
function Base.:(==)(r1::DataFrameRow, r2::DataFrameRow)
    if r1.df !== r2.df
        throw(ArgumentError("Comparing rows from different frames not supported"))
    end
    if r1.row == r2.row
        return Nullable(true)
    end
    eq = Nullable(true)
    for col in columns(r1.df)
        eq_col = convert(Nullable{Bool}, col[r1.row] == col[r2.row])
        # If true or null, need to compare remaining columns
        get(eq_col, true) || return Nullable(false)
        eq &= eq_col
    end
    return eq
end

function Base.isequal(r1::DataFrameRow, r2::DataFrameRow)
    if r1.df !== r2.df
        throw(ArgumentError("Comparing rows from different frames not supported"))
    end
    if r1.row == r2.row
        return true
    end
    for col in columns(r1.df)
        if !isequal(col[r1.row], col[r2.row])
            return false
        end
    end
    return true
end
