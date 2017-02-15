# Container for a DataTable row
immutable DataTableRow{T <: AbstractDataTable}
    df::T
    row::Int
end

function Base.getindex(r::DataTableRow, idx::AbstractArray)
    return DataTableRow(r.df[idx], r.row)
end

function Base.getindex(r::DataTableRow, idx::Any)
    return r.df[r.row, idx]
end

function Base.setindex!(r::DataTableRow, value::Any, idx::Any)
    return setindex!(r.df, value, r.row, idx)
end

Base.names(r::DataTableRow) = names(r.df)
_names(r::DataTableRow) = _names(r.df)

Base.view(r::DataTableRow, c) = DataTableRow(r.df[[c]], r.row)

index(r::DataTableRow) = index(r.df)

Base.length(r::DataTableRow) = size(r.df, 2)

Base.endof(r::DataTableRow) = size(r.df, 2)

Base.collect(r::DataTableRow) = @compat Tuple{Symbol, Any}[x for x in r]

Base.start(r::DataTableRow) = 1

Base.next(r::DataTableRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataTableRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataTableRow) = convert(Array, r.df[r.row,:])

# hash of DataTable rows based on its values
# so that duplicate rows would have the same hash
function Base.hash(r::DataTableRow, h::UInt)
    for col in columns(r.df)
        if _isnull(col[r.row])
            h = hash(false, h)
        else
            h = hash(true, hash(col[r.row], h))
        end
    end
    return h
end

# comparison of DataTable rows
# only the rows of the same DataTable could be compared
# rows are equal if they have the same values (while the row indices could differ)
@compat(Base.:(==))(r1::DataTableRow, r2::DataTableRow) = isequal(r1, r2)

function Base.isequal(r1::DataTableRow, r2::DataTableRow)
    r1.df == r2.df || throw(ArgumentError("Comparing rows from different frames not supported"))
    r1.row == r2.row && return true
    for col in columns(r1.df)
        if !isequal(col[r1.row], col[r2.row])
            return false
        end
    end
    return true
end
