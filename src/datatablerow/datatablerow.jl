# Container for a DataTable row
immutable DataTableRow{T <: AbstractDataTable}
    dt::T
    row::Int
end

function Base.getindex(r::DataTableRow, idx::AbstractArray)
    return DataTableRow(r.dt[idx], r.row)
end

function Base.getindex(r::DataTableRow, idx::Any)
    return r.dt[r.row, idx]
end

function Base.setindex!(r::DataTableRow, value::Any, idx::Any)
    return setindex!(r.dt, value, r.row, idx)
end

Base.names(r::DataTableRow) = names(r.dt)
_names(r::DataTableRow) = _names(r.dt)

Base.view(r::DataTableRow, c) = DataTableRow(r.dt[[c]], r.row)

index(r::DataTableRow) = index(r.dt)

Base.length(r::DataTableRow) = size(r.dt, 2)

Base.endof(r::DataTableRow) = size(r.dt, 2)

Base.collect(r::DataTableRow) = Tuple{Symbol, Any}[x for x in r]

Base.start(r::DataTableRow) = 1

Base.next(r::DataTableRow, s) = ((_names(r)[s], r[s]), s + 1)

Base.done(r::DataTableRow, s) = s > length(r)

Base.convert(::Type{Array}, r::DataTableRow) = convert(Array, r.dt[r.row,:])

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

# hash of DataTable rows based on its values
# so that duplicate rows would have the same hash
# table columns are passed as a tuple of vectors to ensure type specialization
rowhash(cols::Tuple{AbstractVector}, r::Int, h::UInt = zero(UInt))::UInt =
    hash_colel(cols[1], r, h)
function rowhash(cols::Tuple{Vararg{AbstractVector}}, r::Int, h::UInt = zero(UInt))::UInt
    h = hash_colel(cols[1], r, h)
    rowhash(Base.tail(cols), r, h)
end

Base.hash(r::DataTableRow, h::UInt = zero(UInt)) =
    rowhash(ntuple(i -> r.dt[i], ncol(r.dt)), r.row, h)

# comparison of DataTable rows
# only the rows of the same DataTable could be compared
# rows are equal if they have the same values (while the row indices could differ)
# returns Nullable{Bool}
# if all non-null values are equal, but there are nulls, returns null
@compat(Base.:(==))(r1::DataTableRow, r2::DataTableRow) = isequal(r1, r2)

function Base.isequal(r1::DataTableRow, r2::DataTableRow)
    isequal_row(r1.dt, r1.row, r2.dt, r2.row)
end

# internal method for comparing the elements of the same data table column
isequal_colel(col::AbstractArray, r1::Int, r2::Int) =
    (r1 == r2) || isequal(Base.unsafe_getindex(col, r1), Base.unsafe_getindex(col, r2))

isequal_colel(a::Any, b::Any) = isequal(a, b)
isequal_colel(a::Nullable, b::Any) = !isnull(a) & isequal(unsafe_get(a), b)
isequal_colel(a::Any, b::Nullable) = isequal_colel(b, a)
isequal_colel(a::Nullable, b::Nullable) = isequal(a, b)

# table columns are passed as a tuple of vectors to ensure type specialization
isequal_row(cols::Tuple{AbstractVector}, r1::Int, r2::Int) =
    isequal_colel(cols[1][r1], cols[1][r2])
isequal_row(cols::Tuple{Vararg{AbstractVector}}, r1::Int, r2::Int) =
    isequal_colel(cols[1][r1], cols[1][r2]) && isequal_row(Base.tail(cols), r1, r2)

isequal_row(cols1::Tuple{AbstractVector}, r1::Int, cols2::Tuple{AbstractVector}, r2::Int) =
    isequal_colel(cols1[1][r1], cols2[1][r2])
isequal_row(cols1::Tuple{Vararg{AbstractVector}}, r1::Int,
            cols2::Tuple{Vararg{AbstractVector}}, r2::Int) =
    isequal_colel(cols1[1][r1], cols2[1][r2]) &&
        isequal_row(Base.tail(cols1), r1, Base.tail(cols2), r2)

function isequal_row(dt1::AbstractDataTable, r1::Int, dt2::AbstractDataTable, r2::Int)
    if dt1 === dt2
        if r1 == r2
            return true
        end
    elseif !(ncol(dt1) == ncol(dt2))
        throw(ArgumentError("Rows of the tables that have different number of columns cannot be compared. Got $(ncol(dt1)) and $(ncol(dt2)) columns"))
    end
    @inbounds for (col1, col2) in zip(columns(dt1), columns(dt2))
        isequal_colel(col1[r1], col2[r2]) || return false
    end
    return true
end

# lexicographic ordering on DataTable rows, null > !null
function Base.isless(r1::DataTableRow, r2::DataTableRow)
    (ncol(r1.dt) == ncol(r2.dt)) ||
        throw(ArgumentError("Rows of the data tables that have different number of columns cannot be compared ($(ncol(dt1)) and $(ncol(dt2)))"))
    @inbounds for i in 1:ncol(r1.dt)
        x = r1.dt[i][r1.row]
        y = r2.dt[i][r2.row]
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
