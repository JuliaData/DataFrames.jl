"""
An AbstractDataTable that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector, NullableVector, or CategoricalVector.

**Constructors**

```julia
DataTable(columns::Vector{Any}, names::Vector{Symbol})
DataTable(kwargs...)
DataTable() # an empty DataTable
DataTable(t::Type, nrows::Integer, ncols::Integer) # an empty DataTable of arbitrary size
DataTable(column_eltypes::Vector, names::Vector, nrows::Integer)
DataTable(ds::Vector{Associative})
```

**Arguments**

* `columns` : a Vector{Any} with each column as contents
* `names` : the column names
* `kwargs` : the key gives the column names, and the value is the
  column contents
* `t` : elemental type of all columns
* `nrows`, `ncols` : number of rows and columns
* `column_eltypes` : elemental type of each column
* `ds` : a vector of Associatives

Each column in `columns` should be the same length.

**Notes**

Most of the default constructors convert columns to `NullableArray`.  The
base constructor, `DataTable(columns::Vector{Any},
names::Vector{Symbol})` does not convert to `NullableArray`.

A `DataTable` is a lightweight object. As long as columns are not
manipulated, creation of a DataTable from existing AbstractVectors is
inexpensive. For example, indexing on columns is inexpensive, but
indexing by rows is expensive because copies are made of each column.

Because column types can vary, a DataTable is not type stable. For
performance-critical code, do not index into a DataTable inside of
loops.

**Examples**

```julia
dt = DataTable()
v = ["x","y","z"][rand(1:3, 10)]
dt1 = DataTable(Any[collect(1:10), v, rand(10)], [:A, :B, :C])  # columns are Arrays
dt2 = DataTable(A = 1:10, B = v, C = rand(10))           # columns are NullableArrays
dump(dt1)
dump(dt2)
describe(dt2)
DataTables.head(dt1)
dt1[:A] + dt2[:C]
dt1[1:4, 1:2]
dt1[[:A,:C]]
dt1[1:2, [:A,:C]]
dt1[:, [:A,:C]]
dt1[:, [1,3]]
dt1[1:4, :]
dt1[1:4, :C]
dt1[1:4, :C] = 40. * dt1[1:4, :C]
[dt1; dt2]  # vcat
[dt1  dt2]  # hcat
size(dt1)
```

"""
type DataTable <: AbstractDataTable
    columns::Vector{Any}
    colindex::Index

    function DataTable(columns::Vector{Any}, colindex::Index)
        ncols = length(columns)
        if ncols > 1
            nrows = length(columns[1])
            equallengths = true
            for i in 2:ncols
                equallengths &= length(columns[i]) == nrows
            end
            if !equallengths
                msg = "All columns in a DataTable must be the same length"
                throw(ArgumentError(msg))
            end
        end
        if length(colindex) != ncols
            msg = "Columns and column index must be the same length"
            throw(ArgumentError(msg))
        end
        new(columns, colindex)
    end
end

function DataTable(; kwargs...)
    result = DataTable(Any[], Index())
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end

function DataTable(columns::AbstractVector,
                   cnames::AbstractVector{Symbol} = gennames(length(columns)))
    return DataTable(convert(Vector{Any}, columns), Index(convert(Vector{Symbol}, cnames)))
end


# Initialize empty DataTable objects of arbitrary size
function DataTable(t::Type, nrows::Integer, ncols::Integer)
    columns = Vector{Any}(ncols)
    for i in 1:ncols
        columns[i] = NullableArray(t, nrows)
    end
    cnames = gennames(ncols)
    return DataTable(columns, Index(cnames))
end

# Initialize an empty DataTable with specific eltypes and names
function DataTable(column_eltypes::Vector, cnames::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Vector{Any}(p)
    for j in 1:p
        columns[j] = NullableArray(column_eltypes[j], nrows)
    end
    return DataTable(columns, Index(cnames))
end
# Initialize an empty DataTable with specific eltypes and names
# and whether a nominal array should be created
function DataTable(column_eltypes::Vector{DataType}, cnames::Vector{Symbol},
                   nominal::Vector{Bool}, nrows::Integer)
    p = length(column_eltypes)
    columns = Vector{Any}(p)
    for j in 1:p
      if nominal[j]
        columns[j] = NullableCategoricalArray{column_eltypes[j]}(nrows)
      else
        columns[j] = NullableArray{column_eltypes[j]}(nrows)
      end
    end
    return DataTable(columns, Index(cnames))
end

# Initialize an empty DataTable with specific eltypes
function DataTable(column_eltypes::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Vector{Any}(p)
    cnames = gennames(p)
    for j in 1:p
        columns[j] = NullableArray{column_eltypes[j]}(nrows)
    end
    return DataTable(columns, Index(cnames))
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataTable{D <: Associative}(ds::Vector{D})
    ks = Set()
    for d in ds
        union!(ks, keys(d))
    end
    DataTable(ds, [ks...])
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataTable{D <: Associative}(ds::Vector{D}, ks::Vector)
    #get column eltypes
    col_eltypes = Type[@compat(Union{}) for _ = 1:length(ks)]
    for d in ds
        for (i,k) in enumerate(ks)
            if haskey(d, k) && !_isnull(d[k])
                col_eltypes[i] = promote_type(col_eltypes[i], typeof(d[k]))
            end
        end
    end
    col_eltypes[col_eltypes .== @compat(Union{})] = Any

    # create empty DataTable, and fill
    dt = DataTable(col_eltypes, ks, length(ds))
    for (i,d) in enumerate(ds)
        for (j,k) in enumerate(ks)
            dt[i,j] = get(d, k, Nullable())
        end
    end

    dt
end

##############################################################################
##
## AbstractDataTable interface
##
##############################################################################

index(dt::DataTable) = dt.colindex
columns(dt::DataTable) = dt.columns

# TODO: Remove these
nrow(dt::DataTable) = ncol(dt) > 0 ? length(dt.columns[1])::Int : 0
ncol(dt::DataTable) = length(index(dt))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# Cases:
#
# dt[SingleColumnIndex] => AbstractDataVector
# dt[MultiColumnIndex] => DataTable
# dt[SingleRowIndex, SingleColumnIndex] => Scalar
# dt[SingleRowIndex, MultiColumnIndex] => DataTable
# dt[MultiRowIndex, SingleColumnIndex] => AbstractVector
# dt[MultiRowIndex, MultiColumnIndex] => DataTable
#
# General Strategy:
#
# Let getindex(index(dt), col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(dt.columns[j], row_inds) from AbstractVector() handle
#  the resolution of row indices

@compat const ColumnIndex = Union{Real, Symbol}

# dt[SingleColumnIndex] => AbstractDataVector
function Base.getindex(dt::DataTable, col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column]
end

# dt[MultiColumnIndex] => DataTable
function Base.getindex{T <: ColumnIndex}(dt::DataTable,
                                         col_inds::Union{AbstractVector{T},
                                                         AbstractVector{Nullable{T}}})
    selected_columns = index(dt)[col_inds]
    new_columns = dt.columns[selected_columns]
    return DataTable(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[:] => DataTable
Base.getindex(dt::DataTable, col_inds::Colon) = copy(dt)

# dt[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(dt::DataTable, row_ind::Real, col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column][row_ind]
end

# dt[SingleRowIndex, MultiColumnIndex] => DataTable
function Base.getindex{T <: ColumnIndex}(dt::DataTable,
                                         row_ind::Real,
                                         col_inds::Union{AbstractVector{T},
                                                         AbstractVector{Nullable{T}}})
    selected_columns = index(dt)[col_inds]
    new_columns = Any[dv[[row_ind]] for dv in dt.columns[selected_columns]]
    return DataTable(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[MultiRowIndex, SingleColumnIndex] => AbstractVector
function Base.getindex{T <: Real}(dt::DataTable,
                                  row_inds::Union{AbstractVector{T}, AbstractVector{Nullable{T}}},
                                  col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column][row_inds]
end

# dt[MultiRowIndex, MultiColumnIndex] => DataTable
function Base.getindex{R <: Real, T <: ColumnIndex}(dt::DataTable,
                                                    row_inds::Union{AbstractVector{R},
                                                                    AbstractVector{Nullable{R}}},
                                                    col_inds::Union{AbstractVector{T},
                                                                    AbstractVector{Nullable{T}}})
    selected_columns = index(dt)[col_inds]
    new_columns = Any[dv[row_inds] for dv in dt.columns[selected_columns]]
    return DataTable(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[:, SingleColumnIndex] => AbstractVector
# dt[:, MultiColumnIndex] => DataTable
Base.getindex{T<:ColumnIndex}(dt::DataTable,
                              row_inds::Colon,
                              col_inds::Union{T, AbstractVector{T},
                                              AbstractVector{Nullable{T}}}) =
    dt[col_inds]

# dt[SingleRowIndex, :] => DataTable
Base.getindex(dt::DataTable, row_ind::Real, col_inds::Colon) = dt[[row_ind], col_inds]

# dt[MultiRowIndex, :] => DataTable
function Base.getindex{R<:Real}(dt::DataTable,
                                row_inds::Union{AbstractVector{R},
                                                AbstractVector{Nullable{R}}},
                                col_inds::Colon)
    new_columns = Any[dv[row_inds] for dv in dt.columns]
    return DataTable(new_columns, copy(index(dt)))
end

# dt[:, :] => (Sub)?DataTable
Base.getindex(dt::DataTable, ::Colon, ::Colon) = copy(dt)

##############################################################################
##
## setindex!()
##
##############################################################################

isnextcol(dt::DataTable, col_ind::Symbol) = true
function isnextcol(dt::DataTable, col_ind::Real)
    return ncol(dt) + 1 == @compat Int(col_ind)
end

function nextcolname(dt::DataTable)
    return @compat(Symbol(string("x", ncol(dt) + 1)))
end

# Will automatically add a new column if needed
function insert_single_column!(dt::DataTable,
                               dv::AbstractVector,
                               col_ind::ColumnIndex)

    if ncol(dt) != 0 && nrow(dt) != length(dv)
        error("New columns must have the same length as old columns")
    end
    if haskey(index(dt), col_ind)
        j = index(dt)[col_ind]
        dt.columns[j] = dv
    else
        if typeof(col_ind) <: Symbol
            push!(index(dt), col_ind)
            push!(dt.columns, dv)
        else
            if isnextcol(dt, col_ind)
                push!(index(dt), nextcolname(dt))
                push!(dt.columns, dv)
            else
                error("Cannot assign to non-existent column: $col_ind")
            end
        end
    end
    return dv
end

function insert_single_entry!(dt::DataTable, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        dt.columns[index(dt)[col_ind]][row_ind] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function insert_multiple_entries!{T <: Real}(dt::DataTable,
                                            v::Any,
                                            row_inds::AbstractVector{T},
                                            col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        dt.columns[index(dt)[col_ind]][row_inds] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

upgrade_vector{T<:Nullable}(v::AbstractArray{T}) = v
upgrade_vector(v::CategoricalArray) = NullableCategoricalArray(v)
upgrade_vector(v::AbstractArray) = NullableArray(v)

function upgrade_scalar(dt::DataTable, v::AbstractArray)
    msg = "setindex!(::DataTable, ...) only broadcasts scalars, not arrays"
    throw(ArgumentError(msg))
end
function upgrade_scalar(dt::DataTable, v::Any)
    n = (ncol(dt) == 0) ? 1 : nrow(dt)
    NullableArray(fill(v, n))
end

# dt[SingleColumnIndex] = AbstractVector
function Base.setindex!(dt::DataTable,
                v::AbstractVector,
                col_ind::ColumnIndex)
    insert_single_column!(dt, upgrade_vector(v), col_ind)
end

# dt[SingleColumnIndex] = Single Item (EXPANDS TO NROW(DT) if NCOL(DT) > 0)
function Base.setindex!(dt::DataTable, v, col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        fill!(dt[col_ind], v)
    else
        insert_single_column!(dt, upgrade_scalar(dt, v), col_ind)
    end
    return dt
end

# dt[MultiColumnIndex] = DataTable
function Base.setindex!(dt::DataTable,
                new_dt::DataTable,
                col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  new_dt::DataTable,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_single_column!(dt, new_dt[j], col_inds[j])
    end
    return dt
end

# dt[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function Base.setindex!(dt::DataTable,
                v::AbstractVector,
                col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  v::AbstractVector,
                                  col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for col_ind in col_inds
        dt[col_ind] = dv
    end
    return dt
end

# dt[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(DT) if NCOL(DT) > 0)
function Base.setindex!(dt::DataTable,
                val::Any,
                col_inds::AbstractVector{Bool})
    setindex!(dt, val, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  val::Any,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        dt[col_ind] = val
    end
    return dt
end

# dt[:] = AbstractVector or Single Item
Base.setindex!(dt::DataTable, v, ::Colon) = (dt[1:size(dt, 2)] = v; dt)

# dt[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(dt::DataTable,
                v::Any,
                row_ind::Real,
                col_ind::ColumnIndex)
    insert_single_entry!(dt, v, row_ind, col_ind)
end

# dt[SingleRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(dt::DataTable,
                v::Any,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_ind, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  v::Any,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_single_entry!(dt, v, row_ind, col_ind)
    end
    return dt
end

# dt[SingleRowIndex, MultiColumnIndex] = 1-Row DataTable
function Base.setindex!(dt::DataTable,
                new_dt::DataTable,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, row_ind, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  new_dt::DataTable,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_single_entry!(dt, new_dt[j][1], row_ind, col_inds[j])
    end
    return dt
end

# dt[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(dt::DataTable,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(dt, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(dt::DataTable,
                           v::AbstractVector,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    insert_multiple_entries!(dt, v, row_inds, col_ind)
    return dt
end

# dt[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(dt::DataTable,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(dt, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(dt::DataTable,
                           v::Any,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    insert_multiple_entries!(dt, v, row_inds, col_ind)
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = DataTable
function Base.setindex!(dt::DataTable,
                new_dt::DataTable,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  new_dt::DataTable,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(dt, new_dt, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(dt::DataTable,
                           new_dt::DataTable,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(dt::DataTable,
                                             new_dt::DataTable,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_multiple_entries!(dt, new_dt[:, j], row_inds, col_inds[j])
    end
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(dt::DataTable,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  v::AbstractVector,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(dt, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(dt::DataTable,
                           v::AbstractVector,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(dt::DataTable,
                                             v::AbstractVector,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_multiple_entries!(dt, v, row_inds, col_ind)
    end
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(dt::DataTable,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(dt::DataTable,
                                  v::Any,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(dt, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(dt::DataTable,
                           v::Any,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(dt::DataTable,
                                             v::Any,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_multiple_entries!(dt, v, row_inds, col_ind)
    end
    return dt
end

# dt[:] = DataTable, dt[:, :] = DataTable
function Base.setindex!(dt::DataTable,
                                  new_dt::DataTable,
                                  row_inds::Colon,
                                  col_inds::Colon=Colon())
    dt.columns = copy(new_dt.columns)
    dt.colindex = copy(new_dt.colindex)
    dt
end

# dt[:, :] = ...
Base.setindex!(dt::DataTable, v, ::Colon, ::Colon) =
    (dt[1:size(dt, 1), 1:size(dt, 2)] = v; dt)

# dt[Any, :] = ...
Base.setindex!(dt::DataTable, v, row_inds, ::Colon) =
    (dt[row_inds, 1:size(dt, 2)] = v; dt)

# dt[:, Any] = ...
Base.setindex!(dt::DataTable, v, ::Colon, col_inds) =
    (dt[col_inds] = v; dt)

# Special deletion assignment
Base.setindex!(dt::DataTable, x::Void, col_ind::Int) = delete!(dt, col_ind)

##############################################################################
##
## Mutating Associative methods
##
##############################################################################

Base.empty!(dt::DataTable) = (empty!(dt.columns); empty!(index(dt)); dt)

function Base.insert!(dt::DataTable, col_ind::Int, item::AbstractVector, name::Symbol)
    0 < col_ind <= ncol(dt) + 1 || throw(BoundsError())
    size(dt, 1) == length(item) || size(dt, 1) == 0 || error("number of rows does not match")

    insert!(index(dt), col_ind, name)
    insert!(dt.columns, col_ind, item)
    dt
end

# FIXME: Needed to work around a crash: JuliaLang/julia#18299
function Base.insert!(dt::DataTable, col_ind::Int, item::NullableArray, name::Symbol)
    0 < col_ind <= ncol(dt) + 1 || throw(BoundsError())
    size(dt, 1) == length(item) || size(dt, 1) == 0 || error("number of rows does not match")

    insert!(index(dt), col_ind, name)
    insert!(dt.columns, col_ind, item)
    dt
end

function Base.insert!(dt::DataTable, col_ind::Int, item, name::Symbol)
    insert!(dt, col_ind, upgrade_scalar(dt, item), name)
end

function Base.merge!(dt::DataTable, others::AbstractDataTable...)
    for other in others
        for n in _names(other)
            dt[n] = other[n]
        end
    end
    return dt
end

##############################################################################
##
## Copying
##
##############################################################################

# A copy of a DataTable points to the original column vectors but
#   gets its own Index.
Base.copy(dt::DataTable) = DataTable(copy(columns(dt)), copy(index(dt)))

# Deepcopy is recursive -- if a column is a vector of DataTables, each of
#   those DataTables is deepcopied.
function Base.deepcopy(dt::DataTable)
    DataTable(deepcopy(columns(dt)), deepcopy(index(dt)))
end

##############################################################################
##
## Deletion / Subsetting
##
##############################################################################

# delete!() deletes columns; deleterows!() deletes rows
# delete!(dt, 1)
# delete!(dt, :Old)
function Base.delete!(dt::DataTable, inds::Vector{Int})
    for ind in sort(inds, rev = true)
        if 1 <= ind <= ncol(dt)
            splice!(dt.columns, ind)
            delete!(index(dt), ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataTable column"))
        end
    end
    return dt
end
Base.delete!(dt::DataTable, c::Int) = delete!(dt, [c])
Base.delete!(dt::DataTable, c::Any) = delete!(dt, index(dt)[c])

# deleterows!()
function deleterows!(dt::DataTable, ind::@compat(Union{Integer, UnitRange{Int}}))
    for i in 1:ncol(dt)
        dt.columns[i] = deleteat!(dt.columns[i], ind)
    end
    dt
end

function deleterows!(dt::DataTable, ind::AbstractVector{Int})
    ind2 = sort(ind)
    n = size(dt, 1)

    idt = 1
    iind = 1
    ikeep = 1
    keep = Array{Int}(n-length(ind2))
    while idt <= n && iind <= length(ind2)
        1 <= ind2[iind] <= n || error(BoundsError())
        if idt == ind2[iind]
            iind += 1
        else
            keep[ikeep] = idt
            ikeep += 1
        end
        idt += 1
    end
    keep[ikeep:end] = idt:n

    for i in 1:ncol(dt)
        dt.columns[i] = dt.columns[i][keep]
    end
    dt
end

##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments
function hcat!(dt1::DataTable, dt2::AbstractDataTable)
    u = add_names(index(dt1), index(dt2))
    for i in 1:length(u)
        dt1[u[i]] = dt2[i]
    end

    return dt1
end
hcat!(dt::DataTable, x::CategoricalArray) = hcat!(dt, DataTable(Any[x]))
hcat!(dt::DataTable, x::NullableCategoricalArray) = hcat!(dt, DataTable(Any[x]))
hcat!(dt::DataTable, x::NullableVector) = hcat!(dt, DataTable(Any[x]))
hcat!(dt::DataTable, x::Vector) = hcat!(dt, DataTable(Any[NullableArray(x)]))
hcat!(dt::DataTable, x) = hcat!(dt, DataTable(Any[NullableArray([x])]))

# hcat! for 1-n arguments
hcat!(dt::DataTable) = dt
hcat!(a::DataTable, b, c...) = hcat!(hcat!(a, b), c...)

# hcat
Base.hcat(dt::DataTable, x) = hcat!(copy(dt), x)

##############################################################################
##
## Nullability
##
##############################################################################

function nullable!(dt::DataTable, col::ColumnIndex)
    dt[col] = NullableArray(dt[col])
    dt
end
function nullable!{T <: ColumnIndex}(dt::DataTable, cols::Vector{T})
    for col in cols
        nullable!(dt, col)
    end
    dt
end

##############################################################################
##
## Pooling
##
##############################################################################

function categorical!(dt::DataTable, cname::@compat(Union{Integer, Symbol}), compact::Bool=true)
    dt[cname] = categorical(dt[cname], compact)
    return
end

function categorical!{T <: @compat(Union{Integer, Symbol})}(dt::DataTable, cnames::Vector{T},
                                                            compact::Bool=true)
    for cname in cnames
        dt[cname] = categorical(dt[cname], compact)
    end
    return
end

function categorical!(dt::DataTable, compact::Bool=true)
    for i in 1:size(dt, 2)
        if eltype(dt[i]) <: AbstractString
            dt[i] = categorical(dt[i], compact)
        end
    end
    return
end

function Base.append!(dt1::DataTable, dt2::AbstractDataTable)
   _names(dt1) == _names(dt2) || error("Column names do not match")
   eltypes(dt1) == eltypes(dt2) || error("Column eltypes do not match")
   ncols = size(dt1, 2)
   # TODO: This needs to be a sort of transaction to be 100% safe
   for j in 1:ncols
       append!(dt1[j], dt2[j])
   end
   return dt1
end

function Base.convert(::Type{DataTable}, A::Matrix)
    n = size(A, 2)
    cols = Vector{Any}(n)
    for i in 1:n
        cols[i] = A[:, i]
    end
    return DataTable(cols, Index(gennames(n)))
end

function _datatable_from_associative(dnames, d::Associative)
    p = length(dnames)
    p == 0 && return DataTable()
    columns  = Vector{Any}(p)
    colnames = Vector{Symbol}(p)
    n = length(d[dnames[1]])
    for j in 1:p
        name = dnames[j]
        col = d[name]
        if length(col) != n
            throw(ArgumentError("All columns in Dict must have the same length"))
        end
        columns[j] = NullableArray(col)
        colnames[j] = Symbol(name)
    end
    return DataTable(columns, Index(colnames))
end

function Base.convert(::Type{DataTable}, d::Associative)
    dnames = collect(keys(d))
    return _datatable_from_associative(dnames, d)
end

# A Dict is not sorted or otherwise ordered, and it's nicer to return a
# DataTable which is ordered in some way
function Base.convert(::Type{DataTable}, d::Dict)
    dnames = collect(keys(d))
    sort!(dnames)
    return _datatable_from_associative(dnames, d)
end


##############################################################################
##
## push! a row onto a DataTable
##
##############################################################################

function Base.push!(dt::DataTable, associative::Associative{Symbol,Any})
    i = 1
    for nm in _names(dt)
        try
            push!(dt[nm], associative[nm])
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(dt[_names(dt)[j]])
            end
            msg = "Error adding value to column :$nm."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end

function Base.push!(dt::DataTable, associative::Associative)
    i = 1
    for nm in _names(dt)
        try
            val = get(() -> associative[string(nm)], associative, nm)
            push!(dt[nm], val)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(dt[_names(dt)[j]])
            end
            msg = "Error adding value to column :$nm."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end

# array and tuple like collections
function Base.push!(dt::DataTable, iterable::Any)
    if length(iterable) != length(dt.columns)
        msg = "Length of iterable does not match DataTable column count."
        throw(ArgumentError(msg))
    end
    i = 1
    for t in iterable
        try
            push!(dt.columns[i], t)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(dt.columns[j])
            end
            msg = "Error adding $t to column :$(_names(dt)[i]). Possible type mis-match."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end
