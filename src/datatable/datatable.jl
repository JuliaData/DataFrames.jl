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
df = DataTable()
v = ["x","y","z"][rand(1:3, 10)]
df1 = DataTable(Any[collect(1:10), v, rand(10)], [:A, :B, :C])  # columns are Arrays
df2 = DataTable(A = 1:10, B = v, C = rand(10))           # columns are NullableArrays
dump(df1)
dump(df2)
describe(df2)
DataTables.head(df1)
df1[:A] + df2[:C]
df1[1:4, 1:2]
df1[[:A,:C]]
df1[1:2, [:A,:C]]
df1[:, [:A,:C]]
df1[:, [1,3]]
df1[1:4, :]
df1[1:4, :C]
df1[1:4, :C] = 40. * df1[1:4, :C]
[df1; df2]  # vcat
[df1  df2]  # hcat
size(df1)
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
    columns = Array(Any, ncols)
    for i in 1:ncols
        columns[i] = NullableArray(t, nrows)
    end
    cnames = gennames(ncols)
    return DataTable(columns, Index(cnames))
end

# Initialize an empty DataTable with specific eltypes and names
function DataTable(column_eltypes::Vector, cnames::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Array(Any, p)
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
    columns = Array(Any, p)
    for j in 1:p
      if nominal[j]
        columns[j] = NullableCategoricalArray(column_eltypes[j], nrows)
      else
        columns[j] = NullableArray(column_eltypes[j], nrows)
      end
    end
    return DataTable(columns, Index(cnames))
end

# Initialize an empty DataTable with specific eltypes
function DataTable(column_eltypes::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Array(Any, p)
    cnames = gennames(p)
    for j in 1:p
        columns[j] = NullableArray(column_eltypes[j], nrows)
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
    df = DataTable(col_eltypes, ks, length(ds))
    for (i,d) in enumerate(ds)
        for (j,k) in enumerate(ks)
            df[i,j] = get(d, k, Nullable())
        end
    end

    df
end

##############################################################################
##
## AbstractDataTable interface
##
##############################################################################

index(df::DataTable) = df.colindex
columns(df::DataTable) = df.columns

# TODO: Remove these
nrow(df::DataTable) = ncol(df) > 0 ? length(df.columns[1])::Int : 0
ncol(df::DataTable) = length(index(df))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# Cases:
#
# df[SingleColumnIndex] => AbstractDataVector
# df[MultiColumnIndex] => DataTable
# df[SingleRowIndex, SingleColumnIndex] => Scalar
# df[SingleRowIndex, MultiColumnIndex] => DataTable
# df[MultiRowIndex, SingleColumnIndex] => AbstractVector
# df[MultiRowIndex, MultiColumnIndex] => DataTable
#
# General Strategy:
#
# Let getindex(index(df), col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(df.columns[j], row_inds) from AbstractVector() handle
#  the resolution of row indices

typealias ColumnIndex @compat(Union{Real, Symbol})

# df[SingleColumnIndex] => AbstractDataVector
function Base.getindex(df::DataTable, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column]
end

# df[MultiColumnIndex] => DataTable
function Base.getindex{T <: ColumnIndex}(df::DataTable,
                                         col_inds::Union{AbstractVector{T},
                                                         AbstractVector{Nullable{T}}})
    selected_columns = index(df)[col_inds]
    new_columns = df.columns[selected_columns]
    return DataTable(new_columns, Index(_names(df)[selected_columns]))
end

# df[:] => DataTable
Base.getindex(df::DataTable, col_inds::Colon) = copy(df)

# df[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(df::DataTable, row_ind::Real, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => DataTable
function Base.getindex{T <: ColumnIndex}(df::DataTable,
                                         row_ind::Real,
                                         col_inds::Union{AbstractVector{T},
                                                         AbstractVector{Nullable{T}}})
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[[row_ind]] for dv in df.columns[selected_columns]]
    return DataTable(new_columns, Index(_names(df)[selected_columns]))
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector
function Base.getindex{T <: Real}(df::DataTable,
                                  row_inds::Union{AbstractVector{T}, AbstractVector{Nullable{T}}},
                                  col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => DataTable
function Base.getindex{R <: Real, T <: ColumnIndex}(df::DataTable,
                                                    row_inds::Union{AbstractVector{R},
                                                                    AbstractVector{Nullable{R}}},
                                                    col_inds::Union{AbstractVector{T},
                                                                    AbstractVector{Nullable{T}}})
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[row_inds] for dv in df.columns[selected_columns]]
    return DataTable(new_columns, Index(_names(df)[selected_columns]))
end

# df[:, SingleColumnIndex] => AbstractVector
# df[:, MultiColumnIndex] => DataTable
Base.getindex{T<:ColumnIndex}(df::DataTable,
                              row_inds::Colon,
                              col_inds::Union{T, AbstractVector{T},
                                              AbstractVector{Nullable{T}}}) =
    df[col_inds]

# df[SingleRowIndex, :] => DataTable
Base.getindex(df::DataTable, row_ind::Real, col_inds::Colon) = df[[row_ind], col_inds]

# df[MultiRowIndex, :] => DataTable
function Base.getindex{R<:Real}(df::DataTable,
                                row_inds::Union{AbstractVector{R},
                                                AbstractVector{Nullable{R}}},
                                col_inds::Colon)
    new_columns = Any[dv[row_inds] for dv in df.columns]
    return DataTable(new_columns, copy(index(df)))
end

# df[:, :] => (Sub)?DataTable
Base.getindex(df::DataTable, ::Colon, ::Colon) = copy(df)

##############################################################################
##
## setindex!()
##
##############################################################################

isnextcol(df::DataTable, col_ind::Symbol) = true
function isnextcol(df::DataTable, col_ind::Real)
    return ncol(df) + 1 == @compat Int(col_ind)
end

function nextcolname(df::DataTable)
    return @compat(Symbol(string("x", ncol(df) + 1)))
end

# Will automatically add a new column if needed
function insert_single_column!(df::DataTable,
                               dv::AbstractVector,
                               col_ind::ColumnIndex)

    if ncol(df) != 0 && nrow(df) != length(dv)
        error("New columns must have the same length as old columns")
    end
    if haskey(index(df), col_ind)
        j = index(df)[col_ind]
        df.columns[j] = dv
    else
        if typeof(col_ind) <: Symbol
            push!(index(df), col_ind)
            push!(df.columns, dv)
        else
            if isnextcol(df, col_ind)
                push!(index(df), nextcolname(df))
                push!(df.columns, dv)
            else
                error("Cannot assign to non-existent column: $col_ind")
            end
        end
    end
    return dv
end

function insert_single_entry!(df::DataTable, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        df.columns[index(df)[col_ind]][row_ind] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function insert_multiple_entries!{T <: Real}(df::DataTable,
                                            v::Any,
                                            row_inds::AbstractVector{T},
                                            col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        df.columns[index(df)[col_ind]][row_inds] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

upgrade_vector{T<:Nullable}(v::AbstractArray{T}) = v
upgrade_vector(v::CategoricalArray) = NullableCategoricalArray(v)
upgrade_vector(v::AbstractArray) = NullableArray(v)

function upgrade_scalar(df::DataTable, v::AbstractArray)
    msg = "setindex!(::DataTable, ...) only broadcasts scalars, not arrays"
    throw(ArgumentError(msg))
end
function upgrade_scalar(df::DataTable, v::Any)
    n = (ncol(df) == 0) ? 1 : nrow(df)
    NullableArray(fill(v, n))
end

# df[SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataTable,
                v::AbstractVector,
                col_ind::ColumnIndex)
    insert_single_column!(df, upgrade_vector(v), col_ind)
end

# df[SingleColumnIndex] = Single Item (EXPANDS TO NROW(DF) if NCOL(DF) > 0)
function Base.setindex!(df::DataTable, v, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        fill!(df[col_ind], v)
    else
        insert_single_column!(df, upgrade_scalar(df, v), col_ind)
    end
    return df
end

# df[MultiColumnIndex] = DataTable
function Base.setindex!(df::DataTable,
                new_df::DataTable,
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  new_df::DataTable,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_single_column!(df, new_df[j], col_inds[j])
    end
    return df
end

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function Base.setindex!(df::DataTable,
                v::AbstractVector,
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  v::AbstractVector,
                                  col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for col_ind in col_inds
        df[col_ind] = dv
    end
    return df
end

# df[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(DF) if NCOL(DF) > 0)
function Base.setindex!(df::DataTable,
                val::Any,
                col_inds::AbstractVector{Bool})
    setindex!(df, val, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  val::Any,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        df[col_ind] = val
    end
    return df
end

# df[:] = AbstractVector or Single Item
Base.setindex!(df::DataTable, v, ::Colon) = (df[1:size(df, 2)] = v; df)

# df[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataTable,
                v::Any,
                row_ind::Real,
                col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
end

# df[SingleRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataTable,
                v::Any,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(df, v, row_ind, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  v::Any,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_single_entry!(df, v, row_ind, col_ind)
    end
    return df
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataTable
function Base.setindex!(df::DataTable,
                new_df::DataTable,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_ind, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  new_df::DataTable,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_single_entry!(df, new_df[j][1], row_ind, col_inds[j])
    end
    return df
end

# df[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataTable,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(df::DataTable,
                           v::AbstractVector,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    return df
end

# df[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataTable,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(df::DataTable,
                           v::Any,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = DataTable
function Base.setindex!(df::DataTable,
                new_df::DataTable,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  new_df::DataTable,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, new_df, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataTable,
                           new_df::DataTable,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataTable,
                                             new_df::DataTable,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        insert_multiple_entries!(df, new_df[:, j], row_inds, col_inds[j])
    end
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(df::DataTable,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  v::AbstractVector,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataTable,
                           v::AbstractVector,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataTable,
                                             v::AbstractVector,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataTable,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataTable,
                                  v::Any,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataTable,
                           v::Any,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataTable,
                                             v::Any,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[:] = DataTable, df[:, :] = DataTable
function Base.setindex!(df::DataTable,
                                  new_df::DataTable,
                                  row_inds::Colon,
                                  col_inds::Colon=Colon())
    df.columns = copy(new_df.columns)
    df.colindex = copy(new_df.colindex)
    df
end

# df[:, :] = ...
Base.setindex!(df::DataTable, v, ::Colon, ::Colon) =
    (df[1:size(df, 1), 1:size(df, 2)] = v; df)

# df[Any, :] = ...
Base.setindex!(df::DataTable, v, row_inds, ::Colon) =
    (df[row_inds, 1:size(df, 2)] = v; df)

# df[:, Any] = ...
Base.setindex!(df::DataTable, v, ::Colon, col_inds) =
    (df[col_inds] = v; df)

# Special deletion assignment
Base.setindex!(df::DataTable, x::Void, col_ind::Int) = delete!(df, col_ind)

##############################################################################
##
## Mutating Associative methods
##
##############################################################################

Base.empty!(df::DataTable) = (empty!(df.columns); empty!(index(df)); df)

function Base.insert!(df::DataTable, col_ind::Int, item::AbstractVector, name::Symbol)
    0 < col_ind <= ncol(df) + 1 || throw(BoundsError())
    size(df, 1) == length(item) || size(df, 1) == 0 || error("number of rows does not match")

    insert!(index(df), col_ind, name)
    insert!(df.columns, col_ind, item)
    df
end

# FIXME: Needed to work around a crash: JuliaLang/julia#18299
function Base.insert!(df::DataTable, col_ind::Int, item::NullableArray, name::Symbol)
    0 < col_ind <= ncol(df) + 1 || throw(BoundsError())
    size(df, 1) == length(item) || size(df, 1) == 0 || error("number of rows does not match")

    insert!(index(df), col_ind, name)
    insert!(df.columns, col_ind, item)
    df
end

function Base.insert!(df::DataTable, col_ind::Int, item, name::Symbol)
    insert!(df, col_ind, upgrade_scalar(df, item), name)
end

function Base.merge!(df::DataTable, others::AbstractDataTable...)
    for other in others
        for n in _names(other)
            df[n] = other[n]
        end
    end
    return df
end

##############################################################################
##
## Copying
##
##############################################################################

# A copy of a DataTable points to the original column vectors but
#   gets its own Index.
Base.copy(df::DataTable) = DataTable(copy(columns(df)), copy(index(df)))

# Deepcopy is recursive -- if a column is a vector of DataTables, each of
#   those DataTables is deepcopied.
function Base.deepcopy(df::DataTable)
    DataTable(deepcopy(columns(df)), deepcopy(index(df)))
end

##############################################################################
##
## Deletion / Subsetting
##
##############################################################################

# delete!() deletes columns; deleterows!() deletes rows
# delete!(df, 1)
# delete!(df, :Old)
function Base.delete!(df::DataTable, inds::Vector{Int})
    for ind in sort(inds, rev = true)
        if 1 <= ind <= ncol(df)
            splice!(df.columns, ind)
            delete!(index(df), ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataTable column"))
        end
    end
    return df
end
Base.delete!(df::DataTable, c::Int) = delete!(df, [c])
Base.delete!(df::DataTable, c::Any) = delete!(df, index(df)[c])

# deleterows!()
function deleterows!(df::DataTable, ind::@compat(Union{Integer, UnitRange{Int}}))
    for i in 1:ncol(df)
        df.columns[i] = deleteat!(df.columns[i], ind)
    end
    df
end

function deleterows!(df::DataTable, ind::AbstractVector{Int})
    ind2 = sort(ind)
    n = size(df, 1)

    idf = 1
    iind = 1
    ikeep = 1
    keep = Array(Int, n-length(ind2))
    while idf <= n && iind <= length(ind2)
        1 <= ind2[iind] <= n || error(BoundsError())
        if idf == ind2[iind]
            iind += 1
        else
            keep[ikeep] = idf
            ikeep += 1
        end
        idf += 1
    end
    keep[ikeep:end] = idf:n

    for i in 1:ncol(df)
        df.columns[i] = df.columns[i][keep]
    end
    df
end

##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments
function hcat!(df1::DataTable, df2::AbstractDataTable)
    u = add_names(index(df1), index(df2))
    for i in 1:length(u)
        df1[u[i]] = df2[i]
    end

    return df1
end
hcat!(df::DataTable, x::CategoricalArray) = hcat!(df, DataTable(Any[x]))
hcat!(df::DataTable, x::NullableCategoricalArray) = hcat!(df, DataTable(Any[x]))
hcat!(df::DataTable, x::NullableVector) = hcat!(df, DataTable(Any[x]))
hcat!(df::DataTable, x::Vector) = hcat!(df, DataTable(Any[NullableArray(x)]))
hcat!(df::DataTable, x) = hcat!(df, DataTable(Any[NullableArray([x])]))

# hcat! for 1-n arguments
hcat!(df::DataTable) = df
hcat!(a::DataTable, b, c...) = hcat!(hcat!(a, b), c...)

# hcat
Base.hcat(df::DataTable, x) = hcat!(copy(df), x)

##############################################################################
##
## Nullability
##
##############################################################################

function nullable!(df::DataTable, col::ColumnIndex)
    df[col] = NullableArray(df[col])
    df
end
function nullable!{T <: ColumnIndex}(df::DataTable, cols::Vector{T})
    for col in cols
        nullable!(df, col)
    end
    df
end

##############################################################################
##
## Pooling
##
##############################################################################

function categorical!(df::DataTable, cname::@compat(Union{Integer, Symbol}), compact::Bool=true)
    df[cname] = categorical(df[cname], compact)
    return
end

function categorical!{T <: @compat(Union{Integer, Symbol})}(df::DataTable, cnames::Vector{T},
                                                            compact::Bool=true)
    for cname in cnames
        df[cname] = categorical(df[cname], compact)
    end
    return
end

function categorical!(df::DataTable, compact::Bool=true)
    for i in 1:size(df, 2)
        if eltype(df[i]) <: AbstractString
            df[i] = categorical(df[i], compact)
        end
    end
    return
end

function Base.append!(df1::DataTable, df2::AbstractDataTable)
   _names(df1) == _names(df2) || error("Column names do not match")
   eltypes(df1) == eltypes(df2) || error("Column eltypes do not match")
   ncols = size(df1, 2)
   # TODO: This needs to be a sort of transaction to be 100% safe
   for j in 1:ncols
       append!(df1[j], df2[j])
   end
   return df1
end

function Base.convert(::Type{DataTable}, A::Matrix)
    n = size(A, 2)
    cols = Array(Any, n)
    for i in 1:n
        cols[i] = A[:, i]
    end
    return DataTable(cols, Index(gennames(n)))
end

function _datatable_from_associative(dnames, d::Associative)
    p = length(dnames)
    p == 0 && return DataTable()
    columns  = Array(Any, p)
    colnames = Array(Symbol, p)
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

function Base.push!(df::DataTable, associative::Associative{Symbol,Any})
    i = 1
    for nm in _names(df)
        try
            push!(df[nm], associative[nm])
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(df[_names(df)[j]])
            end
            msg = "Error adding value to column :$nm."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end

function Base.push!(df::DataTable, associative::Associative)
    i = 1
    for nm in _names(df)
        try
            val = get(() -> associative[string(nm)], associative, nm)
            push!(df[nm], val)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(df[_names(df)[j]])
            end
            msg = "Error adding value to column :$nm."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end

# array and tuple like collections
function Base.push!(df::DataTable, iterable::Any)
    if length(iterable) != length(df.columns)
        msg = "Length of iterable does not match DataTable column count."
        throw(ArgumentError(msg))
    end
    i = 1
    for t in iterable
        try
            push!(df.columns[i], t)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(df.columns[j])
            end
            msg = "Error adding $t to column :$(_names(df)[i]). Possible type mis-match."
            throw(ArgumentError(msg))
        end
        i += 1
    end
end
