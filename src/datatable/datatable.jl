"""
An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

**Constructors**

```julia
DataFrame(columns::Vector{Any}, names::Vector{Symbol})
DataFrame(kwargs...)
DataFrame() # an empty DataFrame
DataFrame(t::Type, nrows::Integer, ncols::Integer) # an empty DataFrame of arbitrary size
DataFrame(column_eltypes::Vector, names::Vector, nrows::Integer)
DataFrame(ds::Vector{Associative})
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

A `DataFrame` is a lightweight object. As long as columns are not
manipulated, creation of a DataFrame from existing AbstractVectors is
inexpensive. For example, indexing on columns is inexpensive, but
indexing by rows is expensive because copies are made of each column.

Because column types can vary, a DataFrame is not type stable. For
performance-critical code, do not index into a DataFrame inside of
loops.

**Examples**

```julia
dt = DataFrame()
v = ["x","y","z"][rand(1:3, 10)]
dt1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])
dt2 = DataFrame(A = 1:10, B = v, C = rand(10))
dump(dt1)
dump(dt2)
describe(dt2)
DataFrames.head(dt1)
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
type DataFrame <: AbstractDataFrame
    columns::Vector{Any}
    colindex::Index

    function DataFrame(columns::Vector{Any}, colindex::Index)
        if length(columns) == length(colindex) == 0
            return new(Vector{Any}(0), Index())
        elseif length(columns) != length(colindex)
            throw(DimensionMismatch("Number of columns ($(length(columns))) and number of column names ($(length(colindex))) are not equal"))
        end
        lengths = [isa(col, AbstractArray) ? length(col) : 1 for col in columns]
        minlen, maxlen = extrema(lengths)
        if minlen == 0 && maxlen == 0
            return new(columns, colindex)
        elseif minlen != maxlen || minlen == maxlen == 1
            # recycle scalars
            for i in 1:length(columns)
                isa(columns[i], AbstractArray) && continue
                columns[i] = fill(columns[i], maxlen)
                lengths[i] = maxlen
            end
            uls = unique(lengths)
            if length(uls) != 1
                strnames = string.(names(colindex))
                estrings = ["column length $u for column(s) " *
                            join(strnames[lengths .== u], ", ", " and ") for (i, u) in enumerate(uls)]
                throw(DimensionMismatch(join(estrings, " is incompatible with ", ", and is incompatible with ")))
            end
        end
        for (i, c) in enumerate(columns)
            if isa(c, Range)
                columns[i] = collect(c)
            elseif !isa(c, AbstractVector)
                throw(DimensionMismatch("columns must be 1-dimensional"))
            end
        end
        new(columns, colindex)
    end
end

function DataFrame(; kwargs...)
    colnames = Symbol[k for (k,v) in kwargs]
    columns = Any[v for (k,v) in kwargs]
    DataFrame(columns, Index(colnames))
end

function DataFrame(columns::AbstractVector,
                   cnames::AbstractVector{Symbol} = gennames(length(columns)))
    return DataFrame(convert(Vector{Any}, columns), Index(convert(Vector{Symbol}, cnames)))
end

# Initialize an empty DataFrame with specific eltypes and names
function DataFrame{T<:Type}(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol}, nrows::Integer)
    columns = Vector{Any}(length(column_eltypes))
    for (j, elty) in enumerate(column_eltypes)
        if elty >: Null
            if Nulls.T(elty) <: CategoricalValue
                columns[j] = CategoricalArray{Union{Nulls.T(elty).parameters[1], Null}}(nrows)
            else
                columns[j] = nulls(elty, nrows)
            end
        else
            if elty <: CategoricalValue
                columns[j] = CategoricalVector{elty}(nrows)
            else
                columns[j] = Vector{elty}(nrows)
            end
        end
    end
    return DataFrame(columns, Index(convert(Vector{Symbol}, cnames)))
end

# Initialize an empty DataFrame with specific eltypes and names
# and whether a nominal array should be created
function DataFrame{T<:Type}(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                            nominal::Vector{Bool}, nrows::Integer)
    # upcast Vector{DataType} -> Vector{Type} which can hold CategoricalValues
    updated_types = convert(Vector{Type}, column_eltypes)
    for i in eachindex(nominal)
        nominal[i] || continue
        if updated_types[i] >: Null
            updated_types[i] = Union{CategoricalValue{Nulls.T(updated_types[i])}, Null}
        else
            updated_types[i] = CategoricalValue{updated_types[i]}
        end
    end
    return DataFrame(updated_types, cnames, nrows)
end

# Initialize empty DataFrame objects of arbitrary size
function DataFrame(t::Type, nrows::Integer, ncols::Integer)
    return DataFrame(fill(t, ncols), nrows)
end

# Initialize an empty DataFrame with specific eltypes
function DataFrame{T<:Type}(column_eltypes::AbstractVector{T}, nrows::Integer)
    return DataFrame(column_eltypes, gennames(length(column_eltypes)), nrows)
end

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(dt::DataFrame) = dt.colindex
columns(dt::DataFrame) = dt.columns

# TODO: Remove these
nrow(dt::DataFrame) = ncol(dt) > 0 ? length(dt.columns[1])::Int : 0
ncol(dt::DataFrame) = length(index(dt))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# Cases:
#
# dt[SingleColumnIndex] => AbstractDataVector
# dt[MultiColumnIndex] => DataFrame
# dt[SingleRowIndex, SingleColumnIndex] => Scalar
# dt[SingleRowIndex, MultiColumnIndex] => DataFrame
# dt[MultiRowIndex, SingleColumnIndex] => AbstractVector
# dt[MultiRowIndex, MultiColumnIndex] => DataFrame
#
# General Strategy:
#
# Let getindex(index(dt), col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(dt.columns[j], row_inds) from AbstractVector() handle
#  the resolution of row indices

@compat const ColumnIndex = Union{Real, Symbol}

# dt[SingleColumnIndex] => AbstractDataVector
function Base.getindex(dt::DataFrame, col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column]
end

# dt[MultiColumnIndex] => DataFrame
function Base.getindex(dt::DataFrame,
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(dt)[col_inds]
    new_columns = dt.columns[selected_columns]
    return DataFrame(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[:] => DataFrame
Base.getindex(dt::DataFrame, col_inds::Colon) = copy(dt)

# dt[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(dt::DataFrame, row_ind::Real, col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column][row_ind]
end

# dt[SingleRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(dt::DataFrame,
                       row_ind::Real,
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(dt)[col_inds]
    new_columns = Any[dv[[row_ind]] for dv in dt.columns[selected_columns]]
    return DataFrame(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[MultiRowIndex, SingleColumnIndex] => AbstractVector
function Base.getindex(dt::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_ind::ColumnIndex)
    selected_column = index(dt)[col_ind]
    return dt.columns[selected_column][row_inds]
end

# dt[MultiRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(dt::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(dt)[col_inds]
    new_columns = Any[dv[row_inds] for dv in dt.columns[selected_columns]]
    return DataFrame(new_columns, Index(_names(dt)[selected_columns]))
end

# dt[:, SingleColumnIndex] => AbstractVector
# dt[:, MultiColumnIndex] => DataFrame
Base.getindex(dt::DataFrame, row_ind::Colon, col_inds::Union{T, AbstractVector{T}}) where
    T <: Union{ColumnIndex, Null} = dt[col_inds]

# dt[SingleRowIndex, :] => DataFrame
Base.getindex(dt::DataFrame, row_ind::Real, col_inds::Colon) = dt[[row_ind], col_inds]

# dt[MultiRowIndex, :] => DataFrame
function Base.getindex(dt::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_inds::Colon)
    new_columns = Any[dv[row_inds] for dv in dt.columns]
    return DataFrame(new_columns, copy(index(dt)))
end

# dt[:, :] => DataFrame
Base.getindex(dt::DataFrame, ::Colon, ::Colon) = copy(dt)

##############################################################################
##
## setindex!()
##
##############################################################################

isnextcol(dt::DataFrame, col_ind::Symbol) = true
function isnextcol(dt::DataFrame, col_ind::Real)
    return ncol(dt) + 1 == Int(col_ind)
end

function nextcolname(dt::DataFrame)
    return Symbol(string("x", ncol(dt) + 1))
end

# Will automatically add a new column if needed
function insert_single_column!(dt::DataFrame,
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

function insert_single_entry!(dt::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        dt.columns[index(dt)[col_ind]][row_ind] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function insert_multiple_entries!(dt::DataFrame,
                                  v::Any,
                                  row_inds::AbstractVector{<:Real},
                                  col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        dt.columns[index(dt)[col_ind]][row_inds] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function upgrade_scalar(dt::DataFrame, v::AbstractArray)
    msg = "setindex!(::DataFrame, ...) only broadcasts scalars, not arrays"
    throw(ArgumentError(msg))
end
function upgrade_scalar(dt::DataFrame, v::Any)
    n = (ncol(dt) == 0) ? 1 : nrow(dt)
    fill(v, n)
end

# dt[SingleColumnIndex] = AbstractVector
function Base.setindex!(dt::DataFrame, v::AbstractVector, col_ind::ColumnIndex)
    insert_single_column!(dt, v, col_ind)
end

# dt[SingleColumnIndex] = Single Item (EXPANDS TO NROW(DT) if NCOL(DT) > 0)
function Base.setindex!(dt::DataFrame, v, col_ind::ColumnIndex)
    if haskey(index(dt), col_ind)
        fill!(dt[col_ind], v)
    else
        insert_single_column!(dt, upgrade_scalar(dt, v), col_ind)
    end
    return dt
end

# dt[MultiColumnIndex] = DataFrame
function Base.setindex!(dt::DataFrame, new_dt::DataFrame, col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_single_column!(dt, new_dt[j], col_inds[j])
    end
    return dt
end

# dt[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function Base.setindex!(dt::DataFrame, v::AbstractVector, col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        dt[col_ind] = v
    end
    return dt
end

# dt[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(DT) if NCOL(DT) > 0)
function Base.setindex!(dt::DataFrame,
                        val::Any,
                        col_inds::AbstractVector{Bool})
    setindex!(dt, val, find(col_inds))
end
function Base.setindex!(dt::DataFrame, val::Any, col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        dt[col_ind] = val
    end
    return dt
end

# dt[:] = AbstractVector or Single Item
Base.setindex!(dt::DataFrame, v, ::Colon) = (dt[1:size(dt, 2)] = v; dt)

# dt[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(dt::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    insert_single_entry!(dt, v, row_ind, col_ind)
end

# dt[SingleRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_ind::Real,
                        col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_ind, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_ind::Real,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_single_entry!(dt, v, row_ind, col_ind)
    end
    return dt
end

# dt[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_ind::Real,
                        col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, row_ind, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_ind::Real,
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_single_entry!(dt, new_dt[j][1], row_ind, col_inds[j])
    end
    return dt
end

# dt[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(dt, v, find(row_inds), col_ind)
end
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(dt, v, row_inds, col_ind)
    return dt
end

# dt[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(dt, v, find(row_inds), col_ind)
end
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(dt, v, row_inds, col_ind)
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = DataFrame
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, find(row_inds), find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(dt, new_dt, find(row_inds), col_inds)
end
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, new_dt, row_inds, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_multiple_entries!(dt, new_dt[:, j], row_inds, col_inds[j])
    end
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(row_inds), find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(dt, v, find(row_inds), col_inds)
end
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_inds, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_multiple_entries!(dt, v, row_inds, col_ind)
    end
    return dt
end

# dt[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, v, find(row_inds), find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(dt, v, find(row_inds), col_inds)
end
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(dt, v, row_inds, find(col_inds))
end
function Base.setindex!(dt::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_multiple_entries!(dt, v, row_inds, col_ind)
    end
    return dt
end

# dt[:] = DataFrame, dt[:, :] = DataFrame
function Base.setindex!(dt::DataFrame,
                        new_dt::DataFrame,
                        row_inds::Colon,
                        col_inds::Colon=Colon())
    dt.columns = copy(new_dt.columns)
    dt.colindex = copy(new_dt.colindex)
    dt
end

# dt[:, :] = ...
Base.setindex!(dt::DataFrame, v, ::Colon, ::Colon) =
    (dt[1:size(dt, 1), 1:size(dt, 2)] = v; dt)

# dt[Any, :] = ...
Base.setindex!(dt::DataFrame, v, row_inds, ::Colon) =
    (dt[row_inds, 1:size(dt, 2)] = v; dt)

# dt[:, Any] = ...
Base.setindex!(dt::DataFrame, v, ::Colon, col_inds) =
    (dt[col_inds] = v; dt)

# Special deletion assignment
Base.setindex!(dt::DataFrame, x::Void, col_ind::Int) = delete!(dt, col_ind)

##############################################################################
##
## Mutating Associative methods
##
##############################################################################

Base.empty!(dt::DataFrame) = (empty!(dt.columns); empty!(index(dt)); dt)

function Base.insert!(dt::DataFrame, col_ind::Int, item::AbstractVector, name::Symbol)
    0 < col_ind <= ncol(dt) + 1 || throw(BoundsError())
    size(dt, 1) == length(item) || size(dt, 1) == 0 || error("number of rows does not match")

    insert!(index(dt), col_ind, name)
    insert!(dt.columns, col_ind, item)
    dt
end

function Base.insert!(dt::DataFrame, col_ind::Int, item, name::Symbol)
    insert!(dt, col_ind, upgrade_scalar(dt, item), name)
end

function Base.merge!(dt::DataFrame, others::AbstractDataFrame...)
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

# A copy of a DataFrame points to the original column vectors but
#   gets its own Index.
Base.copy(dt::DataFrame) = DataFrame(copy(columns(dt)), copy(index(dt)))

# Deepcopy is recursive -- if a column is a vector of DataFrames, each of
#   those DataFrames is deepcopied.
function Base.deepcopy(dt::DataFrame)
    DataFrame(deepcopy(columns(dt)), deepcopy(index(dt)))
end

##############################################################################
##
## Deletion / Subsetting
##
##############################################################################

# delete!() deletes columns; deleterows!() deletes rows
# delete!(dt, 1)
# delete!(dt, :Old)
function Base.delete!(dt::DataFrame, inds::Vector{Int})
    for ind in sort(inds, rev = true)
        if 1 <= ind <= ncol(dt)
            splice!(dt.columns, ind)
            delete!(index(dt), ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    return dt
end
Base.delete!(dt::DataFrame, c::Int) = delete!(dt, [c])
Base.delete!(dt::DataFrame, c::Any) = delete!(dt, index(dt)[c])

# deleterows!()
function deleterows!(dt::DataFrame, ind::Union{Integer, UnitRange{Int}})
    for i in 1:ncol(dt)
        dt.columns[i] = deleteat!(dt.columns[i], ind)
    end
    dt
end

function deleterows!(dt::DataFrame, ind::AbstractVector{Int})
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
function hcat!(dt1::DataFrame, dt2::AbstractDataFrame)
    u = add_names(index(dt1), index(dt2))
    for i in 1:length(u)
        dt1[u[i]] = dt2[i]
    end
    return dt1
end
hcat!(dt::DataFrame, x::AbstractVector) = hcat!(dt, DataFrame(Any[x]))

# hcat! for 1-n arguments
hcat!(dt::DataFrame) = dt
hcat!(a::DataFrame, b, c...) = hcat!(hcat!(a, b), c...)

# hcat
Base.hcat(dt::DataFrame, x) = hcat!(copy(dt), x)
Base.hcat(dt1::DataFrame, dt2::AbstractDataFrame) = hcat!(copy(dt1), dt2)
Base.hcat(dt1::DataFrame, dt2::AbstractDataFrame, dtn::AbstractDataFrame...) = hcat!(hcat(dt1, dt2), dtn...)

##############################################################################
##
## Nullability
##
##############################################################################

function nullable!(dt::DataFrame, col::ColumnIndex)
    dt[col] = Vector{Union{eltype(dt[col]), Null}}(dt[col])
    dt
end
function nullable!{T <: ColumnIndex}(dt::DataFrame, cols::Vector{T})
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

function categorical!(dt::DataFrame, cname::Union{Integer, Symbol})
    dt[cname] = CategoricalVector(dt[cname])
    dt
end

function categorical!(dt::DataFrame, cnames::Vector{<:Union{Integer, Symbol}})
    for cname in cnames
        dt[cname] = CategoricalVector(dt[cname])
    end
    dt
end

function categorical!(dt::DataFrame)
    for i in 1:size(dt, 2)
        if eltype(dt[i]) <: AbstractString
            dt[i] = CategoricalVector(dt[i])
        end
    end
    dt
end

function Base.append!(dt1::DataFrame, dt2::AbstractDataFrame)
   _names(dt1) == _names(dt2) || error("Column names do not match")
   eltypes(dt1) == eltypes(dt2) || error("Column eltypes do not match")
   ncols = size(dt1, 2)
   # TODO: This needs to be a sort of transaction to be 100% safe
   for j in 1:ncols
       append!(dt1[j], dt2[j])
   end
   return dt1
end

function Base.convert(::Type{DataFrame}, A::AbstractMatrix)
    n = size(A, 2)
    cols = Vector{Any}(n)
    for i in 1:n
        cols[i] = A[:, i]
    end
    return DataFrame(cols, Index(gennames(n)))
end

function Base.convert(::Type{DataFrame}, d::Associative)
    colnames = keys(d)
    if isa(d, Dict)
        colnames = sort!(collect(keys(d)))
    else
        colnames = keys(d)
    end
    colindex = Index(Symbol[k for k in colnames])
    columns = Any[d[c] for c in colnames]
    DataFrame(columns, colindex)
end


##############################################################################
##
## push! a row onto a DataFrame
##
##############################################################################

function Base.push!(dt::DataFrame, associative::Associative{Symbol,Any})
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

function Base.push!(dt::DataFrame, associative::Associative)
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
function Base.push!(dt::DataFrame, iterable::Any)
    if length(iterable) != length(dt.columns)
        msg = "Length of iterable does not match DataFrame column count."
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
