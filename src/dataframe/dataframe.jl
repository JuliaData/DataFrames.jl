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
df = DataFrame()
v = ["x","y","z"][rand(1:3, 10)]
df1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])
df2 = DataFrame(A = 1:10, B = v, C = rand(10))
dump(df1)
dump(df2)
describe(df2)
DataFrames.head(df1)
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

index(df::DataFrame) = df.colindex
columns(df::DataFrame) = df.columns

# TODO: Remove these
nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1])::Int : 0
ncol(df::DataFrame) = length(index(df))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# Cases:
#
# df[SingleColumnIndex] => AbstractDataVector
# df[MultiColumnIndex] => DataFrame
# df[SingleRowIndex, SingleColumnIndex] => Scalar
# df[SingleRowIndex, MultiColumnIndex] => DataFrame
# df[MultiRowIndex, SingleColumnIndex] => AbstractVector
# df[MultiRowIndex, MultiColumnIndex] => DataFrame
#
# General Strategy:
#
# Let getindex(index(df), col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(df.columns[j], row_inds) from AbstractVector() handle
#  the resolution of row indices

const ColumnIndex = Union{Real, Symbol}

# df[SingleColumnIndex] => AbstractDataVector
function Base.getindex(df::DataFrame, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column]
end

# df[MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame,
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(df)[col_inds]
    new_columns = df.columns[selected_columns]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[:] => DataFrame
Base.getindex(df::DataFrame, col_inds::Colon) = copy(df)

# df[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(df::DataFrame, row_ind::Real, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame,
                       row_ind::Real,
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[[row_ind]] for dv in df.columns[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector
function Base.getindex(df::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return df.columns[selected_column][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_inds::AbstractVector{<:Union{ColumnIndex, Null}})
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[row_inds] for dv in df.columns[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[:, SingleColumnIndex] => AbstractVector
# df[:, MultiColumnIndex] => DataFrame
Base.getindex(df::DataFrame, row_ind::Colon, col_inds::Union{T, AbstractVector{T}}) where
    T <: Union{ColumnIndex, Null} = df[col_inds]

# df[SingleRowIndex, :] => DataFrame
Base.getindex(df::DataFrame, row_ind::Real, col_inds::Colon) = df[[row_ind], col_inds]

# df[MultiRowIndex, :] => DataFrame
function Base.getindex(df::DataFrame,
                       row_inds::AbstractVector{<:Union{Real, Null}},
                       col_inds::Colon)
    new_columns = Any[dv[row_inds] for dv in df.columns]
    return DataFrame(new_columns, copy(index(df)))
end

# df[:, :] => DataFrame
Base.getindex(df::DataFrame, ::Colon, ::Colon) = copy(df)

##############################################################################
##
## setindex!()
##
##############################################################################

isnextcol(df::DataFrame, col_ind::Symbol) = true
function isnextcol(df::DataFrame, col_ind::Real)
    return ncol(df) + 1 == Int(col_ind)
end

function nextcolname(df::DataFrame)
    return Symbol(string("x", ncol(df) + 1))
end

# Will automatically add a new column if needed
function insert_single_column!(df::DataFrame,
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

function insert_single_entry!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        df.columns[index(df)[col_ind]][row_ind] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function insert_multiple_entries!(df::DataFrame,
                                  v::Any,
                                  row_inds::AbstractVector{<:Real},
                                  col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        df.columns[index(df)[col_ind]][row_inds] = v
        return v
    else
        error("Cannot assign to non-existent column: $col_ind")
    end
end

function upgrade_scalar(df::DataFrame, v::AbstractArray)
    msg = "setindex!(::DataFrame, ...) only broadcasts scalars, not arrays"
    throw(ArgumentError(msg))
end
function upgrade_scalar(df::DataFrame, v::Any)
    n = (ncol(df) == 0) ? 1 : nrow(df)
    fill(v, n)
end

# df[SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame, v::AbstractVector, col_ind::ColumnIndex)
    insert_single_column!(df, v, col_ind)
end

# df[SingleColumnIndex] = Single Item (EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame, v, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        fill!(df[col_ind], v)
    else
        insert_single_column!(df, upgrade_scalar(df, v), col_ind)
    end
    return df
end

# df[MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame, new_df::DataFrame, col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_single_column!(df, new_df[j], col_inds[j])
    end
    return df
end

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function Base.setindex!(df::DataFrame, v::AbstractVector, col_inds::AbstractVector{Bool})
    setindex!(df, v, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        df[col_ind] = v
    end
    return df
end

# df[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame,
                        val::Any,
                        col_inds::AbstractVector{Bool})
    setindex!(df, val, find(col_inds))
end
function Base.setindex!(df::DataFrame, val::Any, col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        df[col_ind] = val
    end
    return df
end

# df[:] = AbstractVector or Single Item
Base.setindex!(df::DataFrame, v, ::Colon) = (df[1:size(df, 2)] = v; df)

# df[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
end

# df[SingleRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_ind::Real,
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_ind, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_ind::Real,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_single_entry!(df, v, row_ind, col_ind)
    end
    return df
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_ind::Real,
                        col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_ind, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_ind::Real,
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_single_entry!(df, new_df[j][1], row_ind, col_inds[j])
    end
    return df
end

# df[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    return df
end

# df[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_ind::ColumnIndex)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(row_inds), find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, new_df, find(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_inds, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for j in 1:length(col_inds)
        insert_multiple_entries!(df, new_df[:, j], row_inds, col_inds[j])
    end
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[:] = DataFrame, df[:, :] = DataFrame
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::Colon,
                        col_inds::Colon=Colon())
    df.columns = copy(new_df.columns)
    df.colindex = copy(new_df.colindex)
    df
end

# df[:, :] = ...
Base.setindex!(df::DataFrame, v, ::Colon, ::Colon) =
    (df[1:size(df, 1), 1:size(df, 2)] = v; df)

# df[Any, :] = ...
Base.setindex!(df::DataFrame, v, row_inds, ::Colon) =
    (df[row_inds, 1:size(df, 2)] = v; df)

# df[:, Any] = ...
Base.setindex!(df::DataFrame, v, ::Colon, col_inds) =
    (df[col_inds] = v; df)

# Special deletion assignment
Base.setindex!(df::DataFrame, x::Void, col_ind::Int) = delete!(df, col_ind)

##############################################################################
##
## Mutating Associative methods
##
##############################################################################

Base.empty!(df::DataFrame) = (empty!(df.columns); empty!(index(df)); df)

function Base.insert!(df::DataFrame, col_ind::Int, item::AbstractVector, name::Symbol)
    0 < col_ind <= ncol(df) + 1 || throw(BoundsError())
    size(df, 1) == length(item) || size(df, 1) == 0 || error("number of rows does not match")

    insert!(index(df), col_ind, name)
    insert!(df.columns, col_ind, item)
    df
end

function Base.insert!(df::DataFrame, col_ind::Int, item, name::Symbol)
    insert!(df, col_ind, upgrade_scalar(df, item), name)
end

function Base.merge!(df::DataFrame, others::AbstractDataFrame...)
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

# A copy of a DataFrame points to the original column vectors but
#   gets its own Index.
Base.copy(df::DataFrame) = DataFrame(copy(columns(df)), copy(index(df)))

# Deepcopy is recursive -- if a column is a vector of DataFrames, each of
#   those DataFrames is deepcopied.
function Base.deepcopy(df::DataFrame)
    DataFrame(deepcopy(columns(df)), deepcopy(index(df)))
end

##############################################################################
##
## Deletion / Subsetting
##
##############################################################################

# delete!() deletes columns; deleterows!() deletes rows
# delete!(df, 1)
# delete!(df, :Old)
function Base.delete!(df::DataFrame, inds::Vector{Int})
    for ind in sort(inds, rev = true)
        if 1 <= ind <= ncol(df)
            splice!(df.columns, ind)
            delete!(index(df), ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    return df
end
Base.delete!(df::DataFrame, c::Int) = delete!(df, [c])
Base.delete!(df::DataFrame, c::Any) = delete!(df, index(df)[c])

# deleterows!()
function deleterows!(df::DataFrame, ind::Union{Integer, UnitRange{Int}})
    for i in 1:ncol(df)
        df.columns[i] = deleteat!(df.columns[i], ind)
    end
    df
end

function deleterows!(df::DataFrame, ind::AbstractVector{Int})
    ind2 = sort(ind)
    n = size(df, 1)

    idf = 1
    iind = 1
    ikeep = 1
    keep = Array{Int}(n-length(ind2))
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
function hcat!(df1::DataFrame, df2::AbstractDataFrame)
    u = add_names(index(df1), index(df2))
    for i in 1:length(u)
        df1[u[i]] = df2[i]
    end
    return df1
end
hcat!(df::DataFrame, x::AbstractVector) = hcat!(df, DataFrame(Any[x]))

# hcat! for 1-n arguments
hcat!(df::DataFrame) = df
hcat!(a::DataFrame, b, c...) = hcat!(hcat!(a, b), c...)

# hcat
Base.hcat(df::DataFrame, x) = hcat!(copy(df), x)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame) = hcat!(copy(df1), df2)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame, dfn::AbstractDataFrame...) = hcat!(hcat(df1, df2), dfn...)

##############################################################################
##
## Nullability
##
##############################################################################

function nullable!(df::DataFrame, col::ColumnIndex)
    df[col] = Vector{Union{eltype(df[col]), Null}}(df[col])
    df
end
function nullable!{T <: ColumnIndex}(df::DataFrame, cols::Vector{T})
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

function categorical!(df::DataFrame, cname::Union{Integer, Symbol})
    df[cname] = CategoricalVector(df[cname])
    df
end

function categorical!(df::DataFrame, cnames::Vector{<:Union{Integer, Symbol}})
    for cname in cnames
        df[cname] = CategoricalVector(df[cname])
    end
    df
end

function categorical!(df::DataFrame)
    for i in 1:size(df, 2)
        if eltype(df[i]) <: AbstractString
            df[i] = CategoricalVector(df[i])
        end
    end
    df
end

function Base.append!(df1::DataFrame, df2::AbstractDataFrame)
   _names(df1) == _names(df2) || error("Column names do not match")
   eltypes(df1) == eltypes(df2) || error("Column eltypes do not match")
   ncols = size(df1, 2)
   # TODO: This needs to be a sort of transaction to be 100% safe
   for j in 1:ncols
       append!(df1[j], df2[j])
   end
   return df1
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

function Base.push!(df::DataFrame, associative::Associative{Symbol,Any})
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

function Base.push!(df::DataFrame, associative::Associative)
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
function Base.push!(df::DataFrame, iterable::Any)
    if length(iterable) != length(df.columns)
        msg = "Length of iterable does not match DataFrame column count."
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
