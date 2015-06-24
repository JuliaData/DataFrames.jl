@comment """
# DataFrame
"""

"""
An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector, DataVector, or PooledDataVector.

### Constructors

```julia
DataFrame(columns::Vector{Any}, names::Vector{Symbol})
DataFrame(kwargs...)
DataFrame() # an empty DataFrame
DataFrame(t::Type, nrows::Integer, ncols::Integer) # an empty DataFrame of arbitrary size
DataFrame(column_eltypes::Vector, names::Vector, nrows::Integer)
DataFrame(ds::Vector{Associative})
```

### Arguments

* `columns` : a Vector{Any} with each column as contents
* `names` : the column names
* `kwargs` : the key gives the column names, and the value is the
  column contents
* `t` : elemental type of all columns
* `nrows`, `ncols` : number of rows and columns
* `column_eltypes` : elemental type of each column
* `ds` : a vector of Associatives

Each column in `columns` should be the same length.

### Notes

Most of the default constructors convert columns to `DataArrays`.  The
base constructor, `DataFrame(columns::Vector{Any},
names::Vector{Symbol})` does not convert to `DataArrays`.

A `DataFrame` is a lightweight object. As long as columns are not
manipulated, creation of a DataFrame from existing AbstractVectors is
inexpensive. For example, indexing on columns is inexpensive, but
indexing by rows is expensive because copies are made of each column.

Because column types can vary, a DataFrame is not type stable. For
performance-critical code, do not index into a DataFrame inside of
loops.

### Examples

```julia
df = DataFrame()
v = ["x","y","z"][rand(1:3, 10)]
df1 = DataFrame(Any[[1:10], v, rand(10)], [:A, :B, :C])  # columns are Arrays
df2 = DataFrame(A = 1:10, B = v, C = rand(10))           # columns are DataArrays
dump(df1)
dump(df2)
describe(df2)
head(df1)
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
        ncols = length(columns)
        if ncols > 1
            nrows = length(columns[1])
            equallengths = true
            for i in 2:ncols
                equallengths &= length(columns[i]) == nrows
            end
            if !equallengths
                msg = "All columns in a DataFrame must be the same length"
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

function DataFrame(; kwargs...)
    result = DataFrame(Any[], Index())
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end

function DataFrame(columns::Vector{Any},
                   cnames::Vector{Symbol} = gennames(length(columns)))
    return DataFrame(columns, Index(cnames))
end


# Initialize empty DataFrame objects of arbitrary size
function DataFrame(t::Type, nrows::Integer, ncols::Integer)
    columns = Array(Any, ncols)
    for i in 1:ncols
        columns[i] = DataArray(t, nrows)
    end
    cnames = gennames(ncols)
    return DataFrame(columns, Index(cnames))
end

# Initialize an empty DataFrame with specific eltypes and names
function DataFrame(column_eltypes::Vector, cnames::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Array(Any, p)
    for j in 1:p
        columns[j] = DataArray(column_eltypes[j], nrows)
    end
    return DataFrame(columns, Index(cnames))
end
# Initialize an empty DataFrame with specific eltypes and names and whether is pooled data array
function DataFrame(column_eltypes::Vector{DataType}, cnames::Vector{Symbol}, ispda::Vector{Bool}, nrows::Integer)
    p = length(column_eltypes)
    columns = Array(Any, p)
    for j in 1:p
      if ispda[j]
        columns[j] = PooledDataArray(column_eltypes[j], nrows)
      else
        columns[j] = DataArray(column_eltypes[j], nrows)
      end
    end
    return DataFrame(columns, Index(cnames))
end

# Initialize an empty DataFrame with specific eltypes
function DataFrame(column_eltypes::Vector, nrows::Integer)
    p = length(column_eltypes)
    columns = Array(Any, p)
    cnames = gennames(p)
    for j in 1:p
        columns[j] = DataArray(column_eltypes[j], nrows)
    end
    return DataFrame(columns, Index(cnames))
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataFrame{D <: Associative}(ds::Vector{D})
    ks = Set()
    for d in ds
        union!(ks, keys(d))
    end
    DataFrame(ds, [ks...])
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataFrame{D <: Associative}(ds::Vector{D}, ks::Vector)
    #get column eltypes
    col_eltypes = Type[None for _ = 1:length(ks)]
    for d in ds
        for (i,k) in enumerate(ks)
            # TODO: check for user-defined "NA" values, ala pandas
            if haskey(d, k) && !isna(d[k])
                col_eltypes[i] = promote_type(col_eltypes[i], typeof(d[k]))
            end
        end
    end
    col_eltypes[col_eltypes .== None] = Any

    # create empty DataFrame, and fill
    df = DataFrame(col_eltypes, ks, length(ds))
    for (i,d) in enumerate(ds)
        for (j,k) in enumerate(ks)
            df[i,j] = get(d, k, NA)
        end
    end

    df
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
# df[MultiColumnIndex] => (Sub)?DataFrame
# df[SingleRowIndex, SingleColumnIndex] => Scalar
# df[SingleRowIndex, MultiColumnIndex] => (Sub)?DataFrame
# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVector
# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
# df[:] => DataFrame
# df[SingleRowIndex, :] => (Sub)?DataFrame
# df[MultiRowIndex, :] => (Sub)?DataFrame
# df[:, SingleColumnIndex] => (Sub)?AbstractDataVector
# df[:, MultiColumnIndex] => (Sub)?DataFrame
#
# General Strategy:
#
# Let getindex(index(df), col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(df.columns[j], row_inds) from AbstractDataVector() handle
#  the resolution of row indices

typealias ColumnIndex Union(Real, Symbol)

##############################################################################
##
## setindex!() helper functions
##
##############################################################################

isnextcol(df::DataFrame, col_ind::Symbol) = true
function isnextcol(df::DataFrame, col_ind::Real)
    return ncol(df) + 1 == @compat Int(col_ind)
end

function nextcolname(df::DataFrame)
    return symbol(string("x", ncol(df) + 1))
end

# Assign the whole column, will automatically add a new column if needed
function set_column!(df::DataFrame,
                     dv::AbstractVector,
                     ::Colon,
                     col_ind::ColumnIndex)
    @assert ncol(df) == 0 || nrow(df) == length(dv) # user-friendly check should have been done before
    col_pos = get(index(df), col_ind, 0)
    if col_pos > 0
        df.columns[col_pos] = dv
    else
        if typeof(col_ind) <: Symbol
            push!(index(df), col_ind)
            push!(df.columns, dv)
        else
            if isnextcol(df, col_ind)
                push!(index(df), nextcolname(df))
                push!(df.columns, dv)
            else
                throw(KeyError("Cannot assign to non-existent column: $col_ind"))
            end
        end
    end
    return dv
end

# Assign the single element in a column
function set_column!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    col_pos = get(index(df), col_ind, 0)
    if col_pos > 0
        df.columns[col_pos][row_ind] = v
        return v
    else
        throw(KeyError("Cannot assign to non-existent column: $col_ind"))
    end
end

# Assign the subset of elements in a column
function set_column!{T <: Real}(df::DataFrame,
                                v::Any,
                                row_inds::AbstractVector{T},
                                col_ind::ColumnIndex)
    col_pos = get(index(df), col_ind, 0)
    if col_pos > 0
        df.columns[col_pos][row_inds] = v
        return v
    else
        throw(KeyError("Cannot assign to non-existent column: $col_ind"))
    end
end

upgrade_vector(v::Vector) = DataArray(v, falses(length(v)))
upgrade_vector(v::Range) = DataArray([v;], falses(length(v)))
upgrade_vector(v::BitVector) = DataArray(convert(Array{Bool}, v), falses(length(v)))
upgrade_vector(adv::AbstractDataArray) = adv

function upgrade_scalar(df::DataFrame, v::AbstractArray, row_inds::AbstractArray)
    throw(ArgumentError("setindex!(::DataFrame, ...) only broadcasts scalars, not arrays"))
end
function upgrade_scalar(df::DataFrame, v::AbstractArray, row_inds::Colon)
    throw(ArgumentError("setindex!(::DataFrame, ...) only broadcasts scalars, not arrays"))
end

# how many rows are in the selection
nselrows(df::DataFrame, row_inds::Colon) = (ncol(df) == 0) ? 1 : nrow(df)
nselrows{T<:Real}(df::DataFrame, row_inds::AbstractArray{T}) = length(row_inds)
nselrows(df::DataFrame, row_inds::AbstractArray{Bool}) = sum(row_inds)

function upgrade_scalar(df::DataFrame, v::Any, row_inds::Any)
    n = nselrows(df, row_inds)
    DataArray(fill(v, n), falses(n))
end
function upgrade_scalar(df::DataFrame, ::NAtype, row_inds::Any)
    throw(MethodError("Assigning NAs not supported yet"))
end

##############################################################################
##
## Generation of getindex() and setindex!() methods
##
##############################################################################

_concat_args(args...; delim=",") = join(filter( x -> !isempty(x), [args...]), delim)

for rows in [nothing, :single, :multiple, :colon]
  rows_arg = "" # rows argument declaration
  rows_tparam = "" # template parameter for rows argument
  cell_sel = "" # subset of column-vector to set/return. "" = set the whole vector

  # setup row-dependent elements of the generated function
  if rows == :single
    rows_arg = "row_ind::Real"
    cell_sel = "row_ind"
  elseif rows == :multiple
    rows_arg = "row_inds::AbstractVector{R}"
    rows_tparam = "R<:Real"
    cell_sel = "row_inds"
  elseif rows == :mask
    rows_arg = "row_mask::AbstractVector{Bool}"
    cell_sel = "row_mask"
  elseif rows == :colon
    rows_arg = "::Colon"
  elseif rows == nothing
  else
    throw(ArgumentError("Unknown kind of rows argument: $(rows)"))
  end

  for cols in [:single, :multiple, :colon]
    getindex_body = nothing
    get_cols_sel = "index(df)[col_inds]" # column positions for getindex() loop
    set_cols_sel = "col_inds" # collection of column indices for setindex!() loop
    set_col_ind = "col_inds[j]" # column index being used inside setindex!() loop
    cols_tparam = ""

    # correct column element accesses for getindex()
    if cols != :single && rows == :single
      # correct column access for 1-row DataFrame case
      get_cell_sel = "[[row_ind]]" # return 1-element DataVector
    else
      get_cell_sel = cell_sel != "" ? "[$(cell_sel)]" : ""
    end

    # setup column-dependent elements of the generated function
    if cols == :single
      # vector or scalar returned
      cols_arg = "col_ind::ColumnIndex"
      getindex_body = "return df.columns[index(df)[col_ind]]$(get_cell_sel)"
    elseif cols == :multiple
      cols_arg = "col_inds::AbstractVector{C}"
      cols_sel = "index(df)[col_inds]"
      cols_tparam = "C<:ColumnIndex"
    elseif cols == :mask
      cols_arg = "col_mask::AbstractVector{Bool}"
      cols_sel = "index(df)[col_mask]"
    elseif cols == :colon
      cols_arg = "::Colon"
      cols_sel = "1:ncol(df)" # iterate over all columns
      set_cols_sel = cols_sel
      set_col_ind = "j"
      if rows == nothing || rows == :colon
        # getindex returns the whole dataframe
        getindex_body = "return copy(df)"
      end
    else
      throw(ArgumentError("Unknown kind of cols argument: $(cols)"))
    end

    getindex_args = _concat_args("df::DataFrame", rows_arg, cols_arg)
    tparams = _concat_args(rows_tparam, cols_tparam)
    if (!isempty(tparams)) tparams = "{$(tparams)}" end

    if getindex_body == nothing
      getindex_body = "col_poses = index(df)[$(cols_sel)]
           new_columns = Any[df.columns[j]$(get_cell_sel) for j in col_poses]
           return DataFrame(new_columns, Index(_names(df)[col_poses]))"
    end

    # declare getindex() function
    getindex_func = "function Base.getindex$(tparams)($(getindex_args))
      $(getindex_body)
    end"
    eval(parse(getindex_func))

    for val in [:frame, :scalar, :vector]
      val_insert = "v" # value to pass to set_column!()
      val_checks = ""  # check to do before the assignment
      val_prepare = "" # value preparations before the assignment

      # correct column element accesses for setindex!()
      if rows == :colon || rows == nothing
        set_cell_sel = ":"
      elseif rows == :single && val != :scalar
        set_cell_sel = "[row_ind]" # assign to 1-element DataVector
      else
        set_cell_sel = cell_sel
      end

      # setup value-dependent elements of the generated function
      if val == :scalar
        val_arg = "v"
        if rows == :colon || rows == nothing
          val_prepare = "dv = upgrade_scalar(df, v, :)"
          val_insert = "dv"
        elseif rows == :multi
          val_prepare = "dv = upgrade_scalar(df, v, row_inds)"
          val_insert = "dv"
        end
      elseif val == :vector
        if rows == :single continue # impossible combination
        elseif rows == :colon || rows == nothing # check that vector is compatible with the dataframe
            val_checks = "if ncol(df) != 0 && nrow(df) != length(v)
              throw(DimensionMismatch(\"Data size (\$(length(v))) doesn't match the number of rows \$(nrow(df))\"))
            end"
        end
        val_arg = "v::AbstractVector"
        val_prepare = "dv = upgrade_vector(v)"
        val_insert = "dv"
      elseif val == :frame
        if cols == :single continue end # impossible combination
        if rows == :colon || rows == nothing # check that vector is compatible with the dataframe
            val_checks = "if ncol(df) != 0 && nrow(df) != nrow(v)
              throw(DimensionMismatch(\"Data size (\$(nrow(v))) doesn't match the number of rows \$(nrow(df))\"))
            end"
        else
            val_checks = "if ncol(v) != length($(set_cols_sel))
              throw(DimensionMismatch(\"Trying to assign \$(ncol(v)) column(s) to \$(length(col_inds)) column(s)\"))
            end"
        end
        val_arg = "v::DataFrame"
        val_insert = "v[j]"
      else
        throw(ArgumentError("Unknown kind of val argument: $(val)"))
      end

      if cols == :single
        setindex_body = "set_column!( df, $(val_insert), $(set_cell_sel), col_ind )"
      else
        setindex_body = "for j in @compat eachindex($(set_cols_sel))
              set_column!( df, $(val_insert), $(set_cell_sel), $(set_col_ind) )
            end"
      end

      setindex_args = _concat_args("df::DataFrame",val_arg,rows_arg,cols_arg)
      setindex_func = "function Base.setindex!$(tparams)($(setindex_args))
        $(val_checks)
        $(val_prepare)
        $(setindex_body)
        return df
      end"
      eval(parse(setindex_func))
    end
  end
end

# Special deletion assignment
Base.setindex!(df::DataFrame, x::Nothing, col_ind::Int) = delete!(df, col_ind)

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
Base.insert!(df::DataFrame, col_ind::Int, item, name::Symbol) =
    insert!(df, col_ind, upgrade_scalar(df, item), name)

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
function deleterows!(df::DataFrame, ind::Union(Integer, UnitRange{Int}))
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
function hcat!(df1::DataFrame, df2::AbstractDataFrame)
    u = add_names(index(df1), index(df2))
    for i in 1:length(u)
        df1[u[i]] = df2[i]
    end

    return df1
end
hcat!{T}(df::DataFrame, x::DataVector{T}) = hcat!(df, DataFrame(Any[x]))
hcat!{T}(df::DataFrame, x::Vector{T}) = hcat!(df, DataFrame(Any[DataArray(x)]))
hcat!{T}(df::DataFrame, x::T) = hcat!(df, DataFrame(Any[DataArray([x])]))

# hcat! for 1-n arguments
hcat!(df::DataFrame) = df
hcat!(a::DataFrame, b, c...) = hcat!(hcat!(a, b), c...)

# hcat
Base.hcat(df::DataFrame, x) = hcat!(copy(df), x)

##############################################################################
##
## Nullability
##
##############################################################################

function nullable!(df::DataFrame, col::ColumnIndex)
    df[col] = DataArray(df[col])
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

pool(a::AbstractVector) = compact(PooledDataArray(a))

function pool!(df::DataFrame, cname::Union(Integer, Symbol))
    df[cname] = pool(df[cname])
    return
end

function pool!{T <: Union(Integer, Symbol)}(df::DataFrame, cnames::Vector{T})
    for cname in cnames
        df[cname] = pool(df[cname])
    end
    return
end

# TODO: Deprecate or change for being too inconsistent with other pool methods
function pool!(df::DataFrame)
    for i in 1:size(df, 2)
        if eltype(df[i]) <: String
            df[i] = pool(df[i])
        end
    end
    return
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

function Base.convert(::Type{DataFrame}, A::Matrix)
    n = size(A, 2)
    cols = Array(Any, n)
    for i in 1:n
        cols[i] = A[:, i]
    end
    return DataFrame(cols, Index(gennames(n)))
end

function Base.convert(::Type{DataFrame}, d::Dict)
    dnames = collect(keys(d))
    sort!(dnames)
    p = length(dnames)
    p == 0 && return DataFrame()
    columns  = Array(Any, p)
    colnames = Array(Symbol, p)
    n = length(d[dnames[1]])
    for j in 1:p
        name = dnames[j]
        col = d[name]
        if length(col) != n
            throw(ArgumentError("All columns in Dict must have the same length"))
        end
        columns[j] = DataArray(col)
        colnames[j] = symbol(name)
    end
    return DataFrame(columns, Index(colnames))
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
