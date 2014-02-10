#' @@exported
#'
#' @@name
#'
#' @@description
#'
#' An DataFrame is a Julia type that implements the AbstractDataFrame
#' interface by storing a set of named columns in memory.
#'
#' @@field columns::Vector{Any} The columns of a DataFrame are stored in
#'         a vector. Each entry of this vector should be a Vector, DataVector
#'         or PooledDataVector.
#' @@field colindex::Index A data structure used to map column names to
#'         their numeric indices in `columns`.
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

#' @@exported
#'
#' @@name DataFrame
#'
#' @@description
#'
#' Construct a DataFrame from keyword arguments. Each argument should be
#' a Vector, DataVector or PooledDataVector.
#'
#' NOTE: This also covers the empty DataFrame if no keyword arguments are
#'       passed in.
#'
#' @@arg kwargs... A set of named columns to be placed in a DataFrame.
#'
#' @@return df::DataFrame A newly constructed DataFrame.
#'
#' @@examples
#'
#' df = DataFrame()
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
function DataFrame(; kwargs...)
    result = DataFrame({}, Index())
    for (k, v) in kwargs
        result[k] = v
    end
    return result
end

#' @@exported
#'
#' @@name DataFrame
#'
#' @@description
#'
#' Construct a DataFrame from a vector of columns and, optionally, specify
#' the names of the columns as a vector of symbols.
#'
#' @@return df::DataFrame A newly constructed DataFrame.
#'
#' @@examples
#'
#' df = DataFrame()
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
function DataFrame(columns::Vector{Any},
                   cnames::Vector{Symbol} = gennames(length(columns)))
    return DataFrame(columns, Index(cnames))
end

# TODO: Document these better.
function DataFrame{K, V}(d::Associative{K, V})
    # Find the first position with maximum length in the Dict.
    lengths = map(length, values(d))
    max_length = maximum(lengths)
    maxpos = findfirst(lengths .== max_length)
    keymaxlen = keys(d)[maxpos]
    nrows = max_length
    # Start with a blank DataFrame
    df = DataFrame()
    for (k, v) in d
        if length(v) == nrows
            df[k] = v
        elseif rem(nrows, length(v)) == 0    # nrows is a multiple of length(v)
            df[k] = vcat(fill(v, div(nrows, length(v)))...)
        else
            vec = fill(v[1], nrows)
            j = 1
            for i = 1:nrows
                vec[i] = v[j]
                j += 1
                if j > length(v)
                    j = 1
                end
            end
            df[k] = vec
        end
    end
    df
end

# Pandas' Dict of Vectors -> DataFrame constructor w/ explicit column names
function DataFrame(d::Dict)
    cnames = sort(Symbol[x for x in keys(d)])
    p = length(cnames)
    if p == 0
        return DataFrame()
    end
    n = length(d[cnames[1]])
    columns = Array(Any, p)
    for j in 1:p
        if length(d[cnames[j]]) != n
            throw(ArgumentError("All columns must have the same length"))
        end
        columns[j] = DataArray(d[cnames[j]])
    end
    return DataFrame(columns, Index(cnames))
end

# Pandas' Dict of Vectors -> DataFrame constructor w/o explicit column names
function DataFrame(d::Dict, cnames::Vector)
    p = length(cnames)
    if p == 0
        DataFrame()
    end
    n = length(d[cnames[1]])
    columns = Array(Any, p)
    for j in 1:p
        if length(d[cnames[j]]) != n
            error("All inputs must have the same length")
        end
        columns[j] = DataArray(d[cnames[j]])
    end
    return DataFrame(columns, Index(cnames))
end

# Initialize empty DataFrame objects of arbitrary size
# t is a Type
function DataFrame(t::Any, nrows::Integer, ncols::Integer)
    columns = Array(Any, ncols)
    for i in 1:ncols
        columns[i] = DataArray(t, nrows)
    end
    cnames = gennames(ncols)
    return DataFrame(columns, Index(cnames))
end

# TODO: Remove this
# Initialize empty DataFrame objects of arbitrary size
# Use the default column type
function DataFrame(nrows::Integer, ncols::Integer)
    columns = Array(Any, ncols)
    for i in 1:ncols
        columns[i] = DataArray(DEFAULT_COLUMN_TYPE, nrows)
    end
    cnames = gennames(ncols)
    return DataFrame(columns, Index(cnames))
end

# Initialize an empty DataFrame with specific types and names
function DataFrame(column_types::Vector, cnames::Vector, nrows::Integer)
    p = length(column_types)
    columns = Array(Any, p)
    for j in 1:p
        columns[j] = DataArray(column_types[j], nrows)
        for i in 1:nrows
            columns[j][i] = NA
        end
    end
    return DataFrame(columns, Index(cnames))
end

# Initialize an empty DataFrame with specific types
function DataFrame(column_types::Vector, nrows::Integer)
    p = length(column_types)
    columns = Array(Any, p)
    cnames = gennames(p)
    for j in 1:p
        columns[j] = DataArray(column_types[j], nrows)
        for i in 1:nrows
            columns[j][i] = NA
        end
    end
    return DataFrame(columns, Index(cnames))
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataFrame{D <: Associative}(ds::Vector{D})
    ks = [Set([[k for k in [collect(keys(d)) for d in ds]]...]...)...]
    DataFrame(ds, ks)
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataFrame{D <: Associative}(ds::Vector{D}, ks::Vector{Symbol})
    invoke(DataFrame, (Vector{D}, Vector), ds, ks)
end

function DataFrame{D <: Associative}(ds::Vector{D}, ks::Vector)
    #get column types
    col_types = Any[None for i = 1:length(ks)]
    for d in ds
        for (i,k) in enumerate(ks)
            # TODO: check for user-defined "NA" values, ala pandas
            if haskey(d, k) && !isna(d[k])
                try
                    col_types[i] = promote_type(col_types[i], typeof(d[k]))
                catch
                    col_types[i] = Any
                end
            end
        end
    end
    col_types[col_types .== None] = Any

    # create empty DataFrame, and fill
    df = DataFrame(col_types, ks, length(ds))
    for (i,d) in enumerate(ds)
        for (j,k) in enumerate(ks)
            df[i,j] = get(d, k, NA)
        end
    end

    df
end

# TODO: Remove this.
# If we have a tuple, convert each value in the tuple to a
# DataVector and then pass the converted columns in, hoping for the best
function DataFrame(vals::Any...)
    p = length(vals)
    columns = Array(Any, p)
    for j in 1:p
        if isa(vals[j], AbstractDataVector)
            columns[j] = vals[j]
        else
            columns[j] = convert(DataArray, vals[j])
        end
    end
    cnames = gennames(p)
    DataFrame(columns, Index(cnames))
end

##############################################################################
##
## Basic properties of a DataFrame
##
##############################################################################

Base.names(df::DataFrame) = names(df.colindex)

names!(df::DataFrame, vals) = names!(df.colindex, vals)

function types(adf::AbstractDataFrame)
    ncols = size(adf, 2)
    res = Array(Type, ncols)
    for j in 1:ncols
        res[j] = eltype(adf[j])
    end
    return res
end

function rename(df::DataFrame, from::Any, to::Any)
    rename(df.colindex, from, to)
end
function rename!(df::DataFrame, from::Any, to::Any)
    rename!(df.colindex, from, to)
end

# TODO: Remove these
nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1]) : 0
ncol(df::DataFrame) = length(df.colindex)

Base.size(df::AbstractDataFrame) = (nrow(df), ncol(df))
function Base.size(df::AbstractDataFrame, i::Integer)
    if i == 1
        nrow(df)
    elseif i == 2
        ncol(df)
    else
        throw(ArgumentError("DataFrames have only two dimensions"))
    end
end

Base.length(df::AbstractDataFrame) = ncol(df)
Base.endof(df::AbstractDataFrame) = ncol(df)

Base.ndims(::AbstractDataFrame) = 2

index(df::DataFrame) = df.colindex

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
#
# General Strategy:
#
# Let getindex(df.colindex, col_inds) from Index() handle the resolution
#  of column indices
# Let getindex(df.columns[j], row_inds) from AbstractDataVector() handle
#  the resolution of row indices

typealias ColumnIndex Union(Real, Symbol)

# df[SingleColumnIndex] => AbstractDataVector
function Base.getindex(df::DataFrame, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column]
end

# df[MultiColumnIndex] => (Sub)?DataFrame
function Base.getindex{T <: ColumnIndex}(df::DataFrame, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = df.columns[selected_columns]
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

# df[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(df::DataFrame, row_ind::Real, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function Base.getindex{T <: ColumnIndex}(df::DataFrame, row_ind::Real, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = {dv[[row_ind]] for dv in df.columns[selected_columns]}
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVector
function Base.getindex{T <: Real}(df::DataFrame, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function Base.getindex{R <: Real, T <: ColumnIndex}(df::DataFrame, row_inds::AbstractVector{R}, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = {dv[row_inds] for dv in df.columns[selected_columns]}
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

##############################################################################
##
## setindex!()
##
##############################################################################

function create_new_column_from_scalar(df::DataFrame, val::NAtype)
    n = max(nrow(df), 1)
    return DataArray(Array(DEFAULT_COLUMN_TYPE, n), trues(n))
end

function create_new_column_from_scalar(df::DataFrame, val::Any)
    n = max(nrow(df), 1)
    col_data = Array(typeof(val), n)
    for i in 1:n
        col_data[i] = val
    end
    return DataArray(col_data, falses(n))
end

isnextcol(df::DataFrame, col_ind::Symbol) = true
function isnextcol(df::DataFrame, col_ind::Real)
    return ncol(df) + 1 == int(col_ind)
end

function nextcolname(df::DataFrame)
    return symbol(string("x", ncol(df) + 1))
end

# Will automatically add a new column if needed
# TODO: Automatically enlarge column to required size?
function insert_single_column!(df::DataFrame,
                               dv::AbstractVector,
                               col_ind::ColumnIndex)
    dv_n, df_n = length(dv), nrow(df)
    if df_n != 0
        if dv_n != df_n
            #dv = repeat(dv, df_n)
            error("New columns must have the same length as old columns")
        end
    end
    if haskey(df.colindex, col_ind)
        j = df.colindex[col_ind]
        df.columns[j] = dv
    else
        if typeof(col_ind) <: Symbol
            push!(df.colindex, col_ind)
            push!(df.columns, dv)
        else
            if isnextcol(df, col_ind)
                push!(df.colindex, nextcolname(df))
                push!(df.columns, dv)
            else
                println("Column does not exist: $col_ind")
                error("Cannot assign to non-existent column")
            end
        end
    end
    return dv
end

# Will automatically enlarge a scalar to a DataVector if needed
function insert_single_entry!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if nrow(df) <= 1
        dv = DataArray([v], falses(1))
        insert_single_column!(df, dv, col_ind)
        return dv
    else
        try
            df.columns[df.colindex[col_ind]][row_ind] = v
            return v
        catch
            df.columns[df.colindex[col_ind]][row_ind] = NA
            return NA
        end
    end
end

upgrade_vector(v::Vector) = DataArray(v, falses(length(v)))
upgrade_vector(v::Ranges) = DataArray([v], falses(length(v)))
upgrade_vector(v::BitVector) = DataArray(convert(Array{Bool}, v), falses(length(v)))
upgrade_vector(adv::AbstractDataArray) = adv
function upgrade_scalar(df::DataFrame, v::Any)
    n = max(nrow(df), 1)
    DataArray(fill(v, n), falses(n))
end

# df[SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                v::AbstractVector,
                col_ind::ColumnIndex)
    insert_single_column!(df, upgrade_vector(v), col_ind)
end

# df[SingleColumnIndex] = Scalar (EXPANDS TO MAX(NROW(DF), 1))
function Base.setindex!(df::DataFrame,
                v::Any,
                col_ind::ColumnIndex)
    insert_single_column!(df, upgrade_scalar(df, v), col_ind)
end

# df[MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame,
                new_df::DataFrame,
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  col_inds::AbstractVector{T})
    for i in 1:length(col_inds)
        insert_single_column!(df, new_df[i], col_inds[i])
    end
    return new_df
end

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function Base.setindex!(df::DataFrame,
                v::AbstractVector,
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  v::AbstractVector,
                                  col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for col_ind in col_inds
        insert_single_column!(df, dv, col_ind)
    end
    return dv
end

# df[MultiColumnIndex] = Scalar (REPEATED FOR EACH COLUMN; EXPANDS TO MAX(NROW(DF), 1))
function Base.setindex!(df::DataFrame,
                val::Any,
                col_inds::AbstractVector{Bool})
    setindex!(df, val, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  val::Any,
                                  col_inds::AbstractVector{T})
    dv = upgrade_scalar(df, val)
    for col_ind in col_inds
        insert_single_column!(df, dv, col_ind)
    end
    return dv
end

# df[SingleRowIndex, SingleColumnIndex] = Scalar
function Base.setindex!(df::DataFrame,
                v::Any,
                row_ind::Real,
                col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
end

# df[SingleRowIndex, MultiColumnIndex] = Scalar (EXPANDS TO MAX(NROW(DF), 1))
function Base.setindex!(df::DataFrame,
                v::Any,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(df, v, row_ind, find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  v::Any,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_single_entry!(df, v, row_ind, col_ind)
    end
    return v
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
function Base.setindex!(df::DataFrame,
                new_df::DataFrame,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_ind, find(col_inds))
end

function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if haskey(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_ind] = new_df[j][1]
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return new_df
end

# df[MultiRowIndex, SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(df::DataFrame,
                           v::AbstractVector,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    dv = upgrade_vector(v)
    if haskey(df.colindex, col_ind)
        df.columns[df.colindex[col_ind]][row_inds] = dv
    else
        error("Cannot assign into a non-existent position")
    end
    return dv
end

# df[MultiRowIndex, SingleColumnIndex] = Single Value
function Base.setindex!(df::DataFrame,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    setindex!(df, v, find(row_inds), col_ind)
end
function Base.setindex!{T <: Real}(df::DataFrame,
                           v::Any,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    if haskey(df.colindex, col_ind)
        try
            df.columns[df.colindex[col_ind]][row_inds] = v
            return v
        catch
            df.columns[df.colindex[col_ind]][row_inds] = NA
            return NA
        end
    else
        error("Cannot assign into a non-existent position")
    end
end

# df[MultiRowIndex, MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame,
                new_df::DataFrame,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, new_df, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, new_df, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataFrame,
                           new_df::DataFrame,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             new_df::DataFrame,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if haskey(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_inds] = new_df[:, j]
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return new_df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  v::AbstractVector,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataFrame,
                           v::AbstractVector,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             v::AbstractVector,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if haskey(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_inds] = dv
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return dv
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    setindex!(df, v, find(row_inds), find(col_inds))
end
function Base.setindex!{T <: ColumnIndex}(df::DataFrame,
                                  v::Any,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    setindex!(df, v, find(row_inds), col_inds)
end
function Base.setindex!{R <: Real}(df::DataFrame,
                           v::Any,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, find(col_inds))
end
function Base.setindex!{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             v::Any,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if haskey(df.colindex, col_ind)
            try
                df.columns[df.colindex[col_ind]][row_inds] = v
                return v
            catch
                df.columns[df.colindex[col_ind]][row_inds] = NA
                return NA
            end
        else
            error("Cannot assign into a non-existent position")
        end
    end
end

# Special deletion assignment
Base.setindex!(df::DataFrame, x::Nothing, icol::Int) = delete!(df, icol)

##############################################################################
##
## Equality
##
##############################################################################

function isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    for idx in 1:size(df1, 2)
        isequal(df1[idx], df2[idx]) || return false
    end
    return true
end

function Base.(:(==))(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    eq = true
    for idx in 1:size(df1, 2)
        coleq = df1[idx] == df2[idx]
        # coleq could be NA
        !isequal(coleq, false) || return false
        eq &= coleq
    end
    return eq
end

##############################################################################
##
## Associative methods
##
##############################################################################

Base.haskey(df::AbstractDataFrame, key::Any) = haskey(index(df), key)
Base.get(df::AbstractDataFrame, key::Any, default::Any) = haskey(df, key) ? df[key] : default
Base.keys(df::AbstractDataFrame) = keys(index(df))
Base.values(df::DataFrame) = df.columns
Base.empty!(df::DataFrame) = DataFrame() # TODO: Make this work right

Base.isempty(df::AbstractDataFrame) = ncol(df) == 0

function Base.insert!(df::AbstractDataFrame, index::Int, item::Any, name::Any)
    @assert 0 < index <= ncol(df) + 1
    df = copy(df)
    df[name] = item
    # rearrange:
    df[[1:index-1, end, index:end-1]]
end

function Base.insert!(df::AbstractDataFrame, df2::AbstractDataFrame)
    @assert nrow(df) == nrow(df2) || nrow(df) == 0
    df = copy(df)
    for n in names(df2)
        df[n] = df2[n]
    end
    df
end

##############################################################################
##
## Copying
##
##############################################################################

# copy of a data frame does a shallow copy
function Base.copy(df::DataFrame)
	newdf = DataFrame(copy(df.columns), names(df))
end
function Base.deepcopy(df::DataFrame)
    newdf = DataFrame([copy(x) for x in df.columns], names(df))
end

##############################################################################
##
## head() and tail()
##
##############################################################################

DataArrays.head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
DataArrays.head(df::AbstractDataFrame) = head(df, 6)
DataArrays.tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
DataArrays.tail(df::AbstractDataFrame) = tail(df, 6)

# get the structure of a DF
function Base.dump(io::IO, x::AbstractDataFrame, n::Int, indent)
    println(io, typeof(x), "  $(nrow(x)) observations of $(ncol(x)) variables")
    if n > 0
        for col in names(x)[1:end]
            print(io, indent, "  ", col, ": ")
            dump(io, x[col], n - 1, string(indent, "  "))
        end
    end
end

function Base.dump(io::IO, x::AbstractDataVector, n::Int, indent)
    println(io, typeof(x), "(", length(x), ") ", x[1:min(4, end)])
end

# summarize the columns of a DF
# if the column's base type derives from Number,
# compute min, 1st quantile, median, mean, 3rd quantile, and max
# filtering NAs, which are reported separately
# if boolean, report trues, falses, and NAs
# if anything else, punt.
# Note that R creates a summary object, which has a print method. That's
# a reasonable alternative to this.
describe(dv::AbstractDataVector) = describe(STDOUT, dv)
describe(df::DataFrame) = describe(STDOUT, df)
function describe{T<:Number}(io, dv::AbstractDataVector{T})
    if all(isna(dv))
        println(io, " * All NA * ")
        return
    end
    filtered = float(dropna(dv))
    qs = quantile(filtered, [0, .25, .5, .75, 1])
    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    statVals = [qs[1:3], mean(filtered), qs[4:5]]
    for i = 1:6
        println(io, string(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
    end
    nas = sum(isna(dv))
    println(io, "NAs      $nas")
    println(io, "NA%      $(round(nas*100/length(dv), 2))%")
    return
end
function describe{T}(io, dv::AbstractDataVector{T})
    ispooled = isa(dv, PooledDataVector) ? "Pooled " : ""
    # if nothing else, just give the length and element type and NA count
    println(io, "Length  $(length(dv))")
    println(io, "Type    $(ispooled)$(string(eltype(dv)))")
    println(io, "NAs     $(sum(isna(dv)))")
    println(io, "NA%     $(round(sum(isna(dv))*100/length(dv), 2))%")
    println(io, "Unique  $(length(unique(dv)))")
    return
end

# TODO: clever layout in rows
# TODO: AbstractDataFrame
function describe(io, df::AbstractDataFrame)
    for c in 1:ncol(df)
        col = df[c]
        println(io, names(df)[c])
        describe(io, col)
        println(io, )
    end
end

# delete!() deletes columns; deleterows!() deletes rows
# delete!(df, 1)
# delete!(df, "old")
function Base.delete!(df::DataFrame, inds::Vector{Int})
    for i in 1:length(inds)
        ind = inds[i] - i + 1
        if 1 <= ind <= ncol(df)
            splice!(df.columns, ind)
            delete!(df.colindex, ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    return df
end
Base.delete!(df::DataFrame, c::Int) = delete!(df, [c])
Base.delete!(df::DataFrame, c::Any) = delete!(df, df.colindex[c])

# deleterows!()
function deleterows!(df::DataFrame, keep_inds::Vector{Int})
    for i in 1:ncol(df)
        df.columns[i] = df.columns[i][keep_inds]
    end
end

function without(df::DataFrame, icols::Vector{Int})
    newcols = _setdiff([1:ncol(df)], icols)
    if length(newcols) == 0
        throw(ArgumentError("Empty DataFrame generated by without()"))
    end
    df[newcols]
end
without(df::DataFrame, i::Int) = without(df, [i])
without(df::DataFrame, c::Any) = without(df, df.colindex[c])

#### hcat, vcat
# vcat(df, ...) only accepts data frames. Finds union of columns, maintaining order
# of first df. Missing data becomes NAs.

# two-argument form, two dfs, references only
function Base.hcat(df1::DataFrame, df2::DataFrame)
    # If df1 had metadata, we should copy that.
    colindex = Index(make_unique([names(df1), names(df2)]))
    columns = [df1.columns, df2.columns]
    d = DataFrame(columns, colindex)
    return d
end
Base.hcat{T}(df::DataFrame, x::DataVector{T}) = hcat(df, DataFrame({x}))
Base.hcat{T}(df::DataFrame, x::Vector{T}) = hcat(df, DataFrame({DataArray(x)}))
Base.hcat{T}(df::DataFrame, x::T) = hcat(df, DataFrame({DataArray([x])}))

# three-plus-argument form recurses
Base.hcat(a::DataFrame, b, c...) = hcat(hcat(a, b), c...)

Base.similar(df::DataFrame, dims) =
    DataFrame([similar(x, dims) for x in df.columns], names(df))

Base.zeros{T<:String}(::Type{T},args...) = fill("",args...) # needed for string arrays in the `nas` method above

nas{T}(dv::DataArray{T}, dims) =   # TODO move to datavector.jl?
    DataArray(zeros(T, dims), fill(true, dims))

nas{T,R}(dv::PooledDataVector{T,R}, dims) =
    PooledDataArray(DataArrays.RefArray(fill(one(R), dims)), dv.pool)

nas(df::DataFrame, dims) =
    DataFrame([nas(x, dims) for x in df.columns], names(df))

Base.vcat(df::AbstractDataFrame) = df

Base.vcat{T<:AbstractDataFrame}(dfs::Vector{T}) = vcat(dfs...)

function Base.vcat(dfs::AbstractDataFrame...)
    Nrow = sum(nrow, dfs)
    # build up column names and types
    colnams = names(dfs[1])
    coltyps = types(dfs[1])
    for i in 2:length(dfs)
        cni = names(dfs[i])
        cti = types(dfs[i])
        for j in 1:length(cni)
            cn = cni[j]
            if length(findin([cn], colnams)) == 0  # new column
                push!(colnams, cn)
                push!(coltyps, cti[j])
            end
        end
    end
    Ncol = length(colnams)
    res = DataFrame()
    for i in 1:Ncol
        coldata = {}
        for df in dfs
            push!(coldata,
                  get(df,
                      colnams[i],
                      DataArray(coltyps[i], size(df, 1))))
        end
        res[colnams[i]] = vcat(coldata...)
    end
    res
end

##
## Miscellaneous
##

function complete_cases(df::AbstractDataFrame)
    ## Returns a Vector{Bool} of indexes of complete cases (rows with no NA's).
    res = !isna(df[1])
    for i in 2:ncol(df)
        res &= !isna(df[i])
    end
    res
end

complete_cases!(df::AbstractDataFrame) = deleterows!(df, find(complete_cases(df)))

function DataArrays.array(adf::AbstractDataFrame)
    n, p = size(adf)
    T = reduce(typejoin, types(adf))
    res = Array(T, n, p)
    for j in 1:p
        col = adf[j]
        for i in 1:n
            res[i, j] = col[i]
        end
    end
    return res
end

function DataArrays.DataArray(adf::AbstractDataFrame,
                              T::DataType = reduce(typejoin, types(adf)))
    n, p = size(adf)
    dm = DataArray(T, n, p)
    for j in 1:p
        col = adf[j]
        for i in 1:n
            dm[i, j] = col[i]
        end
    end
    return dm
end

function nonunique(df::AbstractDataFrame)
    # Return a Vector{Bool} indicated whether the row is a duplicate
    # of a prior row.
    res = fill(false, nrow(df))
    di = Dict()
    for i in 1:nrow(df)
        if haskey(di, array(df[i, :])) # Used to convert to Any type
            res[i] = true
        else
            di[array(df[i, :])] = 1 # Used to convert to Any type
        end
    end
    res
end

function unique!(df::AbstractDataFrame)
    deleterows!(df, find(!nonunique(df)))
end

# Unique rows of an AbstractDataFrame.
Base.unique(df::AbstractDataFrame) = df[!nonunique(df), :]

function nonuniquekey(df::AbstractDataFrame)
    # Here's another (probably a lot faster) way to do `nonunique`
    # by grouping on all columns. It will fail if columns cannot be
    # made into PooledDataVector's.
    gd = groupby(df, names(df))
    idx = [1:length(gd.idx)][gd.idx][gd.starts]
    res = fill(true, nrow(df))
    res[idx] = false
    res
end

function cleannames!(df::DataFrame)
    oldnames = map(strip, names(df))
    newnames = map(n -> replace(n, r"\W", "_"), oldnames)
    names!(df, newnames)
    return
end

##############################################################################
##
## Hashing
##
## Make sure this agrees with is_equals()
##
##############################################################################

function Base.hash(adf::AbstractDataFrame)
    h = hash(size(adf)) + 1
    for i in 1:size(adf, 2)
        h = bitmix(h, int(hash(adf[i])))
    end
    return uint(h)
end

# Pooling

pool(a::AbstractVector) = compact(PooledDataArray(a))

function pool!(df::AbstractDataFrame, cname::Union(Integer, Symbol))
    df[cname] = pool(df[cname])
    return
end

function pool!{T <: Union(Integer, Symbol)}(df::AbstractDataFrame, cnames::Vector{T})
    for cname in cnames
        df[cname] = pool(df[cname])
    end
    return
end

function pool!(df)
    for i in 1:size(df, 2)
        if eltype(df[i]) <: String
            df[i] = pool(df[i])
        end
    end
    return
end

function Base.append!(adf1::AbstractDataFrame,
                      adf2::AbstractDataFrame)
   names(adf1) == names(adf2) || error("Column names do not match")
   types(adf1) == types(adf2) || error("Column types do not match")
   ncols = size(adf1, 2)
   # TODO: This needs to be a sort of transaction to be 100% safe
   for j in 1:ncols
       append!(adf1[j], adf2[j])
   end
   return adf1
end

function Base.convert(::Type{DataFrame}, A::Matrix)
    n = size(A, 2)
    cols = Array(Any, n)
    for i in 1:n
        cols[i] = A[:, i]
    end
    return DataFrame(cols, Index(gennames(n)))
end
