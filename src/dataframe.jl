##############################################################################
##
## AbstractDataFrame includes DataFrame and SubDataFrame
##
##############################################################################

abstract AbstractDataFrame <: Associative{Any, Any}

##############################################################################
##
## Basic DataFrame definition
##
## A DataFrame is a vector of heterogeneous DataVec's that be accessed using
## numeric indexing for both rows and columns and name-based indexing for
## columns. The columns are stored in a vector, which means that operations
## that insert/delete columns are O(n).
##
##############################################################################

type DataFrame <: AbstractDataFrame
    columns::Vector{Any}
    colindex::Index
    function DataFrame(cols::Vector, colind::Index)
        # all columns have to be the same length
        if length(cols) > 1 && !all(map(length, cols) .== length(cols[1]))
            error("all columns in a DataFrame have to be the same length")
        end
        # colindex has to be the same length as columns vector
        if length(colind) != length(cols)
            error("column names/index must be the same length as the number of columns")
        end
        new(cols, colind)
    end
end

##############################################################################
##
## DataFrame constructors
##
##############################################################################

# The empty DataFrame
DataFrame() = DataFrame({}, Index())

# No-op given a DataFrame
DataFrame(x::DataFrame) = x

# A single numeric value (what about others, like strings?)
DataFrame(x::Number) = DataFrame(DataVec([x]))

# Convert an arbitrary vector w/ pre-specified names
DataFrame{T <: String}(cs::Vector, cn::Vector{T}) = DataFrame(cs, Index(cn))

# Convert an arbitrary vector w/o pre-specified names
DataFrame(cs::Vector) = DataFrame(cs, Index(generate_column_names(length(cs))))

# Build a DataFrame from an expression
# TODO expand the following to allow unequal lengths that are rep'd to the longest length.
DataFrame(ex::Expr) = based_on(DataFrame(), ex)

# Convert a standard Matrix to a DataFrame w/ pre-specified names
function DataFrame{T}(x::Matrix{T}, cn::Vector)
    DataFrame({DataVec(x[:, i]) for i in 1:length(cn)}, cn)
end

# Convert a standard Matrix to a DataFrame w/o pre-specified names
function DataFrame{T}(x::Matrix{T})
    DataFrame(x, generate_column_names(size(x, 2)))
end

# If we have something a tuple, convert each value in the tuple to a
# DataVec and then pass the converted columns in, hoping for the best
DataFrame(vals...) = DataFrame([DataVec(x) for x = vals])

function DataFrame{K,V}(d::Associative{K,V})
    # Find the first position with maximum length in the Dict.
    # I couldn't get findmax to work here.
    ## (Nrow,maxpos) = findmax(map(length, values(d)))
    lengths = map(length, values(d))
    maxpos = findfirst(lengths .== max(lengths))
    keymaxlen = keys(d)[maxpos]
    Nrow = length(d[keymaxlen])
    # Start with a blank DataFrame
    df = DataFrame() 
    for (k,v) in d
        if length(v) == Nrow
            df[k] = v  
        elseif rem(Nrow, length(v)) == 0    # Nrow is a multiple of length(v)
            df[k] = vcat(fill(v, div(Nrow, length(v)))...)
        else
            vec = fill(v[1], Nrow)
            j = 1
            for i = 1:Nrow
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

# Construct a DataFrame with groupings over the columns
function DataFrame(cs::Vector, cn::Vector, gr::Dict{ByteString,Vector{ByteString}})
  d = DataFrame(cs, cn)
  set_groups(index(d), gr)
  return d
end

# Pandas' Dict of Vectors -> DataFrame constructor w/ explicit column names
function DataFrame(d::Dict)
    column_names = sort(convert(Array{ByteString, 1}, keys(d)))
    p = length(column_names)
    if p == 0
        DataFrame(0, 0)
    end
    n = length(d[column_names[1]])
    columns = Array(Any, p)
    for j in 1:p
        if length(d[column_names[j]]) != n
            error("All inputs must have the same length")
        end
        columns[j] = DataVec(d[column_names[j]])
    end
    return DataFrame(columns, Index(column_names))
end

# Pandas' Dict of Vectors -> DataFrame constructor w/o explicit column names
function DataFrame(d::Dict, column_names::Vector)
    p = length(column_names)
    if p == 0
        DataFrame(0, 0)
    end
    n = length(d[column_names[1]])
    columns = Array(Any, p)
    for j in 1:p
        if length(d[column_names[j]]) != n
            error("All inputs must have the same length")
        end
        columns[j] = DataVec(d[column_names[j]])
    end
    return DataFrame(columns, Index(column_names))
end

# Initialize empty DataFrame objects of arbitrary size
# t is a Type
function DataFrame(t::Any, nrows::Int64, ncols::Int64)
    column_types = Array(Any, ncols)
    for i in 1:ncols
        column_types[i] = t
    end
    column_names = Array(ByteString, 0)
    DataFrame(column_types, column_names, nrows)
end

# Initialize empty DataFrame objects of arbitrary size
# Default to Float64 as the type
function DataFrame(nrows::Int64, ncols::Int64)
    DataFrame(Float64, nrows::Int64, ncols::Int64)
end

# Initialize an empty DataFrame with specific types and names
function DataFrame(column_types::Vector, column_names::Vector, n::Int64)
  p = length(column_types)
  columns = Array(Any, p)

  if column_names == []
    names = Array(ByteString, p)
    for j in 1:p
      names[j] = "x$j"
    end
  else
    names = column_names
  end

  for j in 1:p
    columns[j] = DataVec(Array(column_types[j], n), Array(Bool, n))
    for i in 1:n
      columns[j][i] = baseval(column_types[j])
      columns[j][i] = NA
    end
  end

  DataFrame(columns, Index(names))
end

# Initialize an empty DataFrame with specific types
function DataFrame(column_types::Vector, nrows::Int64)
    p = length(column_types)
    column_names = Array(ByteString, 0)
    DataFrame(column_types, column_names, nrows)
end

# Initialize from a Vector of Associatives (aka list of dicts)
function DataFrame{D<:Associative}(ds::Vector{D})
    ks = [Set([[k for k in [keys(d) for d in ds]]...]...)...]
    DataFrame(ds, ks)
end

# Initialize from a Vector of Associatives (aka list of dicts)
DataFrame{D<:Associative,T<:String}(ds::Vector{D}, ks::Vector{T}) = 
    invoke(DataFrame, (Vector{D}, Vector), ds, ks)
function DataFrame{D<:Associative}(ds::Vector{D}, ks::Vector)
    #get column types
    col_types = Any[None for i = 1:length(ks)]
    for d in ds
        for (i,k) in enumerate(ks)
            # TODO: check for user-defined "NA" values, ala pandas
            if has(d, k) && !isna(d[k])
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

##############################################################################
##
## Basic properties of a DataFrame
##
##############################################################################

colnames(df::DataFrame) = names(df.colindex)
colnames!(df::DataFrame, vals) = names!(df.colindex, vals)

coltypes(df::DataFrame) = {eltype(df[i]) for i in 1:ncol(df)}

names(df::AbstractDataFrame) = colnames(df)
names!(df::DataFrame, vals) = names!(df.colindex, vals)

replace_names(df::DataFrame, from, to) = replace_names(df.colindex, from, to)
replace_names!(df::DataFrame, from, to) = replace_names!(df.colindex, from, to)

nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1]) : 0
ncol(df::DataFrame) = length(df.colindex)

size(df::AbstractDataFrame) = (nrow(df), ncol(df))
function size(df::AbstractDataFrame, i::Int)
    if i == 1
        nrow(df)
    elseif i == 2
        ncol(df)
    else
        error("DataFrames have two dimensions only")
    end
end

length(df::AbstractDataFrame) = ncol(df)

ndims(::AbstractDataFrame) = 2

index(df::DataFrame) = df.colindex

##############################################################################
##
## Tools for working with groups of columsn
##
##############################################################################

function reconcile_groups(olddf, newdf)
	# foreach group, restrict range to intersection with newdf colnames
	# add back any groups with non-null range
	old_groups = get_groups(olddf)
	for key in keys(old_groups)
		# this is clunky -- there are better/faster ways of doing this intersection operation
		match_vals = ByteString[]
		for val in old_groups[key]
			if contains(colnames(newdf), val)
				push(match_vals, val)
			end
		end
		if !isempty(match_vals)
			set_group(newdf, key, match_vals)
		end
	end
	newdf
end

# TODO: reconcile_groups

##############################################################################
##
## ref() definitions
##
##############################################################################

# Cases:
#
# df[SingleColumnIndex] => AbstractDataVec
# df[MultiColumnIndex] => (Sub)?DataFrame
# df[SingleRowIndex, SingleColumnIndex] => Scalar
# df[SingleRowIndex, MultiColumnIndex] => (Sub)?DataFrame
# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVec
# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
#
# General Strategy:
#
# Let ref(df.colindex, col_inds) from Index() handle the resolution
#  of column indices
# Let ref(df.columns[j], row_inds) from AbstractDataVec() handle
#  the resolution of row indices

typealias ColumnIndex Union(Real, String, Symbol)

# df[SingleColumnIndex] => AbstractDataVec
function ref(df::DataFrame, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column]
end

# df[MultiColumnIndex] => (Sub)?DataFrame
function ref{T <: ColumnIndex}(df::DataFrame, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = df.columns[selected_columns]
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

# df[SingleRowIndex, SingleColumnIndex] => Scalar
function ref(df::DataFrame, row_ind::Real, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function ref{T <: ColumnIndex}(df::DataFrame, row_ind::Real, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = {dv[[row_ind]] for dv in df.columns[selected_columns]}
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

# df[MultiRowIndex, SingleColumnIndex] => (Sub)?AbstractDataVec
function ref{T <: Real}(df::DataFrame, row_inds::AbstractVector{T}, col_ind::ColumnIndex)
    selected_column = df.colindex[col_ind]
    return df.columns[selected_column][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => (Sub)?DataFrame
function ref{R <: Real, T <: ColumnIndex}(df::DataFrame, row_inds::AbstractVector{R}, col_inds::AbstractVector{T})
    selected_columns = df.colindex[col_inds]
    new_columns = {dv[row_inds] for dv in df.columns[selected_columns]}
    return DataFrame(new_columns, Index(df.colindex.names[selected_columns]))
end

# Special cases involving expressions
ref(df::DataFrame, ex::Expr) = ref(df, with(df, ex))
ref(df::DataFrame, ex::Expr, c::ColumnIndex) = ref(df, with(df, ex), c)
ref{T <: ColumnIndex}(df::DataFrame, ex::Expr, c::AbstractVector{T}) = ref(df, with(df, ex), c)
ref(df::DataFrame, c::Real, ex::Expr) = ref(df, c, with(df, ex))
ref{T <: Real}(df::DataFrame, c::AbstractVector{T}, ex::Expr) = ref(df, c, with(df, ex))
ref(df::DataFrame, ex1::Expr, ex2::Expr) = ref(df, with(df, ex1), with(df, ex2))

##############################################################################
##
## assign()
##
##############################################################################

function create_new_column_from_scalar(df::DataFrame, val::NAtype)
    n = max(nrow(df), 1)
    return DataVec(Array(DEFAULT_COLUMN_TYPE, n), trues(n))
end

function create_new_column_from_scalar(df::DataFrame, val::Any)
    n = max(nrow(df), 1)
    col_data = Array(typeof(val), n)
    for i in 1:n
        col_data[i] = val
    end
    return DataVec(col_data, falses(n))
end

isnextcol(df::DataFrame, col_ind::String) = true
isnextcol(df::DataFrame, col_ind::Symbol) = true
function isnextcol(df::DataFrame, col_ind::Real)
    return ncol(df) + 1 == int(col_ind)
end

function nextcolname(df::DataFrame)
    return strcat("x", string(ncol(df) + 1))
end

# Will automatically add a new column if needed
# TODO: Automatically enlarge column to required size?
function insert_single_column!(df::DataFrame,
                               dv::AbstractDataVec,
                               col_ind::ColumnIndex)
    dv_n, df_n = length(dv), nrow(df)
    if df_n != 0
        if dv_n != df_n
            #dv = repeat(dv, df_n)
            error("New columns must have the same length as old columns")
        end
    end
    if has(df.colindex, col_ind)
        j = df.colindex[col_ind]
        df.columns[j] = dv
    else
        if typeof(col_ind) <: String || typeof(col_ind) <: Symbol
            push(df.colindex, col_ind)
            push(df.columns, dv)
        else
            if isnextcol(df, col_ind)
                push(df.colindex, nextcolname(df))
                push(df.columns, dv)
            else
                println("Column does not exist: $col_ind")
                error("Cannot assign to non-existent column")
            end
        end
    end
    return dv
end

# Will automatically enlarge a scalar to a DataVec if needed
function insert_single_entry!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if nrow(df) <= 1
        dv = DataVec([v], falses(1))
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

upgrade_vector(v::Vector) = DataVec(v, falses(length(v)))
upgrade_vector(v::Ranges) = DataVec([v], falses(length(v)))
upgrade_vector(v::BitVector) = DataVec(convert(Array{Bool}, v), falses(length(v)))
upgrade_vector(adv::AbstractDataVec) = adv
function upgrade_scalar(df::DataFrame, v::Any)
    n = max(nrow(df), 1)
    DataVec(fill(v, n), falses(n))
end

# df[SingleColumnIndex] = AbstractVector
function assign(df::DataFrame,
                v::AbstractVector,
                col_ind::ColumnIndex)
    insert_single_column!(df, upgrade_vector(v), col_ind)
end

# df[SingleColumnIndex] = Scalar (EXPANDS TO MAX(NROW(DF), 1))
function assign(df::DataFrame,
                v::Any,
                col_ind::ColumnIndex)
    insert_single_column!(df, upgrade_scalar(df, v), col_ind)
end

# df[MultiColumnIndex] = DataFrame
function assign(df::DataFrame,
                new_df::DataFrame,
                col_inds::AbstractVector{Bool})
    assign(df, new_df, find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  col_inds::AbstractVector{T})
    for i in 1:length(col_inds)
        insert_single_column!(df, new_df[i], col_inds[i])
    end
    return new_df
end

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
function assign(df::DataFrame,
                v::AbstractVector,
                col_inds::AbstractVector{Bool})
    assign(df, v, find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  v::AbstractVector,
                                  col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for col_ind in col_inds
        insert_single_column!(df, dv, col_ind)
    end
    return dv
end

# df[MultiColumnIndex] = Scalar (REPEATED FOR EACH COLUMN; EXPANDS TO MAX(NROW(DF), 1))
function assign(df::DataFrame,
                val::Any,
                col_inds::AbstractVector{Bool})
    assign(df, val, find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  val::Any,
                                  col_inds::AbstractVector{T})
    dv = upgrade_scalar(df, val)
    for col_ind in col_inds
        insert_single_column!(df, dv, col_ind)
    end
    return dv
end

# df[SingleRowIndex, SingleColumnIndex] = Scalar
function assign(df::DataFrame,
                v::Any,
                row_ind::Real,
                col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
end

# df[SingleRowIndex, MultiColumnIndex] = Scalar (EXPANDS TO MAX(NROW(DF), 1))
function assign(df::DataFrame,
                v::Any,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    assign(df, v, row_ind, find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  v::Any,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for col_ind in col_inds
        insert_single_entry!(df, v, row_ind, col_ind)
    end
    return v
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
function assign(df::DataFrame,
                new_df::DataFrame,
                row_ind::Real,
                col_inds::AbstractVector{Bool})
    assign(df, new_df, row_ind, find(col_inds))
end

function assign{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  row_ind::Real,
                                  col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if has(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_ind] = new_df[j][1]
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return new_df
end

# df[MultiRowIndex, SingleColumnIndex] = AbstractVector
function assign(df::DataFrame,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    assign(df, v, find(row_inds), col_ind)
end
function assign{T <: Real}(df::DataFrame,
                           v::AbstractVector,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    dv = upgrade_vector(v)
    if has(df.colindex, col_ind)
        df.columns[df.colindex[col_ind]][row_inds] = dv
    else
        error("Cannot assign into a non-existent position")
    end
    return dv
end

# df[MultiRowIndex, SingleColumnIndex] = Single Value
function assign(df::DataFrame,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_ind::ColumnIndex)
    assign(df, v, find(row_inds), col_ind)
end
function assign{T <: Real}(df::DataFrame,
                           v::Any,
                           row_inds::AbstractVector{T},
                           col_ind::ColumnIndex)
    if has(df.colindex, col_ind)
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
function assign(df::DataFrame,
                new_df::DataFrame,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    assign(df, new_df, find(row_inds), find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  new_df::DataFrame,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    assign(df, new_df, find(row_inds), col_inds)
end
function assign{R <: Real}(df::DataFrame,
                           new_df::DataFrame,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    assign(df, new_df, row_inds, find(col_inds))
end
function assign{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             new_df::DataFrame,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if has(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_inds] = new_df[:, j]
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return new_df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function assign(df::DataFrame,
                v::AbstractVector,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    assign(df, v, find(row_inds), find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  v::AbstractVector,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    assign(df, v, find(row_inds), col_inds)
end
function assign{R <: Real}(df::DataFrame,
                           v::AbstractVector,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    assign(df, v, row_inds, find(col_inds))
end
function assign{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             v::AbstractVector,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    dv = upgrade_vector(v)
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if has(df.colindex, col_ind)
            df.columns[df.colindex[col_ind]][row_inds] = dv
        else
            error("Cannot assign into a non-existent position")
        end
    end
    return dv
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function assign(df::DataFrame,
                v::Any,
                row_inds::AbstractVector{Bool},
                col_inds::AbstractVector{Bool})
    assign(df, v, find(row_inds), find(col_inds))
end
function assign{T <: ColumnIndex}(df::DataFrame,
                                  v::Any,
                                  row_inds::AbstractVector{Bool},
                                  col_inds::AbstractVector{T})
    assign(df, v, find(row_inds), col_inds)
end
function assign{R <: Real}(df::DataFrame,
                           v::Any,
                           row_inds::AbstractVector{R},
                           col_inds::AbstractVector{Bool})
    assign(df, v, row_inds, find(col_inds))
end
function assign{R <: Real, T <: ColumnIndex}(df::DataFrame,
                                             v::Any,
                                             row_inds::AbstractVector{R},
                                             col_inds::AbstractVector{T})
    for j in 1:length(col_inds)
        col_ind = col_inds[j]
        if has(df.colindex, col_ind)
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
assign(df::DataFrame, x::Nothing, icol::Int) = del(df, icol)

# Special cases involving expressions
function assign(df::DataFrame, val::Any, ex::Expr)
    assign(df, val, with(df, ex))
end
function assign(df::DataFrame, val::Any, ex::Expr, c::ColumnIndex)
    assign(df, val, with(df, ex), c)
end
function assign{T <: ColumnIndex}(df::DataFrame, val::Any, ex::Expr, c::AbstractVector{T})
    assign(df, val, with(df, ex), c)
end
function assign(df::DataFrame, val::Any, c::Real, ex::Expr)
    assign(df, val, c, with(df, ex))
end
function assign{T <: Real}(df::DataFrame, val::Any, c::AbstractVector{T}, ex::Expr)
    assign(df, val, c, with(df, ex))
end
function assign(df::DataFrame, val::Any, ex1::Expr, ex2::Expr)
    assign(df, val, with(df, ex1), with(df, ex2))
end

#
#
#
#
#
#

# Associative methods:
has(df::AbstractDataFrame, key) = has(index(df), key)
get(df::AbstractDataFrame, key, default) = has(df, key) ? df[key] : default
keys(df::AbstractDataFrame) = keys(index(df))
values(df::DataFrame) = df.columns
del_all(df::DataFrame) = DataFrame()

# Collection methods:
start(df::AbstractDataFrame) = 1
done(df::AbstractDataFrame, i) = i > ncol(df)
next(df::AbstractDataFrame, i) = (df[i], i + 1)

## numel(df::AbstractDataFrame) = ncol(df)
isempty(df::AbstractDataFrame) = ncol(df) == 0

# Column groups
set_group(d::AbstractDataFrame, newgroup, names) = set_group(index(d), newgroup, names)
set_groups(d::AbstractDataFrame, gr::Dict{ByteString,Vector{ByteString}}) = set_groups(index(d), gr)
get_groups(d::AbstractDataFrame) = get_groups(index(d))
rename_group!(d::AbstractDataFrame,a,b) =  replace_names!(index(d), a, b)

function insert(df::AbstractDataFrame, index::Int, item, name)
    @assert 0 < index <= ncol(df) + 1
    df = copy(df)
    df[name] = item
    # rearrange:
    df[[1:index-1, end, index:end-1]]
end

function insert(df::AbstractDataFrame, df2::AbstractDataFrame)
    @assert nrow(df) == nrow(df2) || nrow(df) == 0
    df = copy(df)
    for n in colnames(df2)
        df[n] = df2[n]
    end
    df
end

# copy of a data frame does a shallow copy
function copy(df::DataFrame)
	newdf = DataFrame(copy(df.columns), colnames(df))
	reconcile_groups(df, newdf)
end
function deepcopy(df::DataFrame)
    newdf = DataFrame([copy(x) for x in df.columns], colnames(df))
    reconcile_groups(df, newdf)
end
#deepcopy_with_groups(df::DataFrame) = DataFrame([copy(x) for x in df.columns], colnames(df), get_groups(df))

# dimilar of a data frame creates new vectors, but with the same columns. Dangerous, as 
# changing the in one df can break the other.

head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
head(df::AbstractDataFrame) = head(df, 6)
tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
tail(df::AbstractDataFrame) = tail(df, 6)

# to print a DataFrame, find the max string length of each column
# then print the column names with an appropriate buffer
# then row-by-row print with an appropriate buffer
_string(x) = sprint(showcompact, x)
maxShowLength(v::Vector) = length(v) > 0 ? max([length(_string(x)) for x = v]) : 0
maxShowLength(dv::AbstractDataVec) = max([length(_string(x)) for x = dv])
function show(io, df::AbstractDataFrame)
    ## TODO use alignment() like print_matrix in show.jl.
    nrowz, ncolz = size(df)
    println(io, "$(nrowz)x$(ncolz) $(typeof(df)):")
    gr = get_groups(df)
    if length(gr) > 0
        #print(io, "Column groups: ")
        pretty_show(io, gr)
        println(io)
    end
    N = nrow(df)
    Nmx = 20   # maximum head and tail lengths
    if N <= 2Nmx
        rowrng = 1:min(2Nmx,N)
    else
        rowrng = [1:Nmx, N-Nmx+1:N]
    end
    # we don't have row names -- use indexes
    rowNames = [@sprintf("[%d,]", r) for r = rowrng]
    
    rownameWidth = maxShowLength(rowNames)
    
    # if we don't have columns names, use indexes
    # note that column names in R are obligatory
    if eltype(colnames(df)) == Nothing
        colNames = [@sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = colnames(df)
    end
    
    colWidths = [max(length(string(colNames[c])), maxShowLength(df[rowrng,c])) for c = 1:ncol(df)]

    header = strcat(" " ^ (rownameWidth+1),
                    join([lpad(string(colNames[i]), colWidths[i]+1, " ") for i = 1:ncol(df)], ""))
    println(io, header)

    for i = 1:length(rowrng)
        rowname = rpad(string(rowNames[i]), rownameWidth+1, " ")
        line = strcat(rowname,
                      join([lpad(_string(df[rowrng[i],c]), colWidths[c]+1, " ") for c = 1:ncol(df)], ""))
        println(io, line)
        if i == Nmx && N > 2Nmx
            println(io, "  :")
        end
    end
end

# get the structure of a DF
function dump(io::IOStream, x::AbstractDataFrame, n::Int, indent)
    println(io, typeof(x), "  $(nrow(x)) observations of $(ncol(x)) variables")
    gr = get_groups(x)
    if length(gr) > 0
        pretty_show(io, gr)
        println(io)
    end
    if n > 0
        for col in names(x)[1:end]
            print(io, indent, "  ", col, ": ")
            dump(io, x[col], n - 1, strcat(indent, "  "))
        end
    end
end
dump(io::IOStream, x::AbstractDataVec, n::Int, indent) =
    println(io, typeof(x), "(", length(x), ") ", x[1:min(4, end)])

# summarize the columns of a DF
# if the column's base type derives from Number, 
# compute min, 1st quantile, median, mean, 3rd quantile, and max
# filtering NAs, which are reported separately
# if boolean, report trues, falses, and NAs
# if anything else, punt.
# Note that R creates a summary object, which has a print method. That's
# a reasonable alternative to this. The describe() functions in show.jl
# return a string.
describe(dv::AbstractDataVec) = describe(OUTPUT_STREAM::IOStream, dv)
describe(df::DataFrame) = describe(OUTPUT_STREAM::IOStream, df)
function describe{T<:Number}(io, dv::AbstractDataVec{T})
    filtered = float(removeNA(dv))
    qs = quantile(filtered, [0, .25, .5, .75, 1])
    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    statVals = [qs[1:3], mean(filtered), qs[4:5]]
    for i = 1:6
        println(io, strcat(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
    end
    nas = sum(isna(dv))
    if nas > 0
        println(io, "NAs      $nas")
    end
end
function describe{T}(io, dv::AbstractDataVec{T})
    ispooled = isa(dv, PooledDataVec) ? "Pooled " : ""
    # if nothing else, just give the length and element type and NA count
    println(io, "Length: $(length(dv))")
    println(io, "Type  : $(ispooled)$(string(eltype(dv)))")
    println(io, "NAs   : $(sum(isna(dv)))")
end

# TODO: clever layout in rows
# TODO: AbstractDataFrame
function describe(io, df::AbstractDataFrame)
    for c in 1:ncol(df)
        col = df[c]
        println(io, colnames(df)[c])
        describe(io, col)
        println(io, )
    end
end

##############################################################################
##
## We use SubDataFrame's to maintain a reference to a subset of a DataFrame
## without making copies.
##
##############################################################################

# a SubDataFrame is a lightweight wrapper around a DataFrame used most frequently in
# split/apply sorts of operations.
type SubDataFrame <: AbstractDataFrame
    parent::DataFrame
    rows::Vector{Int} # maps from subdf row indexes to parent row indexes
    
    function SubDataFrame(parent::DataFrame, rows::Vector{Int})
        if any(rows .< 1)
            error("all SubDataFrame indices must be > 0")
        end
        if max(rows) > nrow(parent)
            error("all SubDataFrame indices must be <= the number of rows of the DataFrame")
        end
        new(parent, rows)
    end
end

sub(D::DataFrame, r, c) = sub(D[[c]], r)    # If columns are given, pass in a subsetted parent D.
                                            # Columns are not copies, so it's not expensive.
sub(D::DataFrame, r::Int) = sub(D, [r])
sub(D::DataFrame, rs::Vector{Int}) = SubDataFrame(D, rs)
sub(D::DataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # this is a wacky fall-through that uses light-weight fake indexes!
sub(D::DataFrame, ex::Expr) = sub(D, with(D, ex))

sub(D::SubDataFrame, r, c) = sub(D[[c]], r)
sub(D::SubDataFrame, r::Int) = sub(D, [r])
sub(D::SubDataFrame, rs::Vector{Int}) = SubDataFrame(D.parent, D.rows[rs])
sub(D::SubDataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # another wacky fall-through
sub(D::SubDataFrame, ex::Expr) = sub(D, with(D, ex))
const subset = sub

ref(df::SubDataFrame, c) = df.parent[df.rows, c]
ref(df::SubDataFrame, r, c) = df.parent[df.rows[r], c]

nrow(df::SubDataFrame) = length(df.rows)
ncol(df::SubDataFrame) = ncol(df.parent)
colnames(df::SubDataFrame) = colnames(df.parent) 

# Associative methods:
index(df::SubDataFrame) = index(df.parent)

# del(df, 1)
# del(df, "old")
function del(df::DataFrame, inds::Vector{Int})
    for ind in inds
        if 1 <= ind <= ncol(df)
            del(df.columns, ind)
            del(df.colindex, ind)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    return df
end
del(df::DataFrame, c::Int) = del(df, [c])
del(df::DataFrame, c::Any) = del(df, df.colindex[c])
del(df::SubDataFrame, c::Any) = SubDataFrame(del(df.parent, c), df.rows)

function without(df::DataFrame, icols::Vector{Int})
    newcols = _setdiff([1:ncol(df)], icols)
    if length(newcols) == 0
        throw(ArgumentError("Empty DataFrame generated by without()"))
    end
    df[newcols]
end
without(df::DataFrame, i::Int) = without(df, [i])
without(df::DataFrame, c::Any) = without(df, df.colindex[c])
without(df::SubDataFrame, c::Any) = SubDataFrame(without(df.parent, c), df.rows)

#### cbind, rbind, hcat, vcat
# hcat() is just cbind()
# rbind(df, ...) only accepts data frames. Finds union of columns, maintaining order
# of first df. Missing data becomes NAs.
# vcat() is just rbind()
 
# two-argument form, two dfs, references only
function cbind(df1::DataFrame, df2::DataFrame)
    # If df1 had metadata, we should copy that.
    colindex = Index(make_unique(concat(colnames(df1), colnames(df2))))
    columns = [df1.columns, df2.columns]
    d = DataFrame(columns, colindex)  
    set_groups(d, get_groups(df1))
    set_groups(d, get_groups(df2))
    return d
end

function cbind{T}(df::DataFrame, x::DataVec{T})
    cbind(df, DataFrame({x}))
end

function cbind{T}(df::DataFrame, x::Vector{T})
    cbind(df, DataFrame({DataVec(x)}))
end

function cbind{T}(df::DataFrame, x::T)
    cbind(df, DataFrame({DataVec([x])}))
end

# three-plus-argument form recurses
cbind(a, b, c...) = cbind(cbind(a, b), c...)
hcat(dfs::DataFrame...) = cbind(dfs...)

is_group(df::AbstractDataFrame, name::ByteString) = is_group(index(df), name)

similar(df::DataFrame, dims) = 
    DataFrame([similar(x, dims) for x in df.columns], colnames(df)) 

similar(df::SubDataFrame, dims) = 
    DataFrame([similar(df[x], dims) for x in colnames(df)], colnames(df)) 

nas{T}(dv::DataVec{T}, dims) =   # TODO move to datavec.jl?
    DataVec(zeros(T, dims), fill(true, dims))

zeros{T<:ByteString}(::Type{T},args...) = fill("",args...) # needed for string arrays in the `nas` method above
    
nas{T}(dv::PooledDataVec{T}, dims) =
    PooledDataVec(fill(uint16(1), dims), dv.pool)

nas(df::DataFrame, dims) = 
    DataFrame([nas(x, dims) for x in df.columns], colnames(df)) 

nas(df::SubDataFrame, dims) = 
    DataFrame([nas(df[x], dims) for x in colnames(df)], colnames(df)) 

rbind(df::DataFrame) = df

function rbind(df1::DataFrame, df2::DataFrame)
    if size(df1) == (0, 0) && size(df2) == (0, 0)
        return DataFrame(0, 0)
    end
    if size(df1) == (0, 0) && size(df2) != (0, 0)
        return df2
    end
    if size(df1) != (0, 0) && size(df2) == (0, 0)
        return df1
    end
    # Tolerate permutations of the same columns?
    # if any(coltypes(df1) .!= coltypes(df2)) || any(colnames(df1) .!= colnames(df2))
    #     error("Cannot rbind dissimilar DataFrames")
    # end
    res = DataFrame(coltypes(df1), nrow(df1) + nrow(df2))
    colnames!(res, colnames(df1))
    ind = 0
    for i in 1:nrow(df1)
        ind += 1
        for j in 1:ncol(df1)
            res[ind, j] = df1[i, j]
        end
    end
    for i in 1:nrow(df2)
        ind += 1
        for j in 1:ncol(df1)
            res[ind, j] = df2[i, j]
        end
    end
    return res
end

# Use induction to define results for arbitrary lengths?
function rbind(dfs::DataFrame...)
    res = dfs[1]
    for j in 2:length(dfs)
        res = rbind(res, dfs[j])
    end
    return res
end

function rbind(dfs::Vector)   # for a Vector of DataFrame's
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
    # for idx in 2:length(dfs)
    #     if colnames(dfs[1]) != colnames(dfs[idx])
    #         error("DataFrame column names must match.")
    #     end
    # end
    idx = 1
    for df in dfs
        for kdx in 1:nrow(df)
            for jdx in 1:Ncol
                res[jdx][idx] = df[kdx, jdx]
            end
            idx += 1
        end
        set_groups(res, get_groups(df))
    end
    res
end

# function rbind(dfs::DataFrame...)
#     L = length(dfs)
#     T = 0
#     total_rows = 0
#     real_cols = 0
#     non_empty_dfs = Array(Any, L)
#     for i in 1:L
#         df = dfs[i]
#         if nrow(df) > 0
#             T += 1
#             non_empty_dfs[T] = df
#         end
#     end
#     if T == 0
#         return DataFrame(0, 0)
#     end
#     dfs = non_empty_dfs[1:T]
#     Nrow = sum(nrow, dfs)
#     Ncol = max(ncol, dfs)
#     res = similar(dfs[1], Nrow)
#     # TODO fix PooledDataVec columns with different pools.
#     # for idx in 2:length(dfs)
#     #     if colnames(dfs[1]) != colnames(dfs[idx])
#     #         error("DataFrame column names must match.")
#     #     end
#     # end
#     idx = 1
#     for df in dfs
#         for kdx in 1:nrow(df)
#             for jdx in 1:Ncol
#                 res[jdx][idx] = df[kdx, jdx]
#             end
#             idx += 1
#         end
#         set_groups(res, get_groups(df))
#     end
#     res
# end
vcat(dfs::DataFrame...) = rbind(dfs...)

# DF row operations -- delete and append
# df[1] = nothing
# df[1:3] = nothing
# df3 = rbind(df1, df2...)
# rbind!(df1, df2...)


# split-apply-combine
# co(ap(myfun,
#    sp(df, ["region", "product"])))
# (|)(x, f::Function) = f(x)
# split(df, ["region", "product"]) | (apply(nrow)) | mean
# apply(f::function) = (x -> map(f, x))
# split(df, ["region", "product"]) | @@@)) | mean
# how do we add col names to the name space?
# transform(df, :(cat=dog*2, clean=proc(dirty)))
# summarise(df, :(cat=sum(dog), all=strcat(strs)))

function with(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Dict) = x
    replace_symbols(e::Expr, d::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
    function replace_symbols{K,V}(s::Symbol, d::Dict{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$(expr(:quote,s))])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$(string(s))])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    global _ex = ex
    f = @eval (_D) -> $ex
    f(d)
end

function within!(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_D[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$(expr(:quote,s))])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$(string(s))])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        $ex
        _D
    end
    f(d)
end



myref(d, key) = ref(d, key)
myref{K<:String,V}(d::Associative{K,V}, key) = ref(d, string(key))
myref(d::AbstractDataFrame, key::Symbol) = ref(d, string(key))

myhas(d, key) = has(d, key)
myhas{K<:String,V}(d::Associative{K,V}, key) = has(d, string(key))
myhas(d::AbstractDataFrame, key::Symbol) = has(d, string(key))

bestkey(d, key) = key
bestkey{K<:String,V}(d::Associative{K,V}, key) = string(key)
bestkey(d::AbstractDataFrame, key) = string(key)
bestkey(d::NamedArray, key) = string(key)

replace_syms(x) = x
replace_syms(s::Symbol) = :( myhas(d, $(quot(s))) ? myref(d, $(quot(s))) : $(esc(s)) )
replace_syms(e::Expr) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_syms(x), e.args), e.typ)

quot(value) = expr(:quote, value)  # Toivo special

function transform_helper(d, args...)
    exa = {:(local d = $(esc(d)))}
    for ex in args
        left = ex.args[1]
        right = replace_syms(ex.args[2])
        push(exa,
            :(d[bestkey(d, $(quot(left)))] = $(right)))
    end
    push(exa, :d)
    Expr(:block, exa, Any)
end

macro transform(df, args...)
    transform_helper(df, args...)
end

macro DataFrame(args...)
    :(DataFrame( @transform(NamedArray(), $(map(esc, args)...)) ))
end



    
function based_on(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_ND[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$(expr(:quote,s))])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$(string(s))])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        _ND = similar(_D)
        $ex
        _ND
    end
    f(d)
end

function within!(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame that allows replacing or adding columns.
    # Returns the transformed DataFrame.
    #   
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_DF[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = Dict(colnames(df), 1:ncol(df))
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> begin
        $ex
        _DF
    end
    f(df)
end

within(x, args...) = within!(copy(x), args...)

function based_on_f(df::AbstractDataFrame, ex::Expr)
    # Returns a function for use on an AbstractDataFrame
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in a new df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_col_dict[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = Dict(colnames(df), [1:ncol(df)])
    ex = replace_symbols(ex, cn_dict)
    @eval (_DF) -> begin
        _col_dict = NamedArray()
        $ex
        DataFrame(_col_dict)
    end
end
function based_on(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame.
    # Returns a new DataFrame.
    f = based_on_f(df, ex)
    f(df)
end

function with(df::AbstractDataFrame, ex::Expr)
    # By-column operation with the columns of a DataFrame.
    # Returns the result of evaluating ex.
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    replace_symbols(e::Expr, syms::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = Dict(colnames(df), [1:ncol(df)])
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> $ex
    f(df)
end

with(df::AbstractDataFrame, s::Symbol) = df[string(s)]

# add function curries to ease pipelining:
with(e::Expr) = x -> with(x, e)
within(e::Expr) = x -> within(x, e)
within!(e::Expr) = x -> within!(x, e)
based_on(e::Expr) = x -> based_on(x, e)

# allow pipelining straight to an expression using within!:
(|)(x::AbstractDataFrame, e::Expr) = within!(x, e)


##
## Reshaping
##

function stack(df::DataFrame, icols::Vector{Int})
    remainingcols = _setdiff([1:ncol(df)], icols)
    res = rbind([insert(df[[i, remainingcols]], 1, colnames(df)[i], "key") for i in icols]...)
    replace_names!(res, colnames(res)[2], "value")
    res 
end
stack(df::DataFrame, icols) = stack(df, [df.colindex[icols]])

function unstack(df::DataFrame, ikey::Int, ivalue::Int, irefkey::Int)
    keycol = PooledDataVec(df[ikey])
    valuecol = df[ivalue]
    # TODO make a version with a default refkeycol
    refkeycol = PooledDataVec(df[irefkey])
    remainingcols = _setdiff([1:ncol(df)], [ikey, ivalue])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length) 
    payload = DataFrame({DataVec([fill(valuecol[1],Nrow)], fill(true, Nrow))  for i in 1:Ncol}, map(string, keycol.pool))
    nowarning = true 
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i]) 
                println("Warning: duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert(payload, 1, refkeycol.pool, colnames(df)[irefkey])
end
unstack(df::DataFrame, ikey, ivalue, irefkey) =
    unstack(df, df.colindex[ikey], df.colindex[ivalue], df.colindex[irefkey])

##
## Join / merge
##

function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NA group in location 0

    left_sorter, where, left_count = groupsort_indexer(left, max_groups)
    right_sorter, where, right_count = groupsort_indexer(right, max_groups)

    # First pass, determine size of result set, do not use the NA group
    count = 0
    rcount = 0
    lcount = 0
    for i in 2 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc > 0 && lc > 0
            count += lc * rc
        elseif rc > 0
            rcount += rc
        else
            lcount += lc
        end
    end
    
    # group 0 is the NA group
    position = 0
    lposition = 0
    rposition = 0

    # exclude the NA group
    left_pos = left_count[1]
    right_pos = right_count[1]

    left_indexer = Array(Int, count)
    right_indexer = Array(Int, count)
    leftonly_indexer = Array(Int, lcount)
    rightonly_indexer = Array(Int, rcount)
    for i in 1 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc == 0
            for j in 1:lc
                leftonly_indexer[lposition + j] = left_pos + j
            end
            lposition += lc
        elseif lc == 0
            for j in 1:rc
                rightonly_indexer[rposition + j] = right_pos + j
            end
            rposition += rc
        else
            for j in 1:lc
                offset = position + (j-1) * rc
                for k in 1:rc
                    left_indexer[offset + k] = left_pos + j
                    right_indexer[offset + k] = right_pos + k
                end
            end
            position += lc * rc
        end
        left_pos += lc
        right_pos += rc
    end

    ## (left_sorter, left_indexer, leftonly_indexer,
    ##  right_sorter, right_indexer, rightonly_indexer)
    (left_sorter[left_indexer], left_sorter[leftonly_indexer],
     right_sorter[right_indexer], right_sorter[rightonly_indexer])
end

function merge(df1::AbstractDataFrame, df2::AbstractDataFrame, bycol, jointype)

    dv1, dv2 = PooledDataVecs(df1[bycol], df2[bycol])
    left_indexer, leftonly_indexer,
    right_indexer, rightonly_indexer =
        join_idx(dv1.refs, dv2.refs, length(dv1.pool))

    if jointype == "inner"
        return cbind(df1[left_indexer,:], without(df2, bycol)[right_indexer,:])
    elseif jointype == "left"
        left = df1[[left_indexer,leftonly_indexer],:]
        right = rbind(without(df2, bycol)[right_indexer,:],
                      nas(without(df2, bycol), length(leftonly_indexer)))
        return cbind(left, right)
    elseif jointype == "right"
        left = rbind(df1[left_indexer,:],
                     nas(df1, length(rightonly_indexer)))
        right = without(df2, bycol)[[right_indexer,rightonly_indexer],:]
        return cbind(left, right)
    elseif jointype == "outer"
        left = rbind(df1[[left_indexer,leftonly_indexer],:],
                     nas(df1, length(rightonly_indexer)))
        right = rbind(without(df2, bycol)[right_indexer,:],
                      nas(without(df2, bycol), length(leftonly_indexer)),
                      without(df2, bycol)[rightonly_indexer,:])
        return cbind(left, right)
    end
    # TODO add support for multiple columns
end

merge(df1::AbstractDataFrame, df2::AbstractDataFrame, bycol) = merge(df1, df2, bycol, "inner")

# TODO: Make this method work with multiple columns
#       Will need to fix PooledDataVecs for that
function merge(df1::AbstractDataFrame, df2::AbstractDataFrame)
    s1 = Set{ByteString}()
    for coln in colnames(df1)
        add(s1, coln)
    end
    s2 = Set{ByteString}()
    for coln in colnames(df2)
        add(s2, coln)
    end
    bycol = first(elements(intersect(s1, s2)))
    merge(df1, df2, bycol, "inner")
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

function array(d::AbstractDataFrame)
    # DataFrame -> Array{Any}
    if nrow(d) == 1  # collapse to one element
       [el[1] for el in d[1,:]]
    else
       [col for col in d]
    end
end

# DataFrame -> Array{promoted_type, 2}
# Note: this doesn't work yet for DataVecs. It might once promotion
# with Arrays is added (work needed).
# matrix(d::AbstractDataFrame) = reshape([d...],size(d))
function matrix(df::DataFrame)
    n, p = size(df)
    m = zeros(n, p)
    for i in 1:n
        for j in 1:p
            if isna(df[i, j])
                error("DataFrame's with missing entries cannot be converted")
            else
                m[i, j] = df[i, j]
            end
        end
    end
    return m
end

function duplicated(df::AbstractDataFrame)
    # Return a Vector{Bool} indicated whether the row is a duplicate
    # of a prior row.
    res = fill(false, nrow(df))
    di = Dict()
    for i in 1:nrow(df)
        if has(di, array(df[i,:]))
            res[i] = true
        else
            di[array(df[i,:])] = 1 
        end
    end
    res
end

function drop_duplicates!(df::AbstractDataFrame)
    df = df[!duplicated(df), :]
    return
end

# Unique rows of an AbstractDataFrame.        
unique(df::AbstractDataFrame) = df[!duplicated(df), :] 

function duplicatedkey(df::AbstractDataFrame)
    # Here's another (probably a lot faster) way to do `duplicated`
    # by grouping on all columns. It will fail if columns cannot be
    # made into PooledDataVec's.
    gd = groupby(df, colnames(df))
    idx = [1:length(gd.idx)][gd.idx][gd.starts]
    res = fill(true, nrow(df))
    res[idx] = false
    res
end

function isna(df::DataFrame)
    results = BitArray(size(df))
    for i in 1:nrow(df)
        for j in 1:ncol(df)
            results[i, j] = isna(df[i, j])
        end
    end
    return results
end

function isnan(df::DataFrame)
    p = ncol(df)
    res_columns = Array(Any, p)
    for j in 1:p
        res_columns[j] = isnan(df[j])
    end
    return DataFrame(res_columns, colnames(df))
end

function isfinite(df::DataFrame)
    p = ncol(df)
    res_columns = Array(Any, p)
    for j in 1:p
        res_columns[j] = isfinite(df[j])
    end
    return DataFrame(res_columns, colnames(df))
end

function sort_by(df::DataFrame, colname::String)
    return df[order(df[colname]), :]
end

# TODO: Use cor_pearson and cov_pearson for DataMatrix to do this
function cor_pearson(df::DataFrame)
    numeric_cols = find(map(t -> t <: Number, coltypes(df)))
    cor_pearson(matrix(df[:, numeric_cols]))
end
function cov_pearson(df::DataFrame)
    numeric_cols = find(map(t -> t <: Number, coltypes(df)))
    cov_pearson(matrix(df[:, numeric_cols]))
end

function clean_colnames!(df::DataFrame)
    new_names = map(n -> replace(n, r"\W", "_"), colnames(df))
    colnames!(df, new_names)
    return
end

function flipud(df::DataFrame)
    return df[reverse(1:nrow(df)), :]
end

function flipud!(df::DataFrame)
    df[1:nrow(df), :] = df[reverse(1:nrow(df)), :]
    return
end
