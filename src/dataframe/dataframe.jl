"""
An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

**Constructors**

```julia
DataFrame(columns::Vector, names::Vector{Symbol}; makeunique::Bool=false)
DataFrame(columns::Matrix, names::Vector{Symbol}; makeunique::Bool=false)
DataFrame(kwargs...)
DataFrame(pairs::Pair{Symbol}...; makeunique::Bool=false)
DataFrame() # an empty DataFrame
DataFrame(t::Type, nrows::Integer, ncols::Integer) # an empty DataFrame of arbitrary size
DataFrame(column_eltypes::Vector, names::Vector, nrows::Integer; makeunique::Bool=false)
DataFrame(column_eltypes::Vector, cnames::Vector, categorical::Vector, nrows::Integer;
          makeunique::Bool=false)
DataFrame(ds::AbstractDict)
DataFrame(table; makeunique::Bool=false)
```

**Arguments**

* `columns` : a Vector with each column as contents or a Matrix
* `names` : the column names
* `makeunique` : if `false` (the default), an error will be raised
  if duplicates in `names` are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).
* `kwargs` : the key gives the column names, and the value is the
  column contents
* `t` : elemental type of all columns
* `nrows`, `ncols` : number of rows and columns
* `column_eltypes` : elemental type of each column
* `categorical` : `Vector{Bool}` indicating which columns should be converted to
                  `CategoricalVector`
* `ds` : `AbstractDict` of columns
* `table`: any type that implements the
  [Tables.jl](https://github.com/JuliaData/Tables.jl) interface

Each column in `columns` should be the same length.

**Notes**

A `DataFrame` is a lightweight object. As long as columns are not
manipulated, creation of a `DataFrame` from existing AbstractVectors is
inexpensive. For example, indexing on columns is inexpensive, but
indexing by rows is expensive because copies are made of each column.

If a column is passed to a `DataFrame` constructor or is assigned as a whole
using `setindex!` then its reference is stored in the `DataFrame`. An exception
to this rule is assignment of an `AbstractRange` as a column, in which case the
range is collected to a `Vector`.

Because column types can vary, a `DataFrame` is not type stable. For
performance-critical code, do not index into a `DataFrame` inside of
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
mutable struct DataFrame <: AbstractDataFrame
    columns::Vector{AbstractVector}
    colindex::Index

    function DataFrame(columns::Union{Vector{Any}, Vector{AbstractVector}},
                       colindex::Index)
        if length(columns) == length(colindex) == 0
            return new(AbstractVector[], Index())
        elseif length(columns) != length(colindex)
            throw(DimensionMismatch("Number of columns ($(length(columns))) and number of" *
                                    " column names ($(length(colindex))) are not equal"))
        end
        lengths = [isa(col, AbstractArray) ? length(col) : 1 for col in columns]
        minlen, maxlen = extrema(lengths)
        if minlen == 0 && maxlen == 0
            return new(columns, colindex)
        elseif minlen != maxlen || minlen == maxlen == 1
            # recycle scalars
            for i in 1:length(columns)
                isa(columns[i], AbstractArray) && continue
                columns[i] = fill!(Tables.allocatecolumn(typeof(columns[i]), maxlen),
                                   columns[i])
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
            if isa(c, AbstractRange)
                columns[i] = collect(c)
            elseif !isa(c, AbstractVector)
                throw(DimensionMismatch("columns must be 1-dimensional"))
            end
        end
        new(convert(Vector{AbstractVector}, columns), colindex)
    end
end

function DataFrame(df::DataFrame)
    Base.depwarn("In the future DataFrame constructor called with a `DataFrame` argument will return a copy. " *
                 "Use `convert(DataFrame, df)` to avoid copying if `df` is a `DataFrame`.", :DataFrame)
    return df
end

function DataFrame(pairs::Pair{Symbol,<:Any}...; makeunique::Bool=false)::DataFrame
    colnames = [Symbol(k) for (k,v) in pairs]
    columns = Any[v for (k,v) in pairs]
    DataFrame(columns, Index(colnames, makeunique=makeunique))
end

function DataFrame(d::AbstractDict)
    colnames = keys(d)
    if isa(d, Dict)
        colnames = sort!(collect(keys(d)))
    else
        colnames = keys(d)
    end
    colindex = Index([Symbol(k) for k in colnames])
    columns = Any[d[c] for c in colnames]
    DataFrame(columns, colindex)
end

function DataFrame(; kwargs...)
    if isempty(kwargs)
        DataFrame([], Index())
    else
        DataFrame(pairs(kwargs)...)
    end
end

function DataFrame(columns::AbstractVector, cnames::AbstractVector{Symbol};
                   makeunique::Bool=false)::DataFrame
    if !all(col -> isa(col, AbstractVector), columns)
        throw(ArgumentError("columns argument must be a vector of AbstractVector objects"))
    end
    return DataFrame(convert(Vector{AbstractVector}, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique))
end

function DataFrame(columns::AbstractVector{<:AbstractVector},
                   cnames::AbstractVector{Symbol}=gennames(length(columns));
                   makeunique::Bool=false)::DataFrame
    return DataFrame(convert(Vector{AbstractVector}, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique))
end

DataFrame(columns::AbstractMatrix, cnames::AbstractVector{Symbol} = gennames(size(columns, 2));
          makeunique::Bool=false) =
    DataFrame(AbstractVector[columns[:, i] for i in 1:size(columns, 2)], cnames, makeunique=makeunique)

# Initialize an empty DataFrame with specific eltypes and names
function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   nrows::Integer; makeunique::Bool=false)::DataFrame where T<:Type
    columns = AbstractVector[elty >: Missing ?
                             fill!(Tables.allocatecolumn(elty, nrows), missing) :
                             Tables.allocatecolumn(elty, nrows)
                             for elty in column_eltypes]
    return DataFrame(columns, Index(convert(Vector{Symbol}, cnames), makeunique=makeunique))
end

# Initialize an empty DataFrame with specific eltypes and names
# and whether a CategoricalArray should be created
function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   categorical::Vector{Bool}, nrows::Integer;
                   makeunique::Bool=false)::DataFrame where T<:Type
    # upcast Vector{DataType} -> Vector{Type} which can hold CategoricalValues
    updated_types = convert(Vector{Type}, column_eltypes)
    if length(categorical) != length(column_eltypes)
        throw(DimensionMismatch("arguments column_eltypes and categorical must have the same length " *
                                "(got $(length(column_eltypes)) and $(length(categorical)))"))
    end
    for i in eachindex(categorical)
        categorical[i] || continue
        elty = CategoricalArrays.catvaluetype(Missings.T(updated_types[i]),
                                              CategoricalArrays.DefaultRefType)
        if updated_types[i] >: Missing
            updated_types[i] = Union{elty, Missing}
        else
            updated_types[i] = elty
        end
    end
    return DataFrame(updated_types, cnames, nrows, makeunique=makeunique)
end

# Initialize empty DataFrame objects of arbitrary size
function DataFrame(t::Type, nrows::Integer, ncols::Integer)
    return DataFrame(fill(t, ncols), nrows)
end

# Initialize an empty DataFrame with specific eltypes
function DataFrame(column_eltypes::AbstractVector{T}, nrows::Integer) where T<:Type
    return DataFrame(column_eltypes, gennames(length(column_eltypes)), nrows)
end

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(df::DataFrame) = getfield(df, :colindex)
columns(df::DataFrame) = getfield(df, :columns)

# note: these type assertions are required to pass tests
nrow(df::DataFrame) = ncol(df) > 0 ? length(columns(df)[1])::Int : 0
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
# Let getindex(columns(df)[j], row_inds) from AbstractVector() handle
#  the resolution of row indices

# TODO: change Real to Integer in this union after deprecation period
const ColumnIndex = Union{Real, Symbol}

# df[SingleColumnIndex] => AbstractDataVector
function Base.getindex(df::DataFrame, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return columns(df)[selected_column]
end

# df[MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, col_inds::AbstractVector)
    selected_columns = index(df)[col_inds]
    new_columns = columns(df)[selected_columns]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[:] => DataFrame
Base.getindex(df::DataFrame, col_inds::Colon) = copy(df)

# df[SingleRowIndex, SingleColumnIndex] => Scalar
function Base.getindex(df::DataFrame, row_ind::Real, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return columns(df)[selected_column][row_ind]
end

# df[SingleRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, row_ind::Bool, col_inds::AbstractVector)
    throw(ArgumentError("invalid row index: $row_ind of type Bool"))
end

# df[SingleRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, row_ind::Real, col_inds::AbstractVector)
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[[row_ind]] for dv in columns(df)[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector
function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return columns(df)[selected_column][row_inds]
end

# df[MultiRowIndex, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_inds::AbstractVector)
    selected_columns = index(df)[col_inds]
    new_columns = Any[dv[row_inds] for dv in columns(df)[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[:, SingleColumnIndex] => AbstractVector
# df[:, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, row_ind::Colon, col_inds)
    Base.depwarn("indexing with colon as row will create a copy in the future" *
                 " use df[col_inds] to get the columns without copying", :getindex)
    df[col_inds]
end

# df[SingleRowIndex, :] => DataFrame
Base.getindex(df::DataFrame, row_ind::Real, col_inds::Colon) = df[[row_ind], col_inds]

# df[MultiRowIndex, :] => DataFrame
function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_inds::Colon)
    new_columns = Any[dv[row_inds] for dv in columns(df)]
    return DataFrame(new_columns, copy(index(df)))
end

# df[:, :] => DataFrame
function Base.getindex(df::DataFrame, ::Colon, ::Colon)
    Base.depwarn("indexing with colon as row will create a copy of rows in the future", :getindex)
    copy(df)
end

##############################################################################
##
## setindex!()
##
##############################################################################

function nextcolname(df::DataFrame)
    col = Symbol(string("x", ncol(df) + 1))
    haskey(index(df), col) || return col
    i = 1
    while true
        col = Symbol(string("x", ncol(df) + 1, "_", i))
        haskey(index(df), col) || return col
        i += 1
    end
end

# Will automatically add a new column if needed
function insert_single_column!(df::DataFrame,
                               v::AbstractVector,
                               col_ind::ColumnIndex)

    if ncol(df) != 0 && nrow(df) != length(v)
        throw(ArgumentError("New columns must have the same length as old columns"))
    end
    dv = isa(v, AbstractRange) ? collect(v) : v
    if haskey(index(df), col_ind)
        j = index(df)[col_ind]
        columns(df)[j] = dv
    else
        if typeof(col_ind) <: Symbol
            push!(index(df), col_ind)
            push!(columns(df), dv)
        else
            if ncol(df) + 1 == Int(col_ind)
                push!(index(df), nextcolname(df))
                push!(columns(df), dv)
            else
                throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
            end
        end
    end
    return dv
end

function insert_single_entry!(df::DataFrame, v::Any, row_ind::Real, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        columns(df)[index(df)[col_ind]][row_ind] = v
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
        columns(df)[index(df)[col_ind]][row_inds] .= v
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
    setindex!(df, new_df, findall(col_inds))
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
    setindex!(df, v, findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        col_inds::AbstractVector{<:ColumnIndex})
    for col_ind in col_inds
        df[col_ind] = copy(v)
    end
    return df
end

# df[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame,
                        val::Any,
                        col_inds::AbstractVector{Bool})
    setindex!(df, val, findall(col_inds))
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
    setindex!(df, v, row_ind, findall(col_inds))
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
    setindex!(df, new_df, row_ind, findall(col_inds))
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
    setindex!(df, v, findall(row_inds), col_ind)
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
    setindex!(df, v, findall(row_inds), col_ind)
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
    setindex!(df, new_df, findall(row_inds), findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, new_df, findall(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, new_df, row_inds, findall(col_inds))
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
    setindex!(df, v, findall(row_inds), findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, findall(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, findall(col_inds))
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
    setindex!(df, v, findall(row_inds), findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, findall(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Real},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, findall(col_inds))
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
    setfield!(df, :columns, copy(columns(new_df)))
    setfield!(df, :colindex, copy(index(new_df)))
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
Base.setindex!(df::DataFrame, x::Nothing, col_ind::Int) = delete!(df, col_ind)

##############################################################################
##
## Mutating AbstractDict methods
##
##############################################################################

Base.empty!(df::DataFrame) = (empty!(columns(df)); empty!(index(df)); df)

"""
Insert a column into a data frame in place.


```julia
insert!(df::DataFrame, col_ind::Int, item::AbstractVector, name::Symbol;
        makeunique::Bool=false)
```

### Arguments

* `df` : the DataFrame to which we want to add a column

* `col_ind` : a position at which we want to insert a column

* `item` : a column to be inserted into `df`

* `name` : column name

* `makeunique` : Defines what to do if `name` already exists in `df`;
  if it is `false` an error will be thrown; if it is `true` a new unique name will
  be generated by adding a suffix

### Result

* `::DataFrame` : a `DataFrame` with added column.

### Examples

```jldoctest
julia> d = DataFrame(a=1:3)
3×1 DataFrame
│ Row │ a     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> insert!(d, 1, 'a':'c', :b)
3×2 DataFrame
│ Row │ b    │ a     │
│     │ Char │ Int64 │
├─────┼──────┼───────┤
│ 1   │ 'a'  │ 1     │
│ 2   │ 'b'  │ 2     │
│ 3   │ 'c'  │ 3     │
```

"""
function Base.insert!(df::DataFrame, col_ind::Int, item::AbstractVector, name::Symbol;
                      makeunique::Bool=false)
    0 < col_ind <= ncol(df) + 1 || throw(BoundsError())
    size(df, 1) == length(item) || size(df, 1) == 0 || error("number of rows does not match")

    if haskey(df, name)
        if makeunique
            k = 1
            while true
                # we only make sure that new column name is unique
                # if df originally had duplicates in names we do not fix it
                nn = Symbol("$(name)_$k")
                if !haskey(df, nn)
                    name = nn
                    break
                end
                k += 1
            end
        else
            # TODO: remove depwarn and call and uncomment ArgumentError below
            Base.depwarn("Inserting duplicate column name is deprecated, use makeunique=true.", :insert!)
            insert!(df, col_ind, item, name; makeunique=true) # temporary fix to avoid duplicates
            # msg = """Duplicate variable name $(name).
            #      Pass makeunique=true to make it unique using a suffix automatically."""
            # throw(ArgumentError(msg))
        end
    end
    insert!(index(df), col_ind, name)
    insert!(columns(df), col_ind, item)
    df
end

function Base.insert!(df::DataFrame, col_ind::Int, item, name::Symbol; makeunique::Bool=false)
    insert!(df, col_ind, upgrade_scalar(df, item), name, makeunique=makeunique)
end

"""
Merge data frames.


```julia
merge!(df::DataFrame, others::AbstractDataFrame...)
```

For every column `c` with name `n` in `others` sequentially perform `df[n] = c`.
In particular, if there are duplicate column names present in `df` and `others`
the last encountered column will be retained.
This behavior is identical with how `merge!` works for any `AbstractDict` type.
Use `join` if you want to join two `DataFrame`s.

**Arguments**

* `df` : the DataFrame to merge into
* `others` : `AbstractDataFrame`s to be merged into `df`

**Result**

* `::DataFrame` : the updated result. Columns with duplicate names are overwritten.

**Examples**

```julia
df = DataFrame(id = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df2 = DataFrame(id = 11:20, z = rand(10))
merge!(df, df2)  # column z is added, column id is overwritten
```
"""
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
            splice!(columns(df), ind)
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
        columns(df)[i] = deleteat!(columns(df)[i], ind)
    end
    df
end

function deleterows!(df::DataFrame, ind::AbstractVector{Int})
    ind2 = sort(ind)
    n = size(df, 1)

    idf = 1
    iind = 1
    ikeep = 1
    keep = Vector{Int}(undef, n-length(ind2))
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
        columns(df)[i] = columns(df)[i][keep]
    end
    df
end

##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments, only a vector or a data frame is allowed
function hcat!(df1::DataFrame, df2::AbstractDataFrame; makeunique::Bool=false)
    u = add_names(index(df1), index(df2), makeunique=makeunique)
    for i in 1:length(u)
        df1[u[i]] = df2[i]
    end
    return df1
end

# definition required to avoid hcat! ambiguity
function hcat!(df1::DataFrame, df2::DataFrame; makeunique::Bool=false)
    invoke(hcat!, Tuple{DataFrame, AbstractDataFrame}, df1, df2, makeunique=makeunique)
end

hcat!(df::DataFrame, x::AbstractVector; makeunique::Bool=false) =
    hcat!(df, DataFrame(AbstractVector[x]), makeunique=makeunique)
hcat!(x::AbstractVector, df::DataFrame; makeunique::Bool=false) =
    hcat!(DataFrame(AbstractVector[x]), df, makeunique=makeunique)
function hcat!(x, df::DataFrame; makeunique::Bool=false)
    throw(ArgumentError("x must be AbstractVector or AbstractDataFrame"))
end
function hcat!(df::DataFrame, x; makeunique::Bool=false)
    throw(ArgumentError("x must be AbstractVector or AbstractDataFrame"))
end

# hcat! for 1-n arguments
hcat!(df::DataFrame; makeunique::Bool=false) = df
hcat!(a::DataFrame, b, c...; makeunique::Bool=false) =
    hcat!(hcat!(a, b, makeunique=makeunique), c..., makeunique=makeunique)

# hcat
Base.hcat(df::DataFrame, x; makeunique::Bool=false) =
    hcat!(copy(df), x, makeunique=makeunique)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame; makeunique::Bool=false) =
    hcat!(copy(df1), df2, makeunique=makeunique)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame, dfn::AbstractDataFrame...;
          makeunique::Bool=false) =
    hcat!(hcat(df1, df2, makeunique=makeunique), dfn..., makeunique=makeunique)

##############################################################################
##
## Missing values support
##
##############################################################################
"""
    allowmissing!(df::DataFrame)

Convert all columns of a `df` from element type `T` to
`Union{T, Missing}` to support missing values.

    allowmissing!(df::DataFrame, col::Union{Integer, Symbol})

Convert a single column of a `df` from element type `T` to
`Union{T, Missing}` to support missing values.

    allowmissing!(df::DataFrame, cols::AbstractVector{<:Union{Integer, Symbol}})

Convert multiple columns of a `df` from element type `T` to
`Union{T, Missing}` to support missing values.
"""
function allowmissing! end

function allowmissing!(df::DataFrame, col::ColumnIndex)
    df[col] = allowmissing(df[col])
    df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{<: ColumnIndex}=1:size(df, 2))
    for col in cols
        allowmissing!(df, col)
    end
    df
end

"""
    disallowmissing!(df::DataFrame)

Convert all columns of a `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

    disallowmissing!(df::DataFrame, col::Union{Integer, Symbol})

Convert a single column of a `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

    disallowmissing!(df::DataFrame, cols::AbstractVector{<:Union{Integer, Symbol}})

Convert multiple columns of a `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.
"""
function disallowmissing! end

function disallowmissing!(df::DataFrame, col::ColumnIndex)
    df[col] = disallowmissing(df[col])
    df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{<: ColumnIndex}=1:size(df, 2))
    for col in cols
        disallowmissing!(df, col)
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
    nrows, ncols = size(df1)
    try
        for j in 1:ncols
            append!(df1[j], df2[j])
        end
    catch err
        # Undo changes in case of error
        for j in 1:ncols
            resize!(df1[j], nrows)
        end
        rethrow(err)
    end
    return df1
end

Base.convert(::Type{DataFrame}, A::AbstractMatrix) = DataFrame(A)

Base.convert(::Type{DataFrame}, d::AbstractDict) = DataFrame(d)


##############################################################################
##
## push! a row onto a DataFrame
##
##############################################################################

function Base.push!(df::DataFrame, dict::AbstractDict)
    i = 1
    for nm in _names(df)
        try
            val = get(dict, nm) do
                v = dict[string(nm)]
                Base.depwarn("push!(::DataFrame, ::AbstractDict) with " *
                             "AbstractDict keys other than Symbol is deprecated",
                             :push!)
                v
            end
            # after deprecation replace above call by
            # val = dict[nm]
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
    df
end

# array and tuple like collections
function Base.push!(df::DataFrame, iterable::Any)
    if length(iterable) != size(df, 2)
        msg = "Length of iterable does not match DataFrame column count."
        throw(ArgumentError(msg))
    end
    i = 1
    for t in iterable
        try
            push!(columns(df)[i], t)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(columns(df)[j])
            end
            msg = "Error adding $t to column :$(_names(df)[i]). Possible type mis-match."
            throw(ArgumentError(msg))
        end
        i += 1
    end
    df
end

##############################################################################
##
## Reorder columns
##
##############################################################################

"""
    permutecols!(df::DataFrame, p::AbstractVector)

Permute the columns of `df` in-place, according to permutation `p`. Elements of `p` may be
either column indices (`Int`) or names (`Symbol`), but cannot be a combination of both. All
columns must be listed.

### Examples

```julia
julia> df = DataFrame(a=1:5, b=2:6, c=3:7)
5×3 DataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 3     │
│ 2   │ 2     │ 3     │ 4     │
│ 3   │ 3     │ 4     │ 5     │
│ 4   │ 4     │ 5     │ 6     │
│ 5   │ 5     │ 6     │ 7     │

julia> permutecols!(df, [2, 1, 3]);

julia> df
5×3 DataFrame
│ Row │ b     │ a     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 2     │ 1     │ 3     │
│ 2   │ 3     │ 2     │ 4     │
│ 3   │ 4     │ 3     │ 5     │
│ 4   │ 5     │ 4     │ 6     │
│ 5   │ 6     │ 5     │ 7     │

julia> permutecols!(df, [:c, :a, :b]);

julia> df
5×3 DataFrame
│ Row │ c     │ a     │ b     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 3     │ 1     │ 2     │
│ 2   │ 4     │ 2     │ 3     │
│ 3   │ 5     │ 3     │ 4     │
│ 4   │ 6     │ 4     │ 5     │
│ 5   │ 7     │ 5     │ 6     │
```
"""
function permutecols!(df::DataFrame, p::AbstractVector)
    if !(length(p) == size(df, 2) && isperm(p))
        throw(ArgumentError("$p is not a valid column permutation for this DataFrame"))
    end
    permute!(columns(df), p)
    setfield!(df, :colindex, Index(names(df)[p]))
    df
end

function permutecols!(df::DataFrame, p::AbstractVector{Symbol})
    lu = index(df).lookup
    permutecols!(df, [lu[x] for x in p])
end
