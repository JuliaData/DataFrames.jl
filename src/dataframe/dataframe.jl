"""
    DataFrame <: AbstractDataFrame

An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

**Constructors**

```julia
DataFrame(columns::Vector, names::Vector{Symbol};
          makeunique::Bool=false, copycols::Bool=true)
DataFrame(columns::NTuple{N,AbstractVector}, names::NTuple{N,Symbol};
          makeunique::Bool=false, copycols::Bool=true)
DataFrame(columns::Matrix, names::Vector{Symbol}; makeunique::Bool=false)
DataFrame(kwargs...)
DataFrame(pairs::NTuple{N, Pair{Symbol, AbstractVector}}; copycols::Bool=true)
DataFrame() # an empty DataFrame
DataFrame(column_eltypes::Vector, names::AbstractVector{Symbol}, nrows::Integer=0;
          makeunique::Bool=false)
DataFrame(ds::AbstractDict; copycols::Bool=true)
DataFrame(table; makeunique::Bool=false, copycols::Bool=true)
DataFrame(::Union{DataFrame, SubDataFrame}; copycols::Bool=true)
DataFrame(::GroupedDataFrame)
```

**Arguments**

* `columns` : a Vector with each column as contents or a Matrix
* `names` : the column names
* `makeunique` : if `false` (the default), an error will be raised
  if duplicates in `names` are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).
* `kwargs` : the key gives the column names, and the value is the
  column contents; note that the `copycols` keyword argument indicates if
  if vectors passed as columns should be copied so it is not possible to create
  a column whose name is `:copycols` using this constructor
* `t` : elemental type of all columns
* `nrows`, `ncols` : number of rows and columns
* `column_eltypes` : element type of each column
* `categorical` : a vector of `Bool` indicating which columns should be converted to
                  `CategoricalVector`
* `ds` : `AbstractDict` of columns
* `table` : any type that implements the
  [Tables.jl](https://github.com/JuliaData/Tables.jl) interface; in particular
  a tuple or vector of `Pair{Symbol, <:AbstractVector}}` objects is a table.
* `copycols` : whether vectors passed as columns should be copied; note that
  `DataFrame(kwargs...)` does not support this keyword argument and always copies columns.

All columns in `columns` should have the same length.

**Notes**

The `DataFrame` constructor by default copies all columns vectors passed to it.
Pass `copycols=false` to reuse vectors without copying them

If a column is passed to a `DataFrame` constructor or is assigned as a whole
using `setindex!` then its reference is stored in the `DataFrame`. An exception
to this rule is assignment of an `AbstractRange` as a column, in which case the
range is collected to a `Vector`.

Because column types can vary, a `DataFrame` is not type stable. For
performance-critical code, do not index into a `DataFrame` inside of loops.

**Examples**

```julia
df = DataFrame()
v = ["x","y","z"][rand(1:3, 10)]
df1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])
df2 = DataFrame(A = 1:10, B = v, C = rand(10))
summary(df1)
describe(df2)
first(df1, 10)
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
[df1 df2]  # hcat
size(df1)
```

"""
struct DataFrame <: AbstractDataFrame
    columns::Vector{AbstractVector}
    colindex::Index

    function DataFrame(columns::Union{Vector{Any}, Vector{AbstractVector}},
                       colindex::Index; copycols::Bool=true)
        if length(columns) == length(colindex) == 0
            return new(AbstractVector[], Index())
        elseif length(columns) != length(colindex)
            throw(DimensionMismatch("Number of columns ($(length(columns))) and number of" *
                                    " column names ($(length(colindex))) are not equal"))
        end
        lengths = [isa(col, AbstractArray) ? length(col) : 1 for col in columns]
        minlen, maxlen = extrema(lengths)
        if minlen != maxlen || minlen == maxlen == 1
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
            elseif copycols
                columns[i] = copy(c)
            end
        end
        new(convert(Vector{AbstractVector}, columns), colindex)
    end
end

DataFrame(df::DataFrame; copycols::Bool=true) = copy(df, copycols=copycols)

function DataFrame(pairs::Pair{Symbol,<:Any}...; makeunique::Bool=false,
                   copycols::Bool=true)::DataFrame
    colnames = [Symbol(k) for (k,v) in pairs]
    columns = Any[v for (k,v) in pairs]
    DataFrame(columns, Index(colnames, makeunique=makeunique), copycols=copycols)
end

function DataFrame(d::AbstractDict; copycols::Bool=true)
    colnames = keys(d)
    if isa(d, Dict)
        colnames = sort!(collect(keys(d)))
    else
        colnames = keys(d)
    end
    colindex = Index([Symbol(k) for k in colnames])
    columns = Any[d[c] for c in colnames]
    DataFrame(columns, colindex, copycols=copycols)
end

function DataFrame(; kwargs...)
    if isempty(kwargs)
        DataFrame([], Index())
    else
        cnames = Symbol[]
        columns = Any[]
        copycols = true
        for (kw, val) in kwargs
            if kw == :copycols
                if val isa Bool
                    copycols = val
                else
                    throw(ArgumentError("the `copycols` keyword argument must be Boolean"))
                end
            else
                push!(cnames, kw)
                push!(columns, val)
            end
        end
        DataFrame(columns, Index(cnames), copycols=copycols)
    end
end

function DataFrame(columns::AbstractVector, cnames::AbstractVector{Symbol};
                   makeunique::Bool=false, copycols::Bool=true)::DataFrame
    if !all(col -> isa(col, AbstractVector), columns)
        throw(ArgumentError("columns argument must be a vector of AbstractVector objects"))
    end
    return DataFrame(convert(Vector{AbstractVector}, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
                     copycols=copycols)
end

function DataFrame(columns::AbstractVector{<:AbstractVector},
                   cnames::AbstractVector{Symbol}=gennames(length(columns));
                   makeunique::Bool=false, copycols::Bool=true)::DataFrame
    return DataFrame(convert(Vector{AbstractVector}, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
                     copycols=copycols)
end

DataFrame(columns::NTuple{N, AbstractVector}, cnames::NTuple{N, Symbol};
          makeunique::Bool=false, copycols::Bool=true) where {N} =
    DataFrame(collect(AbstractVector, columns), collect(Symbol, cnames),
              makeunique=makeunique, copycols=copycols)

DataFrame(columns::NTuple{N, AbstractVector}; copycols::Bool=true) where {N} =
    DataFrame(collect(AbstractVector, columns), gennames(length(columns)),
              copycols=copycols)

DataFrame(columns::AbstractMatrix, cnames::AbstractVector{Symbol} = gennames(size(columns, 2));
          makeunique::Bool=false) =
    DataFrame(AbstractVector[columns[:, i] for i in 1:size(columns, 2)], cnames,
              makeunique=makeunique, copycols=false)

function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   nrows::Integer=0; makeunique::Bool=false)::DataFrame where T<:Type
    columns = AbstractVector[elty >: Missing ?
                             fill!(Tables.allocatecolumn(elty, nrows), missing) :
                             Tables.allocatecolumn(elty, nrows)
                             for elty in column_eltypes]
    return DataFrame(columns, Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
                     copycols=false)
end

"""
    DataFrame!(args...; kwargs...)

Equivalent to `DataFrame(args...; copycols=false, kwargs...)`.

If `kwargs` contains the `copycols` keyword argument an error is thrown.

### Examples

```jldoctest
julia> df1 = DataFrame(a=1:3)
3×1 DataFrame
│ Row │ a     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> df2 = DataFrame!(df1)

julia> df1.a === df2.a
true
"""
function DataFrame!(args...; kwargs...)
    if :copycols in keys(kwargs)
        throw(ArgumentError("`copycols` keyword argument is not allowed"))
    end
    DataFrame(args...; copycols=false, kwargs...)
end

DataFrame!(columns::AbstractMatrix,
           cnames::AbstractVector{Symbol} = gennames(size(columns, 2));
           makeunique::Bool=false) =
    throw(ArgumentError("It is not possible to construct a `DataFrame` from " *
                        "`$(typeof(columns))` without allocating new columns: " *
                        "use `DataFrame(...)` instead"))


DataFrame!(column_eltypes::AbstractVector{<:Type}, cnames::AbstractVector{Symbol},
           nrows::Integer=0; makeunique::Bool=false)::DataFrame =
    throw(ArgumentError("It is not possible to construct an uninitialized `DataFrame`" *
                        "without allocating new columns: use `DataFrame(...)` instead"))

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(df::DataFrame) = getfield(df, :colindex)
_columns(df::DataFrame) = getfield(df, :columns)

# note: these type assertions are required to pass tests
nrow(df::DataFrame) = ncol(df) > 0 ? length(_columns(df)[1])::Int : 0
ncol(df::DataFrame) = length(index(df))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# df[SingleColumnIndex] => AbstractVector, the same vector
@inline function Base.getindex(df::DataFrame, col_ind::Union{Signed, Unsigned})
    cols = _columns(df)
    @boundscheck if !checkindex(Bool, axes(cols, 1), col_ind)
        throw(BoundsError("attempt to access a data frame with $(ncol(df)) " *
                          "columns at index $col_ind"))
    end
    @inbounds cols[col_ind]
end

function Base.getindex(df::DataFrame, col_ind::Symbol)
    selected_column = index(df)[col_ind]
    return _columns(df)[selected_column]
end

# df[MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, col_inds::Union{AbstractVector, Regex, Not})
    selected_columns = index(df)[col_inds]
    new_columns = _columns(df)[selected_columns]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]))
end

# df[:] => DataFrame
Base.getindex(df::DataFrame, col_inds::Colon) = copy(df)

# df[SingleRowIndex, SingleColumnIndex] => Scalar
@inline function Base.getindex(df::DataFrame, row_ind::Integer,
                               col_ind::Union{Signed, Unsigned})
    cols = _columns(df)
    @boundscheck begin
        if !checkindex(Bool, axes(cols, 1), col_ind)
            throw(BoundsError("attempt to access a data frame with $(ncol(df)) " *
                              "columns at index $row_ind"))
        end
        if !checkindex(Bool, axes(df, 1), row_ind)
            throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                              "rows at index $row_ind"))
        end
    end

    @inbounds cols[col_ind][row_ind]
end

@inline function Base.getindex(df::DataFrame, row_ind::Integer, col_ind::Symbol)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_ind)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row_ind"))
    end
    @inbounds _columns(df)[selected_column][row_ind]
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector, copy
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row_inds"))
    end
    @inbounds return _columns(df)[selected_column][row_inds]
end

@inline Base.getindex(df::DataFrame, row_inds::Not, col_ind::ColumnIndex) =
    df[axes(df, 1)[row_inds], col_ind]

# df[MultiRowIndex, MultiColumnIndex] => DataFrame
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T},
                               col_inds::Union{AbstractVector, Regex, Not}) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row_inds"))
    end
    selected_columns = index(df)[col_inds]
    # Computing integer indices once for all columns is faster
    selected_rows = T === Bool ? findall(row_inds) : row_inds
    new_columns = AbstractVector[dv[selected_rows] for dv in _columns(df)[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]), copycols=false)
end

@inline Base.getindex(df::DataFrame, row_inds::Not,
                      col_inds::Union{AbstractVector, Regex, Not}) =
    df[axes(df, 1)[row_inds], col_inds]

# df[:, SingleColumnIndex] => AbstractVector
function Base.getindex(df::DataFrame, row_inds::Colon, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    copy(_columns(df)[selected_column])
end

# df[:, MultiColumnIndex] => DataFrame
function Base.getindex(df::DataFrame, row_ind::Colon, col_inds::Union{AbstractVector, Regex, Not})
    selected_columns = index(df)[col_inds]
    new_columns = AbstractVector[copy(dv) for dv in _columns(df)[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]), copycols=false)
end

# df[MultiRowIndex, :] => DataFrame
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T}, ::Colon) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError("attempt to access a data frame with $(nrow(df)) " *
                          "rows at index $row_inds"))
    end
    # Computing integer indices once for all columns is faster
    selected_rows = T === Bool ? findall(row_inds) : row_inds
    new_columns = AbstractVector[dv[selected_rows] for dv in _columns(df)]
    return DataFrame(new_columns, copy(index(df)), copycols=false)
end

@inline Base.getindex(df::DataFrame, row_inds::Not, ::Colon) =
    df[axes(df, 1)[row_inds], :]

# df[:, :] => DataFrame
function Base.getindex(df::DataFrame, ::Colon, ::Colon)
    new_columns = AbstractVector[copy(dv) for dv in _columns(df)]
    return DataFrame(new_columns, Index(_names(df)), copycols=false)
end

##############################################################################
##
## setindex!()
##
##############################################################################

function nextcolname(df::DataFrame)
    col = Symbol(string("x", ncol(df) + 1))
    hasproperty(df, col) || return col
    i = 1
    while true
        col = Symbol(string("x", ncol(df) + 1, "_", i))
        hasproperty(df, col) || return col
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
        _columns(df)[j] = dv
    else
        if col_ind isa Symbol
            push!(index(df), col_ind)
            push!(_columns(df), dv)
        else
            if ncol(df) + 1 == Int(col_ind)
                push!(index(df), nextcolname(df))
                push!(_columns(df), dv)
            else
                throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
            end
        end
    end
    return dv
end

function insert_single_entry!(df::DataFrame, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        _columns(df)[index(df)[col_ind]][row_ind] = v
        return v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
    end
end

function insert_multiple_entries!(df::DataFrame,
                                  v::Any,
                                  row_inds::AbstractVector{<:Integer},
                                  col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        _columns(df)[index(df)[col_ind]][row_inds] .= v
        return v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
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

# df[Regex] = value
Base.setindex!(df::DataFrame, v::Any, col_inds::Regex) =
    setindex!(df, v, index(df)[col_inds])

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
        insert_multiple_entries!(df, new_df[j], row_inds, col_inds[j])
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

# df[rows, Regex] = value
Base.setindex!(df::DataFrame, v::Any, row_inds, col_inds::Regex) =
    setindex!(df, v, row_inds, index(df)[col_inds])

# df[:] = DataFrame, df[:, :] = DataFrame
function Base.setindex!(df::DataFrame,
                        new_df::DataFrame,
                        row_inds::Colon,
                        col_inds::Colon=Colon())
    setfield!(df, :columns, copy(_columns(new_df)))
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

##############################################################################
##
## Mutating methods
##
##############################################################################

"""
Insert a column into a data frame in place.


```julia
insertcols!(df::DataFrame, ind::Int; name=col,
            makeunique::Bool=false)
insertcols!(df::DataFrame, ind::Int, (:name => col)::Pair{Symbol,<:AbstractVector};
            makeunique::Bool=false)
```

### Arguments

* `df` : the DataFrame to which we want to add a column

* `ind` : a position at which we want to insert a column

* `name` : the name of the new column

* `col` : an `AbstractVector` giving the contents of the new column

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

julia> insertcols!(d, 1, b=['a', 'b', 'c'])
3×2 DataFrame
│ Row │ b    │ a     │
│     │ Char │ Int64 │
├─────┼──────┼───────┤
│ 1   │ 'a'  │ 1     │
│ 2   │ 'b'  │ 2     │
│ 3   │ 'c'  │ 3     │

julia> insertcols!(d, 1, :c => [2, 3, 4])
3×3 DataFrame
│ Row │ c     │ b    │ a     │
│     │ Int64 │ Char │ Int64 │
├─────┼───────┼──────┼───────┤
│ 1   │ 2     │ 'a'  │ 1     │
│ 2   │ 3     │ 'b'  │ 2     │
│ 3   │ 4     │ 'c'  │ 3     │
```

"""
function insertcols!(df::DataFrame, col_ind::Int, name_col::Pair{Symbol, <:AbstractVector};
                     makeunique::Bool=false)
    name, item = name_col
    0 < col_ind <= ncol(df) + 1 || throw(BoundsError())
    size(df, 1) == length(item) || size(df, 2) == 0 || error("number of rows does not match")

    if hasproperty(df, name)
        if makeunique
            k = 1
            while true
                # we only make sure that new column name is unique
                # if df originally had duplicates in names we do not fix it
                nn = Symbol("$(name)_$k")
                if !hasproperty(df, nn)
                    name = nn
                    break
                end
                k += 1
            end
        else
            msg = "Duplicate variable name $name. Pass makeunique=true" *
                  " to make it unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end
    insert!(index(df), col_ind, name)
    insert!(_columns(df), col_ind, item)
    df
end

insertcols!(df::DataFrame, col_ind::Int, name_col::Pair{Symbol}; makeunique::Bool=false) =
    insertcols!(df, col_ind, name_col[1] => upgrade_scalar(df, name_col[2]), makeunique=makeunique)

function insertcols!(df::DataFrame, col_ind::Int; makeunique::Bool=false, name_col...)
    length(name_col) == 1 || throw(ArgumentError("one and only one column must be provided"))
    insertcols!(df, col_ind, makeunique=makeunique, keys(name_col)[1] => name_col[1])
end


"""
    copy(df::DataFrame; copycols::Bool=true)

Copy data frame `df`.
If `copycols=true` (the default), return a new  `DataFrame` holding
copies of column vectors in `df`.
If `copycols=false`, return a new `DataFrame` sharing column vectors with `df`.
"""
function Base.copy(df::DataFrame; copycols::Bool=true)
    if copycols
        df[:, :]
    else
        DataFrame(eachcol(df), names(df), copycols=false)
    end
end

"""
    deleterows!(df::DataFrame, inds)

Delete rows specified by `inds` from a `DataFrame` `df` in place and return it.

Internally `deleteat!` is called for all columns so `inds` must
be: a vector of sorted and unique integers, a boolean vector or an integer.

### Examples

```jldoctest
julia> d = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> deleterows!(d, 2)
2×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 3     │ 6     │
```

"""
function deleterows!(df::DataFrame, inds)
    if !isempty(inds) && size(df, 2) == 0
        throw(BoundsError())
    end
    # we require ind to be stored and unique like in Base
    foreach(col -> deleteat!(col, inds), _columns(df))
    df
end

function deleterows!(df::DataFrame, inds::AbstractVector{Bool})
    if length(inds) != size(df, 1)
        throw(BoundsError())
    end
    drop = findall(inds)
    foreach(col -> deleteat!(col, drop), _columns(df))
    df
end

"""
    select!(df::DataFrame, inds)

Mutate `df` in place to retain only columns specified by `inds` and return it.

Argument `inds` can be any index that is allowed for column indexing of
a `DataFrame` provided that the columns requested to be removed are unique.

### Examples

```jldoctest
julia> d = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select!(d, 2)
3×1 DataFrame
│ Row │ b     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 4     │
│ 2   │ 5     │
│ 3   │ 6     │
```

"""
function select!(df::DataFrame, inds::AbstractVector{Int})
    if isempty(inds)
        empty!(_columns(df))
        empty!(index(df))
        return df
    end
    indmin, indmax = extrema(inds)
    if indmin < 1
        throw(ArgumentError("indices must be positive"))
    end
    if indmax > ncol(df)
        throw(ArgumentError("indices must not be greater than number of columns"))
    end
    if !allunique(inds)
        throw(ArgumentError("indices must not contain duplicates"))
    end

    targetnames = _names(df)[inds]
    for i in setdiff(ncol(df):-1:1, inds)
        splice!(_columns(df), i)
        delete!(index(df), i)
    end
    permutecols!(df, targetnames)
end

select!(df::DataFrame, c::Int) = select!(df, [c])
select!(df::DataFrame, c::Any) = select!(df, index(df)[c])

"""
    select(df::AbstractDataFrame, inds, copycols::Bool=true)

Create a new data frame that contains columns from `df`
specified by `inds` and return it.

Argument `inds` can be any index that is allowed for column indexing.

If `df` is a `DataFrame` return a new `DataFrame` that contains columns from `df`
specified by `inds`.
If `copycols=true` (the default), then returned `DataFrame` holds
copies of column vectors in `df`.
If `copycols=false`, then returned `DataFrame` shares column vectors with `df`.

If `df` is a `SubDataFrame` then a `SubDataFrame` is returned if `copycols=false`
and a `DataFrame` with freshly allocated columns otherwise.

### Examples

```jldoctest
julia> d = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select(d, :b)
3×1 DataFrame
│ Row │ b     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 4     │
│ 2   │ 5     │
│ 3   │ 6     │
```

"""
select(df::DataFrame, inds::AbstractVector{Int}; copycols::Bool=true) =
    DataFrame(_columns(df)[inds], Index(_names(df)[inds]),
              copycols=copycols)

select(df::DataFrame, c::Int; copycols::Bool=true) =
    select(df, [c], copycols=copycols)
select(df::DataFrame, c::Any; copycols::Bool=true) =
    select(df, index(df)[c], copycols=copycols)

##############################################################################
##
## Hcat specialization
##
##############################################################################

# hcat! for 2 arguments, only a vector or a data frame is allowed
function hcat!(df1::DataFrame, df2::AbstractDataFrame;
               makeunique::Bool=false, copycols::Bool=true)
    u = add_names(index(df1), index(df2), makeunique=makeunique)
    for i in 1:length(u)
        df1[u[i]] = copycols ? copy(df2[i]) : df2[i]
    end
    return df1
end

# definition required to avoid hcat! ambiguity
function hcat!(df1::DataFrame, df2::DataFrame;
               makeunique::Bool=false, copycols::Bool=true)
    invoke(hcat!, Tuple{DataFrame, AbstractDataFrame}, df1, df2,
           makeunique=makeunique, copycols=copycols)::DataFrame
end

hcat!(df::DataFrame, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(df, DataFrame(AbstractVector[x], copycols=copycols),
          makeunique=makeunique, copycols=copycols)
hcat!(x::AbstractVector, df::DataFrame; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(AbstractVector[x], copycols=copycols), df,
          makeunique=makeunique, copycols=copycols)
hcat!(x, df::DataFrame; makeunique::Bool=false, copycols::Bool=true) =
    throw(ArgumentError("x must be AbstractVector or AbstractDataFrame"))
hcat!(df::DataFrame, x; makeunique::Bool=false, copycols::Bool=true) =
    throw(ArgumentError("x must be AbstractVector or AbstractDataFrame"))

# hcat! for 1-n arguments
hcat!(df::DataFrame; makeunique::Bool=false, copycols::Bool=true) = df
hcat!(a::DataFrame, b, c...; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat!(a, b, makeunique=makeunique, copycols=copycols),
          c..., makeunique=makeunique, copycols=copycols)

# hcat
Base.hcat(df::DataFrame, x; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(copy(df, copycols=copycols), x,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(copy(df1, copycols=copycols), df2,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df1::DataFrame, df2::AbstractDataFrame, dfn::AbstractDataFrame...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(df1, df2, makeunique=makeunique, copycols=copycols), dfn...,
          makeunique=makeunique, copycols=copycols)

##############################################################################
##
## Missing values support
##
##############################################################################
"""
    allowmissing!(df::DataFrame, cols::Colon=:)
    allowmissing!(df::DataFrame, cols::Union{Integer, Symbol})
    allowmissing!(df::DataFrame, cols::Union{AbstractVector, Regex, Not})

Convert columns `cols` of data frame `df` from element type `T` to
`Union{T, Missing}` to support missing values.

If `cols` is omitted all columns in the data frame are converted.
"""
function allowmissing! end

function allowmissing!(df::DataFrame, col::ColumnIndex)
    df[col] = allowmissing(df[col])
    df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        allowmissing!(df, col)
    end
    df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{Bool})
    length(cols) == size(df, 2) || throw(BoundsError(df, cols))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(df, col)
    end
    df
end

allowmissing!(df::DataFrame, cols::Union{Regex, Not}) =
    allowmissing!(df, index(df)[cols])

allowmissing!(df::DataFrame, cols::Colon=:) =
    allowmissing!(df, axes(df, 2))

"""
    disallowmissing!(df::DataFrame, cols::Colon=:)
    disallowmissing!(df::DataFrame, cols::Union{Integer, Symbol})
    disallowmissing!(df::DataFrame, cols::Union{AbstractVector, Regex, Not})

Convert columns `cols` of data frame `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

If `cols` is omitted all columns in the data frame are converted.
"""
function disallowmissing! end

function disallowmissing!(df::DataFrame, col::ColumnIndex)
    df[col] = disallowmissing(df[col])
    df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        disallowmissing!(df, col)
    end
    df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{Bool})
    length(cols) == size(df, 2) || throw(BoundsError(df, cols))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(df, col)
    end
    df
end

disallowmissing!(df::DataFrame, cols::Union{Regex, Not}) =
    disallowmissing!(df, index(df)[cols])

disallowmissing!(df::DataFrame, cols::Colon=:) =
    disallowmissing!(df, axes(df, 2))

##############################################################################
##
## Pooling
##
##############################################################################

"""
    categorical!(df::DataFrame, cname::Union{Integer, Symbol};
                 compress::Bool=false)
    categorical!(df::DataFrame, cnames::Vector{<:Union{Integer, Symbol}};
                 compress::Bool=false)
    categorical!(df::DataFrame, cnames::Union{Regex, Not};
                 compress::Bool=false)
    categorical!(df::DataFrame; compress::Bool=false)

Change columns selected by `cname` or `cnames` in data frame `df`
to `CategoricalVector`. If no columns are indicated then all columns whose element type
is a subtype of `Union{AbstractString, Missing}` will be converted to categorical.

If the `compress` keyword argument is set to `true` then the created `CategoricalVector`s
will be compressed.

All created `CategoricalVector`s are unordered.

### Examples

```julia
julia> df = DataFrame(X=["a", "b"], Y=[1, 2], Z=["p", "q"])
2×3 DataFrame
│ Row │ X      │ Y     │ Z      │
│     │ String │ Int64 │ String │
├─────┼────────┼───────┼────────┤
│ 1   │ a      │ 1     │ p      │
│ 2   │ b      │ 2     │ q      │

julia> categorical!(df)
2×3 DataFrame
│ Row │ X            │ Y     │ Z            │
│     │ Categorical… │ Int64 │ Categorical… │
├─────┼──────────────┼───────┼──────────────┤
│ 1   │ a            │ 1     │ p            │
│ 2   │ b            │ 2     │ q            │

julia> eltypes(df)
3-element Array{DataType,1}:
 CategoricalString{UInt32}
 Int64
 CategoricalString{UInt32}

julia> df = DataFrame(X=["a", "b"], Y=[1, 2], Z=["p", "q"])
2×3 DataFrame
│ Row │ X      │ Y     │ Z      │
│     │ String │ Int64 │ String │
├─────┼────────┼───────┼────────┤
│ 1   │ a      │ 1     │ p      │
│ 2   │ b      │ 2     │ q      │

julia> categorical!(df, :Y, compress=true)
2×3 DataFrame
│ Row │ X      │ Y            │ Z      │
│     │ String │ Categorical… │ String │
├─────┼────────┼──────────────┼────────┤
│ 1   │ a      │ 1            │ p      │
│ 2   │ b      │ 2            │ q      │

julia> eltypes(df)
3-element Array{DataType,1}:
 String
 CategoricalValue{Int64,UInt8}
 String
```
"""
function categorical! end

function categorical!(df::DataFrame, cname::ColumnIndex;
                      compress::Bool=false)
    df[cname] = categorical(df[cname], compress)
    df
end

function categorical!(df::DataFrame, cnames::AbstractVector{<:ColumnIndex};
                      compress::Bool=false)
    for cname in cnames
        df[cname] = categorical(df[cname], compress)
    end
    df
end

categorical!(df::DataFrame, cnames::Union{Regex, Not}; compress::Bool=false) =
    categorical!(df, index(df)[cnames], compress=compress)

function categorical!(df::DataFrame, cnames::Colon=:; compress::Bool=false)
    for i in 1:size(df, 2)
        if eltype(df[i]) <: Union{AbstractString, Missing}
            df[i] = categorical(df[i], compress)
        end
    end
    df
end

"""
    append!(df1::DataFrame, df2::AbstractDataFrame)

Add the rows of `df2` to the end of `df1`.

Column names must be equal (including order), with the following exceptions:
* If `df1` has no columns then copies of
  columns from `df2` are added to it.
* If `df2` has no columns then calling `append!` leaves `df1` unchanged.

Values corresponding to new rows are appended in-place to the column vectors of `df1`.
Column types are therefore preserved, and new values are converted if necessary.
An error is thrown if conversion fails: this is the case in particular if a column
in `df2` contains `missing` values but the corresponding column in `df1` does not
accept them.

Please note that `append!` must not be used on a `DataFrame` that contains columns
that are aliases (equal when compared with `===`) as it will silently produce
a wrong result in such a situation.

!!! note
    Use [`vcat`](@ref) instead of `append!` when more flexibility is needed.
    Since `vcat` does not operate in place, it is able to use promotion to find
    an appropriate element type to hold values from both data frames.
    It also accepts columns in different orders between `df1` and `df2`.

    Use [`push!`](@ref) to add individual rows to a data frame.

# Examples
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3);

julia> df2 = DataFrame(A=4.0:6.0, B=4:6);

julia> append!(df1, df2);

julia> df1
6×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 4     │ 4     │
│ 5   │ 5     │ 5     │
│ 6   │ 6     │ 6     │
```
"""
function Base.append!(df1::DataFrame, df2::AbstractDataFrame)
    if ncol(df1) == 0
        for (n, v) in eachcol(df2, true)
            df1[n] = copy(v) # make sure df1 does not reuse df2
        end
        return df1
    end
    ncol(df2) == 0 && return df1

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

Base.convert(::Type{DataFrame}, d::AbstractDict) = DataFrame(d, copycols=false)

function Base.push!(df::DataFrame, row::Union{AbstractDict, NamedTuple}; columns::Symbol=:equal)
    if !(columns in (:equal, :intersect))
        throw(ArgumentError("`columns` keyword argument must be `:equal` or `:intersect`"))
    end
    if ncol(df) == 0 && row isa NamedTuple
        for (n, v) in pairs(row)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end
    i = 1
    # Only check for equal lengths, as an error will be thrown below if some names don't match
    if columns === :equal && length(row) != size(df, 2)
        # TODO: add tests for this case after the deprecation period
        Base.depwarn("In the future push! will require that `row` has the same number" *
                      "of elements as is the number of columns in `df`." *
                      "Use `columns=:intersect` to disable this check.", :push!)
    end
    for nm in _names(df)
        try
            push!(df[i], row[nm])
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(df[j])
            end
            msg = "Error adding value to column :$nm."
            throw(ArgumentError(msg))
        end
        i += 1
    end
    df
end

"""
    push!(df::DataFrame, row)
    push!(df::DataFrame, row::Union{DataFrameRow, NamedTuple, AbstractDict};
          columns::Symbol=:intersect)

Add in-place one row at the end of `df` taking the values from `row`.

Column types of `df` are preserved, and new values are converted if necessary.
An error is thrown if conversion fails.

If `row` is neither a `DataFrameRow`, `NamedTuple` nor `AbstractDict` then
it is assumed to be an iterable and columns are matched by order of appearance.
In this case `row` must contain the same number of elements as the number of columns in `df`.

If `row` is a `DataFrameRow`, `NamedTuple` or `AbstractDict` then
values in `row` are matched to columns in `df` based on names (order is ignored).
`row` may contain more columns than `df` if `columns=:intersect`
(this is currently the default, but will change in the future), but all column names
that are present in `df` must be present in `row`.
Otherwise if `columns=:equal` then `row` must contain exactly the same columns as `df`
(but possibly in a different order).

As a special case, if `df` has no columns and `row` is a `NamedTuple` or `DataFrameRow`,
columns are created for all values in `row`, using their names and order.

Please note that `push!` must not be used on a `DataFrame` that contains columns
that are aliases (equal when compared with `===`) as it will silently produce
a wrong result in such a situation.

# Examples
```jldoctest
julia> df = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │

julia> push!(df, (true, false))
4×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 1     │ 0     │

julia> push!(df, df[1, :])
5×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 1     │ 0     │
│ 5   │ 1     │ 1     │

julia> push!(df, (C="something", A=true, B=false), columns=:intersect)
4×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 1     │ 0     │
│ 5   │ 1     │ 1     │
│ 6   │ 1     │ 0     │

julia> push!(df, Dict(:A=>1.0, :B=>2.0))
5×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 1     │ 0     │
│ 5   │ 1     │ 1     │
│ 6   │ 1     │ 0     │
│ 7   │ 1     │ 2     │
```
"""
function Base.push!(df::DataFrame, row::Any)
    if length(row) != size(df, 2)
        msg = "Length of `row` does not match `DataFrame` column count."
        throw(ArgumentError(msg))
    end
    i = 1
    for t in row
        try
            push!(_columns(df)[i], t)
        catch
            #clean up partial row
            for j in 1:(i - 1)
                pop!(_columns(df)[j])
            end
            msg = "Error adding $(repr(t)) to column :$(_names(df)[i]). Possible type mis-match."
            throw(ArgumentError(msg))
        end
        i += 1
    end
    df
end

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
    permute!(_columns(df), p)
    @inbounds permute!(index(df), p)
    df
end

function permutecols!(df::DataFrame, p::AbstractVector{Symbol})
    permutecols!(df, index(df)[p])
end
