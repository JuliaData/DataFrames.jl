"""
    DataFrame <: AbstractDataFrame

An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

# Constructors
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

# Arguments
- `columns` : a Vector with each column as contents or a Matrix
- `names` : the column names
- `makeunique` : if `false` (the default), an error will be raised
  if duplicates in `names` are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).
- `kwargs` : the key gives the column names, and the value is the
  column contents; note that the `copycols` keyword argument indicates if
  if vectors passed as columns should be copied so it is not possible to create
  a column whose name is `:copycols` using this constructor
- `t` : elemental type of all columns
- `nrows`, `ncols` : number of rows and columns
- `column_eltypes` : element type of each column
- `categorical` : a vector of `Bool` indicating which columns should be converted to
                  `CategoricalVector`
- `ds` : `AbstractDict` of columns
- `table` : any type that implements the
  [Tables.jl](https://github.com/JuliaData/Tables.jl) interface; in particular
  a tuple or vector of `Pair{Symbol, <:AbstractVector}}` objects is a table.
- `copycols` : whether vectors passed as columns should be copied; if set
  to `false` then the constructor will still copy the passed columns
  if it is not possible to construct a `DataFrame` without materializing new columns.

All columns in `columns` should have the same length.

# Notes
The `DataFrame` constructor by default copies all columns vectors passed to it.
Pass `copycols=false` to reuse vectors without copying them

If a column is passed to a `DataFrame` constructor or is assigned as a whole
using `setindex!` then its reference is stored in the `DataFrame`. An exception
to this rule is assignment of an `AbstractRange` as a column, in which case the
range is collected to a `Vector`.

Because column types can vary, a `DataFrame` is not type stable. For
performance-critical code, do not index into a `DataFrame` inside of loops.

# Examples
```julia
df = DataFrame()
v = ["x","y","z"][rand(1:3, 10)]
df1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])
df2 = DataFrame(A = 1:10, B = v, C = rand(10))
summary(df1)
describe(df2)
first(df1, 10)
df1.B
df2[!, :C]
df1[:, :A]
df1[1:4, 1:2]
df1[Not(1:4), Not(1:2)]
df1[1:2, [:A,:C]]
df1[1:2, r"[AC]"]
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

# Examples
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
```
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
## DataFrame consistency check
##
##############################################################################

function _check_consistency(df::DataFrame)
    cols, idx = _columns(df), index(df)
    ncols = length(cols)
    @assert length(idx.names) == length(idx.lookup) == ncols
    ncols == 0 && return nothing
    nrows = length(cols[1])
    for i in 2:length(cols)
        @assert length(cols[i]) == nrows "Data frame is corrupt: length of column :$(names(df)[i]) ($(length(df[!, i])))" *
                                         " does not match length of column 1 ($(length(df[!, 1]))). " *
                                         "The column vector has likely been resized unintentionally " *
                                         "(either directly or because it is shared with another data frame)."
    end
    nothing
end

_check_consistency(df::AbstractDataFrame) = _check_consistency(parent(df))

##############################################################################
##
## getindex() definitions
##
##############################################################################

# df[SingleRowIndex, SingleColumnIndex] => Scalar
@inline function Base.getindex(df::DataFrame, row_ind::Integer,
                               col_ind::Union{Signed, Unsigned})
    cols = _columns(df)
    @boundscheck begin
        if !checkindex(Bool, axes(cols, 1), col_ind)
            throw(BoundsError(df, (row_ind, col_ind)))
        end
        if !checkindex(Bool, axes(df, 1), row_ind)
            throw(BoundsError(df, (row_ind, col_ind)))
        end
    end

    @inbounds cols[col_ind][row_ind]
end

@inline function Base.getindex(df::DataFrame, row_ind::Integer, col_ind::Symbol)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_ind)
        throw(BoundsError(df, (row_ind, col_ind)))
    end
    @inbounds _columns(df)[selected_column][row_ind]
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector, copy
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, col_ind)))
    end
    @inbounds return _columns(df)[selected_column][row_inds]
end

@inline Base.getindex(df::DataFrame, row_inds::Not, col_ind::ColumnIndex) =
    df[axes(df, 1)[row_inds], col_ind]

# df[:, SingleColumnIndex] => AbstractVector
function Base.getindex(df::DataFrame, row_inds::Colon, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    copy(_columns(df)[selected_column])
end

# df[!, SingleColumnIndex] => AbstractVector, the same vector
@inline function Base.getindex(df::DataFrame, ::typeof(!), col_ind::Union{Signed, Unsigned})
    cols = _columns(df)
    @boundscheck if !checkindex(Bool, axes(cols, 1), col_ind)
        throw(BoundsError(df, (!, col_ind)))
    end
    @inbounds cols[col_ind]
end

function Base.getindex(df::DataFrame, ::typeof(!), col_ind::Symbol)
    selected_column = index(df)[col_ind]
    return _columns(df)[selected_column]
end

# df[MultiRowIndex, MultiColumnIndex] => DataFrame
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T},
                               col_inds::Union{AbstractVector, Regex, Not, Between, All}) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, col_inds)))
    end
    selected_columns = index(df)[col_inds]
    # Computing integer indices once for all columns is faster
    selected_rows = T === Bool ? findall(row_inds) : row_inds
    new_columns = AbstractVector[dv[selected_rows] for dv in _columns(df)[selected_columns]]
    return DataFrame(new_columns, Index(_names(df)[selected_columns]), copycols=false)
end

@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T}, ::Colon) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, :)))
    end
    # Computing integer indices once for all columns is faster
    selected_rows = T === Bool ? findall(row_inds) : row_inds
    new_columns = AbstractVector[dv[selected_rows] for dv in _columns(df)]
    return DataFrame(new_columns, copy(index(df)), copycols=false)
end

@inline Base.getindex(df::DataFrame, row_inds::Not,
                      col_inds::Union{AbstractVector, Regex, Not, Between, All, Colon}) =
    df[axes(df, 1)[row_inds], col_inds]

# df[:, MultiColumnIndex] => DataFrame
Base.getindex(df::DataFrame, row_ind::Colon, col_inds::Union{AbstractVector, Regex, Not, Between, All, Colon}) =
    select(df, col_inds, copycols=true)

# df[!, MultiColumnIndex] => DataFrame
Base.getindex(df::DataFrame, row_ind::typeof(!), col_inds::Union{AbstractVector, Regex, Not, Between, All, Colon}) =
    select(df, col_inds, copycols=false)

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
                Base.depwarn("In the future setindex! will disallow adding columns" *
                             " to a DataFrame using integer index. " *
                             "Use a Symbol to specify a column name instead.", :setindex!)
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
                                  row_inds::AbstractVector,
                                  col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        _columns(df)[index(df)[col_ind]][row_inds] .= v
        return v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
    end
end

# df[!, SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame, v::AbstractVector, ::typeof(!), col_ind::ColumnIndex)
    insert_single_column!(df, v, col_ind)
    return df
end

# df.col = AbstractVector
Base.setproperty!(df::DataFrame, col_ind::Symbol, v::AbstractVector) = (df[!, col_ind] = v)

# df[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
    return df
end

# df[SingleRowIndex, MultiColumnIndex] = value
# the method for value of type DataFrameRow, AbstractDict and NamedTuple
# is defined in dataframerow.jl

for T in (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  v::Union{Tuple, AbstractArray},
                                  row_ind::Integer,
                                  col_inds::$T)
        idxs = index(df)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned" *
                                    " collection contains $(length(v)) elements"))
        end
        for (i, x) in enumerate(v)
            df[row_ind, i] = x
        end
        return df
    end
end

# df[MultiRowIndex, SingleColumnIndex] = AbstractVector
for T in (:AbstractVector, :Not, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  v::AbstractVector,
                                  row_inds::$T,
                                  col_ind::ColumnIndex)
        if row_inds isa Colon && !haskey(index(df), col_ind)
            df[!, col_ind] = copy(v)
            return df
        end
        x = df[!, col_ind]
        try
            x[row_inds] = v
        catch
            insert_multiple_entries!(df, v, axes(df, 1)[row_inds], col_ind)
            Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                         "write `df[row_inds, col_ind] .= v` instead", :setindex!)
        end
        return df
    end
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractDataFrame
for T1 in (:AbstractVector, :Not, :Colon),
    T2 in (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  new_df::AbstractDataFrame,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(df)[col_inds]
        for (j, col) in enumerate(idxs)
            df[row_inds, col] = new_df[!, j]
        end
        if view(_names(df), idxs) != _names(new_df)
            Base.depwarn("in the future column names in source and target will have to match", :setindex!)
        end
        return df
    end
end

for T in (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  new_df::AbstractDataFrame,
                                  row_inds::typeof(!),
                                  col_inds::$T)
        idxs = index(df)[col_inds]
        if view(_names(df), idxs) != _names(new_df)
            throw(ArgumentError("Column names in source and target data frames do not match"))
        end
        for (j, col) in enumerate(idxs)
            # make sure we make a copy on assignment
            df[!, col] = new_df[:, j]
        end
        return df
    end
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractMatrix
for T1 in (:AbstractVector, :Not, :Colon, :(typeof(!))),
    T2 in (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)
    @eval function Base.setindex!(df::DataFrame,
                                  mx::AbstractMatrix,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(df)[col_inds]
        if size(mx, 2) != length(idxs)
            throw(DimensionMismatch("number of selected columns ($(length(idxs))) and number of columns in" *
                                    " matrix ($(size(mx, 2))) do not match"))
        end
        for (j, col) in enumerate(idxs)
            df[row_inds, col] = (row_inds === !) ? mx[:, j] : view(mx, :, j)
        end
        return df
    end
end

##############################################################################
##
## Mutating methods
##
##############################################################################

"""
    insertcols!(df::DataFrame, ind::Int, (name=>col)::Pair{Symbol,<:AbstractVector}...;
                makeunique::Bool=false, copycols::Bool=true)
    insertcols!(df::DataFrame, (name=>col)::Pair{Symbol,<:AbstractVector}...;
                makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data frame in place. Return the updated `DataFrame`.
If `ind` argument is omitted it is by default set to `ncol(df)+1`
(the column is inserted as a last column of a `DataFrame`).

# Arguments
- `df` : the DataFrame to which we want to add a column
- `ind` : a position at which we want to insert a column
- `name` : the name of the new column
- `col` : an `AbstractVector` giving the contents of the new column
- `makeunique` : Defines what to do if `name` already exists in `df`;
  if it is `false` an error will be thrown; if it is `true` a new unique name will
  be generated by adding a suffix
- `copycols` : whether vectors passed as columns should be copied

If `col` is an `AbstractRange` then the result of `collect(col)` is inserted.

# Examples
```jldoctest
julia> d = DataFrame(a=1:3)
3×1 DataFrame
│ Row │ a     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> insertcols!(d, 1, :b => 'a':'c')
3×2 DataFrame
│ Row │ b    │ a     │
│     │ Char │ Int64 │
├─────┼──────┼───────┤
│ 1   │ 'a'  │ 1     │
│ 2   │ 'b'  │ 2     │
│ 3   │ 'c'  │ 3     │

julia> insertcols!(d, 2, :c => 2:4, :c => 3:5, makeunique=true)
3×4 DataFrame
│ Row │ b    │ c     │ c_1   │ a     │
│     │ Char │ Int64 │ Int64 │ Int64 │
├─────┼──────┼───────┼───────┼───────┤
│ 1   │ 'a'  │ 2     │ 3     │ 1     │
│ 2   │ 'b'  │ 3     │ 4     │ 2     │
│ 3   │ 'c'  │ 4     │ 5     │ 3     │
```
"""
function insertcols!(df::DataFrame, col_ind::Int, name_cols::Pair{Symbol, <:AbstractVector}...;
                     makeunique::Bool=false, copycols::Bool=true)
    if !(0 < col_ind <= ncol(df) + 1)
        throw(ArgumentError("attempt to insert a column to a data frame with " *
                            "$(ncol(df)) columns at index $col_ind"))
    end

    for (name, item) in name_cols
        if ncol(df) == 0
            if copycols
                df[:, name] = item
            else
                df[!, name] = item
            end
            col_ind += 1
            continue
        end

        if nrow(df) != length(item)
            throw(DimensionMismatch("length of new column ($(length(item))) must match" *
                                    " the number of rows in data frame ($(nrow(df)))"))
        end

        if hasproperty(df, name)
            if makeunique
                k = 1
                while true
                    nn = Symbol("$(name)_$k")
                    if !hasproperty(df, nn)
                        name = nn
                        break
                    end
                    k += 1
                end
            else
                throw(ArgumentError("Duplicate variable name $name. Pass makeunique=true" *
                                    " to make it unique using a suffix automatically."))
            end
        end
        insert!(index(df), col_ind, name)
        if item isa AbstractRange
            item_new = collect(item)
        elseif copycols
            item_new = copy(item)
        else
            item_new = item
        end
        insert!(_columns(df), col_ind, item_new)
        col_ind += 1
    end
    return df
end

insertcols!(df::DataFrame, name_cols::Pair{Symbol, <:AbstractVector}...;
            makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, ncol(df)+1, name_cols..., makeunique=makeunique, copycols=copycols)

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

# Examples
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
        throw(BoundsError(df, (inds, :)))
    end
    # we require ind to be stored and unique like in Base
    foreach(col -> deleteat!(col, inds), _columns(df))
    df
end

function deleterows!(df::DataFrame, inds::AbstractVector{Bool})
    if length(inds) != size(df, 1)
        throw(BoundsError(df, (inds, :)))
    end
    drop = findall(inds)
    foreach(col -> deleteat!(col, drop), _columns(df))
    df
end

"""
    select!(df::DataFrame, inds...)

Mutate `df` in place to retain only columns specified by `inds...` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing
provided that the columns requested in each of them are unique and present in `df`.
In particular, regular expressions, `All`, `Between`, and `Not` selectors are supported.

If more than one argument is passed then they are joined as `All(inds...)`.
Note that `All` selects the union of columns passed to it, so columns selected
in different `inds...` do not have to be unique. For example a call
`select!(df, :col, All())` is valid and moves column `:col` in the
data frame to be the first, provided it is present in `df`.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select!(df, 2)
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
    copy!(_columns(df), _columns(df)[inds])
    x = index(df)
    copy!(_names(x), _names(df)[inds])
    empty!(x.lookup)
    for (i, n) in enumerate(x.names)
        x.lookup[n] = i
    end
    df
end

select!(df::DataFrame, c::Int) = select!(df, [c])
select!(df::DataFrame, c::Any) = select!(df, index(df)[c])
select!(df::DataFrame, c, cs...) = select!(df, All(c, cs...))

"""
    select(df::AbstractDataFrame, inds...; copycols::Bool=true)

Create a new data frame that contains columns from `df`
specified by `inds` and return it.

Arguments passed as `inds...` can be any index that is allowed for column indexing
provided that the columns requested in each of them are unique and present in `df`.
In particular, regular expressions, `All`, `Between`, and `Not` selectors  are supported.

If more than one argument is passed then they are joined as `All(inds...)`.
Note that `All` selects the union of columns passed to it, so columns selected
in different `inds...` do not have to be unique. For example a call
`select(df, :col, All())` is valid and creates a new data frame with column `:col`
moved to be the first, provided it is present in `df`.


If `df` is a `DataFrame` return a new `DataFrame` that contains columns from `df`
specified by `inds`.
If `copycols=true` (the default), then returned `DataFrame` holds
copies of column vectors in `df`.
If `copycols=false`, then returned `DataFrame` shares column vectors with `df`.

If `df` is a `SubDataFrame` then a `SubDataFrame` is returned if `copycols=false`
and a `DataFrame` with freshly allocated columns otherwise.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 4     │
│ 2   │ 2     │ 5     │
│ 3   │ 3     │ 6     │

julia> select(df, :b)
3×1 DataFrame
│ Row │ b     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 4     │
│ 2   │ 5     │
│ 3   │ 6     │

julia> select(df, Not(:b)) # drop column :b from df
3×1 DataFrame
│ Row │ a     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │
```

"""
select(df::DataFrame, inds::AbstractVector{Int}; copycols::Bool=true) =
    DataFrame(_columns(df)[inds], Index(_names(df)[inds]),
              copycols=copycols)

select(df::DataFrame, c::Int; copycols::Bool=true) =
    select(df, [c], copycols=copycols)
select(df::DataFrame, c::Any; copycols::Bool=true) =
    select(df, index(df)[c], copycols=copycols)
select(df::DataFrame, c, cs...; copycols::Bool=true) =
    select(df, All(c, cs...), copycols=copycols)

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
        df1[!, u[i]] = copycols ? df2[:, i] : df2[!, i]
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
    allowmissing!(df::DataFrame, cols::Union{AbstractVector, Regex, Not, Between, All})

Convert columns `cols` of data frame `df` from element type `T` to
`Union{T, Missing}` to support missing values.

If `cols` is omitted all columns in the data frame are converted.
"""
function allowmissing! end

function allowmissing!(df::DataFrame, col::ColumnIndex)
    df[!, col] = allowmissing(df[!, col])
    df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        allowmissing!(df, col)
    end
    df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{Bool})
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(df, col)
    end
    df
end

allowmissing!(df::DataFrame, cols::Union{Regex, Not, Between, All}) =
    allowmissing!(df, index(df)[cols])

allowmissing!(df::DataFrame, cols::Colon=:) =
    allowmissing!(df, axes(df, 2))

"""
    disallowmissing!(df::DataFrame, cols::Colon=:; error::Bool=true)
    disallowmissing!(df::DataFrame, cols::Union{Integer, Symbol}; error::Bool=true)
    disallowmissing!(df::DataFrame, cols::Union{AbstractVector, Regex, Not, Between, All};
                     error::Bool=true)

Convert columns `cols` of data frame `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

If `cols` is omitted all columns in the data frame are converted.

If `error=false` then columns containing a `missing` value will be skipped instead of throwing an error.
"""
function disallowmissing! end

function disallowmissing!(df::DataFrame, col::ColumnIndex; error::Bool=true)
    x = df[!, col]
    if !(!error && Missing <: eltype(x) && any(ismissing, x))
        df[!, col] = disallowmissing(x)
    end
    df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex};
                          error::Bool=true)
    for col in cols
        disallowmissing!(df, col, error=error)
    end
    df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{Bool}; error::Bool=true)
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(df, col, error=error)
    end
    df
end

disallowmissing!(df::DataFrame, cols::Union{Regex, Not, Between, All}; error::Bool=true) =
    disallowmissing!(df, index(df)[cols], error=error)

disallowmissing!(df::DataFrame, cols::Colon=:; error::Bool=true) =
    disallowmissing!(df, axes(df, 2), error=error)

##############################################################################
##
## Pooling
##
##############################################################################

"""
    categorical!(df::DataFrame, cols::Type=Union{AbstractString, Missing};
                 compress::Bool=false)
    categorical!(df::DataFrame, cname::Union{Integer, Symbol};
                 compress::Bool=false)
    categorical!(df::DataFrame, cnames::Vector{<:Union{Integer, Symbol}};
                 compress::Bool=false)
    categorical!(df::DataFrame, cnames::Union{Regex, Not, Between, All};
                 compress::Bool=false)

Change columns selected by `cname` or `cnames` in data frame `df`
to `CategoricalVector`.

If `categorical!` is called with the `cols` argument being a `Type`, then
all columns whose element type is a subtype of this type
(by default `Union{AbstractString, Missing}`) will be converted to categorical.

If the `compress` keyword argument is set to `true` then the created `CategoricalVector`s
will be compressed.

All created `CategoricalVector`s are unordered.

# Examples
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

julia> eltype.(eachcol(df))
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

julia> eltype.(eachcol(df))
3-element Array{DataType,1}:
 String
 CategoricalValue{Int64,UInt8}
 String
```
"""
function categorical! end

function categorical!(df::DataFrame, cname::ColumnIndex;
                      compress::Bool=false)
    df[!, cname] = categorical(df[!, cname], compress)
    df
end

function categorical!(df::DataFrame, cnames::AbstractVector{<:ColumnIndex};
                      compress::Bool=false)
    for cname in cnames
        df[!, cname] = categorical(df[!, cname], compress)
    end
    df
end

categorical!(df::DataFrame, cnames::Union{Regex, Not, Between, All, Colon}; compress::Bool=false) =
    categorical!(df, index(df)[cnames], compress=compress)

function categorical!(df::DataFrame,
                      cols::Type=Union{AbstractString, Missing};
                      compress::Bool=false)
    for i in 1:size(df, 2)
        if eltype(df[!, i]) <: cols
            df[!, i] = categorical(df[!, i], compress)
        end
    end
    df
end

"""
    append!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal)
    append!(df::DataFrame, table; cols::Symbol=:setequal)

Add the rows of `df2` to the end of `df1`. If the second argument `table` is
not an `AbstractDataFrame` then it is converted using `DataFrame(table, copycols=false)`
before being appended.

Column names of  `df1` and `df2` must be equal.
If `cols` is `:setequal` (the default) then column names may have different orders
and `append!` is performed by matching column names.
If `cols` is `:orderequal` then the order of columns in `df1` and `df2` or `table`
must be the same. In particular, if `table` is a `Dict` an error is thrown
as it is an unordered collection.

The above rule has the following exceptions:
* If `df1` has no columns then copies of
  columns from `df2` are added to it.
* If `df2` has no columns then calling `append!` leaves `df1` unchanged.

Values corresponding to new rows are appended in-place to the column vectors of `df1`.
Column types are therefore preserved, and new values are converted if necessary.
An error is thrown if conversion fails: this is the case in particular if a column
in `df2` contains `missing` values but the corresponding column in `df1` does not
accept them.

Please note that `append!` must not be used on a `DataFrame` that contains columns
that are aliases (equal when compared with `===`).

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
function Base.append!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal)
    if !(cols in (:orderequal, :setequal))
        throw(ArgumentError("`cols` keyword argument must be any of :setequal, :orderequal"))
    end

    if ncol(df1) == 0
        for (n, v) in eachcol(df2, true)
            df1[!, n] = copy(v) # make sure df1 does not reuse df2
        end
        return df1
    end
    ncol(df2) == 0 && return df1

    if cols == :orderequal && _names(df1) != _names(df2)
        wrongnames = symdiff(_names(df1), _names(df2))
        if isempty(wrongnames)
            mismatches = findall(_names(df1) .!= _names(df2))
            @assert !isempty(mismatches)
            throw(ArgumentError("Columns number " *
                                join(mismatches, ", ", " and ") *
                                " do not have the same names in both passed data frames" *
                                "and `cols=:orderequal`"))
        else
            mismatchmsg = " Column names :" *
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", ", :" and :") *
                                "were found in only one of the passed data frames " *
                                "and `cols=:orderequal`"))
        end
    elseif cols == :setequal
        wrongnames = symdiff(_names(df1), _names(df2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", ", :" and :") *
                                "were found in only one of the passed data frames " *
                                "and passed `cols=:setequal`"))
        end
    end

    nrows, ncols = size(df1)
    targetrows = nrows + nrow(df2)
    current_col = 0
    try
        for (j, n) in enumerate(_names(df1))
            current_col += 1
            append!(df1[!, j], df2[!, n])
        end
        current_col = 0
        for col in _columns(df1)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        # Undo changes in case of error
        for col in _columns(df1)
            resize!(col, nrows)
        end
        @error "Error adding value to column $(names(df1)[current_col])."
        rethrow(err)
    end
    return df1
end

Base.convert(::Type{DataFrame}, A::AbstractMatrix) = DataFrame(A)

Base.convert(::Type{DataFrame}, d::AbstractDict) = DataFrame(d, copycols=false)

function Base.push!(df::DataFrame, row::Union{AbstractDict, NamedTuple}; cols::Symbol=:setequal,
                    columns::Union{Nothing,Symbol}=nothing)
    if columns !== nothing
        cols = columns
        Base.depwarn("`columns` keyword argument is deprecated. Use `cols` instead. ", :push!)
    end
    possible_cols = (:orderequal, :setequal, :intersect, :subset)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" * join(possible_cols, ", :")))
    end
    nrows, ncols = size(df)
    targetrows = nrows + 1
    if ncols == 0 && row isa NamedTuple
        for (n, v) in pairs(row)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end
    if cols == :orderequal
        if row isa Dict
            throw(ArgumentError("passing `Dict` as `row` when `cols=:orderequal` " *
                                "is not allowed as it is unordered"))
        elseif length(row) != ncol(df) || any(x -> x[1] != x[2], zip(keys(row), _names(df)))
            throw(ArgumentError("when `cols=:orderequal` all data frames must have " *
                                "the same column names and in the same order"))
        end
    elseif cols == :setequal || cols === :equal
        if cols == :equal
            Base.depwarn("`cols=:equal` is deprecated." *
                         "Use `:setequal` instead.", :push!)
        end
        # Only check for equal lengths if :setequal is selected,
        # as an error will be thrown below if some names don't match
        if length(row) != ncols
            Base.depwarn("In the future `push!` with `cols` equal to `:setequal`" *
                         "will require `row` to have the same number of elements as is the " *
                         "number of columns in `df`.", :push!)
        end
    end
    current_col = 0
    try
        for (col, nm) in zip(_columns(df), _names(df))
            current_col += 1
            if cols === :subset
                val = get(row, nm, missing)
            else
                val = row[nm]
            end
            push!(col, val)
        end
        current_col = 0
        for col in _columns(df)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        for col in _columns(df)
            resize!(col, nrows)
        end
        @error "Error adding value to column :$(names(df)[current_col])."
        rethrow(err)
    end
    df
end

"""
    push!(df::DataFrame, row::Union{Tuple, AbstractArray})
    push!(df::DataFrame, row::Union{DataFrameRow, NamedTuple, AbstractDict};
          cols::Symbol=:setequal)

Add in-place one row at the end of `df` taking the values from `row`.

Column types of `df` are preserved, and new values are converted if necessary.
An error is thrown if conversion fails.

If `row` is neither a `DataFrameRow`, `NamedTuple` nor `AbstractDict` then
it must be a `Tuple` or an `AbstractArray`
and columns are matched by order of appearance. In this case `row` must contain
the same number of elements as the number of columns in `df`.

If `row` is a `DataFrameRow`, `NamedTuple` or `AbstractDict` then
values in `row` are matched to columns in `df` based on names. The exact behavior
depends on the `cols` argument value in the following way:
* If `cols=:setequal` (this is the default)
  then `row` must contain exactly the same columns as `df` (but possibly in a different order).
* If `cols=:orderequal` then `row` must contain the same columns in the same order
  (for `AbstractDict` this option requires that `keys(row)` matches `names(df)`
   to allow for support of ordered dicts; however, if `row` is a `Dict` an error is thrown
   as it is an unordered collection).
* If `cols=:intersect` then `row` may contain more columns than `df`,
  but all column names that are present in `df` must be present in `row` and only they
  are used to populate a new row in `df`.
* If `cols=:subset` then `push!` behaves like for `:intersect` but if some column
  is missing in `row` then a `missing` value is pushed to `df`.

As a special case, if `df` has no columns and `row` is a `NamedTuple` or `DataFrameRow`,
columns are created for all values in `row`, using their names and order.

Please note that `push!` must not be used on a `DataFrame` that contains columns
that are aliases (equal when compared with `===`).

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

julia> push!(df, (C="something", A=true, B=false), cols=:intersect)
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
    if !(row isa Union{Tuple, AbstractArray})
        Base.depwarn("In the future `push!` will not allow passing collections of type" *
                     " $(typeof(row)) to be pushed into a DataFrame. " *
                     "Only `Tuple`, `AbstractArray`, `AbstractDict`, `DataFrameRow` and " *
                     "`NamedTuple` will be allowed.", :push!)
    end
    nrows, ncols = size(df)
    targetrows = nrows + 1
    if length(row) != ncols
        msg = "Length of `row` does not match `DataFrame` column count."
        throw(ArgumentError(msg))
    end
    current_col = 0
    try
        for (col, t) in zip(_columns(df), row)
            current_col += 1
            push!(col, t)
        end
        current_col = 0
        for col in _columns(df)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        #clean up partial row
        for col in _columns(df)
            resize!(col, nrows)
        end
        @error "Error adding value to column :$(names(df)[current_col])."
        rethrow(err)
    end
    df
end
