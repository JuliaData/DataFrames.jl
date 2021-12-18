"""
    DataFrame <: AbstractDataFrame

An AbstractDataFrame that stores a set of named columns

The columns are normally AbstractVectors stored in memory,
particularly a Vector or CategoricalVector.

# Constructors
```julia
DataFrame(pairs::Pair...; makeunique::Bool=false, copycols::Bool=true)
DataFrame(pairs::AbstractVector{<:Pair}; makeunique::Bool=false, copycols::Bool=true)
DataFrame(ds::AbstractDict; copycols::Bool=true)
DataFrame(kwargs..., copycols::Bool=true)

DataFrame(columns::AbstractVecOrMat,
          names::AbstractVector;
          makeunique::Bool=false, copycols::Bool=true)

DataFrame(table; copycols::Union{Bool, Nothing}=nothing)
DataFrame(::DataFrameRow)
DataFrame(::GroupedDataFrame; keepkeys::Bool=true)
```

# Keyword arguments

- `copycols` : whether vectors passed as columns should be copied; by default set
  to `true` and the vectors are copied; if set to `false` then the constructor
  will still copy the passed columns if it is not possible to construct a
  `DataFrame` without materializing new columns. Note the `copycols=nothing`
  default in the Tables.jl compatible constructor; it is provided as certain
  input table types may have already made a copy of columns or the columns may
  otherwise be immutable, in which case columns are not copied by default.
  To force a copy in such cases, or to get mutable columns from an immutable
  input table (like `Arrow.Table`), pass `copycols=true` explicitly.
- `makeunique` : if `false` (the default), an error will be raised

(note that not all constructors support these keyword arguments)

# Details on behavior of different constructors

It is allowed to pass a vector of `Pair`s, a list of `Pair`s as positional
arguments, or a list of keyword arguments. In this case each pair is considered
to represent a column name to column value mapping and column name must be a
`Symbol` or string. Alternatively a dictionary can be passed to the constructor
in which case its entries are considered to define the column name and column
value pairs. If the dictionary is a `Dict` then column names will be sorted in
the returned `DataFrame`.

In all the constructors described above column value can be a vector which is
consumed as is or an object of any other type (except `AbstractArray`). In the
latter case the passed value is automatically repeated to fill a new vector of
the appropriate length. As a particular rule values stored in a `Ref` or a
`0`-dimensional `AbstractArray` are unwrapped and treated in the same way.

It is also allowed to pass a vector of vectors or a matrix as as the first
argument. In this case the second argument must be
a vector of `Symbol`s or strings specifying column names, or the symbol `:auto`
to generate column names `x1`, `x2`, ... automatically. Note that in this case
if the first argument is a matrix and `copycols=false` the columns of the created
`DataFrame` will be views of columns the source matrix.

If a single positional argument is passed to a `DataFrame` constructor then it
is assumed to be of type that implements the
[Tables.jl](https://github.com/JuliaData/Tables.jl) interface using which the
returned `DataFrame` is materialized.

Finally it is allowed to construct a `DataFrame` from a `DataFrameRow` or a
`GroupedDataFrame`. In the latter case the `keepkeys` keyword argument specifies
whether the resulting `DataFrame` should contain the grouping columns of the
passed `GroupedDataFrame` and the order of rows in the result follows the order
of groups in the `GroupedDataFrame` passed.

# Notes

The `DataFrame` constructor by default copies all columns vectors passed to it.
Pass the `copycols=false` keyword argument (where supported) to reuse vectors without
copying them.

By default an error will be raised if duplicates in column names are found. Pass
`makeunique=true` keyword argument (where supported) to accept duplicate names,
in which case they will be suffixed with `_i` (`i` starting at 1 for the first
duplicate).

If an `AbstractRange` is passed to a `DataFrame` constructor as a column it is
always collected to a `Vector` (even if `copycols=false`). As a general rule
`AbstractRange` values are always materialized to a `Vector` by all functions in
DataFrames.jl before being stored in a `DataFrame`.

`DataFrame` can store only columns that use 1-based indexing. Attempting
to store a vector using non-standard indexing raises an error.

The `DataFrame` type is designed to allow column types to vary and to be
dynamically changed also after it is constructed. Therefore `DataFrame`s are not
type stable. For performance-critical code that requires type-stability either
use the functionality provided by `select`/`transform`/`combine` functions, use
`Tables.columntable` and `Tables.namedtupleiterator` functions, use barrier
functions, or provide type assertions to the variables that hold columns
extracted from a `DataFrame`.

# Examples
```jldoctest
julia> DataFrame((a=[1, 2], b=[3, 4])) # Tables.jl table constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> DataFrame([(a=1, b=0), (a=2, b=0)]) # Tables.jl table constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame("a" => 1:2, "b" => 0) # Pair constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([:a => 1:2, :b => 0]) # vector of Pairs constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame(Dict(:a => 1:2, :b => 0)) # dictionary constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame(a=1:2, b=0) # keyword argument constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([[1, 2], [0, 0]], [:a, :b]) # vector of vectors constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([1 0; 2 0], :auto) # matrix constructor
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```
"""
struct DataFrame <: AbstractDataFrame
    columns::Vector{AbstractVector}
    colindex::Index

    # the inner constructor should not be used directly
    function DataFrame(columns::Union{Vector{Any}, Vector{AbstractVector}},
                       colindex::Index; copycols::Bool=true)
        if length(columns) == length(colindex) == 0
            return new(AbstractVector[], Index())
        elseif length(columns) != length(colindex)
            throw(DimensionMismatch("Number of columns ($(length(columns))) and number of " *
                                    "column names ($(length(colindex))) are not equal"))
        end

        len = -1
        firstvec = -1
        for (i, col) in enumerate(columns)
            if col isa AbstractVector
                if len == -1
                    len = length(col)
                    firstvec = i
                elseif len != length(col)
                    n1 = _names(colindex)[firstvec]
                    n2 = _names(colindex)[i]
                    throw(DimensionMismatch("column :$n1 has length $len and column " *
                                            ":$n2 has length $(length(col))"))
                end
            end
        end
        len == -1 && (len = 1) # we got no vectors so make one row of scalars

        # we write into columns as we know that it is guaranteed
        # that it was freshly allocated in the outer constructor
        @static if VERSION >= v"1.4"
            if copycols && len >= 1_000_000 && length(columns) > 1 && Threads.nthreads() > 1
                @sync for i in eachindex(columns)
                    Threads.@spawn columns[i] = _preprocess_column(columns[i], len, copycols)
                end
            else
                for i in eachindex(columns)
                    columns[i] = _preprocess_column(columns[i], len, copycols)
                end
            end
        else
            for i in eachindex(columns)
                columns[i] = _preprocess_column(columns[i], len, copycols)
            end
        end

        for (i, col) in enumerate(columns)
            firstindex(col) != 1 && _onebased_check_error(i, col)
        end

        new(convert(Vector{AbstractVector}, columns), colindex)
    end
end

function _preprocess_column(col::Any, len::Integer, copycols::Bool)
    if col isa AbstractRange
        return collect(col)
    elseif col isa AbstractVector
        return copycols ? copy(col) : col
    elseif col isa Union{AbstractArray{<:Any, 0}, Ref}
        x = col[]
        return fill!(Tables.allocatecolumn(typeof(x), len), x)
    elseif col isa AbstractArray
        throw(ArgumentError("adding AbstractArray other than AbstractVector " *
                            "as a column of a data frame is not allowed"))
    else
        return fill!(Tables.allocatecolumn(typeof(col), len), col)
    end
end

DataFrame(df::DataFrame; copycols::Bool=true) = copy(df, copycols=copycols)

function DataFrame(pairs::Pair{Symbol, <:Any}...; makeunique::Bool=false,
                   copycols::Bool=true)::DataFrame
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return DataFrame(columns, Index(colnames, makeunique=makeunique),
                     copycols=copycols)
end

function DataFrame(pairs::Pair{<:AbstractString, <:Any}...; makeunique::Bool=false,
                   copycols::Bool=true)::DataFrame
    colnames = [Symbol(k) for (k, v) in pairs]
    columns = Any[v for (k, v) in pairs]
    return DataFrame(columns, Index(colnames, makeunique=makeunique),
                     copycols=copycols)
end

# this is needed as a workaround for Tables.jl dispatch
function DataFrame(pairs::AbstractVector{<:Pair}; makeunique::Bool=false,
                   copycols::Bool=true)
    if isempty(pairs)
        return DataFrame()
    else
        if !(all(((k, v),) -> k isa Symbol, pairs) || all(((k, v),) -> k isa AbstractString, pairs))
            throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
        end
        colnames = [Symbol(k) for (k, v) in pairs]
        columns = Any[v for (k, v) in pairs]
        return DataFrame(columns, Index(colnames, makeunique=makeunique),
                         copycols=copycols)
    end
end

function DataFrame(d::AbstractDict; copycols::Bool=true)
    if all(k -> k isa Symbol, keys(d))
        colnames = collect(Symbol, keys(d))
    elseif all(k -> k isa AbstractString, keys(d))
        colnames = [Symbol(k) for k in keys(d)]
    else
        throw(ArgumentError("All column names must be either Symbols or strings (mixing is not allowed)"))
    end

    colindex = Index(colnames)
    columns = Any[v for v in values(d)]
    df = DataFrame(columns, colindex, copycols=copycols)
    d isa Dict && select!(df, sort!(propertynames(df)))
    return df
end

function DataFrame(; kwargs...)
    if isempty(kwargs)
        DataFrame([], Index())
    else
        cnames = Symbol[]
        columns = Any[]
        copycols = true
        for (kw, val) in kwargs
            if kw === :copycols
                if val isa Bool
                    copycols = val
                else
                    throw(ArgumentError("the `copycols` keyword argument must be Boolean"))
                end
            elseif kw === :makeunique
                    throw(ArgumentError("the `makeunique` keyword argument is not allowed " *
                                        "in DataFrame(; kwargs...) constructor"))
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
    if !(eltype(columns) <: AbstractVector) && !all(col -> isa(col, AbstractVector), columns)
        throw(ArgumentError("columns argument must be a vector of AbstractVector objects"))
    end
    return DataFrame(collect(AbstractVector, columns),
                     Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
                     copycols=copycols)
end

function _name2symbol(str::AbstractVector)
    if !(all(x -> x isa AbstractString, str) || all(x -> x isa Symbol, str))
        throw(ArgumentError("All passed column names must be strings or Symbols"))
    end
    return Symbol[Symbol(s) for s in str]
end

DataFrame(columns::AbstractVector, cnames::AbstractVector;
          makeunique::Bool=false, copycols::Bool=true) =
    DataFrame(columns, _name2symbol(cnames), makeunique=makeunique, copycols=copycols)

DataFrame(columns::AbstractVector{<:AbstractVector}, cnames::AbstractVector{Symbol};
          makeunique::Bool=false, copycols::Bool=true)::DataFrame =
    DataFrame(collect(AbstractVector, columns),
              Index(convert(Vector{Symbol}, cnames), makeunique=makeunique),
              copycols=copycols)

DataFrame(columns::AbstractVector{<:AbstractVector}, cnames::AbstractVector;
          makeunique::Bool=false, copycols::Bool=true) =
    DataFrame(columns, _name2symbol(cnames); makeunique=makeunique, copycols=copycols)

function DataFrame(columns::AbstractVector, cnames::Symbol; copycols::Bool=true)
    if cnames !== :auto
        throw(ArgumentError("if the first positional argument to DataFrame " *
                            "constructor is a vector of vectors and the second " *
                            "positional argument is passed then the second " *
                            "argument must be a vector of column names or :auto"))
    end
    return DataFrame(columns, gennames(length(columns)), copycols=copycols)
end

function DataFrame(columns::AbstractMatrix, cnames::AbstractVector{Symbol};
                   makeunique::Bool=false, copycols::Bool=true)
    getter = copycols ? getindex : view
    return DataFrame(AbstractVector[getter(columns, :, i) for i in 1:size(columns, 2)],
                     cnames, makeunique=makeunique, copycols=false)
end

DataFrame(columns::AbstractMatrix, cnames::AbstractVector;
          makeunique::Bool=false, copycols::Bool=true) =
    DataFrame(columns, _name2symbol(cnames); makeunique=makeunique, copycols=copycols)

function DataFrame(columns::AbstractMatrix, cnames::Symbol; copycols::Bool=true)
    if cnames !== :auto
        throw(ArgumentError("if the first positional argument to DataFrame " *
                            "constructor is a matrix and a second " *
                            "positional argument is passed then the second " *
                            "argument must be a vector of column names or :auto"))
    end
    return DataFrame(columns, gennames(size(columns, 2)), makeunique=false, copycols=copycols)
end

# Discontinued constructors

DataFrame(matrix::Matrix) =
    throw(ArgumentError("`DataFrame` constructor from a `Matrix` requires " *
                        "passing :auto as a second argument to automatically " *
                        "generate column names: `DataFrame(matrix, :auto)`"))

DataFrame(vecs::Vector{<:AbstractVector}) =
    throw(ArgumentError("`DataFrame` constructor from a `Vector` of vectors requires " *
                        "passing :auto as a second argument to automatically " *
                        "generate column names: `DataFrame(vecs, :auto)`"))

DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
          nrows::Integer=0; makeunique::Bool=false) where T<:Type =
    throw(ArgumentError("`DataFrame` constructor with passed eltypes is " *
                        "not supported. Pass explicitly created columns to a " *
                        "`DataFrame` constructor instead."))

DataFrame(column_eltypes::AbstractVector{<:Type}, cnames::AbstractVector{<:AbstractString},
          nrows::Integer=0; makeunique::Bool=false) where T<:Type =
    throw(ArgumentError("`DataFrame` constructor with passed eltypes is " *
                        "not supported. Pass explicitly created columns to a " *
                        "`DataFrame` constructor instead."))

##############################################################################
##
## AbstractDataFrame interface
##
##############################################################################

index(df::DataFrame) = getfield(df, :colindex)

# this function grants the access to the internal storage of columns of the
# `DataFrame` and its use is unsafe. If the returned vector is mutated then
# make sure that:
# 1. `AbstractRange` columns are not added to a `DataFrame`
# 2. all inserted columns use 1-based indexing
# 3. after several mutating operations on the vector are performed
#    each element (column) has the same length
# 4. if length of the vector is changed that the index of the `DataFrame`
#    is adjusted appropriately
_columns(df::DataFrame) = getfield(df, :columns)

_onebased_check_error() =
    throw(ArgumentError("Currently DataFrames.jl supports only columns " *
                        "that use 1-based indexing"))
_onebased_check_error(i, col) =
    throw(ArgumentError("Currently DataFrames.jl supports only " *
                        "columns that use 1-based indexing, but " *
                        "column $i has starting index equal to $(firstindex(col))"))

# note: these type assertions are required to pass tests
nrow(df::DataFrame) = ncol(df) > 0 ? length(_columns(df)[1])::Int : 0
ncol(df::DataFrame) = length(index(df))

##############################################################################
##
## DataFrame consistency check
##
##############################################################################

corrupt_msg(df::DataFrame, i::Integer) =
    "Data frame is corrupt: length of column " *
    ":$(_names(df)[i]) ($(length(df[!, i]))) " *
    "does not match length of column 1 ($(length(df[!, 1]))). " *
    "The column vector has likely been resized unintentionally " *
    "(either directly or because it is shared with another data frame)."

function _check_consistency(df::DataFrame)
    cols, idx = _columns(df), index(df)

    for (i, col) in enumerate(cols)
        firstindex(col) != 1 && _onebased_check_error(i, col)
    end

    ncols = length(cols)
    @assert length(idx.names) == length(idx.lookup) == ncols
    ncols == 0 && return nothing
    nrows = length(cols[1])
    for i in 2:length(cols)
        @assert length(cols[i]) == nrows corrupt_msg(df, i)
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

@inline function Base.getindex(df::DataFrame, row_ind::Integer,
                               col_ind::SymbolOrString)
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

function Base.getindex(df::DataFrame, ::typeof(!), col_ind::SymbolOrString)
    selected_column = index(df)[col_ind]
    return _columns(df)[selected_column]
end

# df[MultiRowIndex, MultiColumnIndex] => DataFrame

function _threaded_getindex(selected_rows::AbstractVector,
                            selected_columns::AbstractVector,
                            df_columns::AbstractVector,
                            idx::AbstractIndex)
    @static if VERSION >= v"1.4"
        if length(selected_rows) >= 1_000_000 && Threads.nthreads() > 1
            new_columns = Vector{AbstractVector}(undef, length(selected_columns))
            @sync for i in eachindex(new_columns)
                Threads.@spawn new_columns[i] = df_columns[selected_columns[i]][selected_rows]
            end
            return DataFrame(new_columns, idx, copycols=false)
        else
            return DataFrame(AbstractVector[df_columns[i][selected_rows] for i in selected_columns],
                             idx, copycols=false)
        end
    else
        return DataFrame(AbstractVector[df_columns[i][selected_rows] for i in selected_columns],
                         idx, copycols=false)
    end
end

@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T},
                               col_inds::MultiColumnIndex) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, col_inds)))
    end
    selected_columns = index(df)[col_inds]

    u = _names(df)[selected_columns]
    lookup = Dict{Symbol, Int}(zip(u, 1:length(u)))
    # use this constructor to avoid checking twice if column names are not
    # duplicate as index(df)[col_inds] already checks this
    idx = Index(lookup, u)

    if length(selected_columns) == 1
        return DataFrame(AbstractVector[_columns(df)[selected_columns[1]][row_inds]],
                         idx, copycols=false)
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        _threaded_getindex(selected_rows, selected_columns, _columns(df), idx)
    end
end

@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T}, ::Colon) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, :)))
    end
    idx = copy(index(df))

    if ncol(df) == 1
        return DataFrame(AbstractVector[_columns(df)[1][row_inds]], idx, copycols=false)
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        _threaded_getindex(selected_rows, 1:ncol(df), _columns(df), idx)
    end
end

@inline Base.getindex(df::DataFrame, row_inds::Not, col_inds::MultiColumnIndex) =
    df[axes(df, 1)[row_inds], col_inds]

# df[:, MultiColumnIndex] => DataFrame
Base.getindex(df::DataFrame, row_ind::Colon, col_inds::MultiColumnIndex) =
    select(df, index(df)[col_inds], copycols=true)

# df[!, MultiColumnIndex] => DataFrame
Base.getindex(df::DataFrame, row_ind::typeof(!), col_inds::MultiColumnIndex) =
    select(df, index(df)[col_inds], copycols=false)

##############################################################################
##
## setindex!()
##
##############################################################################

# Will automatically add a new column if needed
function insert_single_column!(df::DataFrame, v::AbstractVector, col_ind::ColumnIndex)
    if ncol(df) != 0 && nrow(df) != length(v)
        throw(ArgumentError("New columns must have the same length as old columns"))
    end
    dv = isa(v, AbstractRange) ? collect(v) : v
    firstindex(dv) != 1 && _onebased_check_error()

    if haskey(index(df), col_ind)
        j = index(df)[col_ind]
        _columns(df)[j] = dv
    else
        if col_ind isa SymbolOrString
            push!(index(df), Symbol(col_ind))
            push!(_columns(df), dv)
        else
            throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
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

# df[!, SingleColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame, v::AbstractVector, ::typeof(!), col_ind::ColumnIndex)
    insert_single_column!(df, v, col_ind)
    return df
end

# df.col = AbstractVector
# separate methods are needed due to dispatch ambiguity
Base.setproperty!(df::DataFrame, col_ind::Symbol, v::AbstractVector) =
    (df[!, col_ind] = v)
Base.setproperty!(df::DataFrame, col_ind::AbstractString, v::AbstractVector) =
    (df[!, col_ind] = v)
Base.setproperty!(::DataFrame, col_ind::Symbol, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a DataFrame. " *
                        "Instead use `df[!, col_ind] .= v` if you want to use broadcasting."))
Base.setproperty!(::DataFrame, col_ind::AbstractString, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a DataFrame. " *
                        "Instead use `df[!, col_ind] .= v` if you want to use broadcasting."))

# df[SingleRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(df, v, row_ind, col_ind)
    return df
end

# df[SingleRowIndex, MultiColumnIndex] = value
# the method for value of type DataFrameRow, AbstractDict and NamedTuple
# is defined in dataframerow.jl

for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(df::DataFrame,
                                  v::Union{Tuple, AbstractArray},
                                  row_ind::Integer,
                                  col_inds::$T)
        idxs = index(df)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end
        for (i, x) in zip(idxs, v)
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
        x[row_inds] = v
        return df
    end
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractDataFrame
for T1 in (:AbstractVector, :Not, :Colon),
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(df::DataFrame,
                                  new_df::AbstractDataFrame,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(df)[col_inds]
        if view(_names(df), idxs) != _names(new_df)
            throw(ArgumentError("column names in source and target do not match"))
        end
        for (j, col) in enumerate(idxs)
            df[row_inds, col] = new_df[!, j]
        end
        return df
    end
end

for T in MULTICOLUMNINDEX_TUPLE
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
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(df::DataFrame,
                                  mx::AbstractMatrix,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(df)[col_inds]
        if size(mx, 2) != length(idxs)
            throw(DimensionMismatch("number of selected columns ($(length(idxs))) " *
                                    "and number of columns in " *
                                    "matrix ($(size(mx, 2))) do not match"))
        end
        for (j, col) in enumerate(idxs)
            df[row_inds, col] = (row_inds === !) ? mx[:, j] : view(mx, :, j)
        end
        return df
    end
end

# insertcols!
# TODO: move to abstractdataframe/abstractdataframe.jl

"""
    insertcols!(df::AbstractDataFrame[, col], (name=>val)::Pair...;
                after::Bool=false, makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data frame in place. Return the updated data frame.
If `col` is omitted it is set to `ncol(df)+1`
(the column is inserted as the last column).

# Arguments
- `df` : the data frame to which we want to add columns
- `col` : a position at which we want to insert a column, passed as an integer
  or a column name (a string or a `Symbol`); the column selected with `col`
  and columns following it are shifted to the right in `df` after the operation
- `name` : the name of the new column
- `val` : an `AbstractVector` giving the contents of the new column or a value of any
  type other than `AbstractArray` which will be repeated to fill a new vector;
  As a particular rule a values stored in a `Ref` or a `0`-dimensional `AbstractArray`
  are unwrapped and treated in the same way
- `after` : if `true` columns are inserted after `col`
- `makeunique` : defines what to do if `name` already exists in `df`;
  if it is `false` an error will be thrown; if it is `true` a new unique name will
  be generated by adding a suffix
- `copycols` : whether vectors passed as columns should be copied

If `val` is an `AbstractRange` then the result of `collect(val)` is inserted.

If `df` is a `SubDataFrame` then it must have been created with `:` as column selector
(otherwise an error is thrown). In this case the `copycols` keyword argument
is ignored (i.e. the added column is always copied) and the parent data frame's
column is filled with `missing` in rows that are filtered out by `df`.

If `df` isa `DataFrame` that has no columns and only values
other than `AbstractVector` are passed then it is used to create a one-element
column.
If `df` isa `DataFrame` that has no columns and at least one `AbstractVector` is
passed then its length is used to determine the number of elements in all
created columns.
In all other cases the number of rows in all created columns must match
`nrow(df)`.
.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3)
3×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> insertcols!(df, 1, :b => 'a':'c')
3×2 DataFrame
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols!(df, 2, :c => 2:4, :c => 3:5, makeunique=true)
3×4 DataFrame
 Row │ b     c      c_1    a
     │ Char  Int64  Int64  Int64
─────┼───────────────────────────
   1 │ a         2      3      1
   2 │ b         3      4      2
   3 │ c         4      5      3

julia> insertcols!(df, :b, :d => 7:9, after=true)
3×5 DataFrame
 Row │ b     d      c      c_1    a
     │ Char  Int64  Int64  Int64  Int64
─────┼──────────────────────────────────
   1 │ a         7      2      3      1
   2 │ b         8      3      4      2
   3 │ c         9      4      5      3
```
"""
function insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{Symbol, <:Any}...;
                     after::Bool=false, makeunique::Bool=false, copycols::Bool=true)
    if !is_column_insertion_allowed(df)
        throw(ArgumentError("insertcols! is only supported for DataFrame, or for " *
                            "SubDataFrame created with `:` as column selector"))
    end
    if !(copycols || df isa DataFrame)
        throw(ArgumentError("copycols=false is only allowed if df isa DataFrame "))
    end
    if col isa SymbolOrString
        col_ind = Int(columnindex(df, col))
        if col_ind == 0
            throw(ArgumentError("column $col does not exist in data frame"))
        end
    else
        col_ind = Int(col)
    end

    if after
        col_ind += 1
    end

    if !(0 < col_ind <= ncol(df) + 1)
        throw(ArgumentError("attempt to insert a column to a data frame with " *
                            "$(ncol(df)) columns at index $col_ind"))
    end

    if !makeunique
        if !allunique(first.(name_cols))
            throw(ArgumentError("Names of columns to be inserted into a data frame " *
                                "must be unique when `makeunique=true`"))
        end
        for (n, _) in name_cols
            if hasproperty(df, n)
                throw(ArgumentError("Column $n is already present in the data frame " *
                                    "which is not allowed when `makeunique=true`"))
            end
        end
    end

    if ncol(df) == 0 && df isa DataFrame
        target_row_count = -1
    else
        target_row_count = nrow(df)
    end

    for (n, v) in name_cols
        if v isa AbstractVector
            if target_row_count == -1
                target_row_count = length(v)
            elseif length(v) != target_row_count
                if target_row_count == nrow(df)
                    throw(DimensionMismatch("length of new column $n which is " *
                                            "$(length(v)) must match the number " *
                                            "of rows in data frame ($(nrow(df)))"))
                else
                    throw(DimensionMismatch("all vectors passed to be inserted into " *
                                            "a data frame must have the same length"))
                end
            end
        elseif v isa AbstractArray && ndims(v) > 1
            throw(ArgumentError("adding AbstractArray other than AbstractVector as " *
                                "a column of a data frame is not allowed"))
        end
    end
    if target_row_count == -1
        target_row_count = 1
    end

    for (name, item) in name_cols
        if !(item isa AbstractVector)
            if item isa Union{AbstractArray{<:Any, 0}, Ref}
                x = item[]
                item_new = fill!(Tables.allocatecolumn(typeof(x), target_row_count), x)
            else
                @assert !(item isa AbstractArray)
                item_new = fill!(Tables.allocatecolumn(typeof(item), target_row_count), item)
            end
        elseif item isa AbstractRange
            item_new = collect(item)
        elseif copycols && df isa DataFrame
            item_new = copy(item)
        else
            item_new = item
        end

        if df isa DataFrame
            dfp = df
        else
            @assert df isa SubDataFrame
            dfp = parent(df)
            item_new_orig = item_new
            T = eltype(item_new_orig)
            item_new = similar(item_new_orig, Union{T, Missing}, nrow(dfp))
            fill!(item_new, missing)
            item_new[rows(df)] = item_new_orig
        end

        firstindex(item_new) != 1 && _onebased_check_error()

        if ncol(dfp) == 0
            dfp[!, name] = item_new
        else
            if hasproperty(dfp, name)
                @assert makeunique
                k = 1
                while true
                    nn = Symbol("$(name)_$k")
                    if !hasproperty(dfp, nn)
                        name = nn
                        break
                    end
                    k += 1
                end
            end
            insert!(index(dfp), col_ind, name)
            insert!(_columns(dfp), col_ind, item_new)
        end
        col_ind += 1
    end
    return df
end

insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{<:AbstractString, <:Any}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, col, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{Symbol, <:Any}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, ncol(df)+1, name_cols..., after=after,
                makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{<:AbstractString, <:Any}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

function insertcols!(df::AbstractDataFrame, col::Int=ncol(df)+1; makeunique::Bool=false, name_cols...)
    if !(0 < col <= ncol(df) + 1)
        throw(ArgumentError("attempt to insert a column to a data frame with " *
                            "$(ncol(df)) columns at index $col"))
    end
    if !isempty(name_cols)
        # an explicit error is thrown as keyword argument was supported in the past
        throw(ArgumentError("inserting colums using a keyword argument is not supported, " *
                            "pass a Pair as a positional argument instead"))
    end
    return df
end

"""
    copy(df::DataFrame; copycols::Bool=true)

Copy data frame `df`.
If `copycols=true` (the default), return a new  `DataFrame` holding
copies of column vectors in `df`.
If `copycols=false`, return a new `DataFrame` sharing column vectors with `df`.
"""
function Base.copy(df::DataFrame; copycols::Bool=true)
    return DataFrame(copy(_columns(df)), copy(index(df)), copycols=copycols)
end

"""
    deleteat!(df::DataFrame, inds)

Delete rows specified by `inds` from a `DataFrame` `df` in place and return it.

Internally `deleteat!` is called for all columns so `inds` must be:
a vector of sorted and unique integers, a boolean vector, an integer,
or `Not` wrapping any valid selector.

# Examples
```jldoctest
julia> df = DataFrame(a=1:3, b=4:6)
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> deleteat!(df, 2)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      6
```
"""
function Base.deleteat!(df::DataFrame, inds)
    if !isempty(inds) && size(df, 2) == 0
        throw(BoundsError(df, (inds, :)))
    end

    # workaround https://github.com/JuliaLang/julia/pull/41646
    if VERSION <= v"1.6.2" && inds isa UnitRange{<:Integer}
        inds = collect(inds)
    end

    # we require ind to be stored and unique like in Base
    # otherwise an error will be thrown and the data frame will get corrupted
    return _deleteat!_helper(df, inds)
end

function Base.deleteat!(df::DataFrame, inds::AbstractVector{Bool})
    if length(inds) != size(df, 1)
        throw(BoundsError(df, (inds, :)))
    end
    drop = _findall(inds)
    # workaround https://github.com/JuliaLang/julia/pull/41646
    if VERSION <= v"1.6.2" && drop isa UnitRange{<:Integer}
        drop = collect(drop)
    end
    return _deleteat!_helper(df, drop)
end

Base.deleteat!(df::DataFrame, inds::Not) = deleteat!(df, axes(df, 1)[inds])

function _deleteat!_helper(df::DataFrame, drop)
    cols = _columns(df)
    isempty(cols) && return df

    n = nrow(df)
    col1 = cols[1]
    deleteat!(col1, drop)
    newn = length(col1)

    for i in 2:length(cols)
        col = cols[i]
        if length(col) == n
            deleteat!(col, drop)
        end
    end

    for i in 1:length(cols)
        # this should never happen, but we add it for safety
        @assert length(cols[i]) == newn corrupt_msg(df, i)
    end

    return df
end

"""
    empty!(df::DataFrame)

Remove all rows from `df`, making each of its columns empty.
"""
function Base.empty!(df::DataFrame)
    foreach(empty!, eachcol(df))
    return df
end

##############################################################################
##
## hcat!
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

# TODO: after deprecation remove AbstractVector methods

function hcat!(df::DataFrame, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true)
    Base.depwarn("horizontal concatenation of data frame with a vector is deprecated. " *
                 "Pass DataFrame(x1=x) instead.", :hcat!)
    return hcat!(df, DataFrame(AbstractVector[x], [:x1], copycols=false),
                 makeunique=makeunique, copycols=copycols)
end

function hcat!(x::AbstractVector, df::DataFrame; makeunique::Bool=false, copycols::Bool=true)
    Base.depwarn("horizontal concatenation of data frame with a vector is deprecated. " *
                 "Pass DataFrame(x1=x) instead.", :hcat!)
    return hcat!(DataFrame(AbstractVector[x], [:x1], copycols=copycols), df,
                 makeunique=makeunique, copycols=copycols)
end

# hcat! for 1-n arguments
hcat!(df::DataFrame; makeunique::Bool=false, copycols::Bool=true) = df
hcat!(a::DataFrame, b::Union{AbstractDataFrame, AbstractVector},
      c::Union{AbstractDataFrame, AbstractVector}...;
      makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat!(a, b, makeunique=makeunique, copycols=copycols),
          c..., makeunique=makeunique, copycols=copycols)

##############################################################################
##
## Missing values support
##
##############################################################################
"""
    allowmissing!(df::DataFrame, cols=:)

Convert columns `cols` of data frame `df` from element type `T` to
`Union{T, Missing}` to support missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.
"""
function allowmissing! end

function allowmissing!(df::DataFrame, col::ColumnIndex)
    df[!, col] = allowmissing(df[!, col])
    return df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex})
    for col in cols
        allowmissing!(df, col)
    end
    return df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{Bool})
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(df, col)
    end
    return df
end

allowmissing!(df::DataFrame, cols::MultiColumnIndex) =
    allowmissing!(df, index(df)[cols])

allowmissing!(df::DataFrame, cols::Colon=:) =
    allowmissing!(df, axes(df, 2))

"""
    disallowmissing!(df::DataFrame, cols=:; error::Bool=true)

Convert columns `cols` of data frame `df` from element type `Union{T, Missing}` to
`T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.
"""
function disallowmissing! end

function disallowmissing!(df::DataFrame, col::ColumnIndex; error::Bool=true)
    x = df[!, col]
    if Missing <: eltype(x)
        try
            df[!, col] = disallowmissing(x)
        catch e
            row = findfirst(ismissing, x)
            if row !== nothing
                if error
                    col_name = only(names(df, col))
                    throw(ArgumentError("Missing value found in column " *
                                        ":$col_name in row $row"))
                end
            else
                rethrow(e)
            end
        end
    end
    return df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex};
                          error::Bool=true)
    for col in cols
        disallowmissing!(df, col, error=error)
    end
    return df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{Bool}; error::Bool=true)
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(df, col, error=error)
    end
    return df
end

disallowmissing!(df::DataFrame, cols::MultiColumnIndex; error::Bool=true) =
    disallowmissing!(df, index(df)[cols], error=error)

disallowmissing!(df::DataFrame, cols::Colon=:; error::Bool=true) =
    disallowmissing!(df, axes(df, 2), error=error)

"""
    append!(df::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))
    append!(df::DataFrame, table; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))

Add the rows of `df2` to the end of `df`. If the second argument `table` is not an
`AbstractDataFrame` then it is converted using `DataFrame(table, copycols=false)`
before being appended.

The exact behavior of `append!` depends on the `cols` argument:
* If `cols == :setequal` (this is the default)
  then `df2` must contain exactly the same columns as `df` (but possibly in a
  different order).
* If `cols == :orderequal` then `df2` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(df)` to allow for support of ordered dicts; however, if `df2`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `df2` may contain more columns than `df`, but all
  column names that are present in `df` must be present in `df2` and only these
  are used.
* If `cols == :subset` then `append!` behaves like for `:intersect` but if some
  column is missing in `df2` then a `missing` value is pushed to `df`.
* If `cols == :union` then `append!` adds columns missing in `df` that are present
  in `df2`, for columns present in `df` but missing in `df2` a `missing` value
  is pushed.

If `promote=true` and element type of a column present in `df` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

The above rule has the following exceptions:
* If `df` has no columns then copies of columns from `df2` are added to it.
* If `df2` has no columns then calling `append!` leaves `df` unchanged.

Please note that `append!` must not be used on a `DataFrame` that contains
columns that are aliases (equal when compared with `===`).

# See also

Use [`push!`](@ref) to add individual rows to a data frame and [`vcat`](@ref)
to vertically concatenate data frames.

# Examples
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4.0:6.0, B=4:6)
3×2 DataFrame
 Row │ A        B
     │ Float64  Int64
─────┼────────────────
   1 │     4.0      4
   2 │     5.0      5
   3 │     6.0      6

julia> append!(df1, df2);

julia> df1
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6
```
"""
function Base.append!(df1::DataFrame, df2::AbstractDataFrame; cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    if ncol(df1) == 0
        for (n, v) in pairs(eachcol(df2))
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
                                " do not have the same names in both passed " *
                                "data frames and `cols == :orderequal`"))
        else
            mismatchmsg = " Column names :" *
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data frames " *
                                "and `cols == :orderequal`"))
        end
    elseif cols == :setequal
        wrongnames = symdiff(_names(df1), _names(df2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data frames " *
                                "and `cols == :setequal`"))
        end
    elseif cols == :intersect
        wrongnames = setdiff(_names(df1), _names(df2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only in destination data frame " *
                                "and `cols == :intersect`"))
        end
    end

    nrows, ncols = size(df1)
    targetrows = nrows + nrow(df2)
    current_col = 0
    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `DataFrame` is internally
    # inconsistent and normal data frame indexing would error.
    try
        for (j, n) in enumerate(_names(df1))
            current_col += 1
            if hasproperty(df2, n)
                df2_c = df2[!, n]
                S = eltype(df2_c)
                df1_c = df1[!, j]
                T = eltype(df1_c)
                if S <: T || !promote || promote_type(S, T) <: T
                    # if S <: T || promote_type(S, T) <: T this should never throw an exception
                    append!(df1_c, df2_c)
                else
                    newcol = similar(df1_c, promote_type(S, T), targetrows)
                    copyto!(newcol, 1, df1_c, 1, nrows)
                    copyto!(newcol, nrows+1, df2_c, 1, targetrows - nrows)
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(df1)[j] = newcol
                end
            else
                if Missing <: eltype(df1[!, j])
                    resize!(df1[!, j], targetrows)
                    df1[nrows+1:targetrows, j] .= missing
                elseif promote
                    newcol = similar(df1[!, j], Union{Missing, eltype(df1[!, j])},
                                     targetrows)
                    copyto!(newcol, 1, df1[!, j], 1, nrows)
                    newcol[nrows+1:targetrows] .= missing
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(df1)[j] = newcol
                else
                    throw(ArgumentError("promote=false and source data frame does " *
                                        "not contain column :$n, while destination " *
                                        "column does not allow for missing values"))
                end
            end
        end
        current_col = 0
        for col in _columns(df1)
            current_col += 1
            @assert length(col) == targetrows
        end
        if cols == :union
            for n in setdiff(_names(df2), _names(df1))
                newcol = similar(df2[!, n], Union{Missing, eltype(df2[!, n])},
                                 targetrows)
                @inbounds newcol[1:nrows] .= missing
                copyto!(newcol, nrows+1, df2[!, n], 1, targetrows - nrows)
                df1[!, n] = newcol
            end
        end
    catch err
        # Undo changes in case of error
        for col in _columns(df1)
            resize!(col, nrows)
        end
        @error "Error adding value to column :$(_names(df1)[current_col])."
        rethrow(err)
    end
    return df1
end

function Base.push!(df::DataFrame, row::Union{AbstractDict, NamedTuple};
                    cols::Symbol=:setequal,
                    promote::Bool=(cols in [:union, :subset]))
    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows, ncols = size(df)
    targetrows = nrows + 1

    if ncols == 0 && row isa NamedTuple
        for (n, v) in pairs(row)
            setproperty!(df, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
        end
        return df
    end

    old_row_type = typeof(row)
    if row isa AbstractDict && keytype(row) !== Symbol &&
        (keytype(row) <: AbstractString || all(x -> x isa AbstractString, keys(row)))
        row = (;(Symbol.(keys(row)) .=> values(row))...)
    end

    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `DataFrame` is internally
    # inconsistent and normal data frame indexing would error.
    if cols == :union
        if row isa AbstractDict && keytype(row) !== Symbol && !all(x -> x isa Symbol, keys(row))
            throw(ArgumentError("when `cols == :union` all keys of row must be Symbol"))
        end
        for (i, colname) in enumerate(_names(df))
            col = _columns(df)[i]
            if haskey(row, colname)
                val = row[colname]
            else
                val = missing
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || promote_type(S, T) <: T
                push!(col, val)
            elseif !promote
                try
                    push!(col, val)
                catch err
                    for col in _columns(df)
                        resize!(col, nrows)
                    end
                    @error "Error adding value to column :$colname."
                    rethrow(err)
                end
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(df)[i] = newcol
            end
        end
        for (colname, col) in zip(_names(df), _columns(df))
            if length(col) != targetrows
                for col2 in _columns(df)
                    resize!(col2, nrows)
                end
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        for colname in setdiff(keys(row), _names(df))
            val = row[colname]
            S = typeof(val)
            if nrows == 0
                newcol = [val]
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
                newcol[end] = val
            end
            df[!, colname] = newcol
        end
        return df
    end

    if cols == :orderequal
        if old_row_type <: Dict
            throw(ArgumentError("passing `Dict` as `row` when `cols == :orderequal` " *
                                "is not allowed as it is unordered"))
        elseif length(row) != ncol(df) || any(x -> x[1] != x[2], zip(keys(row), _names(df)))
            throw(ArgumentError("when `cols == :orderequal` pushed row must " *
                                "have the same column names and in the " *
                                "same order as the target data frame"))
        end
    elseif cols === :setequal
        # Only check for equal lengths if :setequal is selected,
        # as an error will be thrown below if some names don't match
        if length(row) != ncols
            # an explicit error is thrown as this was allowed in the past
            throw(ArgumentError("`push!` with `cols` equal to `:setequal` " *
                                "requires `row` to have the same number of elements " *
                                "as the number of columns in `df`."))
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
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(df)[columnindex(df, nm)] = newcol
            end
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
        @error "Error adding value to column :$(_names(df)[current_col])."
        rethrow(err)
    end
    return df
end

"""
    push!(df::DataFrame, row::Union{Tuple, AbstractArray}; promote::Bool=false)
    push!(df::DataFrame, row::Union{DataFrameRow, NamedTuple, AbstractDict};
          cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

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
* If `cols == :setequal` (this is the default)
  then `row` must contain exactly the same columns as `df` (but possibly in a
  different order).
* If `cols == :orderequal` then `row` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(df)` to allow for support of ordered dicts; however, if `row`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `row` may contain more columns than `df`,
  but all column names that are present in `df` must be present in `row` and only
  they are used to populate a new row in `df`.
* If `cols == :subset` then `push!` behaves like for `:intersect` but if some
  column is missing in `row` then a `missing` value is pushed to `df`.
* If `cols == :union` then columns missing in `df` that are present in `row` are
  added to `df` (using `missing` for existing rows) and a `missing` value is
  pushed to columns missing in `row` that are present in `df`.

If `promote=true` and element type of a column present in `df` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

As a special case, if `df` has no columns and `row` is a `NamedTuple` or
`DataFrameRow`, columns are created for all values in `row`, using their names
and order.

Please note that `push!` must not be used on a `DataFrame` that contains columns
that are aliases (equal when compared with `===`).

# Examples
```jldoctest
julia> df = DataFrame(A=1:3, B=1:3);

julia> push!(df, (true, false))
4×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0

julia> push!(df, df[1, :])
5×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1

julia> push!(df, (C="something", A=true, B=false), cols=:intersect)
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1
   6 │     1      0

julia> push!(df, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 DataFrame
 Row │ A        B        C
     │ Float64  Int64?   Float64?
─────┼─────────────────────────────
   1 │     1.0        1  missing
   2 │     2.0        2  missing
   3 │     3.0        3  missing
   4 │     1.0        0  missing
   5 │     1.0        1  missing
   6 │     1.0        0  missing
   7 │     1.0  missing        1.0

julia> push!(df, NamedTuple(), cols=:subset)
8×3 DataFrame
 Row │ A          B        C
     │ Float64?   Int64?   Float64?
─────┼───────────────────────────────
   1 │       1.0        1  missing
   2 │       2.0        2  missing
   3 │       3.0        3  missing
   4 │       1.0        0  missing
   5 │       1.0        1  missing
   6 │       1.0        0  missing
   7 │       1.0  missing        1.0
   8 │ missing    missing  missing
```
"""
function Base.push!(df::DataFrame, row::Any; promote::Bool=false)
    if !(row isa Union{Tuple, AbstractArray})
        # an explicit error is thrown as this was allowed in the past
        throw(ArgumentError("`push!` does not allow passing collections of type " *
                            "$(typeof(row)) to be pushed into a DataFrame. Only " *
                            "`Tuple`, `AbstractArray`, `AbstractDict`, `DataFrameRow` " *
                            "and `NamedTuple` are allowed."))
    end
    nrows, ncols = size(df)
    targetrows = nrows + 1
    if length(row) != ncols
        msg = "Length of `row` does not match `DataFrame` column count."
        throw(DimensionMismatch(msg))
    end
    current_col = 0
    try
        for (i, (col, val)) in enumerate(zip(_columns(df), row))
            current_col += 1
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(df)[i] = newcol
            end
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
        @error "Error adding value to column :$(_names(df)[current_col])."
        rethrow(err)
    end
    df
end

"""
    repeat!(df::DataFrame; inner::Integer=1, outer::Integer=1)

Update a data frame `df` in-place by repeating its rows. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated. Columns of `df` are freshly allocated.

# Example
```jldoctest
julia> df = DataFrame(a=1:2, b=3:4)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat!(df, inner=2, outer=3);

julia> df
12×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     1      3
   3 │     2      4
   4 │     2      4
   5 │     1      3
   6 │     1      3
   7 │     2      4
   8 │     2      4
   9 │     1      3
  10 │     1      3
  11 │     2      4
  12 │     2      4
```
"""
function repeat!(df::DataFrame; inner::Integer=1, outer::Integer=1)
    inner < 0 && throw(ArgumentError("inner keyword argument must be non-negative"))
    outer < 0 && throw(ArgumentError("outer keyword argument must be non-negative"))
    return mapcols!(x -> repeat(x, inner = Int(inner), outer = Int(outer)), df)
end

"""
    repeat!(df::DataFrame, count::Integer)

Update a data frame `df` in-place by repeating its rows the number of times
specified by `count`. Columns of `df` are freshly allocated.

# Example
```jldoctest
julia> df = DataFrame(a=1:2, b=3:4)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat(df, 2)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4
   3 │     1      3
   4 │     2      4
```
"""
function repeat!(df::DataFrame, count::Integer)
    count < 0 && throw(ArgumentError("count must be non-negative"))
    return mapcols!(x -> repeat(x, Int(count)), df)
end

# This is not exactly copy! as in general we allow axes to be different
function _replace_columns!(df::DataFrame, newdf::DataFrame)
    @assert ncol(newdf) == 0 || nrow(df) == nrow(newdf)
    copy!(_columns(df), _columns(newdf))
    copy!(_names(index(df)), _names(newdf))
    copy!(index(df).lookup, index(newdf).lookup)
    return df
end
