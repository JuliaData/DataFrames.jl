"""
    DataFrame <: AbstractDataFrame

An `AbstractDataFrame` that stores a set of named columns.

The columns are normally `AbstractVector`s stored in memory,
particularly a `Vector`, `PooledVector` or `CategoricalVector`.

# Constructors
```julia
DataFrame(pairs::Pair...; makeunique::Bool=false, copycols::Bool=true)
DataFrame(pairs::AbstractVector{<:Pair}; makeunique::Bool=false, copycols::Bool=true)
DataFrame(ds::AbstractDict; copycols::Bool=true)
DataFrame(; kwargs..., copycols::Bool=true)

DataFrame(table; copycols::Union{Bool, Nothing}=nothing)
DataFrame(table, names::AbstractVector;
          makeunique::Bool=false, copycols::Union{Bool, Nothing}=nothing)
DataFrame(columns::AbstractVecOrMat, names::AbstractVector;
          makeunique::Bool=false, copycols::Bool=true)

DataFrame(::DataFrameRow; copycols::Bool=true)
DataFrame(::GroupedDataFrame; copycols::Bool=true, keepkeys::Bool=true)
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

If two positional arguments are passed, where the second argument is an
`AbstractVector`, then the first argument is taken to be a table as described in
the previous paragraph, and columns names of the resulting data frame
are taken from the vector passed as the second positional argument.

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

Metadata: this function preserves all table and column-level metadata.
As a special case if a `GroupedDataFrame` is passed then
only `:note`-style metadata from parent of the `GroupedDataFrame` is preserved.

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
mutable struct DataFrame <: AbstractDataFrame
    columns::Vector{AbstractVector}
    colindex::Index
    metadata::Union{Nothing, Dict{String, Tuple{Any, Any}}}
    colmetadata::Union{Nothing, Dict{Int, Dict{String, Tuple{Any, Any}}}}
    # This is a helper field for optimizing performance of
    # _drop_all_nonnote_metadata! and _drop_table_nonnote_metadata!
    # so that if we only have :note-style metadata these functions are no-op.
    # The contract is that if allnotemetadata=true then it is guaranteed that
    # there are only :note-style metadata entries in the data frame.
    # metadata! and colmetadata! functions appropriately set this field if a
    # non-:note-style metadata is added.
    allnotemetadata::Bool

    # the inner constructor should not be used directly
    function DataFrame(columns::Union{Vector{Any}, Vector{AbstractVector}},
                       colindex::Index; copycols::Bool=true)
        if length(columns) == length(colindex) == 0
            return new(AbstractVector[], Index(), nothing, nothing, true)
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
        if copycols && len >= 100_000 && length(columns) > 1 && Threads.nthreads() > 1
            @sync for i in eachindex(columns)
                @spawn columns[i] = _preprocess_column(columns[i], len, copycols)
            end
        else
            for i in eachindex(columns)
                columns[i] = _preprocess_column(columns[i], len, copycols)
            end
        end

        for (i, col) in enumerate(columns)
            firstindex(col) != 1 && _onebased_check_error(i, col)
        end

        return new(convert(Vector{AbstractVector}, columns), colindex, nothing, nothing, true)
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
    if d isa Dict
        select!(df, sort!(propertynames(df)))
    else
        # AbstractDict can potentially implement Tables.jl table interface
        _copy_all_all_metadata!(df, d)
    end
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
        return rename!(DataFrame(columns, copycols=copycols), cnames, makeunique=makeunique)
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

DataFrame(column_eltypes::AbstractVector{<:Type}, cnames::AbstractVector{Symbol},
          nrows::Integer=0; makeunique::Bool=false) =
    throw(ArgumentError("`DataFrame` constructor with passed eltypes is " *
                        "not supported. Pass explicitly created columns to a " *
                        "`DataFrame` constructor instead."))

DataFrame(column_eltypes::AbstractVector{<:Type}, cnames::AbstractVector{<:AbstractString},
          nrows::Integer=0; makeunique::Bool=false) =
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

"""
    nrow(df::AbstractDataFrame)

Return the number of rows in an `AbstractDataFrame` `df`.

See also: [`ncol`](@ref), [`size`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:10, x=rand(10), y=rand(["a", "b", "c"], 10));

julia> nrow(df)
10
```
""" # note: these type assertions are required to pass tests
nrow(df::DataFrame) = ncol(df) > 0 ? length(_columns(df)[1])::Int : 0

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
    return @inbounds cols[col_ind][row_ind]
end

@inline function Base.getindex(df::DataFrame, row_ind::Integer,
                               col_ind::SymbolOrString)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_ind)
        throw(BoundsError(df, (row_ind, col_ind)))
    end
    return @inbounds _columns(df)[selected_column][row_ind]
end

# df[MultiRowIndex, SingleColumnIndex] => AbstractVector, copy
@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, col_ind)))
    end
    return @inbounds _columns(df)[selected_column][row_inds]
end

@inline Base.getindex(df::DataFrame, row_inds::Not, col_ind::ColumnIndex) =
    df[axes(df, 1)[row_inds], col_ind]

# df[:, SingleColumnIndex] => AbstractVector
function Base.getindex(df::DataFrame, ::Colon, col_ind::ColumnIndex)
    selected_column = index(df)[col_ind]
    return copy(_columns(df)[selected_column])
end

# df[!, SingleColumnIndex] => AbstractVector, the same vector
@inline function Base.getindex(df::DataFrame, ::typeof(!), col_ind::Union{Signed, Unsigned})
    cols = _columns(df)
    @boundscheck if !checkindex(Bool, axes(cols, 1), col_ind)
        throw(BoundsError(df, (!, col_ind)))
    end
    return @inbounds cols[col_ind]
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
    if length(selected_rows) >= 100_000 && Threads.nthreads() > 1
        new_columns = Vector{AbstractVector}(undef, length(selected_columns))
        @sync for i in eachindex(new_columns)
            @spawn new_columns[i] = df_columns[selected_columns[i]][selected_rows]
        end
        return DataFrame(new_columns, idx, copycols=false)
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
        new_df = DataFrame(AbstractVector[_columns(df)[selected_columns[1]][row_inds]],
                           idx, copycols=false)
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        new_df = _threaded_getindex(selected_rows, selected_columns, _columns(df), idx)
    end

    _copy_all_note_metadata!(new_df, df)
    return new_df
end

@inline function Base.getindex(df::DataFrame, row_inds::AbstractVector{T}, ::Colon) where T
    @boundscheck if !checkindex(Bool, axes(df, 1), row_inds)
        throw(BoundsError(df, (row_inds, :)))
    end
    idx = copy(index(df))

    if ncol(df) == 1
        new_df = DataFrame(AbstractVector[_columns(df)[1][row_inds]], idx, copycols=false)
    else
        # Computing integer indices once for all columns is faster
        selected_rows = T === Bool ? _findall(row_inds) : row_inds
        new_df = _threaded_getindex(selected_rows, 1:ncol(df), _columns(df), idx)
    end

    _copy_all_note_metadata!(new_df, df)
    return new_df
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
    _drop_all_nonnote_metadata!(df)
    return dv
end

function insert_single_entry!(df::DataFrame, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        _columns(df)[index(df)[col_ind]][row_ind] = v
        _drop_all_nonnote_metadata!(df)
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
            df[!, i][row_ind] = x
        end
        _drop_all_nonnote_metadata!(df)
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
        _drop_all_nonnote_metadata!(df)
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
            df[!, col][row_inds] = new_df[!, j]
        end
        _drop_all_nonnote_metadata!(df)
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
            # this will drop metadata appropriately
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
            # this will drop metadata appropriately
            df[row_inds, col] = (row_inds === !) ? mx[:, j] : view(mx, :, j)
        end
        return df
    end
end

"""
    copy(df::DataFrame; copycols::Bool=true)

Copy data frame `df`.
If `copycols=true` (the default), return a new  `DataFrame` holding
copies of column vectors in `df`.
If `copycols=false`, return a new `DataFrame` sharing column vectors with `df`.

Metadata: this function preserves all table-level and column-level metadata.
"""
function Base.copy(df::DataFrame; copycols::Bool=true)
    cdf = DataFrame(copy(_columns(df)), copy(index(df)), copycols=copycols)

    df_metadata = getfield(df, :metadata)
    if !isnothing(df_metadata)
        setfield!(cdf, :metadata, copy(df_metadata))
    end
    df_colmetadata = getfield(df, :colmetadata)
    if !isnothing(df_colmetadata)
        cdf_colmetadata = copy(df_colmetadata)
        map!(copy, values(cdf_colmetadata))
        setfield!(cdf, :colmetadata, cdf_colmetadata)
    end
    setfield!(cdf, :allnotemetadata, getfield(df, :allnotemetadata))

    return cdf
end

"""
    deleteat!(df::DataFrame, inds)

Delete rows specified by `inds` from a `DataFrame` `df` in place and return it.

Internally `deleteat!` is called for all columns so `inds` must be:
a vector of sorted and unique integers, a boolean vector, an integer,
or `Not` wrapping any valid selector.

$METADATA_FIXED

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
Base.deleteat!(df::DataFrame, inds)

Base.deleteat!(df::DataFrame, ::Colon) = empty!(df)

function Base.deleteat!(df::DataFrame, inds::Integer)
    inds isa Bool && throw(ArgumentError("Invalid index of type Bool"))
    size(df, 2) == 0 && throw(BoundsError(df, (inds, :)))
    return _deleteat!_helper(df, Int[inds])
end

function Base.deleteat!(df::DataFrame, inds::AbstractVector)
    if isempty(inds)
        _drop_all_nonnote_metadata!(df)
        return df
    elseif size(df, 2) == 0
        throw(BoundsError(df, (inds, :)))
    end

    if Bool <: eltype(inds) && any(x -> x isa Bool, inds)
        throw(ArgumentError("invalid index of type Bool"))
    end

    if !(eltype(inds) <: Integer || all(x -> x isa Integer, inds))
        throw(ArgumentError("unsupported index $inds"))
    end

    # workaround https://github.com/JuliaLang/julia/pull/41646
    if VERSION <= v"1.6.2" && inds isa UnitRange{<:Integer}
        inds = collect(inds)
    end

    if !issorted(inds, lt=<=)
        throw(ArgumentError("Indices passed to deleteat! must be unique and sorted"))
    end

    return _deleteat!_helper(df, inds)
end

function Base.deleteat!(df::DataFrame, inds::AbstractVector{Bool})
    if length(inds) != size(df, 1)
        throw(BoundsError(df, (inds, :)))
    end
    # workaround https://github.com/JuliaLang/julia/pull/41646
    if VERSION <= v"1.6.2" && drop isa UnitRange{<:Integer}
        inds = collect(inds)
    end
    return _deleteat!_helper(df, inds)
end

Base.deleteat!(df::DataFrame, inds::Not) =
    _deleteat!_helper(df, axes(df, 1)[inds])

function _deleteat!_helper(df::DataFrame, drop)
    cols = _columns(df)
    if isempty(cols)
        _drop_all_nonnote_metadata!(df)
        return df
    end

    if any(c -> c === drop || Base.mightalias(c, drop), cols)
        drop = copy(drop)
    end

    n = nrow(df)
    col1 = cols[1]
    deleteat!(col1, drop)
    newn = length(col1)
    @assert newn <= n
    # the 0.06 threshold is heuristic; based on tests
    # the assignment makes the code type-unstable but it is a small union
    # so the overhead should be small
    if drop isa AbstractVector{Bool} && newn < 0.06 * n
        drop = findall(drop)
    end
    for i in 2:length(cols)
        col = cols[i]
        # this check is done to handle column aliases
        if length(col) == n
            deleteat!(col, drop)
        end
    end

    for i in 1:length(cols)
        # this should never happen, but we add it for safety
        @assert length(cols[i]) == newn corrupt_msg(df, i)
    end

    _drop_all_nonnote_metadata!(df)
    return df
end

"""
    keepat!(df::DataFrame, inds)

Delete rows at all indices not specified by `inds` from a `DataFrame` `df`
in place and return it.

Internally `deleteat!` is called for all columns so `inds` must be:
a vector of sorted and unique integers, a boolean vector, an integer,
or `Not` wrapping any valid selector.

$METADATA_FIXED

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

julia> keepat!(df, [1, 3])
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      6
```
"""
keepat!(df::DataFrame, inds)

function keepat!(df::DataFrame, ::Colon)
    _drop_all_nonnote_metadata!(df)
    return df
end

function keepat!(df::DataFrame, inds::AbstractVector)
    isempty(inds) && return empty!(df)

    # this is required because of https://github.com/JuliaData/InvertedIndices.jl/issues/31
    if !((eltype(inds) <: Integer) || all(x -> x isa Integer, inds))
        throw(ArgumentError("unsupported index $inds"))
    end

    if Bool <: eltype(inds) && any(x -> x isa Bool, inds)
        throw(ArgumentError("invalid index of type Bool"))
    end

    if !issorted(inds, lt=<=)
        throw(ArgumentError("Indices passed to keepat! must be unique and sorted"))
    end

    return deleteat!(df, Not(inds))
end

function keepat!(df::DataFrame, inds::Integer)
    inds isa Bool && throw(ArgumentError("Invalid index of type Bool"))
    return deleteat!(df, Not(Int[inds]))
end

keepat!(df::DataFrame, inds::AbstractVector{Bool}) = deleteat!(df, .!inds)
keepat!(df::DataFrame, inds::Not) = deleteat!(df, Not(inds))

"""
    empty!(df::DataFrame)

Remove all rows from `df`, making each of its columns empty.

$METADATA_FIXED

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

julia> empty!(df)
0×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┴──────────────

julia> df.a, df.b
(Int64[], Int64[])
```

"""
function Base.empty!(df::DataFrame)
    foreach(empty!, eachcol(df))
    _drop_all_nonnote_metadata!(df)
    return df
end

"""
    resize!(df::DataFrame, n::Integer)

Resize `df` to have `n` rows by calling `resize!` on all columns of `df`.

$METADATA_FIXED

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

julia> resize!(df, 2)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
```
"""
function Base.resize!(df::DataFrame, n::Integer)
    if ncol(df) == 0 && n != 0
        throw(ArgumentError("data frame has no columns and requested number " *
                            "of rows is not zero"))
    end
    foreach(col -> resize!(col, n), eachcol(df))
    _drop_all_nonnote_metadata!(df)
    return df
end

"""
    pop!(df::DataFrame)

Remove the last row from `df` and return a `NamedTuple` created from this row.

!!! note

    Using this method for very wide data frames may lead to expensive compilation.

$METADATA_FIXED

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

julia> pop!(df)
(a = 3, b = 6)

julia> df
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
```

"""
Base.pop!(df::DataFrame) = popat!(df, nrow(df))

"""
    popfirst!(df::DataFrame)

Remove the first row from `df` and return a `NamedTuple` created from this row.

!!! note

    Using this method for very wide data frames may lead to expensive compilation.

$METADATA_FIXED

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

julia> popfirst!(df)
(a = 1, b = 4)

julia> df
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      5
   2 │     3      6
```
"""
Base.popfirst!(df::DataFrame) = popat!(df, 1)

"""
    popat!(df::DataFrame, i::Integer)

Remove the `i`-th row from `df` and return a `NamedTuple` created from this row.

!!! note

    Using this method for very wide data frames may lead to expensive compilation.

$METADATA_FIXED

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

julia> popat!(df, 2)
(a = 2, b = 5)

julia> df
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      6
```
"""
function Base.popat!(df::DataFrame, i::Integer)
    i isa Bool && throw(ArgumentError("Invalid index of type Bool"))
    nt = NamedTuple(df[i, :])
    deleteat!(df, i)
    return nt
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

    _drop_all_nonnote_metadata!(df1)
    _keep_matching_table_note_metadata!(df1, df2)
    for i in 1:length(u)
        df1[!, u[i]] = copycols ? df2[:, i] : df2[!, i]
        _copy_col_note_metadata!(df1, u[i], df2, i)
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
function hcat!(df::DataFrame; makeunique::Bool=false, copycols::Bool=true)
    _drop_all_nonnote_metadata!(df)
    return df
end

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

$METADATA_FIXED
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
    _drop_all_nonnote_metadata!(df)
    return df
end

function allowmissing!(df::DataFrame, cols::AbstractVector{Bool})
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && allowmissing!(df, col)
    end
    _drop_all_nonnote_metadata!(df)
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

$METADATA_FIXED
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
    _drop_all_nonnote_metadata!(df)
    return df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{<:ColumnIndex};
                          error::Bool=true)
    for col in cols
        disallowmissing!(df, col, error=error)
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

function disallowmissing!(df::DataFrame, cols::AbstractVector{Bool}; error::Bool=true)
    length(cols) == size(df, 2) || throw(BoundsError(df, (!, cols)))
    for (col, cond) in enumerate(cols)
        cond && disallowmissing!(df, col, error=error)
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

disallowmissing!(df::DataFrame, cols::MultiColumnIndex; error::Bool=true) =
    disallowmissing!(df, index(df)[cols], error=error)

disallowmissing!(df::DataFrame, cols::Colon=:; error::Bool=true) =
    disallowmissing!(df, axes(df, 2), error=error)

"""
    repeat!(df::DataFrame; inner::Integer=1, outer::Integer=1)

Update a data frame `df` in-place by repeating its rows. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated. Columns of `df` are freshly allocated.

$METADATA_FIXED

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
    cols = _columns(df)
    for (i, col) in enumerate(cols)
        col_new = repeat(col, inner=Int(inner), outer=Int(outer))
        firstindex(col_new) != 1 && _onebased_check_error(i, col_new)
        cols[i] = col_new
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

"""
    repeat!(df::DataFrame, count::Integer)

Update a data frame `df` in-place by repeating its rows the number of times
specified by `count`. Columns of `df` are freshly allocated.

$METADATA_FIXED

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
    cols = _columns(df)
    for (i, col) in enumerate(cols)
        col_new = repeat(col, count)
        firstindex(col_new) != 1 && _onebased_check_error(i, col_new)
        cols[i] = col_new
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

# This is not exactly copy! as in general we allow axes to be different
# Also no table metadata needs to be copied as we use _replace_columns!
# only in situations when table metadata for `df` must be left as-is
function _replace_columns!(df::DataFrame, newdf::DataFrame)
    # for DataFrame object here we do not support keep_present keyword argument
    # like for SubDataFrame because here transform! always falls back to select!
    @assert ncol(newdf) == 0 || nrow(df) == nrow(newdf)
    copy!(_columns(df), _columns(newdf))
    copy!(_names(index(df)), _names(newdf))
    copy!(index(df).lookup, index(newdf).lookup)

    emptycolmetadata!(df)
    for (col, col_keys) in colmetadatakeys(newdf)
        if hasproperty(df, col)
            for key in col_keys
                val, style = colmetadata(newdf, col, key, style=true)
                style === :note && colmetadata!(df, col, key, val, style=:note)
            end
        end
    end
    _drop_all_nonnote_metadata!(df)
    return df
end

allcombinations(::Type{DataFrame}; kwargs...) =
    isempty(kwargs) ? DataFrame() : allcombinations(DataFrame, kwargs...)

allcombinations(::Type{DataFrame}, pairs::Pair{<:AbstractString, <:Any}...) =
    allcombinations(DataFrame, (Symbol(k) => v for (k, v) in pairs)...)

"""
    allcombinations(DataFrame, pairs::Pair...)
    allcombinations(DataFrame; kwargs...)

Create a `DataFrame` from all combinations of values in passed arguments.
The first passed values vary fastest.

Arguments associating a column name with values to expand can be specified
either as `Pair`s passed as positional arguments, or as keyword arguments.
Column names must be `Symbol`s or strings and must be unique.

Column value can be a vector which is consumed as is or an object of any other
type (except `AbstractArray`). In the latter case the passed value is treated
as having length one for expansion. As a particular rule values stored in a `Ref`
or a `0`-dimensional `AbstractArray` are unwrapped and treated as having length one.

See also: [`crossjoin`](@ref) can be used to get the cartesian product
of rows from passed data frames.

# Examples

```jldoctest
julia> allcombinations(DataFrame, a=1:2, b='a':'c')
6×2 DataFrame
 Row │ a      b
     │ Int64  Char
─────┼─────────────
   1 │     1  a
   2 │     2  a
   3 │     1  b
   4 │     2  b
   5 │     1  c
   6 │     2  c

julia> allcombinations(DataFrame, "a" => 1:2, "b" => 'a':'c', "c" => "const")
6×3 DataFrame
 Row │ a      b     c
     │ Int64  Char  String
─────┼─────────────────────
   1 │     1  a     const
   2 │     2  a     const
   3 │     1  b     const
   4 │     2  b     const
   5 │     1  c     const
   6 │     2  c     const
```
"""
function allcombinations(::Type{DataFrame}, pairs::Pair{Symbol, <:Any}...)
    colnames = first.(pairs)
    if !allunique(colnames)
        throw(ArgumentError("All column names passed to allcombinations must be unique"))
    end
    colvalues = map(pairs) do p
        v = last(p)
        if v isa AbstractVector
            return v
        elseif v isa Union{AbstractArray{<:Any, 0}, Ref}
            x = v[]
            return fill!(Tables.allocatecolumn(typeof(x), 1), x)
        elseif v isa AbstractArray
            throw(ArgumentError("adding AbstractArray other than AbstractVector " *
                                "as a column of a data frame is not allowed"))
        else
            return fill!(Tables.allocatecolumn(typeof(v), 1), v)
        end
    end
    @assert length(colvalues) == length(colnames)
    @assert all(x -> x isa AbstractVector, colvalues)

    target_rows = Int(prod(x -> BigInt(length(x)), colvalues))
    out_df = DataFrame()
    inner = 1
    for (val, cname) in zip(colvalues, colnames)
        len = length(val)
        last_inner = inner
        inner *= len
        outer, remv = inner == 0 ? (0, 0) : divrem(target_rows, inner)
        @assert iszero(remv)
        out_df[!, cname] = repeat(val, inner=last_inner, outer=outer)
    end
    @assert inner == target_rows
    @assert size(out_df) == (target_rows, length(colnames))
    return out_df
end

_try_select_no_copy(df::DataFrame, cols) = select(df, cols, copycols=false)
