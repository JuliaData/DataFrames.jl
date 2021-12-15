##############################################################################
##
## Iteration: eachrow, eachcol
##
##############################################################################

# Iteration by rows
"""
    DataFrameRows{D<:AbstractDataFrame} <: AbstractVector{DataFrameRow}

Iterator over rows of an `AbstractDataFrame`,
with each row represented as a `DataFrameRow`.

A value of this type is returned by the [`eachrow`](@ref) function.
"""
struct DataFrameRows{D<:AbstractDataFrame} <: AbstractVector{DataFrameRow}
    df::D
end

Base.summary(dfrs::DataFrameRows) = "$(length(dfrs))-element DataFrameRows"
Base.summary(io::IO, dfrs::DataFrameRows) = print(io, summary(dfrs))

Base.iterate(::AbstractDataFrame) =
    error("AbstractDataFrame is not iterable. Use eachrow(df) to get a row iterator " *
          "or eachcol(df) to get a column iterator")

"""
    eachrow(df::AbstractDataFrame)

Return a `DataFrameRows` that iterates a data frame row by row,
with each row represented as a `DataFrameRow`.

Because `DataFrameRow`s have an `eltype` of `Any`, use `copy(dfr::DataFrameRow)` to obtain
a named tuple, which supports iteration and property access like a `DataFrameRow`,
but also passes information on the `eltypes` of the columns of `df`.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> eachrow(df)
4×2 DataFrameRows
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> copy.(eachrow(df))
4-element Vector{NamedTuple{(:x, :y), Tuple{Int64, Int64}}}:
 (x = 1, y = 11)
 (x = 2, y = 12)
 (x = 3, y = 13)
 (x = 4, y = 14)

julia> eachrow(view(df, [4, 3], [2, 1]))
2×2 DataFrameRows
 Row │ y      x
     │ Int64  Int64
─────┼──────────────
   1 │    14      4
   2 │    13      3
```
"""
eachrow(df::AbstractDataFrame) = DataFrameRows(df)

Base.IndexStyle(::Type{<:DataFrameRows}) = Base.IndexLinear()
Base.size(itr::DataFrameRows) = (size(parent(itr), 1), )

Base.@propagate_inbounds Base.getindex(itr::DataFrameRows, i::Int) = parent(itr)[i, :]

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DataFrameRows, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DataFrameRows, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DataFrameRows, s::Symbol) = haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DataFrameRows, s::AbstractString) = haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameRows, private::Bool=false) = propertynames(parent(itr))

# Iteration by columns

const DATAFRAMECOLUMNS_DOCSTR = """
Indexing into `DataFrameColumns` objects using integer, `Symbol` or string
returns the corresponding column (without copying).
Indexing into `DataFrameColumns` objects using a multiple column selector
returns a subsetted `DataFrameColumns` object with a new parent containing
only the selected columns (without copying).

`DataFrameColumns` supports most of the `AbstractVector` API. The key
differences are that it is read-only and that the `keys` function returns a
vector of `Symbol`s (and not integers as for normal vectors).

In particular `findnext`, `findprev`, `findfirst`, `findlast`, and `findall`
functions are supported, and in `findnext` and `findprev` functions it is allowed
to pass an integer, string, or `Symbol` as a reference index.
"""

"""
    DataFrameColumns{<:AbstractDataFrame}

A vector-like object that allows iteration over columns of an `AbstractDataFrame`.

$DATAFRAMECOLUMNS_DOCSTR
"""
struct DataFrameColumns{T<:AbstractDataFrame}
    df::T
end

Base.summary(dfcs::DataFrameColumns)= "$(length(dfcs))-element DataFrameColumns"
Base.summary(io::IO, dfcs::DataFrameColumns) = print(io, summary(dfcs))

"""
    eachcol(df::AbstractDataFrame)

Return a `DataFrameColumns` object that is a vector-like that allows iterating
an `AbstractDataFrame` column by column.

$DATAFRAMECOLUMNS_DOCSTR

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> eachcol(df)
4×2 DataFrameColumns
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> collect(eachcol(df))
2-element Vector{AbstractVector}:
 [1, 2, 3, 4]
 [11, 12, 13, 14]

julia> map(eachcol(df)) do col
           maximum(col) - minimum(col)
       end
2-element Vector{Int64}:
 3
 3

julia> sum.(eachcol(df))
2-element Vector{Int64}:
 10
 50
```
"""
eachcol(df::AbstractDataFrame) = DataFrameColumns(df)

Base.IteratorSize(::Type{<:DataFrameColumns}) = Base.HasShape{1}()
Base.size(itr::DataFrameColumns) = (size(parent(itr), 2),)

function Base.size(itr::DataFrameColumns, d::Integer)
    d != 1 && throw(ArgumentError("dimension out of range"))
    return size(itr)[1]
end

Base.ndims(::DataFrameColumns) = 1
Base.ndims(::Type{<:DataFrameColumns}) = 1

Base.length(itr::DataFrameColumns) = size(itr)[1]
Base.eltype(::Type{<:DataFrameColumns}) = AbstractVector

Base.firstindex(itr::DataFrameColumns) = 1
Base.lastindex(itr::DataFrameColumns) = length(itr)

if VERSION < v"1.6"
    Base.firstindex(itr::DataFrameColumns, i::Integer) = first(axes(itr, i))
    Base.lastindex(itr::DataFrameColumns, i::Integer) = last(axes(itr, i))
end
Base.axes(itr::DataFrameColumns, i::Integer) = Base.OneTo(size(itr, i))

Base.iterate(itr::DataFrameColumns, i::Integer=1) =
    i <= length(itr) ? (itr[i], i + 1) : nothing
Base.@propagate_inbounds Base.getindex(itr::DataFrameColumns, idx::ColumnIndex) =
    parent(itr)[!, idx]
Base.@propagate_inbounds Base.getindex(itr::DataFrameColumns, idx::MultiColumnIndex) =
    eachcol(parent(itr)[!, idx])
Base.:(==)(itr1::DataFrameColumns, itr2::DataFrameColumns) =
    parent(itr1) == parent(itr2)
Base.isequal(itr1::DataFrameColumns, itr2::DataFrameColumns) =
    isequal(parent(itr1), parent(itr2))

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DataFrameColumns, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DataFrameColumns, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DataFrameColumns, s::Symbol) =
    haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DataFrameColumns, s::AbstractString) =
    haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameColumns, private::Bool=false) =
    propertynames(parent(itr))

"""
    keys(dfc::DataFrameColumns)

Get a vector of column names of `dfc` as `Symbol`s.
"""
Base.keys(itr::DataFrameColumns) = propertynames(itr)

"""
    values(dfc::DataFrameColumns)

Get a vector of columns from `dfc`.
"""
Base.values(itr::DataFrameColumns) = collect(itr)

"""
    pairs(dfc::DataFrameColumns)

Return an iterator of pairs associating the name of each column of `dfc`
with the corresponding column vector, i.e. `name => col`
where `name` is the column name of the column `col`.
"""
Base.pairs(itr::DataFrameColumns) = Base.Iterators.Pairs(itr, keys(itr))
Base.findnext(f::Function, itr::DataFrameColumns, i::Integer) =
    findnext(f, values(itr), i)
Base.findnext(f::Function, itr::DataFrameColumns, i::Union{Symbol, AbstractString}) =
    findnext(f, values(itr), index(parent(itr))[i])
Base.findprev(f::Function, itr::DataFrameColumns, i::Integer) =
    findprev(f, values(itr), i)
Base.findprev(f::Function, itr::DataFrameColumns, i::Union{Symbol, AbstractString}) =
    findprev(f, values(itr), index(parent(itr))[i])
Base.findfirst(f::Function, itr::DataFrameColumns) =
    findfirst(f, values(itr))
Base.findlast(f::Function, itr::DataFrameColumns) =
    findlast(f, values(itr))
Base.findall(f::Function, itr::DataFrameColumns) =
    findall(f, values(itr))

Base.parent(itr::Union{DataFrameRows, DataFrameColumns}) = getfield(itr, :df)
Base.names(itr::Union{DataFrameRows, DataFrameColumns}) = names(parent(itr))
Base.names(itr::Union{DataFrameRows, DataFrameColumns}, cols) = names(parent(itr), cols)

function Base.show(io::IO, dfrs::DataFrameRows;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    df = parent(dfrs)
    title = summary ? "$(nrow(df))×$(ncol(df)) DataFrameRows" : ""
    _show(io, df; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
          summary=false, eltypes=eltypes, truncate=truncate, title=title,
          kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dfrs::DataFrameRows;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dfrs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

Base.show(dfrs::DataFrameRows;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dfrs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

function Base.show(io::IO, dfcs::DataFrameColumns;
                   allrows::Bool = !get(io, :limit, false),
                   allcols::Bool = !get(io, :limit, false),
                   rowlabel::Symbol = :Row,
                   summary::Bool = true,
                   eltypes::Bool = true,
                   truncate::Int = 32,
                   kwargs...)
    df = parent(dfcs)
    title = summary ? "$(nrow(df))×$(ncol(df)) DataFrameColumns" : ""
    _show(io, parent(dfcs); allrows=allrows, allcols=allcols, rowlabel=rowlabel,
          summary=false, eltypes=eltypes, truncate=truncate, title=title,
          kwargs...)
end

Base.show(io::IO, mime::MIME"text/plain", dfcs::DataFrameColumns;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(io, dfcs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

Base.show(dfcs::DataFrameColumns;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, dfcs; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
         summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

"""
    mapcols(f::Union{Function, Type}, df::AbstractDataFrame)

Return a `DataFrame` where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

Note that `mapcols` guarantees not to reuse the columns from `df` in the returned
`DataFrame`. If `f` returns its argument then it gets copied before being stored.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> mapcols(x -> x.^2, df)
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1    121
   2 │     4    144
   3 │     9    169
   4 │    16    196
```
"""
function mapcols(f::Union{Function, Type}, df::AbstractDataFrame)
    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for v in eachcol(df)
        fv = f(v)
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv === v ? copy(fv) : fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end
    return DataFrame(vs, _names(df), copycols=false)
end

"""
    mapcols!(f::Union{Function, Type}, df::DataFrame)

Update a `DataFrame` in-place where each column of `df` is transformed using function `f`.
`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

Note that `mapcols!` reuses the columns from `df` if they are returned by `f`.

# Examples
```jldoctest
julia> df = DataFrame(x=1:4, y=11:14)
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1     11
   2 │     2     12
   3 │     3     13
   4 │     4     14

julia> mapcols!(x -> x.^2, df);

julia> df
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1    121
   2 │     4    144
   3 │     9    169
   4 │    16    196
```
"""
function mapcols!(f::Union{Function, Type}, df::DataFrame)
    # note: `f` must return a consistent length
    ncol(df) == 0 && return df # skip if no columns

    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for v in eachcol(df)
        fv = f(v)
        if fv isa AbstractVector
            if seenscalar
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenvector = true
            push!(vs, fv isa AbstractRange ? collect(fv) : fv)
        else
            if seenvector
                throw(ArgumentError("mixing scalars and vectors in mapcols not allowed"))
            end
            seenscalar = true
            push!(vs, [fv])
        end
    end

    len_min, len_max = extrema(length(v) for v in vs)
    if len_min != len_max
        throw(DimensionMismatch("lengths of returned vectors must be identical"))
    end

    for (i, col) in enumerate(vs)
        firstindex(col) != 1 && _onebased_check_error(i, col)
    end

    @assert length(vs) == ncol(df)
    raw_columns = _columns(df)
    for i in 1:ncol(df)
        raw_columns[i] = vs[i]
    end

    return df
end
