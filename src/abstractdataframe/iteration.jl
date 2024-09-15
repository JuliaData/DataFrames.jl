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
4-element Vector{@NamedTuple{x::Int64, y::Int64}}:
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
Base.eachrow(df::AbstractDataFrame) = DataFrameRows(df)

nrow(itr::DataFrameRows) = nrow(parent(itr))
ncol(itr::DataFrameRows) = ncol(parent(itr))

Base.IndexStyle(::Type{<:DataFrameRows}) = Base.IndexLinear()
Base.size(itr::DataFrameRows) = (size(parent(itr), 1), )

Base.@propagate_inbounds Base.getindex(itr::DataFrameRows, i::Int) = parent(itr)[i, :]
Base.@propagate_inbounds Base.getindex(itr::DataFrameRows, i::CartesianIndex{1}) = itr[i[1]]
Base.@propagate_inbounds Base.getindex(itr::DataFrameRows, idx) =
    eachrow(@view parent(itr)[idx isa AbstractVector && !(eltype(idx) <: Bool) ? copy(idx) : idx, :])

# separate methods are needed due to dispatch ambiguity
Base.getproperty(itr::DataFrameRows, col_ind::Symbol) =
    getproperty(parent(itr), col_ind)
Base.getproperty(itr::DataFrameRows, col_ind::AbstractString) =
    getproperty(parent(itr), col_ind)
Compat.hasproperty(itr::DataFrameRows, s::Symbol) = haskey(index(parent(itr)), s)
Compat.hasproperty(itr::DataFrameRows, s::AbstractString) = haskey(index(parent(itr)), s)

# Private fields are never exposed since they can conflict with column names
Base.propertynames(itr::DataFrameRows, private::Bool=false) = propertynames(parent(itr))

"""
    Iterators.partition(dfr::DataFrameRows, n::Integer)

Iterate over `DataFrameRows` `dfr` `n` rows at a time, returning each block
as a `DataFrameRows` over a view of rows of parent of `dfr`.

# Examples

```jldoctest
julia> collect(Iterators.partition(eachrow(DataFrame(x=1:5)), 2))
3-element Vector{DataFrames.DataFrameRows{SubDataFrame{DataFrame, DataFrames.Index, UnitRange{Int64}}}}:
 2×1 DataFrameRows
 Row │ x
     │ Int64
─────┼───────
   1 │     1
   2 │     2
 2×1 DataFrameRows
 Row │ x
     │ Int64
─────┼───────
   1 │     3
   2 │     4
 1×1 DataFrameRows
 Row │ x
     │ Int64
─────┼───────
   1 │     5
```
"""
function Iterators.partition(dfr::DataFrameRows, n::Integer)
    n < 1 && throw(ArgumentError("cannot create partitions of length $n"))
    return Iterators.PartitionIterator(dfr, Int(n))
end

# use autodetection of eltype
Base.IteratorEltype(::Type{<:Iterators.PartitionIterator{<:DataFrameRows}}) =
    Base.EltypeUnknown()

# we do not need to be overly specific here as we rely on autodetection of eltype
# this method is needed only to override the fallback for `PartitionIterator`
Base.eltype(::Type{<:Iterators.PartitionIterator{<:DataFrameRows}}) =
    DataFrameRows

Base.IteratorSize(::Type{<:Iterators.PartitionIterator{<:DataFrameRows}}) =
    Base.HasLength()

function Base.length(itr::Iterators.PartitionIterator{<:DataFrameRows})
    l = nrow(parent(itr.c))
    return cld(l, itr.n)
end

function Base.iterate(itr::Iterators.PartitionIterator{<:DataFrameRows}, state::Int=1)
    df = parent(itr.c)
    last_idx = nrow(df)
    state > last_idx && return nothing
    r = min(state + itr.n - 1, last_idx)
    return eachrow(view(df, state:r, :)), r + 1
end

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
Base.eachcol(df::AbstractDataFrame) = DataFrameColumns(df)

nrow(itr::DataFrameColumns) = nrow(parent(itr))
ncol(itr::DataFrameColumns) = ncol(parent(itr))

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

Base.axes(itr::DataFrameColumns, i::Integer) = Base.OneTo(size(itr, i))

Base.iterate(itr::DataFrameColumns, i::Integer=1) =
    i <= length(itr) ? (itr[i], i + 1) : nothing
Base.@propagate_inbounds Base.getindex(itr::DataFrameColumns, idx::ColumnIndex) =
    parent(itr)[!, idx]
Base.@propagate_inbounds Base.getindex(itr::DataFrameColumns, idx::CartesianIndex{1}) =
    itr[idx[1]]
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

Base.haskey(itr::DataFrameColumns, col::Union{AbstractString, Symbol}) =
    columnindex(parent(itr), col) > 0
Base.haskey(itr::DataFrameColumns, col::Union{Signed, Unsigned}) =
    0 < col <= ncol(parent(itr))
Base.get(itr::DataFrameColumns, col::ColumnIndex, default) =
    haskey(itr, col) ? itr[col] : default

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
    mapcols(f::Union{Function, Type}, df::AbstractDataFrame; cols=All())

Return a `DataFrame` where each column of `df` selected by `cols` (by default, all columns)
is transformed using function `f`.
Columns not selected by `cols` are copied.

`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

The `cols` column selector can be any value accepted as column selector by the `names` function.

Note that `mapcols` guarantees not to reuse the columns from `df` in the returned
`DataFrame`. If `f` returns its argument then it gets copied before being stored.

$METADATA_FIXED

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

julia> mapcols(x -> x.^2, df, cols=r"y")
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1    121
   2 │     2    144
   3 │     3    169
   4 │     4    196
```
"""
function mapcols(f::Union{Function, Type}, df::AbstractDataFrame; cols=All())
    if cols === All() || cols === Colon()
        apply = Iterators.repeated(true)
    else
        picked = Set(names(df, cols))
        apply = Bool[name in picked for name in names(df)]
    end

    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for (v, doapply) in zip(eachcol(df), apply)
        fv = doapply ? f(v) : copy(v)
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

    new_df = DataFrame(vs, _names(df), copycols=false)
    _copy_all_note_metadata!(new_df, df)
    return new_df
end

"""
    mapcols!(f::Union{Function, Type}, df::DataFrame; cols=All())

Update a `DataFrame` in-place where each column of `df` selected by `cols` (by default, all columns)
is transformed using function `f`.
Columns not selected by `cols` are left unchanged.

`f` must return `AbstractVector` objects all with the same length or scalars
(all values other than `AbstractVector` are considered to be a scalar).

Note that `mapcols!` reuses the columns from `df` if they are returned by `f`.

$METADATA_FIXED

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

julia> mapcols!(x -> 2 * x, df, cols=r"x");

julia> df
4×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     2    121
   2 │     8    144
   3 │    18    169
   4 │    32    196
```
"""
function mapcols!(f::Union{Function,Type}, df::DataFrame; cols=All())
    if ncol(df) == 0 # skip if no columns
        _drop_all_nonnote_metadata!(df)
        return df
    end

    if cols === All() || cols === Colon()
        apply = Iterators.repeated(true)
    else
        picked = Set(names(df, cols))
        apply = Bool[name in picked for name in names(df)]
    end

    # note: `f` must return a consistent length
    vs = AbstractVector[]
    seenscalar = false
    seenvector = false
    for (v, doapply) in zip(eachcol(df), apply)
        fv = doapply ? f(v) : v
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

    _drop_all_nonnote_metadata!(df)
    return df
end

##############################################################################
##
## Reduction
##
##############################################################################

"""
    reduce(::typeof(vcat),
           dfs::Union{AbstractVector{<:AbstractDataFrame},
                      Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}};
           cols::Union{Symbol, AbstractVector{Symbol},
                       AbstractVector{<:AbstractString}}=:setequal,
           source::Union{Nothing, Symbol, AbstractString,
                         Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing,
           init::AbstractDataFrame=DataFrame())

Efficiently reduce the given vector or tuple of `AbstractDataFrame`s with
`vcat`.

See the [`vcat`](@ref) docstring for a description of keyword arguments `cols`
and `source`.

The keyword argument `init` is the initial value to use in the reductions.
It must be a data frame that has zero rows. It is not taken into account when
computing the value of the `source` column nor when determining metadata
of the produced data frame.

The column order, names, and types of the resulting `DataFrame`, and the
behavior of `cols` and `source` keyword arguments follow the rules specified for
[`vcat`](@ref) of `AbstractDataFrame`s.

Metadata: `vcat` propagates table-level `:note`-style metadata for keys that are
present in all passed data frames and have the same value. `vcat` propagates
column-level `:note`-style metadata for keys that are present in all passed data
frames that contain this column and have the same value.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4:6, B=4:6)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> df3 = DataFrame(A=7:9, C=7:9)
3×2 DataFrame
 Row │ A      C
     │ Int64  Int64
─────┼──────────────
   1 │     7      7
   2 │     8      8
   3 │     9      9

julia> reduce(vcat, (df1, df2))
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

julia> reduce(vcat, [df1, df2, df3], cols=:union, source=:source)
9×4 DataFrame
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Int64
─────┼─────────────────────────────────
   1 │     1        1  missing       1
   2 │     2        2  missing       1
   3 │     3        3  missing       1
   4 │     4        4  missing       2
   5 │     5        5  missing       2
   6 │     6        6  missing       2
   7 │     7  missing        7       3
   8 │     8  missing        8       3
   9 │     9  missing        9       3
```
"""
function Base.reduce(::typeof(vcat),
    dfs::Union{AbstractVector{AbstractDataFrame},
        AbstractVector{DataFrame},
        AbstractVector{SubDataFrame},
        AbstractVector{Union{DataFrame,SubDataFrame}},
        Tuple{AbstractDataFrame,Vararg{AbstractDataFrame}}};
    cols::Union{Symbol,AbstractVector{Symbol},
        AbstractVector{<:AbstractString}}=:setequal,
    source::Union{Nothing,SymbolOrString,
        Pair{<:SymbolOrString,<:AbstractVector}}=nothing,
    init::AbstractDataFrame=DataFrame())
    if nrow(init) > 0
        throw(ArgumentError("init data frame must have zero rows"))
    end
    dfs_init = AbstractDataFrame[emptycolmetadata!(copy(init))]
    append!(dfs_init, dfs)
    res = _vcat(AbstractDataFrame[df for df in dfs_init if ncol(df) != 0]; cols=cols)
    # only handle table-level metadata, as column-level metadata was done in _vcat
    _merge_matching_table_note_metadata!(res, dfs)

    if source !== nothing
        len = length(dfs)
        if source isa SymbolOrString
            col, vals = source, 1:len
        else
            @assert source isa Pair{<:SymbolOrString,<:AbstractVector}
            col, vals = source
        end

        if columnindex(res, col) > 0
            idx = findfirst(df -> columnindex(df, col) > 0, dfs)
            @assert idx !== nothing
            throw(ArgumentError("source column name :$col already exists in data frame " *
                                " passed in position $idx"))
        end

        if len != length(vals)
            throw(ArgumentError("number of passed source identifiers ($(length(vals)))" *
                                "does not match the number of data frames ($len)"))
        end

        source_vec = Tables.allocatecolumn(eltype(vals), nrow(res))
        @assert firstindex(source_vec) == 1 && lastindex(source_vec) == nrow(res)
        start = 1
        for (v, df) in zip(vals, dfs)
            stop = start + nrow(df) - 1
            source_vec[start:stop] .= Ref(v)
            start = stop + 1
        end

        @assert start == nrow(res) + 1
        insertcols!(res, col => source_vec)
    end

    return res
end

# definition needed to avoid dispatch ambiguity
Base.reduce(::typeof(vcat),
    dfs::Union{SentinelArrays.ChainedVector{AbstractDataFrame,<:AbstractVector{AbstractDataFrame}},
        SentinelArrays.ChainedVector{DataFrame,<:AbstractVector{DataFrame}},
        SentinelArrays.ChainedVector{SubDataFrame,<:AbstractVector{SubDataFrame}},
        SentinelArrays.ChainedVector{Union{DataFrame,SubDataFrame},<:AbstractVector{Union{DataFrame,SubDataFrame}}}};
    cols::Union{Symbol,AbstractVector{Symbol},
        AbstractVector{<:AbstractString}}=:setequal,
    source::Union{Nothing,SymbolOrString,
        Pair{<:SymbolOrString,<:AbstractVector}}=nothing,
    init::AbstractDataFrame=DataFrame()) =
    reduce(vcat, collect(AbstractDataFrame, dfs), cols=cols, source=source, init=init)

function _vcat(dfs::AbstractVector{AbstractDataFrame};
    cols::Union{Symbol,AbstractVector{Symbol},
        AbstractVector{<:AbstractString}}=:setequal)
    # note that empty DataFrame() objects are dropped from dfs before we call _vcat
    if isempty(dfs)
        cols isa Symbol && return DataFrame()
        return DataFrame([col => Missing[] for col in cols])
    end
    # Array of all headers
    allheaders = map(names, dfs)
    # Array of unique headers across all data frames
    uniqueheaders = unique(allheaders)
    # All symbols present across all headers
    unionunique = union(uniqueheaders...)
    # List of symbols present in all dataframes
    intersectunique = intersect(uniqueheaders...)

    if cols === :orderequal
        header = unionunique
        if length(uniqueheaders) > 1
            throw(ArgumentError("when `cols=:orderequal` all data frames need to " *
                                "have the same column names and be in the same order"))
        end
    elseif cols === :setequal || cols === :equal
        # an explicit error is thrown as :equal was supported in the past
        if cols === :equal
            throw(ArgumentError("`cols=:equal` is not supported. " *
                                "Use `:setequal` instead."))
        end

        header = unionunique
        coldiff = setdiff(unionunique, intersectunique)

        if !isempty(coldiff)
            # if any DataFrames are a full superset of names, skip them
            let header = header     # julia #15276
                filter!(u -> !issetequal(u, header), uniqueheaders)
            end
            estrings = map(enumerate(uniqueheaders)) do (i, head)
                matching = findall(h -> head == h, allheaders)
                headerdiff = setdiff(coldiff, head)
                badcols = join(headerdiff, ", ", " and ")
                args = join(matching, ", ", " and ")
                return "column(s) $badcols are missing from argument(s) $args"
            end
            throw(ArgumentError(join(estrings, ", ", ", and ")))
        end
    elseif cols === :intersect
        header = intersectunique
    elseif cols === :union
        header = unionunique
    elseif cols isa Symbol
        throw(ArgumentError("Invalid `cols` value :$cols. " *
                            "Only `:orderequal`, `:setequal`, `:intersect`, " *
                            "`:union`, or a vector of column names is allowed."))
    elseif cols isa AbstractVector{Symbol}
        header = cols
    else
        @assert cols isa AbstractVector{<:AbstractString}
        header = Symbol.(cols)
    end

    if isempty(header)
        out_df = DataFrame()
    else
        all_cols = Vector{AbstractVector}(undef, length(header))
        for (i, name) in enumerate(header)
            newcols = map(dfs) do df
                if hasproperty(df, name)
                    return df[!, name]
                else
                    Iterators.repeated(missing, nrow(df))
                end
            end

            lens = map(length, newcols)
            T = mapreduce(eltype, promote_type, newcols)
            all_cols[i] = Tables.allocatecolumn(T, sum(lens))
            offset = 1
            for j in 1:length(newcols)
                copyto!(all_cols[i], offset, newcols[j])
                offset += lens[j]
            end
        end

        out_df = DataFrame(all_cols, header, copycols=false)
    end

    # here we process column-level metadata, table-level metadata is processed in reduce

    # first check if all data frames do not have column-level metadata
    # in which case we do not have to do anything
    all(df -> getfield(parent(df), :colmetadata) === nothing, dfs) && return out_df

    for colname in _names(out_df)
        if length(dfs) == 1
            df1 = dfs[1]
            hasproperty(df1, colname) && _copy_col_note_metadata!(out_df, colname, df1, colname)
        else
            start = findfirst(x -> hasproperty(x, colname), dfs)
            start === nothing && continue
            df_start = dfs[start]
            for key_start in colmetadatakeys(df_start, colname)
                meta_val_start, meta_style_start = colmetadata(df_start, colname, key_start, style=true)
                if meta_style_start === :note
                    good_key = true
                    for i in start+1:length(dfs)
                        dfi = dfs[i]
                        if hasproperty(dfi, colname)
                            if key_start in colmetadatakeys(dfi, colname)
                                meta_vali, meta_stylei = colmetadata(dfi, colname, key_start, style=true)
                                if !(meta_stylei === :note && isequal(meta_val_start, meta_vali))
                                    good_key = false
                                    break
                                end
                            else
                                good_key = false
                                break
                            end
                        end
                    end
                    good_key && colmetadata!(out_df, colname, key_start, meta_val_start, style=:note)
                end
            end
        end
    end

    return out_df
end
