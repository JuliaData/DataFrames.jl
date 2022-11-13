"""
    AbstractDataFrame

An abstract type for which all concrete types expose an interface
for working with tabular data.

An `AbstractDataFrame` is a two-dimensional table with `Symbol`s or strings
for column names.

DataFrames.jl defines two types that are subtypes of `AbstractDataFrame`:
[`DataFrame`](@ref) and [`SubDataFrame`](@ref).

# Indexing and broadcasting

`AbstractDataFrame` can be indexed by passing two indices specifying
row and column selectors. The allowed indices are a superset of indices
that can be used for standard arrays. You can also access a single column
of an `AbstractDataFrame` using `getproperty` and `setproperty!` functions.
Columns can be selected using integers, `Symbol`s, or strings.
In broadcasting `AbstractDataFrame` behavior is similar to a `Matrix`.

A detailed description of `getindex`, `setindex!`, `getproperty`, `setproperty!`,
broadcasting and broadcasting assignment for data frames is given in the
["Indexing" section](https://juliadata.github.io/DataFrames.jl/stable/lib/indexing/)
of the manual.
"""
abstract type AbstractDataFrame end

"""
    names(df::AbstractDataFrame, cols=:)
    names(df::DataFrameRow, cols=:)
    names(df::GroupedDataFrame, cols=:)
    names(df::DataFrameRows, cols=:)
    names(df::DataFrameColumns, cols=:)
    names(df::GroupKey)

Return a freshly allocated `Vector{String}` of names of columns contained in `df`.

If `cols` is passed then restrict returned column names to those matching the
selector (this is useful in particular with regular expressions, `Cols`, `Not`, and `Between`).
`cols` can be:
* any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR); these column
  selectors are documented in the [General rules](@ref) section of the [Indexing](@ref)
  part of the DataFrames.jl manual
* a `Type`, in which case names of columns whose `eltype` is a subtype of `T`
  are returned
* a `Function` predicate taking the column name as a string and returning `true`
  for columns that should be kept

See also [`propertynames`](@ref) which returns a `Vector{Symbol}`.

# Examples

```jldoctest
julia> df = DataFrame(x1=[1, missing, missing], x2=[3, 2, 4], x3=[3, missing, 2], x4=Union{Int, Missing}[2, 4, 4])
3×4 DataFrame
 Row │ x1       x2     x3       x4
     │ Int64?   Int64  Int64?   Int64?
─────┼─────────────────────────────────
   1 │       1      3        3       2
   2 │ missing      2  missing       4
   3 │ missing      4        2       4

julia> names(df)
4-element Vector{String}:
 "x1"
 "x2"
 "x3"
 "x4"

julia> names(df, Int) # pick columns whose element type is Int
1-element Vector{String}:
 "x2"

julia> names(df, x -> x[end] == '2') # pick columns for which last character in their name is '2'
1-element Vector{String}:
 "x2"

julia> fun(col) = sum(skipmissing(col)) >= 10
fun (generic function with 1 method)

julia> names(df, fun.(eachcol(df))) # pick columns for which sum of their elements is at least 10
1-element Vector{String}:
 "x4"

julia> names(df, eltype.(eachcol(df)) .>: Missing) # pick columns that allow missing values
3-element Vector{String}:
 "x1"
 "x3"
 "x4"

julia> names(df, any.(ismissing, eachcol(df))) # pick columns that contain missing values
2-element Vector{String}:
 "x1"
 "x3"
```
"""
Base.names(df::AbstractDataFrame, cols::Colon=:) = names(index(df))

function Base.names(df::AbstractDataFrame, cols)
    nms = _names(index(df))
    idx = index(df)[cols]
    idxs = idx isa Int ? (idx:idx) : idx
    return [String(nms[i]) for i in idxs]
end

Base.names(df::AbstractDataFrame, T::Type) =
    [String(n) for (n, c) in pairs(eachcol(df)) if eltype(c) <: T]
Base.names(df::AbstractDataFrame, fun::Function) = filter!(fun, names(df))

# _names returns Vector{Symbol} without copying
_names(df::AbstractDataFrame) = _names(index(df))

# separate methods are needed due to dispatch ambiguity
Compat.hasproperty(df::AbstractDataFrame, s::Symbol) = haskey(index(df), s)
Compat.hasproperty(df::AbstractDataFrame, s::AbstractString) = haskey(index(df), s)

"""
    rename!(df::AbstractDataFrame, vals::AbstractVector{Symbol};
            makeunique::Bool=false)
    rename!(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
            makeunique::Bool=false)
    rename!(df::AbstractDataFrame, (from => to)::Pair...)
    rename!(df::AbstractDataFrame, d::AbstractDict)
    rename!(df::AbstractDataFrame, d::AbstractVector{<:Pair})
    rename!(f::Function, df::AbstractDataFrame)

Rename columns of `df` in-place.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataFrame`
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`
- `vals` : new column names as a vector of `Symbol`s or `AbstractString`s
  of the same length as the number of columns in `df`
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

If pairs are passed to `rename!` (as positional arguments or in a dictionary or
a vector) then:
* `from` value can be a `Symbol`, an `AbstractString` or an `Integer`;
* `to` value can be a `Symbol` or an `AbstractString`.

Mixing symbols and strings in `to` and `from` is not allowed.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).
Column-level `:note`-style metadata is considered to be attached to column number:
when a column is renamed, its `:note`-style metadata becomes associated to its new name.

See also: [`rename`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(i=1, x=2, y=3)
1×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, Dict(:i => "A", :x => "X"))
1×3 DataFrame
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, [:a, :b, :c])
1×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(df, [:a, :b, :a])
ERROR: ArgumentError: Duplicate variable names: :a. Pass makeunique=true to make them unique using a suffix automatically.

julia> rename!(df, [:a, :b, :a], makeunique=true)
1×3 DataFrame
 Row │ a      b      a_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename!(uppercase, df)
1×3 DataFrame
 Row │ A      B      A_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
```
"""
function rename!(df::AbstractDataFrame, vals::AbstractVector{Symbol};
                 makeunique::Bool=false)
    rename!(index(df), vals, makeunique=makeunique)
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function rename!(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
                 makeunique::Bool=false)
    rename!(index(df), Symbol.(vals), makeunique=makeunique)
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function rename!(df::AbstractDataFrame, args::AbstractVector{Pair{Symbol, Symbol}})
    rename!(index(df), args)
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function rename!(df::AbstractDataFrame,
                 args::Union{AbstractVector{<:Pair{Symbol, <:AbstractString}},
                             AbstractVector{<:Pair{<:AbstractString, Symbol}},
                             AbstractVector{<:Pair{<:AbstractString, <:AbstractString}},
                             AbstractDict{Symbol, Symbol},
                             AbstractDict{Symbol, <:AbstractString},
                             AbstractDict{<:AbstractString, Symbol},
                             AbstractDict{<:AbstractString, <:AbstractString}})
    rename!(index(df), [Symbol(from) => Symbol(to) for (from, to) in args])
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function rename!(df::AbstractDataFrame,
                 args::Union{AbstractVector{<:Pair{<:Integer, <:AbstractString}},
                             AbstractVector{<:Pair{<:Integer, Symbol}},
                             AbstractDict{<:Integer, <:AbstractString},
                             AbstractDict{<:Integer, Symbol}})
    rename!(index(df), [_names(df)[from] => Symbol(to) for (from, to) in args])
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

 # needed because of dispach ambiguity
function rename!(df::AbstractDataFrame)
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

rename!(df::AbstractDataFrame, args::Pair...) = rename!(df, collect(args))

function rename!(f::Function, df::AbstractDataFrame)
    rename!(f, index(df))
    # renaming columns of SubDataFrame has to clean non-note metadata in its parent
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

"""
    rename(df::AbstractDataFrame, vals::AbstractVector{Symbol};
           makeunique::Bool=false)
    rename(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
           makeunique::Bool=false)
    rename(df::AbstractDataFrame, (from => to)::Pair...)
    rename(df::AbstractDataFrame, d::AbstractDict)
    rename(df::AbstractDataFrame, d::AbstractVector{<:Pair})
    rename(f::Function, df::AbstractDataFrame)

Create a new data frame that is a copy of `df` with changed column names.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataFrame`; if it is a `SubDataFrame` then renaming is
  only allowed if it was created using `:` as a column selector.
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`
- `vals` : new column names as a vector of `Symbol`s or `AbstractString`s
  of the same length as the number of columns in `df`
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

If pairs are passed to `rename` (as positional arguments or in a dictionary or
a vector) then:
* `from` value can be a `Symbol`, an `AbstractString` or an `Integer`;
* `to` value can be a `Symbol` or an `AbstractString`.

Mixing symbols and strings in `to` and `from` is not allowed.

$METADATA_FIXED
Column-level `:note`-style metadata is considered to be attached to column number:
when a column is renamed, its `:note`-style metadata becomes associated to its
new name.

See also: [`rename!`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(i=1, x=2, y=3)
1×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, :i => :A, :x => :X)
1×3 DataFrame
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, :x => :y, :y => :x)
1×3 DataFrame
 Row │ i      y      x
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, [1 => :A, 2 => :X])
1×3 DataFrame
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, Dict("i" => "A", "x" => "X"))
1×3 DataFrame
 Row │ A      X      y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(uppercase, df)
1×3 DataFrame
 Row │ I      X      Y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
```
"""
rename(df::AbstractDataFrame, vals::AbstractVector{Symbol};
       makeunique::Bool=false) = rename!(copy(df), vals, makeunique=makeunique)
rename(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
       makeunique::Bool=false) = rename!(copy(df), vals, makeunique=makeunique)
rename(df::AbstractDataFrame, args...) = rename!(copy(df), args...)
rename(f::Function, df::AbstractDataFrame) = rename!(f, copy(df))

"""
    size(df::AbstractDataFrame[, dim])

Return a tuple containing the number of rows and columns of `df`.
Optionally a dimension `dim` can be specified, where `1` corresponds to rows
and `2` corresponds to columns.

See also: [`nrow`](@ref), [`ncol`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(a=1:3, b='a':'c');

julia> size(df)
(3, 2)

julia> size(df, 1)
3
```
"""
Base.size(df::AbstractDataFrame) = (nrow(df), ncol(df))
function Base.size(df::AbstractDataFrame, i::Integer)
    if i == 1
        nrow(df)
    elseif i == 2
        ncol(df)
    else
        throw(ArgumentError("DataFrames only have two dimensions"))
    end
end

"""
    ncol(df::AbstractDataFrame)

Return the number of columns in an `AbstractDataFrame` `df`.

See also [`nrow`](@ref), [`size`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:10, x=rand(10), y=rand(["a", "b", "c"], 10));

julia> ncol(df)
3
```
"""
ncol(df::AbstractDataFrame) = length(index(df))

"""
    isempty(df::AbstractDataFrame)

Return `true` if data frame `df` has zero rows, and `false` otherwise.
"""
Base.isempty(df::AbstractDataFrame) = nrow(df) == 0

Base.axes(df::AbstractDataFrame, i::Integer) = Base.OneTo(size(df, i))

"""
    ndims(::AbstractDataFrame)
    ndims(::Type{<:AbstractDataFrame})

Return the number of dimensions of a data frame, which is always `2`.
"""
Base.ndims(::AbstractDataFrame) = 2
Base.ndims(::Type{<:AbstractDataFrame}) = 2

# separate methods are needed due to dispatch ambiguity
Base.getproperty(df::AbstractDataFrame, col_ind::Symbol) = df[!, col_ind]
Base.getproperty(df::AbstractDataFrame, col_ind::AbstractString) = df[!, col_ind]

# Private fields are never exposed since they can conflict with column names
"""
    propertynames(df::AbstractDataFrame)

Return a freshly allocated `Vector{Symbol}` of names of columns contained in `df`.
"""
Base.propertynames(df::AbstractDataFrame, private::Bool=false) = copy(_names(df))

##############################################################################
##
## Similar
##
##############################################################################

"""
    similar(df::AbstractDataFrame, rows::Integer=nrow(df))

Create a new `DataFrame` with the same column names and column element types
as `df`. An optional second argument can be provided to request a number of rows
that is different than the number of rows present in `df`.

$METADATA_FIXED
"""
function Base.similar(df::AbstractDataFrame, rows::Integer = size(df, 1))
    rows < 0 && throw(ArgumentError("the number of rows must be non-negative"))
    out_df = DataFrame(AbstractVector[similar(x, rows) for x in eachcol(df)], copy(index(df)),
                       copycols=false)
    _copy_all_note_metadata!(out_df, df)
    return out_df
end

"""
    empty(df::AbstractDataFrame)

Create a new `DataFrame` with the same column names and column element types
as `df` but with zero rows.

$METADATA_FIXED
"""
Base.empty(df::AbstractDataFrame) = similar(df, 0)

function Base.:(==)(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    eq = true
    for idx in 1:size(df1, 2)
        coleq = df1[!, idx] == df2[!, idx]
        # coleq could be missing
        isequal(coleq, false) && return false
        eq &= coleq
    end
    return eq
end

function Base.isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    for idx in 1:size(df1, 2)
        isequal(df1[!, idx], df2[!, idx]) || return false
    end
    return true
end

"""
    isapprox(df1::AbstractDataFrame, df2::AbstractDataFrame;
             rtol::Real=atol>0 ? 0 : √eps, atol::Real=0,
             nans::Bool=false, norm::Function=norm)

Inexact equality comparison. `df1` and `df2` must have the same size and column names.
Return  `true` if `isapprox` with given keyword arguments
applied to all pairs of columns stored in `df1` and `df2` returns `true`.
"""
function Base.isapprox(df1::AbstractDataFrame, df2::AbstractDataFrame;
                       atol::Real=0, rtol::Real=atol>0 ? 0 : √eps(),
                       nans::Bool=false, norm::Function=norm)
    if size(df1) != size(df2)
        throw(DimensionMismatch("dimensions must match: a has dims " *
                                "$(size(df1)), b has dims $(size(df2))"))
    end
    if !isequal(index(df1), index(df2))
        throw(ArgumentError("column names of passed data frames do not match"))
    end
    return all(isapprox.(eachcol(df1), eachcol(df2), atol=atol, rtol=rtol, nans=nans, norm=norm))
end

"""
    only(df::AbstractDataFrame)

If `df` has a single row return it as a `DataFrameRow`; otherwise throw `ArgumentError`.

$METADATA_FIXED
"""
function Base.only(df::AbstractDataFrame)
    nrow(df) != 1 && throw(ArgumentError("data frame must contain exactly 1 row"))
    return df[1, :]
end

"""
    first(df::AbstractDataFrame)

Get the first row of `df` as a `DataFrameRow`.

$METADATA_FIXED
"""
Base.first(df::AbstractDataFrame) = df[1, :]

"""
    first(df::AbstractDataFrame, n::Integer; view::Bool=false)

Get a data frame with the `n` first rows of `df`.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.

$METADATA_FIXED
"""
@inline Base.first(df::AbstractDataFrame, n::Integer; view::Bool=false) =
    view ? Base.view(df, 1:min(n ,nrow(df)), :) : df[1:min(n, nrow(df)), :]

"""
    last(df::AbstractDataFrame)

Get the last row of `df` as a `DataFrameRow`.

$METADATA_FIXED
"""
Base.last(df::AbstractDataFrame) = df[nrow(df), :]

"""
    last(df::AbstractDataFrame, n::Integer; view::Bool=false)

Get a data frame with the `n` last rows of `df`.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.

$METADATA_FIXED
"""
@inline Base.last(df::AbstractDataFrame, n::Integer; view::Bool=false) =
    view ? Base.view(df, max(1, nrow(df)-n+1):nrow(df), :) : df[max(1, nrow(df)-n+1):nrow(df), :]

"""
    completecases(df::AbstractDataFrame, cols=:)

Return a Boolean vector with `true` entries indicating rows without missing values
(complete cases) in data frame `df`.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR)
that returns at least one column if `df` has at least one column.

See also: [`dropmissing`](@ref) and [`dropmissing!`](@ref).
Use `findall(completecases(df))` to get the indices of the rows.

# Examples

```jldoctest
julia> df = DataFrame(i=1:5,
                      x=[missing, 4, missing, 2, 1],
                      y=[missing, missing, "c", "d", "e"])
5×3 DataFrame
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> completecases(df)
5-element BitVector:
 0
 0
 0
 1
 1

julia> completecases(df, :x)
5-element BitVector:
 0
 1
 0
 1
 1

julia> completecases(df, [:x, :y])
5-element BitVector:
 0
 0
 0
 1
 1
```
"""
function completecases(df::AbstractDataFrame, cols::MultiColumnIndex=:)
    colsidx = index(df)[cols]
    length(colsidx) == 1 && return completecases(df, only(colsidx))

    if ncol(df) > 0 && isempty(colsidx)
        throw(ArgumentError("finding complete cases in data frame when " *
                            "`cols` selects no columns is not allowed"))
    end

    res = trues(size(df, 1))
    ncol(df) == 0 && return res
    aux = BitVector(undef, size(df, 1))
    for i in colsidx
        v = df[!, i]
        if Missing <: eltype(v)
            # Disable fused broadcasting as it happens to be much slower
            aux .= .!ismissing.(v)
            res .&= aux
        end
    end
    return res
end

function completecases(df::AbstractDataFrame, col::ColumnIndex)
    v = df[!, col]
    if Missing <: eltype(v)
        res = BitVector(undef, size(df, 1))
        res .= .!ismissing.(v)
        return res
    else
        return trues(size(df, 1))
    end
end

"""
    dropmissing(df::AbstractDataFrame, cols=:; view::Bool=false, disallowmissing::Bool=!view)

Return a data frame excluding rows with missing values in `df`.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned. In this case
`disallowmissing` must be `false`.

If `disallowmissing` is `true` (the default when `view` is `false`)
then columns specified in `cols` will be converted so as not to allow for missing
values using [`disallowmissing!`](@ref).

See also: [`completecases`](@ref) and [`dropmissing!`](@ref).

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(i=1:5,
                      x=[missing, 4, missing, 2, 1],
                      y=[missing, missing, "c", "d", "e"])
5×3 DataFrame
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> dropmissing(df)
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e

julia> dropmissing(df, disallowmissing=false)
2×3 DataFrame
 Row │ i      x       y
     │ Int64  Int64?  String?
─────┼────────────────────────
   1 │     4       2  d
   2 │     5       1  e

julia> dropmissing(df, :x)
3×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e

julia> dropmissing(df, [:x, :y])
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```
"""
@inline function dropmissing(df::AbstractDataFrame,
                             cols::Union{ColumnIndex, MultiColumnIndex}=:;
                             view::Bool=false, disallowmissing::Bool=!view)
    rowidxs = completecases(df, cols)
    if view
        if disallowmissing
            throw(ArgumentError("disallowmissing=true is incompatible with view=true"))
        end
        return Base.view(df, rowidxs, :)
    else
        newdf = df[rowidxs, :]
        disallowmissing && disallowmissing!(newdf, cols)
        return newdf
    end
end

"""
    dropmissing!(df::AbstractDataFrame, cols=:; disallowmissing::Bool=true)

Remove rows with missing values from data frame `df` and return it.

If `cols` is provided, only missing values in the corresponding columns are considered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `disallowmissing` is `true` (the default) then the `cols` columns will
get converted using [`disallowmissing!`](@ref).

$METADATA_FIXED

See also: [`dropmissing`](@ref) and [`completecases`](@ref).

```jldoctest
julia> df = DataFrame(i=1:5,
                      x=[missing, 4, missing, 2, 1],
                      y=[missing, missing, "c", "d", "e"])
5×3 DataFrame
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> dropmissing!(copy(df))
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e

julia> dropmissing!(copy(df), disallowmissing=false)
2×3 DataFrame
 Row │ i      x       y
     │ Int64  Int64?  String?
─────┼────────────────────────
   1 │     4       2  d
   2 │     5       1  e

julia> dropmissing!(copy(df), :x)
3×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e

julia> dropmissing!(df, [:x, :y])
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```
"""
function dropmissing!(df::AbstractDataFrame,
                      cols::Union{ColumnIndex, MultiColumnIndex}=:;
                      disallowmissing::Bool=true)
    inds = completecases(df, cols)
    inds .= .!(inds)
    deleteat!(df, inds) # drops non :note-style metadata
    disallowmissing && disallowmissing!(df, cols)
    df
end

function Base.Matrix(df::AbstractDataFrame)
    T = reduce(promote_type, (eltype(v) for v in eachcol(df)), init=Union{})
    return Matrix{T}(df)
end

function Base.Matrix{T}(df::AbstractDataFrame) where T
    n, p = size(df)
    res = Matrix{T}(undef, n, p)
    idx = 1
    for (name, col) in pairs(eachcol(df))
        try
            copyto!(res, idx, col)
        catch err
            if err isa MethodError && err.f == convert &&
               !(T >: Missing) && any(ismissing, col)
                throw(ArgumentError("cannot convert a DataFrame containing missing " *
                                    "values to Matrix{$T} (found for column $name)"))
            else
                rethrow(err)
            end
        end
        idx += n
    end
    return res
end

Base.Array(df::AbstractDataFrame) = Matrix(df)
Base.Array{T}(df::AbstractDataFrame) where {T} = Matrix{T}(df)

"""
    nonunique(df::AbstractDataFrame)
    nonunique(df::AbstractDataFrame, cols)

Return a `Vector{Bool}` in which `true` entries indicate duplicate rows.
A row is a duplicate if there exists a prior row with all columns containing
equal values (according to `isequal`).

See also [`unique`](@ref) and [`unique!`](@ref).

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to compare.
  Can be any column selector or transformation accepted by [`select`](@ref) that
  returns at least one column if `df` has at least one column.

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> nonunique(df)
8-element Vector{Bool}:
 0
 0
 0
 0
 1
 1
 1
 1

julia> nonunique(df, 2)
8-element Vector{Bool}:
 0
 0
 1
 1
 1
 1
 1
 1
```
"""
function nonunique(df::AbstractDataFrame)
    ncol(df) == 0 && return Bool[]
    gslots = row_group_slots(ntuple(i -> df[!, i], ncol(df)), Val(true), nothing, false, nothing)[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(df))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

function nonunique(df::AbstractDataFrame, cols)
    udf = select(df, cols, copycols=false)
    if ncol(df) > 0 && ncol(udf) == 0
         throw(ArgumentError("finding duplicate rows in data frame when " *
                             "`cols` selects no columns is not allowed"))
    else
        return nonunique(udf)
    end
end

"""
    unique(df::AbstractDataFrame; view::Bool=false)
    unique(df::AbstractDataFrame, cols; view::Bool=false)

Return a data frame containing only the first occurrence of unique rows in `df`.
When `cols` is specified, the returned `DataFrame` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).

If `view=false` a freshly allocated `DataFrame` is returned,
and if `view=true` then a `SubDataFrame` view into `df` is returned.

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (`Symbol`, `Int`, `Vector{Symbol}`, `Regex`, etc.)
specifying the column(s) to compare.

$METADATA_FIXED

See also: [`unique!`](@ref), [`nonunique`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> unique(df)   # doesn't modify df
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> unique(df, 2)
2×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
```
"""
@inline function Base.unique(df::AbstractDataFrame; view::Bool=false)
    rowidxs = (!).(nonunique(df))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

@inline function Base.unique(df::AbstractDataFrame, cols; view::Bool=false)
    rowidxs = (!).(nonunique(df, cols))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

"""
    unique!(df::AbstractDataFrame)
    unique!(df::AbstractDataFrame, cols)

Update `df` in-place to contain only the first occurrence of unique rows in `df`.
When `cols` is specified, the returned `DataFrame` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (`Symbol`, `Int`, `Vector{Symbol}`, `Regex`, etc.)
specifying the column(s) to compare.

$METADATA_FIXED

See also: [`unique!`](@ref), [`nonunique`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> unique!(df)  # modifies df
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
```
"""
Base.unique!(df::AbstractDataFrame) = deleteat!(df, _findall(nonunique(df)))
Base.unique!(df::AbstractDataFrame, cols::AbstractVector) =
    deleteat!(df, _findall(nonunique(df, cols)))
Base.unique!(df::AbstractDataFrame, cols) =
    deleteat!(df, _findall(nonunique(df, cols)))

"""
    fillcombinations(df::AbstractDataFrame, indexcols;
                         allowduplicates::Bool=false,
                         fill=missing)

Generate all combinations of levels of column(s) `indexcols` in data frame `df`.
Levels and their order are determined by the `levels` function
(i.e. unique values sorted lexicographically by default, or a custom set
of levels for e.g. `CategoricalArray` columns), in addition to `missing` if present.

For combinations of `indexcols` not present in `df` these columns are
filled with the `fill` value (`missing` by default).

If `allowduplicates=false` (the default) `indexcols` may only contain
unique combinations of `indexcols` values. If `allowduplicates=true`
duplicates are allowed.

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(x=1:2, y='a':'b', z=["x", "y"])
2×3 DataFrame
 Row │ x      y     z
     │ Int64  Char  String
─────┼─────────────────────
   1 │     1  a     x
   2 │     2  b     y

julia> fillcombinations(df, [:x, :y])
4×3 DataFrame
 Row │ x      y     z
     │ Int64  Char  String?
─────┼──────────────────────
   1 │     1  a     x
   2 │     2  a     missing
   3 │     1  b     missing
   4 │     2  b     y

julia> fillcombinations(df, [:y, :z], fill=0)
4×3 DataFrame
 Row │ x       y     z
     │ Int64?  Char  String
─────┼──────────────────────
   1 │      1  a     x
   2 │      0  b     x
   3 │      0  a     y
   4 │      2  b     y
```
"""
function fillcombinations(df::AbstractDataFrame, indexcols;
                          allowduplicates::Bool=false, fill=missing)
    _check_consistency(df)

    colind = index(df)[indexcols]

    if length(colind) == 0
        throw(ArgumentError("At least one column to fill combinations " *
                            "must be specified"))
    end

    has_duplicates = row_group_slots(ntuple(i -> df[!, colind[i]], length(colind)),
                                     Val(false), nothing, false, nothing)[1] != nrow(df)
    if has_duplicates && !allowduplicates
        throw(ArgumentError("duplicate combinations of `indexcols` are not " *
                            "allowed in input when `allowduplicates=false`"))
    end

    # Create a vector of vectors of unique values in each column
    uniquevals = []
    for col in colind
        # All levels are retained, missing is added only if present
        tempcol = levels(df[!, col], skipmissing=false)
        push!(uniquevals, tempcol)
    end

    # make sure we do not overflow in the target data frame size
    target_rows = Int(prod(x -> big(length(x)), uniquevals))
    if iszero(target_rows)
        @assert iszero(nrow(df))
        cdf = copy(df)
        _drop_all_nonnote_metadata!(cdf)
        return cdf
    end

    # construct expanded columns
    out_df = DataFrame()
    inner = 1
    @assert length(uniquevals) == length(colind)
    for (val, cind) in zip(uniquevals, colind)
        len = length(val)
        target_col = Tables.allocatecolumn(eltype(df[!, cind]), len)
        copy!(target_col, val)
        last_inner = inner
        inner *= len
        outer, remv = divrem(target_rows, inner)
        @assert iszero(remv)
        out_df[!, _names(df)[cind]] = repeat(target_col, inner=last_inner, outer=outer)
    end
    @assert inner == target_rows

    idx_ind = 0
    idx_col = ""
    while true
        idx_col = string("source7249", idx_ind)
        columnindex(df, idx_col) == 0 && break
        idx_ind += 1
    end

    if has_duplicates
        order_ind = 0
        order_col = ""
        while true
            order_col = string("order9427", order_ind)
            columnindex(df, order_col) == 0 && break
            order_ind += 1
        end
        insertcols!(out_df, 1, order_col => 1:nrow(out_df))
        out_df = leftjoin(out_df, df; on=_names(df)[colind],
                          source=idx_col,
                          matchmissing=:equal)
        sort!(out_df, 1)
        select!(out_df, 2:ncol(out_df))
    else
        leftjoin!(out_df, df; on=_names(df)[colind],
                  source=string("source7249", idx_ind),
                  matchmissing=:equal)
    end

    # Replace missing values with the fill
    if !ismissing(fill)
        mask = out_df[!, end] .== "left_only"
        if count(mask) > 0
            for n in length(colind)+1:ncol(out_df)-1
                tmp_col = out_df[!, n]
                out_col = similar(tmp_col,
                                  promote_type(eltype(tmp_col), typeof(fill)),
                                  length(tmp_col))
                out_col .= ifelse.(mask, Ref(fill), out_df[!, n])
                out_df[!, n] = out_col
            end
        end
    end

    # keep only columns from the source in their original order
    select!(out_df, _names(df))
    _copy_all_note_metadata!(out_df, df)

    return out_df
end

##############################################################################
##
## Hashing
##
##############################################################################

const hashdf_seed = UInt == UInt32 ? 0xfd8bb02e : 0x6215bada8c8c46de

function Base.hash(df::AbstractDataFrame, h::UInt)
    h += hashdf_seed
    h += hash(size(df))
    for i in 1:size(df, 2)
        h = hash(df[!, i], h)
    end
    return h
end

Base.parent(adf::AbstractDataFrame) = adf
Base.parentindices(adf::AbstractDataFrame) = axes(adf)

"""
    disallowmissing(df::AbstractDataFrame, cols=:; error::Bool=true)

Return a copy of data frame `df` with columns `cols` converted
from element type `Union{T, Missing}` to `T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(a=Union{Int, Missing}[1, 2])
2×1 DataFrame
 Row │ a
     │ Int64?
─────┼────────
   1 │      1
   2 │      2

julia> disallowmissing(df)
2×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> df = DataFrame(a=[1, missing])
2×1 DataFrame
 Row │ a
     │ Int64?
─────┼─────────
   1 │       1
   2 │ missing

julia> disallowmissing(df, error=false)
2×1 DataFrame
 Row │ a
     │ Int64?
─────┼─────────
   1 │       1
   2 │ missing
```
"""
function Missings.disallowmissing(df::AbstractDataFrame,
                                  cols::Union{ColumnIndex, MultiColumnIndex}=:;
                                  error::Bool=true)
    idxcols = Set(index(df)[cols])
    newcols = AbstractVector[]
    for i in axes(df, 2)
        x = df[!, i]
        if i in idxcols
            y = x
            if Missing <: eltype(x)
                try
                    y = disallowmissing(x)
                catch e
                    row = findfirst(ismissing, x)
                    if row !== nothing
                        if error
                            col = _names(df)[i]
                            throw(ArgumentError("Missing value found in column " *
                                                ":$col in row $row"))
                        end
                    else
                        rethrow(e)
                    end
                end
            end
            push!(newcols, y === x ? copy(y) : y)
        else
            push!(newcols, copy(x))
        end
    end

    new_df = DataFrame(newcols, _names(df), copycols=false)
    _copy_all_note_metadata!(new_df, df)
    return new_df
end

"""
    allowmissing(df::AbstractDataFrame, cols=:)

Return a copy of data frame `df` with columns `cols` converted
to element type `Union{T, Missing}` from `T` to allow support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(a=[1, 2])
2×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> allowmissing(df)
2×1 DataFrame
 Row │ a
     │ Int64?
─────┼────────
   1 │      1
   2 │      2
```
"""
function Missings.allowmissing(df::AbstractDataFrame,
                               cols::Union{ColumnIndex, MultiColumnIndex}=:)
    idxcols = Set(index(df)[cols])
    newcols = AbstractVector[]
    for i in axes(df, 2)
        x = df[!, i]
        if i in idxcols
            y = allowmissing(x)
            push!(newcols, y === x ? copy(y) : y)
        else
            push!(newcols, copy(x))
        end
    end

    new_df = DataFrame(newcols, _names(df), copycols=false)
    _copy_all_note_metadata!(new_df, df)
    return new_df
end

"""
    flatten(df::AbstractDataFrame, cols)

When columns `cols` of data frame `df` have iterable elements that define
`length` (for example a `Vector` of `Vector`s), return a `DataFrame` where each
element of each `col` in `cols` is flattened, meaning the column corresponding
to `col` becomes a longer vector where the original entries are concatenated.
Elements of row `i` of `df` in columns other than `cols` will be repeated
according to the length of `df[i, col]`. These lengths must therefore be the
same for each `col` in `cols`, or else an error is raised. Note that these
elements are not copied, and thus if they are mutable changing them in the
returned `DataFrame` will affect `df`.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

$METADATA_FIXED

# Examples

```jldoctest
julia> df1 = DataFrame(a=[1, 2], b=[[1, 2], [3, 4]], c=[[5, 6], [7, 8]])
2×3 DataFrame
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7, 8]

julia> flatten(df1, :b)
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Array…
─────┼──────────────────────
   1 │     1      1  [5, 6]
   2 │     1      2  [5, 6]
   3 │     2      3  [7, 8]
   4 │     2      4  [7, 8]

julia> flatten(df1, [:b, :c])
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      5
   2 │     1      2      6
   3 │     2      3      7
   4 │     2      4      8

julia> df2 = DataFrame(a=[1, 2], b=[("p", "q"), ("r", "s")])
2×2 DataFrame
 Row │ a      b
     │ Int64  Tuple…
─────┼───────────────────
   1 │     1  ("p", "q")
   2 │     2  ("r", "s")

julia> flatten(df2, :b)
4×2 DataFrame
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  p
   2 │     1  q
   3 │     2  r
   4 │     2  s

julia> df3 = DataFrame(a=[1, 2], b=[[1, 2], [3, 4]], c=[[5, 6], [7]])
2×3 DataFrame
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7]

julia> flatten(df3, [:b, :c])
ERROR: ArgumentError: Lengths of iterables stored in columns :b and :c are not the same in row 2
```
"""
function flatten(df::AbstractDataFrame,
                 cols::Union{ColumnIndex, MultiColumnIndex})
    _check_consistency(df)

    idxcols = index(df)[cols]
    if isempty(idxcols)
        cdf = copy(df)
        _drop_all_nonnote_metadata!(cdf)
        return cdf
    end

    col1 = first(idxcols)
    lengths = length.(df[!, col1])
    for col in idxcols
        v = df[!, col]
        if any(x -> length(x[1]) != x[2], zip(v, lengths))
            r = findfirst(x -> x != 0, length.(v) .- lengths)
            colnames = _names(df)
            throw(ArgumentError("Lengths of iterables stored in columns :$(colnames[col1]) " *
                                "and :$(colnames[col]) are not the same in row $r"))
        end
    end

    new_df = similar(df[!, Not(cols)], sum(lengths))
    for name in _names(new_df)
        repeat_lengths!(new_df[!, name], df[!, name], lengths)
    end
    length(idxcols) > 1 && sort!(idxcols)
    for col in idxcols
        col_to_flatten = df[!, col]
        fast_path = eltype(col_to_flatten) isa AbstractVector &&
                    !isempty(col_to_flatten)
        flattened_col = fast_path ?
            reduce(vcat, col_to_flatten) :
            collect(Iterators.flatten(col_to_flatten))
        insertcols!(new_df, col, _names(df)[col] => flattened_col)
    end

    _copy_all_note_metadata!(new_df, df)
    return new_df
end

function repeat_lengths!(longnew::AbstractVector, shortold::AbstractVector,
                         lengths::AbstractVector{Int})
    counter = 1
    @inbounds for i in eachindex(shortold)
        l = lengths[i]
        longnew[counter:(counter + l - 1)] .= Ref(shortold[i])
        counter += l
    end
end

# Disallowed getindex and setindex! operations that are a common mistake

Base.getindex(::AbstractDataFrame, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax df[column] is not supported use df[!, column] instead"))

Base.setindex!(::AbstractDataFrame, ::Any, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax df[column] is not supported use df[!, column] instead"))

