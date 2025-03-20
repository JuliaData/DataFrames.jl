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

See also [`propertynames`](@ref) which returns a `Vector{Symbol}`
(except for `GroupedDataFrame` in which case use `Symbol.(names(df))`).

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
    rename!(f::Function, df::AbstractDataFrame; cols=All())

Rename columns of `df` in-place.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataFrame`
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column selected by the `cols` keyword argument
  takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`; the `cols`
  column selector can be any value accepted as column selector by the `names` function
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

julia> rename!(lowercase, df, cols=contains('A'))
1×3 DataFrame
 Row │ a      B      a_1
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

 # needed because of dispatch ambiguity
function rename!(df::AbstractDataFrame)
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

rename!(df::AbstractDataFrame, args::Pair...) = rename!(df, collect(args))

rename!(f::Function, df::AbstractDataFrame; cols=All()) =
    rename!(df, [n => Symbol(f(n)) for n in names(df, cols)])

"""
    rename(df::AbstractDataFrame, vals::AbstractVector{Symbol};
           makeunique::Bool=false)
    rename(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
           makeunique::Bool=false)
    rename(df::AbstractDataFrame, (from => to)::Pair...)
    rename(df::AbstractDataFrame, d::AbstractDict)
    rename(df::AbstractDataFrame, d::AbstractVector{<:Pair})
    rename(f::Function, df::AbstractDataFrame; cols=All())

Create a new data frame that is a copy of `df` with changed column names.
Each name is changed at most once. Permutation of names is allowed.

# Arguments
- `df` : the `AbstractDataFrame`; if it is a `SubDataFrame` then renaming is
  only allowed if it was created using `:` as a column selector.
- `d` : an `AbstractDict` or an `AbstractVector` of `Pair`s that maps
  the original names or column numbers to new names
- `f` : a function which for each column selected by the `cols` keyword argument
  takes the old name as a `String`
  and returns the new name that gets converted to a `Symbol`; the `cols`
  column selector can be any value accepted as column selector by the `names` function
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

julia> rename(df, [:a, :b, :c])
1×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> rename(df, :i => "A", :x => "X")
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

julia> rename(uppercase, df, cols=contains('x'))
1×3 DataFrame
 Row │ i      X      y
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
rename(f::Function, df::AbstractDataFrame; cols=All()) = rename!(f, copy(df); cols=cols)

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
    out_df = DataFrame(AbstractVector[similar(x, rows) for x in eachcol(df)],
                       copy(index(df)), copycols=false)
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
    n = nrow(df)
    n != 1 && throw(ArgumentError("data frame must contain exactly 1 row, got $n"))
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
Get all rows if `n` is greater than the number of rows in `df`.
Error if `n` is negative.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.

$METADATA_FIXED
"""
@inline function Base.first(df::AbstractDataFrame, n::Integer; view::Bool=false)
    n < 0 && throw(ArgumentError("Number of elements must be nonnegative"))
    r = min(n, nrow(df))
    return view ? Base.view(df, 1:r, :) : df[1:r, :]
end

"""
    last(df::AbstractDataFrame)

Get the last row of `df` as a `DataFrameRow`.

$METADATA_FIXED
"""
Base.last(df::AbstractDataFrame) = df[nrow(df), :]

"""
    last(df::AbstractDataFrame, n::Integer; view::Bool=false)

Get a data frame with the `n` last rows of `df`.
Get all rows if `n` is greater than the number of rows in `df`.
Error if `n` is negative.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.

$METADATA_FIXED
"""
@inline function Base.last(df::AbstractDataFrame, n::Integer; view::Bool=false)
    n < 0 && throw(ArgumentError("Number of elements must be nonnegative"))
    r = max(1, nrow(df) - n + 1)
    return view ? Base.view(df, r:nrow(df), :) : df[r:nrow(df), :]
end

"""
    describe(df::AbstractDataFrame; cols=:)
    describe(df::AbstractDataFrame, stats::Union{Symbol, Pair}...; cols=:)

Return descriptive statistics for a data frame as a new `DataFrame`
where each row represents a variable and each column a summary statistic.

# Arguments
- `df` : the `AbstractDataFrame`
- `stats::Union{Symbol, Pair}...` : the summary statistics to report.
  Arguments can be:
    - A symbol from the list `:mean`, `:std`, `:min`, `:q25`,
      `:median`, `:q75`, `:max`, `:sum`, `:eltype`, `:nunique`, `:nuniqueall`, `:first`,
      `:last`, `:nnonmissing`, and `:nmissing`. The default statistics used are
      `:mean`, `:min`, `:median`, `:max`, `:nmissing`, and `:eltype`.
    - `:detailed` as the only `Symbol` argument to return all statistics
      except `:first`, `:last`, `:sum`, `:nuniqueall`, and `:nnonmissing`.
    - `:all` as the only `Symbol` argument to return all statistics.
    - A `function => name` pair where `name` is a `Symbol` or string. This will
      create a column of summary statistics with the provided name.
- `cols` : a keyword argument allowing to select only a subset or transformation
  of columns from `df` to describe. Can be any column selector or transformation
  accepted by [`select`](@ref).

# Details
For `Real` columns, compute the mean, standard deviation, minimum, first
quantile, median, third quantile, and maximum. If a column does not derive from
`Real`, `describe` will attempt to calculate all statistics, using `nothing` as
a fall-back in the case of an error.

When `stats` contains `:nunique`, `describe` will report the
number of unique values in a column. If a column's base type derives from `Real`,
`:nunique` will return `nothing`s. Use `:nuniqueall` to report the number of
unique values in all columns.

Missing values are filtered in the calculation of all statistics, however the
column `:nmissing` will report the number of missing values of that variable
and `:nnonmissing` the number of non-missing values.

If custom functions are provided, they are called repeatedly with the vector
corresponding to each column as the only argument. For columns allowing for
missing values, the vector is wrapped in a call to `skipmissing`: custom
functions must therefore support such objects (and not only vectors), and cannot
access missing values.

Metadata: this function drops all metadata.

# Examples

```jldoctest
julia> df = DataFrame(i=1:10, x=0.1:0.1:1.0, y='a':'j');

julia> describe(df)
3×7 DataFrame
 Row │ variable  mean    min  median  max  nmissing  eltype
     │ Symbol    Union…  Any  Union…  Any  Int64     DataType
─────┼────────────────────────────────────────────────────────
   1 │ i         5.5     1    5.5     10          0  Int64
   2 │ x         0.55    0.1  0.55    1.0         0  Float64
   3 │ y                 a            j           0  Char

julia> describe(df, :min, :max)
3×3 DataFrame
 Row │ variable  min  max
     │ Symbol    Any  Any
─────┼────────────────────
   1 │ i         1    10
   2 │ x         0.1  1.0
   3 │ y         a    j

julia> describe(df, :min, sum => :sum)
3×3 DataFrame
 Row │ variable  min  sum
     │ Symbol    Any  Union…
─────┼───────────────────────
   1 │ i         1    55
   2 │ x         0.1  5.5
   3 │ y         a

julia> describe(df, :min, sum => :sum, cols=:x)
1×3 DataFrame
 Row │ variable  min      sum
     │ Symbol    Float64  Float64
─────┼────────────────────────────
   1 │ x             0.1      5.5
```
"""
DataAPI.describe(df::AbstractDataFrame,
                 stats::Union{Symbol, Pair{<:Base.Callable, <:SymbolOrString}}...;
                 cols=:) =
    _describe(_try_select_no_copy(df, cols), Any[s for s in stats])

DataAPI.describe(df::AbstractDataFrame; cols=:) =
    _describe(_try_select_no_copy(df, cols),
              Any[:mean, :min, :median, :max, :nmissing, :eltype])

function _describe(df::AbstractDataFrame, stats::AbstractVector)
    predefined_funs = Symbol[s for s in stats if s isa Symbol]

    allowed_fields = [:mean, :std, :min, :q25, :median, :q75, :max, :sum,
                      :nunique, :nuniqueall, :nmissing, :nnonmissing,
                      :first, :last, :eltype]

    if predefined_funs == [:all]
        predefined_funs = allowed_fields
        i = findfirst(s -> s == :all, stats)
        splice!(stats, i, allowed_fields) # insert in the stats vector to get a good order
    elseif predefined_funs == [:detailed]
        predefined_funs = [:mean, :std, :min, :q25, :median, :q75,
                           :max, :nunique, :nmissing, :eltype]
        i = findfirst(s -> s == :detailed, stats)
        splice!(stats, i, predefined_funs) # insert in the stats vector to get a good order
    elseif :all in predefined_funs || :detailed in predefined_funs
        throw(ArgumentError("`:all` and `:detailed` must be the only `Symbol` argument."))
    elseif !issubset(predefined_funs, allowed_fields)
        not_allowed = join(setdiff(predefined_funs, allowed_fields), ", :")
        allowed_msg = "\nAllowed fields are: :" * join(allowed_fields, ", :")
        throw(ArgumentError(":$not_allowed not allowed." * allowed_msg))
    end

    custom_funs = Any[s[1] => Symbol(s[2]) for s in stats if s isa Pair]

    ordered_names = [s isa Symbol ? s : Symbol(last(s)) for s in stats]

    if !allunique(ordered_names)
        df_ord_names = DataFrame(ordered_names = ordered_names)
        duplicate_names = unique(ordered_names[nonunique(df_ord_names)])
        throw(ArgumentError("Duplicate names not allowed. Duplicated value(s) are: " *
                            ":$(join(duplicate_names, ", "))"))
    end

    # Put the summary stats into the return data frame
    data = DataFrame()
    data.variable = propertynames(df)

    # An array of Dicts for summary statistics
    col_stats_dicts = map(eachcol(df)) do col
        if eltype(col) >: Missing
            t = skipmissing(col)
            d = get_stats(t, predefined_funs)
            get_stats!(d, t, custom_funs)
        else
            d = get_stats(col, predefined_funs)
            get_stats!(d, col, custom_funs)
        end

        if :nmissing in predefined_funs
            d[:nmissing] = count(ismissing, col)
        end

        if :nnonmissing in predefined_funs
            d[:nnonmissing] = count(!ismissing, col)
        end

        if :first in predefined_funs
            d[:first] = isempty(col) ? nothing : first(col)
        end

        if :last in predefined_funs
            d[:last] = isempty(col) ? nothing : last(col)
        end

        if :eltype in predefined_funs
            d[:eltype] = eltype(col)
        end

        return d
    end

    for stat in ordered_names
        # for each statistic, loop through the columns array to find values
        # letting the comprehension choose the appropriate type
        data[!, stat] = [d[stat] for d in col_stats_dicts]
    end

    return data
end

# Compute summary statistics
# use a dict because we don't know which measures the user wants
# Outside of the `describe` function due to something with 0.7
function get_stats(@nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                   stats::AbstractVector{Symbol})
    d = Dict{Symbol, Any}()

    if :q25 in stats || :median in stats || :q75 in stats
        # types that do not support basic arithmetic (like strings) will only fail
        # after sorting the data, so check this beforehand to fail early
        T = eltype(col)
        if isconcretetype(T) && !hasmethod(-, Tuple{T, T})
            d[:q25] = d[:median] = d[:q75] = nothing
        else
            mcol = Base.copymutable(col)
            if :q25 in stats
                d[:q25] = try quantile!(mcol, 0.25) catch; nothing; end
            end
            if :median in stats
                d[:median] = try quantile!(mcol, 0.50) catch; nothing; end
            end
            if :q75 in stats
                d[:q75] = try quantile!(mcol, 0.75) catch; nothing; end
            end
        end
    end

    if :min in stats || :max in stats
        ex = try extrema(col) catch; (nothing, nothing) end
        d[:min] = ex[1]
        d[:max] = ex[2]
    end

    if :mean in stats || :std in stats
        m = try mean(col) catch end
        # we can add non-necessary things to d, because we choose what we need
        # in the main function
        d[:mean] = m

        if :std in stats
            d[:std] = try std(col, mean = m) catch end
        end
    end

    if :nunique in stats
        if eltype(col) <: Real
            d[:nunique] = nothing
        else
            d[:nunique] = try length(Set(col)) catch end
        end
    end

    if :nuniqueall in stats
        d[:nuniqueall] = try length(Set(col)) catch end
    end

    if :sum in stats
        d[:sum] = try sum(col) catch end
    end

    return d
end

function get_stats!(d::Dict, @nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                    stats::Vector{Any})
    for stat in stats
        d[stat[2]] = try stat[1](col) catch end
    end
end

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
    # Identify Bool mask of which rows have no missings
    rowidxs = completecases(df, cols)
    if view
        if disallowmissing
            throw(ArgumentError("disallowmissing=true is incompatible with view=true"))
        end
        return Base.view(df, rowidxs, :)
    else
        # Faster when there are many columns (indexing with integers than via Bool mask)
        # or when there are many missings (as we skip a lot of iterations)
        selected_rows = _findall(rowidxs)
        new_columns = Vector{AbstractVector}(undef, ncol(df))

        # What column indices should disallowmissing be applied to
        cols_inds = BitSet(index(df)[cols])

        use_threads = Threads.nthreads() > 1 && ncol(df) > 1 && length(selected_rows) >= 100_000
        @sync for (i, col) in enumerate(eachcol(df))
            @spawn_or_run use_threads if disallowmissing && (i in cols_inds) &&
                          (Missing <: eltype(col) && eltype(col) !== Any)
                # Perform this path only if column eltype allows missing values
                # except Any, as nonmissingtype(Any) == Any.
                # Under these conditions Missings.disallowmissing must allocate
                # a fresh column
                col_sel = Base.view(col, selected_rows)
                new_col = Missings.disallowmissing(col_sel)
                @assert new_col !== col_sel
                new_columns[i] = new_col
            else
                new_columns[i] = col[selected_rows]
            end
        end

        newdf = DataFrame(new_columns, copy(index(df)), copycols=false)

        _copy_all_note_metadata!(newdf, df)
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

"""
    filter(fun, df::AbstractDataFrame; view::Bool=false)
    filter(cols => fun, df::AbstractDataFrame; view::Bool=false)

Return a data frame containing only rows from `df` for which `fun` returns
`true`.

If `cols` is not specified then the predicate `fun` is passed `DataFrameRow`s.
Elements of a `DataFrameRow` may be accessed with dot syntax or column indexing inside `fun`.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

If `view=false` a freshly allocated `DataFrame` is returned. If `view=true` then
a `SubDataFrame` view into `df` is returned.

Passing `cols` leads to a more efficient execution of the operation for large
data frames.

!!! note

    This method is defined so that DataFrames.jl implements the Julia API for
    collections, but it is generally recommended to use the [`subset`](@ref)
    function instead as it is consistent with other DataFrames.jl functions
    (as opposed to `filter`).

!!! note

    Due to type stability the `filter(cols => fun, df::AbstractDataFrame; view::Bool=false)`
    call is preferred in performance critical applications.

$METADATA_FIXED

See also: [`filter!`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter(row -> row.x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter(row -> row["x"] > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter(:x => x -> x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter([:x, :y] => (x, y) -> x == 1 || y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> filter(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
@inline function Base.filter(f, df::AbstractDataFrame; view::Bool=false)
    rowidxs = _filter_helper(f, eachrow(df))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

@inline function Base.filter((cols, f)::Pair, df::AbstractDataFrame; view::Bool=false)
    int_cols = index(df)[cols] # it will be AbstractVector{Int} or Int
    if length(int_cols) == 0
        rowidxs = [f() for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper(f, (df[!, i] for i in int_cols)...)
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

# this method is needed to allow for passing duplicate columns
@inline function Base.filter((cols, f)::Pair{<:Union{AbstractVector{<:Integer},
                                                     AbstractVector{<:AbstractString},
                                                     AbstractVector{<:Symbol}}},
                             df::AbstractDataFrame; view::Bool=false)
    if length(cols) == 0
        rowidxs = [f() for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper(f, (df[!, i] for i in cols)...)
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

_filter_helper(f, cols...)::AbstractVector{Bool} = ((x...) -> f(x...)::Bool).(cols...)

@inline function Base.filter((cols, f)::Pair{AsTable}, df::AbstractDataFrame;
                             view::Bool=false)
    cols = index(df)[cols.cols]
    df_tmp = select(df, cols, copycols=false)
    if ncol(df_tmp) == 0
        rowidxs = [f(NamedTuple()) for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper_astable(f, Tables.namedtupleiterator(df_tmp))
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

_filter_helper_astable(f, nti::Tables.NamedTupleIterator)::AbstractVector{Bool} = (x -> f(x)::Bool).(nti)

"""
    filter!(fun, df::AbstractDataFrame)
    filter!(cols => fun, df::AbstractDataFrame)

Remove rows from data frame `df` for which `fun` returns `false`.

If `cols` is not specified then the predicate `fun` is passed `DataFrameRow`s.
Elements of a `DataFrameRow` may be accessed with dot syntax or column indexing inside `fun`.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

Passing `cols` leads to a more efficient execution of the operation for large
data frames.

!!! note

    This method is defined so that DataFrames.jl implements the Julia API for
    collections, but it is generally recommended to use the [`subset!`](@ref)
    function instead as it is consistent with other DataFrames.jl functions
    (as opposed to `filter!`).

!!! note

    Due to type stability the `filter!(cols => fun, df::AbstractDataFrame)`
    call is preferred in performance critical applications.

$METADATA_FIXED

See also: [`filter`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter!(row -> row.x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter!(row -> row["x"] > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter!(:x => x -> x == 3, df)
1×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b

julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"]);

julia> filter!([:x, :y] => (x, y) -> x == 1 || y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"]);

julia> filter!(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
Base.filter!(f::Function, df::AbstractDataFrame) = deleteat!(df, findall(!f, eachrow(df)))
Base.filter!((col, f)::Pair{<:ColumnIndex}, df::AbstractDataFrame) =
    _filter!_helper(df, f, df[!, col])
Base.filter!((cols, f)::Pair{<:AbstractVector{Symbol}}, df::AbstractDataFrame) =
    filter!([index(df)[col] for col in cols] => f, df)
Base.filter!((cols, f)::Pair{<:AbstractVector{<:AbstractString}}, df::AbstractDataFrame) =
    filter!([index(df)[col] for col in cols] => f, df)
Base.filter!((cols, f)::Pair, df::AbstractDataFrame) =
    filter!(index(df)[cols] => f, df)
Base.filter!((cols, f)::Pair{<:AbstractVector{Int}}, df::AbstractDataFrame) =
    _filter!_helper(df, f, (df[!, i] for i in cols)...)

function _filter!_helper(df::AbstractDataFrame, f, cols...)
    if length(cols) == 0
        rowidxs = findall(x -> !f(), axes(df, 1))
    else
        rowidxs = findall(((x...) -> !(f(x...)::Bool)).(cols...))
    end
    return deleteat!(df, rowidxs)
end

function Base.filter!((cols, f)::Pair{<:AsTable}, df::AbstractDataFrame)
    cols = index(df)[cols.cols]
    dff = select(df, cols, copycols=false)
    if ncol(dff) == 0
        return deleteat!(df, findall(x -> !f(NamedTuple()), axes(df, 1)))
    else
        return _filter!_helper_astable(df, Tables.namedtupleiterator(dff), f)
    end
end

_filter!_helper_astable(df::AbstractDataFrame, nti::Tables.NamedTupleIterator, f) =
    deleteat!(df, _findall((x -> !(f(x)::Bool)).(nti)))

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

    # we use hashing algorithm here, because we assume that the tables we work with are not huge
    has_duplicates = row_group_slots!(ntuple(i -> df[!, colind[i]], length(colind)),
                                      Val(false), nothing, false, nothing, true)[1] != nrow(df)
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
    target_rows = Int(prod(x -> BigInt(length(x)), uniquevals))
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

"""
    hcat(df::AbstractDataFrame...;
         makeunique::Bool=false, copycols::Bool=true)

Horizontally concatenate data frames.

If `makeunique=false` (the default) column names of passed objects must be unique.
If `makeunique=true` then duplicate column names will be suffixed
with `_i` (`i` starting at 1 for the first duplicate).

If `copycols=true` (the default) then the `DataFrame` returned by `hcat` will
contain copied columns from the source data frames.
If `copycols=false` then it will contain columns as they are stored in the
source (without copying). This option should be used with caution as mutating
either the columns in sources or in the returned `DataFrame` might lead to
the corruption of the other object.

Metadata: `hcat` propagates table-level `:note`-style metadata for keys that are present
in all passed data frames and have the same value;
it propagates column-level `:note`-style metadata.

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

julia> df3 = hcat(df1, df2, makeunique=true)
3×4 DataFrame
 Row │ A      B      A_1    B_1
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      1      4      4
   2 │     2      2      5      5
   3 │     3      3      6      6

julia> df3.A === df1.A
false

julia> df3 = hcat(df1, df2, makeunique=true, copycols=false);

julia> df3.A === df1.A
true
```
"""
function Base.hcat(df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true)
    df = DataFrame(df, copycols=copycols)
    _drop_all_nonnote_metadata!(df)
    return df
end

# TODO: after deprecation remove AbstractVector methods
Base.hcat(df::AbstractDataFrame, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df, copycols=copycols), x, makeunique=makeunique, copycols=copycols)
Base.hcat(x::AbstractVector, df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(x, df, makeunique=makeunique, copycols=copycols)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df1, copycols=copycols), df2,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df::AbstractDataFrame, x::Union{AbstractVector, AbstractDataFrame},
          y::Union{AbstractVector, AbstractDataFrame}...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(df, x, makeunique=makeunique, copycols=copycols), y...,
          makeunique=makeunique, copycols=copycols)

"""
    vcat(dfs::AbstractDataFrame...;
         cols::Union{Symbol, AbstractVector{Symbol},
                     AbstractVector{<:AbstractString}}=:setequal,
         source::Union{Nothing, Symbol, AbstractString,
                       Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Vertically concatenate `AbstractDataFrame`s.

The `cols` keyword argument determines the columns of the returned data frame:

* `:setequal`: require all data frames to have the same column names
  disregarding order. If they appear in different orders, the order of the first
  provided data frame is used.
* `:orderequal`: require all data frames to have the same column names and in
  the same order.
* `:intersect`: only the columns present in *all* provided data frames are kept.
  If the intersection is empty, an empty data frame is returned.
* `:union`: columns present in *at least one* of the provided data frames are
  kept. Columns not present in some data frames are filled with `missing` where
  necessary.
* A vector of `Symbol`s or strings: only listed columns are kept. Columns not
  present in some data frames are filled with `missing` where necessary.

The `source` keyword argument, if not `nothing` (the default), specifies the
additional column to be added in the last position in the resulting data frame
that will identify the source data frame. It can be a `Symbol` or an
`AbstractString`, in which case the identifier will be the number of the passed
source data frame, or a `Pair` consisting of a `Symbol` or an `AbstractString`
and of a vector specifying the data frame identifiers (which do not have to be
unique). The name of the source column is not allowed to be present in any
source data frame.

The order of columns is determined by the order they appear in the included data
frames, searching through the header of the first data frame, then the second,
etc.

The element types of columns are determined using `promote_type`, as with `vcat`
for `AbstractVector`s.

`vcat` ignores empty data frames when composing the result (except for
metadata), making it possible to initialize an empty data frame at the beginning
of a loop and `vcat` onto it.

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

julia> df4 = DataFrame()
0×0 DataFrame

julia> vcat(df1, df2)
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

julia> vcat(df1, df3, cols=:union)
6×3 DataFrame
 Row │ A      B        C
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     1        1  missing
   2 │     2        2  missing
   3 │     3        3  missing
   4 │     7  missing        7
   5 │     8  missing        8
   6 │     9  missing        9

julia> vcat(df1, df3, cols=:intersect)
6×1 DataFrame
 Row │ A
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     7
   5 │     8
   6 │     9

julia> vcat(df4, df1)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> vcat(df1, df2, df3, df4, cols=:union, source="source")
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

julia> vcat(df1, df2, df4, df3, cols=:union, source=:source => 'a':'d')
9×4 DataFrame
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Char
─────┼─────────────────────────────────
   1 │     1        1  missing  a
   2 │     2        2  missing  a
   3 │     3        3  missing  a
   4 │     4        4  missing  b
   5 │     5        5  missing  b
   6 │     6        6  missing  b
   7 │     7  missing        7  d
   8 │     8  missing        8  d
   9 │     9  missing        9  d
```
"""
Base.vcat(dfs::AbstractDataFrame...;
          cols::Union{Symbol, AbstractVector{Symbol},
                      AbstractVector{<:AbstractString}}=:setequal,
          source::Union{Nothing, SymbolOrString,
                           Pair{<:SymbolOrString, <:AbstractVector}}=nothing) =
    reduce(vcat, dfs; cols=cols, source=source)

"""
    repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)

Construct a data frame by repeating rows in `df`. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated.

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

julia> repeat(df, inner=2, outer=3)
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
function Base.repeat(df::AbstractDataFrame; inner::Integer=1, outer::Integer=1)
    inner < 0 && throw(ArgumentError("inner keyword argument must be non-negative"))
    outer < 0 && throw(ArgumentError("outer keyword argument must be non-negative"))
    return mapcols(x -> repeat(x, inner = Int(inner), outer = Int(outer)), df)
end

"""
    repeat(df::AbstractDataFrame, count::Integer)

Construct a data frame by repeating each row in `df` the number of times
specified by `count`.

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
function Base.repeat(df::AbstractDataFrame, count::Integer)
    count < 0 && throw(ArgumentError("count must be non-negative"))
    return mapcols(x -> repeat(x, Int(count)), df)
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
    flatten(df::AbstractDataFrame, cols; scalar::Type=Union{})

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

If `scalar` is passed then values that have this type in flattened columns
are treated as scalars and broadcasted as many times as is needed to match
lengths of values stored in other columns. If all values in a row are scalars,
a single row is produced.

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

julia> df4 = DataFrame(a=[1, 2, 3],
                       b=[[1, 2], missing, missing],
                       c=[[5, 6], missing, [7, 8]])
3×3 DataFrame
 Row │ a      b        c
     │ Int64  Array…?  Array…?
─────┼─────────────────────────
   1 │     1  [1, 2]   [5, 6]
   2 │     2  missing  missing
   3 │     3  missing  [7, 8]

julia> flatten(df4, [:b, :c], scalar=Missing)
5×3 DataFrame
 Row │ a      b        c
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     1        1        5
   2 │     1        2        6
   3 │     2  missing  missing
   4 │     3  missing        7
   5 │     3  missing        8
```
"""
function flatten(df::AbstractDataFrame,
                 cols::Union{ColumnIndex, MultiColumnIndex};
                 scalar::Type=Union{})
    _check_consistency(df)

    idxcols = index(df)[cols]
    if isempty(idxcols)
        cdf = copy(df)
        _drop_all_nonnote_metadata!(cdf)
        return cdf
    end

    col1 = first(idxcols)
    lengths = Int[x isa scalar ? -1 : length(x) for x in df[!, col1]]
    for (i, coli) in enumerate(idxcols)
        i == 1 && continue
        update_lengths!(lengths, df[!, coli], scalar, df, col1, coli)
    end

    # handle case where in all columns we had a scalar
    # in this case we keep it one time
    for i in 1:length(lengths)
        lengths[i] == -1 && (lengths[i] = 1)
    end

    new_df = similar(df[!, Not(cols)], sum(lengths))
    for name in _names(new_df)
        repeat_lengths!(new_df[!, name], df[!, name], lengths)
    end
    length(idxcols) > 1 && sort!(idxcols)
    for col in idxcols
        col_to_flatten = df[!, col]
        fast_path = eltype(col_to_flatten) <: AbstractVector &&
                    !isempty(col_to_flatten)
        flattened_col = if fast_path
                reduce(vcat, col_to_flatten)
            elseif scalar === Union{}
                collect(Iterators.flatten(col_to_flatten))
            else
                collect(Iterators.flatten(v isa scalar ? Iterators.repeated(v, l) : v
                                          for (l, v) in zip(lengths, col_to_flatten)))
            end
        insertcols!(new_df, col, _names(df)[col] => flattened_col)
    end

    _copy_all_note_metadata!(new_df, df)
    return new_df
end

function update_lengths!(lengths::Vector{Int}, col::AbstractVector, scalar::Type,
                         df::AbstractDataFrame, col1::Integer, coli::Integer)
    for (i, v) in enumerate(col)
        v isa scalar && continue
        lv = length(v)
        if lengths[i] == -1
            lengths[i] = lv
        elseif lengths[i] != lv
            colnames = _names(df)
            throw(ArgumentError("Lengths of iterables stored in columns :$(colnames[col1]) " *
                                "and :$(colnames[coli]) are not the same in row $i"))
        end
    end
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

"""
    reverse(df::AbstractDataFrame, start=1, stop=nrow(df))

Return a data frame containing the rows in `df` in reversed order.
If `start` and `stop` are provided, only rows in the `start:stop` range are affected.

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> reverse(df)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     4      9     14
   3 │     3      8     13
   4 │     2      7     12
   5 │     1      6     11

julia> reverse(df, 2, 3)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     3      8     13
   3 │     2      7     12
   4 │     4      9     14
   5 │     5     10     15
```
"""
Base.reverse(df::AbstractDataFrame, start::Integer=1, stop::Integer=nrow(df)) =
    mapcols(x -> reverse(x, start, stop), df)

"""
    reverse!(df::AbstractDataFrame, start=1, stop=nrow(df))

Mutate data frame in-place to reverse its row order.
If `start` and `stop` are provided, only rows in the `start:stop` range are affected.

`reverse!` will produce a correct result even if some columns of passed data frame
are identical (checked with `===`). Otherwise, if two columns share some part of
memory but are not identical (e.g. are different views of the same parent
vector) then `reverse!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> reverse!(df)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     4      9     14
   3 │     3      8     13
   4 │     2      7     12
   5 │     1      6     11

julia> reverse!(df, 2, 3)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     4      9     14
   4 │     2      7     12
   5 │     1      6     11
```
"""
function Base.reverse!(df::AbstractDataFrame, start::Integer=1, stop::Integer=nrow(df))
    _foreach_unique_column!(col -> reverse!(col, start, stop), df)
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function _foreach_unique_column!(f!::Function, df::AbstractDataFrame)
    seen_cols = IdDict{Any, Nothing}()
    for col in eachcol(df)
        if !haskey(seen_cols, col)
            seen_cols[col] = nothing
            f!(col)
        end
    end
    return nothing
end

function _permutation_helper!(fun::Union{typeof(permute!), typeof(invpermute!)},
                              df::AbstractDataFrame, p::AbstractVector{<:Integer})
    nrow(df) != length(p) &&
        throw(DimensionMismatch("Permutation does not have a correct length " *
                                "(expected $(nrow(df)) but got $(length(p)))"))

    cp = _compile_permutation!(Base.copymutable(p))

    if isempty(cp)
        _drop_all_nonnote_metadata!(parent(df))
        return df
    end

    if fun === invpermute!
        reverse!(@view cp[1:end-1])
    end

    _foreach_unique_column!(col -> _cycle_permute!(col, cp), df)

    _drop_all_nonnote_metadata!(parent(df))
    return df
end

# convert a classical permutation to zero terminated cycle
# notation, zeroing the original permutation in the process.
function _compile_permutation!(p::AbstractVector{<:Integer})
    firstindex(p) == 1 ||
        throw(ArgumentError("Permutation vectors must have 1-based indexing"))
    # this length is sufficient because we do not record 1-cycles,
    # so the worst case is all 2-cycles. One extra element gives the
    # algorithm leeway to defer error detection without unsafe reads.
    # trace _compile_permutation!([3,3,1]) for example.
    out = similar(p, 3 * length(p) ÷ 2 + 1)
    out_len = 0
    start = 0
    count = length(p)
    @inbounds while count > 0
        start = findnext(!iszero, p, start + 1)
        start isa Int || throw(ArgumentError("Passed vector p is not a valid permutation"))
        last_k = p[start]
        count -= 1
        last_k == start && continue
        out_len += 1
        out[out_len] = last_k
        p[start] = 0
        start < last_k <= length(p) || throw(ArgumentError("Passed vector p is not a valid permutation"))
        out_len += 1
        k = out[out_len] = p[last_k]
        while true
            count -= 1
            p[last_k] = 0
            last_k = k
            start <= k <= length(p) || throw(ArgumentError("Passed vector p is not a valid permutation"))
            out_len += 1
            k = out[out_len] = p[k]
            k == 0 && break
        end
        last_k == start || throw(ArgumentError("Passed vector p is not a valid permutation"))
    end
    return resize!(out, out_len)
end

# Permute a vector `v` based on a permutation `p` listed in zero terminated
# cycle notation. For example, the permutation 1 -> 2, 2 -> 3, 3 -> 1, 4 -> 6,
# 5 -> 5, 6 -> 4 is traditionally expressed as [2, 3, 1, 6, 5, 4] but in cycle
# notation is expressed as [1, 2, 3, 0, 4, 6, 0]
function _cycle_permute!(v::AbstractVector, p::AbstractVector{<:Integer})
    i = firstindex(p)
    @inbounds while i < lastindex(p)
        last_p_i = p[i]
        start = v[last_p_i]
        while true
            i += 1
            p_i = p[i]
            p_i == 0 && break
            v[last_p_i] = v[p_i]
            last_p_i = p_i
        end
        v[last_p_i] = start
        i += 1
    end
    return v
end

"""
    permute!(df::AbstractDataFrame, p)

Permute data frame `df` in-place, according to permutation `p`.
Throws `ArgumentError` if `p` is not a permutation.

To return a new data frame instead of permuting `df` in-place, use `df[p, :]`.

`permute!` will produce a correct result even if some columns of passed data frame
or permutation `p` are identical (checked with `===`). Otherwise, if two columns share
some part of memory but are not identical (e.g. are different views of the same parent
vector) then `permute!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> permute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     1      6     11
   4 │     2      7     12
   5 │     4      9     14
```
"""
Base.permute!(df::AbstractDataFrame, p::AbstractVector{<:Integer}) =
    _permutation_helper!(permute!, df, p)

"""
    invpermute!(df::AbstractDataFrame, p)

Like [`permute!`](@ref), but the inverse of the given permutation is applied.

`invpermute!` will produce a correct result even if some columns of passed data
frame or permutation `p` are identical (checked with `===`). Otherwise, if two
columns share some part of memory but are not identical (e.g. are different views
of the same parent vector) then `invpermute!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> permute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     1      6     11
   4 │     2      7     12
   5 │     4      9     14

julia> invpermute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15
```
"""
Base.invpermute!(df::AbstractDataFrame, p::AbstractVector{<:Integer}) =
    _permutation_helper!(invpermute!, df, p)

"""
    shuffle([rng=GLOBAL_RNG,] df::AbstractDataFrame)

Return a copy of `df` with randomly permuted rows.
The optional `rng` argument specifies a random number generator.

$METADATA_FIXED

# Examples

```jldoctest
julia> using Random, StableRNGs

julia> rng = StableRNG(1234);

julia> shuffle(rng, DataFrame(a=1:5, b=1:5))
5×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      2
   2 │     1      1
   3 │     3      3
   4 │     5      5
   5 │     4      4
```
"""
Random.shuffle(df::AbstractDataFrame) =
    df[randperm(nrow(df)), :]
Random.shuffle(r::AbstractRNG, df::AbstractDataFrame) =
    df[randperm(r, nrow(df)), :]

"""
    shuffle!([rng=GLOBAL_RNG,] df::AbstractDataFrame)

Randomly permute rows of `df` in-place.
The optional `rng` argument specifies a random number generator.

`shuffle!` will produce a correct result even if some columns of passed data frame
are identical (checked with `===`). Otherwise, if two columns share some part of
memory but are not identical (e.g. are different views of the same parent
vector) then `shuffle!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> using Random, StableRNGs

julia> rng = StableRNG(1234);

julia> shuffle!(rng, DataFrame(a=1:5, b=1:5))
5×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      2
   2 │     1      1
   3 │     3      3
   4 │     5      5
   5 │     4      4
```
"""
Random.shuffle!(df::AbstractDataFrame) =
    permute!(df, randperm(nrow(df)))
Random.shuffle!(r::AbstractRNG, df::AbstractDataFrame) =
    permute!(df, randperm(r, nrow(df)))

const INSERTCOLS_ARGUMENTS =
    """
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
    """

"""
    insertcols(df::AbstractDataFrame[, col], (name=>val)::Pair...;
               after::Bool=false, makeunique::Bool=false, copycols::Bool=true)

Insert a column into a copy of `df` data frame using the [`insertcols!`](@ref)
function and return the newly created data frame.

$INSERTCOLS_ARGUMENTS

$METADATA_FIXED

See also [`insertcols!`](@ref).

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

julia> insertcols(df, 1, :b => 'a':'c')
3×2 DataFrame
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols(df, :c => 2:4, :c => 3:5, makeunique=true)
3×3 DataFrame
 Row │ a      c      c_1
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3
   2 │     2      3      4
   3 │     3      4      5

julia> insertcols(df, :a, :d => 7:9, after=true)
3×2 DataFrame
 Row │ a      d
     │ Int64  Int64
─────┼──────────────
   1 │     1      7
   2 │     2      8
   3 │     3      9
```
"""
insertcols(df::AbstractDataFrame, args...;
           after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(copy(df), args...;
                after=after, makeunique=makeunique, copycols=copycols)

"""
    insertcols!(df::AbstractDataFrame[, col], (name=>val)::Pair...;
                after::Bool=false, makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data frame in place. Return the updated data frame.

$INSERTCOLS_ARGUMENTS

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

See also [`insertcols`](@ref).

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
function insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{Symbol}...;
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

    start_col_ind = col_ind
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

    delta = col_ind - start_col_ind
    colmetadata_dict = getfield(parent(df), :colmetadata)
    if !isnothing(colmetadata_dict) && delta > 0
        to_move = Int[i for i in keys(colmetadata_dict) if i >= start_col_ind]
        sort!(to_move, rev=true)
        for i in to_move
            colmetadata_dict[i + delta] = pop!(colmetadata_dict, i)
        end
    end
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

insertcols!(df::AbstractDataFrame, col::ColumnIndex, name_cols::Pair{<:AbstractString}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, col, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{Symbol}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, ncol(df)+1, name_cols..., after=after,
                makeunique=makeunique, copycols=copycols)

insertcols!(df::AbstractDataFrame, name_cols::Pair{<:AbstractString}...;
            after::Bool=false, makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(df, (Symbol(n) => v for (n, v) in name_cols)...,
                after=after, makeunique=makeunique, copycols=copycols)

function insertcols!(df::AbstractDataFrame, col::ColumnIndex; after::Bool=false,
                     makeunique::Bool=false, copycols::Bool=true)
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

    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function insertcols!(df::AbstractDataFrame; after::Bool=false,
                     makeunique::Bool=false, copycols::Bool=true)
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

"""
    Iterators.partition(df::AbstractDataFrame, n::Integer)

Iterate over `df` data frame `n` rows at a time, returning each block
as a `SubDataFrame`.

# Examples

```jldoctest
julia> collect(Iterators.partition(DataFrame(x=1:5), 2))
3-element Vector{SubDataFrame{DataFrame, DataFrames.Index, UnitRange{Int64}}}:
 2×1 SubDataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     1
   2 │     2
 2×1 SubDataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     3
   2 │     4
 1×1 SubDataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     5
```
"""
function Iterators.partition(df::AbstractDataFrame, n::Integer)
    n < 1 && throw(ArgumentError("cannot create partitions of length $n"))
    return Iterators.PartitionIterator(df, Int(n))
end

# use autodetection of eltype
Base.IteratorEltype(::Type{<:Iterators.PartitionIterator{<:AbstractDataFrame}}) =
    Base.EltypeUnknown()

# we do not need to be overly specific here as we rely on autodetection of eltype
# this method is needed only to override the fallback for `PartitionIterator`
Base.eltype(::Type{<:Iterators.PartitionIterator{<:AbstractDataFrame}}) =
    AbstractDataFrame

IteratorSize(::Type{<:Iterators.PartitionIterator{<:AbstractDataFrame}}) =
    Base.HasLength()

function Base.length(itr::Iterators.PartitionIterator{<:AbstractDataFrame})
    l = nrow(itr.c)
    return cld(l, itr.n)
end

function Base.iterate(itr::Iterators.PartitionIterator{<:AbstractDataFrame}, state::Int=1)
    last_idx = nrow(itr.c)
    state > last_idx && return nothing
    r = min(state + itr.n - 1, last_idx)
    return view(itr.c, state:r, :), r + 1
end
