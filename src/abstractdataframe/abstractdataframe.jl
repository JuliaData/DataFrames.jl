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

##############################################################################
##
## Basic properties of a DataFrame
##
##############################################################################

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
* any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR)
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
    return df
end

function rename!(df::AbstractDataFrame, vals::AbstractVector{<:AbstractString};
                 makeunique::Bool=false)
    rename!(index(df), Symbol.(vals), makeunique=makeunique)
    return df
end

function rename!(df::AbstractDataFrame, args::AbstractVector{Pair{Symbol, Symbol}})
    rename!(index(df), args)
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
    return df
end

function rename!(df::AbstractDataFrame,
                 args::Union{AbstractVector{<:Pair{<:Integer, <:AbstractString}},
                             AbstractVector{<:Pair{<:Integer, Symbol}},
                             AbstractDict{<:Integer, <:AbstractString},
                             AbstractDict{<:Integer, Symbol}})
    rename!(index(df), [_names(df)[from] => Symbol(to) for (from, to) in args])
    return df
end

rename!(df::AbstractDataFrame, args::Pair...) = rename!(df, collect(args))

function rename!(f::Function, df::AbstractDataFrame)
    rename!(f, index(df))
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

Base.isempty(df::AbstractDataFrame) = size(df, 1) == 0 || size(df, 2) == 0

if VERSION < v"1.6"
    Base.firstindex(df::AbstractDataFrame, i::Integer) = first(axes(df, i))
    Base.lastindex(df::AbstractDataFrame, i::Integer) = last(axes(df, i))
end
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
"""
function Base.similar(df::AbstractDataFrame, rows::Integer = size(df, 1))
    rows < 0 && throw(ArgumentError("the number of rows must be non-negative"))
    DataFrame(AbstractVector[similar(x, rows) for x in eachcol(df)], copy(index(df)),
              copycols=false)
end

"""
    empty(df::AbstractDataFrame)

Create a new `DataFrame` with the same column names and column element types
as `df` but with zero rows.
"""
Base.empty(df::AbstractDataFrame) = similar(df, 0)

##############################################################################
##
## Equality
##
##############################################################################

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
##############################################################################
##
## Description
##
##############################################################################

"""
    only(df::AbstractDataFrame)

If `df` has a single row return it as a `DataFrameRow`; otherwise throw `ArgumentError`.
"""
function only(df::AbstractDataFrame)
    nrow(df) != 1 && throw(ArgumentError("data frame must contain exactly 1 row"))
    return df[1, :]
end

"""
    first(df::AbstractDataFrame)

Get the first row of `df` as a `DataFrameRow`.
"""
Base.first(df::AbstractDataFrame) = df[1, :]

"""
    first(df::AbstractDataFrame, n::Integer; view::Bool=false)

Get a data frame with the `n` first rows of `df`.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.
"""
@inline Base.first(df::AbstractDataFrame, n::Integer; view::Bool=false) =
    view ? Base.view(df, 1:min(n ,nrow(df)), :) : df[1:min(n, nrow(df)), :]

"""
    last(df::AbstractDataFrame)

Get the last row of `df` as a `DataFrameRow`.
"""
Base.last(df::AbstractDataFrame) = df[nrow(df), :]

"""
    last(df::AbstractDataFrame, n::Integer; view::Bool=false)

Get a data frame with the `n` last rows of `df`.

If `view=false` a freshly allocated `DataFrame` is returned.
If `view=true` then a `SubDataFrame` view into `df` is returned.
"""
@inline Base.last(df::AbstractDataFrame, n::Integer; view::Bool=false) =
    view ? Base.view(df, max(1, nrow(df)-n+1):nrow(df), :) : df[max(1, nrow(df)-n+1):nrow(df), :]


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
      `:median`, `:q75`, `:max`, `:eltype`, `:nunique`, `:first`, `:last`, and
      `:nmissing`. The default statistics used are `:mean`, `:min`, `:median`,
      `:max`, `:nmissing`, and `:eltype`.
    - `:detailed` as the only `Symbol` argument to return all statistics
      except `first` and `last`.
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
`:nunique` will return `nothing`s.

Missing values are filtered in the calculation of all statistics, however the
column `:nmissing` will report the number of missing values of that variable.

If custom functions are provided, they are called repeatedly with the vector
corresponding to each column as the only argument. For columns allowing for
missing values, the vector is wrapped in a call to `skipmissing`: custom
functions must therefore support such objects (and not only vectors), and cannot
access missing values.

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
    _describe(select(df, cols, copycols=false), Any[s for s in stats])

DataAPI.describe(df::AbstractDataFrame; cols=:) =
    _describe(select(df, cols, copycols=false),
              Any[:mean, :min, :median, :max, :nmissing, :eltype])

function _describe(df::AbstractDataFrame, stats::AbstractVector)
    predefined_funs = Symbol[s for s in stats if s isa Symbol]

    allowed_fields = [:mean, :std, :min, :q25, :median, :q75,
                      :max, :nunique, :nmissing, :first, :last, :eltype]

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

    return d
end

function get_stats!(d::Dict, @nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                    stats::Vector{Any})
    for stat in stats
        d[stat[2]] = try stat[1](col) catch end
    end
end


##############################################################################
##
## Miscellaneous
##
##############################################################################

"""
    completecases(df::AbstractDataFrame, cols=:)

Return a Boolean vector with `true` entries indicating rows without missing values
(complete cases) in data frame `df`.

If `cols` is provided, only missing values in the corresponding columns areconsidered.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

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
function completecases(df::AbstractDataFrame, col::Colon=:)
    if ncol(df) == 0
        throw(ArgumentError("Unable to compute complete cases of a " *
                            "data frame with no columns"))
    end
    res = trues(size(df, 1))
    aux = BitVector(undef, size(df, 1))
    for i in 1:size(df, 2)
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

completecases(df::AbstractDataFrame, cols::MultiColumnIndex) =
    completecases(df[!, cols])

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
    deleteat!(df, inds)
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
    df_tmp = select(df, cols.cols, copycols=false)
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
    dff = select(df, cols.cols, copycols=false)
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
    nonunique(df::AbstractDataFrame)
    nonunique(df::AbstractDataFrame, cols)

Return a `Vector{Bool}` in which `true` entries indicate duplicate rows.
A row is a duplicate if there exists a prior row with all columns containing
equal values (according to `isequal`).

See also [`unique`](@ref) and [`unique!`](@ref).

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to compare.
  Can be any column selector or transformation accepted by [`select`](@ref).

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
    if ncol(df) == 0
        throw(ArgumentError("finding duplicate rows in data frame with no " *
                            "columns is not allowed"))
    end
    gslots = row_group_slots(ntuple(i -> df[!, i], ncol(df)), Val(true), nothing, false, nothing)[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(df))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

nonunique(df::AbstractDataFrame, cols) = nonunique(select(df, cols, copycols=false))

Base.unique!(df::AbstractDataFrame) = deleteat!(df, _findall(nonunique(df)))
Base.unique!(df::AbstractDataFrame, cols::AbstractVector) =
    deleteat!(df, _findall(nonunique(df, cols)))
Base.unique!(df::AbstractDataFrame, cols) =
    deleteat!(df, _findall(nonunique(df, cols)))

# Unique rows of an AbstractDataFrame.
@inline function Base.unique(df::AbstractDataFrame; view::Bool=false)
    rowidxs = (!).(nonunique(df))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

@inline function Base.unique(df::AbstractDataFrame, cols; view::Bool=false)
    rowidxs = (!).(nonunique(df, cols))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

"""
    unique(df::AbstractDataFrame; view::Bool=false)
    unique(df::AbstractDataFrame, cols; view::Bool=false)
    unique!(df::AbstractDataFrame)
    unique!(df::AbstractDataFrame, cols)

Return a data frame containing only the first occurrence of unique rows in `df`.
When `cols` is specified, the returned `DataFrame` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).


For `unique`, if `view=false` a freshly allocated `DataFrame` is returned,
and if `view=true` then a `SubDataFrame` view into `df` is returned.

`unique!` updates `df` in-place and does not support the `view` keyword argument.

See also [`nonunique`](@ref).

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (Symbol, Int, Vector{Symbol}, Regex, etc.)
specifying the column(s) to compare.

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
(unique, unique!)

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
Base.hcat(df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true) =
    DataFrame(df, copycols=copycols)
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

* `:setequal`: require all data frames to have the same column names disregarding
  order. If they appear in different orders, the order of the first provided data
  frame is used.
* `:orderequal`: require all data frames to have the same column names and in the
  same order.
* `:intersect`: only the columns present in *all* provided data frames are kept.
  If the intersection is empty, an empty data frame is returned.
* `:union`: columns present in *at least one* of the provided data frames are kept.
  Columns not present in some data frames are filled with `missing` where necessary.
* A vector of `Symbol`s or strings: only listed columns are kept.
  Columns not present in some data frames are filled with `missing` where necessary.

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

The element types of columns are determined using `promote_type`,
as with `vcat` for `AbstractVector`s.

`vcat` ignores empty data frames, making it possible to initialize an empty
data frame at the beginning of a loop and `vcat` onto it.

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
    reduce(::typeof(vcat),
           dfs::Union{AbstractVector{<:AbstractDataFrame},
                      Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}};
           cols::Union{Symbol, AbstractVector{Symbol},
                       AbstractVector{<:AbstractString}}=:setequal,
           source::Union{Nothing, Symbol, AbstractString,
                         Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Efficiently reduce the given vector or tuple of `AbstractDataFrame`s with `vcat`.

The column order, names, and types of the resulting `DataFrame`, and
the behavior of `cols` and `source` keyword arguments follow the rules specified
for [`vcat`](@ref) of `AbstractDataFrame`s.

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
                     dfs::Union{AbstractVector{<:AbstractDataFrame},
                                Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}};
                     cols::Union{Symbol, AbstractVector{Symbol},
                                 AbstractVector{<:AbstractString}}=:setequal,
                     source::Union{Nothing, SymbolOrString,
                                   Pair{<:SymbolOrString, <:AbstractVector}}=nothing)
    res = _vcat(AbstractDataFrame[df for df in dfs if ncol(df) != 0]; cols=cols)
    if source !== nothing
        len = length(dfs)
        if source isa SymbolOrString
            col, vals = source, 1:len
        else
            @assert source isa Pair{<:SymbolOrString, <:AbstractVector}
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

function _vcat(dfs::AbstractVector{AbstractDataFrame};
               cols::Union{Symbol, AbstractVector{Symbol},
                           AbstractVector{<:AbstractString}}=:setequal)

    isempty(dfs) && return DataFrame()
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
            let header=header     # julia #15276
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

    length(header) == 0 && return DataFrame()
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
    return DataFrame(all_cols, header, copycols=false)
end

"""
    repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)

Construct a data frame by repeating rows in `df`. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated.

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

## Documentation for methods defined elsewhere

function nrow end
function ncol end

"""
    nrow(df::AbstractDataFrame)
    ncol(df::AbstractDataFrame)

Return the number of rows or columns in an `AbstractDataFrame` `df`.

See also [`size`](@ref).

**Examples**

```jldoctest
julia> df = DataFrame(i=1:10, x=rand(10), y=rand(["a", "b", "c"], 10));

julia> size(df)
(10, 3)

julia> nrow(df)
10

julia> ncol(df)
3
```

"""
(nrow, ncol)

"""
    disallowmissing(df::AbstractDataFrame, cols=:; error::Bool=true)

Return a copy of data frame `df` with columns `cols` converted
from element type `Union{T, Missing}` to `T` to drop support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.

If `error=false` then columns containing a `missing` value will be skipped instead
of throwing an error.

**Examples**

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
    return DataFrame(newcols, _names(df), copycols=false)
end

"""
    allowmissing(df::AbstractDataFrame, cols=:)

Return a copy of data frame `df` with columns `cols` converted
to element type `Union{T, Missing}` from `T` to allow support for missing values.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `cols` is omitted all columns in the data frame are converted.

**Examples**

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
    return DataFrame(newcols, _names(df), copycols=false)
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
    isempty(idxcols) && return copy(df)
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
        flattened_col = col_to_flatten isa AbstractVector{<:AbstractVector} ?
            reduce(vcat, col_to_flatten) :
            collect(Iterators.flatten(col_to_flatten))

        insertcols!(new_df, col, _names(df)[col] => flattened_col)
    end

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

# Disallowed operations that are a common mistake

Base.getindex(::AbstractDataFrame, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax df[column] is not supported use df[!, column] instead"))

Base.setindex!(::AbstractDataFrame, ::Any, ::Union{Symbol, Integer, AbstractString}) =
    throw(ArgumentError("syntax df[column] is not supported use df[!, column] instead"))

"""
    reverse(df::AbstractDataFrame)

Return a data frame containing the rows in `df` in reversed order.

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
```
"""
Base.reverse(df::AbstractDataFrame) = df[nrow(df):-1:1, :]
