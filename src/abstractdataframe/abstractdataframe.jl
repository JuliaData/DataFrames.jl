"""
    AbstractDataFrame

An abstract type for which all concrete types expose an interface
for working with tabular data.

**Common methods**

An AbstractDataFrame is a two-dimensional table with Symbols for
column names. An AbstractDataFrame is also similar to an Associative
type in that it allows indexing by a key (the columns).

The following are normally implemented for AbstractDataFrames:

* [`describe`](@ref) : summarize columns
* [`summary`](@ref) : show number of rows and columns
* `hcat` : horizontal concatenation
* `vcat` : vertical concatenation
* [`repeat`](@ref) : repeat rows
* `names` : columns names
* [`names!`](@ref) : set columns names
* [`rename!`](@ref) : rename columns names based on keyword arguments
* [`eltypes`](@ref) : `eltype` of each column
* `length` : number of columns
* `size` : (nrows, ncols)
* [`first`](@ref) : first `n` rows
* [`last`](@ref) : last `n` rows
* `convert` : convert to an array
* [`completecases`](@ref) : boolean vector of complete cases (rows with no missings)
* [`dropmissing`](@ref) : remove rows with missing values
* [`dropmissing!`](@ref) : remove rows with missing values in-place
* [`nonunique`](@ref) : indexes of duplicate rows
* [`unique!`](@ref) : remove duplicate rows
* `similar` : a DataFrame with similar columns as `d`
* `filter` : remove rows
* `filter!` : remove rows in-place

**Indexing**

Table columns are accessed (`getindex`) by a single index that can be
a symbol identifier, an integer, or a vector of each. If a single
column is selected, just the column object is returned. If multiple
columns are selected, some AbstractDataFrame is returned.

```julia
d[:colA]
d[3]
d[[:colA, :colB]]
d[[1:3; 5]]
```

Rows and columns can be indexed like a `Matrix` with the added feature
of indexing columns by name.

```julia
d[1:3, :colA]
d[3,3]
d[3,:]
d[3,[:colA, :colB]]
d[:, [:colA, :colB]]
d[[1:3; 5], :]
```

`setindex` works similarly.
"""
abstract type AbstractDataFrame end

##############################################################################
##
## Interface (not final)
##
##############################################################################

# index(df) => AbstractIndex
# nrow(df) => Int
# ncol(df) => Int
# getindex(...)
# setindex!(...) exclusive of methods that add new columns

##############################################################################
##
## Basic properties of a DataFrame
##
##############################################################################

Base.names(df::AbstractDataFrame) = names(index(df))
_names(df::AbstractDataFrame) = _names(index(df))

Compat.hasproperty(df::AbstractDataFrame, s::Symbol) = haskey(index(df), s)

"""
Set column names


```julia
names!(df::AbstractDataFrame, vals)
```

**Arguments**

* `df` : the AbstractDataFrame
* `vals` : column names, normally a Vector{Symbol} the same length as
  the number of columns in `df`
* `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

**Result**

* `::AbstractDataFrame` : the updated result


**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
names!(df, [:a, :b, :c])
names!(df, [:a, :b, :a])  # throws ArgumentError
names!(df, [:a, :b, :a], makeunique=true)  # renames second :a to :a_1
```

"""
function names!(df::AbstractDataFrame, vals; makeunique::Bool=false)
    names!(index(df), vals, makeunique=makeunique)
    return df
end

function rename!(df::AbstractDataFrame, args...)
    rename!(index(df), args...)
    return df
end
function rename!(f::Function, df::AbstractDataFrame)
    rename!(f, index(df))
    return df
end

rename(df::AbstractDataFrame, args...) = rename!(copy(df), args...)
rename(f::Function, df::AbstractDataFrame) = rename!(f, copy(df))

"""
Rename columns

```julia
rename!(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)
rename!(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})
rename!(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})
rename!(f::Function, df::AbstractDataFrame)
rename(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)
rename(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})
rename(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})
rename(f::Function, df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame
* `d` : an Associative type or an AbstractArray of pairs that maps
  the original names to new names
* `f` : a function which for each column takes the old name (a Symbol)
  and returns the new name (a Symbol)

**Result**

* `::AbstractDataFrame` : the updated result

New names are processed sequentially. A new name must not already exist in the `DataFrame`
at the moment an attempt to rename a column is performed.

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
rename(df, :i => :A, :x => :X)
rename(df, [:i => :A, :x => :X])
rename(df, Dict(:i => :A, :x => :X))
rename(x -> Symbol(uppercase(string(x))), df)
rename(df) do x
    Symbol(uppercase(string(x)))
end
rename!(df, Dict(:i =>: A, :x => :X))
```

"""
(rename!, rename)

"""
Return element types of columns

```julia
eltypes(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::Vector{Type}` : the element type of each column

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
eltypes(df)
```

"""
eltypes(df::AbstractDataFrame) = eltype.(eachcol(df))

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

Base.lastindex(df::AbstractDataFrame) = ncol(df)
Base.lastindex(df::AbstractDataFrame, i::Integer) = last(axes(df, i))
Base.axes(df::AbstractDataFrame, i::Integer) = Base.OneTo(size(df, i))

Base.ndims(::AbstractDataFrame) = 2
Base.ndims(::Type{<:AbstractDataFrame}) = 2

Base.getproperty(df::AbstractDataFrame, col_ind::Symbol) = df[!, col_ind]
# Private fields are never exposed since they can conflict with column names
Base.propertynames(df::AbstractDataFrame, private::Bool=false) = names(df)

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

##############################################################################
##
## Description
##
##############################################################################

"""
    first(df::AbstractDataFrame)

Get the first row of `df` as a `DataFrameRow`.
"""
Base.first(df::AbstractDataFrame) = df[1, :]

"""
    first(df::AbstractDataFrame, n::Integer)

Get a data frame with the `n` first rows of `df`.
"""
Base.first(df::AbstractDataFrame, n::Integer) = df[1:min(n,nrow(df)), :]

"""
    last(df::AbstractDataFrame)

Get the last row of `df` as a `DataFrameRow`.
"""
Base.last(df::AbstractDataFrame) = df[nrow(df), :]

"""
    last(df::AbstractDataFrame, n::Integer)

Get a data frame with the `n` last rows of `df`.
"""
Base.last(df::AbstractDataFrame, n::Integer) = df[max(1,nrow(df)-n+1):nrow(df), :]


"""
Report descriptive statistics for a data frame

```julia
describe(df::AbstractDataFrame)
describe(df::AbstractDataFrame, stats::Union{Symbol, Pair{<:Symbol}}...)
```

**Arguments**

* `df` : the `AbstractDataFrame`
* `stats::Union{Symbol, Pair{<:Symbol}}...` : the summary statistics to report.
  Arguments can be:
    *  A symbol from the list `:mean`, `:std`, `:min`, `:q25`,
      `:median`, `:q75`, `:max`, `:eltype`, `:nunique`, `:first`, `:last`, and
      `:nmissing`. The default statistics used
      are `:mean`, `:min`, `:median`, `:max`, `:nunique`, `:nmissing`, and `:eltype`.
    * `:all` as the only `Symbol` argument to return all statistics.
    * A `name => function` pair where `name` is a `Symbol`. This will create
      a column of summary statistics with the provided name.

**Result**

* A `DataFrame` where each row represents a variable and each column a summary statistic.

**Details**

For `Real` columns, compute the mean, standard deviation, minimum, first quantile, median,
third quantile, and maximum. If a column does not derive from `Real`, `describe` will
attempt to calculate all statistics, using `nothing` as a fall-back in the case of an error.

When `stats` contains `:nunique`, `describe` will report the
number of unique values in a column. If a column's base type derives from `Real`,
`:nunique` will return `nothing`s.

Missing values are filtered in the calculation of all statistics, however the column
`:nmissing` will report the number of missing values of that variable.
If the column does not allow missing values, `nothing` is returned.
Consequently, `nmissing = 0` indicates that the column allows
missing values, but does not currently contain any.

If custom functions are provided, they are called repeatedly with the vector corresponding
to each column as the only argument. For columns allowing for missing values,
the vector is wrapped in a call to [`skipmissing`](@ref): custom functions must therefore
support such objects (and not only vectors), and cannot access missing values.


**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
describe([io,] df)
describe([io,] df, :all)
describe([io,] df, :min, :max)
describe([io,] df, :min, :sum => sum)
```

"""
DataAPI.describe(io::IO, df::AbstractDataFrame, stats::Union{Symbol, Pair{Symbol}}...) =
    _describe(df, collect(stats))

DataAPI.describe(io::IO, df::AbstractDataFrame) =
    _describe(df, [:mean, :min, :median, :max, :nunique, :nmissing, :eltype])

DataAPI.describe(df::AbstractDataFrame, stats::Union{Symbol, Pair{Symbol}}...) = DataAPI.describe(stdout, df, stats...)

function _describe(df::AbstractDataFrame, stats::AbstractVector)
    predefined_funs = Symbol[s for s in stats if s isa Symbol]

    allowed_fields = [:mean, :std, :min, :q25, :median, :q75,
                      :max, :nunique, :nmissing, :first, :last, :eltype]

    if predefined_funs == [:all]
        predefined_funs = allowed_fields
        i = findfirst(s -> s == :all, stats)
        splice!(stats, i, allowed_fields) # insert in the stats vector to get a good order
    elseif :all in predefined_funs
        throw(ArgumentError("`:all` must be the only `Symbol` argument."))
    elseif !issubset(predefined_funs, allowed_fields)
        not_allowed = join(setdiff(predefined_funs, allowed_fields), ", :")
        allowed_msg = "\nAllowed fields are: :" * join(allowed_fields, ", :")
        throw(ArgumentError(":$not_allowed not allowed." * allowed_msg))
    end

    custom_funs = Pair[s for s in stats if s isa Pair]

    ordered_names = [s isa Symbol ? s : s[1] for s in stats]

    if !allunique(ordered_names)
        duplicate_names = unique(ordered_names[nonunique(DataFrame(ordered_names = ordered_names))])
        throw(ArgumentError("Duplicate names not allowed. Duplicated value(s) are: " *
                            ":$(join(duplicate_names, ", "))"))
    end

    # Put the summary stats into the return data frame
    data = DataFrame()
    data.variable = names(df)

    # An array of Dicts for summary statistics
    column_stats_dicts = map(eachcol(df)) do col
        if eltype(col) >: Missing
            t = collect(skipmissing(col))
            d = get_stats(t, predefined_funs)
            get_stats!(d, t, custom_funs)
        else
            d = get_stats(col, predefined_funs)
            get_stats!(d, col, custom_funs)
        end

        if :nmissing in predefined_funs
            d[:nmissing] = eltype(col) >: Missing ? count(ismissing, col) : nothing
        end

        if :first in predefined_funs
            d[:first] = isempty(col) ? nothing : first(col)
        end

        if :last in predefined_funs
            d[:last] = isempty(col) ? nothing : last(col)
        end

        return d
    end

    for stat in ordered_names
        # for each statistic, loop through the columns array to find values
        # letting the comprehension choose the appropriate type
        data[!, stat] = [column_stats_dict[stat] for column_stats_dict in column_stats_dicts]
    end

    return data
end

# Compute summary statistics
# use a dict because we dont know which measures the user wants
# Outside of the `describe` function due to something with 0.7
function get_stats(col::AbstractVector, stats::AbstractVector{Symbol})
    d = Dict{Symbol, Any}()

    if :q25 in stats || :median in stats || :q75 in stats
        q = try quantile(col, [.25, .5, .75]) catch; (nothing, nothing, nothing) end
        d[:q25] = q[1]
        d[:median] = q[2]
        d[:q75] = q[3]
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
            d[:nunique] = try length(unique(col)) catch end
        end
    end

    if :eltype in stats
        d[:eltype] = eltype(col)
    end

    return d
end

function get_stats!(d::Dict, col::AbstractVector, stats::AbstractVector{<:Pair})
    for stat in stats
        d[stat[1]] = try stat[2](col) catch end
    end
end


##############################################################################
##
## Miscellaneous
##
##############################################################################

function _nonmissing!(res, col)
    @inbounds for (i, el) in enumerate(col)
        res[i] &= !ismissing(el)
    end
    return nothing
end

function _nonmissing!(res, col::CategoricalArray{>: Missing})
    for (i, el) in enumerate(col.refs)
        res[i] &= el > 0
    end
    return nothing
end


"""
    completecases(df::AbstractDataFrame, cols::Colon=:)
    completecases(df::AbstractDataFrame, cols::Union{AbstractVector, Regex, Not})
    completecases(df::AbstractDataFrame, cols::Union{Integer, Symbol})

Return a Boolean vector with `true` entries indicating rows without missing values
(complete cases) in data frame `df`. If `cols` is provided, only missing values in
the corresponding columns are considered.

See also: [`dropmissing`](@ref) and [`dropmissing!`](@ref).
Use `findall(completecases(df))` to get the indices of the rows.

# Examples

```julia
julia> df = DataFrame(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])
5×3 DataFrame
│ Row │ i     │ x       │ y       │
│     │ Int64 │ Int64⍰  │ String⍰ │
├─────┼───────┼─────────┼─────────┤
│ 1   │ 1     │ missing │ missing │
│ 2   │ 2     │ 4       │ missing │
│ 3   │ 3     │ missing │ c       │
│ 4   │ 4     │ 2       │ d       │
│ 5   │ 5     │ 1       │ e       │

julia> completecases(df)
5-element BitArray{1}:
 false
 false
 false
  true
  true

julia> completecases(df, :x)
5-element BitArray{1}:
 false
  true
 false
  true
  true

julia> completecases(df, [:x, :y])
5-element BitArray{1}:
 false
 false
 false
  true
  true
```

"""
function completecases(df::AbstractDataFrame, col::Colon=:)
    if ncol(df) == 0
        throw(ArgumentError("Unable to compute complete cases of a data frame with no columns"))
    end
    res = trues(size(df, 1))
    for i in 1:size(df, 2)
        _nonmissing!(res, df[!, i])
    end
    res
end

function completecases(df::AbstractDataFrame, col::ColumnIndex)
    res = trues(size(df, 1))
    _nonmissing!(res, df[!, col])
    res
end

completecases(df::AbstractDataFrame, cols::Union{AbstractVector, Regex, Not}) =
    completecases(df[!, cols])

"""
    dropmissing(df::AbstractDataFrame, cols::Colon=:; disallowmissing::Bool=true)
    dropmissing(df::AbstractDataFrame, cols::Union{AbstractVector, Regex, Not};
                disallowmissing::Bool=true)
    dropmissing(df::AbstractDataFrame, cols::Union{Integer, Symbol};
                disallowmissing::Bool=true)

Return a copy of data frame `df` excluding rows with missing values.
If `cols` is provided, only missing values in the corresponding columns are considered.

If `disallowmissing` is `true` (the default) then columns specified in `cols` will
be converted so as not to allow for missing values using [`disallowmissing!`](@ref).

See also: [`completecases`](@ref) and [`dropmissing!`](@ref).

# Examples

```julia
julia> df = DataFrame(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])
5×3 DataFrame
│ Row │ i     │ x       │ y       │
│     │ Int64 │ Int64⍰  │ String⍰ │
├─────┼───────┼─────────┼─────────┤
│ 1   │ 1     │ missing │ missing │
│ 2   │ 2     │ 4       │ missing │
│ 3   │ 3     │ missing │ c       │
│ 4   │ 4     │ 2       │ d       │
│ 5   │ 5     │ 1       │ e       │

julia> dropmissing(df)
2×3 DataFrame
│ Row │ i     │ x     │ y      │
│     │ Int64 │ Int64 │ String │
├─────┼───────┼───────┼────────┤
│ 1   │ 4     │ 2     │ d      │
│ 2   │ 5     │ 1     │ e      │

julia> dropmissing(df, disallowmissing=false)
2×3 DataFrame
│ Row │ i     │ x      │ y       │
│     │ Int64 │ Int64⍰ │ String⍰ │
├─────┼───────┼────────┼─────────┤
│ 1   │ 4     │ 2      │ d       │
│ 2   │ 5     │ 1      │ e       │

julia> dropmissing(df, :x)
3×3 DataFrame
│ Row │ i     │ x     │ y       │
│     │ Int64 │ Int64 │ String⍰ │
├─────┼───────┼───────┼─────────┤
│ 1   │ 2     │ 4     │ missing │
│ 2   │ 4     │ 2     │ d       │
│ 3   │ 5     │ 1     │ e       │

julia> dropmissing(df, [:x, :y])
2×3 DataFrame
│ Row │ i     │ x     │ y      │
│     │ Int64 │ Int64 │ String │
├─────┼───────┼───────┼────────┤
│ 1   │ 4     │ 2     │ d      │
│ 2   │ 5     │ 1     │ e      │
```

"""
function dropmissing(df::AbstractDataFrame,
                     cols::Union{ColumnIndex, AbstractVector, Regex, Not, Colon}=:;
                     disallowmissing::Bool=true)
    newdf = df[completecases(df, cols), :]
    disallowmissing && disallowmissing!(newdf, cols)
    newdf
end

"""
    dropmissing!(df::AbstractDataFrame, cols::Colon=:; disallowmissing::Bool=true)
    dropmissing!(df::AbstractDataFrame, cols::Union{AbstractVector, Regex, Not};
                 disallowmissing::Bool=true)
    dropmissing!(df::AbstractDataFrame, cols::Union{Integer, Symbol};
                 disallowmissing::Bool=true)

Remove rows with missing values from data frame `df` and return it.
If `cols` is provided, only missing values in the corresponding columns are considered.

If `disallowmissing` is `true` (the default) then the `cols` columns will
get converted using [`disallowmissing!`](@ref).

See also: [`dropmissing`](@ref) and [`completecases`](@ref).

```jldoctest
julia> df = DataFrame(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])
5×3 DataFrame
│ Row │ i     │ x       │ y       │
│     │ Int64 │ Int64⍰  │ String⍰ │
├─────┼───────┼─────────┼─────────┤
│ 1   │ 1     │ missing │ missing │
│ 2   │ 2     │ 4       │ missing │
│ 3   │ 3     │ missing │ c       │
│ 4   │ 4     │ 2       │ d       │
│ 5   │ 5     │ 1       │ e       │

julia> dropmissing!(copy(df))
2×3 DataFrame
│ Row │ i     │ x     │ y      │
│     │ Int64 │ Int64 │ String │
├─────┼───────┼───────┼────────┤
│ 1   │ 4     │ 2     │ d      │
│ 2   │ 5     │ 1     │ e      │

julia> dropmissing!(copy(df), disallowmissing=false)
2×3 DataFrame
│ Row │ i     │ x      │ y       │
│     │ Int64 │ Int64⍰ │ String⍰ │
├─────┼───────┼────────┼─────────┤
│ 1   │ 4     │ 2      │ d       │
│ 2   │ 5     │ 1      │ e       │

julia> dropmissing!(copy(df), :x)
3×3 DataFrame
│ Row │ i     │ x     │ y       │
│     │ Int64 │ Int64 │ String⍰ │
├─────┼───────┼───────┼─────────┤
│ 1   │ 2     │ 4     │ missing │
│ 2   │ 4     │ 2     │ d       │
│ 3   │ 5     │ 1     │ e       │

julia> dropmissing!(df3, [:x, :y])
2×3 DataFrame
│ Row │ i     │ x     │ y      │
│     │ Int64 │ Int64 │ String │
├─────┼───────┼───────┼────────┤
│ 1   │ 4     │ 2     │ d      │
│ 2   │ 5     │ 1     │ e      │
```

"""
function dropmissing!(df::AbstractDataFrame,
                      cols::Union{ColumnIndex, AbstractVector, Regex, Not, Colon}=:;
                      disallowmissing::Bool=true)
    deleterows!(df, (!).(completecases(df, cols)))
    disallowmissing && disallowmissing!(df, cols)
    df
end

"""
    filter(function, df::AbstractDataFrame)

Return a copy of data frame `df` containing only rows for which `function`
returns `true`. The function is passed a `DataFrameRow` as its only argument.

# Examples
```
julia> df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 1     │ c      │
│ 3   │ 2     │ a      │
│ 4   │ 1     │ b      │

julia> filter(row -> row[:x] > 1, df)
2×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 2     │ a      │
```
"""
Base.filter(f, df::AbstractDataFrame) = df[collect(f(r)::Bool for r in eachrow(df)), :]

"""
    filter!(function, df::AbstractDataFrame)

Remove rows from data frame `df` for which `function` returns `false`.
The function is passed a `DataFrameRow` as its only argument.

# Examples
```
julia> df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 1     │ c      │
│ 3   │ 2     │ a      │
│ 4   │ 1     │ b      │

julia> filter!(row -> row[:x] > 1, df);

julia> df
2×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 2     │ a      │
```
"""
Base.filter!(f, df::AbstractDataFrame) =
    deleterows!(df, findall(collect(!f(r)::Bool for r in eachrow(df))))

function Base.convert(::Type{Matrix}, df::AbstractDataFrame)
    T = reduce(promote_type, eltypes(df))
    convert(Matrix{T}, df)
end
function Base.convert(::Type{Matrix{T}}, df::AbstractDataFrame) where T
    n, p = size(df)
    res = Matrix{T}(undef, n, p)
    idx = 1
    for (name, col) in eachcol(df, true)
        try
            copyto!(res, idx, col)
        catch err
            if err isa MethodError && err.f == convert &&
               !(T >: Missing) && any(ismissing, col)
                throw(ArgumentError("cannot convert a DataFrame containing missing values to Matrix{$T} " *
                                    "(found for column $name)"))
            else
                rethrow(err)
            end
        end
        idx += n
    end
    return res
end
Base.Matrix(df::AbstractDataFrame) = Base.convert(Matrix, df)
Base.Matrix{T}(df::AbstractDataFrame) where {T} = Base.convert(Matrix{T}, df)

"""
Indexes of duplicate rows (a row that is a duplicate of a prior row)

```julia
nonunique(df::AbstractDataFrame)
nonunique(df::AbstractDataFrame, cols)
```

**Arguments**

* `df` : the AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.) specifying the column(s) to compare

**Result**

* `::Vector{Bool}` : indicates whether the row is a duplicate of some
  prior row

See also [`unique`](@ref) and [`unique!`](@ref).

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df = vcat(df, df)
nonunique(df)
nonunique(df, 1)
```

"""
function nonunique(df::AbstractDataFrame)
    if ncol(df) == 0
        throw(ArgumentError("finding duplicate rows in data frame with no columns is not allowed"))
    end
    gslots = row_group_slots(ntuple(i -> df[!, i], ncol(df)), Val(true))[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(df))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

nonunique(df::AbstractDataFrame, cols) = nonunique(select(df, cols, copycols=false))

Base.unique!(df::AbstractDataFrame) = deleterows!(df, findall(nonunique(df)))
Base.unique!(df::AbstractDataFrame, cols::AbstractVector) =
    deleterows!(df, findall(nonunique(df, cols)))
Base.unique!(df::AbstractDataFrame, cols) =
    deleterows!(df, findall(nonunique(df, cols)))

# Unique rows of an AbstractDataFrame.
Base.unique(df::AbstractDataFrame) = df[(!).(nonunique(df)), :]
Base.unique(df::AbstractDataFrame, cols) =
    df[(!).(nonunique(df, cols)), :]

"""
Delete duplicate rows

```julia
unique(df::AbstractDataFrame)
unique(df::AbstractDataFrame, cols)
unique!(df::AbstractDataFrame)
unique!(df::AbstractDataFrame, cols)
```

**Arguments**

* `df` : the AbstractDataFrame
* `cols` :  column indicator (Symbol, Int, Vector{Symbol}, Regex, etc.)
specifying the column(s) to compare.

**Result**

* `::AbstractDataFrame` : the updated version of `df` with unique rows.
When `cols` is specified, the return DataFrame contains complete rows,
retaining in each case the first instance for which `df[cols]` is unique.

See also [`nonunique`](@ref).

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df = vcat(df, df)
unique(df)   # doesn't modify df
unique(df, 1)
unique!(df)  # modifies df
```

"""
(unique, unique!)

function without(df::AbstractDataFrame, icols::Vector{<:Integer})
    newcols = setdiff(1:ncol(df), icols)
    view(df, :, newcols)
end
without(df::AbstractDataFrame, i::Int) = without(df, [i])
without(df::AbstractDataFrame, c::Any) = without(df, index(df)[c])

"""
    hcat(df::AbstractDataFrame...;
         makeunique::Bool=false, copycols::Bool=true)
    hcat(df::AbstractDataFrame..., vs::AbstractVector;
         makeunique::Bool=false, copycols::Bool=true)
    hcat(vs::AbstractVector, df::AbstractDataFrame;
         makeunique::Bool=false, copycols::Bool=true)

Horizontally concatenate `AbstractDataFrames` and optionally `AbstractVector`s.

If `AbstractVector` is passed then a column name for it is automatically generated
as `:x1` by default.

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
julia [DataFrame(A=1:3) DataFrame(B=1:3)]
3×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │

julia> df1 = DataFrame(A=1:3, B=1:3);

julia> df2 = DataFrame(A=4:6, B=4:6);

julia> df3 = hcat(df1, df2, makeunique=true)
3×4 DataFrame
│ Row │ A     │ B     │ A_1   │ B_1   │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 4     │ 4     │
│ 2   │ 2     │ 2     │ 5     │ 5     │
│ 3   │ 3     │ 3     │ 6     │ 6     │

julia> df3.A === df1.A
true

julia> df3 = hcat(df1, df2, makeunique=true, copycols=false);

julia> df3.A === df1.A
true

```
"""
Base.hcat(df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true) =
    DataFrame(df, copycols=copycols)
Base.hcat(df::AbstractDataFrame, x; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df, copycols=copycols), x,
          makeunique=makeunique, copycols=copycols)
Base.hcat(x, df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(x, df, makeunique=makeunique, copycols=copycols)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df1, copycols=copycols), df2,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df::AbstractDataFrame, x, y...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(df, x, makeunique=makeunique, copycols=copycols), y...,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame, dfn::AbstractDataFrame...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(df1, df2, makeunique=makeunique, copycols=copycols), dfn...,
          makeunique=makeunique, copycols=copycols)

"""
    vcat(dfs::AbstractDataFrame...; cols::Union{Symbol, AbstractVector{Symbol}}=:equal)

Vertically concatenate `AbstractDataFrame`s.

The `cols` keyword argument determines the columns of the returned data frame:

* `:equal` (the default): require all data frames to have the same column names.
  If they appear in different orders, the order of the first provided data frame is used.
* `:intersect`: only the columns present in *all* provided data frames are kept.
  If the intersection is empty, an empty data frame is returned.
* `:union`: columns present in *at least one* of the provided data frames are kept.
  Columns not present in some data frames are filled with `missing` where necessary.
* A vector of `Symbol`s: only listed columns are kept.
  Columns not present in some data frames are filled with `missing` where necessary.

The order of columns is determined by the order they appear in the included
data frames, searching through the header of the first data frame, then the
second, etc.

The element types of columns are determined using `promote_type`,
as with `vcat` for `AbstractVector`s.

`vcat` ignores empty data frames, making it possible to initialize an empty
data frame at the beginning of a loop and `vcat` onto it.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3);

julia> df2 = DataFrame(A=4:6, B=4:6);

julia> df3 = DataFrame(A=7:9, C=7:9);

julia> d4 = DataFrame();

julia> vcat(df1, df2)
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

julia> vcat(df1, df3, cols=:union)
6×3 DataFrame
│ Row │ A     │ B       │ C       │
│     │ Int64 │ Int64⍰  │ Int64⍰  │
├─────┼───────┼─────────┼─────────┤
│ 1   │ 1     │ 1       │ missing │
│ 2   │ 2     │ 2       │ missing │
│ 3   │ 3     │ 3       │ missing │
│ 4   │ 7     │ missing │ 7       │
│ 5   │ 8     │ missing │ 8       │
│ 6   │ 9     │ missing │ 9       │

julia> vcat(df1, df3, cols=:intersect)
6×1 DataFrame
│ Row │ A     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │
│ 4   │ 7     │
│ 5   │ 8     │
│ 6   │ 9     │

julia> vcat(d4, df1)
3×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
```

"""
Base.vcat(dfs::AbstractDataFrame...;
          cols::Union{Symbol, AbstractVector{Symbol}}=:equal) =
    reduce(vcat, dfs; cols=cols)

Base.reduce(::typeof(vcat),
            dfs::Union{AbstractVector{<:AbstractDataFrame}, Tuple{Vararg{AbstractDataFrame}}};
            cols::Union{Symbol, AbstractVector{Symbol}}=:equal) =
    _vcat([df for df in dfs if ncol(df) != 0]; cols=cols)

function _vcat(dfs::AbstractVector{<:AbstractDataFrame};
               cols::Union{Symbol, AbstractVector{Symbol}}=:equal)

    isempty(dfs) && return DataFrame()
    # Array of all headers
    allheaders = map(names, dfs)
    # Array of unique headers across all data frames
    uniqueheaders = unique(allheaders)
    # All symbols present across all headers
    unionunique = union(uniqueheaders...)
    # List of symbols present in all dataframes
    intersectunique = intersect(uniqueheaders...)

    if cols === :equal
        header = unionunique
        coldiff = setdiff(unionunique, intersectunique)

        if !isempty(coldiff)
            # if any DataFrames are a full superset of names, skip them
            filter!(u -> !issetequal(u, header), uniqueheaders)
            estrings = map(enumerate(uniqueheaders)) do (i, head)
                matching = findall(h -> head == h, allheaders)
                headerdiff = setdiff(coldiff, head)
                cols = join(headerdiff, ", ", " and ")
                args = join(matching, ", ", " and ")
                return "column(s) $cols are missing from argument(s) $args"
            end
        throw(ArgumentError(join(estrings, ", ", ", and ")))
        end

    elseif cols === :intersect
        header = intersectunique
    elseif cols === :union
        header = unionunique
    else
        header = cols
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
julia> df = DataFrame(a = 1:2, b = 3:4)
2×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 4     │

julia> repeat(df, inner = 2, outer = 3)
12×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 1     │ 3     │
│ 3   │ 2     │ 4     │
│ 4   │ 2     │ 4     │
│ 5   │ 1     │ 3     │
│ 6   │ 1     │ 3     │
│ 7   │ 2     │ 4     │
│ 8   │ 2     │ 4     │
│ 9   │ 1     │ 3     │
│ 10  │ 1     │ 3     │
│ 11  │ 2     │ 4     │
│ 12  │ 2     │ 4     │
```
"""
Base.repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1) =
    mapcols(x -> repeat(x, inner = inner, outer = outer), df)

"""
    repeat(df::AbstractDataFrame, count::Integer)

Construct a data frame by repeating each row in `df` the number of times
specified by `count`.

# Example
```jldoctest
julia> df = DataFrame(a = 1:2, b = 3:4)
2×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 4     │

julia> repeat(df, 2)
4×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 4     │
│ 3   │ 1     │ 3     │
│ 4   │ 2     │ 4     │
```
"""
Base.repeat(df::AbstractDataFrame, count::Integer) =
    mapcols(x -> repeat(x, count), df)

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
julia> df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10));

julia> size(df)
(10, 3)

julia> nrow(df)
10

julia> ncol(df)
3
```

"""
(nrow, ncol)
