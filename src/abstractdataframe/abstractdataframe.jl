
"""
An abstract type for which all concrete types expose a database-like
interface.

**Common methods**

An AbstractDataFrame is a two-dimensional table with Symbols for
column names. An AbstractDataFrame is also similar to an Associative
type in that it allows indexing by a key (the columns).

The following are normally implemented for AbstractDataFrames:

* [`describe`](@ref) : summarize columns
* [`dump`](@ref) : show structure
* `hcat` : horizontal concatenation
* `vcat` : vertical concatenation
* `names` : columns names
* [`names!`](@ref) : set columns names
* [`rename!`](@ref) : rename columns names based on keyword arguments
* [`eltypes`](@ref) : `eltype` of each column
* `length` : number of columns
* `size` : (nrows, ncols)
* [`head`](@ref) : first `n` rows
* [`tail`](@ref) : last `n` rows
* `convert` : convert to an array
* [`completecases`](@ref) : boolean vector of complete cases (rows with no nulls)
* [`dropnull`](@ref) : remove rows with null values
* [`dropnull!`](@ref) : remove rows with null values in-place
* [`nonunique`](@ref) : indexes of duplicate rows
* [`unique!`](@ref) : remove duplicate rows
* `similar` : a DataFrame with similar columns as `d`

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

struct Cols{T <: AbstractDataFrame} <: AbstractVector{Any}
    df::T
end
Base.start(::Cols) = 1
Base.done(itr::Cols, st) = st > length(itr.df)
Base.next(itr::Cols, st) = (itr.df[st], st + 1)
Base.length(itr::Cols) = length(itr.df)
Base.size(itr::Cols, ix) = ix==1 ? length(itr) : throw(ArgumentError("Incorrect dimension"))
Base.size(itr::Cols) = (length(itr.df),)
Base.IndexStyle(::Type{<:Cols}) = IndexLinear()
Base.getindex(itr::Cols, inds...) = getindex(itr.df, inds...)

# N.B. where stored as a vector, 'columns(x) = x.vector' is a bit cheaper
columns(df::T) where {T <: AbstractDataFrame} = Cols{T}(df)

Base.names(df::AbstractDataFrame) = names(index(df))
_names(df::AbstractDataFrame) = _names(index(df))

"""
Set column names


```julia
names!(df::AbstractDataFrame, vals)
```

**Arguments**

* `df` : the AbstractDataFrame
* `vals` : column names, normally a Vector{Symbol} the same length as
  the number of columns in `df`
* `allow_duplicates` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

**Result**

* `::AbstractDataFrame` : the updated result


**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
names!(df, [:a, :b, :c])
names!(df, [:a, :b, :a])  # throws ArgumentError
names!(df, [:a, :b, :a], allow_duplicates=true)  # renames second :a to :a_1
```

"""
function names!(df::AbstractDataFrame, vals; allow_duplicates=false)
    names!(index(df), vals; allow_duplicates=allow_duplicates)
    return df
end

function rename!(df::AbstractDataFrame, args...)
    rename!(index(df), args...)
    return df
end
rename!(f::Function, df::AbstractDataFrame) = rename!(df, f)

rename(df::AbstractDataFrame, args...) = rename!(copy(df), args...)
rename(f::Function, df::AbstractDataFrame) = rename(df, f)

"""
Rename columns

```julia
rename!(df::AbstractDataFrame, from::Symbol, to::Symbol)
rename!(df::AbstractDataFrame, d::Associative)
rename!(f::Function, df::AbstractDataFrame)
rename(df::AbstractDataFrame, from::Symbol, to::Symbol)
rename(f::Function, df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame
* `d` : an Associative type that maps the original name to a new name
* `f` : a function that has the old column name (a symbol) as input
  and new column name (a symbol) as output

**Result**

* `::AbstractDataFrame` : the updated result

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
rename(x -> Symbol(uppercase(string(x))), df)
rename(df, Dict(:i=>:A, :x=>:X))
rename(df, :y, :Y)
rename!(df, Dict(:i=>:A, :x=>:X))
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
eltypes(df::AbstractDataFrame) = map!(eltype, Vector{Type}(size(df,2)), columns(df))

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

Base.length(df::AbstractDataFrame) = ncol(df)
Base.endof(df::AbstractDataFrame) = ncol(df)

Base.ndims(::AbstractDataFrame) = 2

##############################################################################
##
## Similar
##
##############################################################################

Base.similar(df::AbstractDataFrame, dims::Int) =
    DataFrame(Any[similar_nullable(x, dims) for x in columns(df)], copy(index(df)))

##############################################################################
##
## Equality
##
##############################################################################

# Imported in DataFrames.jl for compatibility across Julia 0.4 and 0.5
Base.:(==)(df1::AbstractDataFrame, df2::AbstractDataFrame) = isequal(df1, df2)

function Base.isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    for idx in 1:size(df1, 2)
        isequal(df1[idx], df2[idx]) || return false
    end
    return true
end

##############################################################################
##
## Associative methods
##
##############################################################################

Base.haskey(df::AbstractDataFrame, key::Any) = haskey(index(df), key)
Base.get(df::AbstractDataFrame, key::Any, default::Any) = haskey(df, key) ? df[key] : default
Base.isempty(df::AbstractDataFrame) = ncol(df) == 0

##############################################################################
##
## Description
##
##############################################################################

head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
head(df::AbstractDataFrame) = head(df, 6)
tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
tail(df::AbstractDataFrame) = tail(df, 6)

"""
Show the first or last part of an AbstractDataFrame

```julia
head(df::AbstractDataFrame, r::Int = 6)
tail(df::AbstractDataFrame, r::Int = 6)
```

**Arguments**

* `df` : the AbstractDataFrame
* `r` : the number of rows to show

**Result**

* `::AbstractDataFrame` : the first or last part of `df`

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
head(df)
tail(df)
```

"""
(head, tail)

# get the structure of a df
"""
Show the structure of an AbstractDataFrame, in a tree-like format

```julia
dump(df::AbstractDataFrame, n::Int = 5)
dump(io::IO, df::AbstractDataFrame, n::Int = 5)
```

**Arguments**

* `df` : the AbstractDataFrame
* `n` : the number of levels to show
* `io` : optional output descriptor

**Result**

* nothing

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dump(df)
```

"""
function Base.dump(io::IO, df::AbstractDataFrame, n::Int, indent)
    println(io, typeof(df), "  $(nrow(df)) observations of $(ncol(df)) variables")
    if n > 0
        for (name, col) in eachcol(df)
            print(io, indent, "  ", name, ": ")
            dump(io, col, n - 1, string(indent, "  "))
        end
    end
end

# summarize the columns of a df
# TODO: clever layout in rows
"""
Summarize the columns of an AbstractDataFrame

```julia
describe(df::AbstractDataFrame)
describe(io, df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame
* `io` : optional output descriptor

**Result**

* nothing

**Details**

If the column's base type derives from Number, compute the minimum, first
quantile, median, mean, third quantile, and maximum. Nulls are filtered and
reported separately.

For boolean columns, report trues, falses, and nulls.

For other types, show column characteristics and number of nulls.

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
describe(df)
```

"""
StatsBase.describe(df::AbstractDataFrame) = describe(STDOUT, df)
function StatsBase.describe(io, df::AbstractDataFrame)
    for (name, col) in eachcol(df)
        println(io, name)
        describe(io, col)
        println(io, )
    end
end

function StatsBase.describe(io::IO, X::AbstractVector{Union{T, Null}}) where T
    nullcount = count(isnull, X)
    pnull = 100 * nullcount/length(X)
    if pnull != 100 && T <: Real
        show(io, StatsBase.summarystats(collect(Nulls.skip(X))))
    else
        println(io, "Summary Stats:")
    end
    println(io, "Length:         $(length(X))")
    println(io, "Type:           $(eltype(X))")
    !(T <: Real) && println(io, "Number Unique:  $(length(unique(X)))")
    println(io, "Number Missing: $(nullcount)")
    @printf(io, "%% Missing:      %.6f\n", pnull)
    return
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

function _nonnull!(res, col)
    @inbounds for (i, el) in enumerate(col)
        res[i] &= !isnull(el)
    end
end

function _nonnull!(res, col::CategoricalArray{>: Null})
    for (i, el) in enumerate(col.refs)
        res[i] &= el > 0
    end
end


"""
Indexes of complete cases (rows without null values)

```julia
completecases(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::Vector{Bool}` : indexes of complete cases

See also [`dropnull`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
df = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
df[[1,4,5], :x] = null
df[[9,10], :y] = null
completecases(df)
```

"""
function completecases(df::AbstractDataFrame)
    res = trues(size(df, 1))
    for i in 1:size(df, 2)
        _nonnull!(res, df[i])
    end
    res
end

"""
Remove rows with null values.

```julia
dropnull(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated copy

See also [`completecases`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
df = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
df[[1,4,5], :x] = null
df[[9,10], :y] = null
dropnull(df)
```

"""
dropnull(df::AbstractDataFrame) = deleterows!(copy(df), find(!, completecases(df)))

"""
Remove rows with null values in-place.

```julia
dropnull!(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated version

See also [`dropnull`](@ref) and [`completecases`](@ref).

**Examples**

```julia
df = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
df[[1,4,5], :x] = null
df[[9,10], :y] = null
dropnull!(df)
```

"""
dropnull!(df::AbstractDataFrame) = deleterows!(df, find(!, completecases(df)))

function Base.convert(::Type{Array}, df::AbstractDataFrame)
    convert(Matrix, df)
end
function Base.convert(::Type{Matrix}, df::AbstractDataFrame)
    T = reduce(promote_type, eltypes(df))
    convert(Matrix{T}, df)
end
function Base.convert(::Type{Array{T}}, df::AbstractDataFrame) where T
    convert(Matrix{T}, df)
end
function Base.convert(::Type{Matrix{T}}, df::AbstractDataFrame) where T
    n, p = size(df)
    res = Matrix{T}(n, p)
    idx = 1
    for (name, col) in zip(names(df), columns(df))
        !(T >: Null) && any(isnull, col) && error("cannot convert a DataFrame containing null values to array (found for column $name)")
        copy!(res, idx, convert(Vector{T}, col))
        idx += n
    end
    return res
end

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
    gslots = row_group_slots(df)[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(df))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

nonunique(df::AbstractDataFrame, cols::Union{Real, Symbol}) = nonunique(df[[cols]])
nonunique(df::AbstractDataFrame, cols::Any) = nonunique(df[cols])

if isdefined(:unique!)
    import Base.unique!
end

unique!(df::AbstractDataFrame) = deleterows!(df, find(nonunique(df)))
unique!(df::AbstractDataFrame, cols::Any) = deleterows!(df, find(nonunique(df, cols)))

# Unique rows of an AbstractDataFrame.
Base.unique(df::AbstractDataFrame) = df[(!).(nonunique(df)), :]
Base.unique(df::AbstractDataFrame, cols::Any) = df[(!).(nonunique(df, cols)), :]

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
* `cols` :  column indicator (Symbol, Int, Vector{Symbol}, etc.)
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

# Count the number of missing values in every column of an AbstractDataFrame.
function colmissing(df::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(df)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countnull(df[j])
    end
    return missing
end

function without(df::AbstractDataFrame, icols::Vector{Int})
    newcols = setdiff(1:ncol(df), icols)
    df[newcols]
end
without(df::AbstractDataFrame, i::Int) = without(df, [i])
without(df::AbstractDataFrame, c::Any) = without(df, index(df)[c])

##############################################################################
##
## Hcat / vcat
##
##############################################################################

# hcat's first argument must be an AbstractDataFrame
# Trailing arguments (currently) may also be vectors or scalars.

# hcat! is defined in DataFrames/DataFrames.jl
# Its first argument (currently) must be a DataFrame.

# catch-all to cover cases where indexing returns a DataFrame and copy doesn't
Base.hcat(df::AbstractDataFrame, x) = hcat!(df[:, :], x)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame) = hcat!(df1[:, :], df2)

Base.hcat(df::AbstractDataFrame, x, y...) = hcat!(hcat(df, x), y...)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame, dfn::AbstractDataFrame...) = hcat!(hcat(df1, df2), dfn...)

@generated function promote_col_type(cols::AbstractVector...)
    T = promote_type(map(x -> Nulls.T(eltype(x)), cols)...)
    if T <: CategoricalValue
        T = T.parameters[1]
    end
    if any(col -> eltype(col) >: Null, cols)
        if any(col -> col <: AbstractCategoricalArray, cols)
            return :(CategoricalVector{Union{$T, Null}})
        else
            return :(Vector{Union{$T, Null}})
        end
    else
        if any(col -> col <: AbstractCategoricalArray, cols)
            return :(CategoricalVector{$T})
        else
            return :(Vector{$T})
        end
    end
end

"""
    vcat(dfs::AbstractDataFrame...)

Vertically concatenate `AbstractDataFrames` that have the same column names in
the same order.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3);
julia> df2 = DataFrame(A=4:6, B=4:6);
julia> vcat(df1, df2)
6×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ 1 │
│ 2   │ 2 │ 2 │
│ 3   │ 3 │ 3 │
│ 4   │ 4 │ 4 │
│ 5   │ 5 │ 5 │
│ 6   │ 6 │ 6 │
```
"""
Base.vcat(df::AbstractDataFrame) = df
function Base.vcat(dfs::AbstractDataFrame...)
    isempty(dfs) && return DataFrame()
    allheaders = map(names, dfs)
    if all(h -> length(h) == 0, allheaders)
        return DataFrame()
    end
    uniqueheaders = unique(allheaders)
    if length(uniqueheaders) > 1
        unionunique = union(uniqueheaders...)
        coldiff = setdiff(unionunique, intersect(uniqueheaders...))
        if !isempty(coldiff)
            # if any DataFrames are a full superset of names, skip them
            filter!(u -> Set(u) != Set(unionunique), uniqueheaders)
            estrings = Vector{String}(length(uniqueheaders))
            for (i, u) in enumerate(uniqueheaders)
                matching = find(h -> u == h, allheaders)
                headerdiff = setdiff(coldiff, u)
                cols = join(headerdiff, ", ", " and ")
                args = join(matching, ", ", " and ")
                estrings[i] = "column(s) $cols are missing from argument(s) $args"
            end
            throw(ArgumentError(join(estrings, ", ", ", and ")))
        else
            estrings = Vector{String}(length(uniqueheaders))
            for (i, u) in enumerate(uniqueheaders)
                indices = find(a -> a == u, allheaders)
                estrings[i] = "column order of argument(s) $(join(indices, ", ", " and "))"
            end
            throw(ArgumentError(join(estrings, " != ")))
        end
    else
        header = uniqueheaders[1]
        cols = Vector{Any}(length(header))
        for i in 1:length(cols)
            data = [df[i] for df in dfs]
            lens = map(length, data)
            cols[i] = promote_col_type(data...)(sum(lens))
            offset = 1
            for j in 1:length(data)
                copy!(cols[i], offset, data[j])
                offset += lens[j]
            end
        end
        return DataFrame(cols, header)
    end
end

##############################################################################
##
## Hashing
##
## Make sure this agrees with isequals()
##
##############################################################################

function Base.hash(df::AbstractDataFrame)
    h = hash(size(df)) + 1
    for i in 1:size(df, 2)
        h = hash(df[i], h)
    end
    return UInt(h)
end


## Documentation for methods defined elsewhere

"""
Number of rows or columns in an AbstractDataFrame

```julia
nrow(df::AbstractDataFrame)
ncol(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated version

See also [`size`](@ref).

NOTE: these functions may be depreciated for `size`.

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
size(df)
nrow(df)
ncol(df)
```

"""
# nrow, ncol
