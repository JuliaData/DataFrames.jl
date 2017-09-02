
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
@compat abstract type AbstractDataFrame end

##############################################################################
##
## Interface (not final)
##
##############################################################################

# index(dt) => AbstractIndex
# nrow(dt) => Int
# ncol(dt) => Int
# getindex(...)
# setindex!(...) exclusive of methods that add new columns

##############################################################################
##
## Basic properties of a DataFrame
##
##############################################################################

immutable Cols{T <: AbstractDataFrame} <: AbstractVector{Any}
    dt::T
end
Base.start(::Cols) = 1
Base.done(itr::Cols, st) = st > length(itr.dt)
Base.next(itr::Cols, st) = (itr.dt[st], st + 1)
Base.length(itr::Cols) = length(itr.dt)
Base.size(itr::Cols, ix) = ix==1 ? length(itr) : throw(ArgumentError("Incorrect dimension"))
Base.size(itr::Cols) = (length(itr.dt),)
@compat Base.IndexStyle(::Type{<:Cols}) = IndexLinear()
Base.getindex(itr::Cols, inds...) = getindex(itr.dt, inds...)

# N.B. where stored as a vector, 'columns(x) = x.vector' is a bit cheaper
columns{T <: AbstractDataFrame}(dt::T) = Cols{T}(dt)

Base.names(dt::AbstractDataFrame) = names(index(dt))
_names(dt::AbstractDataFrame) = _names(index(dt))

"""
Set column names


```julia
names!(dt::AbstractDataFrame, vals)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `vals` : column names, normally a Vector{Symbol} the same length as
  the number of columns in `dt`
* `allow_duplicates` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

**Result**

* `::AbstractDataFrame` : the updated result


**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
names!(dt, [:a, :b, :c])
names!(dt, [:a, :b, :a])  # throws ArgumentError
names!(dt, [:a, :b, :a], allow_duplicates=true)  # renames second :a to :a_1
```

"""
function names!(dt::AbstractDataFrame, vals; allow_duplicates=false)
    names!(index(dt), vals; allow_duplicates=allow_duplicates)
    return dt
end

function rename!(dt::AbstractDataFrame, args...)
    rename!(index(dt), args...)
    return dt
end
rename!(f::Function, dt::AbstractDataFrame) = rename!(dt, f)

rename(dt::AbstractDataFrame, args...) = rename!(copy(dt), args...)
rename(f::Function, dt::AbstractDataFrame) = rename(dt, f)

"""
Rename columns

```julia
rename!(dt::AbstractDataFrame, from::Symbol, to::Symbol)
rename!(dt::AbstractDataFrame, d::Associative)
rename!(f::Function, dt::AbstractDataFrame)
rename(dt::AbstractDataFrame, from::Symbol, to::Symbol)
rename(f::Function, dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `d` : an Associative type that maps the original name to a new name
* `f` : a function that has the old column name (a symbol) as input
  and new column name (a symbol) as output

**Result**

* `::AbstractDataFrame` : the updated result

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
rename(x -> Symbol(uppercase(string(x))), dt)
rename(dt, Dict(:i=>:A, :x=>:X))
rename(dt, :y, :Y)
rename!(dt, Dict(:i=>:A, :x=>:X))
```

"""
(rename!, rename)

"""
Return element types of columns

```julia
eltypes(dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame

**Result**

* `::Vector{Type}` : the element type of each column

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
eltypes(dt)
```

"""
eltypes(dt::AbstractDataFrame) = map!(eltype, Vector{Type}(size(dt,2)), columns(dt))

Base.size(dt::AbstractDataFrame) = (nrow(dt), ncol(dt))
function Base.size(dt::AbstractDataFrame, i::Integer)
    if i == 1
        nrow(dt)
    elseif i == 2
        ncol(dt)
    else
        throw(ArgumentError("DataFrames only have two dimensions"))
    end
end

Base.length(dt::AbstractDataFrame) = ncol(dt)
Base.endof(dt::AbstractDataFrame) = ncol(dt)

Base.ndims(::AbstractDataFrame) = 2

##############################################################################
##
## Similar
##
##############################################################################

Base.similar(dt::AbstractDataFrame, dims::Int) =
    DataFrame(Any[similar_nullable(x, dims) for x in columns(dt)], copy(index(dt)))

##############################################################################
##
## Equality
##
##############################################################################

# Imported in DataFrames.jl for compatibility across Julia 0.4 and 0.5
Base.:(==)(dt1::AbstractDataFrame, dt2::AbstractDataFrame) = isequal(dt1, dt2)

function Base.isequal(dt1::AbstractDataFrame, dt2::AbstractDataFrame)
    size(dt1, 2) == size(dt2, 2) || return false
    isequal(index(dt1), index(dt2)) || return false
    for idx in 1:size(dt1, 2)
        isequal(dt1[idx], dt2[idx]) || return false
    end
    return true
end

##############################################################################
##
## Associative methods
##
##############################################################################

Base.haskey(dt::AbstractDataFrame, key::Any) = haskey(index(dt), key)
Base.get(dt::AbstractDataFrame, key::Any, default::Any) = haskey(dt, key) ? dt[key] : default
Base.isempty(dt::AbstractDataFrame) = ncol(dt) == 0

##############################################################################
##
## Description
##
##############################################################################

head(dt::AbstractDataFrame, r::Int) = dt[1:min(r,nrow(dt)), :]
head(dt::AbstractDataFrame) = head(dt, 6)
tail(dt::AbstractDataFrame, r::Int) = dt[max(1,nrow(dt)-r+1):nrow(dt), :]
tail(dt::AbstractDataFrame) = tail(dt, 6)

"""
Show the first or last part of an AbstractDataFrame

```julia
head(dt::AbstractDataFrame, r::Int = 6)
tail(dt::AbstractDataFrame, r::Int = 6)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `r` : the number of rows to show

**Result**

* `::AbstractDataFrame` : the first or last part of `dt`

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
head(dt)
tail(dt)
```

"""
(head, tail)

# get the structure of a DT
"""
Show the structure of an AbstractDataFrame, in a tree-like format

```julia
dump(dt::AbstractDataFrame, n::Int = 5)
dump(io::IO, dt::AbstractDataFrame, n::Int = 5)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `n` : the number of levels to show
* `io` : optional output descriptor

**Result**

* nothing

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dump(dt)
```

"""
function Base.dump(io::IO, dt::AbstractDataFrame, n::Int, indent)
    println(io, typeof(dt), "  $(nrow(dt)) observations of $(ncol(dt)) variables")
    if n > 0
        for (name, col) in eachcol(dt)
            print(io, indent, "  ", name, ": ")
            dump(io, col, n - 1, string(indent, "  "))
        end
    end
end

# summarize the columns of a DT
# TODO: clever layout in rows
"""
Summarize the columns of an AbstractDataFrame

```julia
describe(dt::AbstractDataFrame)
describe(io, dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame
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
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
describe(dt)
```

"""
StatsBase.describe(dt::AbstractDataFrame) = describe(STDOUT, dt)
function StatsBase.describe(io, dt::AbstractDataFrame)
    for (name, col) in eachcol(dt)
        println(io, name)
        describe(io, col)
        println(io, )
    end
end

function StatsBase.describe{T}(io::IO, X::AbstractVector{Union{T, Null}})
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
completecases(dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame

**Result**

* `::Vector{Bool}` : indexes of complete cases

See also [`dropnull`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
dt = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
dt[[1,4,5], :x] = null
dt[[9,10], :y] = null
completecases(dt)
```

"""
function completecases(dt::AbstractDataFrame)
    res = trues(size(dt, 1))
    for i in 1:size(dt, 2)
        _nonnull!(res, dt[i])
    end
    res
end

"""
Remove rows with null values.

```julia
dropnull(dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated copy

See also [`completecases`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
dt = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
dt[[1,4,5], :x] = null
dt[[9,10], :y] = null
dropnull(dt)
```

"""
dropnull(dt::AbstractDataFrame) = deleterows!(copy(dt), find(!, completecases(dt)))

"""
Remove rows with null values in-place.

```julia
dropnull!(dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated version

See also [`dropnull`](@ref) and [`completecases`](@ref).

**Examples**

```julia
dt = DataFrame(i = 1:10,
               x = Vector{Union{Null, Float64}}(rand(10)),
               y = Vector{Union{Null, String}}(rand(["a", "b", "c"], 10)))
dt[[1,4,5], :x] = null
dt[[9,10], :y] = null
dropnull!(dt)
```

"""
dropnull!(dt::AbstractDataFrame) = deleterows!(dt, find(!, completecases(dt)))

function Base.convert(::Type{Array}, dt::AbstractDataFrame)
    convert(Matrix, dt)
end
function Base.convert(::Type{Matrix}, dt::AbstractDataFrame)
    T = reduce(promote_type, eltypes(dt))
    convert(Matrix{T}, dt)
end
function Base.convert{T}(::Type{Array{T}}, dt::AbstractDataFrame)
    convert(Matrix{T}, dt)
end
function Base.convert{T}(::Type{Matrix{T}}, dt::AbstractDataFrame)
    n, p = size(dt)
    res = Matrix{T}(n, p)
    idx = 1
    for (name, col) in zip(names(dt), columns(dt))
        !(T >: Null) && any(isnull, col) && error("cannot convert a DataFrame containing null values to array (found for column $name)")
        copy!(res, idx, convert(Vector{T}, col))
        idx += n
    end
    return res
end

"""
Indexes of duplicate rows (a row that is a duplicate of a prior row)

```julia
nonunique(dt::AbstractDataFrame)
nonunique(dt::AbstractDataFrame, cols)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.) specifying the column(s) to compare

**Result**

* `::Vector{Bool}` : indicates whether the row is a duplicate of some
  prior row

See also [`unique`](@ref) and [`unique!`](@ref).

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt = vcat(dt, dt)
nonunique(dt)
nonunique(dt, 1)
```

"""
function nonunique(dt::AbstractDataFrame)
    gslots = row_group_slots(dt)[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(dt))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

nonunique(dt::AbstractDataFrame, cols::Union{Real, Symbol}) = nonunique(dt[[cols]])
nonunique(dt::AbstractDataFrame, cols::Any) = nonunique(dt[cols])

if isdefined(:unique!)
    import Base.unique!
end

unique!(dt::AbstractDataFrame) = deleterows!(dt, find(nonunique(dt)))
unique!(dt::AbstractDataFrame, cols::Any) = deleterows!(dt, find(nonunique(dt, cols)))

# Unique rows of an AbstractDataFrame.
Base.unique(dt::AbstractDataFrame) = dt[(!).(nonunique(dt)), :]
Base.unique(dt::AbstractDataFrame, cols::Any) = dt[(!).(nonunique(dt, cols)), :]

"""
Delete duplicate rows

```julia
unique(dt::AbstractDataFrame)
unique(dt::AbstractDataFrame, cols)
unique!(dt::AbstractDataFrame)
unique!(dt::AbstractDataFrame, cols)
```

**Arguments**

* `dt` : the AbstractDataFrame
* `cols` :  column indicator (Symbol, Int, Vector{Symbol}, etc.)
specifying the column(s) to compare.

**Result**

* `::AbstractDataFrame` : the updated version of `dt` with unique rows.
When `cols` is specified, the return DataFrame contains complete rows,
retaining in each case the first instance for which `dt[cols]` is unique.

See also [`nonunique`](@ref).

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt = vcat(dt, dt)
unique(dt)   # doesn't modify dt
unique(dt, 1)
unique!(dt)  # modifies dt
```

"""
(unique, unique!)

# Count the number of missing values in every column of an AbstractDataFrame.
function colmissing(dt::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(dt)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countnull(dt[j])
    end
    return missing
end

function without(dt::AbstractDataFrame, icols::Vector{Int})
    newcols = setdiff(1:ncol(dt), icols)
    dt[newcols]
end
without(dt::AbstractDataFrame, i::Int) = without(dt, [i])
without(dt::AbstractDataFrame, c::Any) = without(dt, index(dt)[c])

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
Base.hcat(dt::AbstractDataFrame, x) = hcat!(dt[:, :], x)
Base.hcat(dt1::AbstractDataFrame, dt2::AbstractDataFrame) = hcat!(dt1[:, :], dt2)

Base.hcat(dt::AbstractDataFrame, x, y...) = hcat!(hcat(dt, x), y...)
Base.hcat(dt1::AbstractDataFrame, dt2::AbstractDataFrame, dtn::AbstractDataFrame...) = hcat!(hcat(dt1, dt2), dtn...)

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
    vcat(dts::AbstractDataFrame...)

Vertically concatenate `AbstractDataFrames` that have the same column names in
the same order.

# Example
```jldoctest
julia> dt1 = DataFrame(A=1:3, B=1:3);
julia> dt2 = DataFrame(A=4:6, B=4:6);
julia> vcat(dt1, dt2)
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
Base.vcat(dt::AbstractDataFrame) = dt
function Base.vcat(dts::AbstractDataFrame...)
    isempty(dts) && return DataFrame()
    allheaders = map(names, dts)
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
            data = [dt[i] for dt in dts]
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

function Base.hash(dt::AbstractDataFrame)
    h = hash(size(dt)) + 1
    for i in 1:size(dt, 2)
        h = hash(dt[i], h)
    end
    return UInt(h)
end


## Documentation for methods defined elsewhere

"""
Number of rows or columns in an AbstractDataFrame

```julia
nrow(dt::AbstractDataFrame)
ncol(dt::AbstractDataFrame)
```

**Arguments**

* `dt` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated version

See also [`size`](@ref).

NOTE: these functions may be depreciated for `size`.

**Examples**

```julia
dt = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
size(dt)
nrow(dt)
ncol(dt)
```

"""
# nrow, ncol
