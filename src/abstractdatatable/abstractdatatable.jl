
"""
An abstract type for which all concrete types expose a database-like
interface.

**Common methods**

An AbstractDataTable is a two-dimensional table with Symbols for
column names. An AbstractDataTable is also similar to an Associative
type in that it allows indexing by a key (the columns).

The following are normally implemented for AbstractDataTables:

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
* `NullableArray` : convert to a NullableArray
* [`completecases`](@ref) : boolean vector of complete cases (rows with no nulls)
* [`dropnull`](@ref) : remove rows with null values
* [`dropnull!`](@ref) : remove rows with null values in-place
* [`nonunique`](@ref) : indexes of duplicate rows
* [`unique!`](@ref) : remove duplicate rows
* `similar` : a DataTable with similar columns as `d`

**Indexing**

Table columns are accessed (`getindex`) by a single index that can be
a symbol identifier, an integer, or a vector of each. If a single
column is selected, just the column object is returned. If multiple
columns are selected, some AbstractDataTable is returned.

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
@compat abstract type AbstractDataTable end

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
## Basic properties of a DataTable
##
##############################################################################

immutable Cols{T <: AbstractDataTable} <: AbstractVector{Any}
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
columns{T <: AbstractDataTable}(dt::T) = Cols{T}(dt)

Base.names(dt::AbstractDataTable) = names(index(dt))
_names(dt::AbstractDataTable) = _names(index(dt))

"""
Set column names


```julia
names!(dt::AbstractDataTable, vals)
```

**Arguments**

* `dt` : the AbstractDataTable
* `vals` : column names, normally a Vector{Symbol} the same length as
  the number of columns in `dt`
* `allow_duplicates` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

**Result**

* `::AbstractDataTable` : the updated result


**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
names!(dt, [:a, :b, :c])
names!(dt, [:a, :b, :a])  # throws ArgumentError
names!(dt, [:a, :b, :a], allow_duplicates=true)  # renames second :a to :a_1
```

"""
function names!(dt::AbstractDataTable, vals; allow_duplicates=false)
    names!(index(dt), vals; allow_duplicates=allow_duplicates)
    return dt
end

function rename!(dt::AbstractDataTable, args...)
    rename!(index(dt), args...)
    return dt
end
rename!(f::Function, dt::AbstractDataTable) = rename!(dt, f)

rename(dt::AbstractDataTable, args...) = rename!(copy(dt), args...)
rename(f::Function, dt::AbstractDataTable) = rename(dt, f)

"""
Rename columns

```julia
rename!(dt::AbstractDataTable, from::Symbol, to::Symbol)
rename!(dt::AbstractDataTable, d::Associative)
rename!(f::Function, dt::AbstractDataTable)
rename(dt::AbstractDataTable, from::Symbol, to::Symbol)
rename(f::Function, dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable
* `d` : an Associative type that maps the original name to a new name
* `f` : a function that has the old column name (a symbol) as input
  and new column name (a symbol) as output

**Result**

* `::AbstractDataTable` : the updated result

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
rename(x -> @compat(Symbol)(uppercase(string(x))), dt)
rename(dt, @compat(Dict(:i=>:A, :x=>:X)))
rename(dt, :y, :Y)
rename!(dt, @compat(Dict(:i=>:A, :x=>:X)))
```

"""
(rename!, rename)

"""
Return element types of columns

```julia
eltypes(dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable

**Result**

* `::Vector{Type}` : the element type of each column

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
eltypes(dt)
```

"""
eltypes(dt::AbstractDataTable) = map!(eltype, Vector{Type}(size(dt,2)), columns(dt))

Base.size(dt::AbstractDataTable) = (nrow(dt), ncol(dt))
function Base.size(dt::AbstractDataTable, i::Integer)
    if i == 1
        nrow(dt)
    elseif i == 2
        ncol(dt)
    else
        throw(ArgumentError("DataTables only have two dimensions"))
    end
end

Base.length(dt::AbstractDataTable) = ncol(dt)
Base.endof(dt::AbstractDataTable) = ncol(dt)

Base.ndims(::AbstractDataTable) = 2

##############################################################################
##
## Similar
##
##############################################################################

Base.similar(dt::AbstractDataTable, dims::Int) =
    DataTable(Any[similar(x, dims) for x in columns(dt)], copy(index(dt)))

##############################################################################
##
## Equality
##
##############################################################################

# Imported in DataTables.jl for compatibility across Julia 0.4 and 0.5
@compat(Base.:(==))(dt1::AbstractDataTable, dt2::AbstractDataTable) = isequal(dt1, dt2)

function Base.isequal(dt1::AbstractDataTable, dt2::AbstractDataTable)
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

Base.haskey(dt::AbstractDataTable, key::Any) = haskey(index(dt), key)
Base.get(dt::AbstractDataTable, key::Any, default::Any) = haskey(dt, key) ? dt[key] : default
Base.isempty(dt::AbstractDataTable) = ncol(dt) == 0

##############################################################################
##
## Description
##
##############################################################################

head(dt::AbstractDataTable, r::Int) = dt[1:min(r,nrow(dt)), :]
head(dt::AbstractDataTable) = head(dt, 6)
tail(dt::AbstractDataTable, r::Int) = dt[max(1,nrow(dt)-r+1):nrow(dt), :]
tail(dt::AbstractDataTable) = tail(dt, 6)

"""
Show the first or last part of an AbstractDataTable

```julia
head(dt::AbstractDataTable, r::Int = 6)
tail(dt::AbstractDataTable, r::Int = 6)
```

**Arguments**

* `dt` : the AbstractDataTable
* `r` : the number of rows to show

**Result**

* `::AbstractDataTable` : the first or last part of `dt`

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
head(dt)
tail(dt)
```

"""
(head, tail)

# get the structure of a DT
"""
Show the structure of an AbstractDataTable, in a tree-like format

```julia
dump(dt::AbstractDataTable, n::Int = 5)
dump(io::IO, dt::AbstractDataTable, n::Int = 5)
```

**Arguments**

* `dt` : the AbstractDataTable
* `n` : the number of levels to show
* `io` : optional output descriptor

**Result**

* nothing

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dump(dt)
```

"""
function Base.dump(io::IO, dt::AbstractDataTable, n::Int, indent)
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
Summarize the columns of an AbstractDataTable

```julia
describe(dt::AbstractDataTable)
describe(io, dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable
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
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
describe(dt)
```

"""
StatsBase.describe(dt::AbstractDataTable) = describe(STDOUT, dt)
function StatsBase.describe(io, dt::AbstractDataTable)
    for (name, col) in eachcol(dt)
        println(io, name)
        describe(io, col)
        println(io, )
    end
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

function _nonnull!(res, col)
    for (i, el) in enumerate(col)
        res[i] &= !_isnull(el)
    end
end

function _nonnull!(res, col::NullableArray)
    for (i, el) in enumerate(col.isnull)
        res[i] &= !el
    end
end

function _nonnull!(res, col::NullableCategoricalArray)
    for (i, el) in enumerate(col.refs)
        res[i] &= el > 0
    end
end


"""
Indexes of complete cases (rows without null values)

```julia
completecases(dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable

**Result**

* `::Vector{Bool}` : indexes of complete cases

See also [`dropnull`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt[[1,4,5], :x] = Nullable()
dt[[9,10], :y] = Nullable()
completecases(dt)
```

"""
function completecases(dt::AbstractDataTable)
    res = trues(size(dt, 1))
    for i in 1:size(dt, 2)
        _nonnull!(res, dt[i])
    end
    res
end

"""
Remove rows with null values.

```julia
dropnull(dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable

**Result**

* `::AbstractDataTable` : the updated copy

See also [`completecases`](@ref) and [`dropnull!`](@ref).

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt[[1,4,5], :x] = Nullable()
dt[[9,10], :y] = Nullable()
dropnull(dt)
```

"""
dropnull(dt::AbstractDataTable) = deleterows!(copy(dt), find(!, completecases(dt)))

"""
Remove rows with null values in-place.

```julia
dropnull!(dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable

**Result**

* `::AbstractDataTable` : the updated version

See also [`dropnull`](@ref) and [`completecases`](@ref).

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt[[1,4,5], :x] = Nullable()
dt[[9,10], :y] = Nullable()
dropnull!(dt)
```

"""
dropnull!(dt::AbstractDataTable) = deleterows!(dt, find(!, completecases(dt)))

function Base.convert(::Type{Array}, dt::AbstractDataTable)
    convert(Matrix, dt)
end
function Base.convert(::Type{Matrix}, dt::AbstractDataTable)
    T = reduce(promote_type, eltypes(dt))
    T <: Nullable && (T = eltype(T))
    convert(Matrix{T}, dt)
end
function Base.convert{T}(::Type{Array{T}}, dt::AbstractDataTable)
    convert(Matrix{T}, dt)
end
function Base.convert{T}(::Type{Matrix{T}}, dt::AbstractDataTable)
    n, p = size(dt)
    res = Matrix{T}(n, p)
    idx = 1
    for (name, col) in zip(names(dt), columns(dt))
        any(isnull, col) && error("cannot convert a DataTable containing null values to array (found for column $name)")
        copy!(res, idx, convert(Vector{T}, col))
        idx += n
    end
    return res
end

function Base.convert(::Type{NullableArray}, dt::AbstractDataTable)
    convert(NullableMatrix, dt)
end
function Base.convert(::Type{NullableMatrix}, dt::AbstractDataTable)
    T = reduce(promote_type, eltypes(dt))
    T <: Nullable && (T = eltype(T))
    convert(NullableMatrix{T}, dt)
end
function Base.convert{T}(::Type{NullableArray{T}}, dt::AbstractDataTable)
    convert(NullableMatrix{T}, dt)
end
function Base.convert{T}(::Type{NullableMatrix{T}}, dt::AbstractDataTable)
    n, p = size(dt)
    res = NullableArray(T, n, p)
    idx = 1
    for col in columns(dt)
        copy!(res, idx, col)
        idx += n
    end
    return res
end

"""
Indexes of duplicate rows (a row that is a duplicate of a prior row)

```julia
nonunique(dt::AbstractDataTable)
nonunique(dt::AbstractDataTable, cols)
```

**Arguments**

* `dt` : the AbstractDataTable
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.) specifying the column(s) to compare

**Result**

* `::Vector{Bool}` : indicates whether the row is a duplicate of some
  prior row

See also [`unique`](@ref) and [`unique!`](@ref).

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt = vcat(dt, dt)
nonunique(dt)
nonunique(dt, 1)
```

"""
function nonunique(dt::AbstractDataTable)
    gslots = row_group_slots(dt)[3]
    # unique rows are the first encountered group representatives,
    # nonunique are everything else
    res = fill(true, nrow(dt))
    @inbounds for g_row in gslots
        (g_row > 0) && (res[g_row] = false)
    end
    return res
end

nonunique(dt::AbstractDataTable, cols::Union{Real, Symbol}) = nonunique(dt[[cols]])
nonunique(dt::AbstractDataTable, cols::Any) = nonunique(dt[cols])

if isdefined(Base, :unique!) # Julia >= 0.7
    import Base.unique!
end

unique!(dt::AbstractDataTable) = deleterows!(dt, find(nonunique(dt)))
unique!(dt::AbstractDataTable, cols::Any) = deleterows!(dt, find(nonunique(dt, cols)))

# Unique rows of an AbstractDataTable.
Base.unique(dt::AbstractDataTable) = dt[(!).(nonunique(dt)), :]
Base.unique(dt::AbstractDataTable, cols::Any) = dt[(!).(nonunique(dt, cols)), :]

"""
Delete duplicate rows

```julia
unique(dt::AbstractDataTable)
unique(dt::AbstractDataTable, cols)
unique!(dt::AbstractDataTable)
unique!(dt::AbstractDataTable, cols)
```

**Arguments**

* `dt` : the AbstractDataTable
* `cols` :  column indicator (Symbol, Int, Vector{Symbol}, etc.)
specifying the column(s) to compare.

**Result**

* `::AbstractDataTable` : the updated version of `dt` with unique rows.
When `cols` is specified, the return DataTable contains complete rows,
retaining in each case the first instance for which `dt[cols]` is unique.

See also [`nonunique`](@ref).

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
dt = vcat(dt, dt)
unique(dt)   # doesn't modify dt
unique(dt, 1)
unique!(dt)  # modifies dt
```

"""
(unique, unique!)

# Count the number of missing values in every column of an AbstractDataTable.
function colmissing(dt::AbstractDataTable) # -> Vector{Int}
    nrows, ncols = size(dt)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countnull(dt[j])
    end
    return missing
end

function without(dt::AbstractDataTable, icols::Vector{Int})
    newcols = setdiff(1:ncol(dt), icols)
    dt[newcols]
end
without(dt::AbstractDataTable, i::Int) = without(dt, [i])
without(dt::AbstractDataTable, c::Any) = without(dt, index(dt)[c])

##############################################################################
##
## Hcat / vcat
##
##############################################################################

# hcat's first argument must be an AbstractDataTable
# Trailing arguments (currently) may also be NullableVectors, Vectors, or scalars.

# hcat! is defined in datatables/datatables.jl
# Its first argument (currently) must be a DataTable.

# catch-all to cover cases where indexing returns a DataTable and copy doesn't
Base.hcat(dt::AbstractDataTable, x) = hcat!(dt[:, :], x)
Base.hcat(dt1::AbstractDataTable, dt2::AbstractDataTable) = hcat!(dt1[:, :], dt2)

Base.hcat(dt::AbstractDataTable, x, y...) = hcat!(hcat(dt, x), y...)
Base.hcat(dt1::AbstractDataTable, dt2::AbstractDataTable, dtn::AbstractDataTable...) = hcat!(hcat(dt1, dt2), dtn...)

@generated function promote_col_type(cols::AbstractVector...)
    elty = Base.promote_eltype(cols...)
    if elty <: Nullable
        elty = eltype(elty)
    end
    if elty <: CategoricalValue
        elty = elty.parameters[1]
    end
    if any(col -> eltype(col) <: Nullable, cols)
        if any(col -> col <: Union{AbstractCategoricalArray, AbstractNullableCategoricalArray}, cols)
            return :(NullableCategoricalVector{$elty})
        else
            return :(NullableVector{$elty})
        end
    else
        if any(col -> col <: Union{AbstractCategoricalArray, AbstractNullableCategoricalArray}, cols)
            return :(CategoricalVector{$elty})
        else
            return :(Vector{$elty})
        end
    end
end

"""
    vcat(dts::AbstractDataTable...)

Vertically concatenate `AbstractDataTables` that have the same column names in
the same order.

# Example
```jldoctest
julia> dt1 = DataTable(A=1:3, B=1:3);
julia> dt2 = DataTable(A=4:6, B=4:6);
julia> vcat(dt1, dt2)
6×2 DataTables.DataTable
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
Base.vcat(dt::AbstractDataTable) = dt
function Base.vcat(dts::AbstractDataTable...)
    isempty(dts) && return DataTable()
    allheaders = map(names, dts)
    if all(h -> length(h) == 0, allheaders)
        return DataTable()
    end
    uniqueheaders = unique(allheaders)
    if length(uniqueheaders) > 1
        unionunique = union(uniqueheaders...)
        coldiff = setdiff(unionunique, intersect(uniqueheaders...))
        if !isempty(coldiff)
            # if any datatables are a full superset of names, skip them
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
        return DataTable(cols, header)
    end
end

##############################################################################
##
## Hashing
##
## Make sure this agrees with isequals()
##
##############################################################################

function Base.hash(dt::AbstractDataTable)
    h = hash(size(dt)) + 1
    for i in 1:size(dt, 2)
        h = hash(dt[i], h)
    end
    return @compat UInt(h)
end


## Documentation for methods defined elsewhere

"""
Number of rows or columns in an AbstractDataTable

```julia
nrow(dt::AbstractDataTable)
ncol(dt::AbstractDataTable)
```

**Arguments**

* `dt` : the AbstractDataTable

**Result**

* `::AbstractDataTable` : the updated version

See also [`size`](@ref).

NOTE: these functions may be depreciated for `size`.

**Examples**

```julia
dt = DataTable(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
size(dt)
nrow(dt)
ncol(dt)
```

"""
# nrow, ncol
