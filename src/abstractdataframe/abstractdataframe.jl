
"""
An abstract type for which all concrete types expose a database-like
interface.

**Common methods**

An AbstractDataFrame is a two-dimensional table with Symbols for
column names. An AbstractDataFrame is also similar to an Associative
type in that it allows indexing by a key (the columns).

The following are normally implemented for AbstractDataFrames:

* [`describe`]({ref}) : summarize columns
* [`dump`]({ref}) : show structure
* `hcat` : horizontal concatenation
* `vcat` : vertical concatenation
* `names` : columns names
* [`names!`]({ref}) : set columns names
* [`rename!`]({ref}) : rename columns names based on keyword arguments
* [`eltypes`]({ref}) : `eltype` of each column
* `length` : number of columns
* `size` : (nrows, ncols)
* [`head`]({ref}) : first `n` rows
* [`tail`]({ref}) : last `n` rows
* `convert` : convert to an array
* `DataArray` : convert to a DataArray
* [`complete_cases`]({ref}) : indexes of complete cases (rows with no NA's)
* [`complete_cases!`]({ref}) : remove rows with NA's
* [`nonunique`]({ref}) : indexes of duplicate rows
* [`unique!`]({ref}) : remove duplicate rows
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
abstract AbstractDataFrame

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

immutable Cols{T <: AbstractDataFrame}
    df::T
end
Base.start(::Cols) = 1
Base.done(itr::Cols, st) = st > length(itr.df)
Base.next(itr::Cols, st) = (itr.df[st], st + 1)
Base.length(itr::Cols) = length(itr.df)

# N.B. where stored as a vector, 'columns(x) = x.vector' is a bit cheaper
columns{T <: AbstractDataFrame}(df::T) = Cols{T}(df)

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
rename(x -> @compat(Symbol)(uppercase(string(x))), df)
rename(df, @compat(Dict(:i=>:A, :x=>:X)))
rename(df, :y, :Y)
rename!(df, @compat(Dict(:i=>:A, :x=>:X)))
```

"""
(rename!, rename)

"""
Column elemental types

```julia
eltypes(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::Vector{Type}` : the elemental type of each column

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
eltypes(df)
```

"""
function eltypes(df::AbstractDataFrame)
    ncols = size(df, 2)
    res = Array(Type, ncols)
    for j in 1:ncols
        res[j] = eltype(df[j])
    end
    return res
end

Base.size(df::AbstractDataFrame) = (nrow(df), ncol(df))
function Base.size(df::AbstractDataFrame, i::Integer)
    if i == 1
        nrow(df)
    elseif i == 2
        ncol(df)
    else
        throw(ArgumentError("DataFrames have only two dimensions"))
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
    DataFrame(Any[similar(x, dims) for x in columns(df)], copy(index(df)))

nas{T}(dv::AbstractArray{T}, dims::@compat(Union{Int, Tuple{Vararg{Int}}})) =   # TODO move to datavector.jl?
    DataArray(Array(T, dims), trues(dims))

nas{T,R}(dv::PooledDataArray{T,R}, dims::@compat(Union{Int, Tuple{Vararg{Int}}})) =
    PooledDataArray(DataArrays.RefArray(zeros(R, dims)), dv.pool)

nas(df::AbstractDataFrame, dims::Int) =
    DataFrame(Any[nas(x, dims) for x in columns(df)], copy(index(df)))

##############################################################################
##
## Equality
##
##############################################################################

function Base.isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    for idx in 1:size(df1, 2)
        isequal(df1[idx], df2[idx]) || return false
    end
    return true
end

# Imported in DataFrames.jl for compatibility across Julia 0.4 and 0.5
function (==)(df1::AbstractDataFrame, df2::AbstractDataFrame)
    size(df1, 2) == size(df2, 2) || return false
    isequal(index(df1), index(df2)) || return false
    eq = true
    for idx in 1:size(df1, 2)
        coleq = df1[idx] == df2[idx]
        # coleq could be NA
        !isequal(coleq, false) || return false
        eq &= coleq
    end
    return eq
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

DataArrays.head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
DataArrays.head(df::AbstractDataFrame) = head(df, 6)
DataArrays.tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
DataArrays.tail(df::AbstractDataFrame) = tail(df, 6)

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

# get the structure of a DF
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
str(df)
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

function Base.dump(io::IO, dv::AbstractDataVector, n::Int, indent)
    println(io, typeof(dv), "(", length(dv), ") ", dv[1:min(4, end)])
end

# summarize the columns of a DF
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
quantile, median, mean, third quantile, and maximum. NA's are filtered and
reported separately.

For boolean columns, report trues, falses, and NAs.

For other types, show column characteristics and number of NAs.

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
StatsBase.describe(dv::AbstractArray) = describe(STDOUT, dv)
function StatsBase.describe{T<:Number}(io, dv::AbstractArray{T})
    if all(isna(dv))
        println(io, " * All NA * ")
        return
    end
    filtered = float(dropna(dv))
    qs = quantile(filtered, [0, .25, .5, .75, 1])
    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    statVals = [qs[1:3]; mean(filtered); qs[4:5]]
    for i = 1:6
        println(io, string(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
    end
    nas = sum(isna(dv))
    println(io, "NAs      $nas")
    println(io, "NA%      $(round(nas*100/length(dv), 2))%")
    return
end
function StatsBase.describe{T}(io, dv::AbstractArray{T})
    ispooled = isa(dv, PooledDataVector) ? "Pooled " : ""
    # if nothing else, just give the length and element type and NA count
    println(io, "Length  $(length(dv))")
    println(io, "Type    $(ispooled)$(string(eltype(dv)))")
    println(io, "NAs     $(sum(isna(dv)))")
    println(io, "NA%     $(round(sum(isna(dv))*100/length(dv), 2))%")
    println(io, "Unique  $(length(unique(dv)))")
    return
end

##############################################################################
##
## Miscellaneous
##
##############################################################################

"""
Indexes of complete cases (rows without NA's)

```julia
complete_cases(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::Vector{Bool}` : indexes of complete cases

See also [`complete_cases!`]({ref}).

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df[[1,4,5], :x] = NA
df[[9,10], :y] = NA
complete_cases(df)
```

"""
function complete_cases(df::AbstractDataFrame)
    ## Returns a Vector{Bool} of indexes of complete cases (rows with no NA's).
    res = !isna(df[1])
    for i in 2:ncol(df)
        res &= !isna(df[i])
    end
    res
end

"""
Delete rows with NA's.

```julia
complete_cases!(df::AbstractDataFrame)
```

**Arguments**

* `df` : the AbstractDataFrame

**Result**

* `::AbstractDataFrame` : the updated version

See also [`complete_cases`]({ref}).

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df[[1,4,5], :x] = NA
df[[9,10], :y] = NA
complete_cases!(df)
```

"""
complete_cases!(df::AbstractDataFrame) = deleterows!(df, find(!complete_cases(df)))

function Base.convert(::Type{Array}, df::AbstractDataFrame)
    convert(Matrix, df)
end
function Base.convert(::Type{Matrix}, df::AbstractDataFrame)
    T = reduce(typejoin, eltypes(df))
    convert(Matrix{T}, df)
end
function Base.convert{T}(::Type{Array{T}}, df::AbstractDataFrame)
    convert(Matrix{T}, df)
end
function Base.convert{T}(::Type{Matrix{T}}, df::AbstractDataFrame)
    n, p = size(df)
    res = Array(T, n, p)
    idx = 1
    for col in columns(df)
        anyna(col) && error("DataFrame contains NAs")
        copy!(res, idx, data(col))
        idx += n
    end
    return res
end

function Base.convert(::Type{DataArray}, df::AbstractDataFrame)
    convert(DataMatrix, df)
end
function Base.convert(::Type{DataMatrix}, df::AbstractDataFrame)
    T = reduce(typejoin, eltypes(df))
    convert(DataMatrix{T}, df)
end
function Base.convert{T}(::Type{DataArray{T}}, df::AbstractDataFrame)
    convert(DataMatrix{T}, df)
end
function Base.convert{T}(::Type{DataMatrix{T}}, df::AbstractDataFrame)
    n, p = size(df)
    res = DataArray(T, n, p)
    idx = 1
    for col in columns(df)
        copy!(res, idx, col)
        idx += n
    end
    return res
end

"""
Indexes of complete cases (rows without NA's)

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

See also [`unique`]({ref}) and [`unique!`]({ref}).

**Examples**

```julia
df = DataFrame(i = 1:10, x = rand(10), y = rand(["a", "b", "c"], 10))
df = vcat(df, df)
nonunique(df)
nonunique(df, 1)
```

"""
function nonunique(df::AbstractDataFrame)
    res = fill(false, nrow(df))
    rows = Set{DataFrameRow}()
    for i in 1:nrow(df)
        arow = DataFrameRow(df, i)
        if in(arow, rows)
            res[i] = true
        else
            push!(rows, arow)
        end
    end
    res
end

nonunique(df::AbstractDataFrame, cols::Union{Real, Symbol}) = nonunique(df[[cols]])
nonunique(df::AbstractDataFrame, cols::Any) = nonunique(df[cols])

unique!(df::AbstractDataFrame) = deleterows!(df, find(nonunique(df)))
unique!(df::AbstractDataFrame, cols::Any) = deleterows!(df, find(nonunique(df, cols)))

# Unique rows of an AbstractDataFrame.
Base.unique(df::AbstractDataFrame) = df[!nonunique(df), :]
Base.unique(df::AbstractDataFrame, cols::Any) = df[!nonunique(df, cols), :]

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

See also [`nonunique`]({ref}).

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

function nonuniquekey(df::AbstractDataFrame)
    # Here's another (probably a lot faster) way to do `nonunique`
    # by grouping on all columns. It will fail if columns cannot be
    # made into PooledDataVector's.
    gd = groupby(df, _names(df))
    idx = [1:length(gd.idx)][gd.idx][gd.starts]
    res = fill(true, nrow(df))
    res[idx] = false
    res
end

# Count the number of missing values in every column of an AbstractDataFrame.
function colmissing(df::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(df)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countna(df[j])
    end
    return missing
end

function without(df::AbstractDataFrame, icols::Vector{Int})
    newcols = _setdiff(1:ncol(df), icols)
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
# Trailing arguments (currently) may also be DataVectors, Vectors, or scalars.

# hcat! is defined in dataframes/dataframes.jl
# Its first argument (currently) must be a DataFrame.

# catch-all to cover cases where indexing returns a DataFrame and copy doesn't
Base.hcat(df::AbstractDataFrame, x) = hcat!(df[:, :], x)

Base.hcat(df::AbstractDataFrame, x, y...) = hcat!(hcat(df, x), y...)

# vcat only accepts DataFrames. Finds union of columns, maintaining order
# of first df. Missing data becomes NAs.

Base.vcat(df::AbstractDataFrame) = df

Base.vcat(dfs::AbstractDataFrame...) = vcat(AbstractDataFrame[dfs...])

Base.vcat(dfs::Vector{Void}) = dfs
function Base.vcat{T<:AbstractDataFrame}(dfs::Vector{T})
    isempty(dfs) && return DataFrame()
    coltyps, colnams, similars = _colinfo(dfs)

    res = DataFrame()
    Nrow = sum(nrow, dfs)
    for j in 1:length(colnams)
        colnam = colnams[j]
        col = similar(similars[j], coltyps[j], Nrow)

        i = 1
        for df in dfs
            if haskey(df, colnam) && eltype(df[colnam]) != NAtype
                copy!(col, i, df[colnam])
            end
            i += size(df, 1)
        end

        res[colnam] = col
    end
    res
end

_isnullable(::AbstractArray) = false
_isnullable(::AbstractDataArray) = true
const EMPTY_DATA = DataArray(Void, 0)

function _colinfo{T<:AbstractDataFrame}(dfs::Vector{T})
    df1 = dfs[1]
    colindex = copy(index(df1))
    coltyps = eltypes(df1)
    similars = collect(columns(df1))
    nonnull_ct = Int[_isnullable(c) for c in columns(df1)]

    for i in 2:length(dfs)
        df = dfs[i]
        for j in 1:size(df, 2)
            col = df[j]
            cn, ct = _names(df)[j], eltype(col)
            if haskey(colindex, cn)
                idx = colindex[cn]

                oldtyp = coltyps[idx]
                if !(ct <: oldtyp)
                    coltyps[idx] = promote_type(oldtyp, ct)
                end
                nonnull_ct[idx] += !_isnullable(col)
            else # new column
                push!(colindex, cn)
                push!(coltyps, ct)
                push!(similars, col)
                push!(nonnull_ct, !_isnullable(col))
            end
        end
    end

    for j in 1:length(colindex)
        if nonnull_ct[j] < length(dfs) && !_isnullable(similars[j])
            similars[j] = EMPTY_DATA
        end
    end
    colnams = _names(colindex)

    coltyps, colnams, similars
end

##############################################################################
##
## Hashing
##
## Make sure this agrees with is_equals()
##
##############################################################################

function Base.hash(df::AbstractDataFrame)
    h = hash(size(df)) + 1
    for i in 1:size(df, 2)
        h = hash(df[i], h)
    end
    return @compat UInt(h)
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

See also [`size`]({ref}).

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
