"""
    GroupedDataFrame

The result of a `groupby` operation on an AbstractDataFrame; a
view into the AbstractDataFrame grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
struct GroupedDataFrame{T<:AbstractDataFrame}
    parent::T
    cols::Vector{Int}    # columns used for grouping
    idx::Vector{Int}     # indexing vector when grouped by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups
end

#
# Split
#

"""
A view of an AbstractDataFrame split into row groups

```julia
groupby(d::AbstractDataFrame, cols; sort = false, skipmissing = false)
groupby(cols; sort = false, skipmissing = false)
```

### Arguments

* `d` : an AbstractDataFrame to split (optional, see [Returns](#returns))
* `cols` : data table columns to group by
* `sort`: whether to sort rows according to the values of the grouping columns `cols`
* `skipmissing`: whether to skip rows with `missing` values in one of the grouping columns `cols`

### Returns

* `::GroupedDataFrame` : a grouped view into `d`
* `::Function`: a function `x -> groupby(x, cols)` (if `d` is not specified)

### Details

An iterator over a `GroupedDataFrame` returns a `SubDataFrame` view
for each grouping into `d`. A `GroupedDataFrame` also supports
indexing by groups, `map` (which applies a function to each group)
and `combine` (which applies a function to each group
and combines the result into a data frame).

See the following for additional split-apply-combine operations:

* `by` : split-apply-combine using functions
* `aggregate` : split-apply-combine; applies functions in the form of a cross product
* `colwise` : apply a function to each column in an `AbstractDataFrame` or `GroupedDataFrame`
* `map` : apply a function to each group of a `GroupedDataFrame` (without combining)
* `combine` : combine a `GroupedDataFrame`, optionally applying a function to each group

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
gd = groupby(df, :a)
gd[1]
last(gd)
vcat([g[:b] for g in gd]...)
for g in gd
    println(g)
end
map(d -> sum(skipmissing(d[:c])), gd)
combine(d -> sum(skipmissing(d[:c])), gd)
```

"""
function groupby(df::AbstractDataFrame, cols::AbstractVector;
                 sort::Bool = false, skipmissing::Bool = false)
    intcols = index(df)[cols]
    sdf = df[intcols]
    df_groups = group_rows(sdf, skipmissing)
    # sort the groups
    if sort
        group_perm = sortperm(view(sdf, df_groups.rperm[df_groups.starts], :))
        permute!(df_groups.starts, group_perm)
        Base.permute!!(df_groups.stops, group_perm)
    end
    GroupedDataFrame(df, intcols, df_groups.rperm,
                     df_groups.starts, df_groups.stops)
end
groupby(d::AbstractDataFrame, cols;
        sort::Bool = false, skipmissing::Bool = false) =
    groupby(d, [cols], sort = sort, skipmissing = skipmissing)

function Base.iterate(gd::GroupedDataFrame, i=1)
    if i > length(gd.starts)
        nothing
    else
        (view(gd.parent, gd.idx[gd.starts[i]:gd.ends[i]], :), i+1)
    end
end

Base.length(gd::GroupedDataFrame) = length(gd.starts)
Compat.lastindex(gd::GroupedDataFrame) = length(gd.starts)
Base.first(gd::GroupedDataFrame) = gd[1]
Base.last(gd::GroupedDataFrame) = gd[end]

Base.getindex(gd::GroupedDataFrame, idx::Integer) =
    view(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]], :)
Base.getindex(gd::GroupedDataFrame, idxs::AbstractArray) =
    GroupedDataFrame(gd.parent, gd.cols, gd.idx, gd.starts[idxs], gd.ends[idxs])
Base.getindex(gd::GroupedDataFrame, idxs::Colon) =
    GroupedDataFrame(gd.parent, gd.cols, gd.idx, gd.starts, gd.ends)

function Base.:(==)(gd1::GroupedDataFrame, gd2::GroupedDataFrame)
    gd1.cols == gd2.cols &&
        length(gd1) == length(gd2) &&
        all(x -> ==(x...), zip(gd1, gd2))
end

function Base.isequal(gd1::GroupedDataFrame, gd2::GroupedDataFrame)
    isequal(gd1.cols, gd2.cols) &&
        isequal(length(gd1), length(gd2)) &&
        all(x -> isequal(x...), zip(gd1, gd2))
end

Base.names(gd::GroupedDataFrame) = names(gd.parent)
_names(gd::GroupedDataFrame) = _names(gd.parent)

# Wrapping automatically adds column names when the value returned
# by the user-provided function lacks them
wrap(x::Union{AbstractDataFrame, NamedTuple, DataFrameRow}) = x
wrap(x::AbstractMatrix) = convert(DataFrame, x)
wrap(x::AbstractVector) = DataFrame(x1 = x)
wrap(x::Any) = (x1 = x,)

"""
    map(f::Function, gd::GroupedDataFrame)

Apply a function to each group of rows and return a `GroupedDataFrame`.

For each group in `gd`, `f` is passed a `SubDataFrame` view holding the corresponding rows.
`f` can return a single value, a row or multiple rows. The type of the returned value
determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple or a `DataFrameRow` gives a data frame with one column for each field
  and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame or a matrix gives a data frame with the same columns and as many rows
  for each group as the rows returned for that group.

In all cases, the resulting `GroupedDataFrame` contains all the grouping columns in addition
to those listed above. Note that `f` must always return the same type of object for
all groups, and (if a named tuple or data frame) with the same fields or columns.
Returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
gd = groupby(df, :a)
map(d -> sum(skipmissing(d[:c])), gd)
```

### See also

`combine(f, gd)` returns a `DataFrame` rather than a `GroupedDataFrame`

"""
function Base.map(f::Function, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(wrap(f(gd[1])), f, gd)
        parent = hcat!(gd.parent[idx, gd.cols], valscat)
        starts = Vector{Int}(undef, length(gd))
        ends = Vector{Int}(undef, length(gd))
        starts[1] = 1
        j = 2
        @inbounds for i in 2:length(idx)
            if idx[i] != idx[i-1]
                starts[j] = i
                ends[j-1] = i - 1
                j += 1
            end
        end
        # In case some groups have to be dropped
        resize!(starts, j-1)
        resize!(ends, j-1)
        ends[end] = length(idx)
        return GroupedDataFrame(parent, gd.cols, collect(1:length(idx)), starts, ends)
    else
        return GroupedDataFrame(similar(gd.parent[gd.cols], 0), gd.cols,
                                Int[], Int[], Int[])
    end
end

"""
    combine(gd::GroupedDataFrame)
    combine(f::Function, gd::GroupedDataFrame)

Transform a `GroupedDataFrame` into a `DataFrame`.
If a function `f` is provided, it is called for each group in `gd` with a `SubDataFrame` view
holding the corresponding rows, and the returned `DataFrame` then consists of the returned rows
plus the grouping columns.

`f` can return a single value, a row or multiple rows. The type of the returned value
determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple or a `DataFrameRow` gives a data frame with one column for each field
  and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame or a matrix gives a data frame with the same columns and as many rows
  for each group as the rows returned for that group.

In all cases, the resulting data frame contains all the grouping columns in addition
to those listed above. Note that `f` must always return the same type of object for
all groups, and (if a named tuple or data frame) with the same fields or columns.
Returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

The resulting data frame will be sorted if `sort=true` was passed to the [`groupby`](@ref)
call from which `gd` was constructed. Otherwise, ordering of rows is undefined.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
gd = groupby(df, :a)
combine(d -> sum(skipmissing(d[:c])), gd)
```

### See also

[`by(f, df, cols)`](@ref) is a shorthand for `combine(f, groupby(df, cols))`.

[`map`](@ref): `combine(f, groupby(df, cols))` is a more efficient equivalent
of `combine(map(f, groupby(df, cols)))`.

"""
function combine(f::Function, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(wrap(f(gd[1])), f, gd)
        return hcat!(gd.parent[idx, gd.cols], valscat, makeunique=true)
    else
        return similar(gd.parent[gd.cols], 0)
    end
end

combine(gd::GroupedDataFrame) = combine(identity, gd)

function _combine(first::Union{NamedTuple, DataFrameRow}, f::Function,
                  gd::GroupedDataFrame)
    m = length(first)
    n = length(gd)
    idx = Vector{Int}(undef, n)
    initialcols = ntuple(i -> Tables.allocatecolumn(typeof(first[i]), n), m)
    cols = _combine!(first, initialcols, idx, 1, 1, f, gd, tuple(propertynames(first)...))
    valscat = DataFrame(collect(cols), collect(propertynames(first)))
    idx, valscat
end

function _combine(first::AbstractDataFrame, f::Function, gd::GroupedDataFrame)
    m = size(first, 2)
    idx = Vector{Int}()
    initialcols = ntuple(i -> similar(first[i], 0), m)
    cols = _combine!(first, initialcols, idx, 1, 1, f, gd, names(first))
    valscat = DataFrame(collect(cols), names(first))
    idx, valscat
end

# Use function barrier to ensure iteration over columns is fast
@noinline function fill_row!(row, cols::NTuple{N, AbstractVector},
                             i::Integer, colstart::Integer,
                             colnames::NTuple{N, Symbol}) where N
    if !isa(row, Union{NamedTuple, DataFrameRow})
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "`NamedTuple`/`DataFrameRow`, vector or data frame) across groups"))
    elseif length(row) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(length(row)))"))
    end
    @inbounds for j in colstart:length(cols)
        col = cols[j]
        cn = colnames[j]
        local val
        try
            val = row[cn]
        catch
            throw(ArgumentError("return value must have the same column names " *
                                "for all groups (got $colnames and $(propertynames(row)))"))
        end
        S = typeof(val)
        T = eltype(col)
        if S <: T || promote_type(S, T) <: T
            col[i] = val
        else
            return j
        end
    end
    return nothing
end

function _combine!(first::Union{NamedTuple, DataFrameRow}, cols::NTuple{N, AbstractVector},
                   idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                   f::Function, gd::GroupedDataFrame, colnames::NTuple{N, Symbol}) where N
    n = length(first)
    len = length(gd)
    # Handle first group
    j = fill_row!(first, cols, rowstart, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    idx[rowstart] = gd.idx[gd.starts[rowstart]]
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        row = wrap(f(gd[i]))
        j = fill_row!(row, cols, i, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, cols=cols, row=row # Workaround for julia#15276
                newcols = ntuple(n) do k
                    S = typeof(row[k])
                    T = eltype(cols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        cols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(cols[k])),
                                1, cols[k], 1, k >= j ? i-1 : i)
                    end
                end
            end
            return _combine!(row, newcols, idx, i, j, f, gd, colnames)
        end
        idx[i] = gd.idx[gd.starts[i]]
    end
    cols
end

# This needs to be in a separate function
# to work around a crash due to JuliaLang/julia#29430
@noinline function do_append!(do_it, col, vals)
    do_it && append!(col, vals)
    return do_it
end

function append_rows!(rows, cols::NTuple{N, AbstractVector},
                      colstart::Integer, colnames::AbstractVector{Symbol}) where N
    if !isa(rows, AbstractDataFrame)
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "`NamedTuple`/`DataFrameRow`, vector or data frame) across groups"))
    elseif size(rows, 2) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(size(rows, 2)))"))
    end
    @inbounds for j in colstart:length(cols)
        col = cols[j]
        cn = colnames[j]
        local vals
        try
            vals = rows[cn]
        catch
            throw(ArgumentError("return value must have the same column names " *
                                "for all groups (got $(Tuple(colnames)) and $(Tuple(names(rows))))"))
        end
        S = eltype(vals)
        T = eltype(col)
        if !do_append!(S <: T || promote_type(S, T) <: T, col, vals)
            return j
        end
    end
    return nothing
end

function _combine!(first::AbstractDataFrame, cols::NTuple{N, AbstractVector},
                   idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                   f::Function, gd::GroupedDataFrame,
                   colnames::AbstractVector{Symbol}) where N
    n = size(first, 2)
    colnames = names(first)
    len = length(gd)
    # Handle first group
    j = append_rows!(first, cols, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    append!(idx, Iterators.repeated(gd.idx[gd.starts[rowstart]], size(first, 1)))
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        rows = wrap(f(gd[i]))
        j = append_rows!(rows, cols, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, cols=cols, rows=rows # Workaround for julia#15276
                newcols = ntuple(n) do k
                    S = eltype(rows[k])
                    T = eltype(cols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        cols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(cols[k])), cols[k])
                    end
                end
            end
            return _combine!(rows, newcols, idx, i, j, f, gd, colnames)
        end
        append!(idx, Iterators.repeated(gd.idx[gd.starts[i]], size(rows, 1)))
    end
    cols
end

"""
Apply a function to each column in an AbstractDataFrame or
GroupedDataFrame

```julia
colwise(f::Function, d)
colwise(d)
```

### Arguments

* `f` : a function or vector of functions
* `d` : an AbstractDataFrame of GroupedDataFrame

If `d` is not provided, a curried version of groupby is given.

### Returns

* various, depending on the call

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
colwise(sum, df)
colwise([sum, length], df)
colwise((minimum, maximum), df)
colwise(sum, groupby(df, :a))
```

"""
colwise(f, d::AbstractDataFrame) = [f(d[i]) for i in 1:ncol(d)]

# apply several functions to each column in a data frame
colwise(fns::Union{AbstractVector, Tuple}, d::AbstractDataFrame) = [f(d[i]) for f in fns, i in 1:ncol(d)]
colwise(f, gd::GroupedDataFrame) = [colwise(f, g) for g in gd]

"""
Split-apply-combine in one step; apply `f` to each grouping in `d`
based on columns `col`

```julia
by(d::AbstractDataFrame, cols, f::Function; sort::Bool = false)
by(f::Function, d::AbstractDataFrame, cols; sort::Bool = false)
```

### Arguments

* `d` : an AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `f` : a function to be applied to groups; expects each argument to
  be an AbstractDataFrame
* `sort`: sort row groups (no sorting by default)

### Details

For each group in `gd`, `f` is passed a `SubDataFrame` view with the corresponding rows.
`f` can return a single value, a named tuple, a vector, or a data frame.
This determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple or a `DataFrameRow` gives a data frame with one column for each field
and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame gives a data frame with the same columns and as many rows
  for each group as the rows returned for that group.

In all cases, the resulting data frame contains all the grouping columns in addition
to those listed above. Note that `f` must always return the same type of object for
all groups, and (if a named tuple or data frame) with the same fields or columns.
Returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

A method is defined with `f` as the first argument, so do-block
notation can be used.

`by(d, cols, f)` is equivalent to `combine(f, groupby(d, cols))`.

### Returns

* `::DataFrame`

### Examples

```julia
using Statistics
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
by(df, :a, d -> sum(d[:c]))
by(df, :a, d -> 2 * skipmissing(d[:c]))
by(df, :a, d -> (c_sum = sum(d[:c]), c_mean = mean(skipmissing(d[:c]))))
by(df, :a, d -> DataFrame(c = d[:c], c_mean = mean(skipmissing(d[:c]))))
by(df, [:a, :b]) do d
    (m = mean(skipmissing(d[:c])), v = var(skipmissing(d[:c])))
end
```

"""
by(d::AbstractDataFrame, cols, f::Function; sort::Bool = false) =
    combine(f, groupby(d, cols, sort = sort))
by(f::Function, d::AbstractDataFrame, cols; sort::Bool = false) =
    by(d, cols, f, sort = sort)

#
# Aggregate convenience functions
#

# Applies a set of functions over a DataFrame, in the from of a cross-product
"""
Split-apply-combine that applies a set of functions over columns of an
AbstractDataFrame or GroupedDataFrame

```julia
aggregate(d::AbstractDataFrame, cols, fs)
aggregate(gd::GroupedDataFrame, fs)
```

### Arguments

* `d` : an AbstractDataFrame
* `gd` : a GroupedDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `fs` : a function or vector of functions to be applied to vectors
  within groups; expects each argument to be a column vector

Each `fs` should return a value or vector. All returns must be the
same length.

### Returns

* `::DataFrame`

### Examples

```julia
using Statistics
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
aggregate(df, :a, sum)
aggregate(df, :a, [sum, x->mean(skipmissing(x))])
aggregate(groupby(df, :a), [sum, x->mean(skipmissing(x))])
```

"""
aggregate(d::AbstractDataFrame, fs::Function; sort::Bool=false) = aggregate(d, [fs], sort=sort)
function aggregate(d::AbstractDataFrame, fs::Vector{T}; sort::Bool=false) where T<:Function
    headers = _makeheaders(fs, _names(d))
    _aggregate(d, fs, headers, sort)
end

# Applies aggregate to non-key cols of each SubDataFrame of a GroupedDataFrame
aggregate(gd::GroupedDataFrame, f::Function; sort::Bool=false) = aggregate(gd, [f], sort=sort)
function aggregate(gd::GroupedDataFrame, fs::Vector{T}; sort::Bool=false) where T<:Function
    headers = _makeheaders(fs, setdiff(_names(gd), _names(gd.parent[gd.cols])))
    res = combine(x -> _aggregate(without(x, gd.cols), fs, headers), gd)
    sort && sort!(res, headers)
    res
end

# Groups DataFrame by cols before applying aggregate
function aggregate(d::AbstractDataFrame,
                   cols::Union{S, AbstractVector{S}},
                   fs::Union{T, Vector{T}};
                   sort::Bool=false) where {S<:ColumnIndex, T <:Function}
    aggregate(groupby(d, cols, sort=sort), fs)
end

function funname(f)
    n = nameof(f)
    String(n)[1] == '#' ? :function : n
end

_makeheaders(fs::Vector{<:Function}, cn::Vector{Symbol}) =
    [Symbol(colname, '_', funname(f)) for f in fs for colname in cn]

function _aggregate(d::AbstractDataFrame, fs::Vector{T}, headers::Vector{Symbol}, sort::Bool=false) where T<:Function
    res = DataFrame(AbstractVector[vcat(f(d[i])) for f in fs for i in 1:size(d, 2)], headers, makeunique=true)
    sort && sort!(res, headers)
    res
end
