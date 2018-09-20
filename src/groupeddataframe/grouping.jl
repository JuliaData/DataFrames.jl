"""
    GroupedDataFrame

The result of a `groupby` operation on an AbstractDataFrame; a
view into the AbstractDataFrame grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
struct GroupedDataFrame{T<:AbstractDataFrame}
    parent::T
    cols::Vector{Int}    # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
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
indexing by groups and `map` (which applies a function to each group
and combines the result into a data frame).

See the following for additional split-apply-combine operations:

* `by` : split-apply-combine using functions
* `aggregate` : split-apply-combine; applies functions in the form of a cross product
* `colwise` : apply a function to each column in an AbstractDataFrame or GroupedDataFrame

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
map(d -> mean(skipmissing(d[:c])), gd)
```

"""
function groupby(df::AbstractDataFrame, cols::Vector;
                 sort::Bool = false, skipmissing::Bool = false)
    sdf = df[cols]
    df_groups = group_rows(sdf, skipmissing)
    # sort the groups
    if sort
        group_perm = sortperm(view(sdf, df_groups.rperm[df_groups.starts], :))
        permute!(df_groups.starts, group_perm)
        Base.permute!!(df_groups.stops, group_perm)
    end
    GroupedDataFrame(df, DataFrames.index(df)[cols], df_groups.rperm,
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

wrap(df::AbstractDataFrame) = df
wrap(nt::NamedTuple) = nt
wrap(A::Matrix) = convert(DataFrame, A)
wrap(s::Union{AbstractVector, Tuple}) = DataFrame(x1 = s)
wrap(s::Any) = (x1 = s,)

"""
Apply a function to each group of rows and combine the result

```julia
map(f::Function, gd::GroupedDataFrame)
```

### Arguments

* `gd` : a `GroupedDataFrame` object

### Returns

* `::DataFrame`

### Details

For each group in `gd`, `f` is passed a `SubDataFrame` view with the corresponding rows.
`f` can return a single value, a named tuple, a vector, or a data frame.
This determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple gives a data frame with one column for each field in the named tuple
  and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame gives a data frame with the same columns and as many rows
  for each group as the rows returned for that group.

In all cases, the resulting data frame contains all the grouping columns in addition
to those listed above. Note that `f` must always return the same type of object for
all groups, and (if a named tuple or data frame) with the same fields or columns in the
same order. Returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

The resulting data frame will be sorted if `sort=true` was passed to the [`groupby`](@ref)
call from which `gd` was constructed. Otherwise, ordering of rows is undefined.

### Examples

```julia
df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
gd = groupby(df, :a)
map(d -> sum(skipmissing(d[:c])), gd)
```

### See also

`by(f, df, cols)` is a shorthand for `map(f, groupby(df, cols))`.

"""
function Base.map(f::Function, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(wrap(f(gd[1])), f, gd)
        return hcat!(gd.parent[idx, gd.cols], valscat)
    else
        return similar(gd.parent, 0)[gd.cols]
    end
end

function _combine(first::NamedTuple, f::Function, gd::GroupedDataFrame)
    m = length(first)
    n = length(gd)
    idx = Vector{Int}(undef, n)
    initialcols = ntuple(i -> Vector{typeof(first[i])}(undef, n), m)
    cols = _combine!(first, initialcols, idx, 1, 1, f, gd, propertynames(first))
    valscat = DataFrame(collect(cols), collect(propertynames(first)))
    idx, valscat
end

function _combine(first::AbstractDataFrame, f::Function, gd::GroupedDataFrame)
    m = size(first, 2)
    idx = Vector{Int}()
    initialcols = ntuple(i -> Vector{eltype(first[i])}(), m)
    cols = _combine!(first, initialcols, idx, 1, 1, f, gd, names(first))
    valscat = DataFrame(collect(cols), names(first))
    idx, valscat
end

# Use function barrier to ensure iteration over columns is fast
@noinline function fill_row!(row, cols::NTuple{N, AbstractVector},
                             i::Integer, colstart::Integer,
                             colnames::NTuple{N, Symbol}) where N
    if !isa(row, NamedTuple)
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "named tuple, vector or data frame) across groups"))
    elseif length(row) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(length(row)))"))
    elseif propertynames(row) != colnames
        throw(ArgumentError("return value must have the same column names " *
                            "for all groups (got $colnames and $(propertynames(row)))"))
    end
    @inbounds for j in colstart:length(cols)
        col = cols[j]
        val = row[j]
        if val isa eltype(col)
            col[i] = val
        else
            return j
        end
    end
    return nothing
end

function _combine!(first::NamedTuple, cols::NTuple{N, AbstractVector}, idx::Vector{Int},
                   rowstart::Integer, colstart::Integer,
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
                    if S <: T
                        return cols[k]
                    else
                        return copyto!(Vector{promote_type(S, T)}(undef, len), 1,
                                    cols[k], 1, k >= j ? i-1 : i)
                    end
                end
            end
            return _combine!(row, newcols, idx, i, j, f, gd, colnames)
        end
        idx[i] = gd.idx[gd.starts[i]]
    end
    cols
end

function append_rows!(rows, cols::NTuple{N, AbstractVector},
                      colstart::Integer, colnames::AbstractVector{Symbol}) where N
    if !isa(rows, AbstractDataFrame)
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "named tuple, vector or data frame) across groups"))
    elseif size(rows, 2) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(size(rows, 2)))"))
    elseif names(rows) != colnames
        throw(ArgumentError("return value must have the same column names " *
                            "for all groups (got $(Tuple(colnames)) and $(Tuple(names(rows))))"))
    end
    @inbounds for j in colstart:length(cols)
        col = cols[j]
        vals = rows[j]
        if eltype(vals) <: eltype(col)
            append!(col, vals)
        else
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
                    if S <: T
                        return cols[k]
                    else
                        return copyto!(similar(cols[k], promote_type(S, T)), cols[k])
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
- A named tuple gives a data frame with one column for each field in the named tuple
  and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame gives a data frame with the same columns and as many rows
  for each group as the rows returned for that group.

In all cases, the resulting data frame contains all the grouping columns in addition
to those listed above. Note that `f` must always return the same type of object for
all groups, and (if a named tuple or data frame) with the same fields or columns in the
same order. Returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

A method is defined with `f` as the first argument, so do-block
notation can be used.

`by(d, cols, f)` is equivalent to `map(f, groupby(d, cols))`.

### Returns

* `::DataFrame`

### Examples

```julia
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
    map(f, groupby(d, cols, sort = sort))
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
    res = map(x -> _aggregate(without(x, gd.cols), fs, headers), gd)
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
