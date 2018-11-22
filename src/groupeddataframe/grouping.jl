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

A `GroupedDataFrame` : a grouped view into `d`

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
```

"""
function groupby(df::AbstractDataFrame, cols::AbstractVector;
                 sort::Bool = false, skipmissing::Bool = false)
    intcols = index(df)[cols]
    sdf = df[intcols]
    df_groups = group_rows(sdf, false, sort, skipmissing)
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
wrap(x::AbstractVector) = DataFrame(x1=x)
wrap(x::Any) = (x1=x,)

function wrap_call(f::Union{Function, Type}, gd::GroupedDataFrame,
                   incols::Tuple{Vararg{AbstractVector}}, i::Integer)
    idx = gd.idx[gd.starts[i]:gd.ends[i]]
    wrap(f(map(c -> view(c, idx), incols)...))
end
wrap_call(f::Union{Function, Type}, gd::GroupedDataFrame, incols::Nothing, i::Integer) =
    wrap(f(gd[i]))

"""
    map(f, gd::GroupedDataFrame)

Apply a function to each group of rows and return a `GroupedDataFrame`.

If `f` is a function, it is passed a `SubDataFrame` view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If `f` is a `Pair`, its first element must be a column name or index, or
a vector or tuple thereof, and its second element must be a function to which `SubArray`
views into these columns are passed to `f` as separate arguments.

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

Note that specifying columns via `select` is dramatically faster than `select=nothing`,
and that returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

### Examples

```julia
julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> gd = groupby(df, :a);

julia> map(:c => sum, gd)
GroupedDataFrame{DataFrame} with 4 groups based on key: :a
First Group: 1 row
│ Row │ a     │ c_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
⋮
Last Group: 1 row
│ Row │ a     │ c_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 4     │ 12    │

julia> map(df -> sum(df.c), gd) # Slower variant
GroupedDataFrame{DataFrame} with 4 groups based on key: :a
First Group: 1 row
│ Row │ a     │ x1    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
⋮
Last Group: 1 row
│ Row │ a     │ x1    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 4     │ 12    │
```

### See also

`combine(f, gd)` returns a `DataFrame` rather than a `GroupedDataFrame`

"""
function Base.map(f::Union{Function, Type, Pair}, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(f, gd)
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
        return GroupedDataFrame(parent, gd.cols, Int[], Int[], Int[])
    end
end

"""
    combine(gd::GroupedDataFrame)
    combine(f, gd::GroupedDataFrame)

Transform a `GroupedDataFrame` into a `DataFrame`.
If a function `f` is provided, it is passed a `SubDataFrame` view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If `f` is a `Pair`, its first element must be a column name or index, or
a vector or tuple thereof, and its second element must be a function to which `SubArray`
views into these columns are passed to `f` as separate arguments.

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

Note that specifying columns via `select` is dramatically faster than `select=nothing`,
and that returning a single value or a named tuple is significantly faster than
returning a vector or a data frame.

The resulting data frame will be sorted if `sort=true` was passed to the [`groupby`](@ref)
call from which `gd` was constructed. Otherwise, ordering of rows is undefined.

### Examples

```julia
julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> gd = groupby(df, :a);

julia> combine(:c => sum, gd)
4×2 DataFrame
│ Row │ a     │ c_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

julia> combine(df -> sum(df.c), gd) # Slower variant
4×2 DataFrame
│ Row │ a     │ x1    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │
```

### See also

[`by(f, df, cols)`](@ref) is a shorthand for `combine(f, groupby(df, cols))`.

[`map`](@ref): `combine(f, groupby(df, cols))` is a more efficient equivalent
of `combine(map(f, groupby(df, cols)))`.

"""
function combine(f::Union{Function, Type, Pair}, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(f, gd)
        return hcat!(gd.parent[idx, gd.cols], valscat, makeunique=true)
    else
        return similar(gd.parent[gd.cols], 0)
    end
end

combine(gd::GroupedDataFrame) = combine(identity, gd)

function _combine(f::Union{Function, Type, Pair}, gd::GroupedDataFrame)
    if f isa Pair{<:Union{Symbol,Integer}}
        incols = (gd.parent[first(f)],)
        fun = last(f)
    elseif f isa Pair
        incols = Tuple(columns(gd.parent[collect(first(f))]))
        fun = last(f)
    else
        incols = nothing
        fun = f
    end
    idx, valscat = _combine(wrap_call(fun, gd, incols, 1), fun, gd, incols)
    if f isa Pair{<:Union{Symbol,Integer}} &&
        ncol(valscat) == 1 && names(valscat)[1] === :x1
        nam = Symbol(names(gd.parent)[index(gd.parent)[first(f)]],
                     '_', funname(fun))
        names!(valscat, [nam])
    end
    return idx, valscat
end

function _combine(first::Union{NamedTuple, DataFrameRow}, f::Union{Function, Type},
                  gd::GroupedDataFrame,
                  incols::Union{Nothing, Tuple{Vararg{AbstractVector}}})
    m = length(first)
    n = length(gd)
    idx = Vector{Int}(undef, n)
    initialcols = ntuple(i -> Tables.allocatecolumn(typeof(first[i]), n), m)
    outcols = _combine!(first, initialcols, idx, 1, 1, f, gd, incols,
                        tuple(propertynames(first)...))
    valscat = DataFrame(collect(outcols), collect(propertynames(first)))
    idx, valscat
end

function _combine(first::AbstractDataFrame, f::Union{Function, Type}, gd::GroupedDataFrame,
                  incols::Union{Nothing, Tuple{Vararg{AbstractVector}}})
    m = size(first, 2)
    idx = Vector{Int}()
    initialcols = ntuple(i -> similar(first[i], 0), m)
    outcols = _combine!(first, initialcols, idx, 1, 1, f, gd, incols, names(first))
    valscat = DataFrame(collect(outcols), names(first))
    idx, valscat
end

# Use function barrier to ensure iteration over columns is fast
@noinline function fill_row!(row, outcols::NTuple{N, AbstractVector},
                             i::Integer, colstart::Integer,
                             colnames::NTuple{N, Symbol}) where N
    if !isa(row, Union{NamedTuple, DataFrameRow})
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "`NamedTuple`/`DataFrameRow`, vector or data frame) across groups"))
    elseif length(row) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(length(row)))"))
    end
    @inbounds for j in colstart:length(outcols)
        col = outcols[j]
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

function _combine!(first::Union{NamedTuple, DataFrameRow}, outcols::NTuple{N, AbstractVector},
                   idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                   f::Union{Function, Type}, gd::GroupedDataFrame,
                   incols::Union{Nothing, Tuple{Vararg{AbstractVector}}},
                   colnames::NTuple{N, Symbol}) where N
    n = length(first)
    len = length(gd)
    # Handle first group
    j = fill_row!(first, outcols, rowstart, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    idx[rowstart] = gd.idx[gd.starts[rowstart]]
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        row = wrap_call(f, gd, incols, i)
        j = fill_row!(row, outcols, i, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, outcols=outcols, row=row # Workaround for julia#15276
                newcols = ntuple(n) do k
                    S = typeof(row[k])
                    T = eltype(outcols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        outcols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(outcols[k])),
                                1, outcols[k], 1, k >= j ? i-1 : i)
                    end
                end
            end
            return _combine!(row, newcols, idx, i, j, f, gd, incols, colnames)
        end
        idx[i] = gd.idx[gd.starts[i]]
    end
    outcols
end

# This needs to be in a separate function
# to work around a crash due to JuliaLang/julia#29430
@noinline function do_append!(do_it, col, vals)
    do_it && append!(col, vals)
    return do_it
end

function append_rows!(rows, outcols::NTuple{N, AbstractVector},
                      colstart::Integer, colnames::AbstractVector{Symbol}) where N
    if !isa(rows, AbstractDataFrame)
        throw(ArgumentError("return value must not change its kind (single value, " *
                            "`NamedTuple`/`DataFrameRow`, vector or data frame) across groups"))
    elseif size(rows, 2) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(size(rows, 2)))"))
    end
    @inbounds for j in colstart:length(outcols)
        col = outcols[j]
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

function _combine!(first::AbstractDataFrame, outcols::NTuple{N, AbstractVector},
                   idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                   f::Union{Function, Type}, gd::GroupedDataFrame,
                   incols::Union{Nothing, Tuple{Vararg{AbstractVector}}},
                   colnames::AbstractVector{Symbol}) where N
    n = size(first, 2)
    colnames = names(first)
    len = length(gd)
    # Handle first group
    j = append_rows!(first, outcols, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    append!(idx, Iterators.repeated(gd.idx[gd.starts[rowstart]], size(first, 1)))
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        rows = wrap_call(f, gd, incols, i)
        j = append_rows!(rows, outcols, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, outcols=outcols, rows=rows # Workaround for julia#15276
                newcols = ntuple(n) do k
                    S = eltype(rows[k])
                    T = eltype(outcols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        outcols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(outcols[k])), outcols[k])
                    end
                end
            end
            return _combine!(rows, newcols, idx, i, j, f, gd, incols, colnames)
        end
        append!(idx, Iterators.repeated(gd.idx[gd.starts[i]], size(rows, 1)))
    end
    outcols
end

"""
Apply a function to each column in an AbstractDataFrame or
GroupedDataFrame

```julia
colwise(f, d)
```

### Arguments

* `f` : a function or vector of functions
* `d` : an AbstractDataFrame of GroupedDataFrame

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
by(d::AbstractDataFrame, cols, f; sort::Bool = false)
by(f, d::AbstractDataFrame, cols; sort::Bool = false)
```

### Arguments

* `d` : an AbstractDataFrame
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `f` : a function to be applied to groups; expects each argument to
  be an AbstractDataFrame
* `sort`: sort row groups (no sorting by default)

### Details

If `f` is a function, it is passed a `SubDataFrame` view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If `f` is a `Pair`, its first element must be a column name or index, or
a vector or tuple thereof, and its second element must be a function to which `SubArray`
views into these columns are passed to `f` as separate arguments.

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
julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> by(df, :a, :c => sum)
4×2 DataFrame
│ Row │ a     │ c_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

julia> by(df, :a, d -> sum(d.c)) # Slower variant
4×2 DataFrame
│ Row │ a     │ x1    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

julia> by(df, :a) do d # do syntax for the slower variant
           sum(d.c)
       end
4×2 DataFrame
│ Row │ a     │ x1    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

julia> by(df, :a, :c => x -> 2 .* skipmissing(x))
8×2 DataFrame
│ Row │ a     │ c_function │
│     │ Int64 │ Int64      │
├─────┼───────┼────────────┤
│ 1   │ 1     │ 2          │
│ 2   │ 1     │ 10         │
│ 3   │ 2     │ 4          │
│ 4   │ 2     │ 12         │
│ 5   │ 3     │ 6          │
│ 6   │ 3     │ 14         │
│ 7   │ 4     │ 8          │
│ 8   │ 4     │ 16         │

julia> by(df, :a, :c => x -> (c_sum = sum(x), c_sum2 = sum(x.^2)))
4×3 DataFrame
│ Row │ a     │ c_sum │ c_sum2 │
│     │ Int64 │ Int64 │ Int64  │
├─────┼───────┼───────┼────────┤
│ 1   │ 1     │ 6     │ 26     │
│ 2   │ 2     │ 8     │ 40     │
│ 3   │ 3     │ 10    │ 58     │
│ 4   │ 4     │ 12    │ 80     │

julia> by(df, :a, :c => x -> DataFrame(c = x, c_sum = sum(x)))
8×3 DataFrame
│ Row │ a     │ c     │ c_sum │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 6     │
│ 2   │ 1     │ 5     │ 6     │
│ 3   │ 2     │ 2     │ 8     │
│ 4   │ 2     │ 6     │ 8     │
│ 5   │ 3     │ 3     │ 10    │
│ 6   │ 3     │ 7     │ 10    │
│ 7   │ 4     │ 4     │ 12    │
│ 8   │ 4     │ 8     │ 12    │
```

"""
by(d::AbstractDataFrame, cols, f::Union{Function, Type, Pair}; sort::Bool = false) =
    combine(f, groupby(d, cols, sort = sort))
by(f::Union{Function, Type, Pair}, d::AbstractDataFrame, cols; sort::Bool = false) =
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
aggregate(d::AbstractDataFrame, fs::Union{Function, Type}; sort::Bool=false) =
    aggregate(d, [fs], sort=sort)
function aggregate(d::AbstractDataFrame, fs::Vector{T}; sort::Bool=false) where T<:Union{Function, Type}
    headers = _makeheaders(fs, _names(d))
    _aggregate(d, fs, headers, sort)
end

# Applies aggregate to non-key cols of each SubDataFrame of a GroupedDataFrame
aggregate(gd::GroupedDataFrame, f::Union{Function, Type}; sort::Bool=false) = aggregate(gd, [f], sort=sort)
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
