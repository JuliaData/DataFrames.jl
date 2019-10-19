"""
    GroupedDataFrame

The result of a [`groupby`](@ref) operation on an `AbstractDataFrame`; a
view into the `AbstractDataFrame` grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
struct GroupedDataFrame{T<:AbstractDataFrame}
    parent::T
    cols::Vector{Int}    # columns used for grouping
    groups::Vector{Int}  # group indices for each row
    idx::Vector{Int}     # indexing vector when grouped by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups
end

Base.broadcastable(::GroupedDataFrame) =
    throw(ArgumentError("broadcasting over `GroupedDataFrame`s is reserved"))

"""
    parent(gd::GroupedDataFrame)

Return the parent data frame of `gd`.
"""
Base.parent(gd::GroupedDataFrame) = getfield(gd, :parent)

#
# Split
#

"""
A view of an `AbstractDataFrame` split into row groups

```julia
groupby(d::AbstractDataFrame, cols; sort=false, skipmissing=false)
```

### Arguments

* `df` : an `AbstractDataFrame` to split
* `cols` : data frame columns to group by
* `sort` : whether to sort rows according to the values of the grouping columns `cols`
* `skipmissing` : whether to skip rows with `missing` values in one of the grouping columns `cols`

### Returns

A `GroupedDataFrame` : a grouped view into `df`

### Details

An iterator over a `GroupedDataFrame` returns a `SubDataFrame` view
for each grouping into `df`.
Within each group, the order of rows in `df` is preserved.

`cols` can be any valid data frame indexing expression.
In particular if it is an empty vector then a single-group `GroupedDataFrame`
is created.

A `GroupedDataFrame` also supports
indexing by groups, `map` (which applies a function to each group)
and `combine` (which applies a function to each group
and combines the result into a data frame).

See the following for additional split-apply-combine operations:

* [`by`](@ref) : split-apply-combine using functions
* [`aggregate`](@ref) : split-apply-combine; applies functions in the form of a cross product
* [`map`](@ref) : apply a function to each group of a `GroupedDataFrame` (without combining)
* [`combine`](@ref) : combine a `GroupedDataFrame`, optionally applying a function to each group

### Examples

```julia
julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> gd = groupby(df, :a)
GroupedDataFrame with 4 groups based on key: a
First Group (2 rows): a = 1
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 1     │
│ 2   │ 1     │ 2     │ 5     │
⋮
Last Group (2 rows): a = 4
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 4     │ 1     │ 4     │
│ 2   │ 4     │ 1     │ 8     │

julia> gd[1]
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 1     │
│ 2   │ 1     │ 2     │ 5     │

julia> last(gd)
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 4     │ 1     │ 4     │
│ 2   │ 4     │ 1     │ 8     │

julia> for g in gd
           println(g)
       end
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 1     │
│ 2   │ 1     │ 2     │ 5     │
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 2     │ 1     │ 2     │
│ 2   │ 2     │ 1     │ 6     │
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 3     │ 2     │ 3     │
│ 2   │ 3     │ 2     │ 7     │
2×3 SubDataFrame
│ Row │ a     │ b     │ c     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 4     │ 1     │ 4     │
│ 2   │ 4     │ 1     │ 8     │
```

"""
function groupby(df::AbstractDataFrame, cols;
                 sort::Bool=false, skipmissing::Bool=false)
    _check_consistency(df)
    idxcols = index(df)[cols]
    intcols = idxcols isa Int ? [idxcols] : convert(Vector{Int}, idxcols)
    if isempty(intcols)
        return GroupedDataFrame(df, intcols, ones(Int, nrow(df)),
                                collect(axes(df, 1)), [1], [nrow(df)])
    end
    sdf = df[!, intcols]
    df_groups = group_rows(sdf, false, sort, skipmissing)
    GroupedDataFrame(df, intcols, df_groups.groups, df_groups.rperm,
                     df_groups.starts, df_groups.stops)
end

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

function Base.getindex(gd::GroupedDataFrame, idxs::AbstractArray)
    new_starts = gd.starts[idxs]
    new_ends = gd.ends[idxs]
    if !allunique(new_starts)
        throw(ArgumentError("duplicates in idxs argument are not allowed"))
    end
    new_groups = zeros(Int, length(gd.groups))
    for idx in eachindex(new_starts)
        @inbounds for j in new_starts[idx]:new_ends[idx]
            new_groups[gd.idx[j]] = idx
        end
    end
    GroupedDataFrame(gd.parent, gd.cols, new_groups, gd.idx, new_starts, new_ends)
end

Base.getindex(gd::GroupedDataFrame, idxs::Colon) =
    GroupedDataFrame(gd.parent, gd.cols, gd.groups, gd.idx, gd.starts, gd.ends)

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

"""
    map(cols => f, gd::GroupedDataFrame)
    map(f, gd::GroupedDataFrame)

Apply a function to each group of rows and return a [`GroupedDataFrame`](@ref).

If the first argument is a `cols => f` pair, `cols` must be a column name or index, or
a vector or tuple thereof, and `f` must be a callable. If `cols` is a single column index,
`f` is called with a `SubArray` view into that column for each group; else, `f` is called
with a named tuple holding `SubArray` views into these columns.

If the first argument is a vector, tuple or named tuple of such pairs, each pair is
handled as described above. If a named tuple, field names are used to name
each generated column.

If the first argument is a callable `f`, it is passed a [`SubDataFrame`](@ref) view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If the returned data frame contains columns with the same names as the grouping columns,
they are required to be equal.
Note that this second form is much slower than the first one due to type instability.

`f` can return a single value, a row or multiple rows. The type of the returned value
determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple of single values or a [`DataFrameRow`](@ref) gives a data frame with one column
  for each field and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame, a named tuple of vectors or a matrix gives a data frame
  with the same columns and as many rows for each group as the rows returned for that group.

`f` must always return the same kind of object (as defined in the above list) for
all groups, and if a named tuple or data frame, with the same fields or columns.
Named tuples cannot mix single values and vectors.
Due to type instability, returning a single value or a named tuple is dramatically
faster than returning a data frame.

As a special case, if a tuple or vector of pairs is passed as the first argument, each function
is required to return a single value or vector, which will produce each a separate column.

In all cases, the resulting `GroupedDataFrame` contains all the grouping columns in addition
to those generated by the application of `f`.
Column names are automatically generated when necessary: for functions
operating on a single column and returning a single value or vector, the function name is
appended to the input column name; for other functions, columns are called `x1`, `x2`
and so on.

Optimized methods are used when standard summary functions (`sum`, `prod`,
`minimum`, `maximum`, `mean`, `var`, `std`, `first`, `last` and `length`)
are specified using the pair syntax (e.g. `col => sum`).
When computing the `sum` or `mean` over floating point columns, results will be less
accurate than the standard [`sum`](@ref) function (which uses pairwise summation). Use
`col => x -> sum(x)` to avoid the optimized method and use the slower, more accurate one.

### Examples

```jldoctest
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

See [`by`](@ref) for more examples.

### See also

`combine(f, gd)` returns a `DataFrame` rather than a `GroupedDataFrame`

"""
function Base.map(f::Any, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(f, gd)
        keys = _names(gd.parent)[gd.cols]
        for key in keys
            if hasproperty(valscat, key) &&
               !isequal(valscat[!, key], view(gd.parent[!, key], idx))
               throw(ArgumentError("column :$key in returned data frame " *
                                   "is not equal to grouping key :$key"))
            end
        end
        parent = hcat!(gd.parent[idx, gd.cols],
                       without(valscat, intersect(keys, _names(valscat))))
        if length(idx) == 0
            return GroupedDataFrame(parent, collect(1:length(gd.cols)), idx,
                                    Int[], Int[], Int[])
        end
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
        return GroupedDataFrame(parent, collect(1:length(gd.cols)), idx,
                                collect(1:length(idx)), starts, ends)
    else
        return GroupedDataFrame(gd.parent[1:0, gd.cols], collect(1:length(gd.cols)),
                                Int[], Int[], Int[], Int[])
    end
end

"""
    combine(gd::GroupedDataFrame, cols => f...)
    combine(gd::GroupedDataFrame; (colname = cols => f)...)
    combine(gd::GroupedDataFrame, f)
    combine(f, gd::GroupedDataFrame)

Transform a [`GroupedDataFrame`](@ref) into a `DataFrame`.

If the last argument(s) consist(s) in one or more `cols => f` pair(s), or if
`colname = cols => f` keyword arguments are provided, `cols` must be
a column name or index, or a vector or tuple thereof, and `f` must be a callable.
A pair or a (named) tuple of pairs can also be provided as the first or last argument.
If `cols` is a single column index, `f` is called with a `SubArray` view into that
column for each group; else, `f` is called with a named tuple holding `SubArray`
views into these columns.

If the last argument is a callable `f`, it is passed a [`SubDataFrame`](@ref) view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If the returned data frame contains columns with the same names as the grouping columns,
they are required to be equal.
Note that this second form is much slower than the first one due to type instability.
A method is defined with `f` as the first argument, so do-block
notation can be used.

`f` can return a single value, a row or multiple rows. The type of the returned value
determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple of single values or a [`DataFrameRow`](@ref) gives a data frame with one column
  for each field and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame, a named tuple of vectors or a matrix gives a data frame
  with the same columns and as many rows for each group as the rows returned for that group.

`f` must always return the same kind of object (as defined in the above list) for
all groups, and if a named tuple or data frame, with the same fields or columns.
Named tuples cannot mix single values and vectors.
Due to type instability, returning a single value or a named tuple is dramatically
faster than returning a data frame.

As a special case, if a tuple or vector of pairs is passed as the first argument, each function
is required to return a single value or vector, which will produce each a separate column.

In all cases, the resulting data frame contains all the grouping columns in addition
to those generated by the application of `f`.
Column names are automatically generated when necessary: for functions
operating on a single column and returning a single value or vector, the function name is
appended to the input column name; for other functions, columns are called `x1`, `x2`
and so on. The resulting data frame will be sorted if `sort=true` was passed to the
[`groupby`](@ref) call from which `gd` was constructed. Otherwise, ordering of rows
is undefined.

Optimized methods are used when standard summary functions (`sum`, `prod`,
`minimum`, `maximum`, `mean`, `var`, `std`, `first`, `last` and `length`)
are specified using the pair syntax (e.g. `col => sum`).
When computing the `sum` or `mean` over floating point columns, results will be less
accurate than the standard [`sum`](@ref) function (which uses pairwise summation). Use
`col => x -> sum(x)` to avoid the optimized method and use the slower, more accurate one.

### Examples

```jldoctest
julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> gd = groupby(df, :a);

julia> combine(gd, :c => sum)
4×2 DataFrame
│ Row │ a     │ c_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 8     │
│ 3   │ 3     │ 10    │
│ 4   │ 4     │ 12    │

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

See [`by`](@ref) for more examples.

### See also

[`by(f, df, cols)`](@ref) is a shorthand for `combine(f, groupby(df, cols))`.

[`map`](@ref): `combine(f, groupby(df, cols))` is a more efficient equivalent
of `combine(map(f, groupby(df, cols)))`.

"""
function combine(f::Any, gd::GroupedDataFrame)
    if length(gd) > 0
        idx, valscat = _combine(f, gd)
        keys = _names(gd.parent)[gd.cols]
        for key in keys
            if hasproperty(valscat, key) &&
               !isequal(valscat[!, key], view(gd.parent[!, key], idx))
               throw(ArgumentError("column :$key in returned data frame " *
                                   "is not equal to grouping key :$key"))
            end
        end
        return hcat!(gd.parent[idx, gd.cols],
                     without(valscat, intersect(keys, _names(valscat))))
    else
        return gd.parent[1:0, gd.cols]
    end
end
combine(gd::GroupedDataFrame, f::Any) = combine(f, gd)
combine(gd::GroupedDataFrame, f::Pair...) = combine(f, gd)
combine(gd::GroupedDataFrame, f::Pair) = combine(f, gd)

function combine(gd::GroupedDataFrame; f...)
    if length(f) == 0
        Base.depwarn("combine(gd) is deprecated, use DataFrame(gd) instead", :combine)
        combine(identity, gd)
    else
        combine(values(f), gd)
    end
end

# Wrapping automatically adds column names when the value returned
# by the user-provided function lacks them
wrap(x::Union{AbstractDataFrame, NamedTuple, DataFrameRow}) = x
wrap(x::AbstractMatrix) =
    NamedTuple{Tuple(gennames(size(x, 2)))}(Tuple(view(x, :, i) for i in 1:size(x, 2)))
wrap(x::Any) = (x1=x,)

function do_call(f::Any, gd::GroupedDataFrame, incols::AbstractVector, i::Integer)
    idx = gd.idx[gd.starts[i]:gd.ends[i]]
    f(view(incols, idx))
end
function do_call(f::Any, gd::GroupedDataFrame, incols::NamedTuple, i::Integer)
    idx = gd.idx[gd.starts[i]:gd.ends[i]]
    f(map(c -> view(c, idx), incols))
end
do_call(f::Any, gd::GroupedDataFrame, incols::Nothing, i::Integer) =
    f(gd[i])

_nrow(df::AbstractDataFrame) = nrow(df)
_nrow(x::NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}) =
    isempty(x) ? 0 : length(x[1])
_ncol(df::AbstractDataFrame) = ncol(df)
_ncol(x::Union{NamedTuple, DataFrameRow}) = length(x)

abstract type AbstractAggregate end

struct Reduce{O, C, A} <: AbstractAggregate
    op::O
    condf::C
    adjust::A
end
Reduce(f, condf=nothing) = Reduce(f, condf, nothing)

check_aggregate(f::Any) = f
check_aggregate(::typeof(sum)) = Reduce(Base.add_sum)
check_aggregate(::typeof(prod)) = Reduce(Base.mul_prod)
check_aggregate(::typeof(maximum)) = Reduce(max)
check_aggregate(::typeof(minimum)) = Reduce(min)
check_aggregate(::typeof(mean)) = Reduce(Base.add_sum, nothing, /)
check_aggregate(::typeof(sum∘skipmissing)) = Reduce(Base.add_sum, !ismissing)
check_aggregate(::typeof(prod∘skipmissing)) = Reduce(Base.mul_prod, !ismissing)
check_aggregate(::typeof(maximum∘skipmissing)) = Reduce(max, !ismissing)
check_aggregate(::typeof(minimum∘skipmissing)) = Reduce(min, !ismissing)
check_aggregate(::typeof(mean∘skipmissing)) = Reduce(Base.add_sum, !ismissing, /)

# Other aggregate functions which are not strictly reductions
struct Aggregate{F, C} <: AbstractAggregate
    f::F
    condf::C
end
Aggregate(f) = Aggregate(f, nothing)

check_aggregate(::typeof(var)) = Aggregate(var)
check_aggregate(::typeof(var∘skipmissing)) = Aggregate(var, !ismissing)
check_aggregate(::typeof(std)) = Aggregate(std)
check_aggregate(::typeof(std∘skipmissing)) = Aggregate(std, !ismissing)
check_aggregate(::typeof(first)) = Aggregate(first)
check_aggregate(::typeof(first∘skipmissing)) = Aggregate(first, !ismissing)
check_aggregate(::typeof(last)) = Aggregate(last)
check_aggregate(::typeof(last∘skipmissing)) = Aggregate(last, !ismissing)
check_aggregate(::typeof(length)) = Aggregate(length)
# SkipMissing does not support length

# Find first value matching condition for each group
# Optimized for situations where a matching value is typically encountered
# among the first rows for each group
function fillfirst!(condf, outcol::AbstractVector, incol::AbstractVector,
                    gd::GroupedDataFrame; rev::Bool=false)
    nfilled = 0
    @inbounds for i in eachindex(outcol)
        s = gd.starts[i]
        offsets = rev ? (nrow(gd[i])-1:-1:0) : (0:nrow(gd[i])-1)
        for j in offsets
            x = incol[gd.idx[s+j]]
            if !condf === nothing || condf(x)
                outcol[i] = x
                nfilled += 1
                break
            end
        end
    end
    if nfilled < length(outcol)
        throw(ArgumentError("some groups contain only missing values"))
    end
    outcol
end

# Use reducedim_init to get a vector of the right type,
# but trick it into using the expected length
groupreduce_init(op, condf, incol, gd) =
    Base.reducedim_init(identity, op, view(incol, 1:length(gd)), 2)
for (op, initf) in ((:max, :typemin), (:min, :typemax))
    @eval begin
        function groupreduce_init(::typeof($op), condf, incol::AbstractVector{T}, gd) where T
            # !ismissing check is purely an optimization to avoid a copy later
            outcol = similar(incol, condf === !ismissing ? nonmissingtype(T) : T, length(gd))
            # Comparison is possible only between CatValues from the same pool
            if incol isa CategoricalVector
                U = Union{CategoricalArrays.leveltype(outcol),
                          eltype(outcol) >: Missing ? Missing : Union{}}
                outcol = CategoricalArray{U, 1}(outcol.refs, incol.pool)
            end
            # It is safe to use a non-missing init value
            # since missing will poison the result if present
            S = nonmissingtype(T)
            if isconcretetype(S) && hasmethod($initf, Tuple{S})
                fill!(outcol, $initf(S))
            elseif condf !== nothing
                fillfirst!(condf, outcol, incol, gd)
            else
                @inbounds for i in eachindex(outcol)
                    outcol[i] = incol[gd.idx[gd.starts[i]]]
                end
            end
            return outcol
        end
    end
end

function copyto_widen!(res::AbstractVector{T}, x::AbstractVector) where T
    @inbounds for i in eachindex(res, x)
        val = x[i]
        S = typeof(val)
        if S <: T || promote_type(S, T) <: T
            res[i] = val
        else
            newres = Tables.allocatecolumn(promote_type(S, T), length(x))
            return copyto_widen!(newres, x)
        end
    end
    return res
end

function groupreduce!(res, f, op, condf, adjust,
                      incol::AbstractVector{T}, gd::GroupedDataFrame) where T
    n = length(gd)
    if adjust !== nothing
        counts = zeros(Int, n)
    end
    @inbounds for i in eachindex(incol, gd.groups)
        gix = gd.groups[i]
        x = incol[i]
        if gix > 0 && (condf === nothing || condf(x))
            res[gix] = op(res[gix], f(x, gix))
            adjust !== nothing && (counts[gix] += 1)
        end
    end
    outcol = adjust === nothing ? res : map(adjust, res, counts)
    # Undo pool sharing done by groupreduce_init
    if outcol isa CategoricalVector
        U = Union{CategoricalArrays.leveltype(outcol),
                  eltype(outcol) >: Missing ? Missing : Union{}}
        outcol = CategoricalArray{U, 1}(outcol.refs, incol.pool)
    end
    if isconcretetype(eltype(outcol))
        return outcol
    else
        copyto_widen!(Tables.allocatecolumn(typeof(first(outcol)), n), outcol)
    end
end

# Function barrier works around type instability of _groupreduce_init due to applicable
groupreduce(f, op, condf, adjust, incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce!(groupreduce_init(op, condf, incol, gd),
                 f, op, condf, adjust, incol, gd)
# Avoids the overhead due to Missing when computing reduction
groupreduce(f, op, condf::typeof(!ismissing), adjust,
            incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce!(disallowmissing(groupreduce_init(op, condf, incol, gd)),
                 f, op, condf, adjust, incol, gd)

(r::Reduce)(incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce((x, i) -> x, r.op, r.condf, r.adjust, incol, gd)

function (agg::Aggregate{typeof(var)})(incol::AbstractVector, gd::GroupedDataFrame)
    means = groupreduce((x, i) -> x, Base.add_sum, agg.condf, /, incol, gd)
    # !ismissing check is purely an optimization to avoid a copy later
    if eltype(means) >: Missing && agg.condf !== !ismissing
        T = Union{Missing, real(eltype(means))}
    else
        T = real(eltype(means))
    end
    res = zeros(T, length(gd))
    groupreduce!(res, (x, i) -> @inbounds(abs2(x - means[i])), +,
                 agg.condf, (x, l) -> x / (l-1), incol, gd)
end

function (agg::Aggregate{typeof(std)})(incol::AbstractVector, gd::GroupedDataFrame)
    outcol = Aggregate(var, agg.condf)(incol, gd)
    map!(sqrt, outcol, outcol)
end

for f in (first, last)
    function (agg::Aggregate{typeof(f)})(incol::AbstractVector, gd::GroupedDataFrame)
        n = length(gd)
        outcol = similar(incol, n)
        if agg.condf === !ismissing
            fillfirst!(agg.condf, outcol, incol, gd, rev=agg.f === last)
        else
            v = agg.f === first ? gd.starts : gd.ends
            map!(i -> incol[gd.idx[v[i]]], outcol, 1:n)
        end
        if isconcretetype(eltype(outcol))
            return outcol
        else
            return copyto_widen!(Tables.allocatecolumn(typeof(first(outcol)), n), outcol)
        end
    end
end

(agg::Aggregate{typeof(length)})(incol::AbstractVector, gd::GroupedDataFrame) =
    gd.ends .- gd.starts .+ 1

function do_f(f, x...)
    @inline function fun(x...)
        res = f(x...)
        if res isa Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}
            throw(ArgumentError("a single value or vector result is required when passing " *
                                "a vector or tuple of functions (got $(typeof(res)))"))
        end
        res
    end
end

function _combine(f::Union{AbstractVector{<:Pair}, Tuple{Vararg{Pair}},
                           NamedTuple{<:Any, <:Tuple{Vararg{Pair}}}},
                  gd::GroupedDataFrame)
    res = map(f) do p
        agg = check_aggregate(last(p))
        if agg isa AbstractAggregate && p isa Pair{<:ColumnIndex}
            incol = gd.parent[!, first(p)]
            idx = gd.idx[gd.starts]
            outcol = agg(incol, gd)
            return idx, outcol
        else
            fun = do_f(last(p))
            if p isa Pair{<:ColumnIndex}
                incols = gd.parent[!, first(p)]
            else
                df = gd.parent[!, collect(first(p))]
                incols = NamedTuple{Tuple(names(df))}(eachcol(df))
            end
            firstres = do_call(fun, gd, incols, 1)
            idx, outcols, _ = _combine_with_first(wrap(firstres), fun, gd, incols)
            return idx, outcols[1]
        end
    end
    # TODO: avoid recomputing idx for each pair
    idx = res[1][1]
    outcols = map(x -> x[2], res)
    if !all(x -> length(x) == length(outcols[1]), outcols)
        throw(ArgumentError("all functions must return values of the same length"))
    end
    if f isa NamedTuple
        nams = collect(Symbol, propertynames(f))
    else
        nams = [f[i] isa Pair{<:ColumnIndex} ?
                    Symbol(names(gd.parent)[index(gd.parent)[first(f[i])]],
                           '_', funname(last(f[i]))) :
                    Symbol('x', i)
                for i in 1:length(f)]
    end
    valscat = DataFrame(collect(AbstractVector, outcols), nams, makeunique=true)
    return idx, valscat
end

function _combine(f::Any, gd::GroupedDataFrame)
    if f isa Pair{<:ColumnIndex}
        incols = gd.parent[!, first(f)]
        fun = last(f)
    elseif f isa Pair
        df = gd.parent[! , collect(first(f))]
        incols = NamedTuple{Tuple(names(df))}(eachcol(df))
        fun = last(f)
    else
        incols = nothing
        fun = f
    end
    agg = check_aggregate(fun)
    if agg isa AbstractAggregate && f isa Pair{<:ColumnIndex}
        idx = gd.idx[gd.starts]
        outcols = (agg(incols, gd),)
        # nms is set below
    else
        firstres = do_call(fun, gd, incols, 1)
        idx, outcols, nms = _combine_with_first(wrap(firstres), fun, gd, incols)
    end
    if f isa Pair{<:ColumnIndex} &&
        (agg isa AbstractAggregate ||
         !isa(firstres, Union{AbstractDataFrame, NamedTuple, DataFrameRow, AbstractMatrix}))
         nms = [Symbol(names(gd.parent)[index(gd.parent)[first(f)]], '_', funname(fun))]
    end
    valscat = DataFrame(collect(AbstractVector, outcols), collect(Symbol, nms))
    return idx, valscat
end

function _combine_with_first(first::Union{NamedTuple, DataFrameRow, AbstractDataFrame},
                             f::Any, gd::GroupedDataFrame,
                             incols::Union{Nothing, AbstractVector, NamedTuple})
    if first isa AbstractDataFrame
        n = 0
        eltys = eltype.(eachcol(first))
    elseif first isa NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}
        n = 0
        eltys = map(eltype, first)
    elseif first isa DataFrameRow
        n = length(gd)
        eltys = eltype.(eachcol(parent(first)))
    else # NamedTuple giving a single row
        n = length(gd)
        eltys = map(typeof, first)
        if any(x -> x <: AbstractVector, eltys)
            throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
        end
    end
    idx = Vector{Int}(undef, n)
    local initialcols
    let eltys=eltys, n=n # Workaround for julia#15276
        initialcols = ntuple(i -> Tables.allocatecolumn(eltys[i], n), _ncol(first))
    end
    outcols = _combine_with_first!(first, initialcols, idx, 1, 1, f, gd, incols,
                                   tuple(propertynames(first)...))
    idx, outcols, propertynames(first)
end

function fill_row!(row, outcols::NTuple{N, AbstractVector},
                   i::Integer, colstart::Integer,
                   colnames::NTuple{N, Symbol}) where N
    if !isa(row, Union{NamedTuple, DataFrameRow})
        throw(ArgumentError("return value must not change its kind " *
                            "(single row or variable number of rows) across groups"))
    elseif row isa NamedTuple && any(x -> x isa AbstractVector, row)
        throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
    elseif _ncol(row) != N
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

function _combine_with_first!(first::Union{NamedTuple, DataFrameRow},
                              outcols::NTuple{N, AbstractVector},
                              idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                              f::Any, gd::GroupedDataFrame,
                              incols::Union{Nothing, AbstractVector, NamedTuple},
                              colnames::NTuple{N, Symbol}) where N
    len = length(gd)
    # Handle first group
    j = fill_row!(first, outcols, rowstart, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    idx[rowstart] = gd.idx[gd.starts[rowstart]]
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        row = wrap(do_call(f, gd, incols, i))
        j = fill_row!(row, outcols, i, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, outcols=outcols, row=row # Workaround for julia#15276
                newcols = ntuple(length(outcols)) do k
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
            return _combine_with_first!(row, newcols, idx, i, j, f, gd, incols, colnames)
        end
        idx[i] = gd.idx[gd.starts[i]]
    end
    outcols
end

# This needs to be in a separate function
# to work around a crash due to JuliaLang/julia#29430
if VERSION >= v"1.1.0-DEV.723"
    @inline function do_append!(do_it, col, vals)
        do_it && append!(col, vals)
        return do_it
    end
else
    @noinline function do_append!(do_it, col, vals)
        do_it && append!(col, vals)
        return do_it
    end
end

function append_rows!(rows, outcols::NTuple{N, AbstractVector},
                      colstart::Integer, colnames::NTuple{N, Symbol}) where N
    if !isa(rows, Union{AbstractDataFrame, NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}})
        throw(ArgumentError("return value must not change its kind " *
                            "(single row or variable number of rows) across groups"))
    elseif _ncol(rows) != N
        throw(ArgumentError("return value must have the same number of columns " *
                            "for all groups (got $N and $(_ncol(rows)))"))
    end
    @inbounds for j in colstart:length(outcols)
        col = outcols[j]
        cn = colnames[j]
        local vals
        try
            vals = getproperty(rows, cn)
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

function _combine_with_first!(first::Union{AbstractDataFrame,
                                           NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}},
                              outcols::NTuple{N, AbstractVector},
                              idx::Vector{Int}, rowstart::Integer, colstart::Integer,
                              f::Any, gd::GroupedDataFrame,
                              incols::Union{Nothing, AbstractVector, NamedTuple},
                              colnames::NTuple{N, Symbol}) where N
    len = length(gd)
    # Handle first group
    j = append_rows!(first, outcols, colstart, colnames)
    @assert j === nothing # eltype is guaranteed to match
    append!(idx, Iterators.repeated(gd.idx[gd.starts[rowstart]], _nrow(first)))
    # Handle remaining groups
    @inbounds for i in rowstart+1:len
        rows = wrap(do_call(f, gd, incols, i))
        j = append_rows!(rows, outcols, 1, colnames)
        if j !== nothing # Need to widen column type
            local newcols
            let i = i, j = j, outcols=outcols, rows=rows # Workaround for julia#15276
                newcols = ntuple(length(outcols)) do k
                    S = eltype(rows isa AbstractDataFrame ? rows[!, k] : rows[k])
                    T = eltype(outcols[k])
                    U = promote_type(S, T)
                    if S <: T || U <: T
                        outcols[k]
                    else
                        copyto!(Tables.allocatecolumn(U, length(outcols[k])), outcols[k])
                    end
                end
            end
            return _combine_with_first!(rows, newcols, idx, i, j, f, gd, incols, colnames)
        end
        append!(idx, Iterators.repeated(gd.idx[gd.starts[i]], _nrow(rows)))
    end
    outcols
end

"""
    by(df::AbstractDataFrame, keys, cols=>f...;
       sort::Bool=false, skipmissing::Bool=false)
    by(df::AbstractDataFrame, keys; (colname = cols => f)...,
       sort::Bool=false, skipmissing::Bool=false)
    by(df::AbstractDataFrame, keys, f;
       sort::Bool=false, skipmissing::Bool=false)
    by(f, df::AbstractDataFrame, keys;
       sort::Bool=false, skipmissing::Bool=false)

Split-apply-combine in one step: apply `f` to each grouping in `df`
based on grouping columns `keys`, and return a `DataFrame`.

`keys` can be either a single column index, or a vector thereof.

If the last argument(s) consist(s) in one or more `cols => f` pair(s), or if
`colname = cols => f` keyword arguments are provided, `cols` must be
a column name or index, or a vector or tuple thereof, and `f` must be a callable.
A pair or a (named) tuple of pairs can also be provided as the first or last argument.
If `cols` is a single column index, `f` is called with a `SubArray` view into that
column for each group; else, `f` is called with a named tuple holding `SubArray`
views into these columns.

If the last argument is a callable `f`, it is passed a [`SubDataFrame`](@ref) view for each group,
and the returned `DataFrame` then consists of the returned rows plus the grouping columns.
If the returned data frame contains columns with the same names as the grouping columns,
they are required to be equal.
Note that this second form is much slower than the first one due to type instability.
A method is defined with `f` as the first argument, so do-block
notation can be used.

`f` can return a single value, a row or multiple rows. The type of the returned value
determines the shape of the resulting data frame:
- A single value gives a data frame with a single column and one row per group.
- A named tuple of single values or a [`DataFrameRow`](@ref) gives a data frame with one column
  for each field and one row per group.
- A vector gives a data frame with a single column and as many rows
  for each group as the length of the returned vector for that group.
- A data frame, a named tuple of vectors or a matrix gives a data frame
  with the same columns and as many rows for each group as the rows returned for that group.

`f` must always return the same kind of object (as defined in the above list) for
all groups, and if a named tuple or data frame, with the same fields or columns.
Named tuples cannot mix single values and vectors.
Due to type instability, returning a single value or a named tuple is dramatically
faster than returning a data frame.

As a special case, if multiple pairs are passed as last arguments, each function
is required to return a single value or vector, which will produce each a separate column.

In all cases, the resulting data frame contains all the grouping columns in addition
to those generated by the application of `f`.
Column names are automatically generated when necessary: for functions
operating on a single column and returning a single value or vector, the function name is
appended to the input colummn name; for other functions, columns are called `x1`, `x2`
and so on. The resulting data frame will be sorted on `keys` if `sort=true`.
Otherwise, ordering of rows is undefined.
If `skipmissing=true` then the resulting data frame will not contain groups
with `missing` values in one of the `keys` columns.

Optimized methods are used when standard summary functions (`sum`, `prod`,
`minimum`, `maximum`, `mean`, `var`, `std`, `first`, `last` and `length)
are specified using the pair syntax (e.g. `col => sum`).
When computing the `sum` or `mean` over floating point columns, results will be less
accurate than the standard [`sum`](@ref) function (which uses pairwise summation). Use
`col => x -> sum(x)` to avoid the optimized method and use the slower, more accurate one.

`by(d, cols, f)` is equivalent to `combine(f, groupby(d, cols))` and to the
less efficient `combine(map(f, groupby(d, cols)))`.

### Examples

```jldoctest
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

julia> by(df, :a, :c => x -> 2 .* x)
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

julia> by(df, :a, c_sum = :c => sum, c_sum2 = :c => x -> sum(x.^2))
4×3 DataFrame
│ Row │ a     │ c_sum │ c_sum2 │
│     │ Int64 │ Int64 │ Int64  │
├─────┼───────┼───────┼────────┤
│ 1   │ 1     │ 6     │ 26     │
│ 2   │ 2     │ 8     │ 40     │
│ 3   │ 3     │ 10    │ 58     │
│ 4   │ 4     │ 12    │ 80     │

julia> by(df, :a, (:b, :c) => x -> (minb = minimum(x.b), sumc = sum(x.c)))
4×3 DataFrame
│ Row │ a     │ minb  │ sumc  │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 6     │
│ 2   │ 2     │ 1     │ 8     │
│ 3   │ 3     │ 2     │ 10    │
│ 4   │ 4     │ 1     │ 12    │
```

"""
by(d::AbstractDataFrame, cols::Any, f::Any;
   sort::Bool=false, skipmissing::Bool=false) =
    combine(f, groupby(d, cols, sort=sort, skipmissing=skipmissing))
by(f::Any, d::AbstractDataFrame, cols::Any;
   sort::Bool=false, skipmissing::Bool=false) =
    by(d, cols, f, sort=sort, skipmissing=skipmissing)
by(d::AbstractDataFrame, cols::Any, f::Pair;
   sort::Bool=false, skipmissing::Bool=false) =
    combine(f, groupby(d, cols, sort=sort, skipmissing=skipmissing))
by(d::AbstractDataFrame, cols::Any, f::Pair...;
   sort::Bool=false, skipmissing::Bool=false) =
    combine(f, groupby(d, cols, sort=sort, skipmissing=skipmissing))
by(d::AbstractDataFrame, cols::Any;
   sort::Bool=false, skipmissing::Bool=false, f...) =
    combine(values(f), groupby(d, cols, sort=sort, skipmissing=skipmissing))

#
# Aggregate convenience functions
#

# Applies a set of functions over a DataFrame, in the from of a cross-product
"""
Split-apply-combine that applies a set of functions over columns of an
`AbstractDataFrame` or [`GroupedDataFrame`](@ref)

```julia
aggregate(df::AbstractDataFrame, fs)
aggregate(df::AbstractDataFrame, cols, fs; sort=false, skipmissing=false)
aggregate(gd::GroupedDataFrame, fs; sort=false)
```

### Arguments

* `df` : an `AbstractDataFrame`
* `gd` : a `GroupedDataFrame`
* `cols` : a column indicator (`Symbol`, `Int`, `Vector{Symbol}`, etc.)
* `fs` : a function or vector of functions to be applied to vectors
  within groups; expects each argument to be a column vector
* `sort` : whether to sort rows according to the values of the grouping columns
* `skipmissing` : whether to skip rows with `missing` values in one of the grouping columns `cols`

Each `fs` should return a value or vector. All returns must be the
same length.

### Returns

* `::DataFrame`

### Examples

```jldoctest
julia> using Statistics

julia> df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                      b = repeat([2, 1], outer=[4]),
                      c = 1:8);

julia> aggregate(df, :a, sum)
4×3 DataFrame
│ Row │ a     │ b_sum │ c_sum │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 4     │ 6     │
│ 2   │ 2     │ 2     │ 8     │
│ 3   │ 3     │ 4     │ 10    │
│ 4   │ 4     │ 2     │ 12    │

julia> aggregate(df, :a, [sum, x->mean(skipmissing(x))])
4×5 DataFrame
│ Row │ a     │ b_sum │ c_sum │ b_function │ c_function │
│     │ Int64 │ Int64 │ Int64 │ Float64    │ Float64    │
├─────┼───────┼───────┼───────┼────────────┼────────────┤
│ 1   │ 1     │ 4     │ 6     │ 2.0        │ 3.0        │
│ 2   │ 2     │ 2     │ 8     │ 1.0        │ 4.0        │
│ 3   │ 3     │ 4     │ 10    │ 2.0        │ 5.0        │
│ 4   │ 4     │ 2     │ 12    │ 1.0        │ 6.0        │

julia> aggregate(groupby(df, :a), [sum, x->mean(skipmissing(x))])
4×5 DataFrame
│ Row │ a     │ b_sum │ c_sum │ b_function │ c_function │
│     │ Int64 │ Int64 │ Int64 │ Float64    │ Float64    │
├─────┼───────┼───────┼───────┼────────────┼────────────┤
│ 1   │ 1     │ 4     │ 6     │ 2.0        │ 3.0        │
│ 2   │ 2     │ 2     │ 8     │ 1.0        │ 4.0        │
│ 3   │ 3     │ 4     │ 10    │ 2.0        │ 5.0        │
│ 4   │ 4     │ 2     │ 12    │ 1.0        │ 6.0        │
```
"""
aggregate(d::AbstractDataFrame, fs::Any; sort::Bool=false) =
    aggregate(d, [fs], sort=sort)
function aggregate(d::AbstractDataFrame, fs::AbstractVector; sort::Bool=false)
    headers = _makeheaders(fs, _names(d))
    _aggregate(d, fs, headers, sort)
end

# Applies aggregate to non-key cols of each SubDataFrame of a GroupedDataFrame
aggregate(gd::GroupedDataFrame, f::Any; sort::Bool=false) = aggregate(gd, [f], sort=sort)
function aggregate(gd::GroupedDataFrame, fs::AbstractVector; sort::Bool=false)
    headers = _makeheaders(fs, setdiff(_names(gd), _names(gd.parent)[gd.cols]))
    res = combine(x -> _aggregate(without(x, gd.cols), fs, headers), gd)
    sort && sort!(res, headers)
    res
end

# Groups DataFrame by cols before applying aggregate
function aggregate(d::AbstractDataFrame, cols, fs::Any;
                   sort::Bool=false, skipmissing::Bool=false)
    aggregate(groupby(d, cols, sort=sort, skipmissing=skipmissing), fs)
end

function funname(f)
    n = nameof(f)
    String(n)[1] == '#' ? :function : n
end

_makeheaders(fs::AbstractVector, cn::AbstractVector{Symbol}) =
    [Symbol(colname, '_', funname(f)) for f in fs for colname in cn]

function _aggregate(d::AbstractDataFrame, fs::AbstractVector,
                    headers::AbstractVector{Symbol}, sort::Bool=false)
    res = DataFrame(AbstractVector[vcat(f(d[!, i])) for f in fs for i in 1:size(d, 2)], headers, makeunique=true)
    sort && sort!(res, headers)
    res
end

function DataFrame(gd::GroupedDataFrame; copycols::Bool=true)
    if !copycols
        throw(ArgumentError("It is not possible to construct a `DataFrame`" *
                            "from GroupedDataFrame with `copycols=false`"))
    end
    length(gd) == 0 && return similar(parent(gd), 0)
    idx = similar(gd.idx)
    doff = 1
    for (s,e) in zip(gd.starts, gd.ends)
        n = e - s + 1
        copyto!(idx, doff, gd.idx, s, n)
        doff += n
    end
    resize!(idx, doff - 1)
    parent(gd)[idx, :]
end

"""
    groupindices(gd::GroupedDataFrame)

Return a vector of group indices for each row of `parent(gd)`.

Rows appearing in group `gd[i]` are attributed index `i`. Rows not present in
any group are attributed `missing` (this can happen if `skipmissing=true` was
passed when creating `gd`, or if `gd` is a subset from a larger [`GroupedDataFrame`](@ref)).
"""
groupindices(gd::GroupedDataFrame) = replace(gd.groups, 0=>missing)

"""
    groupvars(gd::GroupedDataFrame)

Return a vector of column names in `parent(gd)` used for grouping.
"""
groupvars(gd::GroupedDataFrame) = _names(gd)[gd.cols]
