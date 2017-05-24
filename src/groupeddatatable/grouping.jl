#
#  Split - Apply - Combine operations
#

##############################################################################
##
## GroupedDataTable...
##
##############################################################################

"""
The result of a `groupby` operation on an AbstractDataTable; a
view into the AbstractDataTable grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
type GroupedDataTable
    parent::AbstractDataTable
    cols::Vector         # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups
end

#
# Split
#

"""
A view of an AbstractDataTable split into row groups

```julia
groupby(d::AbstractDataTable, cols)
groupby(cols)
```

### Arguments

* `d` : an AbstractDataTable to split (optional, see [Returns](#returns))
* `cols` : data table columns to group by

### Returns

* `::GroupedDataTable` : a grouped view into `d`
* `::Function`: a function `x -> groupby(x, cols)` (if `d` is not specified)

### Details

An iterator over a `GroupedDataTable` returns a `SubDataTable` view
for each grouping into `d`. A `GroupedDataTable` also supports
indexing by groups and `map`.

See the following for additional split-apply-combine operations:

* `by` : split-apply-combine using functions
* `aggregate` : split-apply-combine; applies functions in the form of a cross product
* `combine` : combine (obviously)
* `colwise` : apply a function to each column in an AbstractDataTable or GroupedDataTable

Piping methods `|>` are also provided.

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
gd = groupby(dt, :a)
gd[1]
last(gd)
vcat([g[:b] for g in gd]...)
for g in gd
    println(g)
end
map(d -> mean(dropnull(d[:c])), gd)   # returns a GroupApplied object
combine(map(d -> mean(dropnull(d[:c])), gd))
dt |> groupby(:a) |> [sum, length]
dt |> groupby([:a, :b]) |> [sum, length]
```

"""
function groupby{T}(dt::AbstractDataTable, cols::Vector{T}; sort::Bool = false)
    sdt = dt[cols]
    dt_groups = group_rows(sdt)
    # sort the groups
    if sort
        group_perm = sortperm(view(sdt, dt_groups.rperm[dt_groups.starts]))
        permute!(dt_groups.starts, group_perm)
        Base.permute!!(dt_groups.stops, group_perm)
    end
    GroupedDataTable(dt, cols, dt_groups.rperm,
                     dt_groups.starts, dt_groups.stops)
end
groupby(d::AbstractDataTable, cols; sort::Bool = false) = groupby(d, [cols], sort = sort)

# add a function curry
groupby{T}(cols::Vector{T}; sort::Bool = false) = x -> groupby(x, cols, sort = sort)
groupby(cols; sort::Bool = false) = x -> groupby(x, cols, sort = sort)

Base.start(gd::GroupedDataTable) = 1
Base.next(gd::GroupedDataTable, state::Int) =
    (view(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
Base.done(gd::GroupedDataTable, state::Int) = state > length(gd.starts)
Base.length(gd::GroupedDataTable) = length(gd.starts)
Base.endof(gd::GroupedDataTable) = length(gd.starts)
Base.first(gd::GroupedDataTable) = gd[1]
Base.last(gd::GroupedDataTable) = gd[end]

Base.getindex(gd::GroupedDataTable, idx::Int) =
    view(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]])
Base.getindex(gd::GroupedDataTable, I::AbstractArray{Bool}) =
    GroupedDataTable(gd.parent, gd.cols, gd.idx, gd.starts[I], gd.ends[I])

Base.names(gd::GroupedDataTable) = names(gd.parent)
_names(gd::GroupedDataTable) = _names(gd.parent)

##############################################################################
##
## GroupApplied...
##    the result of a split-apply operation
##    TODOs:
##      - better name?
##      - ref
##      - keys, vals
##      - length
##      - start, next, done -- should this return (k,v) or just v?
##      - make it a real associative type? Is there a need to look up key columns?
##
##############################################################################

"""
The result of a `map` operation on a GroupedDataTable; mainly for use
with `combine`

Not meant to be constructed directly, see `groupby` abnd
`combine`. Minimal support is provided for this type. `map` is
provided for a GroupApplied object.

"""
immutable GroupApplied{T<:AbstractDataTable}
    gd::GroupedDataTable
    vals::Vector{T}

    @compat function (::Type{GroupApplied})(gd::GroupedDataTable, vals::Vector)
        length(gd) == length(vals) ||
            throw(DimensionMismatch("GroupApplied requires keys and vals be of equal length (got $(length(gd)) and $(length(vals)))."))
        new{eltype(vals)}(gd, vals)
    end
end


#
# Apply / map
#

# map() sweeps along groups
function Base.map(f::Function, gd::GroupedDataTable)
    GroupApplied(gd, [wrap(f(dt)) for dt in gd])
end
function Base.map(f::Function, ga::GroupApplied)
    GroupApplied(ga.gd, [wrap(f(dt)) for dt in ga.vals])
end

wrap(dt::AbstractDataTable) = dt
wrap(A::Matrix) = convert(DataTable, A)
wrap(s::Any) = DataTable(x1 = s)

"""
Combine a GroupApplied object (rudimentary)

```julia
combine(ga::GroupApplied)
```

### Arguments

* `ga` : a GroupApplied

### Returns

* `::DataTable`

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
combine(map(d -> mean(dropnull(d[:c])), gd))
```

"""
function combine(ga::GroupApplied)
    gd, vals = ga.gd, ga.vals
    valscat = vcat(vals...)
    idx = Vector{Int}(size(valscat, 1))
    j = 0
    @inbounds for (start, val) in zip(gd.starts, vals)
        n = size(val, 1)
        idx[j + (1:n)] = gd.idx[start]
        j += n
    end
    hcat!(gd.parent[idx, gd.cols], valscat)
end


"""
Apply a function to each column in an AbstractDataTable or
GroupedDataTable

```julia
colwise(f::Function, d)
colwise(d)
```

### Arguments

* `f` : a function or vector of functions
* `d` : an AbstractDataTable of GroupedDataTable

If `d` is not provided, a curried version of groupby is given.

### Returns

* various, depending on the call

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
colwise(sum, dt)
colwise([sum, lenth], dt)
colwise((minimum, maximum), dt)
colwise(sum, groupby(dt, :a))
```

"""
function colwise(f, d::AbstractDataTable)
    x = [f(d[i]) for i in 1:ncol(d)]
    if eltype(x) <: Nullable
        return NullableArray(x)
    else
        return x
    end
end
# apply several functions to each column in a DataTable
function colwise(fns::Union{AbstractVector, Tuple}, d::AbstractDataTable)
    x = [f(d[i]) for f in fns, i in 1:ncol(d)]
    if eltype(x) <: Nullable
        return NullableArray(x)
    else
        return x
    end
end
colwise(f, gd::GroupedDataTable) = [colwise(f, g) for g in gd]
colwise(f) = x -> colwise(f, x)

"""
Split-apply-combine in one step; apply `f` to each grouping in `d`
based on columns `col`

```julia
by(d::AbstractDataTable, cols, f::Function; sort::Bool = false)
by(f::Function, d::AbstractDataTable, cols; sort::Bool = false)
```

### Arguments

* `d` : an AbstractDataTable
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `f` : a function to be applied to groups; expects each argument to
  be an AbstractDataTable
* `sort`: sort row groups (no sorting by default)

`f` can return a value, a vector, or a DataTable. For a value or
vector, these are merged into a column along with the `cols` keys. For
a DataTable, `cols` are combined along columns with the resulting
DataTable. Returning a DataTable is the clearest because it allows
column labeling.

A method is defined with `f` as the first argument, so do-block
notation can be used.

`by(d, cols, f)` is equivalent to `combine(map(f, groupby(d, cols)))`.

### Returns

* `::DataTable`

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
by(dt, :a, d -> sum(d[:c]))
by(dt, :a, d -> 2 * dropnull(d[:c]))
by(dt, :a, d -> DataTable(c_sum = sum(d[:c]), c_mean = mean(dropnull(d[:c]))))
by(dt, :a, d -> DataTable(c = d[:c], c_mean = mean(dropnull(d[:c]))))
by(dt, [:a, :b]) do d
    DataTable(m = mean(dropnull(d[:c])), v = var(dropnull(d[:c])))
end
```

"""
by(d::AbstractDataTable, cols, f::Function; sort::Bool = false) =
    combine(map(f, groupby(d, cols, sort = sort)))
by(f::Function, d::AbstractDataTable, cols; sort::Bool = false) =
    by(d, cols, f, sort = sort)

#
# Aggregate convenience functions
#

# Applies a set of functions over a DataTable, in the from of a cross-product
"""
Split-apply-combine that applies a set of functions over columns of an
AbstractDataTable or GroupedDataTable

```julia
aggregate(d::AbstractDataTable, cols, fs)
aggregate(gd::GroupedDataTable, fs)
```

### Arguments

* `d` : an AbstractDataTable
* `gd` : a GroupedDataTable
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `fs` : a function or vector of functions to be applied to vectors
  within groups; expects each argument to be a column vector

Each `fs` should return a value or vector. All returns must be the
same length.

### Returns

* `::DataTable`

### Examples

```julia
dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
               b = repeat([2, 1], outer=[4]),
               c = randn(8))
aggregate(dt, :a, sum)
aggregate(dt, :a, [sum, x->mean(dropnull(x))])
aggregate(groupby(dt, :a), [sum, x->mean(dropnull(x))])
dt |> groupby(:a) |> [sum, x->mean(dropnull(x))]   # equivalent
```

"""
aggregate(d::AbstractDataTable, fs::Function; sort::Bool=false) = aggregate(d, [fs], sort=sort)
function aggregate{T<:Function}(d::AbstractDataTable, fs::Vector{T}; sort::Bool=false)
    headers = _makeheaders(fs, _names(d))
    _aggregate(d, fs, headers, sort)
end

# Applies aggregate to non-key cols of each SubDataTable of a GroupedDataTable
aggregate(gd::GroupedDataTable, f::Function; sort::Bool=false) = aggregate(gd, [f], sort=sort)
function aggregate{T<:Function}(gd::GroupedDataTable, fs::Vector{T}; sort::Bool=false)
    headers = _makeheaders(fs, setdiff(_names(gd), gd.cols))
    res = combine(map(x -> _aggregate(without(x, gd.cols), fs, headers), gd))
    sort && sort!(res, cols=headers)
    res
end

(|>)(gd::GroupedDataTable, fs::Function) = aggregate(gd, fs)
(|>){T<:Function}(gd::GroupedDataTable, fs::Vector{T}) = aggregate(gd, fs)

# Groups DataTable by cols before applying aggregate
function aggregate{S<:ColumnIndex, T <:Function}(d::AbstractDataTable,
                                                 cols::Union{S, AbstractVector{S}},
                                                 fs::Union{T, Vector{T}};
                                                 sort::Bool=false)
    aggregate(groupby(d, cols, sort=sort), fs)
end

function _makeheaders{T<:Function}(fs::Vector{T}, cn::Vector{Symbol})
    fnames = _fnames(fs) # see other/utils.jl
    [Symbol(colname,'_',fname) for fname in fnames for colname in cn]
end

function _aggregate{T<:Function}(d::AbstractDataTable, fs::Vector{T}, headers::Vector{Symbol}, sort::Bool=false)
    res = DataTable(Any[vcat(f(d[i])) for f in fs for i in 1:size(d, 2)], headers)
    sort && sort!(res, cols=headers)
    res
end
