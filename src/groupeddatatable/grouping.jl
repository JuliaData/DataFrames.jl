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

function groupsort_indexer(x::AbstractVector, ngroups::Integer, null_last::Bool=false)
    # translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).

    # count group sizes, location 0 for NULL
    n = length(x)
    # counts = x.pool
    counts = fill(0, ngroups + 1)
    for i = 1:n
        counts[x[i] + 1] += 1
    end

    # mark the start of each contiguous group of like-indexed data
    where = fill(1, ngroups + 1)
    if null_last
        for i = 3:ngroups+1
            where[i] = where[i - 1] + counts[i - 1]
        end
        where[1] = where[end] + counts[end]
    else
        for i = 2:ngroups+1
            where[i] = where[i - 1] + counts[i - 1]
        end
    end

    # this is our indexer
    result = fill(0, n)
    for i = 1:n
        label = x[i] + 1
        result[where[label]] = i
        where[label] += 1
    end
    result, where, counts
end

"""
A view of an AbstractDataTable split into row groups

```julia
groupby(d::AbstractDataTable, cols)
groupby(cols)
```

### Arguments

* `d` : an AbstractDataTable to split (optional, see [Returns](#returns))
* `cols` : data frame columns to group by

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
function groupby{T}(d::AbstractDataTable, cols::Vector{T})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489

    ncols = length(cols)
    # use CategoricalArray to get a set of integer references for each unique item
    nv = NullableCategoricalArray(d[cols[ncols]])
    # if there are NULLs, add 1 to the refs to avoid underflows in x later
    anynulls = (findfirst(nv.refs, 0) > 0 ? 1 : 0)
    # use UInt32 instead of the original array's integer size since the number of levels can be high
    x = similar(nv.refs, UInt32)
    for i = 1:nrow(d)
        if nv.refs[i] == 0
            x[i] = 1
        else
            x[i] = CategoricalArrays.order(nv.pool)[nv.refs[i]] + anynulls
        end
    end
    # also compute the number of groups, which is the product of the set lengths
    ngroups = length(levels(nv)) + anynulls
    # if there's more than 1 column, do roughly the same thing repeatedly
    for j = (ncols - 1):-1:1
        nv = NullableCategoricalArray(d[cols[j]])
        anynulls = (findfirst(nv.refs, 0) > 0 ? 1 : 0)
        for i = 1:nrow(d)
            if nv.refs[i] != 0
                x[i] += (CategoricalArrays.order(nv.pool)[nv.refs[i]] + anynulls - 1) * ngroups
            end
        end
        ngroups = ngroups * (length(levels(nv)) + anynulls)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = groupsort_indexer(x, ngroups)
    # Remove zero-length groupings
    starts = _uniqueofsorted(starts)
    ends = starts[2:end] - 1
    GroupedDataTable(d, cols, idx, starts[1:end-1], ends)
end
groupby(d::AbstractDataTable, cols) = groupby(d, [cols])

# add a function curry
groupby{T}(cols::Vector{T}) = x -> groupby(x, cols)
groupby(cols) = x -> groupby(x, cols)

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
    valscat = vcat(vals)
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
colwise(sum, groupby(dt, :a))
```

"""
colwise(f::Function, d::AbstractDataTable) = Any[vcat(f(d[idx])) for idx in 1:size(d, 2)]
colwise(f::Function, gd::GroupedDataTable) = map(colwise(f), gd)
colwise(f::Function) = x -> colwise(f, x)
colwise(f) = x -> colwise(f, x)
# apply several functions to each column in a DataTable
colwise{T<:Function}(fns::Vector{T}, d::AbstractDataTable) =
    reshape(Any[vcat(f(d[idx])) for f in fns, idx in 1:size(d, 2)],
            length(fns)*size(d, 2))
colwise{T<:Function}(fns::Vector{T}, gd::GroupedDataTable) = map(colwise(fns), gd)
colwise{T<:Function}(fns::Vector{T}) = x -> colwise(fns, x)


"""
Split-apply-combine in one step; apply `f` to each grouping in `d`
based on columns `col`

```julia
by(d::AbstractDataTable, cols, f::Function)
by(f::Function, d::AbstractDataTable, cols)
```

### Arguments

* `d` : an AbstractDataTable
* `cols` : a column indicator (Symbol, Int, Vector{Symbol}, etc.)
* `f` : a function to be applied to groups; expects each argument to
  be an AbstractDataTable

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
by(d::AbstractDataTable, cols, f::Function) = combine(map(f, groupby(d, cols)))
by(f::Function, d::AbstractDataTable, cols) = by(d, cols, f)

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
aggregate(d::AbstractDataTable, fs::Function) = aggregate(d, [fs])
function aggregate{T<:Function}(d::AbstractDataTable, fs::Vector{T})
    headers = _makeheaders(fs, _names(d))
    _aggregate(d, fs, headers)
end

# Applies aggregate to non-key cols of each SubDataTable of a GroupedDataTable
aggregate(gd::GroupedDataTable, f::Function) = aggregate(gd, [f])
function aggregate{T<:Function}(gd::GroupedDataTable, fs::Vector{T})
    headers = _makeheaders(fs, _setdiff(_names(gd), gd.cols))
    combine(map(x -> _aggregate(without(x, gd.cols), fs, headers), gd))
end
(|>)(gd::GroupedDataTable, fs::Function) = aggregate(gd, fs)
(|>){T<:Function}(gd::GroupedDataTable, fs::Vector{T}) = aggregate(gd, fs)

# Groups DataTable by cols before applying aggregate
function aggregate{S <: ColumnIndex, T <:Function}(d::AbstractDataTable,
                                     cols::@compat(Union{S, AbstractVector{S}}),
                                     fs::@compat(Union{T, Vector{T}}))
    aggregate(groupby(d, cols), fs)
end

function _makeheaders{T<:Function}(fs::Vector{T}, cn::Vector{Symbol})
    fnames = _fnames(fs) # see other/utils.jl
    reshape([Symbol(colname,'_',fname) for fname in fnames, colname in cn],
            length(fnames)*length(cn))
end

function _aggregate{T<:Function}(d::AbstractDataTable, fs::Vector{T}, headers::Vector{Symbol})
    DataTable(colwise(fs, d), headers)
end
