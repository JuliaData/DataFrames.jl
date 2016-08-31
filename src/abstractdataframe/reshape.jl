##############################################################################
##
## Reshaping
##
## Also, see issue # ??
##
##############################################################################

##############################################################################
##
## stack()
## melt()
##
##############################################################################

"""
Stacks a DataFrame; convert from a wide to long format


```julia
stack(df::AbstractDataFrame, measure_vars, id_vars)
stack(df::AbstractDataFrame, measure_vars)
stack(df::AbstractDataFrame)
melt(df::AbstractDataFrame, id_vars, measure_vars)
melt(df::AbstractDataFrame, id_vars)
```

### Arguments

* `df` : the AbstractDataFrame to be stacked

* `measure_vars` : the columns to be stacked (the measurement
  variables), a normal column indexing type, like a Symbol,
  Vector{Symbol}, Int, etc.; for `melt`, defaults to all
  variables that are not `id_vars`

* `id_vars` : the identifier columns that are repeated during
  stacking, a normal column indexing type; for `stack` defaults to all
  variables that are not `measure_vars`

If neither `measure_vars` or `id_vars` are given, `measure_vars`
defaults to all floating point columns.

### Result

* `::DataFrame` : the long-format dataframe with column `:value`
  holding the values of the stacked columns (`measure_vars`), with
  column `:variable` a Vector of Symbols with the `measure_vars` name,
  and with columns for each of the `id_vars`.

See also `stackdf` and `meltdf` for stacking methods that return a
view into the original DataFrame. See `unstack` for converting from
long to wide format.


### Examples

```julia
d1 = DataFrame(a = repeat([1:3;], inner = [4]),
               b = repeat([1:4;], inner = [3]),
               c = randn(12),
               d = randn(12),
               e = map(string, 'a':'l'))

d1s = stack(d1, [:c, :d])
d1s2 = stack(d1, [:c, :d], [:a])
d1m = melt(d1, [:a, :b, :e])
```

"""
function stack(df::AbstractDataFrame, measure_vars::Vector{Int}, id_vars::Vector{Int})
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, :value)
    insert!(cnames, 1, :variable)
    DataFrame(Any[Compat.repeat(_names(df)[measure_vars], inner=nrow(df)),   # variable
                  vcat([df[c] for c in measure_vars]...),                    # value
                  [Compat.repeat(df[c], outer=N) for c in id_vars]...],      # id_var columns
              cnames)
end
function stack(df::AbstractDataFrame, measure_vars::Int, id_vars::Int)
    stack(df, [measure_vars], [id_vars])
end
function stack(df::AbstractDataFrame, measure_vars::Vector{Int}, id_vars::Int)
    stack(df, measure_vars, [id_vars])
end
function stack(df::AbstractDataFrame, measure_vars::Int, id_vars::Vector{Int})
    stackdf(df, [measure_vars], id_vars)
end
stack(df::AbstractDataFrame, measure_vars, id_vars) =
    stack(df, index(df)[measure_vars], index(df)[id_vars])
function stack(df::AbstractDataFrame, measure_vars)
    mv_inds = index(df)[measure_vars]
    stack(df, mv_inds, _setdiff(1:ncol(df), mv_inds))
end
function stack(df::AbstractDataFrame)
    idx = [1:length(df);][[t <: AbstractFloat for t in eltypes(df)]]
    stack(df, idx)
end

"""
Stacks a DataFrame; convert from a wide to long format; see
`stack`.
"""
melt(df::AbstractDataFrame, id_vars::@compat(Union{Int,Symbol})) = melt(df, [id_vars])
function melt(df::AbstractDataFrame, id_vars)
    id_inds = index(df)[id_vars]
    stack(df, _setdiff(1:ncol(df), id_inds), id_inds)
end
melt(df::AbstractDataFrame, id_vars, measure_vars) = stack(df, measure_vars, id_vars)
melt(df::AbstractDataFrame) = stack(df)

##############################################################################
##
## unstack()
##
##############################################################################

"""
Unstacks a DataFrame; convert from a long to wide format

```julia
unstack(df::AbstractDataFrame, rowkey, colkey, value)
unstack(df::AbstractDataFrame, colkey, value)
unstack(df::AbstractDataFrame)
```

### Arguments

* `df` : the AbstractDataFrame to be unstacked

* `rowkey` : the column with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`

* `colkey` : the column holding the column names in wide format,
  defaults to `:variable`

* `value` : the value column, defaults to `:value`

### Result

* `::DataFrame` : the wide-format dataframe


### Examples

```julia
wide = DataFrame(id = 1:12,
                 a  = repeat([1:3;], inner = [4]),
                 b  = repeat([1:4;], inner = [3]),
                 c  = randn(12),
                 d  = randn(12))

long = stack(wide)
wide0 = unstack(long)
wide1 = unstack(long, :variable, :value)
wide2 = unstack(long, :id, :variable, :value)
```
Note that there are some differences between the widened results above.

"""
function unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int, value::Int)
    # `rowkey` integer indicating which column to place along rows
    # `colkey` integer indicating which column to place along column headers
    # `value` integer indicating which column has values
    refkeycol = PooledDataArray(df[rowkey])
    valuecol = df[value]
    # TODO make a version with a default refkeycol
    keycol = PooledDataArray(df[colkey])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length)
    payload = DataFrame(Any[DataArray(eltype(valuecol), Nrow) for i in 1:Ncol], map(Symbol, keycol.pool))
    nowarning = true
    for k in 1:nrow(df)
        j = @compat Int(keycol.refs[k])
        i = @compat Int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i])
                warn("Duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert!(payload, 1, refkeycol.pool, _names(df)[rowkey])
end
unstack(df::AbstractDataFrame, rowkey, colkey, value) =
    unstack(df, index(df)[rowkey], index(df)[colkey], index(df)[value])

# Version of unstack with just the colkey and value columns provided
unstack(df::AbstractDataFrame, colkey, value) =
    unstack(df, index(df)[colkey], index(df)[value])

function unstack(df::AbstractDataFrame, colkey::Int, value::Int)
    # group on anything not a key or value:
    g = groupby(df, setdiff(_names(df), _names(df)[[colkey, value]]))
    groupidxs = [g.idx[g.starts[i]:g.ends[i]] for i in 1:length(g.starts)]
    rowkey = PooledDataArray(zeros(Int, size(df, 1)), [1:length(groupidxs);])
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] = i
    end
    keycol = PooledDataArray(df[colkey])
    valuecol = df[value]
    df1 = df[g.idx[g.starts], g.cols]
    keys = unique(keycol)
    Nrow = length(g)
    Ncol = length(keycol.pool)
    df2 = DataFrame(Any[DataArray(fill(valuecol[1], Nrow), fill(true, Nrow)) for i in 1:Ncol], map(@compat(Symbol), keycol.pool))
    nowarning = true
    for k in 1:nrow(df)
        j = @compat Int(keycol.refs[k])
        i = rowkey[k]
        if i > 0 && j > 0
            if nowarning && !isna(df2[j][i])
                warn("Duplicate entries in unstack.")
                nowarning = false
            end
            df2[j][i]  = valuecol[k]
        end
    end
    hcat(df1, df2)
end

unstack(df::AbstractDataFrame) = unstack(df, :id, :variable, :value)


##############################################################################
##
## Reshaping using referencing (issue #145)
## New AbstractVector types (all read only):
##     StackedVector
##     RepeatedVector
##
##############################################################################

"""
An AbstractVector{Any} that is a linear, concatenated view into
another set of AbstractVectors

NOTE: Not exported.

### Constructor

```julia
RepeatedVector(d::AbstractVector...)
```

### Arguments

* `d...` : one or more AbstractVectors

### Examples

```julia
StackedVector(Any[[1,2], [9,10], [11,12]])  # [1,2,9,10,11,12]
```

"""
type StackedVector <: AbstractVector{Any}
    components::Vector{Any}
end

function Base.getindex(v::StackedVector,i::Real)
    lengths = [length(x)::Int for x in v.components]
    cumlengths = [0; cumsum(lengths)]
    j = searchsortedlast(cumlengths .+ 1, i)
    if j > length(cumlengths)
        error("indexing bounds error")
    end
    k = i - cumlengths[j]
    if k < 1 || k > length(v.components[j])
        error("indexing bounds error")
    end
    v.components[j][k]
end

function Base.getindex{I<:Real}(v::StackedVector,i::AbstractVector{I})
    result = similar(v.components[1], length(i))
    for idx in 1:length(i)
        result[idx] = v[i[idx]]
    end
    result
end

Base.size(v::StackedVector) = (length(v),)
Base.length(v::StackedVector) = sum(map(length, v.components))
Base.ndims(v::StackedVector) = 1
Base.eltype(v::StackedVector) = promote_type(map(eltype, v.components)...)
Base.similar(v::StackedVector, T, dims::Dims) = similar(v.components[1], T, dims)

DataArrays.PooledDataArray(v::StackedVector) = PooledDataArray(v[:]) # could be more efficient


"""
An AbstractVector that is a view into another AbstractVector with
repeated elements

NOTE: Not exported.

### Constructor

```julia
RepeatedVector(parent::AbstractVector, inner::Int, outer::Int)
```

### Arguments

* `parent` : the AbstractVector that's repeated
* `inner` : the numer of times each element is repeated
* `outer` : the numer of times the whole vector is repeated after
  expanded by `inner`

`inner` and `outer` have the same meaning as similarly named arguments
to `repeat`.

### Examples

```julia
RepeatedVector([1,2], 3, 1)   # [1,1,1,2,2,2]
RepeatedVector([1,2], 1, 3)   # [1,2,1,2,1,2]
RepeatedVector([1,2], 2, 2)   # [1,2,1,2,1,2,1,2]
```

"""
type RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    inner::Int
    outer::Int
end

function Base.getindex{T,I<:Real}(v::RepeatedVector{T},i::AbstractVector{I})
    N = length(v.parent)
    idx = Int[Base.fld1(mod1(j,v.inner*N),v.inner) for j in i]
    v.parent[idx]
end
function Base.getindex{T}(v::RepeatedVector{T},i::Real)
    N = length(v.parent)
    idx = Base.fld1(mod1(i,v.inner*N),v.inner)
    v.parent[idx]
end
Base.getindex(v::RepeatedVector,i::Range) = getindex(v, [i;])

Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.inner * v.outer * length(v.parent)
Base.ndims(v::RepeatedVector) = 1
Base.eltype{T}(v::RepeatedVector{T}) = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(v.parent), v.inner, v.outer)
Base.similar(v::RepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)
Base.unique(v::RepeatedVector) = unique(v.parent)

function DataArrays.PooledDataArray(v::RepeatedVector)
    res = DataArrays.PooledDataArray(v.parent)
    res.refs = repeat(res.refs, inner = [v.inner], outer = [v.outer])
    res
end

##############################################################################
##
## stackdf()
## meltdf()
## Reshaping using referencing (issue #145), using the above vector types
##
##############################################################################

"""
A stacked view of a DataFrame (long format)

Like `stack` and `melt`, but a view is returned rather than data
copies.

```julia
stackdf(df::AbstractDataFrame, measure_vars, id_vars)
stackdf(df::AbstractDataFrame, measure_vars)
meltdf(df::AbstractDataFrame, id_vars, measure_vars)
meltdf(df::AbstractDataFrame, id_vars)
```

### Arguments

* `df` : the wide AbstractDataFrame

* `measure_vars` : the columns to be stacked (the measurement
  variables), a normal column indexing type, like a Symbol,
  Vector{Symbol}, Int, etc.; for `melt`, defaults to all
  variables that are not `id_vars`

* `id_vars` : the identifier columns that are repeated during
  stacking, a normal column indexing type; for `stack` defaults to all
  variables that are not `measure_vars`

### Result

* `::DataFrame` : the long-format dataframe with column `:value`
  holding the values of the stacked columns (`measure_vars`), with
  column `:variable` a Vector of Symbols with the `measure_vars` name,
  and with columns for each of the `id_vars`.

The result is a view because the columns are special AbstractVectors
that return indexed views into the original DataFrame.

### Examples

```julia
d1 = DataFrame(a = repeat([1:3;], inner = [4]),
               b = repeat([1:4;], inner = [3]),
               c = randn(12),
               d = randn(12),
               e = map(string, 'a':'l'))

d1s = stackdf(d1, [:c, :d])
d1s2 = stackdf(d1, [:c, :d], [:a])
d1m = meltdf(d1, [:a, :b, :e])
```

"""
function stackdf(df::AbstractDataFrame, measure_vars::Vector{Int}, id_vars::Vector{Int})
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, :value)
    insert!(cnames, 1, :variable)
    DataFrame(Any[RepeatedVector(_names(df)[measure_vars], nrow(df), 1),   # variable
                  StackedVector(Any[df[:,c] for c in measure_vars]),     # value
                  [RepeatedVector(df[:,c], 1, N) for c in id_vars]...],     # id_var columns
              cnames)
end
function stackdf(df::AbstractDataFrame, measure_vars::Int, id_vars::Int)
    stackdf(df, [measure_vars], [id_vars])
end
function stackdf(df::AbstractDataFrame, measure_vars, id_vars::Int)
    stackdf(df, measure_vars, [id_vars])
end
function stackdf(df::AbstractDataFrame, measure_vars::Int, id_vars)
    stackdf(df, [measure_vars], id_vars)
end
function stackdf(df::AbstractDataFrame, measure_vars, id_vars)
    stackdf(df, index(df)[measure_vars], index(df)[id_vars])
end
function stackdf(df::AbstractDataFrame, measure_vars)
    m_inds = index(df)[measure_vars]
    stackdf(df, m_inds, _setdiff(1:ncol(df), m_inds))
end
function stackdf(df::AbstractDataFrame)
    idx = [1:length(df);][[t <: AbstractFloat for t in eltypes(df)]]
    stackdf(df, idx)
end

"""
A stacked view of a DataFrame (long format); see `stackdf`
"""
function meltdf(df::AbstractDataFrame, id_vars)
    id_inds = index(df)[id_vars]
    stackdf(df, _setdiff(1:ncol(df), id_inds), id_inds)
end
meltdf(df::AbstractDataFrame, id_vars, measure_vars) =
    stackdf(df, measure_vars, id_vars)
meltdf(df::AbstractDataFrame) = stackdf(df)
