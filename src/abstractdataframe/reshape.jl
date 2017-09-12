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
stack(df::AbstractDataFrame, [measure_vars], [id_vars];
      variable_name::Symbol=:variable, value_name::Symbol=:value)
melt(df::AbstractDataFrame, [id_vars], [measure_vars];
     variable_name::Symbol=:variable, value_name::Symbol=:value)
```

### Arguments

* `df` : the AbstractDataFrame to be stacked

* `measure_vars` : the columns to be stacked (the measurement
  variables), a normal column indexing type, like a Symbol,
  Vector{Symbol}, Int, etc.; for `melt`, defaults to all
  variables that are not `id_vars`. If neither `measure_vars`
  or `id_vars` are given, `measure_vars` defaults to all
  floating point columns.

* `id_vars` : the identifier columns that are repeated during
  stacking, a normal column indexing type; for `stack` defaults to all
  variables that are not `measure_vars`

* `variable_name` : the name of the new stacked column that shall hold the names
  of each of `measure_vars`

* `value_name` : the name of the new stacked column containing the values from
  each of `measure_vars`


### Result

* `::DataFrame` : the long-format DataFrame with column `:value`
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
d1s_name = melt(d1, [:a, :b, :e], variable_name=:somemeasure)
```

"""
function stack(df::AbstractDataFrame, measure_vars::Vector{Int},
               id_vars::Vector{Int}; variable_name::Symbol=:variable,
               value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataFrame(Any[repeat(_names(df)[measure_vars], inner=nrow(df)),   # variable
                  vcat([df[c] for c in measure_vars]...),             # value
                  [repeat(df[c], outer=N) for c in id_vars]...],      # id_var columns
              cnames)
end
function stack(df::AbstractDataFrame, measure_var::Int, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, [measure_var], [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_vars::Vector{Int}, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, measure_vars, [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_var::Int, id_vars::Vector{Int};
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, [measure_var], id_vars;
            variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_vars, id_vars;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, index(df)[measure_vars], index(df)[id_vars];
          variable_name=variable_name, value_name=value_name)
end
# no vars specified, by default select only numeric columns
numeric_vars(df::AbstractDataFrame) =
    [T <: AbstractFloat || (T >: Null && Nulls.T(T) <: AbstractFloat)
     for T in eltypes(df)]

function stack(df::AbstractDataFrame, measure_vars = numeric_vars(df);
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    mv_inds = index(df)[measure_vars]
    stack(df, mv_inds, setdiff(1:ncol(df), mv_inds);
          variable_name=variable_name, value_name=value_name)
end

"""
Stacks a DataFrame; convert from a wide to long format; see
`stack`.
"""
function melt(df::AbstractDataFrame, id_vars::Union{Int,Symbol};
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    melt(df, [id_vars]; variable_name=variable_name, value_name=value_name)
end
function melt(df::AbstractDataFrame, id_vars;
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    id_inds = index(df)[id_vars]
    stack(df, setdiff(1:ncol(df), id_inds), id_inds;
          variable_name=variable_name, value_name=value_name)
end
function melt(df::AbstractDataFrame, id_vars, measure_vars;
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, measure_vars, id_vars; variable_name=variable_name,
          value_name=value_name)
end
melt(df::AbstractDataFrame; variable_name::Symbol=:variable, value_name::Symbol=:value) =
    stack(df; variable_name=variable_name, value_name=value_name)

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

* `::DataFrame` : the wide-format DataFrame


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
    refkeycol = CategoricalArray{Union{eltype(df[rowkey]), Null}}(df[rowkey])
    valuecol = df[value]
    keycol = CategoricalArray{Union{eltype(df[colkey]), Null}}(df[colkey])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    payload = DataFrame(Any[similar_nullable(valuecol, Nrow) for i in 1:Ncol], map(Symbol, levels(keycol)))
    nowarning = true
    for k in 1:nrow(df)
        j = Int(CategoricalArrays.order(keycol.pool)[keycol.refs[k]])
        i = Int(CategoricalArrays.order(refkeycol.pool)[refkeycol.refs[k]])
        if i > 0 && j > 0
            if nowarning && !isnull(payload[j][i])
                warn("Duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    levs = levels(refkeycol)
    col = similar_nullable(df[rowkey], length(levs))
    insert!(payload, 1, copy!(col, levs), _names(df)[rowkey])
end
unstack(df::AbstractDataFrame, rowkey, colkey, value) =
    unstack(df, index(df)[rowkey], index(df)[colkey], index(df)[value])

# Version of unstack with just the colkey and value columns provided
unstack(df::AbstractDataFrame, colkey, value) =
    unstack(df, index(df)[colkey], index(df)[value])

function unstack(df::AbstractDataFrame, colkey::Int, value::Int)
    # group on anything not a key or value:
    g = groupby(df, setdiff(_names(df), _names(df)[[colkey, value]]), sort=true)
    groupidxs = [g.idx[g.starts[i]:g.ends[i]] for i in 1:length(g.starts)]
    rowkey = zeros(Int, size(df, 1))
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] = i
    end
    keycol = CategoricalArray{Union{eltype(df[colkey]), Null}}(df[colkey])
    valuecol = df[value]
    df1 = nullable!(df[g.idx[g.starts], g.cols], g.cols)
    Nrow = length(g)
    Ncol = length(levels(keycol))
    df2 = DataFrame(Any[similar_nullable(valuecol, Nrow) for i in 1:Ncol], map(Symbol, levels(keycol)))
    nowarning = true
    for k in 1:nrow(df)
        j = Int(CategoricalArrays.order(keycol.pool)[keycol.refs[k]])
        i = rowkey[k]
        if i > 0 && j > 0
            if nowarning && !isnull(df2[j][i])
                warn("Duplicate entries in unstack at row $k.")
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
StackedVector(d::AbstractVector...)
```

### Arguments

* `d...` : one or more AbstractVectors

### Examples

```julia
StackedVector(Any[[1,2], [9,10], [11,12]])  # [1,2,9,10,11,12]
```

"""
mutable struct StackedVector <: AbstractVector{Any}
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

function Base.getindex(v::StackedVector,i::AbstractVector{I}) where I<:Real
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
Base.similar(v::StackedVector, T::Type, dims::Union{Integer, AbstractUnitRange}...) =
    similar(v.components[1], T, dims...)

CategoricalArrays.CategoricalArray(v::StackedVector) = CategoricalArray(v[:]) # could be more efficient


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
mutable struct RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    inner::Int
    outer::Int
end

function Base.getindex(v::RepeatedVector{T},i::AbstractVector{I}) where {T,I<:Real}
    N = length(v.parent)
    idx = Int[Base.fld1(mod1(j,v.inner*N),v.inner) for j in i]
    v.parent[idx]
end
function Base.getindex(v::RepeatedVector{T},i::Real) where T
    N = length(v.parent)
    idx = Base.fld1(mod1(i,v.inner*N),v.inner)
    v.parent[idx]
end
Base.getindex(v::RepeatedVector,i::Range) = getindex(v, [i;])

Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.inner * v.outer * length(v.parent)
Base.ndims(v::RepeatedVector) = 1
Base.eltype(v::RepeatedVector{T}) where {T} = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(v.parent), v.inner, v.outer)
Base.similar(v::RepeatedVector, T, dims::Dims) = similar(v.parent, T, dims)
Base.unique(v::RepeatedVector) = unique(v.parent)

function CategoricalArrays.CategoricalArray(v::RepeatedVector)
    res = CategoricalArrays.CategoricalArray(v.parent)
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
stackdf(df::AbstractDataFrame, [measure_vars], [id_vars];
        variable_name::Symbol=:variable, value_name::Symbol=:value)
meltdf(df::AbstractDataFrame, [id_vars], [measure_vars];
       variable_name::Symbol=:variable, value_name::Symbol=:value)
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

* `::DataFrame` : the long-format DataFrame with column `:value`
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
function stackdf(df::AbstractDataFrame, measure_vars::Vector{Int},
                 id_vars::Vector{Int}; variable_name::Symbol=:variable,
                 value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataFrame(Any[RepeatedVector(_names(df)[measure_vars], nrow(df), 1),   # variable
                  StackedVector(Any[df[:,c] for c in measure_vars]),     # value
                  [RepeatedVector(df[:,c], 1, N) for c in id_vars]...],     # id_var columns
              cnames)
end
function stackdf(df::AbstractDataFrame, measure_var::Int, id_var::Int;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, [measure_var], [id_var]; variable_name=variable_name,
            value_name=value_name)
end
function stackdf(df::AbstractDataFrame, measure_vars, id_var::Int;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, measure_vars, [id_var]; variable_name=variable_name,
            value_name=value_name)
end
function stackdf(df::AbstractDataFrame, measure_var::Int, id_vars;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, [measure_var], id_vars; variable_name=variable_name,
            value_name=value_name)
end
function stackdf(df::AbstractDataFrame, measure_vars, id_vars;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, index(df)[measure_vars], index(df)[id_vars];
            variable_name=variable_name, value_name=value_name)
end
function stackdf(df::AbstractDataFrame, measure_vars = numeric_vars(df);
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    m_inds = index(df)[measure_vars]
    stackdf(df, m_inds, setdiff(1:ncol(df), m_inds);
            variable_name=variable_name, value_name=value_name)
end

"""
A stacked view of a DataFrame (long format); see `stackdf`
"""
function meltdf(df::AbstractDataFrame, id_vars; variable_name::Symbol=:variable,
                value_name::Symbol=:value)
    id_inds = index(df)[id_vars]
    stackdf(df, setdiff(1:ncol(df), id_inds), id_inds;
            variable_name=variable_name, value_name=value_name)
end
function meltdf(df::AbstractDataFrame, id_vars, measure_vars;
                variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdf(df, measure_vars, id_vars; variable_name=variable_name,
            value_name=value_name)
end
meltdf(df::AbstractDataFrame; variable_name::Symbol=:variable, value_name::Symbol=:value) =
    stackdf(df; variable_name=variable_name, value_name=value_name)
