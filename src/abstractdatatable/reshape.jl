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
Stacks a DataTable; convert from a wide to long format


```julia
stack(dt::AbstractDataTable, [measure_vars], [id_vars];
      variable_name::Symbol=:variable, value_name::Symbol=:value)
melt(dt::AbstractDataTable, [id_vars], [measure_vars];
     variable_name::Symbol=:variable, value_name::Symbol=:value)
```

### Arguments

* `dt` : the AbstractDataTable to be stacked

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

* `::DataTable` : the long-format datatable with column `:value`
  holding the values of the stacked columns (`measure_vars`), with
  column `:variable` a Vector of Symbols with the `measure_vars` name,
  and with columns for each of the `id_vars`.

See also `stackdt` and `meltdt` for stacking methods that return a
view into the original DataTable. See `unstack` for converting from
long to wide format.


### Examples

```julia
d1 = DataTable(a = repeat([1:3;], inner = [4]),
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
function stack(dt::AbstractDataTable, measure_vars::Vector{Int},
               id_vars::Vector{Int}; variable_name::Symbol=:variable,
               value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(dt)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataTable(Any[Compat.repeat(_names(dt)[measure_vars], inner=nrow(dt)),   # variable
                  vcat([dt[c] for c in measure_vars]...),                    # value
                  [Compat.repeat(dt[c], outer=N) for c in id_vars]...],      # id_var columns
              cnames)
end
function stack(dt::AbstractDataTable, measure_var::Int, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(dt, [measure_var], [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(dt::AbstractDataTable, measure_vars::Vector{Int}, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(dt, measure_vars, [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(dt::AbstractDataTable, measure_var::Int, id_vars::Vector{Int};
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, [measure_var], id_vars;
            variable_name=variable_name, value_name=value_name)
end
function stack(dt::AbstractDataTable, measure_vars, id_vars;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(dt, index(dt)[measure_vars], index(dt)[id_vars];
          variable_name=variable_name, value_name=value_name)
end
# no vars specified, by default select only numeric columns
numeric_vars(dt::AbstractDataTable) =
    [T <: AbstractFloat || (T <: Nullable && eltype(T) <: AbstractFloat)
     for T in eltypes(dt)]

function stack(dt::AbstractDataTable, measure_vars = numeric_vars(dt);
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    mv_inds = index(dt)[measure_vars]
    stack(dt, mv_inds, setdiff(1:ncol(dt), mv_inds);
          variable_name=variable_name, value_name=value_name)
end

"""
Stacks a DataTable; convert from a wide to long format; see
`stack`.
"""
function melt(dt::AbstractDataTable, id_vars::@compat(Union{Int,Symbol});
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    melt(dt, [id_vars]; variable_name=variable_name, value_name=value_name)
end
function melt(dt::AbstractDataTable, id_vars;
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    id_inds = index(dt)[id_vars]
    stack(dt, setdiff(1:ncol(dt), id_inds), id_inds;
          variable_name=variable_name, value_name=value_name)
end
function melt(dt::AbstractDataTable, id_vars, measure_vars;
              variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(dt, measure_vars, id_vars; variable_name=variable_name,
          value_name=value_name)
end
melt(dt::AbstractDataTable; variable_name::Symbol=:variable, value_name::Symbol=:value) =
    stack(dt; variable_name=variable_name, value_name=value_name)

##############################################################################
##
## unstack()
##
##############################################################################

"""
Unstacks a DataTable; convert from a long to wide format

```julia
unstack(dt::AbstractDataTable, rowkey, colkey, value)
unstack(dt::AbstractDataTable, colkey, value)
unstack(dt::AbstractDataTable)
```

### Arguments

* `dt` : the AbstractDataTable to be unstacked

* `rowkey` : the column with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`

* `colkey` : the column holding the column names in wide format,
  defaults to `:variable`

* `value` : the value column, defaults to `:value`

### Result

* `::DataTable` : the wide-format datatable


### Examples

```julia
wide = DataTable(id = 1:12,
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
function unstack(dt::AbstractDataTable, rowkey::Int, colkey::Int, value::Int)
    # `rowkey` integer indicating which column to place along rows
    # `colkey` integer indicating which column to place along column headers
    # `value` integer indicating which column has values
    refkeycol = NullableCategoricalArray(dt[rowkey])
    valuecol = dt[value]
    keycol = NullableCategoricalArray(dt[colkey])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    payload = DataTable(Any[similar_nullable(valuecol, Nrow) for i in 1:Ncol], map(Symbol, levels(keycol)))
    nowarning = true
    for k in 1:nrow(dt)
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
    col = similar_nullable(dt[rowkey], length(levs))
    insert!(payload, 1, copy!(col, levs), _names(dt)[rowkey])
end
unstack(dt::AbstractDataTable, rowkey, colkey, value) =
    unstack(dt, index(dt)[rowkey], index(dt)[colkey], index(dt)[value])

# Version of unstack with just the colkey and value columns provided
unstack(dt::AbstractDataTable, colkey, value) =
    unstack(dt, index(dt)[colkey], index(dt)[value])

function unstack(dt::AbstractDataTable, colkey::Int, value::Int)
    # group on anything not a key or value:
    g = groupby(dt, setdiff(_names(dt), _names(dt)[[colkey, value]]), sort=true)
    groupidxs = [g.idx[g.starts[i]:g.ends[i]] for i in 1:length(g.starts)]
    rowkey = zeros(Int, size(dt, 1))
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] = i
    end
    keycol = NullableCategoricalArray(dt[colkey])
    valuecol = dt[value]
    dt1 = nullable!(dt[g.idx[g.starts], g.cols], g.cols)
    Nrow = length(g)
    Ncol = length(levels(keycol))
    dt2 = DataTable(Any[similar_nullable(valuecol, Nrow) for i in 1:Ncol], map(Symbol, levels(keycol)))
    nowarning = true
    for k in 1:nrow(dt)
        j = Int(CategoricalArrays.order(keycol.pool)[keycol.refs[k]])
        i = rowkey[k]
        if i > 0 && j > 0
            if nowarning && !isnull(dt2[j][i])
                warn("Duplicate entries in unstack at row $k.")
                nowarning = false
            end
            dt2[j][i]  = valuecol[k]
        end
    end
    hcat(dt1, dt2)
end

unstack(dt::AbstractDataTable) = unstack(dt, :id, :variable, :value)


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

function CategoricalArrays.CategoricalArray(v::RepeatedVector)
    res = CategoricalArrays.CategoricalArray(v.parent)
    res.refs = repeat(res.refs, inner = [v.inner], outer = [v.outer])
    res
end

##############################################################################
##
## stackdt()
## meltdt()
## Reshaping using referencing (issue #145), using the above vector types
##
##############################################################################

"""
A stacked view of a DataTable (long format)

Like `stack` and `melt`, but a view is returned rather than data
copies.

```julia
stackdt(dt::AbstractDataTable, [measure_vars], [id_vars];
        variable_name::Symbol=:variable, value_name::Symbol=:value)
meltdt(dt::AbstractDataTable, [id_vars], [measure_vars];
       variable_name::Symbol=:variable, value_name::Symbol=:value)
```

### Arguments

* `dt` : the wide AbstractDataTable

* `measure_vars` : the columns to be stacked (the measurement
  variables), a normal column indexing type, like a Symbol,
  Vector{Symbol}, Int, etc.; for `melt`, defaults to all
  variables that are not `id_vars`

* `id_vars` : the identifier columns that are repeated during
  stacking, a normal column indexing type; for `stack` defaults to all
  variables that are not `measure_vars`

### Result

* `::DataTable` : the long-format datatable with column `:value`
  holding the values of the stacked columns (`measure_vars`), with
  column `:variable` a Vector of Symbols with the `measure_vars` name,
  and with columns for each of the `id_vars`.

The result is a view because the columns are special AbstractVectors
that return indexed views into the original DataTable.

### Examples

```julia
d1 = DataTable(a = repeat([1:3;], inner = [4]),
               b = repeat([1:4;], inner = [3]),
               c = randn(12),
               d = randn(12),
               e = map(string, 'a':'l'))

d1s = stackdt(d1, [:c, :d])
d1s2 = stackdt(d1, [:c, :d], [:a])
d1m = meltdt(d1, [:a, :b, :e])
```

"""
function stackdt(dt::AbstractDataTable, measure_vars::Vector{Int},
                 id_vars::Vector{Int}; variable_name::Symbol=:variable,
                 value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(dt)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataTable(Any[RepeatedVector(_names(dt)[measure_vars], nrow(dt), 1),   # variable
                  StackedVector(Any[dt[:,c] for c in measure_vars]),     # value
                  [RepeatedVector(dt[:,c], 1, N) for c in id_vars]...],     # id_var columns
              cnames)
end
function stackdt(dt::AbstractDataTable, measure_var::Int, id_var::Int;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, [measure_var], [id_var]; variable_name=variable_name,
            value_name=value_name)
end
function stackdt(dt::AbstractDataTable, measure_vars, id_var::Int;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, measure_vars, [id_var]; variable_name=variable_name,
            value_name=value_name)
end
function stackdt(dt::AbstractDataTable, measure_var::Int, id_vars;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, [measure_var], id_vars; variable_name=variable_name,
            value_name=value_name)
end
function stackdt(dt::AbstractDataTable, measure_vars, id_vars;
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, index(dt)[measure_vars], index(dt)[id_vars];
            variable_name=variable_name, value_name=value_name)
end
function stackdt(dt::AbstractDataTable, measure_vars = numeric_vars(dt);
                 variable_name::Symbol=:variable, value_name::Symbol=:value)
    m_inds = index(dt)[measure_vars]
    stackdt(dt, m_inds, setdiff(1:ncol(dt), m_inds);
            variable_name=variable_name, value_name=value_name)
end

"""
A stacked view of a DataTable (long format); see `stackdt`
"""
function meltdt(dt::AbstractDataTable, id_vars; variable_name::Symbol=:variable,
                value_name::Symbol=:value)
    id_inds = index(dt)[id_vars]
    stackdt(dt, setdiff(1:ncol(dt), id_inds), id_inds;
            variable_name=variable_name, value_name=value_name)
end
function meltdt(dt::AbstractDataTable, id_vars, measure_vars;
                variable_name::Symbol=:variable, value_name::Symbol=:value)
    stackdt(dt, measure_vars, id_vars; variable_name=variable_name,
            value_name=value_name)
end
meltdt(dt::AbstractDataTable; variable_name::Symbol=:variable, value_name::Symbol=:value) =
    stackdt(dt; variable_name=variable_name, value_name=value_name)
