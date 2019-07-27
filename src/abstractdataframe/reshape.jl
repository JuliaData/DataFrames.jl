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
function stack(df::AbstractDataFrame, measure_vars::AbstractVector{<:Integer},
               id_vars::AbstractVector{<:Integer}; variable_name::Symbol=:variable,
               value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataFrame(AbstractVector[repeat(_names(df)[measure_vars], inner=nrow(df)), # variable
                             vcat([df[!, c] for c in measure_vars]...),           # value
                             [repeat(df[!, c], outer=N) for c in id_vars]...],    # id_var columns
              cnames, copycols=false)
end
function stack(df::AbstractDataFrame, measure_var::Int, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, [measure_var], [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_vars::AbstractVector{<:Integer}, id_var::Int;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, measure_vars, [id_var];
          variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_var::Int, id_vars::AbstractVector{<:Integer};
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, [measure_var], id_vars;
          variable_name=variable_name, value_name=value_name)
end
function stack(df::AbstractDataFrame, measure_vars, id_vars;
               variable_name::Symbol=:variable, value_name::Symbol=:value)
    stack(df, index(df)[measure_vars], index(df)[id_vars];
          variable_name=variable_name, value_name=value_name)
end
# no vars specified, by default select only numeric columns
numeric_vars(df::AbstractDataFrame) =
    [T <: AbstractFloat || (T >: Missing && Missings.T(T) <: AbstractFloat)
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
function melt(df::AbstractDataFrame, id_vars::ColumnIndex;
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
unstack(df::AbstractDataFrame, rowkeys::Union{Integer, Symbol},
        colkey::Union{Integer, Symbol}, value::Union{Integer, Symbol};
        renamecols::Function=identity)
unstack(df::AbstractDataFrame, rowkeys::AbstractVector{<:Union{Integer, Symbol}},
        colkey::Union{Integer, Symbol}, value::Union{Integer, Symbol};
        renamecols::Function=identity)
unstack(df::AbstractDataFrame, colkey::Union{Integer, Symbol},
        value::Union{Integer, Symbol}; renamecols::Function=identity)
unstack(df::AbstractDataFrame; renamecols::Function=identity)
```

### Arguments

* `df` : the AbstractDataFrame to be unstacked

* `rowkeys` : the column(s) with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`

* `colkey` : the column holding the column names in wide format,
  defaults to `:variable`

* `value` : the value column, defaults to `:value`

* `renamecols` : a function called on each unique value in `colkey` which must
                 return the name of the column to be created (typically as a string
                 or a `Symbol`). Duplicate names are not allowed.

### Result

* `::DataFrame` : the wide-format DataFrame

If `colkey` contains `missing` values then they will be skipped and a warning will be printed.

If combination of `rowkeys` and `colkey` contains duplicate entries then last `value` will
be retained and a warning will be printed.

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
wide3 = unstack(long, [:id, :a], :variable, :value)
wide4 = unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x))
```
Note that there are some differences between the widened results above.
"""
function unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int, value::Int;
                 renamecols::Function=identity)
    refkeycol = categorical(df[!, rowkey])
    droplevels!(refkeycol)
    keycol = categorical(df[!, colkey])
    droplevels!(keycol)
    valuecol = df[!, value]
    _unstack(df, rowkey, colkey, value, keycol, valuecol, refkeycol, renamecols)
end

function _unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int, value::Int,
                  keycol, valuecol, refkeycol, renamecols::Function)
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    hadmissing = false # have we encountered missing in refkeycol
    mask_filled = falses(Nrow+1, Ncol) # has a given [row,col] entry been filled?
    warned_dup = false # have we already printed duplicate entries warning?
    warned_missing = false # have we already printed missing in keycol warning?
    keycol_order = Vector{Int}(CategoricalArrays.order(keycol.pool))
    refkeycol_order = Vector{Int}(CategoricalArrays.order(refkeycol.pool))
    for k in 1:nrow(df)
        kref = keycol.refs[k]
        if kref <= 0 # we have found missing in colkey
            if !warned_missing
                @warn("Missing value in variable $(_names(df)[colkey]) at row $k. Skipping.")
                warned_missing = true
            end
            continue # skip processing it
        end
        j = keycol_order[kref]
        refkref = refkeycol.refs[k]
        if refkref <= 0 # we have found missing in rowkey
            if !hadmissing # if it is the first time we have to add a new row
                hadmissing = true
                # we use the fact that missing is greater than anything
                for i in eachindex(unstacked_val)
                    push!(unstacked_val[i], missing)
                end
            end
            i = length(unstacked_val[1])
        else
            i = refkeycol_order[refkref]
        end
        if !warned_dup && mask_filled[i, j]
            @warn("Duplicate entries in unstack at row $k for key "*
                  "$(refkeycol[k]) and variable $(keycol[k]).")
            warned_dup = true
        end
        unstacked_val[j][i] = valuecol[k]
        mask_filled[i, j] = true
    end
    levs = levels(refkeycol)
    # we have to handle a case with missings in refkeycol as levs will skip missing
    col = similar(df[!, rowkey], length(levs) + hadmissing)
    copyto!(col, levs)
    hadmissing && (col[end] = missing)
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(levels(keycol))), copycols=false)
    insertcols!(df2, 1, _names(df)[rowkey] => col)
end

unstack(df::AbstractDataFrame, rowkey::ColumnIndex, colkey::Int, value::Int;
        renamecols::Function=identity) =
    unstack(df, index(df)[rowkey], colkey, value, renamecols=renamecols)

# Version of unstack with just the colkey and value columns provided
unstack(df::AbstractDataFrame, colkey::ColumnIndex, value::ColumnIndex;
        renamecols::Function=identity) =
    unstack(df, index(df)[colkey], index(df)[value], renamecols=renamecols)

# group on anything not a key or value
unstack(df::AbstractDataFrame, colkey::Int, value::Int; renamecols::Function=identity) =
    unstack(df, setdiff(_names(df), _names(df)[[colkey, value]]), colkey, value,
            renamecols=renamecols)

unstack(df::AbstractDataFrame, rowkeys, colkey::ColumnIndex, value::ColumnIndex;
        renamecols::Function=identity) =
    unstack(df, rowkeys, index(df)[colkey], index(df)[value], renamecols=renamecols)

unstack(df::AbstractDataFrame, rowkeys, colkey::Int, value::Int;
        renamecols::Function=identity) =
    unstack(df, names(df)[index(df)[rowkeys]], colkey, value, renamecols=renamecols)

unstack(df::AbstractDataFrame, rowkeys::AbstractVector{<:Integer}, colkey::Int,
        value::Int; renamecols::Function=identity) =
    unstack(df, names(df)[rowkeys], colkey, value, renamecols=renamecols)

function unstack(df::AbstractDataFrame, rowkeys::AbstractVector{Symbol}, colkey::Int,
                 value::Int; renamecols::Function=identity)
    length(rowkeys) == 0 && throw(ArgumentError("No key column found"))
    length(rowkeys) == 1 && return unstack(df, rowkeys[1], colkey, value, renamecols=renamecols)
    g = groupby(df, rowkeys, sort=true)
    keycol = categorical(df[!, colkey])
    droplevels!(keycol)
    valuecol = df[!, value]
    _unstack(df, rowkeys, colkey, value, keycol, valuecol, g, renamecols)
end

function _unstack(df::AbstractDataFrame, rowkeys::AbstractVector{Symbol},
                  colkey::Int, value::Int, keycol, valuecol, g, renamecols)
    groupidxs = [g.idx[g.starts[i]:g.ends[i]] for i in 1:length(g.starts)]
    rowkey = zeros(Int, size(df, 1))
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] .= i
    end
    df1 = df[g.idx[g.starts], g.cols]
    Nrow = length(g)
    Ncol = length(levels(keycol))
    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    mask_filled = falses(Nrow, Ncol)
    warned_dup = false
    warned_missing = false
    keycol_order = Vector{Int}(CategoricalArrays.order(keycol.pool))
    for k in 1:nrow(df)
        kref = keycol.refs[k]
        if kref <= 0
            if !warned_missing
                @warn("Missing value in variable $(_names(df)[colkey]) at row $k. Skipping.")
                warned_missing = true
            end
            continue
        end
        j = keycol_order[kref]
        i = rowkey[k]
        if !warned_dup && mask_filled[i, j]
            @warn("Duplicate entries in unstack at row $k for key "*
                 "$(tuple((df[1,s] for s in rowkeys)...)) and variable $(keycol[k]).")
            warned_dup = true
        end
        unstacked_val[j][i] = valuecol[k]
        mask_filled[i, j] = true
    end
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(levels(keycol))), copycols=false)
    hcat(df1, df2, copycols=false)
end

unstack(df::AbstractDataFrame; renamecols::Function=identity) =
    unstack(df, :variable, :value, renamecols=renamecols)

##############################################################################
##
## Reshaping using referencing (issue #145)
## New AbstractVector types (all read only):
##     StackedVector
##     RepeatedVector
##
##############################################################################

"""
    StackedVector <: AbstractVector{Any}

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
struct StackedVector <: AbstractVector{Any}
    components::Vector{Any}
end

function Base.getindex(v::StackedVector,i::Int)
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

Base.IndexStyle(::Type{StackedVector}) = Base.IndexLinear()
Base.size(v::StackedVector) = (length(v),)
Base.length(v::StackedVector) = sum(map(length, v.components))
Base.eltype(v::StackedVector) = promote_type(map(eltype, v.components)...)
Base.similar(v::StackedVector, T::Type, dims::Union{Integer, AbstractUnitRange}...) =
    similar(v.components[1], T, dims...)

CategoricalArrays.CategoricalArray(v::StackedVector) = CategoricalArray(v[:]) # could be more efficient


"""
    RepeatedVector{T} <: AbstractVector{T}

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
struct RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    inner::Int
    outer::Int
end

function Base.getindex(v::RepeatedVector, i::Int)
    N = length(v.parent)
    idx = Base.fld1(mod1(i,v.inner*N),v.inner)
    v.parent[idx]
end

Base.IndexStyle(::Type{<:RepeatedVector}) = Base.IndexLinear()
Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.inner * v.outer * length(v.parent)
Base.eltype(v::RepeatedVector{T}) where {T} = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(v.parent), v.inner, v.outer)
Base.similar(v::RepeatedVector, T::Type, dims::Dims) = similar(v.parent, T, dims)
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
function stackdf(df::AbstractDataFrame, measure_vars::AbstractVector{<:Integer},
                 id_vars::AbstractVector{<:Integer}; variable_name::Symbol=:variable,
                 value_name::Symbol=:value)
    N = length(measure_vars)
    cnames = names(df)[id_vars]
    insert!(cnames, 1, value_name)
    insert!(cnames, 1, variable_name)
    DataFrame(AbstractVector[RepeatedVector(_names(df)[measure_vars], nrow(df), 1), # variable
                             StackedVector(Any[df[!, c] for c in measure_vars]),       # value
                             [RepeatedVector(df[!, c], 1, N) for c in id_vars]...],    # id_var columns
              cnames, copycols=false)
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
