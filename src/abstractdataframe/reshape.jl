"""
    stack(df::AbstractDataFrame, [measure_vars], [id_vars];
          variable_name=:variable, value_name=:value,
          view::Bool=false, variable_eltype::Type=String)

Stack a data frame `df`, i.e. convert it from wide to long format.

Return the long-format `DataFrame` with: columns for each of the `id_vars`,
column `value_name` (`:value` by default)
holding the values of the stacked columns (`measure_vars`), and
column `variable_name` (`:variable` by default) a vector holding
the name of the corresponding `measure_vars` variable.

If `view=true` then return a stacked view of a data frame (long format).
The result is a view because the columns are special `AbstractVectors`
that return views into the original data frame.


# Arguments
- `df` : the AbstractDataFrame to be stacked
- `measure_vars` : the columns to be stacked (the measurement variables),
  as a column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
  If neither `measure_vars` or `id_vars` are given, `measure_vars`
  defaults to all floating point columns.
- `id_vars` : the identifier columns that are repeated during stacking,
  as a column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
  Defaults to all variables that are not `measure_vars`
- `variable_name` : the name (`Symbol` or string) of the new stacked column that
  shall hold the names of each of `measure_vars`
- `value_name` : the name (`Symbol` or string) of the new stacked column containing
  the values from each of `measure_vars`
- `view` : whether the stacked data frame should be a view rather than contain
  freshly allocated vectors.
- `variable_eltype` : determines the element type of column `variable_name`.
  By default a `PooledArray{String}` is created.
  If `variable_eltype=Symbol` a `PooledVector{Symbol}` is created,
  and if `variable_eltype=CategoricalValue{String}`
  a `CategoricalArray{String}` is produced (call `using CategoricalArrays` first if needed)
  Passing any other type `T` will produce a `PooledVector{T}` column
  as long as it supports conversion from `String`.
  When `view=true`, a `RepeatedVector{T}` is produced.


# Examples
```julia
d1 = DataFrame(a = repeat([1:3;], inner = [4]),
               b = repeat([1:4;], inner = [3]),
               c = randn(12),
               d = randn(12),
               e = map(string, 'a':'l'))

d1s = stack(d1, [:c, :d])
d1s2 = stack(d1, [:c, :d], [:a])
d1m = stack(d1, Not([:a, :b, :e]))
d1s_name = stack(d1, Not([:a, :b, :e]), variable_name=:somemeasure)
```
"""
function stack(df::AbstractDataFrame,
               measure_vars = findall(col -> eltype(col) <: Union{AbstractFloat, Missing},
                                      eachcol(df)),
               id_vars = Not(measure_vars);
               variable_name::SymbolOrString=:variable,
               value_name::SymbolOrString=:value, view::Bool=false,
               variable_eltype::Type=String)
    variable_name_s = Symbol(variable_name)
    value_name_s = Symbol(value_name)
    # getindex from index returns either Int or AbstractVector{Int}
    mv_tmp = index(df)[measure_vars]
    ints_measure_vars = mv_tmp isa Int ? [mv_tmp] : mv_tmp
    idv_tmp = index(df)[id_vars]
    ints_id_vars = idv_tmp isa Int ? [idv_tmp] : idv_tmp
    if view
        return _stackview(df, ints_measure_vars, ints_id_vars,
                          variable_name=variable_name_s,
                          value_name=value_name_s,
                          variable_eltype=variable_eltype)
    end
    N = length(ints_measure_vars)
    cnames = _names(df)[ints_id_vars]
    push!(cnames, variable_name_s)
    push!(cnames, value_name_s)
    if variable_eltype === Symbol
        catnms = PooledArray(_names(df)[ints_measure_vars])
    elseif variable_eltype === String
        catnms = PooledArray(names(df, ints_measure_vars))
    else
        # this covers CategoricalArray{String} in particular
        # (note that copyto! inserts levels in their order of appearance)
        nms = names(df, ints_measure_vars)
        simnms = similar(nms, variable_eltype)
        catnms = simnms isa Vector ? PooledArray(catnms) : simnms
        copyto!(catnms, nms)
    end
    return DataFrame(AbstractVector[[repeat(df[!, c], outer=N) for c in ints_id_vars]..., # id_var columns
                                    repeat(catnms, inner=nrow(df)),                       # variable
                                    vcat([df[!, c] for c in ints_measure_vars]...)],      # value
                     cnames, copycols=false)
end

function _stackview(df::AbstractDataFrame, measure_vars::AbstractVector{Int},
                    id_vars::AbstractVector{Int}; variable_name::Symbol,
                    value_name::Symbol, variable_eltype::Type)
    N = length(measure_vars)
    cnames = _names(df)[id_vars]
    push!(cnames, variable_name)
    push!(cnames, value_name)
    if variable_eltype === Symbol
        catnms = _names(df)[measure_vars]
    elseif variable_eltype === String
        catnms = names(df, measure_vars)
    else
        # this covers CategoricalArray{String} in particular,
        # as copyto! inserts levels in their order of appearance
        nms = names(df, measure_vars)
        catnms = copyto!(similar(nms, variable_eltype), nms)
    end
    return DataFrame(AbstractVector[[RepeatedVector(df[!, c], 1, N) for c in id_vars]..., # id_var columns
                                    RepeatedVector(catnms, nrow(df), 1),                  # variable
                                    StackedVector(Any[df[!, c] for c in measure_vars])],  # value
                     cnames, copycols=false)
end

"""
    unstack(df::AbstractDataFrame, rowkeys, colkey, value; renamecols::Function=identity)
    unstack(df::AbstractDataFrame, colkey, value; renamecols::Function=identity)
    unstack(df::AbstractDataFrame; renamecols::Function=identity)

Unstack data frame `df`, i.e. convert it from long to wide format.

If `colkey` contains `missing` values then they will be skipped and a warning
will be printed.

If combination of `rowkeys` and `colkey` contains duplicate entries then last
`value` will be retained and a warning will be printed.

# Arguments
- `df` : the AbstractDataFrame to be unstacked
- `rowkeys` : the columns with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`.
  Can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
- `colkey` : the column ($COLUMNINDEX_STR) holding the column names in wide format,
  defaults to `:variable`
- `value` : the value column ($COLUMNINDEX_STR), defaults to `:value`
- `renamecols` : a function called on each unique value in `colkey` which must
                 return the name of the column to be created (typically as a string
                 or a `Symbol`). Duplicate names are not allowed.


# Examples
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
function unstack(df::AbstractDataFrame, rowkey::ColumnIndex, colkey::ColumnIndex,
                 value::ColumnIndex; renamecols::Function=identity)
    refkeycol = categorical(df[!, rowkey])
    droplevels!(refkeycol)
    keycol = categorical(df[!, colkey])
    droplevels!(keycol)
    valuecol = df[!, value]
    return _unstack(df, index(df)[rowkey], index(df)[colkey],
                    keycol, valuecol, refkeycol, renamecols)
end

function _unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int,
                  keycol::CategoricalVector, valuecol::AbstractVector,
                  refkeycol::CategoricalVector, renamecols::Function)
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    hadmissing = false # have we encountered missing in refkeycol
    mask_filled = falses(Nrow+1, Ncol) # has a given [row,col] entry been filled?
    warned_dup = false # have we already printed duplicate entries warning?
    warned_missing = false # have we already printed missing in keycol warning?
    for k in 1:nrow(df)
        kref = keycol.refs[k]
        if kref <= 0 # we have found missing in colkey
            if !warned_missing
                @warn("Missing value in variable :$(_names(df)[colkey]) at row $k. Skipping.")
                warned_missing = true
            end
            continue # skip processing it
        end
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
            i = refkref
        end
        if !warned_dup && mask_filled[i, kref]
            @warn("Duplicate entries in unstack at row $k for key "*
                  "$(refkeycol[k]) and variable $(keycol[k]).")
            warned_dup = true
        end
        unstacked_val[kref][i] = valuecol[k]
        mask_filled[i, kref] = true
    end
    levs = levels(refkeycol)
    # we have to handle a case with missings in refkeycol as levs will skip missing
    col = similar(df[!, rowkey], length(levs) + hadmissing)
    copyto!(col, levs)
    hadmissing && (col[end] = missing)
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(levels(keycol))), copycols=false)
    return insertcols!(df2, 1, _names(df)[rowkey] => col)
end

function unstack(df::AbstractDataFrame, rowkeys, colkey::ColumnIndex,
                 value::ColumnIndex; renamecols::Function=identity)
    rowkey_ints = index(df)[rowkeys]
    @assert rowkey_ints isa AbstractVector{Int}
    length(rowkey_ints) == 0 && throw(ArgumentError("No key column found"))
    length(rowkey_ints) == 1 && return unstack(df, rowkey_ints[1], colkey, value,
                                               renamecols=renamecols)
    g = groupby(df, rowkey_ints, sort=true)
    keycol = categorical(df[!, colkey])
    droplevels!(keycol)
    valuecol = df[!, value]
    return _unstack(df, rowkey_ints, index(df)[colkey], keycol, valuecol, g, renamecols)
end

function unstack(df::AbstractDataFrame, colkey::ColumnIndex, value::ColumnIndex;
                 renamecols::Function=identity)
    colkey_int = index(df)[colkey]
    value_int = index(df)[value]
    return unstack(df, Not(colkey_int, value_int), colkey_int, value_int,
            renamecols=renamecols)
end

unstack(df::AbstractDataFrame; renamecols::Function=identity) =
    unstack(df, :variable, :value, renamecols=renamecols)

function _unstack(df::AbstractDataFrame, rowkeys::AbstractVector{Int},
                  colkey::Int, keycol::CategoricalVector,
                  valuecol::AbstractVector, g::GroupedDataFrame,
                  renamecols::Function)
    idx, starts, ends = g.idx, g.starts, g.ends
    groupidxs = [idx[starts[i]:ends[i]] for i in 1:length(starts)]
    rowkey = zeros(Int, size(df, 1))
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] .= i
    end
    df1 = df[idx[starts], g.cols]
    Nrow = length(g)
    Ncol = length(levels(keycol))
    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    mask_filled = falses(Nrow, Ncol)
    warned_dup = false
    warned_missing = false
    for k in 1:nrow(df)
        kref = keycol.refs[k]
        if kref <= 0
            if !warned_missing
                @warn("Missing value in variable :$(_names(df)[colkey]) at row $k. Skipping.")
                warned_missing = true
            end
            continue
        end
        i = rowkey[k]
        if !warned_dup && mask_filled[i, kref]
            @warn("Duplicate entries in unstack at row $k for key "*
                 "$(tuple((df[k,s] for s in rowkeys)...)) and variable $(keycol[k]).")
            warned_dup = true
        end
        unstacked_val[kref][i] = valuecol[k]
        mask_filled[i, kref] = true
    end
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(levels(keycol))), copycols=false)
    hcat(df1, df2, copycols=false)
end

"""
    StackedVector <: AbstractVector

An `AbstractVector` that is a linear, concatenated view into
another set of AbstractVectors

NOTE: Not exported.

# Constructor
```julia
StackedVector(d::AbstractVector)
```

# Arguments
- `d...` : one or more AbstractVectors

# Examples
```julia
StackedVector(Any[[1,2], [9,10], [11,12]])  # [1,2,9,10,11,12]
```
"""
struct StackedVector{T} <: AbstractVector{T}
    components::Vector{Any}
end

StackedVector(d::AbstractVector) =
    StackedVector{promote_type(map(eltype, d)...)}(d)

function Base.getindex(v::StackedVector{T}, i::Int)::T where T
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
    return v.components[j][k]
end

Base.IndexStyle(::Type{StackedVector}) = Base.IndexLinear()
Base.size(v::StackedVector) = (length(v),)
Base.length(v::StackedVector) = sum(map(length, v.components))
Base.eltype(v::Type{StackedVector{T}}) where {T} = T
Base.similar(v::StackedVector, T::Type, dims::Union{Integer, AbstractUnitRange}...) =
    similar(v.components[1], T, dims...)

CategoricalArrays.CategoricalArray(v::StackedVector) =
    CategoricalArray(v[:]) # could be more efficient


"""
    RepeatedVector{T} <: AbstractVector{T}

An AbstractVector that is a view into another AbstractVector with
repeated elements

NOTE: Not exported.

# Constructor
```julia
RepeatedVector(parent::AbstractVector, inner::Int, outer::Int)
```

# Arguments
- `parent` : the AbstractVector that's repeated
- `inner` : the numer of times each element is repeated
- `outer` : the numer of times the whole vector is repeated after
  expanded by `inner`

`inner` and `outer` have the same meaning as similarly named arguments
to `repeat`.

# Examples
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

Base.parent(v::RepeatedVector) = v.parent
DataAPI.levels(v::RepeatedVector) = levels(parent(v))
CategoricalArrays.isordered(v::RepeatedVector{<:Union{CategoricalValue, Missing}}) =
    isordered(parent(v))

function Base.getindex(v::RepeatedVector, i::Int)
    N = length(parent(v))
    idx = Base.fld1(mod1(i,v.inner*N),v.inner)
    parent(v)[idx]
end

Base.IndexStyle(::Type{<:RepeatedVector}) = Base.IndexLinear()
Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.inner * v.outer * length(parent(v))
Base.eltype(v::Type{RepeatedVector{T}}) where {T} = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(parent(v)), v.inner, v.outer)
Base.similar(v::RepeatedVector, T::Type, dims::Dims) = similar(parent(v), T, dims)
Base.unique(v::RepeatedVector) = unique(parent(v))

function CategoricalArrays.CategoricalArray(v::RepeatedVector)
    res = CategoricalArray(parent(v), levels=levels(parent(v)))
    res.refs = repeat(res.refs, inner = [v.inner], outer = [v.outer])
    res
end


Base.transpose(::AbstractDataFrame, args...; kwargs...) =
    MethodError("`transpose` not defined for `AbstractDataFrame`s. Try `permutedims` instead")

"""
    permutedims(df::AbstractDataFrame, src_namescol::Union{Int, Symbol, <:AbstractString},
                dest_namescol::Union{Symbol,AbstractString};
                makeunique::Bool=false)
    permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex; makeunique::Bool=false)
    permutedims(df::AbstractDataFrame; makeunique::Bool=false)

Turn `df` on its side such that rows become columns
and the column indexed by `src_namescol` becomes the names of new columns.
In the resulting `DataFrame`, column names of `df` will become the first column
with name specified by `dest_namescol`.

# Arguments
- `df` : the `AbstractDataFrame`
- `src_namescol` : the column that will become the new header.
  This column eltype must be `<: Union{String, Symbol}`.
  Defaults to first column.
- `dest_namescol` : the name of the first column in the returned `DataFrame`.
  Defaults to the same name as `src_namescol`.
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

Note: The eltypes of columns in resulting `DataFrame`
(other than the first column, which always has eltype `String`)
will depend on the eltypes of _all_ input columns
based on the results of `prote_type`.
That is, if the source `DataFrame` contains `String` and `Int` columns,
resulting columns will have eltype `Any`.
If the source has a mix of numeric types (eg. `Float64` and `Int`),
columns in resulting `DataFrame` will be promoted to `Float64`.

# Examples

```jldoctest
julia> df1 = DataFrame(a=["x", "y"], b=[1.,2.], c=[3,4], d=[true,false])
2×4 DataFrame
│ Row │ a      │ b       │ c     │ d    │
│     │ String │ Float64 │ Int64 │ Bool │
├─────┼────────┼─────────┼───────┼──────┤
│ 1   │ x      │ 1.0     │ 3     │ 1    │
│ 2   │ y      │ 2.0     │ 4     │ 0    │

julia> df2 = DataFrame(a=["x", "y"], b=[1, "two"], c=[3,4], d=[true,false])
2×4 DataFrame
│ Row │ a      │ b   │ c     │ d    │
│     │ String │ Any │ Int64 │ Bool │
├─────┼────────┼─────┼───────┼──────┤
│ 1   │ x      │ 1   │ 3     │ 1    │
│ 2   │ y      │ two │ 4     │ 0    │

julia> permutedims(df1) # note the column types
3×3 DataFrame
│ Row │ a      │ x       │ y       │
│     │ String │ Float64 │ Float64 │
├─────┼────────┼─────────┼─────────┤
│ 1   │ b      │ 1.0     │ 2.0     │
│ 2   │ c      │ 3.0     │ 4.0     │
│ 3   │ d      │ 1.0     │ 0.0     │

julia> permutedims(df2)
3×3 DataFrame
│ Row │ a      │ x   │ y   │
│     │ String │ Any │ Any │
├─────┼────────┼─────┼─────┤
│ 1   │ b      │ 1   │ two │
│ 2   │ c      │ 3   │ 4   │
│ 3   │ d      │ 1   │ 0   │
```
"""
function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex,
                          dest_namescol::Union{Symbol, <:AbstractString};
                          makeunique::Bool=false)

    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(df, src_namescol))
    end
    eltype(df[!, src_namescol]) <: SymbolOrString ||
        throw(ArgumentError("src_namescol must have eltype `Symbol` or `<:AbstractString`"))

    df_notsrc = df[!, Not(src_namescol)]
    df_permuted = DataFrame(dest_namescol => names(df_notsrc))

    if ncol(df_notsrc) == 0
        df_tmp = DataFrame((n=>[] for n in df[!, src_namescol])...)
    else
        m = permutedims(Matrix(df_notsrc))
        df_tmp = rename!(DataFrame(Tables.table(m)), df[!, src_namescol], makeunique=makeunique)
    end
    return hcat!(df_permuted, df_tmp, copycols=false)
end

function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex;
                          makeunique::Bool=false)
    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(df, src_namescol))
        dest_namescol = _names(df)[src_namescol]
    else
        dest_namescol = src_namescol
    end
    return permutedims(df, src_namescol, dest_namescol; makeunique=makeunique)
end

Base.permutedims(df::AbstractDataFrame; makeunique::Bool=false) =
    permutedims(df, 1; makeunique=makeunique)
