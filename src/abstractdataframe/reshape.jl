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
julia> df = DataFrame(a = repeat([1:3;], inner = [2]),
                      b = repeat([1:2;], inner = [3]),
                      c = randn(6),
                      d = randn(),
                      e = map(string, 'a':'f'))
6×5 DataFrame
│ Row │ a     │ b     │ c        │ d        │ e      │
│     │ Int64 │ Int64 │ Float64  │ Float64  │ String │
├─────┼───────┼───────┼──────────┼──────────┼────────┤
│ 1   │ 1     │ 1     │ -1.1078  │ 0.680175 │ a      │
│ 2   │ 1     │ 1     │ 0.078634 │ 0.680175 │ b      │
│ 3   │ 2     │ 1     │ -1.47615 │ 0.680175 │ c      │
│ 4   │ 2     │ 2     │ 0.826434 │ 0.680175 │ d      │
│ 5   │ 3     │ 2     │ 0.597258 │ 0.680175 │ e      │
│ 6   │ 3     │ 2     │ 1.49645  │ 0.680175 │ f      │

julia> stack(df, [:c, :d])
12×5 DataFrame
│ Row │ a     │ b     │ e      │ variable │ value    │
│     │ Int64 │ Int64 │ String │ String   │ Float64  │
├─────┼───────┼───────┼────────┼──────────┼──────────┤
│ 1   │ 1     │ 1     │ a      │ c        │ -1.1078  │
│ 2   │ 1     │ 1     │ b      │ c        │ 0.078634 │
│ 3   │ 2     │ 1     │ c      │ c        │ -1.47615 │
│ 4   │ 2     │ 2     │ d      │ c        │ 0.826434 │
│ 5   │ 3     │ 2     │ e      │ c        │ 0.597258 │
│ 6   │ 3     │ 2     │ f      │ c        │ 1.49645  │
│ 7   │ 1     │ 1     │ a      │ d        │ 0.680175 │
│ 8   │ 1     │ 1     │ b      │ d        │ 0.680175 │
│ 9   │ 2     │ 1     │ c      │ d        │ 0.680175 │
│ 10  │ 2     │ 2     │ d      │ d        │ 0.680175 │
│ 11  │ 3     │ 2     │ e      │ d        │ 0.680175 │
│ 12  │ 3     │ 2     │ f      │ d        │ 0.680175 │

julia> stack(df, [:c, :d], [:a])
12×3 DataFrame
│ Row │ a     │ variable │ value    │
│     │ Int64 │ String   │ Float64  │
├─────┼───────┼──────────┼──────────┤
│ 1   │ 1     │ c        │ -1.1078  │
│ 2   │ 1     │ c        │ 0.078634 │
│ 3   │ 2     │ c        │ -1.47615 │
│ 4   │ 2     │ c        │ 0.826434 │
│ 5   │ 3     │ c        │ 0.597258 │
│ 6   │ 3     │ c        │ 1.49645  │
│ 7   │ 1     │ d        │ 0.680175 │
│ 8   │ 1     │ d        │ 0.680175 │
│ 9   │ 2     │ d        │ 0.680175 │
│ 10  │ 2     │ d        │ 0.680175 │
│ 11  │ 3     │ d        │ 0.680175 │
│ 12  │ 3     │ d        │ 0.680175 │

julia> stack(df, Not([:a, :b, :e]))
12×5 DataFrame
│ Row │ a     │ b     │ e      │ variable │ value    │
│     │ Int64 │ Int64 │ String │ String   │ Float64  │
├─────┼───────┼───────┼────────┼──────────┼──────────┤
│ 1   │ 1     │ 1     │ a      │ c        │ -1.1078  │
│ 2   │ 1     │ 1     │ b      │ c        │ 0.078634 │
│ 3   │ 2     │ 1     │ c      │ c        │ -1.47615 │
│ 4   │ 2     │ 2     │ d      │ c        │ 0.826434 │
│ 5   │ 3     │ 2     │ e      │ c        │ 0.597258 │
│ 6   │ 3     │ 2     │ f      │ c        │ 1.49645  │
│ 7   │ 1     │ 1     │ a      │ d        │ 0.680175 │
│ 8   │ 1     │ 1     │ b      │ d        │ 0.680175 │
│ 9   │ 2     │ 1     │ c      │ d        │ 0.680175 │
│ 10  │ 2     │ 2     │ d      │ d        │ 0.680175 │
│ 11  │ 3     │ 2     │ e      │ d        │ 0.680175 │
│ 12  │ 3     │ 2     │ f      │ d        │ 0.680175 │

julia> stack(df, Not([:a, :b, :e]), variable_name=:somemeasure)
12×5 DataFrame
│ Row │ a     │ b     │ e      │ somemeasure │ value    │
│     │ Int64 │ Int64 │ String │ String      │ Float64  │
├─────┼───────┼───────┼────────┼─────────────┼──────────┤
│ 1   │ 1     │ 1     │ a      │ c           │ -1.1078  │
│ 2   │ 1     │ 1     │ b      │ c           │ 0.078634 │
│ 3   │ 2     │ 1     │ c      │ c           │ -1.47615 │
│ 4   │ 2     │ 2     │ d      │ c           │ 0.826434 │
│ 5   │ 3     │ 2     │ e      │ c           │ 0.597258 │
│ 6   │ 3     │ 2     │ f      │ c           │ 1.49645  │
│ 7   │ 1     │ 1     │ a      │ d           │ 0.680175 │
│ 8   │ 1     │ 1     │ b      │ d           │ 0.680175 │
│ 9   │ 2     │ 1     │ c      │ d           │ 0.680175 │
│ 10  │ 2     │ 2     │ d      │ d           │ 0.680175 │
│ 11  │ 3     │ 2     │ e      │ d           │ 0.680175 │
│ 12  │ 3     │ 2     │ f      │ d           │ 0.680175 │
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
    unstack(df::AbstractDataFrame, rowkeys, colkey, value; renamecols::Function=identity,
            allowmissing::Bool=false, allowduplicates::Bool=false)
    unstack(df::AbstractDataFrame, colkey, value; renamecols::Function=identity,
            allowmissing::Bool=false, allowduplicates::Bool=false)
    unstack(df::AbstractDataFrame; renamecols::Function=identity,
            allowmissing::Bool=false, allowduplicates::Bool=false)

Unstack data frame `df`, i.e. convert it from long to wide format.

Row keys and values from value column will be sorted by default unless they are
not ordered (i.e. passing them to `sort` fails) in which case the order of the
result is unspecified.

# Positional arguments
- `df` : the AbstractDataFrame to be unstacked
- `rowkeys` : the columns with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`.
  Can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
- `colkey` : the column ($COLUMNINDEX_STR) holding the column names in wide format,
  defaults to `:variable`
- `value` : the value column ($COLUMNINDEX_STR), defaults to `:value`

# Keyword arguments

`renamecols` is a function called on each unique value in `colkey` which must
return the name of the column to be created (typically as a string or a
`Symbol`). Duplicates in resulting names when converted to `Symbol` are not allowed.

If `colkey` contains `missing` values then they will be included  if
`allowmissing=true` and an error will be thrown otherwise (the default).

If combination of `rowkeys` and `colkey` contains duplicate entries then last
`value` will be retained and a warning will be printed if `allowduplicates=true`
and an error will be thrown otherwise (the default).

# Examples

```julia
julia> wide = DataFrame(id = 1:6,
                        a  = repeat([1:3;], inner = [2]),
                        b  = repeat([1:2;], inner = [3]),
                        c  = randn(6),
                        d  = randn(6))
6×5 DataFrame
│ Row │ id    │ a     │ b     │ c         │ d         │
│     │ Int64 │ Int64 │ Int64 │ Float64   │ Float64   │
├─────┼───────┼───────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1     │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ 1     │ 1     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 2     │ 1     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 2     │ 2     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 3     │ 2     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ 3     │ 2     │ -0.172475 │ -2.81039  │

julia> long = stack(wide)
12×5 DataFrame
│ Row │ id    │ a     │ b     │ variable │ value     │
│     │ Int64 │ Int64 │ Int64 │ String   │ Float64   │
├─────┼───────┼───────┼───────┼──────────┼───────────┤
│ 1   │ 1     │ 1     │ 1     │ c        │ 1.20649   │
│ 2   │ 2     │ 1     │ 1     │ c        │ -0.917794 │
│ 3   │ 3     │ 2     │ 1     │ c        │ 0.309629  │
│ 4   │ 4     │ 2     │ 2     │ c        │ 1.46677   │
│ 5   │ 5     │ 3     │ 2     │ c        │ 1.04339   │
│ 6   │ 6     │ 3     │ 2     │ c        │ -0.172475 │
│ 7   │ 1     │ 1     │ 1     │ d        │ -1.27628  │
│ 8   │ 2     │ 1     │ 1     │ d        │ 0.940007  │
│ 9   │ 3     │ 2     │ 1     │ d        │ 0.820397  │
│ 10  │ 4     │ 2     │ 2     │ d        │ -1.03457  │
│ 11  │ 5     │ 3     │ 2     │ d        │ -0.770464 │
│ 12  │ 6     │ 3     │ 2     │ d        │ -2.81039  │

julia> unstack(long)
6×5 DataFrame
│ Row │ id    │ a     │ b     │ c         │ d         │
│     │ Int64 │ Int64 │ Int64 │ Float64?  │ Float64?  │
├─────┼───────┼───────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1     │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ 1     │ 1     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 2     │ 1     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 2     │ 2     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 3     │ 2     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ 3     │ 2     │ -0.172475 │ -2.81039  │

julia> unstack(long, :variable, :value)
6×5 DataFrame
│ Row │ id    │ a     │ b     │ c         │ d         │
│     │ Int64 │ Int64 │ Int64 │ Float64?  │ Float64?  │
├─────┼───────┼───────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1     │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ 1     │ 1     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 2     │ 1     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 2     │ 2     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 3     │ 2     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ 3     │ 2     │ -0.172475 │ -2.81039  │

julia> unstack(long, :id, :variable, :value)
6×3 DataFrame
│ Row │ id    │ c         │ d         │
│     │ Int64 │ Float64?  │ Float64?  │
├─────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ -0.172475 │ -2.81039  │

julia> unstack(long, [:id, :a], :variable, :value)
6×4 DataFrame
│ Row │ id    │ a     │ c         │ d         │
│     │ Int64 │ Int64 │ Float64?  │ Float64?  │
├─────┼───────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ 1     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 2     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 2     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 3     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ 3     │ -0.172475 │ -2.81039  │

julia> unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x))
6×3 DataFrame
│ Row │ id    │ _c        │ _d        │
│     │ Int64 │ Float64?  │ Float64?  │
├─────┼───────┼───────────┼───────────┤
│ 1   │ 1     │ 1.20649   │ -1.27628  │
│ 2   │ 2     │ -0.917794 │ 0.940007  │
│ 3   │ 3     │ 0.309629  │ 0.820397  │
│ 4   │ 4     │ 1.46677   │ -1.03457  │
│ 5   │ 5     │ 1.04339   │ -0.770464 │
│ 6   │ 6     │ -0.172475 │ -2.81039  │
```
Note that there are some differences between the widened results above.
"""
function unstack(df::AbstractDataFrame, rowkey::ColumnIndex, colkey::ColumnIndex,
                 value::ColumnIndex; renamecols::Function=identity,
                 allowmissing::Bool=false, allowduplicates::Bool=false)
    refkeycol = df[!, rowkey]
    keycol = df[!, colkey]
    valuecol = df[!, value]
    return _unstack(df, index(df)[rowkey], index(df)[colkey],
                    keycol, valuecol, refkeycol, renamecols, allowmissing, allowduplicates)
end

function _unstack_preprocess_vector(v::AbstractVector)
    v_unique = unique(v)
    had_missing = any(ismissing, v_unique)
    v_unique = intersect(levels(v), v_unique)
    had_missing && (v_unique = vcat(v_unique, [missing]))
    len_v = length(v_unique)
    v_map = Dict([x => i for (i,x) in enumerate(v_unique)])
    # both unique and Dict should use isequal to test for identity of values
    @assert length(v_map) == length(v_unique)
    # if there are no missings in v then set reference index of missing to 0
    col = similar(v, length(v_unique))
    copyto!(col, v_unique)
    return col, v_map, get(v_map, missing, 0)
end

function _unstack(df::AbstractDataFrame, rowkey::Int, colkey::Int,
                  keycol::AbstractVector, valuecol::AbstractVector,
                  refkeycol::AbstractVector, renamecols::Function,
                  allowmissing::Bool, allowduplicates::Bool)
    col, refkeycol_map, refkeycol_missing = _unstack_preprocess_vector(refkeycol)
    Nrow = length(refkeycol_map)
    colnames, keycol_map, keycol_missing = _unstack_preprocess_vector(keycol)
    Ncol = length(keycol_map)

    if keycol_missing != 0 && !allowmissing
        throw(ArgumentError("Missing value in variable :$(_names(df)[colkey]). " *
                            "Pass `allowmissing=true` to skip missings."))
    end

    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    mask_filled = falses(Nrow, Ncol) # has a given [row,col] entry been filled?
    for k in 1:nrow(df)
        kref = keycol_map[keycol[k]]
        refkref = refkeycol_map[refkeycol[k]]
        if !allowduplicates && mask_filled[refkref, kref]
            throw(ArgumentError("Duplicate entries in unstack at row $k for key "*
                                "$(refkeycol[k]) and variable $(keycol[k]). " *
                                "Pass allowduplicates=true to allow them."))
        end
        unstacked_val[kref][refkref] = valuecol[k]
        mask_filled[refkref, kref] = true
    end
    # note that Symbol.(renamecols.(colnames)) must produce unique column names
    # and _names(df)[rowkey] must also produce a unique name
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(colnames)), copycols=false)
    return insertcols!(df2, 1, _names(df)[rowkey] => col, copycols=false)
end

function unstack(df::AbstractDataFrame, rowkeys, colkey::ColumnIndex,
                 value::ColumnIndex; renamecols::Function=identity,
                 allowmissing::Bool=false, allowduplicates::Bool=false)
    rowkey_ints = index(df)[rowkeys]
    @assert rowkey_ints isa AbstractVector{Int}
    length(rowkey_ints) == 0 && throw(ArgumentError("No key column found"))
    length(rowkey_ints) == 1 && return unstack(df, rowkey_ints[1], colkey, value,
                                               renamecols=renamecols,
                                               allowmissing=allowmissing,
                                               allowduplicates=allowduplicates)
    local g
    try
        g = groupby(df, rowkey_ints, sort=true)
    catch
        g = groupby(df, rowkey_ints, sort=false)
    end
    keycol = df[!, colkey]
    valuecol = df[!, value]
    return _unstack(df, rowkey_ints, index(df)[colkey], keycol, valuecol, g,
                    renamecols, allowmissing, allowduplicates)
end

function unstack(df::AbstractDataFrame, colkey::ColumnIndex, value::ColumnIndex;
                 renamecols::Function=identity,
                 allowmissing::Bool=false, allowduplicates::Bool=false)
    colkey_int = index(df)[colkey]
    value_int = index(df)[value]
    return unstack(df, Not(colkey_int, value_int), colkey_int, value_int,
            renamecols=renamecols, allowmissing=allowmissing,
            allowduplicates=allowduplicates)
end

unstack(df::AbstractDataFrame; renamecols::Function=identity,
        allowmissing::Bool=false, allowduplicates::Bool=false) =
    unstack(df, :variable, :value, renamecols=renamecols, allowmissing=allowmissing,
            allowduplicates=allowduplicates)

function _unstack(df::AbstractDataFrame, rowkeys::AbstractVector{Int},
                  colkey::Int, keycol::AbstractVector,
                  valuecol::AbstractVector, g::GroupedDataFrame,
                  renamecols::Function,
                  allowmissing::Bool, allowduplicates::Bool)
    idx, starts, ends = g.idx, g.starts, g.ends
    groupidxs = [idx[starts[i]:ends[i]] for i in 1:length(starts)]
    rowkey = zeros(Int, size(df, 1))
    for i in 1:length(groupidxs)
        rowkey[groupidxs[i]] .= i
    end
    df1 = df[idx[starts], g.cols]
    Nrow = length(g)

    colnames, keycol_map, keycol_missing = _unstack_preprocess_vector(keycol)
    Ncol = length(keycol_map)

    if keycol_missing != 0 && !allowmissing
        throw(ArgumentError("Missing value in variable :$(_names(df)[colkey])." *
                            " Pass `allowmissing=true` to skip missings."))
    end

    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    mask_filled = falses(Nrow, Ncol)
    for k in 1:nrow(df)
        kref = keycol_map[keycol[k]]
        i = rowkey[k]
        if !allowduplicates && mask_filled[i, kref]
            throw(ArgumentError("Duplicate entries in unstack at row $k for key "*
                                "$(tuple((df[k,s] for s in rowkeys)...)) and variable $(keycol[k]). " *
                                "Pass allowduplicates=true to allow them."))
        end
        unstacked_val[kref][i] = valuecol[k]
        mask_filled[i, kref] = true
    end
    # note that Symbol.(renamecols.(colnames)) must produce unique column names
    # and names between df1 and df2 must be unique
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(colnames)), copycols=false)
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

# TODO: uncomment when DataAPI.jl supports this
# DataAPI.isordered(v::RepeatedVector) =
#     isordered(parent(v))

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

# TODO: @nalimilan: is there a generic way to support this?
# function CategoricalArrays.CategoricalArray(v::RepeatedVector)
#     res = CategoricalArray(parent(v), levels=levels(parent(v)))
#     res.refs = repeat(res.refs, inner = [v.inner], outer = [v.outer])
#     res
# end

Base.transpose(::AbstractDataFrame, args...; kwargs...) =
    MethodError("`transpose` not defined for `AbstractDataFrame`s. Try `permutedims` instead")

"""
    permutedims(df::AbstractDataFrame, src_namescol::Union{Int, Symbol, AbstractString},
                [dest_namescol::Union{Symbol, AbstractString}];
                makeunique::Bool=false)

Turn `df` on its side such that rows become columns
and values in the column indexed by `src_namescol` become the names of new columns.
In the resulting `DataFrame`, column names of `df` will become the first column
with name specified by `dest_namescol`.

# Arguments
- `df` : the `AbstractDataFrame`
- `src_namescol` : the column that will become the new header.
  This column's element type must be `AbstractString` or `Symbol`.
- `dest_namescol` : the name of the first column in the returned `DataFrame`.
  Defaults to the same name as `src_namescol`.
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).

Note: The element types of columns in resulting `DataFrame`
(other than the first column, which always has element type `String`)
will depend on the element types of _all_ input columns
based on the result of `promote_type`.
That is, if the source data frame contains `Int` and `Float64` columns,
resulting columns will have element type `Float64`. If the source has
`Int` and `String` columns, resulting columns will have element type `Any`.

# Examples

```jldoctest
julia> df1 = DataFrame(a=["x", "y"], b=[1., 2.], c=[3, 4], d=[true,false])
2×4 DataFrame
│ Row │ a      │ b       │ c     │ d    │
│     │ String │ Float64 │ Int64 │ Bool │
├─────┼────────┼─────────┼───────┼──────┤
│ 1   │ x      │ 1.0     │ 3     │ 1    │
│ 2   │ y      │ 2.0     │ 4     │ 0    │

julia> permutedims(df1, 1) # note the column types
3×3 DataFrame
│ Row │ a      │ x       │ y       │
│     │ String │ Float64 │ Float64 │
├─────┼────────┼─────────┼─────────┤
│ 1   │ b      │ 1.0     │ 2.0     │
│ 2   │ c      │ 3.0     │ 4.0     │
│ 3   │ d      │ 1.0     │ 0.0     │

julia> df2 = DataFrame(a=["x", "y"], b=[1, "two"], c=[3, 4], d=[true, false])
2×4 DataFrame
│ Row │ a      │ b   │ c     │ d    │
│     │ String │ Any │ Int64 │ Bool │
├─────┼────────┼─────┼───────┼──────┤
│ 1   │ x      │ 1   │ 3     │ 1    │
│ 2   │ y      │ two │ 4     │ 0    │

julia> permutedims(df2, 1, "different_name")
3×3 DataFrame
│ Row │ different_name │ x   │ y   │
│     │ String         │ Any │ Any │
├─────┼────────────────┼─────┼─────┤
│ 1   │ b              │ 1   │ two │
│ 2   │ c              │ 3   │ 4   │
│ 3   │ d              │ 1   │ 0   │
```
"""
function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex,
                          dest_namescol::Union{Symbol, AbstractString};
                          makeunique::Bool=false)

    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(index(df), src_namescol))
    end
    eltype(df[!, src_namescol]) <: SymbolOrString ||
        throw(ArgumentError("src_namescol must have eltype `Symbol` or `<:AbstractString`"))

    df_notsrc = df[!, Not(src_namescol)]
    df_permuted = DataFrame(dest_namescol => names(df_notsrc))

    if ncol(df_notsrc) == 0
        df_tmp = DataFrame(AbstractVector[[] for _ in 1:nrow(df)], df[!, src_namescol],
                           makeunique=makeunique, copycols=false)
    else
        m = permutedims(Matrix(df_notsrc))
        df_tmp = rename!(DataFrame(Tables.table(m)), df[!, src_namescol], makeunique=makeunique)
    end
    return hcat!(df_permuted, df_tmp, makeunique=makeunique, copycols=false)
end

function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex;
                          makeunique::Bool=false)
    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(index(df), src_namescol))
        dest_namescol = _names(df)[src_namescol]
    else
        dest_namescol = src_namescol
    end
    return permutedims(df, src_namescol, dest_namescol; makeunique=makeunique)
end