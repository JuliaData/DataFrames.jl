"""
    stack(df::AbstractDataFrame[, measure_vars[, id_vars] ];
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

Metadata: table-level `:note`-style metadata and column-level `:note`-style metadata
for identifier columns are preserved.

# Examples
```jldoctest
julia> df = DataFrame(a=repeat(1:3, inner=2),
                      b=repeat(1:2, inner=3),
                      c=repeat(1:1, inner=6),
                      d=repeat(1:6, inner=1),
                      e=string.('a':'f'))
6×5 DataFrame
 Row │ a      b      c      d      e
     │ Int64  Int64  Int64  Int64  String
─────┼────────────────────────────────────
   1 │     1      1      1      1  a
   2 │     1      1      1      2  b
   3 │     2      1      1      3  c
   4 │     2      2      1      4  d
   5 │     3      2      1      5  e
   6 │     3      2      1      6  f

julia> stack(df, [:c, :d])
12×5 DataFrame
 Row │ a      b      e       variable  value
     │ Int64  Int64  String  String    Int64
─────┼───────────────────────────────────────
   1 │     1      1  a       c             1
   2 │     1      1  b       c             1
   3 │     2      1  c       c             1
   4 │     2      2  d       c             1
   5 │     3      2  e       c             1
   6 │     3      2  f       c             1
   7 │     1      1  a       d             1
   8 │     1      1  b       d             2
   9 │     2      1  c       d             3
  10 │     2      2  d       d             4
  11 │     3      2  e       d             5
  12 │     3      2  f       d             6

julia> stack(df, [:c, :d], [:a])
12×3 DataFrame
 Row │ a      variable  value
     │ Int64  String    Int64
─────┼────────────────────────
   1 │     1  c             1
   2 │     1  c             1
   3 │     2  c             1
   4 │     2  c             1
   5 │     3  c             1
   6 │     3  c             1
   7 │     1  d             1
   8 │     1  d             2
   9 │     2  d             3
  10 │     2  d             4
  11 │     3  d             5
  12 │     3  d             6

julia> stack(df, Not([:a, :b, :e]))
12×5 DataFrame
 Row │ a      b      e       variable  value
     │ Int64  Int64  String  String    Int64
─────┼───────────────────────────────────────
   1 │     1      1  a       c             1
   2 │     1      1  b       c             1
   3 │     2      1  c       c             1
   4 │     2      2  d       c             1
   5 │     3      2  e       c             1
   6 │     3      2  f       c             1
   7 │     1      1  a       d             1
   8 │     1      1  b       d             2
   9 │     2      1  c       d             3
  10 │     2      2  d       d             4
  11 │     3      2  e       d             5
  12 │     3      2  f       d             6

julia> stack(df, Not([:a, :b, :e]), variable_name=:somemeasure)
12×5 DataFrame
 Row │ a      b      e       somemeasure  value
     │ Int64  Int64  String  String       Int64
─────┼──────────────────────────────────────────
   1 │     1      1  a       c                1
   2 │     1      1  b       c                1
   3 │     2      1  c       c                1
   4 │     2      2  d       c                1
   5 │     3      2  e       c                1
   6 │     3      2  f       c                1
   7 │     1      1  a       d                1
   8 │     1      1  b       d                2
   9 │     2      1  c       d                3
  10 │     2      2  d       d                4
  11 │     3      2  e       d                5
  12 │     3      2  f       d                6
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
        catnms = simnms isa Vector ? PooledArray(simnms) : simnms
        copyto!(catnms, nms)
    end
    out_df =  DataFrame(AbstractVector[[repeat(df[!, c], outer=N) for c in ints_id_vars]..., # id_var columns
                                       repeat(catnms, inner=nrow(df)),                       # variable
                                       vcat([df[!, c] for c in ints_measure_vars]...)],      # value
                        cnames, copycols=false)
    _copy_table_note_metadata!(out_df, df)
    if !isempty(colmetadatakeys(df))
        for (i_out, i_in) in enumerate(ints_id_vars)
            _copy_col_note_metadata!(out_df, i_out, df, i_in)
        end
    end
    return out_df
end

function _stackview(df::AbstractDataFrame, measure_vars::AbstractVector{Int},
                    ints_id_vars::AbstractVector{Int}; variable_name::Symbol,
                    value_name::Symbol, variable_eltype::Type)
    N = length(measure_vars)
    cnames = _names(df)[ints_id_vars]
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
    out_df = DataFrame(AbstractVector[[RepeatedVector(df[!, c], 1, N) for c in ints_id_vars]..., # id_var columns
                                      RepeatedVector(catnms, nrow(df), 1),                       # variable
                                      StackedVector(Any[df[!, c] for c in measure_vars])],       # value
                       cnames, copycols=false)
    _copy_table_note_metadata!(out_df, df)
    if !isempty(colmetadatakeys(df))
        for (i_out, i_in) in enumerate(ints_id_vars)
            _copy_col_note_metadata!(out_df, i_out, df, i_in)
        end
    end
    return out_df
end

"""
    unstack(df::AbstractDataFrame, rowkeys, colkey, value;
            renamecols::Function=identity, allowmissing::Bool=false,
            combine=only, fill=missing, threads::Bool=true)
    unstack(df::AbstractDataFrame, colkey, value;
            renamecols::Function=identity, allowmissing::Bool=false,
            combine=only, fill=missing, threads::Bool=true)
    unstack(df::AbstractDataFrame;
            renamecols::Function=identity, allowmissing::Bool=false,
            combine=only, fill=missing, threads::Bool=true)

Unstack data frame `df`, i.e. convert it from long to wide format.

Row and column keys are ordered in the order of their first appearance.

# Positional arguments
- `df` : the AbstractDataFrame to be unstacked
- `rowkeys` : the columns with a unique key for each row, if not given, find a
  key by grouping on anything not a `colkey` or `value`. Can be any column
  selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR). If `rowkeys` contains no
  columns all rows are assumed to have the same key.
- `colkey` : the column ($COLUMNINDEX_STR) holding the column names in wide
  format, defaults to `:variable`
- `values` : the column storing values ($COLUMNINDEX_STR), defaults to `:value`

# Keyword arguments

- `renamecols`: a function called on each unique value in `colkey`; it must
  return the name of the column to be created (typically as a string or a
  `Symbol`). Duplicates in resulting names when converted to `Symbol` are not
  allowed. By default no transformation is performed.
- `allowmissing`: if `false` (the default) then an error is thrown if
  `colkey` contains `missing` values; if `true` then a column referring to
  `missing` value is created.
- `combine`: if `only` (the default) then an error is thrown if combination
  of `rowkeys` and `colkey` contains duplicate entries. Otherwise the passed
  value must be a function that is called on a vector view containing all
  elements for each combination of `rowkeys` and `colkey` present in the data.
- `fill`: missing row/column combinations are filled with this value. The
  default is `missing`. If the `value` column is a `CategoricalVector` and
  `fill` is not `missing` then in order to keep unstacked value columns also
  `CategoricalVector` the `fill` must be passed as `CategoricalValue`
- `threads`: whether `combine` function may be run in separate tasks which can
  execute in parallel (possibly being applied to multiple groups at the same
  time). Whether or not tasks are actually spawned and their number are
  determined automatically. Set to `false` if `combine` requires serial
  execution or is not thread-safe.

Metadata: table-level `:note`-style metadata and column-level `:note`-style
metadata for row keys columns are preserved.

# Deprecations

- `allowduplicates` keyword argument is deprecated; instead use `combine`
  keyword argument; an equivalent to `allowduplicates=true` is `combine=last`
  and to `allowduplicates=false` is `combine=only` (the default);

# Examples

```jldoctest
julia> wide = DataFrame(id=1:6,
                        a=repeat(1:3, inner=2),
                        b=repeat(1.0:2.0, inner=3),
                        c=repeat(1.0:1.0, inner=6),
                        d=repeat(1.0:3.0, inner=2))
6×5 DataFrame
 Row │ id     a      b        c        d
     │ Int64  Int64  Float64  Float64  Float64
─────┼─────────────────────────────────────────
   1 │     1      1      1.0      1.0      1.0
   2 │     2      1      1.0      1.0      1.0
   3 │     3      2      1.0      1.0      2.0
   4 │     4      2      2.0      1.0      2.0
   5 │     5      3      2.0      1.0      3.0
   6 │     6      3      2.0      1.0      3.0

julia> long = stack(wide)
18×4 DataFrame
 Row │ id     a      variable  value
     │ Int64  Int64  String    Float64
─────┼─────────────────────────────────
   1 │     1      1  b             1.0
   2 │     2      1  b             1.0
   3 │     3      2  b             1.0
   4 │     4      2  b             2.0
   5 │     5      3  b             2.0
   6 │     6      3  b             2.0
   7 │     1      1  c             1.0
   8 │     2      1  c             1.0
  ⋮  │   ⋮      ⋮       ⋮         ⋮
  12 │     6      3  c             1.0
  13 │     1      1  d             1.0
  14 │     2      1  d             1.0
  15 │     3      2  d             2.0
  16 │     4      2  d             2.0
  17 │     5      3  d             3.0
  18 │     6      3  d             3.0
                         3 rows omitted

julia> unstack(long)
6×5 DataFrame
 Row │ id     a      b         c         d
     │ Int64  Int64  Float64?  Float64?  Float64?
─────┼────────────────────────────────────────────
   1 │     1      1       1.0       1.0       1.0
   2 │     2      1       1.0       1.0       1.0
   3 │     3      2       1.0       1.0       2.0
   4 │     4      2       2.0       1.0       2.0
   5 │     5      3       2.0       1.0       3.0
   6 │     6      3       2.0       1.0       3.0

julia> unstack(long, :variable, :value)
6×5 DataFrame
 Row │ id     a      b         c         d
     │ Int64  Int64  Float64?  Float64?  Float64?
─────┼────────────────────────────────────────────
   1 │     1      1       1.0       1.0       1.0
   2 │     2      1       1.0       1.0       1.0
   3 │     3      2       1.0       1.0       2.0
   4 │     4      2       2.0       1.0       2.0
   5 │     5      3       2.0       1.0       3.0
   6 │     6      3       2.0       1.0       3.0

julia> unstack(long, :id, :variable, :value)
6×4 DataFrame
 Row │ id     b         c         d
     │ Int64  Float64?  Float64?  Float64?
─────┼─────────────────────────────────────
   1 │     1       1.0       1.0       1.0
   2 │     2       1.0       1.0       1.0
   3 │     3       1.0       1.0       2.0
   4 │     4       2.0       1.0       2.0
   5 │     5       2.0       1.0       3.0
   6 │     6       2.0       1.0       3.0

julia> unstack(long, [:id, :a], :variable, :value)
6×5 DataFrame
 Row │ id     a      b         c         d
     │ Int64  Int64  Float64?  Float64?  Float64?
─────┼────────────────────────────────────────────
   1 │     1      1       1.0       1.0       1.0
   2 │     2      1       1.0       1.0       1.0
   3 │     3      2       1.0       1.0       2.0
   4 │     4      2       2.0       1.0       2.0
   5 │     5      3       2.0       1.0       3.0
   6 │     6      3       2.0       1.0       3.0

julia> unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x))
6×4 DataFrame
 Row │ id     _b        _c        _d
     │ Int64  Float64?  Float64?  Float64?
─────┼─────────────────────────────────────
   1 │     1       1.0       1.0       1.0
   2 │     2       1.0       1.0       1.0
   3 │     3       1.0       1.0       2.0
   4 │     4       2.0       1.0       2.0
   5 │     5       2.0       1.0       3.0
   6 │     6       2.0       1.0       3.0
```
Note that there are some differences between the widened results above.

```jldoctest
julia> df = DataFrame(id=["1", "1", "2"],
                      variable=["Var1", "Var2", "Var1"],
                      value=[1, 2, 3])
3×3 DataFrame
 Row │ id      variable  value
     │ String  String    Int64
─────┼─────────────────────────
   1 │ 1       Var1          1
   2 │ 1       Var2          2
   3 │ 2       Var1          3

julia> unstack(df, :variable, :value, fill=0)
2×3 DataFrame
 Row │ id      Var1   Var2
     │ String  Int64  Int64
─────┼──────────────────────
   1 │ 1           1      2
   2 │ 2           3      0

julia> df = DataFrame(cols=["a", "a", "b"], values=[1, 2, 4])
3×2 DataFrame
 Row │ cols    values
     │ String  Int64
─────┼────────────────
   1 │ a            1
   2 │ a            2
   3 │ b            4

julia> unstack(df, :cols, :values, combine=copy)
1×2 DataFrame
 Row │ a        b
     │ Array…?  Array…?
─────┼──────────────────
   1 │ [1, 2]   [4]

julia> unstack(df, :cols, :values, combine=sum)
1×2 DataFrame
 Row │ a       b
     │ Int64?  Int64?
─────┼────────────────
   1 │      3       4
```
"""
function unstack(df::AbstractDataFrame, rowkeys, colkey::ColumnIndex,
                 values::ColumnIndex; renamecols::Function=identity,
                 allowmissing::Bool=false,  allowduplicates::Bool=false,
                 combine=only, fill=missing, threads::Bool=true)
    if allowduplicates
        Base.depwarn("allowduplicates keyword argument is deprecated. " *
                     "Pass `combine=last` instead of `allowduplicates=true`.", :unstack)
        combine = last
    end
    # first make sure that rowkeys are unique and
    # normalize all selectors as a strings
    # if some of the selectors are wrong we will get an early error here
    rowkeys = names(df, index(df)[rowkeys])
    colkey = only(names(df, colkey))
    values = only(names(df, values))

    if combine !== only
        # potentially colkey can be also part of rowkeys so we need to do unique
        groupcols = unique!([rowkeys; colkey])
        @assert groupcols isa Vector{String}

        # generate some column name that is not conflicting with column name
        # already present in the data frame
        values_out = "values_out_3490283_"
        while hasproperty(df, values_out)
            values_out *= "1"
        end

        gdf = groupby(df, groupcols)
        if check_aggregate(combine, df[!, values]) isa AbstractAggregate
            # if combine function is AbstractAggregate
            # then we are sure it will return a scalar number so we can
            # leave it as is and be sure we use fast path in combine
            agg_fun = combine
        else
            # in general combine function could return e.g. a vector,
            # which would get expanded to multiple rows so we protect it with
            # Ref that will get unwrapped by combine
            agg_fun = Ref∘combine
        end
        df_op = DataFrames.combine(gdf, values => agg_fun => values_out,
                                   threads=threads)

        group_rows = find_group_row(gdf)
        if !issorted(group_rows)
            df_op = df_op[sortperm(group_rows), :]
        end
        # we should not have any duplicates in df_op now
        noduplicates = true
    else
        df_op = df
        values_out = values
        noduplicates = false
    end

    g_rowkey = groupby(df_op, rowkeys)
    g_colkey = groupby(df_op, colkey)
    valuecol = df_op[!, values_out]
    return _unstack(df_op, index(df_op)[rowkeys], index(df_op)[colkey], g_colkey,
                    valuecol, g_rowkey, renamecols, allowmissing, noduplicates, fill)
end

function unstack(df::AbstractDataFrame, colkey::ColumnIndex, values::ColumnIndex;
                 renamecols::Function=identity, allowmissing::Bool=false,
                  allowduplicates::Bool=false, combine=only, fill=missing,
                  threads::Bool=true)
    if allowduplicates
        Base.depwarn("allowduplicates keyword argument is deprecated. " *
                     "Pass `combine=last` instead of allowduplicates=true.", :unstack)
        combine = last
    end
    colkey_int = index(df)[colkey]
    value_int = index(df)[values]
    return unstack(df, Not(colkey_int, value_int), colkey_int, value_int,
            renamecols=renamecols, allowmissing=allowmissing,
            combine=combine,
            fill=fill, threads=threads)
end

function unstack(df::AbstractDataFrame; renamecols::Function=identity,
                 allowmissing::Bool=false, allowduplicates::Bool=false,
                 combine=only, fill=missing, threads::Bool=true)
    if allowduplicates
        Base.depwarn("allowduplicates keyword argument is deprecated. " *
                     "Pass `combine=last` instead of allowduplicates=true.", :unstack)
        combine = last
    end
    unstack(df, :variable, :value, renamecols=renamecols, allowmissing=allowmissing,
            combine=combine, fill=fill, threads=threads)
end

# we take into account the fact that idx, starts and ends are computed lazily
# so we rather directly reference the gdf.groups
# this function is tailor made for unstack so it does assume that no groups were
# dropped (i.e. gdf.groups does not contain 0 entries)
function find_group_row(gdf::GroupedDataFrame)
    rows = zeros(Int, length(gdf))
    isempty(rows) && return rows

    filled = 0
    i = 1
    groups = gdf.groups
    while filled < length(gdf)
        group = groups[i]
        if rows[group] == 0
            rows[group] = i
            filled += 1
        end
        i += 1
    end
    return rows # return row index of first occurrence of each group in gdf.groups
end

function _unstack(df::AbstractDataFrame, rowkeys::AbstractVector{Int},
                  colkey::Int, g_colkey::GroupedDataFrame,
                  valuecol::AbstractVector, g_rowkey::GroupedDataFrame,
                  renamecols::Function, allowmissing::Bool, noduplicates::Bool, fill)
    rowref = g_rowkey.groups
    row_group_row_idxs = find_group_row(g_rowkey)
    Nrow = length(g_rowkey)

    @assert groupcols(g_colkey) == _names(df)[colkey:colkey]
    colref = g_colkey.groups
    Ncol = length(g_colkey)
    col_group_row_idxs = find_group_row(g_colkey)
    colref_map = df[col_group_row_idxs, colkey]
    if any(ismissing, colref_map) && !allowmissing
        throw(ArgumentError("Missing value in variable :$(_names(df)[colkey]). " *
                            "Pass `allowmissing=true` to create a :missing column " *
                            "referring to `missing` values."))
    end
    @assert length(rowref) == length(colref) == length(valuecol)

    unstacked_val = [fill!(similar(valuecol,
                           promote_type(eltype(valuecol), typeof(fill)),
                           Nrow),
                     fill) for _ in 1:Ncol]

    # use a separate path for noduplicates to reduce memory use and increase speed
    if noduplicates
        for (k, (row_id, col_id, val)) in enumerate(zip(rowref, colref, valuecol))
            unstacked_val[col_id][row_id] = val
        end
    else
        mask_filled = falses(Nrow, Ncol)
        for (k, (row_id, col_id, val)) in enumerate(zip(rowref, colref, valuecol))
            if mask_filled[row_id, col_id]
                bad_key = tuple((df[k, s] for s in rowkeys)...)
                bad_var = colref_map[col_id]
                throw(ArgumentError("Duplicate entries in unstack at row $k for key "*
                                    "$bad_key and variable $bad_var. " *
                                    "Pass `combine` keyword argument to specify " *
                                    "how they should be handled."))
            end
            unstacked_val[col_id][row_id] = val
            mask_filled[row_id, col_id] = true
        end
    end

    # note that Symbol(renamecols(x)) must produce unique column names
    # and names between df1 and df2 must be unique
    # here df1 gets proper column-level metadata with :note style
    df1 = df[row_group_row_idxs, g_rowkey.cols]
    new_col_names = Symbol[Symbol(renamecols(x)) for x in colref_map]
    if !allunique(new_col_names)
        throw(ArgumentError("Non-unique column names produced. " *
                            "Non equal values in `colkey` were mapped " *
                            "to the same column name."))
    end
    df2 = DataFrame(unstacked_val, new_col_names,
                    copycols=false)

    @assert length(col_group_row_idxs) == ncol(df2)
    # avoid reordering when col_group_row_idxs was already ordered
    if !issorted(col_group_row_idxs)
        df2 = df2[!, sortperm(col_group_row_idxs)]
    end

    if !isempty(intersect(_names(df1), _names(df2)))
        throw(ArgumentError("Non-unique column names produced. " *
                            "Column names created using the `colkey` " *
                            "conflict with `rowkeys` column names."))
    end

    res_df = hcat(df1, df2, copycols=false)

    @assert length(row_group_row_idxs) == nrow(res_df)
    # avoid reordering when row_group_row_idxs was already ordered
    if !issorted(row_group_row_idxs)
        res_df = res_df[sortperm(row_group_row_idxs), :]
    end

    # only table-level :note-style metadata needs to be copied
    # as column-level :note-style metadata is already correctly set
    _copy_table_note_metadata!(res_df, df)

    return res_df
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
StackedVector(Any[[1, 2], [9, 10], [11, 12]])  # [1, 2, 9, 10, 11, 12]
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
- `inner` : the number of times each element is repeated
- `outer` : the number of times the whole vector is repeated after
  expanded by `inner`

`inner` and `outer` have the same meaning as similarly named arguments
to `repeat`.

# Examples
```julia
RepeatedVector([1, 2], 3, 1)   # [1, 1, 1, 2, 2, 2]
RepeatedVector([1, 2], 1, 3)   # [1, 2, 1, 2, 1, 2]
RepeatedVector([1, 2], 2, 2)   # [1, 1, 2, 2, 1, 1, 2, 2]
```
"""
struct RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    inner::Int
    outer::Int
end

Base.parent(v::RepeatedVector) = v.parent

function Base.getindex(v::RepeatedVector, i::Int)
    N = length(parent(v))
    idx = Base.fld1(mod1(i, v.inner*N), v.inner)
    parent(v)[idx]
end

Base.IndexStyle(::Type{<:RepeatedVector}) = Base.IndexLinear()
Base.size(v::RepeatedVector) = (length(v),)
Base.length(v::RepeatedVector) = v.inner * v.outer * length(parent(v))
Base.eltype(v::Type{RepeatedVector{T}}) where {T} = T
Base.reverse(v::RepeatedVector) = RepeatedVector(reverse(parent(v)), v.inner, v.outer)
Base.similar(v::RepeatedVector, T::Type, dims::Dims) = similar(parent(v), T, dims)
Base.unique(v::RepeatedVector) = unique(parent(v))

Base.transpose(::AbstractDataFrame, args...; kwargs...) =
    throw(ArgumentError("`transpose` not defined for `AbstractDataFrame`s. Try `permutedims` instead"))

"""
    permutedims(df::AbstractDataFrame,
                [src_namescol::Union{Int, Symbol, AbstractString}],
                [dest_namescol::Union{Symbol, AbstractString}];
                makeunique::Bool=false, strict::Bool=true)

Turn `df` on its side such that rows become columns
and values in the column indexed by `src_namescol` become the names of new columns.
In the resulting `DataFrame`, column names of `df` will become the first column
with name specified by `dest_namescol`.

# Arguments
- `df` : the `AbstractDataFrame`
- `src_namescol` : the column that will become the new header.
   If omitted then column names `:x1`, `:x2`, ... are generated automatically.
- `dest_namescol` : the name of the first column in the returned `DataFrame`.
  Defaults to the same name as `src_namescol`.
  Not supported when `src_namescol` is a vector or is omitted.
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found; if `true`, duplicate names will be suffixed
  with `_i` (`i` starting at 1 for the first duplicate).
  Not supported when `src_namescol` is omitted.
- `strict` : if `true` (the default), an error will be raised if the values
  contained in the `src_namescol` are not all `Symbol` or all `AbstractString`,
  or can all be converted to `String` using `convert`. If `false`
  then any values are accepted and the will be changed to strings using
  the `string` function.
  Not supported when `src_namescol` is a vector or is omitted.

Note: The element types of columns in resulting `DataFrame`
(other than the first column if it is created from `df` column names,
which always has element type `String`) will depend on the element types of
_all_ input columns based on the result of `promote_type`.
That is, if the source data frame contains `Int` and `Float64` columns,
resulting columns will have element type `Float64`. If the source has
`Int` and `String` columns, resulting columns will have element type `Any`.

Metadata: table-level `:note`-style metadata is preserved and
column-level metadata is dropped.

# Examples

```jldoctest
julia> df = DataFrame(a=1:2, b=3:4)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> permutedims(df)
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     3      4

julia> permutedims(df, [:p, :q])
2×2 DataFrame
 Row │ p      q
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     3      4

julia> df1 = DataFrame(a=["x", "y"], b=[1.0, 2.0], c=[3, 4], d=[true, false])
2×4 DataFrame
 Row │ a       b        c      d
     │ String  Float64  Int64  Bool
─────┼───────────────────────────────
   1 │ x           1.0      3   true
   2 │ y           2.0      4  false

julia> permutedims(df1, 1) # note the column types
3×3 DataFrame
 Row │ a       x        y
     │ String  Float64  Float64
─────┼──────────────────────────
   1 │ b           1.0      2.0
   2 │ c           3.0      4.0
   3 │ d           1.0      0.0

julia> df2 = DataFrame(a=["x", "y"], b=[1, "two"], c=[3, 4], d=[true, false])
2×4 DataFrame
 Row │ a       b    c      d
     │ String  Any  Int64  Bool
─────┼───────────────────────────
   1 │ x       1        3   true
   2 │ y       two      4  false

julia> permutedims(df2, 1, "different_name")
3×3 DataFrame
 Row │ different_name  x     y
     │ String          Any   Any
─────┼─────────────────────────────
   1 │ b               1     two
   2 │ c               3     4
   3 │ d               true  false
```
"""
function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex,
                          dest_namescol::Union{Symbol, AbstractString};
                          makeunique::Bool=false, strict::Bool=true)

    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(index(df), src_namescol))
    end
    src_col_names = df[!, src_namescol]
    local new_col_names
    if eltype(src_col_names) <: SymbolOrString
        new_col_names = src_col_names
    elseif all(x -> x isa Symbol, src_col_names)
        new_col_names = collect(Symbol, src_col_names)
    elseif !strict
        new_col_names = string.(src_col_names)
    else
        try
            new_col_names = collect(String, src_col_names)
        catch e
            if e isa MethodError && e.f === convert
                throw(ArgumentError("all elements of src_namescol must support " *
                                    "conversion to String"))
            else
                rethrow(e)
            end
        end
    end

    df_notsrc = df[!, Not(src_namescol)]
    df_permuted = DataFrame(dest_namescol => names(df_notsrc))

    if ncol(df_notsrc) == 0
        df_tmp = DataFrame(AbstractVector[[] for _ in 1:nrow(df)], new_col_names,
                           makeunique=makeunique, copycols=false)
    else
        m = permutedims(Matrix(df_notsrc))
        df_tmp = rename!(DataFrame(Tables.table(m)), new_col_names, makeunique=makeunique)
    end
    out_df = hcat!(df_permuted, df_tmp, makeunique=makeunique, copycols=false)
    _copy_table_note_metadata!(out_df, df)
    return out_df
end

function Base.permutedims(df::AbstractDataFrame, src_namescol::ColumnIndex;
                          makeunique::Bool=false, strict::Bool=true)
    if src_namescol isa Integer
        1 <= src_namescol <= ncol(df) || throw(BoundsError(index(df), src_namescol))
        dest_namescol = _names(df)[src_namescol]
    else
        dest_namescol = src_namescol
    end
    return permutedims(df, src_namescol, dest_namescol;
                       makeunique=makeunique, strict=strict)
end

function Base.permutedims(df::AbstractDataFrame)
    out_df = DataFrame(permutedims(Matrix(df)), :auto)
    _copy_table_note_metadata!(out_df, df)
    return out_df
end

function Base.permutedims(df::AbstractDataFrame, cnames::AbstractVector;
                          makeunique::Bool=false)
    out_df = DataFrame(permutedims(Matrix(df)), cnames, makeunique=makeunique)
    _copy_table_note_metadata!(out_df, df)
    return out_df
end
