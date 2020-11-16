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


# Examples
```julia
julia> using Random

julia> Random.seed!(1234);

julia> df = DataFrame(a = repeat([1:3;], inner = [2]),
                      b = repeat([1:2;], inner = [3]),
                      c = randn(6),
                      d = randn(),
                      e = map(string, 'a':'f'))
6×5 DataFrame
 Row │ a      b      c          d         e
     │ Int64  Int64  Float64    Float64   String
─────┼───────────────────────────────────────────
   1 │     1      1   0.867347  0.532813  a
   2 │     1      1  -0.901744  0.532813  b
   3 │     2      1  -0.494479  0.532813  c
   4 │     2      2  -0.902914  0.532813  d
   5 │     3      2   0.864401  0.532813  e
   6 │     3      2   2.21188   0.532813  f

julia> stack(df, [:c, :d])
12×5 DataFrame
 Row │ a      b      e       variable  value
     │ Int64  Int64  String  String    Float64
─────┼───────────────────────────────────────────
   1 │     1      1  a       c          0.867347
   2 │     1      1  b       c         -0.901744
   3 │     2      1  c       c         -0.494479
   4 │     2      2  d       c         -0.902914
  ⋮  │   ⋮      ⋮      ⋮        ⋮          ⋮
  10 │     2      2  d       d          0.532813
  11 │     3      2  e       d          0.532813
  12 │     3      2  f       d          0.532813
                                   5 rows omitted

julia> stack(df, [:c, :d], [:a])
12×3 DataFrame
 Row │ a      variable  value
     │ Int64  String    Float64
─────┼────────────────────────────
   1 │     1  c          0.867347
   2 │     1  c         -0.901744
   3 │     2  c         -0.494479
   4 │     2  c         -0.902914
  ⋮  │   ⋮       ⋮          ⋮
  10 │     2  d          0.532813
  11 │     3  d          0.532813
  12 │     3  d          0.532813
                    5 rows omitted

julia> stack(df, Not([:a, :b, :e]))
12×5 DataFrame
 Row │ a      b      e       variable  value
     │ Int64  Int64  String  String    Float64
─────┼───────────────────────────────────────────
   1 │     1      1  a       c          0.867347
   2 │     1      1  b       c         -0.901744
   3 │     2      1  c       c         -0.494479
   4 │     2      2  d       c         -0.902914
  ⋮  │   ⋮      ⋮      ⋮        ⋮          ⋮
  10 │     2      2  d       d          0.532813
  11 │     3      2  e       d          0.532813
  12 │     3      2  f       d          0.532813
                                   5 rows omitted

julia> stack(df, Not([:a, :b, :e]), variable_name=:somemeasure)
12×5 DataFrame
 Row │ a      b      e       somemeasure  value
     │ Int64  Int64  String  String       Float64
─────┼──────────────────────────────────────────────
   1 │     1      1  a       c             0.867347
   2 │     1      1  b       c            -0.901744
   3 │     2      1  c       c            -0.494479
   4 │     2      2  d       c            -0.902914
  ⋮  │   ⋮      ⋮      ⋮          ⋮           ⋮
  10 │     2      2  d       d             0.532813
  11 │     3      2  e       d             0.532813
  12 │     3      2  f       d             0.532813
                                      5 rows omitted
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

Row and column keys will be ordered in the order of their first appearance.

# Positional arguments
- `df` : the AbstractDataFrame to be unstacked
- `rowkeys` : the columns with a unique key for each row, if not given,
  find a key by grouping on anything not a `colkey` or `value`.
  Can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
- `colkey` : the column ($COLUMNINDEX_STR) holding the column names in wide format,
  defaults to `:variable`
- `value` : the value column ($COLUMNINDEX_STR), defaults to `:value`

# Keyword arguments

- `renamecols`: a function called on each unique value in `colkey`; it must return
  the name of the column to be created (typically as a string or a `Symbol`).
  Duplicates in resulting names when converted to `Symbol` are not allowed.
  By default no transformation is performed.
- `allowmissing`: if `false` (the default) then an error will be thrown if `colkey`
  contains `missing` values; if `true` then a column referring to `missing` value
  will be created.
- allowduplicates`: if `false` (the default) then an error an error will be thrown
  if combination of `rowkeys` and `colkey` contains duplicate entries; if `true`
  then  then the last encountered `value` will be retained.

# Examples

```julia
julia> using Random

julia> Random.seed!(1234);

julia> wide = DataFrame(id = 1:6,
                        a  = repeat([1:3;], inner = [2]),
                        b  = repeat([1:2;], inner = [3]),
                        c  = randn(6),
                        d  = randn(6))
6×5 DataFrame
 Row │ id     a      b      c          d
     │ Int64  Int64  Int64  Float64    Float64
─────┼────────────────────────────────────────────
   1 │     1      1      1   0.867347   0.532813
   2 │     2      1      1  -0.901744  -0.271735
   3 │     3      2      1  -0.494479   0.502334
   4 │     4      2      2  -0.902914  -0.516984
   5 │     5      3      2   0.864401  -0.560501
   6 │     6      3      2   2.21188   -0.0192918

julia> long = stack(wide)
12×5 DataFrame
 Row │ id     a      b      variable  value
     │ Int64  Int64  Int64  String    Float64
─────┼───────────────────────────────────────────
   1 │     1      1      1  c          0.867347
   2 │     2      1      1  c         -0.901744
   3 │     3      2      1  c         -0.494479
   4 │     4      2      2  c         -0.902914
  ⋮  │   ⋮      ⋮      ⋮       ⋮          ⋮
  10 │     4      2      2  d         -0.516984
  11 │     5      3      2  d         -0.560501
  12 │     6      3      2  d         -0.0192918
                                   5 rows omitted

julia> unstack(long)
6×5 DataFrame
 Row │ id     a      b      c          d
     │ Int64  Int64  Int64  Float64?   Float64?
─────┼────────────────────────────────────────────
   1 │     1      1      1   0.867347   0.532813
   2 │     2      1      1  -0.901744  -0.271735
   3 │     3      2      1  -0.494479   0.502334
   4 │     4      2      2  -0.902914  -0.516984
   5 │     5      3      2   0.864401  -0.560501
   6 │     6      3      2   2.21188   -0.0192918

julia> unstack(long, :variable, :value)
6×5 DataFrame
 Row │ id     a      b      c          d
     │ Int64  Int64  Int64  Float64?   Float64?
─────┼────────────────────────────────────────────
   1 │     1      1      1   0.867347   0.532813
   2 │     2      1      1  -0.901744  -0.271735
   3 │     3      2      1  -0.494479   0.502334
   4 │     4      2      2  -0.902914  -0.516984
   5 │     5      3      2   0.864401  -0.560501
   6 │     6      3      2   2.21188   -0.0192918

julia> unstack(long, :id, :variable, :value)
6×3 DataFrame
 Row │ id     c          d
     │ Int64  Float64?   Float64?
─────┼──────────────────────────────
   1 │     1   0.867347   0.532813
   2 │     2  -0.901744  -0.271735
   3 │     3  -0.494479   0.502334
   4 │     4  -0.902914  -0.516984
   5 │     5   0.864401  -0.560501
   6 │     6   2.21188   -0.0192918

julia> unstack(long, [:id, :a], :variable, :value)
6×4 DataFrame
 Row │ id     a      c          d
     │ Int64  Int64  Float64?   Float64?
─────┼─────────────────────────────────────
   1 │     1      1   0.867347   0.532813
   2 │     2      1  -0.901744  -0.271735
   3 │     3      2  -0.494479   0.502334
   4 │     4      2  -0.902914  -0.516984
   5 │     5      3   0.864401  -0.560501
   6 │     6      3   2.21188   -0.0192918

julia> unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x))
6×3 DataFrame
 Row │ id     _c         _d
     │ Int64  Float64?   Float64?
─────┼──────────────────────────────
   1 │     1   0.867347   0.532813
   2 │     2  -0.901744  -0.271735
   3 │     3  -0.494479   0.502334
   4 │     4  -0.902914  -0.516984
   5 │     5   0.864401  -0.560501
   6 │     6   2.21188   -0.0192918
```
Note that there are some differences between the widened results above.
"""
function unstack(df::AbstractDataFrame, rowkeys, colkey::ColumnIndex,
                 value::ColumnIndex; renamecols::Function=identity,
                 allowmissing::Bool=false, allowduplicates::Bool=false)
    rowkey_ints = vcat(index(df)[rowkeys])
    @assert rowkey_ints isa AbstractVector{Int}
    length(rowkey_ints) == 0 && throw(ArgumentError("No key column found"))
    g_rowkey = groupby(df, rowkey_ints)
    g_colkey = groupby(df, colkey)
    valuecol = df[!, value]
    return _unstack(df, rowkey_ints, index(df)[colkey], g_colkey,
                    valuecol, g_rowkey, renamecols, allowmissing, allowduplicates)
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
                  renamecols::Function,
                  allowmissing::Bool, allowduplicates::Bool)
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
                            "Pass `allowmissing=true` to skip missings."))
    end

    unstacked_val = [similar_missing(valuecol, Nrow) for i in 1:Ncol]
    mask_filled = falses(Nrow, Ncol)

    @assert length(rowref) == length(colref) == length(valuecol)
    for (k, (row_id, col_id, val)) in enumerate(zip(rowref, colref, valuecol))
        if !allowduplicates && mask_filled[row_id, col_id]
            throw(ArgumentError("Duplicate entries in unstack at row $k for key "*
                                "$(tuple((df[k, s] for s in rowkeys)...)) and variable $(colref_map[col_id]). " *
                                "Pass allowduplicates=true to allow them."))
        end
        unstacked_val[col_id][row_id] = val
        mask_filled[row_id, col_id] = true
    end

    # note that Symbol.(renamecols.(colref_map)) must produce unique column names
    # and names between df1 and df2 must be unique
    df1 = df[row_group_row_idxs, g_rowkey.cols]
    df2 = DataFrame(unstacked_val, Symbol.(renamecols.(colref_map)), copycols=false)

    @assert length(col_group_row_idxs) == ncol(df2)
    # avoid reordering when col_group_row_idxs was already ordered
    if !issorted(col_group_row_idxs)
        df2 = df2[!, sortperm(col_group_row_idxs)]
    end

    res_df = hcat(df1, df2, copycols=false)

    @assert length(row_group_row_idxs) == nrow(res_df)
    # avoid reordering when col_group_row_idxs was already ordered
    if !issorted(row_group_row_idxs)
        res_df = res_df[sortperm(row_group_row_idxs), :]
    end

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
- `inner` : the numer of times each element is repeated
- `outer` : the numer of times the whole vector is repeated after
  expanded by `inner`

`inner` and `outer` have the same meaning as similarly named arguments
to `repeat`.

# Examples
```julia
RepeatedVector([1, 2], 3, 1)   # [1, 1, 1, 2, 2, 2]
RepeatedVector([1, 2], 1, 3)   # [1, 2, 1, 2, 1, 2]
RepeatedVector([1, 2], 2, 2)   # [1, 2, 1, 2, 1, 2, 1, 2]
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
