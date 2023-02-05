"""
    nest(gdf::GroupedDataFrame, cols::Pair{<:AbstractString}...; view::Bool=false)
    nest(gdf::GroupedDataFrame, cols::Pair{Symbol}...; view::Bool=false)
    nest(gdf::GroupedDataFrame; view::Bool=false)

Return a data frame with one row for each group in `gdf` where
or or more columns contain a data frame of the rows that belong to that group.

Every `cols` argument must be a pair `column_selector => column_name`.
If no `cols` are passed, then by default `valuecols(gdf) => :data`
nesting is performed.

The returned data frame has all grouping columns of `gdf`, followed by one
or more columns where `column_name` is the name of the column storing data frames,
and every data frame consists of columns picked by `column_selector` values
computed for each group of `gdf`.

If `view=false` (the default) the nested data frames will hold copies of
data from the source data frame. If `view=true` views of the source data frame
will be created.

TODO: metadata

# Examples

julia> df = DataFrame(id = ["b", "a", "a", "c", "b", "b"],
                      x = 1:6, y = 11:16, z='a':'f')
6×4 DataFrame
 Row │ id      x      y      z
     │ String  Int64  Int64  Char
─────┼────────────────────────────
   1 │ b           1     11  a
   2 │ a           2     12  b
   3 │ a           3     13  c
   4 │ c           4     14  d
   5 │ b           5     15  e
   6 │ b           6     16  f

julia> n1 = nest(groupby(df, :id))
3×2 DataFrame
 Row │ id      data
     │ String  DataFrame
─────┼───────────────────────
   1 │ b       3×3 DataFrame
   2 │ a       2×3 DataFrame
   3 │ c       1×3 DataFrame

julia> n1.data
3-element Vector{DataFrame}:
 3×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Char
─────┼────────────────────
   1 │     1     11  a
   2 │     5     15  e
   3 │     6     16  f
 2×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Char
─────┼────────────────────
   1 │     2     12  b
   2 │     3     13  c
 1×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Char
─────┼────────────────────
   1 │     4     14  d

julia> n2 = nest(groupby(df, :id), [:z, :x] => :zx)
3×2 DataFrame
 Row │ id      zx
     │ String  DataFrame
─────┼───────────────────────
   1 │ b       3×2 DataFrame
   2 │ a       2×2 DataFrame
   3 │ c       1×2 DataFrame

julia> n2.zx
3-element Vector{DataFrame}:
 3×2 DataFrame
 Row │ z     x
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ e         5
   3 │ f         6
 2×2 DataFrame
 Row │ z     x
     │ Char  Int64
─────┼─────────────
   1 │ b         2
   2 │ c         3
 1×2 DataFrame
 Row │ z     x
     │ Char  Int64
─────┼─────────────
   1 │ d         4

julia> n3 = nest(groupby(df, :id), :x => :x, [:y, :z] => :yz)
3×3 DataFrame
 Row │ id      x              yz
     │ String  DataFrame      DataFrame
─────┼──────────────────────────────────────
   1 │ b       3×1 DataFrame  3×2 DataFrame
   2 │ a       2×1 DataFrame  2×2 DataFrame
   3 │ c       1×1 DataFrame  1×2 DataFrame

julia> n3.x
3-element Vector{DataFrame}:
 3×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     1
   2 │     5
   3 │     6
 2×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     2
   2 │     3
 1×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     4

julia> n3.yz
3-element Vector{DataFrame}:
 3×2 DataFrame
 Row │ y      z
     │ Int64  Char
─────┼─────────────
   1 │    11  a
   2 │    15  e
   3 │    16  f
 2×2 DataFrame
 Row │ y      z
     │ Int64  Char
─────┼─────────────
   1 │    12  b
   2 │    13  c
 1×2 DataFrame
 Row │ y      z
     │ Int64  Char
─────┼─────────────
   1 │    14  d
"""
nest(gdf::GroupedDataFrame, cols::Pair{<:Any, <:AbstractString}...;
     view::Bool=false) =
    combine(gdf, (sdf -> (; Symbol(dst) => select(sdf, index(sdf)[src], copycols=!view))
                  for (src, dst) in cols)...)
nest(gdf::GroupedDataFrame, cols::Pair{<:Any, Symbol}...; view::Bool=false) =
    combine(gdf, (sdf -> (; dst => select(sdf, index(sdf)[src], copycols=!view))
                  for (src, dst) in cols)...)
nest(gdf::GroupedDataFrame; view::Bool=false) =
    nest(gdf, valuecols(gdf) => :data, view=view)

const UNNESTING_COMMON = """
`cols` argument affects the created columns in the following way:
* If `cols == :setequal` then each row must contain exactly the same columns
  (but possibly in a different order).
* If `cols == :orderequal` then each row must contain the same columns in the
  same order (for `AbstractDict` this option requires that `keys` of row matches
  to allow for support of ordered dicts; however, if row is a `Dict` an error is
  thrown as it is an unordered collection).
* If `cols == :union` (the default) then in each row can contain different
  colums and a `missing` value is pushed to columns missing in a given row
  that are present in other rows.

If `promote=true` (the default) then `
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `df`. If `promote=false` an error
is thrown.

"""

"""
    unnest(df::AbstractDataFrame, src::ColumnIndex...;
           makeunique::Bool=false)

Extract the contents of one or more columns `cols` in `df` that contain
Tables.jl tables, returning a data frame with as many rows and columns as the
nested data frames contain, in addition to original columns, whose contents
gets appropriately repeated to match the number of rows of the unnested tables.
The newly created columns are stored at the end of the data frame (and the
`src` columns are dropped).

Table stored in each row of `src` can have different columns. `missing` value is
pushed to columns missing in a given row that are present in other rows.
The element type of resulting column is determined by promotion of element types
of columns in individual rows.

If `makeunique=false` (the default) produced column names must be unique.
If `makeunique=true` then duplicate column names will be suffixed with `_i`
(`i` starting at `1` for the first duplicate).

TODO: metadata

"""
function unnest(df::AbstractDataFrame, src::ColumnIndex...;
                makeunique::Bool=false)
    ref_df = select(df, Not(collect(Any, src)))
    col_count = ncol(ref_df)
    for idx in src
        col = df[!, idx]
        if all(x -> x isa AbstractDataFrame, col)
            tmp_df = reduce(vcat, col, cols=:union)
        else
            tmp_df = DataFrame()
            for v in col
                append!(tmp_df, v, cols=:union, promote=true)
            end
        end
        hcat!(ref_df, tmp_df, makeunique=makeunique, copycols=false)
    end
    return ref_df
end

"""
    extract(df::AbstractDataFrame, src::ColumnIndex...;
            makeunique::Bool=false)

Extract the contents of one or more columns `cols` in `df` that contain
`NamedTuple`, a `DataFrameRow`, an `AbstractDict` or a `Tables.AbstractRow`
elements returning a data frame with extracted columns, in addition to original
columns. The newly created columns are stored at the end of the data frame (and
the `src` columns are dropped).

Table stored in each row of `src` can have different columns. `missing` value is
pushed to columns missing in a given row that are present in other rows.
The element type of resulting column is determined by promotion of element types
of columns in individual rows.

If `makeunique=false` (the default) produced column names must be unique.
If `makeunique=true` then duplicate column names will be suffixed with `_i`
(`i` starting at `1` for the first duplicate).

TODO: metadata

"""
function extract(df::AbstractDataFrame, src::ColumnIndex...;
                makeunique::Bool=false)
    ref_df = select(df, Not(collect(Any, src)))
    for idx in src
        col = df[!, idx]
        tmp_df = DataFrame()
        for v in col
            push!(tmp_df, v, cols=:union, promote=true)
        end
        hcat!(ref_df, tmp_df, makeunique=makeunique, copycols=false)
    end
    return ref_df
end

"""
    extract!(df::AbstractDataFrame, src::ColumnIndex...;
             makeunique::Bool=false)

Extract in-place the contents of one or more columns `cols` in `df` that
contain `NamedTuple`, a `DataFrameRow`, an `AbstractDict`, or a
`Tables.AbstractRow` elements returning a data frame with extracted columns, in
addition to original columns. The newly created columns are stored at the end
of the data frame (and the `src` columns are dropped).

Table stored in each row of `src` can have different columns. `missing` value is
pushed to columns missing in a given row that are present in other rows.
The element type of resulting column is determined by promotion of element types
of columns in individual rows.

If `makeunique=false` (the default) produced column names must be unique.
If `makeunique=true` then duplicate column names will be suffixed with `_i`
(`i` starting at `1` for the first duplicate).

TODO: metadata

"""
function extract!(df::AbstractDataFrame, src::ColumnIndex...;
                 makeunique::Bool=false)
    tmp_dfs = DataFrame[]
    for idx in src
        col = df[!, idx]
        tmp_df = DataFrame()
        for v in col
            push!(tmp_df, v, cols=:union, promote=true)
        end
        push!(tmp_dfs, tmp_df)
    end
    ref_df = select!(df, Not(collect(Any, src)))
    hcat!(ref_df, tmp_dfs..., makeunique=makeunique, copycols=false)
    return ref_df
end
