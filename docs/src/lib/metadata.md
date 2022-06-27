# Metadata

## Design of metadata support

DataFrames.jl allows you to store and retrieve metadata on table and column
level. This is supported using the functions defined by DataAPI.jl interface:
`hasmetadata`, `hascolmetadata`, `metadata` and `colmetadata`.
These functions work with `DataFrame`, `SubDataFrame`, `DataFrameRow`,
`GroupedDataFrame`, `DataFrameRows`, and `DataFrameColumns` objects. In this
section collectively these objects will be called *data frame like*.

Assume that we work with a data frame `df` that has a column `:col`.

### Contract for `hasmetadata`

In DataFrames.jl context `hasmetadata(df)` can return either `true` or `false`.
If `false` is returned this means that data frame `df` does not have any table
level metadata defined. If `true` is returned it means that at table level some
metadata is defined for `df`.

Although `hasmetadata` is guaranteed to return `Bool` value in DataFrames.jl in
generic code it is recommended to check its return value against `true` and
`false` explicitly using the `===` operator. The reason is that, in code
accepting any Tables.jl table, `hasmetadata` is also allowed to return `nothing`
if the queried object does not support attaching metadata.

### Contract for `hascolmetadata`

In DataFrames.jl context `hascolmetadata(df, :col)` (alternatively string or
column number can be used) can return either `true` or `false`.
If `false` is returned this means that column `:col` of data frame `df` does not
have any column level metadata defined. If `true` is returned it means that for
`:col` column some metadata is defined.

If `:col` is not present in `df` an error is thrown.

Although `hascolmetadata` is guaranteed to return `Bool` value in DataFrames.jl
in generic code it is recommended to check its return value against `true` and
`false` explicitly using the `===` operator. The reason is that, in code
accepting any Tables.jl table, `hascolmetadata` is also allowed to return
`nothing` if the queried object does not support attaching metadata to a column.

### Contract for `metadata`

In DataFrames.jl `metadata(df)` always returns `AbstractDict{String, Any}` storing
key-value mappings of table level metadata. To add or update metadata mutate
the returned dictionary.

### Contract for `colmetadata`

In DataFrames.jl `colmetadata(df, :col)` (alternatively string or
column number can be used) always returns `AbstractDict{String, Any}` storing
key-value mappings of column level metadata for column `:col`.
To add or update metadata mutate the returned dictionary.

If `:col` is not present in `df` an error is thrown.

### General design principles for use of metadata

It is recommended to use strings as values of the metadata. The reason
is that some storage formats, like for example Apache Arrow, only support
storing string data as metadata values.

The `metadata` and `colmetadata` functions called on objects defined in
DataFrames.jl are not thread safe and should not be used in multi-threaded code.
In particular, as an implementation detail, the first time `metadata` is called
on a data frame object that previously did not have any metadata stored, it will
mutate it (this might change in the future versions of DataFrames.jl).

When working interactively with DataFrames.jl you can safely just rely on the
`metadata` and `colmetadata` functions. This will create a minimal overhead
in case some data frame does not have metadata yet, but it should not be
noticeable in typical usage scenarios.

In generic code or code that is performance critical it is recommended to run
`hasmetadata` check before calling `metadata` function (the same considerations
apply to `hascolmetadata` and `colmetadata`). There are two reasons for this:

* if some Tables.jl table (other than data frame) does not support metadata the
  call to `metadata` function throws an error. A similar rule applies to calling
  `hascolmetadata` before calling `colmetadata`.
* if you know you query a data frame, using the `hasmetadata` function avoids
  creation of metadata dictionary in a data frame in case it were not needed.
  A call to `metadata` will create such a dictionary when it was not present
  in a data frame yet.

## Examples

Here is a simple example how you can work with metadata in DataFrames.jl:

```jldoctest dataframe
julia> df = DataFame(name=["Jan Krzysztof Duda", "Jan Krzysztof Duda",
                           "Radosław Wojtaszek", "Radosław Wojtaszek"],
                     date=["2022-Jun", "2021-Jun", "2022-Jun", "2021-Jun"],
                     rating=[2750, 2729, 2708, 2687])
julia> hasmetadata(df)
julia> df_meta = metadata(df)
julia> df_meta["name"] = "ELO ratings of chess players"
julia> hasmetadata(df)
julia> metadata(df)
julia> empty!(df_meta)
julia> hasmetadata(df)
julia> metadata(df)
julia> hascolmetadata(df, :rating)
julia> rating_meta = colmetadata(df, :rating)
julia> rating_meta["name"] = "First and last name of a player"
julia> rating_meta["rating"] = "ELO rating in classical time control"
julia> rating_meta["date"] = "Rating date in yyyy-u format for \"english\" locale"
julia> hascolmetadata(df, :rating)
julia> colmetadata(df, :rating)
julia> colmetadata.(Ref(df), names(df))
julia> empty!(rating_meta)
julia> hascolmetadata(df, :rating)
julia> colmetadata(df, :rating)
```

As a practical tip if you have metadata attached to some object
(either data frame or data frame column) and you want to propagate it to
some new object you can use:
* `copy!` to fully overwrite destination object metadata with source object
  metadata;
* `merge!` to add destination object metadata with source object metadata
  (overwriting duplicates).

## Propagation of metadata

An important design feature of metatada is how it is handled when you transform
data frames.

!!! note

    The provided rules might change in the future. Any change to metadata
    propagation rules will not be considered to be a breaking change
    in DataFrames.jl and can be done in any minor release of DataFrames.jl.
    Such changes might be made based on users' feedback about what metadata
    propagation rules are most convenient in practice.

The general design rules for propagation of table metadata is as follows.

For table level metadata:
* for all operations that take a single data frame like object
  and return a data frame like object table level metadata is propagated to the
  returned data frame object; similarly operations that mutate a single data
  frame object do not affect table level metadata;
* for all operations that take more than one data frame like object and return a
  data frame like object (e.g. `hcat` or joins) table level metadata is
  preserved only if for some key for all passed tables there is the same value
  of the metadata (e.g., for all tables there is a `"source"` key and the
  value of metadata for this key is the same).

For column level metadata:
* in all cases when a single column from a source data frame is transformed to a
  single column in a destination data frame and the name of the column does not
  change (or is automatically changed e.g. to de-duplicate column names) column
  level metadata is preserved (example operations of this kind are `getindex`,
  `subset`, joins, `mapcols`).
* in all cases when a single column from a source data frame is transformed with
  `identity` or `copy` to a single column in a destination data frame column
  level metadata is preserved even if column name is changed (example operations
  of this kind are `rename` function call or `:x => :y` or `:x => copy => :y`
  operation specification in `select`).
* for all operations that take more than one data frame like object and return a
  data frame like object where a single column in destination data frame is
  created from multiple columns from a source data frames (e.g. `vcat` or joins)
  column level metadata is preserved only if for some key for source column in
  all source passed tables there is the same value of the metadata (e.g., for
  all tables there is a `"source"` key for source column and the value of
  metadata for this key is the same).

!!! note

    The rules for column level metadata propagation are designed to make
    a right decision in common cases. In particular, they assume that if source
    and target column name is the same then the metadata for the column is
    not changed. This is valid for many operations, however, it is not true
    in general. For example `:x => ByRow(log) => :x` transformation might
    invalidate metadata if it contained unit of measure of a variable. In such
    cases user must manually drop or update such matadata from the `:x` column
    after the transformation.

TODO: a decision needs to be made how we propagate metadata in the following case:
  ```
  julia> df = DataFrame(a=1:3, b=11:13, c=111:113)
3×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1     11    111
   2 │     2     12    112
   3 │     3     13    113

julia> df2 = DataFrame(a=1111:1112, b=11111:11112)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │  1111  11111
   2 │  1112  11112

julia> df[1:2, 1:2] = df2
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │  1111  11111
   2 │  1112  11112

julia> df
3×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │  1111  11111    111
   2 │  1112  11112    112
   3 │     3     13    113
  ```
(following the rules above we should use intersection of both table level
and column level metadata since `setindex!` in this case took two source data frames)

The concrete functions listed below follow these general principles.

### Operations that preserve metadata

TODO: the list below is not finished do not read it yet

* `mapcols!`
* `rename!`
* `only`
* `mapcols`
* `rename`
* `first`
* `last`
* `describe` (column level metadata is dropped)
* `dropmissing`
* `dropmissing!`
* `filter`
* `filter!`
* `unique`
* `unique!`
* `fillcombinations`
* `repeat`
* `disallowmissing`
* `disallowmissing!`
* `allowmissing`
* `allowmissing!`
* `flatten`
* `reverse`
* `reverse!`
* `permute!`
* `invpermute!`
* `shuffle`
* `shuffle!`
* `insertcols` (column level metadata present for previously existing columns)
* `insertcols!` (column level metadata present for previously existing columns)
* `stack` (column level metadata is dropped)
* `unstack` (column level metadata is dropped)
* `permutedims` (column level metadata is dropped)
* `sort`
* `sort!`
* `subset`
* `subset!`
* `DataFrame`
* `getindex` (if multiple column selector is used)
* `setindex!` (if a column is replaced then its column metadata is dropped unless,
               a source is another data frame, in which case metadata is copied)
* `copy`
* `deleteat!`
* `allowmissing!`
* `disallowmissing!`
* `repeat!`
* `view` (if `DataFrameRow` or `SubDataFrame` is produced)
* `groupby`
* `vcat` (for table level and column level metadata only if it is present and identical for all passed data frames)
* `hcat`(for table level metadata only if it is present and identical for all passed data frames)
* `innerjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `leftjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `leftjoin!`(for table level metadata only if it is present and identical for all passed data frames)
* `rightjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `outerjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `semijoin`(for table level metadata only if it is present and identical for all passed data frames)
* `antijoin`(for table level metadata only if it is present and identical for all passed data frames)
* `crossjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `empty`
* `empty!`
* `similar`
* `keepat!`
* `resize!`
* `pop!`
* `popfirst!`
* `popat!`
* `push!`
* `pushfirst!`
* `insert!`
* `append!`
* `prepend!`

TODO: `select[!]`, `transform[!]`, `combine`

# Operations that drop table and column level metadata

* TODO (this might change): broadcasting (except for broadcasting assignment into a data frame in which case
  table level metadata and column level metadata for columns that are not changed
  is preserved)
