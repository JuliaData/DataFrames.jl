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

The general design rules for propagation of table metadata is as follows:
* for all operations that take a single data frame like object
  and return a data frame like object table level metadata is propagated to the
  returned data frame object; similarly operations that mutate a single data
  frame object do not affect table level metadata;
* for all operations that take more than one data frame like object and return a
  data frame like object (e.g. `hcat` or joins) table level metadata is
  preserved only if for some key for all passed tables there is the same value
  of the metadata (e.g., for all tables there is a `"source"` key and the
  value of metadata for this key is the same).
* an exception is `empty` and `empty!` which drop table level metadata.

The general design rules for propagation of column metadata is as follows.
When it is possible to determine statically that column values are unchanged,
are subsetted or are repeated and column name is not changed (like in
`getindex`, `filter`, `subset`, or joins) or explicitly renamed (like in
`rename`, in joins or `hcat`, in `:x => :y`, `:x => identity => :y`,
or `:x => copy => :y` operation specification)
then column metadata is retained; in
the context of operation specification minilanguage (used in `select` and
related functions) this condition means that only using single column selector
like `:col`, or multi-column selector like `[:col1, :col2]`, or `identity` or
`copy` transformation propagates column metadata (but operations like
`:col => (x -> identity(x) => :col` do not propagate metadata although they do
not change the data). Similarly in `mapcols`/`mapcols!` only `identity`
and `copy` transformation preserves metadata;

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

* broadcasting (except for broadcasting assignment into a data frame in which case
  table level metadata and column level metadata for columns that are not changed
  is preserved)
