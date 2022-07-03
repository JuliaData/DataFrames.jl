# Metadata

## Design of metadata support

DataFrames.jl allows you to store and retrieve metadata on table and column
level. This is supported using the functions defined by DataAPI.jl interface:
[`hasmetadata`](@ref), [`hascolmetadata`](@ref),
[`metadata`](@ref) and [`colmetadata`](@ref).
These functions work with [`DataFrame`](@ref),
[`SubDataFrame`](@ref), [`DataFrameRow`](@ref), [`GroupedDataFrame`](@ref),
[`DataFrameRows`](@ref), and [`DataFrameColumns`](@ref) objects. In this
section collectively these objects will be called *data frame like*.

Additionally DataFrames.jl defines [`dropallmetadata!`](@ref) function that
removes both table level and column level metadata from a data frame.

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

Additionally `hascolmetadata(df)` returns `true` if for any column `col` of `df`
`hascolmetadata(df, col)` returns `true`. Otherwise `false` is returned.

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
julia> df = DataFrame(name=["Jan Krzysztof Duda", "Jan Krzysztof Duda",
                            "Radosław Wojtaszek", "Radosław Wojtaszek"],
                      date=["2022-Jun", "2021-Jun", "2022-Jun", "2021-Jun"],
                                     rating=[2750, 2729, 2708, 2687])
4×3 DataFrame
 Row │ name                date      rating
     │ String              String    Int64
─────┼──────────────────────────────────────
   1 │ Jan Krzysztof Duda  2022-Jun    2750
   2 │ Jan Krzysztof Duda  2021-Jun    2729
   3 │ Radosław Wojtaszek  2022-Jun    2708
   4 │ Radosław Wojtaszek  2021-Jun    2687

julia> hasmetadata(df)
false

julia> df_meta = metadata(df)
Dict{String, Any}()

julia> df_meta["caption"] = "ELO ratings of chess players"
"ELO ratings of chess players"

julia> hasmetadata(df)
true

julia> metadata(df)
Dict{String, Any} with 1 entry:
  "caption" => "ELO ratings of chess players"

julia> empty!(df_meta)
Dict{String, Any}()

julia> hasmetadata(df)
false

julia> metadata(df)
Dict{String, Any}()

julia> hascolmetadata(df)
false

julia> hascolmetadata(df, :rating)
false

julia> colmetadata(df, :name)["label"] = "First and last name of a player"
"First and last name of a player"

julia> colmetadata(df, :date)["label"] = "Rating date in yyyy-u format"
"Rating date in yyyy-u format"

julia> colmetadata(df, :rating)["label"] = "ELO rating in classical time control"
"ELO rating in classical time control"

julia> hascolmetadata(df)
true

julia> hascolmetadata(df, :rating)
true

julia> colmetadata(df, :rating)
Dict{String, Any} with 1 entry:
  "label" => "ELO rating in classical time control"

julia> colmetadata.(Ref(df), names(df))
3-element Vector{Dict{String, Any}}:
 Dict("label" => "First and last name of a player")
 Dict("label" => "Rating date in yyyy-u format as string")
 Dict("label" => "ELO rating in classical time control")

julia> foreach(col -> empty!(colmetadata(df, col)), names(df))

julia> hascolmetadata(df)
false

julia> hascolmetadata(df, :rating)
false

julia> colmetadata(df, :rating)
Dict{String, Any}()
```

As a practical tip if you have metadata attached to some object
(either data frame or data frame column) and you want to propagate it to
some new object you can use:
* `copy!` to fully overwrite destination object metadata with source object
  metadata;
* `merge!` to add destination object metadata with source object metadata
  (overwriting duplicates).

## Propagation of metadata

An important design feature of metatada is how it is handled when
data frames are  transformed.

!!! note

    The provided rules might change in the future. Any change to metadata
    propagation rules will not be considered to be a breaking change
    in DataFrames.jl and can be done in any minor release of DataFrames.jl.
    Such changes might be made based on users' feedback about what metadata
    propagation rules are most convenient in practice.

The general design rules for propagation of table metadata is as follows.

For operations that take a single data frame as an input:
* Table level metadata is propagated to the returned data frame object.
* For column level metadata:
  - in all cases when a single column from a source data frame is transformed to
    a single column in a destination data frame and the name of the column does
    not change (or is automatically changed e.g. to de-duplicate column names or
    via column renaming in joins)
    column level metadata is preserved (example operations of this kind are
    `getindex`, `subset`, joins, `mapcols`).
  - in all cases when a single column from a source data frame is transformed
    with `identity` or `copy` to a single column in a destination data frame
    column level metadata is preserved even if column name is changed (example
    operations of this kind are `rename` function call or `:x => :y` or
    `:x => copy => :y` operation specification in `select`).

For operations that take a multiple data data frames as an input two cases are
defined.
Case 1 is when there is a natural main table in the operation (`append!`, `prepend!`,
`leftjoin`, `leftjoin!`, `rightjoin`, `semijoin`, `antijoin`, `setindex!`).
Case 2 is when all tables are equivalent (`hcat`, `vcat`, `innerjoin`, `outerjoin`).

In the situation when there is a main table:
* Table level metadata is kept from the main table.
* Column level metadata for columns from the main table is taken from main table.
  Column level metadata for columns from the non-main table is taken only for
  columns not present in the main table.

In the situation when all tables are equivalent:
* Table level metadata is preserved only if for some key for all passed tables
  there is the same value of the metadata (e.g., for all tables there is a `"source"`
  key and the value of metadata for this key is the same).
* Column level metadata is preserved only if for some key for source column in all
  passed tables that contain this column there is the same value of the column
  metadata (e.g., for all tables there is a `"source"` key for source column and
  the value of metadata for this key is the same).

In all these operations when metadata is preserved the values in the key-value
pairs are not copied (this is relevant in case of mutable values).

!!! note

    The rules for column level metadata propagation are designed to make
    a right decision in common cases. In particular, they assume that if source
    and target column name is the same then the metadata for the column is
    not changed. This is valid for many operations, however, it is not true
    in general. For example `:x => ByRow(log) => :x` transformation might
    invalidate metadata if it contained unit of measure of a variable. In such
    cases user must manually drop or update such matadata from the `:x` column
    after the transformation.

### Operations that preserve metadata

Most of the functions in DataFrames.jl just preserve table and column metadata.
Below is a list of cases where a more complex logic (following the rules
described above) is applied:

* [`dropallmetadata!](@ref) removes both table level and column level metadata
  from a data frame; note that removing metadata can speed up certain operations.
* [`describe`](@ref) preserves only table level metadata;
  column level metadata is dropped.
* [`hcat`](@ref): propagates table level metadata if some key is present
  in all passed data frames and value associated with it is identical in all
  passed data frames; propagates all column level metadata.
* [`vcat`](@ref): propagates table level metadata if some key is present
  in all passed data frames and value associated with it is identical in all
  passed data frames; column level metadata is propagated for columns if some
  key for a given column is present in all passed data frames that contain this
  column and value associated with it is identical in all passed data frames.
* [`stack`](@ref): propagates table level metadata and column level metadata
  for identifier columns.
* [`stack`](@ref): propagates table level metadata and column level metadata
  for row keys columns.
* [`permutedims`](@ref): propagates table level metadata and drops column level
   metadata.

* `setindex!` does not affect table level and column level metadata


* `rename!`
* `rename`
* `empty`
* `empty!`
* `similar`
* `only`
* `first`
* `last`
* `dropmissing`
* `dropmissing!`
* `filter`
* `filter!`
* `unique`
* `unique!`
* `fillcombinations`
* `repeat`
* `repeat!`
* `disallowmissing`
* `allowmissing`
* `disallowmissing!`
* `allowmissing!`
* `flatten`
* `reverse`
* `reverse!`
* `permute!`
* `invpermute!`
* `shuffle`
* `shuffle!`
* `insertcols`
* `insertcols!`
* `mapcols!`
* `mapcols`
* `sort`
* `sort!`
* `subset`
* `subset!`
* `DataFrame`
* `copy`
* `view`
* `groupby`
* `eachrow`
* `eachcol`

* `getindex`
* `setindex!`
* `deleteat!`
* `innerjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `leftjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `leftjoin!`(for table level metadata only if it is present and identical for all passed data frames)
* `rightjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `outerjoin`(for table level metadata only if it is present and identical for all passed data frames)
* `semijoin`(for table level metadata only if it is present and identical for all passed data frames)
* `antijoin`(for table level metadata only if it is present and identical for all passed data frames)
* `crossjoin`(for table level metadata only if it is present and identical for all passed data frames)
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
* `select[!]`, `transform[!]`, `combine`

# Operations that drop table and column level metadata

* TODO (this might change): broadcasting (except for broadcasting assignment into a data frame in which case
  table level metadata and column level metadata for columns that are not changed
  is preserved)
