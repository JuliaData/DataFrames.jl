# Metadata

## Design of metadata support

DataFrames.jl allows you to store and retrieve metadata on table and column
level. This is supported using the functions defined by DataAPI.jl interface:
[`hasmetadata`](@ref), [`hascolmetadata`](@ref),
[`metadata`](@ref) and [`colmetadata`](@ref).
These functions work with [`DataFrame`](@ref),
[`SubDataFrame`](@ref), [`DataFrameRow`](@ref), [`GroupedDataFrame`](@ref)
objects, and objects returned by [`eachrow`](@ref), and [`eachcol`](@ref)
functions. In this section collectively these objects will be called
*data frame-like*.

Additionally DataFrames.jl defines [`dropallmetadata!`](@ref) the function that
removes both table level and column level metadata from a data frame.

Assume that we work with a data frame-like object `df` that has a column `col`
(referred to either via a `Symbol`, a string or an integer index).

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

In DataFrames.jl context `hascolmetadata(df, col)` can return either `true` or `false`.
If `false` is returned this means that column `col` of data frame `df` does not
have any column level metadata defined. If `true` is returned it means that for
`col` column some metadata is defined.

If `col` is not present in `df` an error is thrown.

Additionally `hascolmetadata(df)` returns `true` if for any column `col` of `df`
`hascolmetadata(df, col)` returns `true`. Otherwise `false` is returned.

Although `hascolmetadata` is guaranteed to return a `Bool` value in DataFrames.jl,
in generic code it is recommended to check its return value against `true` and
`false` explicitly using the `===` operator. The reason is that, in code
accepting any Tables.jl table, `hascolmetadata` is also allowed to return
`nothing` if the queried object does not support attaching metadata to a column.

### Contract for `metadata`

In DataFrames.jl `metadata(df)` always returns an `AbstractDict{String, Any}` storing
key-value mappings of table level metadata. To add or update metadata mutate
the returned dictionary.

### Contract for `colmetadata`

In DataFrames.jl `colmetadata(df, col)` always returns an `AbstractDict{String, Any}` storing
key-value mappings of column level metadata for column `col`.
To add or update metadata mutate the returned dictionary.

If `col` is not present in `df` an error is thrown.

### General design principles for use of metadata

DataFrames.jl supports storing any object as metadata values. However, 
it is recommended to use strings as values of the metadata,
as some storage formats, like for example Apache Arrow, only support
strings.

The `metadata` and `colmetadata` functions called on objects defined in
DataFrames.jl are not thread safe and should not be used in multi-threaded code.
In particular, as an implementation detail, the first time `metadata` is called
on a data frame object that previously did not have any metadata stored, it will
mutate it (this might change in the future versions of DataFrames.jl).

When working interactively with DataFrames.jl you can safely just rely on the
`metadata` and `colmetadata` functions. This will create a minimal overhead
in case the passed data frame does not have metadata yet, but it should not be
noticeable in typical usage scenarios.

In generic code or code that is performance critical it is recommended to check that
`hasmetadata` returns `true` before calling `metadata`,
and that `hascolmetadata` returns `true` before calling `colmetadata`.
There are two reasons for this:

* if some Tables.jl table (other than data frame) does not support table or column level
  metadata (respectively) the call to `metadata` or `colmetadata` throws an error.
* if you know you query a data frame, checking `hasmetadata` or `hascolmetadata` avoids
  the creation of a metadata dictionary in case it were not needed.
  A call to `metadata` or `colmetadata` will create such a dictionary when it was not present
  in the data frame yet.

## Examples

Here is a simple example how you can work with metadata in DataFrames.jl:

```jldoctest dataframe
julia> using DataFrames

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

julia> names(df) .=> colmetadata.(Ref(df), names(df))
3-element Vector{Pair{String, Dict{String, Any}}}:
   "name" => Dict("label" => "First and last name of a player")
   "date" => Dict("label" => "Rating date in yyyy-u format")
 "rating" => Dict("label" => "ELO rating in classical time control")

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
* `merge!` to add source object metadata to destination object metadata
  (overwriting duplicates).

## Propagation of metadata

An important design feature of metatada is how it is handled when
data frames are  transformed.

!!! note

    The provided rules might change in the future. Any change to metadata
    propagation rules will not be considered as breaking
    and can be done in any minor release of DataFrames.jl.
    Such changes might be made based on users' feedback about what metadata
    propagation rules are most convenient in practice.

The general design rules for propagation of table metadata are as follows.

For operations that take a single data frame as an input:
* Table level metadata is propagated to the returned data frame object.
* For column level metadata:
  - in all cases when a single column is transformed to
    a single column and the name of the column does
    not change (or is automatically changed e.g. to de-duplicate column names or
    via column renaming in joins)
    column level metadata is preserved (example operations of this kind are
    `getindex`, `subset`, joins, `mapcols`).
  - in all cases when a single column is transformed with `identity` or `copy` to a single column,
    column level metadata is preserved even if column name is changed (example
    operations of this kind are `rename`, or the `:x => :y` or
    `:x => copy => :y` operation specification in `select`).

For operations that take multiple data frames as their input two cases are distinguished:
- when there is a natural main table in the operation (`append!`, `prepend!`,
  `leftjoin`, `leftjoin!`, `rightjoin`, `semijoin`, `antijoin`, `setindex!`);
- when all tables are equivalent (`hcat`, `vcat`, `innerjoin`, `outerjoin`).

In the situation when there is a main table:
* Table level metadata is taken from the main table.
* Column level metadata for columns from the main table is taken from main table.
  Column level metadata for columns from the non-main table is taken only for
  columns not present in the main table.

In the situation when all tables are equivalent:
* Table level metadata is preserved only for keys which are defined
  in all passed tables and have the same value;
* Column level metadata is preserved only for keys which are defined
  in all passed tables that contain this column and have the same value

In all these operations when metadata is preserved the values in the key-value
pairs are not copied (this is relevant in case of mutable values).

!!! note

    The rules for column level metadata propagation are designed to make
    a right decision in common cases. In particular, they assume that if source
    and target column name is the same then the metadata for the column is
    not changed. This is valid for many operations, however, it is not true
    in general. For example `:x => ByRow(log) => :x` transformation might
    invalidate metadata if it contained unit of measure of a variable. In such
    cases user must manually drop or update such metadata from the `:x` column
    after the transformation.

### Operations that preserve metadata

Most of the functions in DataFrames.jl just preserve table and column metadata.
Below is a list of cases where a more complex logic (following the rules
described above) is applied:

* [`dropallmetadata!`](@ref) removes both table level and column level metadata
  from a data frame; note that removing metadata can speed up certain operations.
* [`describe`](@ref) preserves only table level metadata;
  column level metadata is dropped.
* [`hcat`](@ref): propagates table level metadata only for keys which are defined
  in all passed tables and have the same value.
* [`vcat`](@ref): propagates table level metadata only for keys which are defined
  in all passed tables and have the same value;
  column level metadata is preserved only for keys which are defined
  in all passed tables that contain this column and have the same value;
* [`stack`](@ref): propagates table level metadata and column level metadata
  for identifier columns.
* [`stack`](@ref): propagates table level metadata and column level metadata
  for row keys columns.
* [`permutedims`](@ref): propagates table level metadata and drops column level
   metadata.
* broadcasted assignment does not change target metadata
* broadcasting propagates table level metadata if some key is present
  in all passed data frames and value associated with it is identical in all
  passed data frames; column level metadata is propagated for columns if some
  key for a given column is present in all passed data frames and value
  associated with it is identical in all passed data frames.
* `getindex` preserves table level metadata and column level metadata
  for selected columns
* `setindex!` does not affect table level and column level metadata
* [`push!`](@ref), [`pushfirst!`](@ref), [`insert!`](@ref) do not affect
  data frame metadata (even if they add new columns and pushed row is
  a `DataFrameRow` or other value supporting metadata interface)
* [`append!`](@ref) and [`prepend!`](@ref) do not change table and column level
  metadata of the destination data frame, except that if new columns are added
  and these columns have metadata in the appended/prepended table then this
  metadata is preserved.
* [`leftjoin!`](@ref), [`leftjoin`](@ref): table and column level metadata is
  taken from the left table except for non-key columns from right table for which
  metadata is taken from right table;
* [`rightjoin`](@ref): table and column level metadata is taken from the right
  table except for non-key columns from left table for which metadata is
  taken from left table;
* [`innerjoin`](@ref), [`outerjoin`](@ref): propagates table level metadata only for keys
  that are defined in all passed data frames and have the same value;
  column level metadata is propagated for all columns except for key
  columns, for which it is propagated only for keys that are defined
  in all passed data frames and have the same value.
* [`semijoin`](@ref), [`antijoin`](@ref): table and column level metadata is
  taken from the left table.
* [`crossjoin`](@ref): propagates table level metadata only for keys
  that are defined in both passed data frames and have the same value;
  propagates column level metadata from both passed data frames.
* [`select`]](@ref), [`select!`](@ref), [`transform`](@ref),
  [`transform!`](@ref), [`combine`]](@ref): propagate table level metadata;
  Column metadata is propagated if:
  a) a single column is transformed to a single column and the name of the column does not change
     (this includes all column selection operations), or
  b) a single column is transformed with `identity` or `copy` to a single column
     even if column name is changed (this includes column renaming).
