# Metadata

## Design of metadata support

DataFrames.jl allows you to store and retrieve metadata on table and column
level. This is supported using the functions defined by DataAPI.jl interface:

* for table level metadata: [`metadata`](@ref), [`metadatakes`](@ref),
  [`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref);
* for column level metatadata: [`colmetadata`](@ref), [`colmetadatakeys`](@ref),
  [`colmetadata!`](@ref), [`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

Assume that we work with a data frame-like object `df` that has a column `col`
(referred to either via a `Symbol`, a string or an integer index).

Table level metadata has a form of key-value pairs that are attached to `df`.
Table level metadata has a form of key-value pairs that are attached to
a specific column `col` of `df` data frame.

Additionally each metadata key-value pair has a style information attached to
it.
In DataFrames.jl the metadata style influences how metadata is propagated when
`df` is transformed. The following metadata styles are supported:

* `:none`: metadata having this style is considered to be attached to a concrete
  state of `df`; this means that any operation on this data frame,
  invalidates such metadata and it is dropped in the result of such operation
  (note that this happens even if the operation eventually does not change
  the data frame; the rule is that calling a function that might alter a data
  frame drops such metadata; in this way it is possible to statically determine
  if metadata of styles other than `:note` is dropped after a function call);
  the only exceptions that keep non-:note metadata are (the reason is that these
  operations are specifically designed to create an identical copy of the source
  data frame):
    - [`DataFrame`](@ref) constructor;
    - [`copy`](@ref) of a data frame;
* `:note`: metadata having this style is considered to be an annotation of
  a table or a column that should be propagated under transformations
  (exact propagation rules of such metadata are described below);
* all other metadata styles are allowed but they are currently treated as having
  `:none` style (this might change in the future if other standard metadata
  styles are defined).

All DataAPI.jl functions work with [`DataFrame`](@ref),
[`SubDataFrame`](@ref), [`DataFrameRow`](@ref)
objects, and objects returned by [`eachrow`](@ref), and [`eachcol`](@ref)
functions. In this section collectively these objects will be called
*data frame-like*, and follow the rules:

* objects returned by
  [`eachrow`](@ref), and [`eachcol`](@ref) functions have the same metadata
  as metadata of their parent `AbstractDataFrame`;
* [`SubDataFrame`](@ref) and [`DataFrameRow`](@ref) have only metadata from
  their parent `DataFrame` that has `:note` style.

Notably metadata is not supported for [`GroupedDataFrame`](@ref) as it does not
expose columns directly. You can inspect metadata of `parent` of a
[`GroupedDataFrame`](@ref) or of any of its groups.

!!! note

    DataFrames.jl allows users to extract out columns of a data frame
    and perform operations on them. Such operations will not affect
    metadata. Therefore, even if some metadata has `:none` style it might
    get invalidated if the user mutates columns of a data frame using
    a direct access to them.

### DataFrames.jl specific design principles for use of metadata

DataFrames.jl supports storing any object as metadata values. However,
it is recommended to use strings as values of the metadata,
as some storage formats, like for example Apache Arrow, only support
strings.

For all functions that operate on column level metadata if passed column
is not present in a data frame an `ArgumentError` is thrown.

If [`metadata`](@ref) or [`colmetadata`](@ref) is used to add metadata
on [`SubDataFrame`](@ref) and [`DataFrameRow`](@ref) then:

* using `:none` style for metadata throws an error;
* trying to add key-value pair such that in the parent data frame already
  mapping for key exists with `:none` style throws an error.

DataFrames.jl is designed to be able to take advantage of the fact that
there is no metadata in a data frame. Therefore if you need maximum performance
of operations that do not rely on metadata use `emptymetadata!` and
`emptycolmetadata!` functions on a `DataFrame` you work with.

Processing metadata for `SubDataFrame` and `DataFrameRow` has more overhead
than for other types defined in DataFrames.jl that support metadata, because
they have a more complex logic of handling it (they support only `:note`
metadata, which means that other metadata needs to be filtered-out).

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

julia> metadatakeys(df)
()

julia> metadata!(df, "caption", "ELO ratings of chess players", style=:note);

julia> collect(metadatakeys(df))
1-element Vector{String}:
 "caption"

julia> metadata(df, "caption")
"ELO ratings of chess players"

julia> metadata(df, "caption", style=true)
("ELO ratings of chess players", :note)

julia> emptymetadata!(df);

julia> metadatakeys(df)
()

julia> colmetadatakeys(df)
()

julia> colmetadata!(df, :name, "label", "First and last name of a player", style=:note);

julia> colmetadata!(df, :date, "label", "Rating date in yyyy-u format", style=:note);

julia> colmetadata!(df, :rating, "label", "ELO rating in classical time control", style=:note);

julia> colmetadata(df, :rating, "label")
"ELO rating in classical time control"

julia> colmetadata(df, :rating, "label", style=true)
("ELO rating in classical time control", :note)

julia> collect(colmetadatakeys(df))
3-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
   :date => ["label"]
 :rating => ["label"]
   :name => ["label"]

julia> [only(names(df, col)) =>
        [key => colmetadata(df, col, key) for key in metakeys] for
        (col, metakeys) in colmetadatakeys(df)]
3-element Vector{Pair{String, Vector{Pair{String, String}}}}:
   "date" => ["label" => "Rating date in yyyy-u format"]
 "rating" => ["label" => "ELO rating in classical time control"]
   "name" => ["label" => "First and last name of a player"]

julia> emptycolmetadata!(df);

julia> colmetadatakeys(df)
()
```

## Propagation of `:note` style metadata

An important design feature of `:note` style metatada is how it is handled when
data frames are transformed.

!!! note

    The provided rules might slightly change in the future. Any change to
    `:note` style metadata propagation rules will not be considered as breaking
    and can be done in any minor release of DataFrames.jl.
    Such changes might be made based on users' feedback about what metadata
    propagation rules are most convenient in practice.

The general design rules for propagation of `:note` style metadata are as follows.

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
  in all passed tables and have the same value and all have `:note` style;
* Column level metadata is preserved only for keys which are defined
  in all passed tables that contain this column and have the same value
  and all have `:note` style.

In all these operations when metadata is preserved the values in the key-value
pairs are not copied (this is relevant in case of mutable values).

!!! note

    The rules for `:note` style column level metadata propagation are designed
    to make a right decision in common cases. In particular, they assume that if
    source and target column name is the same then the metadata for the column is
    not changed. This is valid for many operations, however, it is not true
    in general. For example `:x => ByRow(log) => :x` transformation might
    invalidate metadata if it contained unit of measure of a variable. In such
    cases user must set metadata style to `:none` before operation,
    or manually drop or update such metadata from the `:x` column
    after the transformation.

### Operations that preserve `:note` style metadata metadata

Most of the functions in DataFrames.jl just preserve table and column metadata
that has `:note` style.
Below is a list of cases where a more complex logic (following the rules
described above) is applied:

* [`dropmetadata!`](@ref) removes both table level and/or column level metadata
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
