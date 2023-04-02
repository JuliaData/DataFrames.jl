
```@meta
CurrentModule = DataFrames
```

# Indexing

```@index
Pages = ["indexing.md"]
```

## General rules

The following rules explain target functionality of how `getindex`, `setindex!`,
`view`, and broadcasting are intended to work with `DataFrame`, `SubDataFrame`
and `DataFrameRow` objects.

The following values are a valid column index:
* a scalar, later denoted as `col`:
    * a `Symbol`;
    * an `AbstractString`;
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `cols`:
    * a vector of `Symbol` (does not have to be a subtype of
      `AbstractVector{Symbol}`);
    * a vector of `AbstractString` (does not have to be a subtype of
      `AbstractVector{<:AbstractString}`);
    * a vector of `Integer` that are not `Bool` (does not have to be a subtype of
      `AbstractVector{<:Integer}`);
    * a vector of `Bool` (must be a subtype of `AbstractVector{Bool}`);
    * a [regular expression](https://docs.julialang.org/en/v1/manual/strings/#Regular-Expressions)
      (will be expanded to a vector of matching column names);
    * a `Not` expression (see
      [InvertedIndices.jl](https://github.com/JuliaData/InvertedIndices.jl));
      `Not(idx)` selects all indices not in the passed `idx`;
      when passed as column selector `Not(idx...)` is equivalent to
      `Not(Cols(idx...))`.
    * a `Cols` expression (see
      [DataAPI.jl](https://github.com/JuliaData/DataAPI.jl)); `Cols(idxs...)`
      selects the union of the selections in `idxs`; in particular `Cols()`
      selects no columns and `Cols(:)` selects all columns; a special rule is
      `Cols(predicate)`, where `predicate` is a predicate function; in this case
      the columns whose names passed to `predicate` as strings return `true` are
      selected.
    * a `Between` expression (see
      [DataAPI.jl](https://github.com/JuliaData/DataAPI.jl));
      `Between(first, last)` selects the columns between `first` and `last` inclusively;
    * an `All` expression (see
      [DataAPI.jl](https://github.com/JuliaData/DataAPI.jl)); `All()` selects
      all columns, equivalent to `:`;
    * a literal colon `:` (selects all columns).

The following values are a valid row index:
* a scalar, later denoted as `row`:
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `rows`:
    * a vector of `Integer` that are not `Bool` (does not have to be a subtype of
      `AbstractVector{<:Integer}`);
    * a vector of `Bool` (must be a subtype of `AbstractVector{Bool}`);
    * a `Not` expression (see
      [InvertedIndices.jl](https://github.com/JuliaData/InvertedIndices.jl));
    * a literal colon `:` (selects all rows with copying);
    * a literal exclamation mark `!` (selects all rows without copying).

Additionally it is allowed to index into an `AbstractDataFrame` using a
two-dimensional `CartesianIndex`.

In the descriptions below `df` represents a `DataFrame`, `sdf` is a
`SubDataFrame` and `dfr` is a `DataFrameRow`.

`:` always expands to `axes(df, 1)` or `axes(sdf, 1)`.

`df.col` works like `df[!, col]` and `sdf.col` works like `sdf[!, col]` in all
cases. An exception is that under Julia 1.6 or earlier
`df.col .= v` and `sdf.col .= v` performs in-place broadcasting
if `col` is present in `df`/`sdf` and is a valid identifier (this inconsistency
is not present under Julia 1.7 and later).

## `getindex` and `view`

The following list specifies the behavior of `getindex` and `view` operations
depending on argument types.

In particular a description explicitly mentions that the data is *copied* or
*reused without copying*.

For performance reasons, accessing, via `getindex` or `view`, a single `row` and
multiple `cols` of a `DataFrame`, a `SubDataFrame` or a `DataFrameRow` always
returns a `DataFrameRow` (which is a view type).

`getindex` on `DataFrame`:
* `df[row, col]` -> the value contained in row `row` of column `col`, the same
  as `df[!, col][row]`;
* `df[CartesianIndex(row, col)]` -> the same as `df[row, col]`;
* `df[row, cols]` -> a `DataFrameRow` with parent `df`;
* `df[rows, col]` -> a copy of the vector `df[!, col]` with only the entries
                     corresponding to `rows` selected, the same as `df[!, col][rows]`;
* `df[rows, cols]` -> a `DataFrame` containing copies of columns `cols` with
                      only the entries corresponding to `rows` selected;
* `df[!, col]` -> the vector contained in column `col` returned without copying;
                  the same as `df.col` if `col` is a valid identifier.
* `df[!, cols]` -> create a new `DataFrame` with columns `cols` without copying
                   of columns; the same as `select(df, cols, copycols=false)`.

`view` on `DataFrame`:
* `@view df[row, col]` -> a `0`-dimensional view into `df[!, col]` in row `row`,
                          the same as `view(df[!, col], row)`;
* `@view df[CartesianIndex(row, col)]` -> the same as `@view df[row, col]`;
* `@view df[row, cols]` -> the same as `df[row, cols]`;
* `@view df[rows, col]` -> a view into `df[!, col]` with `rows` selected, the
                           same as `view(df[!, col], rows)`;
* `@view df[rows, cols]` -> a `SubDataFrame` with `rows` selected with parent
  `df`;
* `@view df[!, col]` -> a view into `df[!, col]`  with all rows.
* `@view df[!, cols]` -> the same as `@view df[:, cols]`.

`getindex` on `SubDataFrame`:
* `sdf[row, col]` -> a value contained in row `row` of column `col`;
* `sdf[CartesianIndex(row, col)]` -> the same as `sdf[row, col]`;
* `sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)`;
* `sdf[rows, col]` -> a copy of `sdf[!, col]` with only rows `rows` selected,
                      the same as `sdf[!, col][rows]`;
* `sdf[rows, cols]` -> a `DataFrame` containing columns `cols` and
  `sdf[rows, col]` as a vector for each `col` in `cols`;
* `sdf[!, col]` -> a view of entries corresponding to `sdf` in the vector
                   `parent(sdf)[!, col]`; the same as `sdf.col` if `col` is a
                   valid identifier.
* `sdf[!, cols]` -> create a new `SubDataFrame` with columns `cols`, the same
                    parent as `sdf`, and the same rows selected; the same as
                    `select(sdf, cols, copycols=false)`.


`view` on `SubDataFrame`:
* `@view sdf[row, col]` -> a `0`-dimensional view into `df[!, col]` at row
                           `row`, the same as `view(sdf[!, col], row)`;
* `@view sdf[CartesianIndex(row, col)]` -> the same as `@view sdf[row, col]`;
* `@view sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)`;
* `@view sdf[rows, col]` -> a view into `sdf[!, col]` vector with `rows`
                            selected, the same as `view(sdf[!, col], rows)`;
* `@view sdf[rows, cols]` -> a `SubDataFrame` with parent `parent(sdf)`;
* `@view sdf[!, col]` -> a view into `sdf[!, col]` vector with all rows.
* `@view sdf[!, cols]` -> the same as `@view sdf[:, cols]`.

`getindex` on `DataFrameRow`:
* `dfr[col]` -> the value contained in column `col` of `dfr`; the same as
                `dfr.col` if `col` is a valid identifier;
* `dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)`;

`view` on `DataFrameRow`:
* `@view dfr[col]` -> a `0`-dimensional view into
                      `parent(dfr)[DataFrames.row(dfr), col]`;
* `@view dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)`;

Note that views created with columns selector set to `:` change their columns'
count if columns are added/removed/renamed in the parent; if column selector is
other than `:` then view points to selected columns by their number at the
moment of creation of the view.

## `setindex!`

The following list specifies the behavior of `setindex!` operations depending on
argument types.

In particular a description explicitly mentions if the assignment is *in-place*.

Note that if a `setindex!` operation throws an error the target data frame may
be partially changed so it is unsafe to use it afterwards (the column length
correctness will be preserved).

`setindex!` on `DataFrame`:
* `df[row, col] = v` -> set value of `col` in row `row` to `v` in-place;
* `df[CartesianIndex(row, col)] = v` -> the same as `df[row, col] = v`;
* `df[row, cols] = v` -> set row `row` of columns `cols` in-place; the same as
                         `dfr = df[row, cols]; dfr[:] = v`;
* `df[rows, col] = v` -> set rows `rows` of column `col` in-place; `v` must be
                         an `AbstractVector`; if `rows` is `:` and `col` is a
                         `Symbol` or `AbstractString` that is not present in
                         `df` then a new column in `df` is created and holds a
                         `copy` of `v`; equivalent to `df.col = copy(v)` if
                         `col` is a valid identifier;
* `df[rows, cols] = v` -> set rows `rows` of columns `cols` in-place; `v` must
                          be an `AbstractMatrix` or an `AbstractDataFrame` (in
                          this case column names must match);
* `df[!, col] = v` -> replaces `col` with `v` without copying (with the
                      exception that if `v` is an `AbstractRange` it gets
                      converted to a `Vector`); also if `col` is a `Symbol` or
                      `AbstractString` that is not present in `df` then a new
                      column in `df` is created and holds `v`; equivalent to
                      `df.col = v` if `col` is a valid identifier; this is
                      allowed if `ncol(df) == 0 || length(v) == nrow(df)`;
* `df[!, cols] = v` -> replaces existing columns `cols` in data frame `df` with
                       copying; `v` must be an `AbstractMatrix` or an
                       `AbstractDataFrame` (in the latter case column names must
                       match);

`setindex!` on `SubDataFrame`:
* `sdf[row, col] = v` -> set value of `col` in row `row` to `v` in-place;
* `sdf[CartesianIndex(row, col)] = v` -> the same as `sdf[row, col] = v`;
* `sdf[row, cols] = v` -> the same as `dfr = df[row, cols]; dfr[:] = v` in-place;
* `sdf[rows, col] = v` -> set rows `rows` of column `col`, in-place; `v` must be
                          an abstract vector;
* `sdf[rows, cols] = v` -> set rows `rows` of columns `cols` in-place; `v` can
                           be an `AbstractMatrix` or `v` can be
                           `AbstractDataFrame` in which case column names must
                           match;
* `sdf[!, col] = v` -> replaces `col` with `v` with copying; if `col` is present
                       in `sdf` then filtered-out rows in newly created vector
                       are filled with values already present in that column and
                       `promote_type` is used to determine the `eltype` of the
                       new column; if `col` is not present in `sdf` then the
                       operation is only allowed if `sdf` was created with `:`
                       as column selector, in which case filtered-out rows are
                       filled with `missing`; equivalent to `sdf.col = v` if
                       `col` is a valid identifier; operation is allowed if
                       `length(v) == nrow(sdf)`;
* `sdf[!, cols] = v` -> replaces existing columns `cols` in data frame `sdf`
                        with copying; `v` must be an `AbstractMatrix` or an
                        `AbstractDataFrame` (in the latter case column names
                        must match); filtered-out rows in newly created vectors
                        are filled with values already present in respective
                        columns and `promote_type` is used to determine the
                        `eltype` of the new columns;

!!! note

    The rules above mean that `sdf[:, col] = v` is an in-place operation if
    `col` is present in `sdf`, therefore it will be fast in general. On the
    other hand using `sdf[!, col] = v` or `sdf.col = v` will always allocate a
    new vector, which is more expensive computationally.


`setindex!` on `DataFrameRow`:
* `dfr[col] = v` -> set value of `col` in row `row` to `v` in-place;
                    equivalent to `dfr.col = v` if `col` is a valid identifier;
* `dfr[cols] = v` -> set values of entries in columns `cols` in `dfr` by
                     elements of `v` in place; `v` can be: 1) a `Tuple` or an
                     `AbstractArray`, in which cases it must have a number of
                     elements equal to `length(dfr)`, 2) an `AbstractDict`, in
                     which case column names must match, 3) a `NamedTuple` or
                     `DataFrameRow`, in which case column names and order must
                     match;

## Broadcasting

The following broadcasting rules apply to `AbstractDataFrame` objects:
* `AbstractDataFrame` behaves in broadcasting like a two-dimensional collection
  compatible with matrices.
* If an `AbstractDataFrame` takes part in broadcasting then a `DataFrame` is
  always produced as a result. In this case the requested broadcasting operation
  produces an object with exactly two dimensions. An exception is when an
  `AbstractDataFrame` is used only as a source of broadcast assignment into an
  object of dimensionality higher than two.
* If multiple `AbstractDataFrame` objects take part in broadcasting then they
  have to have identical column names.

Note that if broadcasting assignment operation throws an error the target data
frame may be partially changed so it is unsafe to use it afterwards (the column
length correctness will be preserved).

Broadcasting `DataFrameRow` is currently not allowed (which is consistent with
`NamedTuple`).

It is possible to assign a value to `AbstractDataFrame` and `DataFrameRow`
objects using the `.=` operator. In such an operation `AbstractDataFrame` is
considered as two-dimensional and `DataFrameRow` as single-dimensional.

!!! note

    The rule above means that, similar to single-dimensional objects in Base
    (e.g. vectors), `DataFrameRow` is considered to be column-oriented.

Additional rules:
* in the `df[CartesianIndex(row, col)] .= v`, `df[row, col] .= v` syntaxes `v`
  is broadcasted into the contents of `df[row, col]` (this is consistent with
  Julia Base);
* in the `df[row, cols] .= v` syntaxes the assignment to `df` is performed
  in-place;
* in the `df[rows, col] .= v` and `df[rows, cols] .= v` syntaxes the assignment
  to `df` is performed in-place; if `rows` is `:` and `col` is `Symbol` or
  `AbstractString` and it is missing from `df` then a new column is allocated
  and added; the length of the column is always the value of `nrow(df)` before
  the assignment takes place;
* in the `df[!, col] .= v` syntax column `col` is replaced by a freshly
  allocated vector; if `col` is `Symbol` or `AbstractString` and it is missing
  from `df` then a new column is allocated added; the length of the column is
  always the value of `nrow(df)` before the assignment takes place;
* the `df[!, cols] .= v` syntax replaces existing columns `cols` in data frame
  `df` with freshly allocated vectors;
* `df.col .= v` syntax currently performs in-place assignment to an existing
  vector `df.col`; this behavior is deprecated and a new column will be
  allocated in the future. Starting from Julia 1.7 if `:col` is not present in
  `df` then a new column will be created in `df`.
* in the `sdf[CartesianIndex(row, col)] .= v`, `sdf[row, col] .= v` and
  `sdf[row, cols] .= v` syntaxes the assignment to `sdf` is performed in-place;
* in the `sdf[rows, col] .= v` and `sdf[rows, cols] .= v` syntaxes the
  assignment to `sdf` is performed in-place; if `rows` is `:` and `col` is a
  `Symbol` or `AbstractString` referring to a column missing from `sdf` and
  `sdf` was created with `:` as column selector then a new column is allocated
  and added; the filtered-out rows are filled with `missing`;
* in the `sdf[!, col] .= v` syntax column `col` is replaced by a freshly
  allocated vector; the filtered-out rows are filled with values already present
  in `col`; if `col` is a `Symbol` or `AbstractString` referring to a column
  missing from `sdf` and was `sdf` created with `:` as column selector then a
  new column is allocated and added; in this case the filtered-out rows are
  filled with `missing`;
* the `sdf[!, cols] .= v` syntax replaces existing columns `cols` in data frame
  `sdf` with freshly allocated vectors; the filtered-out rows are filled with
  values already present in `cols`;
* `sdf.col .= v` syntax currently performs in-place assignment to an existing
  vector `sdf.col`; this behavior is deprecated and a new column will be
  allocated in the future. Starting from Julia 1.7 if `:col` is not present in
  `sdf` then a new column will be created in `sdf` if `sdf` was created with `:`
  as a column selector.
* `dfr.col .= v` syntax is allowed and performs in-place assignment to a value
  extracted by `dfr.col`.

Note that `sdf[!, col] .= v` and `sdf[!, cols] .= v` syntaxes are not allowed as
`sdf` can be only modified in-place.

If column indexing using `Symbol` or `AbstractString` names in `cols` is
performed, the order of columns in the operation is specified by the order of
names.

## Indexing `GroupedDataFrame`s

A [`GroupedDataFrame`](@ref) can behave as either an `AbstractVector` or
`AbstractDict` depending on the type of index used. Integers (or arrays of them)
trigger vector-like indexing while `Tuples`s and `NamedTuple`s trigger
dictionary-like indexing. An intermediate between the two is the
[`GroupKey`](@ref) type returned by [`keys(::GroupedDataFrame)`](@ref), which
behaves similarly to a `NamedTuple` but has performance on par with integer
indexing.

The elements of a `GroupedDataFrame` are [`SubDataFrame`](@ref)s of its parent.

* `gd[i::Integer]` -> Get the `i`th group.
* `gd[key::NamedTuple]` -> Get the group corresponding to the given values of
  the grouping columns. The fields of the `NamedTuple` must match the grouping
  columns columns passed to [`groupby`](@ref) (including order).
* `gd[key::Tuple]` -> Same as previous, but omitting the names on `key`.
* `get(gd, key::Union{Tuple, NamedTuple}, default)` -> Get group for key `key`,
  returning `default` if it does not exist.
* `gd[key::GroupKey]` -> Get the group corresponding to the [`GroupKey`](@ref)
  `key` (one of the elements of the vector returned by
  [`keys(::GroupedDataFrame)`](@ref)). This should be nearly as fast as integer
  indexing.
* `gd[a::AbstractVector]` -> Select multiple groups and return them in a new
  `GroupedDataFrame` object. Groups may be selected by integer position using an
  array of `Integer`s or `Bool`s, similar to a standard array. Alternatively the
  array may contain keys of any of the types supported for dictionary-like
  indexing (`GroupKey`, `Tuple`, or `NamedTuple`). Selected groups must be
  unique, and different types of indices cannot be mixed.
* `gd[n::Not]` -> Any of the above types wrapped in `Not`. The result will be a
   new `GroupedDataFrame` containing all groups in `gd` *not* selected by the
   wrapped index.

# Common API for types defined in DataFrames.jl

This table presents return value types of calling `names`, `propertynames`,
`keys`, `length` and `ndims` on types exposed to the user by DataFrames.jl:

| Type                | `names`          | `propertynames`  | `keys`           | `length`  | `ndims` |
|---------------------|------------------|------------------|------------------|-----------|---------|
| `AbstractDataFrame` | `Vector{String}` | `Vector{Symbol}` | undefined        | undefined | `2`     |
| `DataFrameRow`      | `Vector{String}` | `Vector{Symbol}` | `Vector{Symbol}` | `Int`     | `1`     |
| `DataFrameRows`     | `Vector{String}` | `Vector{Symbol}` | vector of `Int`  | `Int`     | `1`     |
| `DataFrameColumns`  | `Vector{String}` | `Vector{Symbol}` | `Vector{Symbol}` | `Int`     | `1`     |
| `GroupedDataFrame`  | `Vector{String}` | tuple of fields  | `GroupKeys`      | `Int`     | `1`     |
| `GroupKeys`         | undefined        | tuple of fields  | vector of `Int`  | `Int`     | `1`     |
| `GroupKey`          | `Vector{String}` | `Vector{Symbol}` | `Vector{Symbol}` | `Int`     | `1`     |

Additionally the above types `T` (i.e. `AbstractDataFrame`, `DataFrameRow`,
`DataFrameRows`, `DataFrameColumns`, `GroupedDataFrame`, `GroupKeys`,
`GroupKey`) the following methods are defined:
* `size(::T)` returning a `Tuple` of `Int`.
* `size(::T, ::Integer)` returning an `Int`.
* `axes(::T)` returning a `Tuple` of `Int` vectors.
* `axes(::T, ::Integer)` returning an `Int` vector for a valid dimension (except
   `DataFrameRows` and `GroupKeys` for which `Base.OneTo(1)` is also returned
   for a dimension higher than a valid one because they are `AbstractVector`).
* `firstindex(::T)` returning `1` (except `AbstractDataFrame` for which it is
  undefined).
* `firstindex(::T, ::Integer)` returning `1` for a valid dimension (except
   `DataFrameRows` and `GroupKeys` for which `1` is also returned for a
   dimension higher than a valid one because they are `AbstractVector`).
* `lastindex(::T)` returning `Int` (except `AbstractDataFrame` for which it is
  undefined).
* `lastindex(::T, ::Integer)` returning `Int` for a valid dimension  (except
   `DataFrameRows` and `GroupKeys` for which `1` is also returned for a
   dimension higher than a valid one because they are `AbstractVector`).
