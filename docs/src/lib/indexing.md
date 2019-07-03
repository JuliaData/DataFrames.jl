
```@meta
CurrentModule = DataFrames
```

# Indexing

```@index
Pages = ["indexing.md"]
```

## General rules

The following rules explain target functionality of how `getindex`, `setindex!`, `view`, and broadcasting are intended to work with `DataFrame`, `SubDataFrame` and `DataFrameRow` objects.

The rules for a valid type of index into a column are the following:
* a value, later denoted as `col`:
    * a `Symbol`;
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `cols`:
    * a vector of `Symbol` (does not have to be a subtype of `AbstractVector{Symbol}`);
    * a vector of `Integer` other than `Bool` (does not have to be a subtype of `AbstractVector{<:Integer}`);
    * a vector of `Bool` that has to be a subtype of `AbstractVector{Bool}`;
    * a regular expression, which gets expanded to a vector of matching column names;
    * a `Not` expression;
    * a colon `:`.

The rules for a valid type of index into a row are the following:
* a value, later denoted as `row`:
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `rows`:
    * a vector of `Integer` other than `Bool` (does not have to be a subtype of `AbstractVector{<:Integer}`);
    * a vector of `Bool` that has to be a subtype of `AbstractVector{Bool}`;
    * a `Not` expression;
* a colon `:`;
* an exclamation mark `!`.

Additionally it is allowed to index into an `AbstractDataFrame` using a two-dimensional `CartesianIndex`.

In the descriptions below `df` represents a `DataFrame`, `sdf` is a `SubDataFrame` and `dfr` is a `DataFrameRow`.

Note that `df.col` works like `df[!, col]` when getting column from an `AbstractDataFrame` or broadcasting into `df.col`,
but it works like `df[:, col]` when used in setting column context. This behavior was chosen
as it is typically what is expected.

## `getindex` and `view`

The following list specifies the behavior of `getindex` and `view` operations depending on argument types.

In particular a description explicitly mentions that the data is *copied* or *reused without copying*.

For performance reasons, accessing, via `getindex` or `view`, a single `row` and multiple `cols` of a `DataFrame`,
a `SubDataFrame` or a `DataFrameRow` always returns a `DataFrameRow` (which is a view type).

`getindex` on `DataFrame`:
* `df[row, col]` -> the value contained in row `row` of column `col`, the same as `df[!, col][row]`;
* `df[CartesianIndex(row, col)]` -> the same as `df[row,col]`;
* `df[row, cols]` -> a `DataFrameRow` with parent `df` if `cols` is a colon or with parent `df[!, cols]` otherwise;
* `df[rows, col]` -> a copy of the vector `df[!, col]` with only the entries corresponding to `rows` selected,
                     the same as `df[!, col][rows]`;
* `df[rows, cols]` -> a `DataFrame` containing copies of columns `cols` with only the entries corresponding to `rows` selected;
* `df[:, col]` -> a copy of vector contained in column `col` of `df`;
* `df[:, cols]` -> a `DataFrame` containing copies of columns `cols`; the same as `select(df, cols)`;
* `df[!, col]` -> the vector contained in column `col` returned without copying; the same as `df.col` if `col` is a valid identifier;
* `df[!, cols]` -> a freshly allocated `DataFrame` holding column vectors corresponding to `cols` (without copying);
                   the same as `select(df, cols, copycols=false)`;

`view` on `DataFrame`:
* `@view df[row, col]` -> a `0`-dimensional view into `df[!, col]` in row `row`, the same as `view(df[!, col], row)`;
* `@view df[CartesianIndex(row, col)]` -> the same as `@view df[row, col]`;
* `@view df[row, cols]` -> the same as `df[row, cols]`;
* `@view df[rows, col]` -> a view into `df[!, col]` with `rows` selected, the same as `view(df[!, col], rows)`;
* `@view df[rows, cols]` -> a `SubDataFrame` with `rows` selected with parent `df` if `cols` is a colon and `df[!, cols]` otherwise;
* `@view df[:, col]` -> a view into `df[!, col]`, the same as `view(df[!, col], :)`;
* `@view df[:, cols]` -> a `SubDataFrame` with all rows selected with parent `df` if `cols` is a colon and `df[!, cols]` otherwise;
* `@view df[!, col]` -> the same as `@view df[:, col]`;
* `@view df[!, cols]` -> the same as `@view df[:, cols]`.

`getindex` on `SubDataFrame`:
* `sdf[row, col]` -> a value contained in row `row` of column `col`;
* `sdf[CartesianIndex(row, col)]` -> the same as `sdf[row,col]`;
* `sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `sdf[rows, col]` -> a copy of `sdf[!, col]` with only rows `rows` selected, the same as `sdf[!, col][rows]`;
* `sdf[rows, cols]` -> a `DataFrame` containing columns `cols` and `sdf[rows, col]` as a vector for each `col` in `cols`;
* `sdf[:, col]` -> a copy of `sdf[!, col]`;
* `sdf[:, cols]` -> a `DataFrame` containing columns `cols` and `df[:, col]` as a vector for each `col` in `cols`;
* `sdf[!, col]` -> a view of entries corresponding to `sdf` in the vector `parent(sdf)[!, col]`;
                   the same as `sdf.col` if `col` is a valid identifier;
* `sdf[!, cols]` -> a `SubDataFrame`, with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[!, cols]` otherwise;

`view` on `SubDataFrame`:
* `@view sdf[row, col]` -> a `0`-dimensional view into `df[!, col]` at row `row`, the same as `view(sdf[!, col], row)`;
* `@view sdf[CartesianIndex(row, col)]` -> the same as `@view sdf[row, col]`;
* `@view sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[!, cols]` otherwise;
* `@view sdf[rows, col]` -> a view into `sdf[!, col]` vector, the same as `view(sdf[!, col], rows)`;
* `@view sdf[rows, cols]` -> a `SubDataFrame` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[!, cols]` otherwise;
* `@view sdf[:, col]` -> the same as `sdf[!, col]`;
* `@view sdf[:, cols]` -> the same as `sdf[!, cols]`;
* `@view sdf[!, col]` -> the same as `sdf[!, col]`;
* `@view sdf[!, cols]` -> the same as `sdf[!, cols]`.

`getindex` on `DataFrameRow`:
* `dfr[col]` -> the value contained in column `col` of `dfr`; the same as `dfr.col` is `col` is a valid identifier;
* `dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)` if `cols` is a colon and `parent(dfr)[cols]` otherwise;

`view` on `DataFrameRow`:
* `@view dfr[col]` -> a `0`-dimensional view into `parent(dfr)[DataFrames.row(dfr), col]`;
* `@view dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)` if `cols` is a colon and `parent(dfr)[cols]` otherwise;

Note that views created with columns selector set to `:` change their columns'
names and count if columns are added/removed/renamed in the parent;

## `setindex!`

The following list specifies the behavior of `setindex!` operations depending on argument types.

In particular a description explicitly mentions if the assignment is *in-place*,
*replaces old vectors without copying source* or *replaces old vectors copying source*.

`setindex!` on `DataFrame`:
* `df[row, col] = v` -> set value of `col` in row `row` to `v` in-place;
* `df[CartesianIndex(row, col)] = v` -> the same as `df[row, col] = v`;
* `df[row, cols] = v` -> set row `row` of columns `cols` in-place; the same as `dfr = df[row, cols]; dfr[:] = v`;
* `df[rows, col] = v` -> set rows `rows` of column `col` in-place; `v` must be an `AbstractVector`;
* `df[rows, cols]` -> set rows `rows` of columns `cols` in-place; `v` must be an `AbstractMatrix` or an `AbstractDataFrame`
                      (in this case column names must match);
* `df[:, col] = v` -> similar to `df[1:nrow(df), col] = v` but replaces old vector with a copy of `v`;
                      the same as `df.col = v` if `col` is a valid identifier;
                      if `col` is a `Symbol` that is not present in `df` then a new column is added to `df`;
* `df[:, cols] = v` -> similar to `df[1:nrow(df), cols] = v` but replaces old vectors with a copy of source;
* `df[!, col] = v` -> similar to `df[1:nrow(df), col] = v` but replaces old vectors without copying source;
                      also if `col` is a `Symbol` that is not present in `df` then a new column in `df` is created without copying of source;
* `df[!, cols] = v` -> similar to `df[1:nrow(df), cols] = v` but replaces old vectors without copying source if `v` is an `AbstractDataFrame`.

`setindex!` on `SubDataFrame`:
* `sdf[row, col]` -> set value of `col` in row `row` to `v` in-place;
* `sdf[CartesianIndex(row, col)] = v` -> the same as `sdf[row, col] = v`;
* `sdf[row, cols]` -> the same as `dfr = df[row, cols]; dfr[:] = v` in-place;
* `sdf[rows, col]` -> set rows `rows` of column `col`, in-place; `v` can be an abstract vector;
* `sdf[rows, cols]` -> set rows `rows` of columns `cols` in-place;
                       `v` can be an `AbstractMatrix` or `v` can be `AbstractDataFrame` when column names must match;
* `sdf[:, col]` -> the same as `sdf[1:nrow(df), col] = v`; the same as `sdf.col = v` if `col` is a valid identifier;
* `sdf[:, cols]` -> the same as `sdf[1:nrow(df), cols] = v`;
* `sdf[!, col]` -> the same as `sdf[1:nrow(df), col] = v`;
* `sdf[!, cols]` -> the same as `sdf[1:nrow(df), cols] = v`;

`setindex!` on `DataFrameRow`:
* `dfr[col] = v` -> set value of `col` in row `row` to `v` in-place;
* `dfr[cols] = v` -> set values of entries in columns `cols` in `dfr` by elements of `v` in place;
                     `v` can be an `AbstractVector` or `v` can be a `NamedTuple` or `DataFrameRow` when column names must match;

## Broadcasting

The following broadcasting rules apply to `AbstractDataFrame` objects:
* `AbstractDataFrame` behaves in broadcasting like a two-dimensional collection compatible with matrices.
* If an `AbstractDataFrame` takes part in broadcasting then a `DataFrame` is always produced as a result.
  In this case the requested broadcasting operation produces an object with exactly two dimensions.
  An exception is when an `AbstractDataFrame` is used only as a source of broadcast assignment into an object
  of dimensionality higher than two.
* If multiple `AbstractDataFrame` objects take part in broadcasting then they have to have identical column names.

Broadcasting `DataFrameRow` is currently not allowed (which is consistent with `NamedTuple`).

It is possible to assign a value to `AbstractDataFrame` and `DataFrameRow` objects using the `.=` operator.
In such an operation `AbstractDataFrame` is considered as two-dimensional and `DataFrameRow` as single-dimensional.

!!! note

    The rule above means that, similar to single-dimensional objects in Base (e.g. vectors),
    `DataFrameRow` is considered to be column-oriented.

Additional rules:
* in the `df[CartesianIndex(row, col)] .= v`, `df[row, col] .= v` and `df[row, cols] .= v` syntaxes the assignment to `df` is performed in-place;
* in the `df[rows, col] .= v` and `df[rows, cols] .= v` syntaxes the assignment to `df` is performed in-place;
* in the `df[:, col] .= v` and `df[:, cols] .= v` syntaxes the assignment to `df` is performed by allocation of fresh columns;
* in the `df[!, col] .= v` and `df[!, cols] .= v` syntaxes the assignment to `df` is performed in-place;
* `df.col .= v` syntax is allowed and performs in-place assignment to an existing vector `df.col`.
* in the `sdf[CartesianIndex(row, col)] .= v`, `sdf[row, col] .= v` and `sdf[row, cols] .= v` syntaxes the assignment to `sdf` is performed in-place;
* in the `sdf[rows, col] .= v` and `sdf[rows, cols] .= v` syntaxes the assignment to `sdf` is performed in-place;
* in the `sdf[:, col] .= v` and `sdf[:, cols] .= v` syntaxes the assignment to `sdf` is performed in-place;
* in the `sdf[!, col] .= v` and `sdf[!, cols] .= v` syntaxes the assignment to `sdf` is performed in-place;
* `sdf.col .= v` syntax is allowed and performs in-place assignment to an existing vector `sdf.col`.

If column indexing using `Symbol` names in `cols` is performed the order of columns in the operation is specified
by the order of names.

`df[!, col] .= v` and `df[:, col] .= v` is allowed when `col` is a `Symbol` even if `col` is not present
in the `DataFrame` under the condition that `df` is not empty: a new column will be created and freshly allocated.
On the contrary, `df.col .= v` is not allowed if `col` is not present in `df`.
