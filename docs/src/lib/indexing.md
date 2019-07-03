
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

The following list specifies return types of `getindex` and `view` operations depending on argument types.

In particular a description explicitly mentions that the data is *copied* or *reused without copying*.

For performance reasons, accessing, via `getindex` or `view`, a single `row` and multiple `cols` of a `DataFrame`,
a `SubDataFrame` or a `DataFrameRow` always returns a `DataFrameRow` (which is a view-like type).

`getindex` on `DataFrame`:
* `df[CartesianIndex(row, col)]` -> the same as `df[row,col]`;
* `df[row, col]` -> the value contained in row `row` of column `col`, the same as `df[!, col][row]`;
* `df[row, cols]` -> a `DataFrameRow` with parent `df` if `cols` is a colon or with parent `df[!, cols]` otherwise;
* `df[rows, col]` -> a copy of the vector `df[!, col]` with only the entries corresponding to `rows` selected,
                     the same as `df[!, col][rows]`;
* `df[rows, cols]` -> a `DataFrame` containing copies of columns `cols` with only the entries corresponding to `rows` selected;
* `df[:, col]` -> a copy of vector contained in column `col` of `df`;
* `df[:, cols]` -> a `DataFrame` containing copies of columns `cols`; the same as `select(df, cols)`;
* `df[!, col]` -> the vector contained in column `col` returned without copying; the same as `df.col` if `col` is a valid identifier;
* `df[!, cols]` -> a freshly allocated `DataFrame` containing vectors contained in columns `cols` (without copying of the vectors);
                   the same as `select(df, cols, copycols=false)`;

`view` on `DataFrame`:
* `@view df[CartesianIndex(row, col)]` -> the same as `@view df[row, col]`;
* `@view df[row, col]` -> a `0`-dimensional view into `df[!, col]` in row `row`, the same as `view(df[!, col], row)`;
* `@view df[row, cols]` -> the same as `df[row, cols]`;
* `@view df[rows, col]` -> a view into `df[!, col]` with `rows` selected, the same as `view(df[!, col], rows)`;
* `@view df[rows, cols]` -> a `SubDataFrame` with `rows` selected with parent `df` if `cols` is a colon and `df[!, cols]` otherwise;
* `@view df[:, col]` -> a view into `df[!, col]`, the same as `view(df[!, col], :)`;
* `@view df[:, cols]` -> a `SubDataFrame` with all rows selected with parent `df` if `cols` is a colon and `df[!, cols]` otherwise;
* `@view df[!, col]` -> the same as `@view df[:, col]`;
* `@view df[!, cols]` -> the same as `@view df[:, cols]`.

`getindex` on `SubDataFrame`:
* `sdf[CartesianIndex(row, col)]` -> the same as `sdf[row,col]`;
* `sdf[row, col]` -> a value contained in row `row` of column `col`;
* `sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `sdf[rows, col]` -> a copy of a vector `sdf[!, col]` with only rows `rows` selected;
* `sdf[rows, cols]` -> a `DataFrame` containing columns `cols` and `sdf[rows, col]` as a vector for each `col` in `cols`;
* `sdf[:, col]` -> a copy of a vector `sdf[!, col]`;
* `sdf[:, cols]` -> a `DataFrame` containing columns `cols` and `df[:, col]` as a vector for each `col` in `cols`;
* `sdf[!, col]` -> a view of the vector contained in column `col` of `parent(sdf)` with `DataFrames.rows(sdf)` as a selector;
                   the same as `sdf.col` if `col` is a valid identifier;
* `sdf[!, cols]` -> a `SubDataFrame`, with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[!, cols]` otherwise;

`view` on `SubDataFrame`:
* `@view sdf[CartesianIndex(row, col)]` -> the same as `@view sdf[row, col]`;
* `@view sdf[row, col]` -> translates to `view(sdf[!, col], row)` (a `0`-dimensional view into `df[!, col]` at row `row`);
* `@view sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[!, cols]` otherwise;
* `@view sdf[rows, col]` -> translates to `view(sdf[!, col], rows)` (a standard view into `sdf[!, col]` vector);
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

Note that when views are created with columns selector set to `:` then the view produced dynamically changes its columns'
names and count if columns of parents are added/removed/renamed;

## `setindex!`

The following list specifies return types of `setindex!` operations depending on argument types.

In particular a description explicitly mentions if the assignment is *in-place*,
*replaces old vectors without copying source* or *replaces old vectors with copying source*.

`setindex!` on `DataFrame`:
* `df[CartesianIndex(row, col)] = v` -> the same as `df[row, col] = v`;
* `df[row, col] = v` -> set value of `col` in row `row` to `v` in-place;
* `df[row, cols] = v` -> the same as `dfr = df[row, cols]; dfr[:] = v` in-place;
* `df[rows, col] = v` -> set rows `rows` of column `col` in-place; `v` can be an abstract vector;
* `df[rows, cols]` -> set rows `rows` of columns `cols` in-place; `v` can be an `AbstractMatrix` or `v` can be `AbstractDataFrame`
                      (in this case column names must match);
* `df[:, col] = v` -> the same as `df[1:nrow(df), col] = v` but replaces old vectors with a copy of source;
                      the same as `df.col = v` if `col` is a valid identifier;
                      also if `col` is a `Symbol` that is not present in `df` then a new column in `df` is created;
* `df[:, cols] = v` -> the same as `df[1:nrow(df), cols] = v` but replaces old vectors with a copy of source;
* `df[!, col] = v` -> the same as `df[1:nrow(df), col] = v` but replaces old vectors without copying source;
                      also if `col` is a `Symbol` that is not present in `df` then a new column in `df` is created without copying of source;
* `df[!, cols] = v` -> the same as `df[1:nrow(df), cols] = v` but replaces old vectors without copying source if `v` is an `AbstractDataFrame`.

`setindex!` on `SubDataFrame`:
* `sdf[CartesianIndex(row, col)] = v` -> the same as `sdf[row, col] = v`;
* `sdf[row, col]` -> set value of `col` in row `row` to `v` in-place;
* `sdf[row, cols]` -> the same as `dfr = df[row, cols]; dfr[:] = v` in-place;
* `sdf[rows, col]` -> set rows `rows` of column `col` in-place; `v` can be an abstract vector;
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

Broadcasting `DataFrameRow` is currently not allowed.

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
