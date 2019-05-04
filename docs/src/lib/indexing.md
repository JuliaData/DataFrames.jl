
```@meta
CurrentModule = DataFrames
```

# Indexing

```@index
Pages = ["indexing.md"]
```

## General rules

The following rules explain target functionality of how `getindex`, `setindex!`, and `view` are intended to work with `DataFrame`, `SubDataFrame` and `DataFrameRow` objects.

The rules for a valid type of index into a column are the following:
* a value, later denoted as `col`:
    * a `Symbol`;
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `cols`:
    * a vector of `Symbol` (does not have to be a subtype of `AbstractVector{Symbol}`);
    * a vector of `Integer` other than `Bool` (does not have to be a subtype of `AbstractVector{<:Integer}`);
    * a vector of `Bool` that has to be a subtype of `AbstractVector{Bool}`.
    * a colon.

The rules for a valid type of index into a row are the following:
* a value, later denoted as `row`:
    * an `Integer` that is not `Bool`;
* a vector, later denoted as `rows`:
    * a vector of `Integer` other than `Bool` (does not have to be a subtype of `AbstractVector{<:Integer}`);
    * a vector of `Bool` that has to be a subtype of `AbstractVector{Bool}`;
    * a colon.

In the descriptions below `df` represents a `DataFrame`, `sdf` is a `SubDataFrame` and `dfr` is a `DataFrameRow`.

## `getindex`

The following list specifies return types of `getindex` operations depending on argument types.

In all operations copying vectors is avoided where possible.
If it is performed a description explicitly mentions that the data is *copied*.

For performance reasons, accessing, via `getindex` or `view`, a single `row` and multiple `cols` of a `DataFrame`, a `SubDataFrame` or a `DataFrameRow` always returns a `DataFrameRow` (which is a view-like type).

`DataFrame`:
* `df[col]` -> the vector contained in column `col`;
* `df[cols]` -> a freshly allocated `DataFrame` containing the copies of vectors contained in columns `cols`;
* `df[row, col]` -> the value contained in row `row` of column `col`, the same as `df[col][row]`;
* `df[row, cols]` -> a `DataFrameRow` with parent `df` if `cols` is a colon and `df[cols]` otherwise;
* `df[rows, col]` -> a copy of the vector `df[col]` with only the entries corresponding to `rows` selected, the same as `df[col][rows]`;
* `df[rows, cols]` -> a `DataFrame` containing copies of columns `cols` with only the entries corresponding to `rows` selected.
* `@view df[col]` -> the vector contained in column `col` (this is equivalent to `df[col]`);
* `@view df[cols]` -> a `SubDataFrame` with parent `df` if `cols` is a colon and `df[cols]` otherwise;
* `@view df[row, col]` -> a `0`-dimensional view into `df[col]`, the same as `view(df[col], row)`;
* `@view df[row, cols]` -> a `DataFrameRow` with parent `df` if `cols` is a colon and `df[cols]` otherwise;
* `@view df[rows, col]` -> a view into `df[col]` with `rows` selected, the same as `view(df[col], rows)`;
* `@view df[rows, cols]` -> a `SubDataFrame` with `rows` selected with parent `df` if `cols` is a colon and `df[cols]` otherwise.

`SubDataFrame`:
* `sdf[col]` -> a view of the vector contained in column `col` of `parent(sdf)` with `DataFrames.rows(sdf)` as a selector;
* `sdf[cols]` -> a `SubDataFrame`, with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `sdf[row, col]` -> a value contained in row `row` of column `col`;
* `sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `sdf[rows, col]` -> a copy of a vector `sdf[col]` with only rows `rows` selected;
* `sdf[rows, cols]` -> a `DataFrame` containing columns `cols` and `df[rows, col]` as a vector in each `col` in `cols`.
* `@view sdf[col]` -> a view of vector contained in column `col` of `parent(sdf)` with `DataFrames.rows(sdf)` as selector;
* `@view sdf[cols]` -> a `SubDataFrame` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `@view sdf[row, col]` -> translates to `view(sdf[col], row)` (a `0`-dimensional view into `df[col]`);
* `@view sdf[row, cols]` -> a `DataFrameRow` with parent `parent(sdf)` if `cols` is a colon and `parent(sdf)[cols]` otherwise;
* `@view sdf[rows, col]` -> translates to `view(sdf[col], rows)` (a standard view into `sdf[col]` vector);
* `@view sdf[rows, cols]` -> a `SubDataFrame` with parent `parent(sdf)` if `cols` is a colon and `sdf[cols]` otherwise.

`DataFrameRow`:
* `dfr[col]` -> the value contained in column `col` of `dfr`;
* `dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)` if `cols` is a colon and `parent(dfr)[cols]` otherwise;
* `@view dfr[col]` -> a `0`-dimensional view into `parent(dfr)[DataFrames.row(dfr), col]`;
* `@view dfr[cols]` -> a `DataFrameRow` with parent `parent(dfr)` if `cols` is a colon and `parent(dfr)[cols]` otherwise;

## `setindex!`, `setproperty!` and broadcasted assignment

The following list the effect of `setindex!`, `setproperty!` and broadcasted assignment and operations.

In all operations copying is performed in general.
If it is not performed a description explicitly mentions that the data is assigned *in-place*.

### `DataFrame`:

> `df[col] = v` and `df.col = v`

* `v` must be an `AbstractVector`. If `ncol(df) > 0` then `length(v)` must be equal to `nrow(df)`.
* `v` is assigned to `df` without copying unless it is a range, in which case it is converted to a `Vector`.
* `haskey(df, col)` must hold unless `col isa Symbol`, in which case a new column is added at the end of `df`.

> `df[col] .= v` and `df.col .= v`

* `v` must be broadcastable.
* if `haskey(df, col)` then standard in-place broadcasted assignment of `v` to the column vector `df[col]` is performed.
* if not `haskey(df, col)` and `col isa Symbol`
    * if `ncol(df) > 0` then a new verctor `c` of an appropriate type is created, having `nrow(df)` entries;
      then `c .= v` and `df.col = c` is performed.
    * if `ncol(df) == 0` then same operation as in the case above is performed, whith the following additional rules:
        * if `v` has 0-dimensions then 1 row is created,
        * if `v` has 1-dimension the number of rows equal to its length are created,
        * if `v` has more than 1 dimension then an error is thrown
* if not `haskey(df, col)` and `!(col isa Symbol)` an error is thrown

> `df[cols] = v`

* passed columns are copied like in `DataFrame` constructor with `copycols=true`;
* if `v` is an `AbstractDataFrame` of vectors:
    * `nrow(df)` must be equal to number of rows in `v` unless `ncol(df)==0` in
      which case it is enough that number of rows in each entry of `v` is constant;
    * number of columns in `v` must be equal to length of `cols`
    * before making any assignment `cols` is transformed to `Symbols` using `names(df)`
      (which means that passing `Integer` or `Bool` will fail for nonexistent columns,
      but adding new columns as `Symbol` is allowed)
    * then `df[col] = copy(v[col])` is called for each column name `col`
* in the future allowing of `NamedTuple`, `AbstractDict` and `AbstractMatrix` as `v` is planned

> `df[cols] .= v`

* before making any assignment `cols` is transformed to `Symbols` using `names(df)`
  (which means that passing `Integer` or `Bool` will fail for missing columns,
  but adding new columns as `Symbol` is allowed)
* if `v` is an `AbstractDataFrame`:
    * number of columns in `v` must be equal to length of `cols`
    * before making any assignment `cols` is transformed to `Symbols` using `names(df)`
      (which means that passing `Integer` or `Bool` will fail for missing columns,
      but adding new columns as `Symbol` is allowed)
    * then `df[col] .= v[col]` for `col in cols` is performed
* otherwise it is broadcasted to call `df[col] .= v` for all `col in cols`
* in the future allowing of `NamedTuple`, `AbstractDict` and `AbstractMatrix` as `v` is planned

> `df[row, col] = v`

* translates to `df[col][row] = v`

> `df[row, col] .= v`

* translates to `df[col][row] .= v`

> `df[row, cols] = v`

* currently this functionality is deprecated
* in the future the following functionality is planned
    * if `v` is a `DataFrameRow` or `NamedTuple` or `AbstractDict` then
        * `length(v)` must be equal to `length(cols)`
        * before making any assignment `cols` is transformed to `Symbols` using `names(df)`
        * column names in `v` must be the same as selected by `cols`
        * an operation `df[row, col] = v[col]` for `col in cols` is performed
    * if `v` is a iterable:
        * `length(v)` must be equal to `length(cols)`
        * an operation `df[row, col] = v[i]` for `(i, col) in enumerate(cols)` is performed
    * otherwise an error is thrown

> `df[row, cols] .= v`

* currently this functionality is undefined
* in the future the following functionality is planned
    * if `v` is a `DataFrameRow` or `NamedTuple` or `AbstractDict` then
        * number of columns in `v` must be equal to length(cols)
        * column names in v must be the same as selected by `cols`
        * an operation `df[row, col] .= v[col]` for `col in cols` is performed
    * if `v` is 0-dimensional:
        * an operation `df[row, col] .= v` for `col in cols` is performed
    * if `v` is 1-dimensional:
        * number of elements in `v` must be equal to length(cols)
        * an operation `df[row, col] .= v[i]` for `(i, col) in enumerate(cols)` is performed
    * otherwise an error is thrown

> `df[rows, col] = v`

* The same rules as for `df[col] = v` but on selected rows and always with copying.
* Data frames with zero columns are not allowed and no column adding is possible.

> `df[rows, col] .= v`

* The same rules as for `df[col] .= v` but on selected rows and always with copying.
* Data frames with zero columns are not allowed and no column adding is possible.

> `df[rows, cols] = v`

* The same rules as for `df[cols] = v` but on selected rows and always with copying.
* Data frames with zero columns are not allowed and no column adding is possible.

> `df[rows, cols] .= v`

* The same rules as for `df[cols] .= v` but on selected rows and always with copying.
* Data frames with zero columns are not allowed and no column adding is possible.

Additionally for all operations:
* If `rows` is `:` then it gets expanded to `axes(df, 1)` before any action.
* if `cols` is `:` then it gets expanded to `axes(df, 2)` before any action.

### `SubDataFrame`:

If `sdf` is a `SubDataFrame` before any operation the following translations take place:
* `sdf[colidx]` to `parent(sdf)[sdf.rows, parentcols(index(sdf), colidx)]`
* `sdf[rowidx, colidx]` to `parent(sdf)[sdf.rows[rowidx], parentcols(index(sdf), colidx)]`

After this translation rules for `DataFrame` assignement and broadcasting apply.

### `DataFrameRow`:

If `dfr` is a `DataFrameRow` before any operation the following translations take place:
* `dfr[idx]` to `parent(dfr)[dfr.row, parentcols(index(dfr), idx)]`

After this translation rules for `DataFrame` assignement and broadcasting apply.
