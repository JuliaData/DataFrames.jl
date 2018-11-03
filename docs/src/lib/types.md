
```@meta
CurrentModule = DataFrames
```

# Types

```@index
Pages = ["types.md"]
```

## Type hierarchy design

`AbstractDataFrame` is an abstract type that provides an interface for working with tabular data.

A key subtype of the `AbstractDataFrame` type is a `DataFrame` type that uses columnar data storage.

The package provides a `SubDataFrame` type that is also `AbstractDataFrame` subtype.
The `SubDataFrame` type is a view into `DataFrame`. It stores only a reference to parent `DataFrame`
and information about which rows from the parent are selected. Typically it is created by using the `view`
function or is returned by indexing into a `GroupedDataFrame` object.

A `GroupedDataFrame` type is a type that allows to store the information about the result of grouping
operation performed on a data frame. It is intended to be created as a result of a call to the `groupby` function.

A `DataFrameRow` type is a view into a single row of an `AbstractDataFrame`. It stores only a reference
to a parent `AbstractDataFrame` and information about which row from the parent are selected.
The `DataFrameRow` type supports iteration over columns of a row and is similar in functionality to
the `NamedTuple` type, but allows for modification of data stored in the parent `AbstractDataFrame`.
Typically objects of `DataFrameRow` type are encountered when returned by `eachrow` function.
In the future accessing a single row of a data frame via `getindex` or `view` will return a `DataFrameRow`.

## Types specification

```@docs
AbstractDataFrame
DataFrame
DataFrameRow
GroupedDataFrame
SubDataFrame
```
