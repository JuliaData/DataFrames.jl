
```@meta
CurrentModule = DataFrames
```

# Types

```@index
Pages = ["types.md"]
```

## Type hierarchy design

`AbstractDataFrame` is an abstract type that provides an interface for data frame types.
It is not intended as a fully generic interface for working with tabular data, which is the role of
interfaces defined by [Tables.jl](https://github.com/JuliaData/Tables.jl/) instead.

`DataFrame` is the most fundamental subtype of `AbstractDataFrame`, which stores a set of columns
as `AbstractVector` objects.

`SubDataFrame` is an `AbstractDataFrame` subtype representing a view into a `DataFrame`.
It stores only a reference to the parent `DataFrame` and information about which rows from the parent are selected.
Typically it is created using the `view` function or is returned by indexing into a `GroupedDataFrame` object.

`GroupedDataFrame` is a type that stores the result of a  grouping operation performed on an `AbstractDataFrame`.
It is intended to be created as a result of a call to the `groupby` function.

`DataFrameRow` is a view into a single row of an `AbstractDataFrame`. It stores only a reference
to a parent `AbstractDataFrame` and information about which row from the parent is selected.
The `DataFrameRow` type supports iteration over columns of the row and is similar in functionality to
the `NamedTuple` type, but allows for modification of data stored in the parent `AbstractDataFrame`
and reflects changes done to the parent after the creation of the view.
Typically objects of the `DataFrameRow` type are encountered when returned by the `eachrow` function.
In the future accessing a single row of a data frame via `getindex` or `view` will return a `DataFrameRow`.

Additionally `eachrow` and `eachcol` functions return values of `DFRowIterator` and `DFColumnIterator` types respectively.
Those types are not exported and should not be constructed directly.
They respectively serve as iterators over rows and columns of an `AbstractDataFrame.

## Types specification

```@docs
AbstractDataFrame
DataFrame
DataFrameRow
GroupedDataFrame
SubDataFrame
DFRowIterator
DFColumnIterator
```
