
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

Additionally, the `eachrow` function returns a value of the `DFRowVector` type, which
serves as an iterator over rows of an `AbstractDataFrame`, returning `DataFrameRow` objects.

Similarly, the `eachcol` and `columns` functions return a value of the `DFColumnVector` type, which
serves as an iterator over columns of an `AbstractDataFrame`.
The difference between the return value of `eachcol` and `columns` is the following:

* The `eachcol` function returns a value of the `DFColumnVector{<:AbstractDataFrame, true}` type, which is an
  iterator returning a pair containing the column name and the column vector.
* The `columns` function returns a value of the `DFColumnVector{<:AbstractDataFrame, false}` type, which is an
  iterator returning the column vector only.

The `DFRowVector` and `DFColumnVector` types are subtypes of `AbstractVector` and support its interface
with the exception that they are read only. Note, that they are not exported and should not be constructed directly,
but using `eachrow`, `eachcol` and `columns` functions.

## Types specification

```@docs
AbstractDataFrame
DataFrame
DataFrameRow
GroupedDataFrame
SubDataFrame
DFRowVector
DFColumnVector
```
