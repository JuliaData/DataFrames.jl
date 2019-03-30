
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
It stores only a reference to the parent `DataFrame` and information about which rows and columns
from the parent are selected (both as integer indices referring to the parent).
Typically it is created using the `view` function or is returned by indexing into a `GroupedDataFrame` object.

`GroupedDataFrame` is a type that stores the result of a  grouping operation performed on an `AbstractDataFrame`.
It is intended to be created as a result of a call to the `groupby` function.

`DataFrameRow` is a view into a single row of an `AbstractDataFrame`. It stores only a reference
to a parent `DataFrame` and information about which row and columns from the parent are selected
(both as integer indices referring to the parent)
The `DataFrameRow` type supports iteration over columns of the row and is similar in functionality to
the `NamedTuple` type, but allows for modification of data stored in the parent `DataFrame`
and reflects changes done to the parent after the creation of the view.
Typically objects of the `DataFrameRow` type are encountered when returned by the `eachrow` function,
or when accessing a single row of a `DataFrame` or `SubDataFrame` via `getindex` or `view`.

The `eachrow` function returns a value of the `DataFrameRows` type, which
serves as an iterator over rows of an `AbstractDataFrame`, returning `DataFrameRow` objects.

Similarly, the `eachcol` function returns a value of the `DataFrameColumns` type, which
serves as an iterator over columns of an `AbstractDataFrame`.
The return value can have two concrete types:

* If the `eachcol` function is called with the `names` argument set to `true` then it returns a value of the
  `DataFrameColumns{<:AbstractDataFrame, Pair{Symbol, AbstractVector}}` type, which is an
  iterator returning a pair containing the column name and the column vector.
* If the `eachcol` function is called with `names` argument set to `false` (the default) then it returns a value of the
  `DataFrameColumns{<:AbstractDataFrame, AbstractVector}` type, which is an
  iterator returning the column vector only.

The `DataFrameRows` and `DataFrameColumns` types are subtypes of `AbstractVector` and support its interface
with the exception that they are read only. Note that they are not exported and should not be constructed directly,
but using the `eachrow` and `eachcol` functions.

The `RepeatedVector` and `StackedVector` types are subtypes of `AbstractVector` and support its interface
with the exception that they are read only. Note that they are not exported and should not be constructed directly,
but they are columns of a `DataFrame` returned by `stackdf` and `meltdf`.

## Types specification

```@docs
AbstractDataFrame
DataFrame
DataFrameRow
GroupedDataFrame
SubDataFrame
DataFrameRows
DataFrameColumns
RepeatedVector
StackedVector
```
