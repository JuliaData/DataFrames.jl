
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

The `DataFrameRows` and `DataFrameColumns` types are subtypes of `AbstractVector` and support its interface
with the exception that they are read only. Note that they are not exported and should not be constructed directly,
but using the `eachrow` and `eachcol` functions.

The `RepeatedVector` and `StackedVector` types are subtypes of `AbstractVector` and support its interface
with the exception that they are read only. Note that they are not exported and should not be constructed directly,
but they are columns of a `DataFrame` returned by `stack` with `view=true`.

The `ByRow` type is a special type used for selection operations to signal that the wrapped function should be applied
to each element (row) of the selection.

The `AsTable` type is a special type used for selection operations to signal that the columns selected by a wrapped
selector should be passed as a `NamedTuple` to the function.

## [The design of handling of columns of a `DataFrame`](@id man-columnhandling)

When a `DataFrame` is constructed columns are copied by default. You can disable
this behavior by setting `copycols` keyword argument to `false` or by using the
[`DataFrame!`](@ref) function.
The exception is if an `AbstractRange` is passed as a column, then it is always collected to a `Vector`.

Also functions that transform a `DataFrame` to produce a new `DataFrame` perform a copy of the columns,
unless they are passed `copycols=false` (available only for functions
that could perform a transformation without copying the columns). Examples of such functions are [`vcat`](@ref),
[`hcat`](@ref), [`filter`](@ref), [`dropmissing`](@ref), `getindex`,
[`copy`](@ref) or the [`DataFrame`](@ref) constructor mentioned above.

On the contrary, functions that create a view of a `DataFrame` *do not* by definition make copies of
the columns, and therefore require particular caution. This includes `view`, which returns
a `SubDataFrame` or a `DataFrameRow`, and `groupby`, which returns a `GroupedDataFrame`.

A partial exception to this rule is the [`stack`](@ref) function with `view=true` which
creates a `DataFrame` that contains views of the columns from the source `DataFrame`.

In-place functions whose names end with `!` (like `sort!` or [`dropmissing!`](@ref),
`setindex!`, `push!`, `append!`) may mutate the column vectors of the `DataFrame` they take
as an argument. These functions are safe to call due to the rules described above,
*except* when a view of the `DataFrame` is in use (via a `SubDataFrame`, a `DataFrameRow`
or a `GroupedDataFrame`). In the latter case, calling such a function on the parent might corrupt the view,
which make trigger errors, silently return invalid data or even cause Julia to crash.
The same caution applies when `DataFrame` was created using columns of another `DataFrame` without copying
(for instance when `copycols=false` in functions such as `DataFrame` or `hcat`).

It is possible to have a direct access to a column `col` of a `DataFrame` `df`
(e.g. this can be useful in performance critical code to avoid copying),
using one of the following methods:

* via the `getproperty` function using the syntax `df.col`;
* via the `getindex` function using the syntax `df[!, :col]` (note this is in contrast to `df[:, :col]` which copies);
* by creating a `DataFrameColumns` object using the [`eachcol`](@ref) function;
* by calling the `parent` function on a view of a column of the `DataFrame`, e.g. `parent(@view df[:, :col])`;
* by storing the reference to the column before creating a `DataFrame` with `copycols=false`;

A column obtained from a `DataFrame` using one of the above methods should not be mutated
without caution because:

* resizing a column vector will corrupt its parent `DataFrame` and any associated views
  as methods only check the length of the column when it is added
  to the `DataFrame` and later assume that all columns have the same length;
* reordering values in a column vector (e.g. using `sort!`) will break the consistency of rows
  with other columns, which will also affect views (if any);
* changing values contained in a column vector is acceptable as long as it is not used as
  a grouping column in a `GroupedDataFrame` created based on the `DataFrame`.

## Types specification

```@docs
AbstractDataFrame
AsTable
ByRow
DataFrame
DataFrame!
DataFrameRow
GroupedDataFrame
GroupKey
GroupKeys
SubDataFrame
DataFrameRows
DataFrameColumns
RepeatedVector
StackedVector
```
