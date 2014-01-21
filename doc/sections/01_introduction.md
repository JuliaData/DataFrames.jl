# Why Use the DataFrames Package?

We believe that Julia is the future of technical computing. Nevertheless,
Base Julia is not sufficient for statistical computing. The DataFrames
package (and its sibling, DataArrays) extends Base Julia by introducing three
basic types needed for statistical computing:

* `NA`: An indicator that a data value is missing
* `DataArray`: An extension to the `Array` type that can contain missing
  values
* `DataFrame`: A data structure for representing tabular data sets

## `NA`: An Indicator for Missing Data Points

Suppose that we want to calculate the mean of a list of five `Float64`
numbers: `x1`, `x2`, `x3`, `x4` and `x5`. We would normally do this
in Julia as follows:

* Represent these five numbers as a `Vector`: `v = [x1, x2, x3, x4, x5]`
* Compute the mean of `v` using the `mean()` function

_But what if one of the five numbers were missing?_

The concept of a missing data point cannot be directly expressed in Julia.
In contrast with languages like Java and R, which provide `NULL` and `NA`
values that represent missingness, there is no missing data value in Base
Julia.

The DataArrays package therefore provides `NA`, which serves as an indicator
that a specific value is missing. In order to exploit Julia's multiple dispatch
rules, `NA` is a singleton object of a new type called `NAtype`.

Like R's `NA` value and unlike Java's `NULL` value, Julia's `NA` value represents
epistemic uncertainty. This means that operations involving `NA` return `NA`
when the result of the operation cannot be determined, but operations whose
value can be determined despite the presence of `NA` will return a value that
is not `NA`.

For example, `false && NA` evaluates to `false` and `true || NA`  evaluates
to `true`. In contrast, `1 + NA` evaluates to `NA` because the outcome is
uncertain in the absence of knowledge about the missing value represented
by `NA`.

## `DataArray`: Efficient Arrays with Missing Values

Although the `NA` value is sufficient for representing missing scalar values,
it cannot be stored efficiently inside of Julia's standard `Array` type. To
represent arrays with potentially missing entries, the DataArrays package
introduces a `DataArray` type. For example, a `DataArray{Float64}` can
contain `Float64` values and `NA` values, but nothing else. In contrast, the
most specific `Array` that can contain both `Float64` and `NA` values is an
`Array{Any}`.

Except for the ability to store `NA` values, the `DataArray` type is meant to
behave exactly like Julia's standard `Array` type. In particular, `DataArray`
provides two typealiases called `DataVector` and `DataMatrix` that mimic the
`Vector` and `Matrix` typealiases for 1D and 2D `Array` types.

## `DataFrame`: Tabular Data Sets

`NA` and `DataArray` provide mechanisms for handling missing values for scalar
types and arrays, but most real world data sets have a tabular structure that
does not correspond to a simple `DataArray`.

For example, the data table shown below highlights some of the ways in which a
typical data set is not like a `DataArray`:

![Tabular Data](figures/data.png)

Note three important properties that this table possesses:

* The columns of a tabular data set may have different types. A `DataMatrix`
  can only contain values of one type: these might all be `String` or `Int`,
  but we cannot have one column of `String` type and another column of `Int`
  type.
* The values of the entries within a column always have a consistent type.
  This means that a single column could be represented using a `DataVector`.
  Unfortunately, the heterogeneity of types between columns means that we
  need some way of wrapping a group of columns together into a coherent whole.
  We could use a standard `Vector` to wrap up all of the columns of the table,
  but this will not enforce an important constraint imposed by our intuitions:
  _every column of a tabular data set has the same length as all of the other
  columns_.
* The columns of a tabular data set are typically named using some sort of
  `String`. Often, one wants to access the entries of a data set by using a
  combination of verbal names and numeric indices.

We can summarize these concerns by noting that we face four problems when with
working with tabular data sets:

* Tabular data sets may have columns of heterogeneous type
* Each column of a tabular data set has a consistent type across all of
  its entries
* All of the columns of a tabular data set have the same length
* The columns of a tabular data set should be addressable using both verbal
  names and numeric indices

The DataFrames package solves these problems by adding a `DataFrame` type
to Julia. This type will be familiar to anyone who has worked with R's
`data.frame` type, Pandas' `DataFrame` type, an SQL-style database, or
Excel spreadsheet.
