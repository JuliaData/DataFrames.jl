# Why Use the DataFrames Package?

We want to use Julia for statistical programming. While Julia is a very powerful tool for general mathematical programming, it is missing several basic features that are essential for statistical applications. This introductory section describes some of the features that are missing from Julia's core. The rest of the manual describes the ways in which the DataFrames package extends Julia to make up for those missing features.

## Missing Data Points

Suppose that we want to calculate the mean of a list of five `Float64` numbers: `x1`, `x2`, `x3`, `x4` and `x5`. We would normally do this in Julia as follows:

* Represent these five numbers as a `Vector`: `v = [x1, x2, x3, x4, x5]`.
* Compute the mean of `v` using the `mean()` function.

_But what if one of the five numbers were missing?_

The concept of a missing data point cannot be directly expressed in Julia because there is no scalar value to denote missingness. While Java has a `NULL` value and R has an `NA` value, there is, _by design_, nothing equivalent in Julia. As such, the DataFrames package's first extension of Julia's core type system is to add a new `NA` value.

## Data Structures for Storing Missing Data Points

Even if we can express the notion that the value of the numeric variable `x2` is unknown by using a new `NA` value, there is little that we can do with this new value because it cannot be directly stored in a standard Julian `Vector` unless that `Vector` has no type constraints on its entries. While we could use a `Vector{Any}` to work around this, that approach would produce very inefficient code.

Instead of trying to use overly generic data structures, we have created extensions of the core Julia `Vector` and `Matrix` data structures that can store `NA` values. These augmented data structures are called `DataVector` and `DataMatrix`. Both `DataVector{T}` and `DataMatrix{T}` can contain either (a) values of any specific type `T` or (b) values of our new `NA` type.

For example, a standard `Vector{Float64}` can contain `Float64` and nothing else. Our new `DataVector{Float64}` can contain `Float64` or `NA` values, but nothing else. This makes the new data types much more efficient than using generic containers like `Vector{Any}` or `Matrix{Any}`.

## Tabular Data Structures

`DataVector` and `DataMatrix` are very powerful data structures, but they are not sufficient for describing most real world data sets. Although most standard data sets are easily described using a simple table of data, these kinds of tables are generally not like matrices. The example table of data shown below highlights some of the ways in which a data set is not like a `DataMatrix`:

![Tabular Data](figures/data.png)

We highlight three major differences below:

* The columns of a tabular data set may have different types. A `DataMatrix` can only contain values of one type: these might all be `String` or `Int`, but we cannot have one column of `String` and another column of `Int`.
* The values of the entries within a column generally have a consistent type. This means that a single column could be represented using a `DataVector`. Unfortunately, the heterogeneity of types between columns means that we need some way of wrapping a group of columns together into a coherent whole. We could use a `Vector` to wrap up all of the columns of the table, but this will not enforce an important constraint imposed by our intuitions: _every column of a tabular data set has the same length as all of the other columns_. A tabular data set is not just a haphazard collection of vectors.
* The columns of a tabular data set are typically named using `String`. Most programs for working with data can access the columns of a data set using these names in addition to simple numeric indices. In other words, a tabular data structure can sometimes behave like an `Array` and can sometimes behave like a `Dict`. This dual indexing strategy makes it particular easy to work with tabular data.

We can summarize these concerns by noting that we face four problems when with working with tabular data sets that are not well solved by existing Julian data structures:

* Tabular data sets may have heterogeneous types of columns.
* Each column of a tabular data set has a consistent type.
* All columns of a tabular data set have a consistent length, although some entries within columns may be missing.
* The columns of a tabular data set should be addressable by both name and numeric index.

We solve all of these four problems by adding a `DataFrame` type to Julia. This type will be familiar to anyone who has worked with R's `data.frame` type or with Pandas' `DataFrame` type. Even if you have never used R or Python to work with data, this tabular data structure will be satisfy many of the intuitions that you've developed while working with spreadsheet programs like Excel.

## A Language for Expressing Statistical Models

Statistical programming is generally focused on answering substantive questions about the properties of a data set. We are generally not interested in thinking about algorithms, but instead want to spend our time thinking about mathematical models.

Part of the power of the R programming language is that it provides a coherent mini-language for talking about various types of linear models ranging from ANOVA's to GLM's. For example, R describes a regression in which a variable `Z` is regressed against the variables `X` and `Y` using the notation:

    Z ~ X + Y

Julia, by default, provides no similar sort of mini-language for describing the mathematical structure of statistical models. To remedy this, we have added a `Formula` type to Julia that provides a simple DSL for describing linear models in Julia.
