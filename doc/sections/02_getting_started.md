# Getting Started

## Installation

The DataFrames package is available through the Julia package system. Throughout
the rest of this tutorial, we will assume that you have installed the DataFrames
package and have already typed `using DataArrays, DataFrames` to bring all of
the relevant variables into your current namespace. In addition, we will make
use of the `RDatasets` package, which provides access to hundreds of
classical data sets.

## The `NA` Value

To get started, let's examine the `NA` value. Type the following into the
REPL:

    NA

One of the essential properties of `NA` is that it poisons other items. To
see this, try to add something like `1` to `NA`:

    1 + NA

## The `DataArray` Type

Now that we see that `NA` is working, let's insert one into a `DataArray`.
We'll create one now using the `@data` macro:

    dv = @data([NA, 3, 2, 5, 4])

To see how `NA` poisons even complex calculations, let's try to take
the mean of the five numbers stored in `dv`:

    mean(dv)

In many cases we're willing to just ignore `NA` values and remove them
from our vector. We can do that using the `dropna` function:

    dropna(dv)
    mean(dropna(dv))

Instead of removing `NA` values, you can try to conver the `DataArray`
into a normal Julia `Array` using `convert`:

    convert(Array, dv)

This fails in the presence of `NA` values, but will succeed if there are
no `NA` values:

    dv[1] = 3
    convert(Array, dv)

In addition to removing `NA` values and hoping they won't occur, you can
also replace any `NA` values using the `array` function, which takes a
replacement value as an argument:

    dv = @data([NA, 3, 2, 5, 4])
    mean(array(dv, 11))

Which strategy for dealing with `NA` values is most appropriate will
typically depend on the specific details of your data analysis pathway.

Although the examples above employed only 1D `DataArray` objects, the
`DataArray` type defines a completely generic N-dimensional array type.
Operations on generic `DataArray` objects work in higher dimensions in
the same way that they work on Julia's Base `Array` type:

    dm = @data([NA 0.0; 0.0 1.0])
    dm * dm

## The `DataFrame` Type

The `DataFrame` type can be used to represent data tables, each column of
which is a `DataArray`. You can specify the columns using keyword arguments:

    df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])

It is also possible to construct a `DataFrame` in stages:

    df = DataFrame()
    df[:A] = 1:8
    df[:B] = ["M", "F", "F", "M", "F", "M", "M", "F"]
    df

The `DataFrame` we build in this way has 8 rows and 2 columns. You
can check this using `size` function:

    nrows = size(df, 1)
    ncols = size(df, 2)

We can also look at small subsets of the data in a couple of different ways:

    head(df)
    tail(df)
    
    df[1:3, :]

Having seen what some of the rows look like, we can try to summarize the
entire data set using `describe`:

    describe(df)

To focus our search, we start looking at just the means and medians of
specific columns. In the example below, we use numeric indexing to access
the columns of the `DataFrame`:

    mean(df[1])
    median(df[1])

We could also have used column names to access individual columns:

    mean(df[:A])
    median(df[:A])

We can also apply a function to each column of a `DataFrame` with the `colwise`
function. For example:

    df = DataFrame(A = 1:4, B = randn(4))
    colwise(cumsum, df)

## Accessing Classic Data Sets

To see more of the functionality for working with `DataFrame` objects, we need
a more complex data set to work with. We'll use the `RDatasets` package, which
provides access to many of the classical data sets that are available in R.

For example, we can access Fisher's iris data set using the following functions:

    using RDatasets
    iris = dataset("datasets", "iris")
    head(iris)

In the next section, we'll discuss generic I/O strategy for reading and writing
`DataFrame` objects that you can use to import and export your own data files.
