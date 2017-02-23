# Getting Started

## Installation

The DataFrames package is available through the Julia package system. Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using NullableArrays, DataFrames` to bring all of the relevant variables into your current namespace. In addition, we will make use of the `RDatasets` package, which provides access to hundreds of classical data sets.

## The `Nullable` Type

To get started, let's examine the `Nullable` type. Objects of this type can either hold a value, or represent a missing value (`null`). For example, this is a `Nullable` holding the integer `1`:

```julia
Nullable(1)
```

And this represents a missing value:
```julia
Nullable()
```

`Nullable` objects support all standard operators, which return another `Nullable`. One of the essential properties of `null` values is that they poison other items. To see this, try to add something like `Nullable(1)` to `Nullable()`:

```julia
Nullable(1) + Nullable()
```

Note that operations mixing `Nullable` and scalars (e.g. `1 + Nullable()`) are not supported.

## The `NullableArray` Type

`Nullable` objects can be stored in a standard `Array` just like any value:

```julia
v = Nullable{Int}[1, 3, 4, 5, 4]
```

But arrays of `Nullable` are inefficient, both in terms of computation costs and of memory use. `NullableArrays` provide a more efficient storage, and behave like `Array{Nullable}` objects.

```julia
nv = NullableArray(Nullable{Int}[Nullable(), 3, 2, 5, 4])
```

In many cases we're willing to just ignore missing values and remove them from our vector. We can do that using the `dropnull` function:

```julia
dropnull(nv)
mean(dropnull(nv))
```

Instead of removing `null` values, you can try to convert the `NullableArray` into a normal Julia `Array` using `convert`:

```julia
convert(Array, nv)
```

This fails in the presence of `null` values, but will succeed if there are no `null` values:

```julia
nv[1] = 3
convert(Array, nv)
```

In addition to removing `null` values and hoping they won't occur, you can also replace any `null` values using the `convert` function, which takes a replacement value as an argument:

```julia
nv = NullableArray(Nullable{Int}[Nullable(), 3, 2, 5, 4])
mean(convert(Array, nv, 0))
```

Which strategy for dealing with `null` values is most appropriate will typically depend on the specific details of your data analysis pathway.

## The `DataFrame` Type

The `DataFrame` type can be used to represent data tables, each column of which is an array (by default, a `NullableArray`). You can specify the columns using keyword arguments:

```julia
df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
```

It is also possible to construct a `DataFrame` in stages:

```julia
df = DataFrame()
df[:A] = 1:8
df[:B] = ["M", "F", "F", "M", "F", "M", "M", "F"]
df
```

The `DataFrame` we build in this way has 8 rows and 2 columns. You can check this using `size` function:

```julia
nrows = size(df, 1)
ncols = size(df, 2)
```

We can also look at small subsets of the data in a couple of different ways:

```julia
head(df)
tail(df)

df[1:3, :]
```

Having seen what some of the rows look like, we can try to summarize the entire data set using `describe`:

```julia
describe(df)
```

To focus our search, we start looking at just the means and medians of specific columns. In the example below, we use numeric indexing to access the columns of the `DataFrame`:

```julia
mean(dropnull(df[1]))
median(dropnull(df[1]))
```

We could also have used column names to access individual columns:

```julia
mean(dropnull(df[:A]))
median(dropnull(df[:A]))
```

We can also apply a function to each column of a `DataFrame` with the `colwise` function. For example:

```julia
df = DataFrame(A = 1:4, B = randn(4))
colwise(c->cumsum(dropnull(c)), df)
```

## Accessing Classic Data Sets

To see more of the functionality for working with `DataFrame` objects, we need a more complex data set to work with. We'll use the `RDatasets` package, which provides access to many of the classical data sets that are available in R.

For example, we can access Fisher's iris data set using the following functions:

```julia
iris = readtable(joinpath(Pkg.dir("DataFrames"), "test/data/iris.csv"))
head(iris)
```

In the next section, we'll discuss generic I/O strategy for reading and writing `DataFrame` objects that you can use to import and export your own data files.

## Querying DataFrames

While the `DataFrames` package provides basic data manipulation capabilities, users are encouraged to use the following packages for more powerful and complete data querying functionality in the spirit of [dplyr](https://github.com/hadley/dplyr) and [LINQ](https://msdn.microsoft.com/en-us/library/bb397926.aspx):

- [Query.jl](https://github.com/davidanthoff/Query.jl) provides a LINQ like interface to a large number of data sources, including `DataFrame` instances.
