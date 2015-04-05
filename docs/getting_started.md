# Getting Started

## Installation

The DataFrames package is available through the Julia package system. Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataArrays` to bring all of the relevant variables into your current namespace. In addition, we will make use of the `RDatasets` package, which provides access to hundreds of classical data sets.

## The `NA` Value

To get started, let's examine the `NA` value. Type the following into the REPL:

```julia
julia> NA
NA
```

One of the essential properties of `NA` is that it poisons other items. To see this, try to add something like `1` to `NA`:

```julia
julia> 1+NA
NA
```

## The `DataArray` Type

Now that we see that `NA` is working, let's insert one into a `DataArray`. We'll create one now using the `@data` macro:

```julia
julia> dv = @data([NA, 3, 2, 5, 4])
5-element DataArray{Int64,1}:
  NA
 3  
 2  
 5  
 4
```

To see how `NA` poisons even complex calculations, let's try to take the mean of the five numbers stored in `dv`:

```julia
julia> mean(dv)
NA
```

In many cases we're willing to just ignore `NA` values and remove them from our vector. We can do that using the `dropna` function:

```julia
julia> dropna(dv)
4-element Array{Int64,1}:
 3
 2
 5
 4
 
julia> mean(dropna(dv))
3.5
```

Instead of removing `NA` values, you can try to convert the `DataArray` into a normal Julia `Array` using `convert`:

```julia
julia> convert(Array, dv)
ERROR: NAException("Cannot convert DataArray with NA's to desired type")
```

This fails in the presence of `NA` values, but will succeed if there are no `NA` values:

```julia
julia> dv[1] = 3
3

julia> convert(Array, dv)
5-element Array{Int64,1}:
 3
 3
 2
 5
 4
```

In addition to removing `NA` values and hoping they won't occur, you can also replace any `NA` values using the `array` function, which takes a replacement value as an argument:

```julia
julia> dv = @data([NA, 3, 2, 5, 4])
5-element DataArray{Int64,1}:
  NA
 3  
 2  
 5  
 4  

julia> mean(array(dv, 11))
5.0
```

Which strategy for dealing with `NA` values is most appropriate will typically depend on the specific details of your data analysis pathway.

Although the examples above employed only 1D `DataArray` objects, the `DataArray` type defines a completely generic N-dimensional array type. Operations on generic `DataArray` objects work in higher dimensions in the same way that they work on Julia's Base `Array` type:

```julia
julia> dm = @data([NA 0.0; 0.0 1.0])
2x2 DataArray{Float64,2}:
  NA  0.0
 0.0  1.0

julia> dm * dm
2x2 DataArray{Float64,2}:
 NA   NA
 NA  1.0
```

## The `DataFrame` Type

The `DataFrame` type can be used to represent data tables, each column of which is a `DataArray`. You can specify the columns using keyword arguments:

```julia
julia> df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
4x2 DataFrame
| Row | A | B   |
|-----|---|-----|
| 1   | 1 | "M" |
| 2   | 2 | "F" |
| 3   | 3 | "F" |
| 4   | 4 | "M" |
```

It is also possible to construct a `DataFrame` in stages:

```julia
julia> df = DataFrame()
0x0 DataFrame

julia> df[:A] = 1:8
1:8

julia> df[:B] = ["M", "F", "F", "M", "F", "M", "M", "F"]
8-element Array{ASCIIString,1}:
 "M"
 "F"
 "F"
 "M"
 "F"
 "M"
 "M"
 "F"
```

The `DataFrame` we build in this way has 8 rows and 2 columns. You can check this using `size` function:

```julia
julia> nrows = size(df, 1)
8

julia> ncols = size(df, 2)
2
```

We can also look at small subsets of the data in a couple of different ways:

```julia
julia> head(df)
6x2 DataFrame
| Row | A | B   |
|-----|---|-----|
| 1   | 1 | "M" |
| 2   | 2 | "F" |
| 3   | 3 | "F" |
| 4   | 4 | "M" |
| 5   | 5 | "F" |
| 6   | 6 | "M" |

julia> tail(df)
6x2 DataFrame
| Row | A | B   |
|-----|---|-----|
| 1   | 3 | "F" |
| 2   | 4 | "M" |
| 3   | 5 | "F" |
| 4   | 6 | "M" |
| 5   | 7 | "M" |
| 6   | 8 | "F" |

julia> df[1:3, :]
3x2 DataFrame
| Row | A | B   |
|-----|---|-----|
| 1   | 1 | "M" |
| 2   | 2 | "F" |
| 3   | 3 | "F" |
```

Having seen what some of the rows look like, we can try to summarize the entire data set using `describe`:

```julia
julia> describe(df)
A
Min      1.0
1st Qu.  2.75
Median   4.5
Mean     4.5
3rd Qu.  6.25
Max      8.0
NAs      0
NA%      0.0%

B
Length  8
Type    ASCIIString
NAs     0
NA%     0.0%
Unique  2
```

To focus our search, we start looking at just the means and medians of specific columns. In the example below, we use numeric indexing to access the columns of the `DataFrame`:

```julia
julia> mean(df[1])
4.5

julia> median(df[1])
4.5
```

We could also have used column names to access individual columns:

```julia
julia> mean(df[:A])
4.5

julia> median(df[:A])
4.5
```

We can also apply a function to each column of a `DataFrame` with the `colwise` function. For example:

```julia
julia> df = DataFrame(A = 1:4, B = randn(4))
4x2 DataFrame
| Row | A | B         |
|-----|---|-----------|
| 1   | 1 | 0.0147021 |
| 2   | 2 | -0.386311 |
| 3   | 3 | -1.84319  |
| 4   | 4 | -0.175922 |

julia> colwise(cumsum, df)
2-element Array{Any,1}:
 [1,3,6,10]                            
 [0.0147021,-0.371609,-2.2148,-2.39072]
```

## Accessing Classic Data Sets

To see more of the functionality for working with `DataFrame` objects, we need a more complex data set to work with. We'll use the `RDatasets` package, which provides access to many of the classical data sets that are available in R.

For example, we can access Fisher's iris data set using the following functions:

```julia
julia> using RDatasets

julia> iris = dataset("datasets", "iris")
150x5 DataFrame
| Row | SepalLength | SepalWidth | PetalLength | PetalWidth | Species     |
|-----|-------------|------------|-------------|------------|-------------|
| 1   | 5.1         | 3.5        | 1.4         | 0.2        | "setosa"    |
| 2   | 4.9         | 3.0        | 1.4         | 0.2        | "setosa"    |
...
```

In the next section, we'll discuss generic I/O strategy for reading and writing `DataFrame` objects that you can use to import and export your own data files.

