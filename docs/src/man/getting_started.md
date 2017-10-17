# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following command:
```julia
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

## The `Null` Type

To get started, let's examine the `Null` type. `Null` is a type implemented by the [Nulls.jl](https://github.com/JuliaData/Nulls.jl) package to represent missing data. `null` is an instance of the type `Null` used to represent a missing value.

```jldoctest nulls
julia> using DataFrames

julia> null
null

julia> typeof(null)
Nulls.Null

```

The `Null` type lets users create `Vector`s and `DataFrame` columns with missing values. Here we create a vector with a null value and the element-type of the returned vector is `Union{Nulls.Null, Int64}`.

```jldoctest nulls
julia> x = [1, 2, null]
3-element Array{Union{Nulls.Null, Int64},1}:
 1
 2
  null

julia> eltype(x)
Union{Nulls.Null, Int64}

julia> Union{Null, Int}
Union{Nulls.Null, Int64}

julia> eltype(x) == Union{Null, Int}
true

```

`null` values can be excluded when performing operations by using `Nulls.skip`, which returns a memory-efficient iterator.

```jldoctest nulls
julia> Nulls.skip(x)
Base.Generator{Base.Iterators.Filter{Nulls.##4#6,Array{Union{Int64, Nulls.Null},1}},Nulls.##3#5}(Nulls.#3, Base.Iterators.Filter{Nulls.##4#6,Array{Union{Int64, Nulls.Null},1}}(Nulls.#4, Union{Int64, Nulls.Null}[1, 2, null]))

```

The output of `Nulls.skip` can be passed directly into functions as an argument. For example, we can find the `sum` of all non-null values or `collect` the non-null values into a new null-free vector.

```jldoctest nulls
julia> sum(Nulls.skip(x))
3

julia> collect(Nulls.skip(x))
2-element Array{Int64,1}:
 1
 2

```

`null` elements can be replaced with other values via `Nulls.replace`.

```jldoctest nulls
julia> collect(Nulls.replace(x, 1))
3-element Array{Int64,1}:
 1
 2
 1

```

The function `Nulls.T` returns the element-type `T` in `Union{T, Null}`.

```jldoctest nulls
julia> eltype(x)
Union{Int64, Nulls.Null}

julia> Nulls.T(eltype(x))
Int64

```

Use `nulls` to generate nullable `Vector`s and `Array`s, using the optional first argument to specify the element-type.

```jldoctest nulls
julia> nulls(1)
1-element Array{Nulls.Null,1}:
 null

julia> nulls(3)
3-element Array{Nulls.Null,1}:
 null
 null
 null

julia> nulls(1, 3)
1×3 Array{Nulls.Null,2}:
 null  null  null

julia> nulls(Int, 1, 3)
1×3 Array{Union{Nulls.Null, Int64},2}:
 null  null  null

```

## The `DataFrame` Type

The `DataFrame` type can be used to represent data tables, each column of which is a vector. You can specify the columns using keyword arguments or pairs:

```jldoctest dataframe
julia> using DataFrames

julia> DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
4×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
│ 2   │ 2 │ F │
│ 3   │ 3 │ F │
│ 4   │ 4 │ M │

```

It is also possible to construct a `DataFrame` in stages:

```jldoctest dataframe
julia> df = DataFrame()
0×0 DataFrames.DataFrame


julia> df[:A] = 1:8
1:8

julia> df[:B] = ["M", "F", "F", "M", "F", "M", "M", "F"]
8-element Array{String,1}:
 "M"
 "F"
 "F"
 "M"
 "F"
 "M"
 "M"
 "F"

julia> df
8×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
│ 2   │ 2 │ F │
│ 3   │ 3 │ F │
│ 4   │ 4 │ M │
│ 5   │ 5 │ F │
│ 6   │ 6 │ M │
│ 7   │ 7 │ M │
│ 8   │ 8 │ F │

```

The `DataFrame` we build in this way has 8 rows and 2 columns. You can check this using the
`size` function:

```jldoctest dataframe
julia> size(df, 1) == 8
true

julia> size(df, 2) == 2
true

julia> size(df) == (8, 2)
true

```

We can also look at small subsets of the data in a couple of different ways:

```jldoctest dataframe
julia> head(df)
6×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
│ 2   │ 2 │ F │
│ 3   │ 3 │ F │
│ 4   │ 4 │ M │
│ 5   │ 5 │ F │
│ 6   │ 6 │ M │

julia> tail(df)
6×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 3 │ F │
│ 2   │ 4 │ M │
│ 3   │ 5 │ F │
│ 4   │ 6 │ M │
│ 5   │ 7 │ M │
│ 6   │ 8 │ F │

julia> df[1:3, :]
3×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
│ 2   │ 2 │ F │
│ 3   │ 3 │ F │

```

Having seen what some of the rows look like, we can try to summarize the entire data set using `describe`:

```jldoctest dataframe
julia> describe(df)
A
Summary Stats:
Mean:           4.500000
Minimum:        1.000000
1st Quartile:   2.750000
Median:         4.500000
3rd Quartile:   6.250000
Maximum:        8.000000
Length:         8
Type:           Int64

B
Summary Stats:
Length:         8
Type:           String
Number Unique:  2


```

To access individual columns of the dataset, you refer to the column names by their symbol
or by their numerical index. Here we extract the first column, `:A`, and use it to compute
the mean and variance.

```jldoctest dataframe
julia> mean(df[:A]) == mean(df[1]) == 4.5
true

julia> var(df[:A]) ==  var(df[1]) == 6.0
true

```

If your dataset has missing values, most functions will require you to remove them
beforehand. Here we will replace all odd-numbered rows in the first column with missing data
to show how to handle the above example when missing values are present in your dataset.

```jldoctest dataframe
julia> df[:A] = [isodd(i) ? null : value for (i, value) in enumerate(df[:A])];

julia> df
8×2 DataFrames.DataFrame
│ Row │ A    │ B │
├─────┼──────┼───┤
│ 1   │ null │ M │
│ 2   │ 2    │ F │
│ 3   │ null │ F │
│ 4   │ 4    │ M │
│ 5   │ null │ F │
│ 6   │ 6    │ M │
│ 7   │ null │ M │
│ 8   │ 8    │ F │

julia> mean(Nulls.skip(df[:A]))
5.0

```

We can also apply a function to each column of a `DataFrame` with the `colwise` function. For example:

```jldoctest dataframe
julia> df = DataFrame(A = 1:4, B = 4.0:-1.0:1.0)
4×2 DataFrames.DataFrame
│ Row │ A │ B   │
├─────┼───┼─────┤
│ 1   │ 1 │ 4.0 │
│ 2   │ 2 │ 3.0 │
│ 3   │ 3 │ 2.0 │
│ 4   │ 4 │ 1.0 │

julia> colwise(sum, df)
2-element Array{Real,1}:
 10
 10.0

```

## Importing and Exporting Data (I/O)

For reading and writing tabular data from CSV and other delimited text files, use the [CSV.jl](https://github.com/JuliaData/CSV.jl) package.

If you have not used the CSV.jl package before then you may need to install it first:
```julia
Pkg.add("CSV")
```

The CSV.jl functions are not loaded automatically and must be imported into the session.
```julia
using CSV
```

A dataset can now be read from a CSV file at path `input` using
```julia
CSV.read(input)
```

A DataFrame can be written to a CSV file at path `output` using
```julia
df = DataFrame(x = 1, y = 2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more information, use the REPL [help-mode](http://docs.julialang.org/en/stable/manual/interacting-with-julia/#help-mode) or checkout the online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).

## Loading a Classic Data Set

To see more of the functionality for working with `DataFrame` objects, we need a more complex data set to work with. We can access Fisher's iris data set using the following functions:

```jldoctest csv
julia> using DataFrames, CSV

julia> iris = CSV.read(joinpath(Pkg.dir("DataFrames"), "test/data/iris.csv"));

julia> head(iris)
6×5 DataFrames.DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa  │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa  │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa  │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa  │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa  │

```

## Querying DataFrames

While the `DataFrames` package provides basic data manipulation capabilities, users are encouraged to use the [Query.jl](https://github.com/davidanthoff/Query.jl), which provides a [LINQ](https://msdn.microsoft.com/en-us/library/bb397926.aspx)-like interface to a large number of data sources, including `DataFrame` instances. See the [Querying frameworks](@ref)  section for more information.
