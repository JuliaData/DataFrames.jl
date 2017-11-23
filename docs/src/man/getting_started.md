# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following command:
```julia
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

## The `Missing` Type

To get started, let's examine the `Missing` type. `Missing` is a type implemented by the [Missings.jl](https://github.com/JuliaData/Missings.jl) package to represent missing data. `missing` is an instance of the type `Missing` used to represent a missing value.

```jldoctest missings
julia> using DataFrames

julia> missing
missing

julia> typeof(missing)
Missings.Missing

```

The `Missing` type lets users create `Vector`s and `DataFrame` columns with missing values. Here we create a vector with a missing value and the element-type of the returned vector is `Union{Missings.Missing, Int64}`.

```jldoctest missings
julia> x = [1, 2, missing]
3-element Array{Union{Missings.Missing, Int64},1}:
 1
 2
  missing

julia> eltype(x)
Union{Missings.Missing, Int64}

julia> Union{Missing, Int}
Union{Missings.Missing, Int64}

julia> eltype(x) == Union{Missing, Int}
true

```

`missing` values can be excluded when performing operations by using `skipmissing`, which returns a memory-efficient iterator.

```jldoctest missings
julia> skipmissing(x)
Missings.EachSkipMissing{Array{Union{$Int, Missings.Missing},1}}(Union{$Int, Missings.Missing}[1, 2, missing])

```

The output of `skipmissing` can be passed directly into functions as an argument. For example, we can find the `sum` of all non-missing values or `collect` the non-missing values into a new missing-free vector.

```jldoctest missings
julia> sum(skipmissing(x))
3

julia> collect(skipmissing(x))
2-element Array{Int64,1}:
 1
 2

```

`missing` elements can be replaced with other values via `Missings.replace`.

```jldoctest missings
julia> collect(Missings.replace(x, 1))
3-element Array{Int64,1}:
 1
 2
 1

```

The function `Missings.T` returns the element-type `T` in `Union{T, Missing}`.

```jldoctest missings
julia> eltype(x)
Union{Int64, Missings.Missing}

julia> Missings.T(eltype(x))
Int64

```

Use `missings` to generate `Vector`s and `Array`s supporting missing values, using the optional first argument to specify the element-type.

```jldoctest missings
julia> missings(1)
1-element Array{Missings.Missing,1}:
 missing

julia> missings(3)
3-element Array{Missings.Missing,1}:
 missing
 missing
 missing

julia> missings(1, 3)
1×3 Array{Missings.Missing,2}:
 missing  missing  missing

julia> missings(Int, 1, 3)
1×3 Array{Union{Missings.Missing, Int64},2}:
 missing  missing  missing

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
julia> df[:A] = [isodd(i) ? missing : value for (i, value) in enumerate(df[:A])];

julia> df
8×2 DataFrames.DataFrame
│ Row │ A       │ B │
├─────┼─────────┼───┤
│ 1   │ missing │ M │
│ 2   │ 2       │ F │
│ 3   │ missing │ F │
│ 4   │ 4       │ M │
│ 5   │ missing │ F │
│ 6   │ 6       │ M │
│ 7   │ missing │ M │
│ 8   │ 8       │ F │

julia> mean(skipmissing(df[:A]))
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
