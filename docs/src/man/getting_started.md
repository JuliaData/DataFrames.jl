# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following command:
```julia
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

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

### Constructing Column by Column

It is also possible to construct a `DataFrame` one column at a time.

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

The `DataFrame` we build in this way has 8 rows and 2 columns.
You can check this using the `size` function:

```jldoctest dataframe
julia> size(df, 1) == 8
true

julia> size(df, 2) == 2
true

julia> size(df) == (8, 2)
true

```

### Constructing Row by Row

It is also possible to construct a `DataFrame` row by row.

First a `DataFrame` with empty columns is constructed:

```jldoctest dataframe
julia> df = DataFrame(A = Int[], B = String[])
0×2 DataFrames.DataFrame
```

Rows can then be added as `Vector`s, where the row order matches the columns order:

```jldoctest dataframe
julia> push!(df, [1, "M"])
1×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
```

Rows can also be added as `Dict`s, where the dictionary keys match the column names:

```jldoctest dataframe
julia> push!(df, Dict(:B => "F", :A => 2))
2×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ M │
│ 2   │ 2 │ F │
```

Note that constructing a `DataFrame` row by row is significantly less performant than
constructing it all at once, or column by column. For many use-cases this will not matter,
but for very large `DataFrame`s  this may be a consideration.

## Working with Data Frames

### Taking a Subset

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

### Summarizing with `describe`

Having seen what some of the rows look like, we can try to summarize the entire data set using `describe`:

```jldoctest dataframe
julia> describe(df)
2×8 DataFrames.DataFrame
│ Row │ variable │ mean │ min │ median │ max │ nunique │ nmissing │ eltype │
├─────┼──────────┼──────┼─────┼────────┼─────┼─────────┼──────────┼────────┤
│ 1   │ A        │ 4.5  │ 1   │ 4.5    │ 8   │         │          │ Int64  │
│ 2   │ B        │      │ F   │        │ M   │ 2       │          │ String │

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

### Column-Wise Operations

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
