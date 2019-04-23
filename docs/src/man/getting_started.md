# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following commands:

```julia
using Pkg
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

## The `DataFrame` Type

Objects of the `DataFrame` type represent a data table as a series of vectors, each corresponding to a column or variable. The simplest way of constructing a `DataFrame` is to pass column vectors using keyword arguments or pairs:

```jldoctest dataframe
julia> using DataFrames

julia> df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
4×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ M      │
│ 2   │ 2     │ F      │
│ 3   │ 3     │ F      │
│ 4   │ 4     │ M      │

```

Columns can be accessed via `df.col` or `df[:col]`. The latter syntax is more flexible as it allows passing a variable holding the name of the column, and not only a literal name. Note that column names are symbols (`:col` or `Symbol("col")`) rather than strings (`"col"`). Columns can also be accessed using an integer index specifying their position.

```jldoctest dataframe
julia> df.A
4-element Array{Int64,1}:
 1
 2
 3
 4

julia> df.A === df[:A]
true

julia> df.A === df[1]
true

julia> firstcolumn = :A
:A

julia> df[firstcolumn] === df.A
true
```

Column names can be obtained using the `names` function:

```jldoctest dataframe
julia> names(df)
2-element Array{Symbol,1}:
 :A
 :B
```

### Constructing Column by Column

It is also possible to start with an empty `DataFrame` and add columns to it one by one:

```jldoctest dataframe
julia> df = DataFrame()
0×0 DataFrame


julia> df.A = 1:8
1:8

julia> df.B = ["M", "F", "F", "M", "F", "M", "M", "F"]
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
8×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ M      │
│ 2   │ 2     │ F      │
│ 3   │ 3     │ F      │
│ 4   │ 4     │ M      │
│ 5   │ 5     │ F      │
│ 6   │ 6     │ M      │
│ 7   │ 7     │ M      │
│ 8   │ 8     │ F      │

```

The `DataFrame` we build in this way has 8 rows and 2 columns.
This can be checked using the `size` function:

```jldoctest dataframe
julia> size(df, 1)
8

julia> size(df, 2)
2

julia> size(df)
(8, 2)

```

### Constructing Row by Row

It is also possible to fill a `DataFrame` row by row. Let us construct an empty data frame with two columns (note that the first column can only contain integers and the second one can only contain strings):

```jldoctest dataframe
julia> df = DataFrame(A = Int[], B = String[])
0×2 DataFrame
```

Rows can then be added as tuples or vectors, where the order of elements matches that of columns:

```jldoctest dataframe
julia> push!(df, (1, "M"))
1×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ M      │

julia> push!(df, [2, "N"])
2×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ M      │
│ 2   │ 2     │ N      │
```

Rows can also be added as `Dict`s, where the dictionary keys match the column names:

```jldoctest dataframe
julia> push!(df, Dict(:B => "F", :A => 3))
3×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ M      │
│ 2   │ 2     │ N      │
│ 3   │ 3     │ F      │
```

Note that constructing a `DataFrame` row by row is significantly less performant than
constructing it all at once, or column by column. For many use-cases this will not matter,
but for very large `DataFrame`s  this may be a consideration.

### Constructing from another table type

DataFrames supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface for
interacting with tabular data. This means that a `DataFrame` can be used as a "source"
to any package that expects a Tables.jl interface input, (file format packages,
data manipulation packages, etc.). A `DataFrame` can also be a sink for any Tables.jl
interface input. Some example uses are:

```julia
df = DataFrame(a=[1, 2, 3], b=[:a, :b, :c])

# write DataFrame out to CSV file
CSV.write("dataframe.csv", df)

# store DataFrame in an SQLite database table
SQLite.load!(df, db, "dataframe_table")

# transform a DataFrame through Query.jl package
df = df |> @map({a=_.a + 1, _.b}) |> DataFrame
```

## Working with Data Frames

### Examining the Data

The default printing of `DataFrame` objects only includes a sample of rows and columns that fits on screen:

```jldoctest dataframe
julia> df = DataFrame(A = 1:2:1000, B = repeat(1:10, inner=50), C = 1:500)
500×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 1     │
│ 2   │ 3     │ 1     │ 2     │
│ 3   │ 5     │ 1     │ 3     │
│ 4   │ 7     │ 1     │ 4     │
⋮
│ 496 │ 991   │ 10    │ 496   │
│ 497 │ 993   │ 10    │ 497   │
│ 498 │ 995   │ 10    │ 498   │
│ 499 │ 997   │ 10    │ 499   │
│ 500 │ 999   │ 10    │ 500   │
```

Printing options can be adjusted by calling the `show` function manually: `show(df, allrows=true)` prints all rows even if they do not fit on screen and `show(df, allcols=true)` does the same for columns.

The `first` and `last` functions can be used to look at the first and last rows of a data frame (respectively):

```jldoctest dataframe
julia> first(df, 6)
6×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 1     │
│ 2   │ 3     │ 1     │ 2     │
│ 3   │ 5     │ 1     │ 3     │
│ 4   │ 7     │ 1     │ 4     │
│ 5   │ 9     │ 1     │ 5     │
│ 6   │ 11    │ 1     │ 6     │

julia> last(df, 6)
6×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 989   │ 10    │ 495   │
│ 2   │ 991   │ 10    │ 496   │
│ 3   │ 993   │ 10    │ 497   │
│ 4   │ 995   │ 10    │ 498   │
│ 5   │ 997   │ 10    │ 499   │
│ 6   │ 999   │ 10    │ 500   │
```

Also notice that when `DataFrame` is printed to the console or rendered in HTML (e.g. in Jupyter Notebook) you get an information about type of elements held in its columns. For example in this case:

```jldoctest dataframe
julia> DataFrame(a = 1:2, b = [1.0, missing],
                 c = categorical('a':'b'), d = [1//2, missing])
2×4 DataFrame
│ Row │ a     │ b        │ c            │ d         │
│     │ Int64 │ Float64⍰ │ Categorical… │ Rationa…⍰ │
├─────┼───────┼──────────┼──────────────┼───────────┤
│ 1   │ 1     │ 1.0      │ 'a'          │ 1//2      │
│ 2   │ 2     │ missing  │ 'b'          │ missing   │
```

we can observe that:

* the first column `:a` can hold elements of type `Int64`;
* the second column `:b` can hold `Float64` or `Missing`, which is indicated by `⍰` printed after the name of type;
* the third column `:c` can hold categorical data; here we notice `…`, which indicates that the actual name of the type was long and got truncated;
* the type information in fourth column `:d` presents a situation where the name is both truncated and the type allows `Missing`.

### Taking a Subset

Specific subsets of a data frame can be extracted using the indexing syntax, similar to matrices. The colon `:` indicates that all items (rows or columns depending on its position) should be retained:

```jldoctest dataframe
julia> df[1:3, :]
3×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 1     │
│ 2   │ 3     │ 1     │ 2     │
│ 3   │ 5     │ 1     │ 3     │

julia> df[[1, 5, 10], :]
3×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 1     │
│ 2   │ 9     │ 1     │ 5     │
│ 3   │ 19    │ 1     │ 10    │

julia> df[:, [:A, :B]]
500×2 DataFrame
│ Row │ A     │ B     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 3     │ 1     │
│ 3   │ 5     │ 1     │
│ 4   │ 7     │ 1     │
⋮
│ 496 │ 991   │ 10    │
│ 497 │ 993   │ 10    │
│ 498 │ 995   │ 10    │
│ 499 │ 997   │ 10    │
│ 500 │ 999   │ 10    │

julia> df[1:3, [:B, :A]]
3×2 DataFrame
│ Row │ B     │ A     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 1     │ 3     │
│ 3   │ 1     │ 5     │

julia> df[[3, 1], [:C]]
2×1 DataFrame
│ Row │ C     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 3     │
│ 2   │ 1     │
```

Do note that `df[[:A]]` and `df[:, [:A]]` return a `DataFrame` object, while `df[:A]` and `df[:, :A]` return a vector:

```jldoctest dataframe
julia> df[[:A]]
500×1 DataFrame
│ Row │ A     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 3     │
│ 3   │ 5     │
│ 4   │ 7     │
⋮
│ 496 │ 991   │
│ 497 │ 993   │
│ 498 │ 995   │
│ 499 │ 997   │
│ 500 │ 999   │

julia> df[[:A]] == df[:, [:A]]
true

julia> df[:A]
500-element Array{Int64,1}:
   1
   3
   5
   7
   9
  11
   ⋮
 991
 993
 995
 997
 999

julia> df[:A] == df[:, :A]
true
```

In the first cases, `[:A]` is a vector, indicating that the resulting object should be a `DataFrame`, since a vector can contain one or more column names. On the other hand, `:A` is a single symbol, indicating that a single column vector should be extracted.

The indexing syntax can also be used to select rows based on conditions on variables:

```jldoctest dataframe
julia> df[df.A .> 500, :]
250×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 501   │ 6     │ 251   │
│ 2   │ 503   │ 6     │ 252   │
│ 3   │ 505   │ 6     │ 253   │
│ 4   │ 507   │ 6     │ 254   │
⋮
│ 246 │ 991   │ 10    │ 496   │
│ 247 │ 993   │ 10    │ 497   │
│ 248 │ 995   │ 10    │ 498   │
│ 249 │ 997   │ 10    │ 499   │
│ 250 │ 999   │ 10    │ 500   │

julia> df[(df.A .> 500) .& (300 .< df.C .< 400), :]
99×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 601   │ 7     │ 301   │
│ 2   │ 603   │ 7     │ 302   │
│ 3   │ 605   │ 7     │ 303   │
│ 4   │ 607   │ 7     │ 304   │
⋮
│ 95  │ 789   │ 8     │ 395   │
│ 96  │ 791   │ 8     │ 396   │
│ 97  │ 793   │ 8     │ 397   │
│ 98  │ 795   │ 8     │ 398   │
│ 99  │ 797   │ 8     │ 399   │
```

While the DataFrames package provides basic data manipulation capabilities, users are encouraged to use querying frameworks for more convenient and powerful operations:
- the [Query.jl](https://github.com/davidanthoff/Query.jl) package provides a [LINQ](https://msdn.microsoft.com/en-us/library/bb397926.aspx)-like interface to a large number of data sources
- the [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl) package provides interfaces similar to LINQ and [dplyr](https://dplyr.tidyverse.org)

See the [Data manipulation frameworks](@ref) section for more information.

### Summarizing Data

The `describe` function returns a data frame summarizing the elementary statistics and information about each column:

```jldoctest dataframe
julia> df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])

julia> describe(df)
2×8 DataFrame
│ Row │ variable │ mean   │ min │ median │ max │ nunique │ nmissing │ eltype   │
│     │ Symbol   │ Union… │ Any │ Union… │ Any │ Union…  │ Nothing  │ DataType │
├─────┼──────────┼────────┼─────┼────────┼─────┼─────────┼──────────┼──────────┤
│ 1   │ A        │ 2.5    │ 1   │ 2.5    │ 4   │         │          │ Int64    │
│ 2   │ B        │        │ F   │        │ M   │ 2       │          │ String   │

```

Of course, one can also compute descrptive statistics directly on individual columns:
```jldoctest dataframe
julia> using Statistics

julia> mean(df.A)
2.5
```

### Column-Wise Operations

We can also apply a function to each column of a `DataFrame` with the `aggregate` function. For example:

```jldoctest dataframe
julia> df = DataFrame(A = 1:4, B = 4.0:-1.0:1.0)
4×2 DataFrame
│ Row │ A     │ B       │
│     │ Int64 │ Float64 │
├─────┼───────┼─────────┤
│ 1   │ 1     │ 4.0     │
│ 2   │ 2     │ 3.0     │
│ 3   │ 3     │ 2.0     │
│ 4   │ 4     │ 1.0     │

julia> aggregate(df, sum)
1×2 DataFrame
│ Row │ A_sum │ B_sum   │
│     │ Int64 │ Float64 │
├─────┼───────┼─────────┤
│ 1   │ 10    │ 10.0    │

julia> aggregate(df, [sum, prod])
1×4 DataFrame
│ Row │ A_sum │ B_sum   │ A_prod │ B_prod  │
│     │ Int64 │ Float64 │ Int64  │ Float64 │
├─────┼───────┼─────────┼────────┼─────────┤
│ 1   │ 10    │ 10.0    │ 24     │ 24.0    │
```

### Handling of Columns Stored in a `DataFrame`

Functions that transform a `DataFrame` to produce a
new `DataFrame` always perform a copy of the columns by default, for example:

```jldoctest dataframe
julia> df = DataFrame(A = 1:4, B = 4.0:-1.0:1.0)
4×2 DataFrame
│ Row │ A     │ B       │
│     │ Int64 │ Float64 │
├─────┼───────┼─────────┤
│ 1   │ 1     │ 4.0     │
│ 2   │ 2     │ 3.0     │
│ 3   │ 3     │ 2.0     │
│ 4   │ 4     │ 1.0     │

julia> df2 = copy(df);

julia> df2.A === df.A
false
```

On the other hand, in-place functions, whose names end with `!`, may mutate the column vectors of the
`DataFrame` they take as an argument, for example:

```jldoctest dataframe
julia> x = [3, 1, 2];

julia> df = DataFrame(x=x)
3×1 DataFrame
│ Row │ x     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 3     │
│ 2   │ 1     │
│ 3   │ 2     │

julia> sort!(df)
3×1 DataFrame
│ Row │ x     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │
│ 2   │ 2     │
│ 3   │ 3     │

julia> x
3-element Array{Int64,1}:
 1
 2
 3

julia> df.x[1] = 100
100

julia> df
3×1 DataFrame
│ Row │ x     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 100   │
│ 2   │ 2     │
│ 3   │ 3     │

julia> x
3-element Array{Int64,1}:
 100
   2
   3
```

In-place functions are safe to call, except when a view of the `DataFrame`
(created via a `view`, `@view` or [`groupby`](@ref))
or when a `DataFrame` created with `copycols=false` are in use.

It is possible to have a direct access to a column `col` of a `DataFrame` `df`
using the syntaxes `df.col`, `df[:col]`, via the [`eachcol`](@ref) function,
by accessing a `parent` of a `view` of a column of a `DataFrame`,
or simply by storing the reference to the column vector before the `DataFrame`
was created with `copycols=false`.

```jldoctest dataframe
julia> x = [3, 1, 2];

julia> df = DataFrame(x=x)
3×1 DataFrame
│ Row │ x     │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 3     │
│ 2   │ 1     │
│ 3   │ 2     │

julia> df.x == x
true

julia> df[1] !== x
true

julia> eachcol(df, false)[1] === df.x
true
```

Note that a column obtained from a `DataFrame` using one of these methods should
not be mutated without caution.

The exact rules of handling columns of a `DataFrame` are explained in
[The design of handling of columns of a `DataFrame`](@ref man-columnhandling) section of the manual.

## Importing and Exporting Data (I/O)

For reading and writing tabular data from CSV and other delimited text files, use the [CSV.jl](https://github.com/JuliaData/CSV.jl) package.

If you have not used the CSV.jl package before then you may need to install it first:
```julia
using Pkg
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

A `DataFrame` can be written to a CSV file at path `output` using
```julia
df = DataFrame(x = 1, y = 2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more information, see `?CSV.read` and `?CSV.write`, or checkout the online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).
