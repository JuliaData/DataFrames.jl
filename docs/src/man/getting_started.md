# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following commands:

```julia
using Pkg
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the
DataFrames package and have already typed `using DataFrames` to bring all of the
relevant variables into your current namespace.

!!! note

    By default Jupyter Notebook will limit the number of rows and columns when displaying a data frame to roughly
    fit the screen size (like in the REPL).

    You can override this behavior by changing the values of the `ENV["COLUMNS"]` and `ENV["LINES"]`
    variables to hold the maximum width and height of output in characters respectively.

    Alternatively, you may want to set the maximum number of data frame rows to print to `100` and the maximum
    output width in characters to `1000` for every Julia session using some Jupyter kernel file (numbers `100`
    and `1000` are only examples and can be adjusted). In such case add a `"COLUMNS": "1000", "LINES": "100"`
    entry to the `"env"` variable in this Jupyter kernel file.
    See [here](https://jupyter-client.readthedocs.io/en/stable/kernels.html) for information about location
    and specification of Jupyter kernels.

## The `DataFrame` Type

Objects of the `DataFrame` type represent a data table as a series of vectors,
each corresponding to a column or variable. The simplest way of constructing a
`DataFrame` is to pass column vectors using keyword arguments or pairs:

```jldoctest dataframe
julia> using DataFrames

julia> df = DataFrame(A=1:4, B=["M", "F", "F", "M"])
4×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  F
   3 │     3  F
   4 │     4  M
```

Columns can be directly (i.e. without copying) accessed via `df.col`,
`df."col"`, `df[!, :col]` or `df[!, "col"]`. The two latter syntaxes are more
flexible as they allow passing a variable holding the name of the column, and
not only a literal name. Note that column names can be either symbols (written
as `:col`, `:var"col"` or `Symbol("col")`) or strings (written as `"col"`). Note
that in the forms `df."col"` and `:var"col"` variable interpolation into a
string using `$` does not work. Columns can also be accessed using an integer
index specifying their position.

Since `df[!, :col]` does not make a copy, changing the elements of the column
vector returned by this syntax will affect the values stored in the original
`df`. To get a copy of the column use `df[:, :col]`: changing the vector
returned by this syntax does not change `df`.


```jldoctest dataframe
julia> df.A
4-element Vector{Int64}:
 1
 2
 3
 4

julia> df."A"
4-element Vector{Int64}:
 1
 2
 3
 4

julia> df.A === df[!, :A]
true

julia> df.A === df[:, :A]
false

julia> df.A == df[:, :A]
true

julia> df.A === df[!, "A"]
true

julia> df.A === df[:, "A"]
false

julia> df.A == df[:, "A"]
true

julia> df.A === df[!, 1]
true

julia> df.A === df[:, 1]
false

julia> df.A == df[:, 1]
true

julia> firstcolumn = :A
:A

julia> df[!, firstcolumn] === df.A
true

julia> df[:, firstcolumn] === df.A
false

julia> df[:, firstcolumn] == df.A
true
```

Column names can be obtained as strings using the `names` function:

```jldoctest dataframe
julia> names(df)
2-element Vector{String}:
 "A"
 "B"
```

You can also filter column names by passing a column selector condition as a second argument.
See the [`names`](@ref) docstring for a detailed list of available conditions.
Here we give some selected examples:

```jldoctest dataframe
julia> names(df, r"A") # a regular expression selector
1-element Vector{String}:
 "A"

julia> names(df, Int) # a selector using column element type
1-element Vector{String}:
 "A"

julia> names(df, Not(:B)) # selector keeping all columns except :B
1-element Vector{String}:
 "A"
```

To get column names as `Symbol`s use the `propertynames` function:

```jldoctest dataframe
julia> propertynames(df)
2-element Vector{Symbol}:
 :A
 :B
```

!!! note

    DataFrames.jl allows to use `Symbol`s (like `:A`) and strings (like `"A"`)
    for all column indexing operations for convenience. However, using `Symbol`s
    is slightly faster and should generally be preferred, if not generating them
    via string manipulation.


### Constructing Column by Column

It is also possible to start with an empty `DataFrame` and add columns to it one by one:

```jldoctest dataframe
julia> df = DataFrame()
0×0 DataFrame

julia> df.A = 1:8
1:8

julia> df.B = ["M", "F", "F", "M", "F", "M", "M", "F"]
8-element Vector{String}:
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
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  F
   3 │     3  F
   4 │     4  M
   5 │     5  F
   6 │     6  M
   7 │     7  M
   8 │     8  F
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

It is also possible to fill a `DataFrame` row by row. Let us construct an empty
data frame with two columns (note that the first column can only contain
integers and the second one can only contain strings):

```jldoctest dataframe
julia> df = DataFrame(A=Int[], B=String[])
0×2 DataFrame
```

Rows can then be added as tuples or vectors, where the order of elements matches that of columns:

```jldoctest dataframe
julia> push!(df, (1, "M"))
1×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M

julia> push!(df, [2, "N"])
2×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  N
```

Rows can also be added as `Dict`s, where the dictionary keys match the column names:

```jldoctest dataframe
julia> push!(df, Dict(:B => "F", :A => 3))
3×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  N
   3 │     3  F
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

A particular common case of a collection that supports the
[Tables.jl](https://github.com/JuliaData/Tables.jl) interface is
a vector of `NamedTuple`s:
```jldoctest dataframe
julia> v = [(a=1, b=2), (a=3, b=4)]
2-element Vector{NamedTuple{(:a, :b), Tuple{Int64, Int64}}}:
 (a = 1, b = 2)
 (a = 3, b = 4)

julia> df = DataFrame(v)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     3      4
```
You can also easily convert a data frame back to a vector of `NamedTuple`s:
```jldoctest dataframe
julia> using Tables

julia> Tables.rowtable(df)
2-element Vector{NamedTuple{(:a, :b), Tuple{Int64, Int64}}}:
 (a = 1, b = 2)
 (a = 3, b = 4)
```
