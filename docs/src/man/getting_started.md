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

    By default DataFrames.jl limits the number of rows and columns when displaying a data frame in a Jupyter
    Notebook to 25 and 100, respectively. You can override this behavior by changing the values of the
    `ENV["DATAFRAMES_COLUMNS"]` and `ENV["DATAFRAMES_ROWS"]` variables to hold the maximum number of columns
    and rows of the output. All columns or rows will be printed if those numbers are equal or lower than 0.

    Alternatively, you may want to set the maximum number of data frame rows to print to `100` and the maximum
    number of columns to print to `1000` for every Julia session using some Jupyter kernel file (numbers `100`
    and `1000` are only examples and can be adjusted). In such case add a
    `"DATAFRAME_COLUMNS": "1000", "DATAFRAMES_ROWS": "100"` entry to the `"env"` variable in this Jupyter kernel
    file. See [here](https://jupyter-client.readthedocs.io/en/stable/kernels.html) for information about location
    and specification of Jupyter kernels.

    The package [PrettyTables.jl](https://github.com/ronisbr/PrettyTables.jl) renders the `DataFrame` in the
    Jupyter notebook. Users can customize the output by passing keywords arguments `kwargs...` to the
    function `show`: `show(stdout, MIME("text/html"), df; kwargs...)`, where `df` is the `DataFrame`. Any
    argument supported by PrettyTables.jl in the HTML backend can be used here. Hence, for example, if the user
    wants to change the color of all numbers smaller than 0 to red in Jupyter, they can execute:
    `show(stdout, MIME("text/html"), df; highlighters = hl_lt(0, HtmlDecoration(color = "red")))` after
    `using PrettyTables`. For more information about the available options, check
    [PrettyTables.jl documentation](https://ronisbr.github.io/PrettyTables.jl/stable/man/usage/).

## The `DataFrame` Type

Objects of the `DataFrame` type represent a data table as a series of vectors,
each corresponding to a column or variable. The simplest way of constructing a
`DataFrame` is to pass column vectors using keyword arguments or pairs:

```jldoctest dataframe
julia> using DataFrames

julia> DataFrame(a=1:4, b=["M", "F", "F", "M"]) # keyword argument constructor
4×2 DataFrame
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  F
   3 │     3  F
   4 │     4  M
```

Here are examples of other commonly used ways to construct a data frame:

```jldoctest dataframe
julia> DataFrame((a=[1, 2], b=[3, 4])) # Tables.jl table constructor from a named tuple of vectors
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> DataFrame([(a=1, b=0), (a=2, b=0)]) # Tables.jl table constructor from a vector of named tuples
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame("a" => 1:2, "b" => 0) # Pair constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([:a => 1:2, :b => 0]) # vector of Pairs constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame(Dict(:a => 1:2, :b => 0)) # dictionary constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([[1, 2], [0, 0]], [:a, :b]) # vector of vectors constructor
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0

julia> DataFrame([1 0; 2 0], :auto) # matrix constructor
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```

Columns can be directly (i.e. without copying) extracted using `df.col`,
`df."col"`, `df[!, :col]` or `df[!, "col"]` (this rule applies to getting data
from a data frame, not writing data to a data frame). The two latter syntaxes
are more flexible as they allow passing a variable holding the name of the
column, and not only a literal name. Note that column names can be either
symbols (written as `:col`, `:var"col"` or `Symbol("col")`) or strings (written
as `"col"`). In the forms `df."col"` and `:var"col"` variable interpolation into
a string using `$` does not work. Columns can also be extracted using an integer
index specifying their position.

Since `df[!, :col]` does not make a copy, changing the elements of the column
vector returned by this syntax will affect the values stored in the original
`df`. To get a copy of the column use `df[:, :col]`: changing the vector
returned by this syntax does not change `df`.


```jldoctest dataframe
julia> df = DataFrame(A=1:4, B=["M", "F", "F", "M"])
4×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │     1  M
   2 │     2  F
   3 │     3  F
   4 │     4  M

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

julia> df[:, :B] = ["M", "F", "F", "M", "F", "M", "M", "F"]
8-element Vector{String}:
 "M"
 "F"
 "F"
 "M"
 "F"
 "M"
 "M"
 "F"

julia> df[!, :C] .= 0
8-element Vector{Int64}:
 0
 0
 0
 0
 0
 0
 0
 0

julia> df
8×3 DataFrame
 Row │ A      B       C
     │ Int64  String  Int64
─────┼──────────────────────
   1 │     1  M           0
   2 │     2  F           0
   3 │     3  F           0
   4 │     4  M           0
   5 │     5  F           0
   6 │     6  M           0
   7 │     7  M           0
   8 │     8  F           0
```

The `DataFrame` we build in this way has 8 rows and 3 columns.
This can be checked using the `size` function:

```jldoctest dataframe
julia> size(df, 1)
8

julia> size(df, 2)
3

julia> size(df)
(8, 3)
```

In the above example notice that the `df[!, :C] .= 0` expression created a new
column in the data frame by broadcasting a scalar.

When setting a column of a data frame the `df[!, :C]` and `df.C` syntaxes are
equivalent and they would replace (or create) the `:C` column in `df`. This
is different from using `df[:, :C]` to set a column in a data frame, which
updates the contents of column in-place if it already exists.

Here is an example showing this difference. Let us try changing the `:B` column
to a binary variable.

```jldoctest dataframe
julia> df[:, :B] = df.B .== "F"
ERROR: MethodError: Cannot `convert` an object of type Bool to an object of type String

julia> df[:, :B] .= df.B .== "F"
ERROR: MethodError: Cannot `convert` an object of type Bool to an object of type String
```

The above operations did not work because when you use `:` as row selector the
`:B` column is updated in-place, and it only supports storing strings.

On the other hand the following works:

```jldoctest dataframe
julia> df.B = df.B .== "F"
8-element BitVector:
 0
 1
 1
 0
 1
 0
 0
 1

julia> df
8×3 DataFrame
 Row │ A      B      C
     │ Int64  Bool   Int64
─────┼─────────────────────
   1 │     1  false      0
   2 │     2   true      0
   3 │     3   true      0
   4 │     4  false      0
   5 │     5   true      0
   6 │     6  false      0
   7 │     7  false      0
   8 │     8   true      0
```

As you can see because we used `df.B` on the right-hand side of the assignment
the `:B` column was replaced. The same effect would be achieved if we used
`df[!, :B]` instead or if we used broadcasted assignment `.=`.

In the [Indexing](@ref) section of the manual you can find all details about all
the available indexing options.

### Constructing Row by Row

It is also possible to fill a `DataFrame` row by row. Let us construct an empty
data frame with two columns (note that the first column can only contain
integers and the second one can only contain strings):

```jldoctest dataframe
julia> df = DataFrame(A=Int[], B=String[])
0×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┴───────────────
```

Rows can then be added as tuples or vectors, where the order of elements matches that of columns.
To add new rows at the end of a data frame use [`push!`](@ref):

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

If you want to add rows at the beginning of a data frame use [`pushfirst!`](@ref)
and to insert a row in an arbitrary location use [`insert!`](@ref).

You can also add whole tables to a data frame using the [`append!`](@ref)
and [`prepend!`](@ref) functions.

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
2-element Vector{@NamedTuple{a::Int64, b::Int64}}:
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
2-element Vector{@NamedTuple{a::Int64, b::Int64}}:
 (a = 1, b = 2)
 (a = 3, b = 4)
```
