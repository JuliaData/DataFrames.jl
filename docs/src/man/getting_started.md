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

julia> df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])
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
4-element Array{Int64,1}:
 1
 2
 3
 4

julia> df."A"
4-element Array{Int64,1}:
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
2-element Array{String,1}:
 "A"
 "B"
```

To get column names as `Symbol`s use the `propertynames` function:
```
julia> propertynames(df)
2-element Array{Symbol,1}:
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
julia> df = DataFrame(A = Int[], B = String[])
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
```
julia> v = [(a=1, b=2), (a=3, b=4)]
2-element Array{NamedTuple{(:a, :b),Tuple{Int64,Int64}},1}:
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
```
julia> using Tables

julia> Tables.rowtable(df)
2-element Array{NamedTuple{(:a, :b),Tuple{Int64,Int64}},1}:
 (a = 1, b = 2)
 (a = 3, b = 4)
```

## Replacing Data

Several approaches can be used to replace some values with others in a data
frame. Some apply the replacement to all values in a data frame, and others to
individual columns or subset of columns.

Do note that in-place replacement requires that the replacement value can be
converted to the column's element type. In particular, this implies that
replacing a value with `missing` requires a call to `allowmissing!` if the
column did not allow for missing values.

Replacement operations affecting a single column can be performed using `replace!`:
```jldoctest replace
julia> df = DataFrame(a = ["a", "None", "b", "None"], b = 1:4, c = ["None", "j", "k", "h"], d = ["x", "y", "None", "z"])
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ None        2  j       y
   3 │ b           3  k       None
   4 │ None        4  h       z

julia> replace!(df.a, "None" => "c")
4-element Array{String,1}:
 "a"
 "c"
 "b"
 "c"

julia> df
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ c           2  j       y
   3 │ b           3  k       None
   4 │ c           4  h       z
```

This is equivalent to `df.a = replace(df.a, "None" => "c")`, but operates
in-place, without allocating a new column vector.

Replacement operations on multiple columns or on the whole data frame can be
performed in-place using the broadcasting syntax:

```jldoctest replace
# replacement on a subset of columns [:c, :d]
julia> df[:, [:c, :d]] .= ifelse.(df[!, [:c, :d]] .== "None", "c", df[!, [:c, :d]])
4×2 SubDataFrame
 Row │ c       d
     │ String  String
─────┼────────────────
   1 │ c       x
   2 │ j       y
   3 │ k       c
   4 │ h       z

julia> df
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  c       x
   2 │ c           2  j       y
   3 │ b           3  k       c
   4 │ c           4  h       z

julia> df .= ifelse.(df .== "c", "None", df) # replacement on entire data frame

4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ None        2  j       y
   3 │ b           3  k       None
   4 │ None        4  h       z
```

Do note that in the above examples, changing `.=` to just `=` will allocate new
column vectors instead of applying the operation in-place.

When replacing values with `missing`, if the columns do not already allow for
missing values, one has to either avoid in-place operation and use `=` instead
of `.=`, or call `allowmissing!` beforehand:

```jldoctest replace
julia> df2 = ifelse.(df .== "None", missing, df) # do not operate in-place (`df = ` would also work)
4×4 DataFrame
 Row │ a        b      c        d
     │ String?  Int64  String?  String?
─────┼──────────────────────────────────
   1 │ a            1  missing  x
   2 │ missing      2  j        y
   3 │ b            3  k        missing
   4 │ missing      4  h        z

julia> allowmissing!(df) # operate in-place after allowing for missing
4×4 DataFrame
 Row │ a        b       c        d
     │ String?  Int64?  String?  String?
─────┼───────────────────────────────────
   1 │ a             1  None     x
   2 │ None          2  j        y
   3 │ b             3  k        None
   4 │ None          4  h        z

julia> df .= ifelse.(df .== "None", missing, df)
4×4 DataFrame
 Row │ a        b       c        d
     │ String?  Int64?  String?  String?
─────┼───────────────────────────────────
   1 │ a             1  missing  x
   2 │ missing       2  j        y
   3 │ b             3  k        missing
   4 │ missing       4  h        z
```

## Importing and Exporting Data (I/O)

For reading and writing tabular data from CSV and other delimited text files,
use the [CSV.jl](https://github.com/JuliaData/CSV.jl) package.

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
DataFrame(CSV.File(input))
```

A `DataFrame` can be written to a CSV file at path `output` using
```julia
df = DataFrame(x = 1, y = 2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more
information, see `?CSV.File`, `?CSV.read` and `?CSV.write`, or checkout the
online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).

For reading and writing tabular data in Apache Arrow format use
[Arrow.jl](https://github.com/JuliaData/Arrow.jl)
