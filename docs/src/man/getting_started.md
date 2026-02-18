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

## Customizing Display Output

DataFrames.jl uses [PrettyTables.jl](https://github.com/ronisbr/PrettyTables.jl) to render
tables in both plain text (`show(df; ...)`) and HTML
(`show(stdout, MIME("text/html"), df; ...)` in notebook-like environments).

The `show` function exposes DataFrames-specific controls and also forwards `kwargs...` to
PrettyTables.jl, so you can customize formatting, styling, and highlights.

DataFrames-specific keywords accepted by `show` are:

- `allrows::Bool` (text): print all rows instead of only the rows that fit the display
   height.
- `allcols::Bool` (text): print all columns instead of only the columns that fit the display
   width.
- `rowlabel::Symbol` (text): set the label used for the row-number column (default: `:Row`).
- `summary::Bool` (text and HTML): show or hide the summary line above the table (for
   example, `3×3 DataFrame`).
- `eltypes::Bool` (text and HTML): show or hide the column element types under the column
   names.
- `truncate::Int` (text): maximum display width for each data column before truncation with
   `…`; `0` or negative disables truncation.
- `show_row_number::Bool` (text and HTML): show or hide row numbers.
- `max_column_width::AbstractString` (HTML): maximum column width as a CSS length (for
   example, `"120px"`); empty string means no width limit.

The remaining `kwargs...` are forwarded to PrettyTables.jl for backend-specific
customization.

For HTML output, DataFrames.jl reserves `rowid`, `title`, and `truncate` keywords. Use
`max_column_width` (instead of `truncate`) to control cell width in HTML rendering, and
`top_left_str` (instead of `title`) to set the table title in HTML rendering.

```jldoctest
julia> using DataFrames

julia> df = DataFrame(
           a = [1, 2, 3],
           b = [3.14, -1.2, 42.0],
           c = ["short", "a very very very very very long string", "ok"]
      );

# This is the default output.
julia> df
3×3 DataFrame
 Row │ a      b        c
     │ Int64  Float64  String
─────┼───────────────────────────────────────────────────
   1 │     1     3.14  short
   2 │     2    -1.2   a very very very very very long …
   3 │     3    42.0   ok

# Using this option, no cell will be truncated if there is room to display it.
julia> show(df; truncate = 0)
3×3 DataFrame
 Row │ a      b        c
     │ Int64  Float64  String
─────┼────────────────────────────────────────────────────────
   1 │     1     3.14  short
   2 │     2    -1.2   a very very very very very long string
   3 │     3    42.0   ok

# Hide row numbers and rename the row label column in text output.
julia> show(df; show_row_number = false)
3×3 DataFrame
 a      b        c
 Int64  Float64  String
───────────────────────────────────────────────────
     1     3.14  short
     2    -1.2   a very very very very very long …
     3    42.0   ok

# Hide the column element types in text output.
julia> show(df; eltypes = false)
3×3 DataFrame
 Row │ a  b      c
─────┼─────────────────────────────────────────────
   1 │ 1   3.14  short
   2 │ 2  -1.2   a very very very very very long …
   3 │ 3  42.0   ok
```

!!! note

    The following examples assume that PrettyTables.jl v3.0 or later is installed. If you
    have an older version of PrettyTables.jl, you may need to update it to use the features
    shown in the examples below.

We can use formatters in PrettyTables.jl to change how cells are converted to strings. The
following example shows how to replace negative values with parentheses in text output.

```jldoctest
julia> using PrettyTables

julia> df = DataFrame(
           A = [ 0.73, -1.28,  1.91, -0.44,  0.12, -2.35,  1.08],
           B = [-0.55,  0.67, -1.49,  2.11, -0.03,  0.94, -2.20],
           C = [ 1.34, -0.88,  0.45, -1.76,  2.53, -0.61,  0.07],
           D = [-1.02,  2.40, -0.31,  0.58, -2.14,  1.77, -0.90],
           E = [ 0.26, -1.67,  2.22, -0.75,  1.05, -0.48, -2.93]
      );

# This is the default output.
julia> df
7×5 DataFrame
 Row │ A        B        C        D        E
     │ Float64  Float64  Float64  Float64  Float64
─────┼──────────────────────────────────────────────
   1 │   0.73    -0.55     1.34    -1.02     0.26
   2 │  -1.28     0.67    -0.88     2.4     -1.67
   3 │   1.91    -1.49     0.45    -0.31     2.22
   4 │  -0.44     2.11    -1.76     0.58    -0.75
   5 │   0.12    -0.03     2.53    -2.14     1.05
   6 │  -2.35     0.94    -0.61     1.77    -0.48
   7 │   1.08    -2.2      0.07    -0.9     -2.93

# We can replace the negative values in text back-end with parentheses using formatters.
# This function is called for each cell in the table. `v` is the current cell value, `i` and
# `j` are the row and column indices of the cell. It must return the new object which will
# be printed in the cell. In this case, we only want to change cells that are negative
# numbers, so we return the original value for all other cells.
julia> function parentheses_fmt(v, i, j)
           !(v isa Number) && return v
           v < 0 && return "($(-v))"
           return v
       end

julia> show(df; formatters = [parentheses_fmt])
7×5 DataFrame
 Row │ A        B        C        D        E
     │ Float64  Float64  Float64  Float64  Float64
─────┼─────────────────────────────────────────────
   1 │   0.73    (0.55)    1.34    (1.02)    0.26
   2 │  (1.28)    0.67    (0.88)    2.4     (1.67)
   3 │   1.91    (1.49)    0.45    (0.31)    2.22
   4 │  (0.44)    2.11    (1.76)    0.58    (0.75)
   5 │   0.12    (0.03)    2.53    (2.14)    1.05
   6 │  (2.35)    0.94    (0.61)    1.77    (0.48)
   7 │   1.08    (2.2)     0.07    (0.9)    (2.93)
```

The color of the cells can be changed using highlighters. The following example shows how to
highlight negative values in red in HTML output.

```julia
# HTML output (e.g. in Jupyter): cap column width and highlight negatives in red.
julia> hl = HtmlHighlighter((data, i, j) -> data[i, j] < 0, ["color" => "red"]);

julia> show(
           stdout,
           MIME("text/html"),
           df;
           highlighters = [hl]
       )
```

You can also add summary rows at the bottom of a table using PrettyTables.jl keywords.  Pass
a vector of functions to the `summary_rows` parameter to compute metrics, and optionally use
`summary_row_labels` to set labels for those rows.

In the following example, a table displays quarterly profits for a fictional company. The
columns represent years (2020 through 2025), the rows represent quarters, and the summary
rows show the mean and standard deviation for each year, calculated across the four
quarterly values in that column.

```julia
julia> using Statistics, PrettyTables

julia> profit = DataFrame(
           "2020" => [ 94.6, -105.6, -104.9,  -88.0],
           "2021" => [-84.3,   -8.7, -109.6,   75.8],
           "2022" => [172.6,  -42.5,   95.5, -141.0],
           "2023" => [-71.2,   51.6,  114.3,   15.5],
           "2024" => [-35.4,  -44.9,  140.3,   30.8],
           "2025" => [ 24.1,  136.1,   34.8, -183.7]
       );

julia> show(
           profit;
           # We use this option to align the summary rows with the data rows at the decimal
           # point.
           apply_alignment_regex_to_summary_rows = true,
           summary_rows = [mean, std],
           summary_row_labels = ["Mean", "Std. Dev."]
       )
4×6 DataFrame
       Row │ 2020       2021       2022      2023      2024      2025
           │ Float64    Float64    Float64   Float64   Float64   Float64
───────────┼──────────────────────────────────────────────────────────────
         1 │   94.6      -84.3      172.6    -71.2     -35.4       24.1
         2 │ -105.6       -8.7      -42.5     51.6     -44.9      136.1
         3 │ -104.9     -109.6       95.5    114.3     140.3       34.8
         4 │  -88.0       75.8     -141.0     15.5      30.8     -183.7
───────────┼──────────────────────────────────────────────────────────────
      Mean │  -50.975    -31.7       21.15    27.55     22.7        2.825
 Std. Dev. │   97.3905    83.5073   140.011   77.4612   85.3244   134.2
```

For more customization options, check the
[PrettyTables.jl documentation](https://ronisbr.github.io/PrettyTables.jl/stable/).
