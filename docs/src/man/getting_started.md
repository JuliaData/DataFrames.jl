# Getting Started

## Installation

The DataFrames package is available through the Julia package system and can be installed using the following commands:

```julia
using Pkg
Pkg.add("DataFrames")
```

Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed `using DataFrames` to bring all of the relevant variables into your current namespace.

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

Columns can be directly (i.e. without copying) accessed via `df.col`, `df."col"`, `df[!, :col]` or `df[!, "col"]`. The two latter syntaxes are more flexible as they allow passing a variable holding the name of the column, and not only a literal name. Note that column names can be either symbols (written as `:col`, `:var"col"` or `Symbol("col")`) or strings (written as `"col"`). Note that in the forms `df."col"` and `:var"col"` variable interpolation into a string using `$` does not work.
Columns can also be accessed using an integer index specifying their position.

Since `df[!, :col]` does not make a copy, changing the elements of the column vector returned by this syntax will affect the values stored in the original `df`. To get a copy of the column use `df[:, :col]`: changing the vector returned by this syntax does not change `df`.


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
    for all column indexing operations for convenience.
    However, using `Symbol`s is slightly faster and should generally be preferred, if not generating them via string manipulation.


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

A particular common case of a collection that supports the
[Tables.jl](https://github.com/JuliaData/Tables.jl) interface is
a vector of `NamedTuple`s:
```
julia> v = [(a=1,b=2), (a=3,b=4)]
2-element Array{NamedTuple{(:a, :b),Tuple{Int64,Int64}},1}:
 (a = 1, b = 2)
 (a = 3, b = 4)

julia> df = DataFrame(v)
2×2 DataFrame
│ Row │ a     │ b     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 2     │
│ 2   │ 3     │ 4     │
```
You can also easily convert a data frame back to a vector of `NamedTuple`s:
```
julia> using Tables

julia> Tables.rowtable(df)
2-element Array{NamedTuple{(:a, :b),Tuple{Int64,Int64}},1}:
 (a = 1, b = 2)
 (a = 3, b = 4)
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
│ Row │ a     │ b        │ c    │ d         │
│     │ Int64 │ Float64? │ Cat… │ Rationa…? │
├─────┼───────┼──────────┼──────┼───────────┤
│ 1   │ 1     │ 1.0      │ 'a'  │ 1//2      │
│ 2   │ 2     │ missing  │ 'b'  │ missing   │
```

we can observe that:

* the first column `:a` can hold elements of type `Int64`;
* the second column `:b` can hold `Float64` or `Missing`, which is indicated by `?` printed after the name of type;
* the third column `:c` can hold categorical data; here we notice `…`, which indicates that the actual name of the type was long and got truncated;
* the type information in fourth column `:d` presents a situation where the name is both truncated and the type allows `Missing`.

### Taking a Subset

#### Indexing syntax

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

Do note that `df[!, [:A]]` and `df[:, [:A]]` return a `DataFrame` object, while `df[!, :A]` and `df[:, :A]` return a vector:

```jldoctest dataframe
julia> df[!, [:A]]
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

julia> df[!, [:A]] == df[:, [:A]]
true

julia> df[!, :A]
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

julia> df[!, :A] == df[:, :A]
true
```

In the first case, `[:A]` is a vector, indicating that the resulting object should be a `DataFrame`. On the other hand, `:A` is a single symbol, indicating that a single column vector should be extracted. Note that in the first case a vector is required to be passed (not just any iterable), so e.g. `df[:, (:x1, :x2)]` is not allowed, but `df[:, [:x1, :x2]]` is valid.

It is also possible to use a regular expression as a selector of columns matching it:
```jldoctest dataframe
julia> df = DataFrame(x1=1, x2=2, y=3)
1×3 DataFrame
│ Row │ x1    │ x2    │ y     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 3     │

julia> df[!, r"x"]
1×2 DataFrame
│ Row │ x1    │ x2    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 2     │
```

A `Not` selector (from the [InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package) can be used to select all columns excluding a specific subset:

```jldoctest dataframe
julia> df[!, Not(:x1)]
1×2 DataFrame
│ Row │ x2    │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 2     │ 3     │
```

Finally, you can use `Not` and `All` selectors in more complex column selection scenarios.
The following examples move all columns whose names match `r"x"` regular expression respectively to the front and to the end of a data frame:
```
julia> df = DataFrame(r=1, x1=2, x2=3, y=4)
1×4 DataFrame
│ Row │ r     │ x1    │ x2    │ y     │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 1     │ 2     │ 3     │ 4     │

julia> df[:, All(r"x", :)]
1×4 DataFrame
│ Row │ x1    │ x2    │ r     │ y     │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 2     │ 3     │ 1     │ 4     │

julia> df[:, All(Not(r"x"), :)]
1×4 DataFrame
│ Row │ r     │ y     │ x1    │ x2    │
│     │ Int64 │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┼───────┤
│ 1   │ 1     │ 4     │ 2     │ 3     │
```

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
Where a specific subset of values needs to be matched, the `in()` function can be applied:

```jldoctest dataframe
julia> df[in.(df.A, Ref([1, 5, 601])), :]
3×3 DataFrame
│ Row │ A     │ B     │ C     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ 1     │
│ 2   │ 5     │ 1     │ 3     │
│ 3   │ 601   │ 7     │ 301   │
```

Equivalently, the `in` function can be called with a single argument to create
a function object that tests whether each value belongs to the subset
(partial application of `in`): `df[in([1, 5, 601]).(df.A), :]`.

!!! note

    As with matrices, subsetting from a data frame will usually return a copy of
    columns, not a view or direct reference.

    The only indexing situations where data frames will **not** return a copy are:

    - when a `!` is placed in the first indexing position (`df[!, :A]`, or `df[!, [:A, :B]]`),
    - when using `.` (`getpropery`) notation (`df.A`),
    - when a single row is selected using an integer (`df[1, [:A, :B]]`)
    - when `view` or `@view` is used (e.g. `@view df[1:3, :A]`).

    More details on copies, views, and references can be found
    [here.](https://juliadata.github.io/DataFrames.jl/stable/lib/indexing/#getindex-and-view-1)

#### Column selection using `select` and `select!`, `transform` and `transform!`

You can also use the [`select`](@ref) and [`select!`](@ref) functions to select,
rename and transform columns in a data frame.

The `select` function creates a new data frame:
```jldoctest dataframe
julia> df = DataFrame(x1=[1, 2], x2=[3, 4], y=[5, 6])
2×3 DataFrame
│ Row │ x1    │ x2    │ y     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 3     │ 5     │
│ 2   │ 2     │ 4     │ 6     │

julia> select(df, Not(:x1)) # drop column :x1 in a new data frame
2×2 DataFrame
│ Row │ x2    │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 3     │ 5     │
│ 2   │ 4     │ 6     │

julia> select(df, r"x") # select columns containing 'x' character
2×2 DataFrame
│ Row │ x1    │ x2    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 4     │

julia> select(df, :x1 => :a1, :x2 => :a2) # rename columns
2×2 DataFrame
│ Row │ a1    │ a2    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 3     │
│ 2   │ 2     │ 4     │

julia> select(df, :x1, :x2 => (x -> x .- minimum(x)) => :x2) # transform columns
2×2 DataFrame
│ Row │ x1    │ x2    │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 0     │
│ 2   │ 2     │ 1     │

julia> select(df, :x2, :x2 => ByRow(sqrt)) # transform columns by row
2×2 DataFrame
│ Row │ x2    │ x2_sqrt │
│     │ Int64 │ Float64 │
├─────┼───────┼─────────┤
│ 1   │ 3     │ 1.73205 │
│ 2   │ 4     │ 2.0     │
```

It is important to note that `select` always returns a data frame,
even if a single column is selected (as opposed to indexing syntax).
```jldoctest dataframe
julia> select(df, :x1)
1×1 DataFrame
│ Row │ x1    │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │

julia> df[:, :x1]
1-element Array{Int64,1}:
 1
```

By default `select` copies columns of a passed source data frame.
In order to avoid copying, pass `copycols=false`:
```
julia> df2 = select(df, :x1)
1×1 DataFrame
│ Row │ x1    │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │

julia> df2.x1 === df.x1
false

julia> df2 = select(df, :x1, copycols=false)
1×1 DataFrame
│ Row │ x1    │
│     │ Int64 │
├─────┼───────┤
│ 1   │ 1     │

julia> df2.x1 === df.x1
true
```

To perform the selection operation in-place use `select!`:
```jldoctest dataframe
julia> select!(df, Not(:x1));

julia> df
1×2 DataFrame
│ Row │ x2    │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 2     │ 3     │
```

`transform` and `transform!` functions work identically to `select` and `select!` with the only difference that
they retain all columns that are present in the source data frame. Here are some more advanced examples.

First we show how to generate a column that is a sum of all other columns in the data frame
using the `All()` selector:

```jldoctest dataframe
julia> df = DataFrame(x1=[1, 2], x2=[3, 4], y=[5, 6])
2×3 DataFrame
│ Row │ x1    │ x2    │ y     │
│     │ Int64 │ Int64 │ Int64 │
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 3     │ 5     │
│ 2   │ 2     │ 4     │ 6     │

julia> transform(df, All() => +)
2×4 DataFrame
│ Row │ x1    │ x2    │ y     │ x1_x2_y_+ │
│     │ Int64 │ Int64 │ Int64 │ Int64     │
├─────┼───────┼───────┼───────┼───────────┤
│ 1   │ 1     │ 3     │ 5     │ 9         │
│ 2   │ 2     │ 4     │ 6     │ 12        │
```
Using the `ByRow` wrapper, we can easily compute for each row the name of column with the highest score:
```
julia> using Random

julia> Random.seed!(1);

julia> df = DataFrame(rand(10, 3), [:a, :b, :c])
10×3 DataFrame
│ Row │ a          │ b         │ c         │
│     │ Float64    │ Float64   │ Float64   │
├─────┼────────────┼───────────┼───────────┤
│ 1   │ 0.236033   │ 0.555751  │ 0.0769509 │
│ 2   │ 0.346517   │ 0.437108  │ 0.640396  │
│ 3   │ 0.312707   │ 0.424718  │ 0.873544  │
│ 4   │ 0.00790928 │ 0.773223  │ 0.278582  │
│ 5   │ 0.488613   │ 0.28119   │ 0.751313  │
│ 6   │ 0.210968   │ 0.209472  │ 0.644883  │
│ 7   │ 0.951916   │ 0.251379  │ 0.0778264 │
│ 8   │ 0.999905   │ 0.0203749 │ 0.848185  │
│ 9   │ 0.251662   │ 0.287702  │ 0.0856352 │
│ 10  │ 0.986666   │ 0.859512  │ 0.553206  │

julia> transform(df, AsTable(:) => ByRow(argmax) => :prediction)
10×4 DataFrame
│ Row │ a          │ b         │ c         │ prediction │
│     │ Float64    │ Float64   │ Float64   │ Symbol     │
├─────┼────────────┼───────────┼───────────┼────────────┤
│ 1   │ 0.236033   │ 0.555751  │ 0.0769509 │ b          │
│ 2   │ 0.346517   │ 0.437108  │ 0.640396  │ c          │
│ 3   │ 0.312707   │ 0.424718  │ 0.873544  │ c          │
│ 4   │ 0.00790928 │ 0.773223  │ 0.278582  │ b          │
│ 5   │ 0.488613   │ 0.28119   │ 0.751313  │ c          │
│ 6   │ 0.210968   │ 0.209472  │ 0.644883  │ c          │
│ 7   │ 0.951916   │ 0.251379  │ 0.0778264 │ a          │
│ 8   │ 0.999905   │ 0.0203749 │ 0.848185  │ a          │
│ 9   │ 0.251662   │ 0.287702  │ 0.0856352 │ b          │
│ 10  │ 0.986666   │ 0.859512  │ 0.553206  │ a          │
```
In the following, most complex, example below we compute row-wise sum, number of elements, and mean,
while ignoring missing values.
```
julia> using Statistics

julia> df = DataFrame(x=[1, 2, missing], y=[1, missing, missing]);

julia> transform(df, AsTable(:) .=>
                     ByRow.([sum∘skipmissing,
                             x -> count(!ismissing, x),
                             mean∘skipmissing]) .=>
                     [:sum, :n, :mean])
3×5 DataFrame
│ Row │ x       │ y       │ sum   │ n     │ mean    │
│     │ Int64?  │ Int64?  │ Int64 │ Int64 │ Float64 │
├─────┼─────────┼─────────┼───────┼───────┼─────────┤
│ 1   │ 1       │ 1       │ 2     │ 2     │ 1.0     │
│ 2   │ 2       │ missing │ 2     │ 1     │ 2.0     │
│ 3   │ missing │ missing │ 0     │ 0     │ NaN     │
```

While the DataFrames.jl package provides basic data manipulation capabilities,
users are encouraged to use querying frameworks for more convenient and powerful operations:
- the [Query.jl](https://github.com/davidanthoff/Query.jl) package provides a
[LINQ](https://msdn.microsoft.com/en-us/library/bb397926.aspx)-like interface to a large number of data sources
- the [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl)
package provides interfaces similar to LINQ and [dplyr](https://dplyr.tidyverse.org)

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

If you are interested in describing only a subset of columns then the easiest way
to do it is to pass a subset of an original data frame to `describe` like this:
```jldoctest dataframe
julia> describe(df[!, [:A]))
1×8 DataFrame
│ Row │ variable │ mean    │ min   │ median  │ max   │ nunique │ nmissing │ eltype   │
│     │ Symbol   │ Float64 │ Int64 │ Float64 │ Int64 │ Nothing │ Nothing  │ DataType │
├─────┼──────────┼─────────┼───────┼─────────┼───────┼─────────┼──────────┼──────────┤
│ 1   │ A        │ 2.5     │ 1     │ 2.5     │ 4     │         │          │ Int64    │
```

Of course, one can also compute descriptive statistics directly on individual columns:
```jldoctest dataframe
julia> using Statistics

julia> mean(df.A)
2.5
```

We can also apply a function to each column of a `DataFrame` using `combine`. For example:
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

julia> combine(df, names(df) .=> sum)
1×2 DataFrame
│ Row │ A_sum │ B_sum   │
│     │ Int64 │ Float64 │
├─────┼───────┼─────────┤
│ 1   │ 10    │ 10.0    │

julia> combine(df, names(df) .=> sum, names(df) .=> prod)
1×4 DataFrame
│ Row │ A_sum │ B_sum   │ A_prod │ B_prod  │
│     │ Int64 │ Float64 │ Int64  │ Float64 │
├─────┼───────┼─────────┼────────┼─────────┤
│ 1   │ 10    │ 10.0    │ 24     │ 24.0    │
```

If you would prefer the result to have the same number of rows as the source data
frame use `select` instead of `combine`.

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
or when a `DataFrame` created with `copycols=false` (or with the `DataFrame!` function)
are in use.

It is possible to have a direct access to a column `col` of a `DataFrame` `df`
using the syntaxes `df.col`, `df[!, :col]`, via the [`eachcol`](@ref) function,
by accessing a `parent` of a `view` of a column of a `DataFrame`,
or simply by storing the reference to the column vector before the `DataFrame`
was created with `copycols=false` (or with the `DataFrame!` function).

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

julia> eachcol(df)[1] === df.x
true
```

Note that a column obtained from a `DataFrame` using one of these methods should
not be mutated without caution.

The exact rules of handling columns of a `DataFrame` are explained in
[The design of handling of columns of a `DataFrame`](@ref man-columnhandling) section of the manual.

## Replacing Data

Several approaches can be used to replace some values with others in a data frame. Some apply the replacement to all values in a data frame, and others to individual columns or subset of columns.

Do note that in-place replacement requires that the replacement value can be converted to the column's element type. In particular, this implies that replacing a value with `missing` requires a call to `allowmissing!` if the column did not allow for missing values.

Replacement operations affecting a single column can be performed using `replace!`:
```jldoctest replace
julia> df = DataFrame(a = ["a", "None", "b", "None"], b = 1:4, c = ["None", "j", "k", "h"], d = ["x", "y", "None", "z"])
4×4 DataFrame
│ Row │ a      │ b     │ c      │ d      │
│     │ String │ Int64 │ String │ String │
├─────┼────────┼───────┼────────┼────────┤
│ 1   │ a      │ 1     │ None   │ x      │
│ 2   │ None   │ 2     │ j      │ y      │
│ 3   │ b      │ 3     │ k      │ None   │
│ 4   │ None   │ 4     │ h      │ z      │

julia> replace!(df.a, "None" => "c")
4-element Array{String,1}:
 "a"
 "c"
 "b"
 "c"

julia> df
4×4 DataFrame
│ Row │ a      │ b     │ c      │ d      │
│     │ String │ Int64 │ String │ String │
├─────┼────────┼───────┼────────┼────────┤
│ 1   │ a      │ 1     │ None   │ x      │
│ 2   │ c      │ 2     │ j      │ y      │
│ 3   │ b      │ 3     │ k      │ None   │
│ 4   │ c      │ 4     │ h      │ z      │
```
This is equivalent to `df.a = replace(df.a, "None" => "c")`, but operates in-place, without allocating a new column vector.

Replacement operations on multiple columns or on the whole data frame can be performed in-place using the broadcasting syntax:
```jldoctest replace
# replacement on a subset of columns [:c, :d]
julia> df[:, [:c, :d]] .= ifelse.(df[!, [:c, :d]] .== "None", "c", df[!, [:c, :d]])
4×2 DataFrame
│ Row │ c      │ d      │
│     │ String │ String │
├─────┼────────┼────────┤
│ 1   │ c      │ x      │
│ 2   │ j      │ y      │
│ 3   │ k      │ c      │
│ 4   │ h      │ z      │

julia> df
4×4 DataFrame
│ Row │ a      │ b     │ c      │ d      │
│     │ String │ Int64 │ String │ String │
├─────┼────────┼───────┼────────┼────────┤
│ 1   │ a      │ 1     │ c      │ x      │
│ 2   │ c      │ 2     │ j      │ y      │
│ 3   │ b      │ 3     │ k      │ c      │
│ 4   │ c      │ 4     │ h      │ z      │

# replacement on entire data frame
julia> df .= ifelse.(df .== "c", "None", df)
4×4 DataFrame
│ Row │ a      │ b     │ c      │ d      │
│     │ String │ Int64 │ String │ String │
├─────┼────────┼───────┼────────┼────────┤
│ 1   │ a      │ 1     │ None   │ x      │
│ 2   │ None   │ 2     │ j      │ y      │
│ 3   │ b      │ 3     │ k      │ None   │
│ 4   │ None   │ 4     │ h      │ z      │
```
Do note that in the above examples, changing `.=` to just `=` will allocate new column vectors instead of applying the operation in-place.

When replacing values with `missing`, if the columns do not already allow for missing values, one has to either avoid in-place operation and use `=` instead of `.=`, or call `allowmissing!` beforehand:
```jldoctest replace
# do not operate in-place (`df = ` would also work)
julia> df2 = ifelse.(df .== "None", missing, df)
4×4 DataFrame
│ Row │ a       │ b     │ c       │ d       │
│     │ String? │ Int64 │ String? │ String? │
├─────┼─────────┼───────┼─────────┼─────────┤
│ 1   │ a       │ 1     │ missing │ x       │
│ 2   │ missing │ 2     │ j       │ y       │
│ 3   │ b       │ 3     │ k       │ missing │
│ 4   │ missing │ 4     │ h       │ z       │

# operate in-place after allowing for missing
julia> allowmissing!(df)
4×4 DataFrame
│ Row │ a       │ b      │ c       │ d       │
│     │ String? │ Int64? │ String? │ String? │
├─────┼─────────┼────────┼─────────┼─────────┤
│ 1   │ a       │ 1      │ None    │ x       │
│ 2   │ None    │ 2      │ j       │ y       │
│ 3   │ b       │ 3      │ k       │ None    │
│ 4   │ None    │ 4      │ h       │ z       │

julia> df .= ifelse.(df .== "None", missing, df)
4×4 DataFrame
│ Row │ a       │ b     │ c       │ d       │
│     │ String? │ Int64 │ String? │ String? │
├─────┼─────────┼───────┼─────────┼─────────┤
│ 1   │ a       │ 1     │ missing │ x       │
│ 2   │ missing │ 2     │ j       │ y       │
│ 3   │ b       │ 3     │ k       │ missing │
│ 4   │ missing │ 4     │ h       │ z       │
```


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
DataFrame(CSV.File(input))
```

A `DataFrame` can be written to a CSV file at path `output` using
```julia
df = DataFrame(x = 1, y = 2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more information, see `?CSV.File`, `?CSV.read` and `?CSV.write`, or checkout the online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).
