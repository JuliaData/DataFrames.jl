# Working with Data Frames

## Examining the Data

The default printing of `DataFrame` objects only includes a sample of rows and
columns that fits on screen:

```jldoctest dataframe
julia> using DataFrames

julia> df = DataFrame(A=1:2:1000, B=repeat(1:10, inner=50), C=1:500)
500×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     3      1      2
   3 │     5      1      3
   4 │     7      1      4
   5 │     9      1      5
   6 │    11      1      6
   7 │    13      1      7
   8 │    15      1      8
  ⋮  │   ⋮      ⋮      ⋮
 494 │   987     10    494
 495 │   989     10    495
 496 │   991     10    496
 497 │   993     10    497
 498 │   995     10    498
 499 │   997     10    499
 500 │   999     10    500
           485 rows omitted
```

Printing options can be adjusted by calling the `show` function manually:
`show(df, allrows=true)` prints all rows even if they do not fit on screen and
`show(df, allcols=true)` does the same for columns.

The `first` and `last` functions can be used to look at the first and last rows
of a data frame (respectively):

```jldoctest dataframe
julia> first(df, 6)
6×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     3      1      2
   3 │     5      1      3
   4 │     7      1      4
   5 │     9      1      5
   6 │    11      1      6

julia> last(df, 6)
6×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │   989     10    495
   2 │   991     10    496
   3 │   993     10    497
   4 │   995     10    498
   5 │   997     10    499
   6 │   999     10    500
```

Also notice that when `DataFrame` is printed to the console or rendered in HTML
(e.g. in Jupyter Notebook) you get an information about type of elements held in
its columns. For example in this case:

```jldoctest dataframe
julia> using CategoricalArrays

julia> DataFrame(a=1:2, b=[1.0, missing],
                 c=categorical('a':'b'), d=[1//2, missing])
2×4 DataFrame
 Row │ a      b          c     d
     │ Int64  Float64?   Cat…  Rational…?
─────┼────────────────────────────────────
   1 │     1        1.0  a           1//2
   2 │     2  missing    b        missing

```

we can observe that:

* the first column `:a` can hold elements of type `Int64`;
* the second column `:b` can hold `Float64` or `Missing`, which is indicated by
  `?` printed after the name of type;
* the third column `:c` can hold categorical data; here we notice `…`, which
  indicates that the actual name of the type was long and got truncated;
* the type information in fourth column `:d` presents a situation where the name
  is both truncated and the type allows `Missing`.

## Taking a Subset

### Indexing syntax

Specific subsets of a data frame can be extracted using the indexing syntax,
similar to matrices. In the [Indexing](@ref) section of the manual you can find
all the details about the available options. Here we highlight the basic options.

The colon `:` indicates that all items (rows or columns
depending on its position) should be retained:

```jldoctest dataframe
julia> df[1:3, :]
3×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     3      1      2
   3 │     5      1      3

julia> df[[1, 5, 10], :]
3×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     9      1      5
   3 │    19      1     10

julia> df[:, [:A, :B]]
500×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     3      1
   3 │     5      1
   4 │     7      1
   5 │     9      1
   6 │    11      1
   7 │    13      1
   8 │    15      1
  ⋮  │   ⋮      ⋮
 494 │   987     10
 495 │   989     10
 496 │   991     10
 497 │   993     10
 498 │   995     10
 499 │   997     10
 500 │   999     10
    485 rows omitted

julia> df[1:3, [:B, :A]]
3×2 DataFrame
 Row │ B      A
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     1      3
   3 │     1      5

julia> df[[3, 1], [:C]]
2×1 DataFrame
 Row │ C
     │ Int64
─────┼───────
   1 │     3
   2 │     1
```

Do note that `df[!, [:A]]` and `df[:, [:A]]` return a `DataFrame` object, while
`df[!, :A]` and `df[:, :A]` return a vector:

```jldoctest dataframe
julia> df[!, [:A]]
500×1 DataFrame
 Row │ A
     │ Int64
─────┼───────
   1 │     1
   2 │     3
   3 │     5
   4 │     7
   5 │     9
   6 │    11
   7 │    13
   8 │    15
  ⋮  │   ⋮
 494 │   987
 495 │   989
 496 │   991
 497 │   993
 498 │   995
 499 │   997
 500 │   999
485 rows omitted

julia> df[!, [:A]] == df[:, [:A]]
true

julia> df[!, :A]
500-element Vector{Int64}:
   1
   3
   5
   7
   9
  11
  13
  15
  17
  19
   ⋮
 983
 985
 987
 989
 991
 993
 995
 997
 999

julia> df[!, :A] == df[:, :A]
true
```

In the first case, `[:A]` is a vector, indicating that the resulting object
should be a `DataFrame`. On the other hand, `:A` is a single symbol, indicating
that a single column vector should be extracted. Note that in the first case a
vector is required to be passed (not just any iterable), so e.g. `df[:, (:x1,
:x2)]` is not allowed, but `df[:, [:x1, :x2]]` is valid.

It is also possible to use a regular expression as a selector of columns
matching it:
```jldoctest dataframe
julia> df = DataFrame(x1=1, x2=2, y=3)
1×3 DataFrame
 Row │ x1     x2     y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> df[!, r"x"]
1×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
```

A `Not` selector (from the
[InvertedIndices](https://github.com/mbauman/InvertedIndices.jl) package) can be
used to select all columns excluding a specific subset:

```jldoctest dataframe
julia> df[!, Not(:x1)]
1×2 DataFrame
 Row │ x2     y
     │ Int64  Int64
─────┼──────────────
   1 │     2      3
```

Finally, you can use `Not`, `Between`, `Cols` and `All` selectors in more
complex column selection scenarios (note that `Cols()` selects no columns while
`All()` selects all columns therefore `Cols` is a preferred selector if you
write generic code). Here are examples of using each of these selectors:

```jldoctest dataframe
julia> df = DataFrame(r=1, x1=2, x2=3, y=4)
1×4 DataFrame
 Row │ r      x1     x2     y
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      3      4

julia> df[:, Not(:r)] # drop :r column
1×3 DataFrame
 Row │ x1     x2     y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     2      3      4

julia> df[:, Between(:r, :x2)] # keep columns between :r and :x2
1×3 DataFrame
 Row │ r      x1     x2
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      3

julia> df[:, All()] # keep all columns
1×4 DataFrame
 Row │ r      x1     x2     y
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      3      4

julia> df[:, Cols(x -> startswith(x, "x"))] # keep columns whose name starts with "x"
1×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     2      3
```

The following examples show a more complex use of the `Cols` selector, which
moves all columns whose names match `r"x"` regular expression respectively to
the front and to the end of the data frame:
```jldoctest dataframe
julia> df[:, Cols(r"x", :)]
1×4 DataFrame
 Row │ x1     x2     r      y
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     2      3      1      4

julia> df[:, Cols(Not(r"x"), :)]
1×4 DataFrame
 Row │ r      y      x1     x2
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4      2      3
```

The indexing syntax can also be used to select rows based on conditions on
variables:

```jldoctest dataframe
julia> df = DataFrame(A=1:2:1000, B=repeat(1:10, inner=50), C=1:500)
500×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     3      1      2
   3 │     5      1      3
   4 │     7      1      4
   5 │     9      1      5
   6 │    11      1      6
   7 │    13      1      7
   8 │    15      1      8
  ⋮  │   ⋮      ⋮      ⋮
 494 │   987     10    494
 495 │   989     10    495
 496 │   991     10    496
 497 │   993     10    497
 498 │   995     10    498
 499 │   997     10    499
 500 │   999     10    500
           485 rows omitted

julia> df[df.A .> 500, :]
250×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │   501      6    251
   2 │   503      6    252
   3 │   505      6    253
   4 │   507      6    254
   5 │   509      6    255
   6 │   511      6    256
   7 │   513      6    257
   8 │   515      6    258
  ⋮  │   ⋮      ⋮      ⋮
 244 │   987     10    494
 245 │   989     10    495
 246 │   991     10    496
 247 │   993     10    497
 248 │   995     10    498
 249 │   997     10    499
 250 │   999     10    500
           235 rows omitted

julia> df[(df.A .> 500) .& (300 .< df.C .< 400), :]
99×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │   601      7    301
   2 │   603      7    302
   3 │   605      7    303
   4 │   607      7    304
   5 │   609      7    305
   6 │   611      7    306
   7 │   613      7    307
   8 │   615      7    308
  ⋮  │   ⋮      ⋮      ⋮
  93 │   785      8    393
  94 │   787      8    394
  95 │   789      8    395
  96 │   791      8    396
  97 │   793      8    397
  98 │   795      8    398
  99 │   797      8    399
            84 rows omitted
```

Where a specific subset of values needs to be matched, the `in()` function can
be applied:

```jldoctest dataframe
julia> df[in.(df.A, Ref([1, 5, 601])), :]
3×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     5      1      3
   3 │   601      7    301
```

The `Ref` wrapper to `[1, 5, 601]` is needed to protect the vector against being
broadcasted over (the vector will be treated as a scalar when wrapped in `Ref`).
You could write this operation using a comprehension like this (note that it would be slower
so it is not recommended):
`[a in [1, 5, 601] for a in df.A]`.

Equivalently, the `in` function can be called with a single argument to create
a function object that tests whether each value belongs to the subset
(partial application of `in`): `df[in([1, 5, 601]).(df.A), :]`.

!!! note

    As with matrices, subsetting from a data frame will usually return a copy of
    columns, not a view or direct reference.

    The only indexing situations where data frames will **not** return a copy are:

    - when a `!` is placed in the first indexing position
      (`df[!, :A]`, or `df[!, [:A, :B]]`),
    - when using `.` (`getpropery`) notation (`df.A`),
    - when a single row is selected using an integer (`df[1, [:A, :B]]`)
    - when `view` or `@view` is used (e.g. `@view df[1:3, :A]`).

    More details on copies, views, and references can be found
    in the [`getindex` and `view`](@ref) section.

### Subsetting functions

An alternative approach to row subsetting in a data frame is to use
the [`subset`](@ref) function, or the [`subset!`](@ref) function,
which is its in-place variant.

These functions take a data frame as their first argument. The
following positional arguments (one or more) are filtering condition
specifications that must be jointly met. Each condition should be passed as a
`Pair` consisting of source column(s) and a function specifying the filtering
condition taking this or these column(s) as arguments:

```jldoctest dataframe
julia> subset(df, :A => a -> a .< 10, :C => c -> isodd.(c))
3×3 DataFrame
 Row │ A      B      C
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      1
   2 │     5      1      3
   3 │     9      1      5
```

It is a frequent situation that `missing` values might be present in the
filtering columns, which could then lead the filtering condition to return
`missing` instead of the expected `true` or `false`. In order
to handle this situation one can either use the `coalesce` function or pass
the `skipmissing=true` keyword argument to `subset`. Here is an example:

```jldoctest dataframe
julia> df = DataFrame(x=[1, 2, missing, 4])
4×1 DataFrame
 Row │ x
     │ Int64?
─────┼─────────
   1 │       1
   2 │       2
   3 │ missing
   4 │       4

julia> subset(df, :x => x -> coalesce.(iseven.(x), false))
2×1 DataFrame
 Row │ x
     │ Int64?
─────┼────────
   1 │      2
   2 │      4

julia> subset(df, :x => x -> iseven.(x), skipmissing=true)
2×1 DataFrame
 Row │ x
     │ Int64?
─────┼────────
   1 │      2
   2 │      4
```

The [`subset`](@ref) function has been designed in a way that is
consistent with how column transformations are specified in functions like
[`combine`](@ref), [`select`](@ref), and [`transform`](@ref). Examples of column
transformations accepted by these functions are provided in the following
section.

Additionally DataFrames.jl extends the [`filter`](@ref) and [`filter!`](@ref)
functions provided in Julia Base, which also allow subsetting a data frame.
These methods are defined so that DataFrames.jl implements the Julia API
for collections, but it is generally recommended to use the [`subset`](@ref)
and [`subset!`](@ref) functions instead, as they are consistent with other
DataFrames.jl functions (as opposed to [`filter`](@ref) and [`filter!`](@ref)).

## Selecting and transforming columns

You can also use the [`select`](@ref)/[`select!`](@ref) and
[`transform`](@ref)/[`transform!`](@ref) functions to select, rename and
transform columns in a data frame.

The `select` function creates a new data frame:
```jldoctest dataframe
julia> df = DataFrame(x1=[1, 2], x2=[3, 4], y=[5, 6])
2×3 DataFrame
 Row │ x1     x2     y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      3      5
   2 │     2      4      6

julia> select(df, Not(:x1)) # drop column :x1 in a new data frame
2×2 DataFrame
 Row │ x2     y
     │ Int64  Int64
─────┼──────────────
   1 │     3      5
   2 │     4      6

julia> select(df, r"x") # select columns containing 'x' character
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> select(df, :x1 => :a1, :x2 => :a2) # rename columns
2×2 DataFrame
 Row │ a1     a2
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> select(df, :x1, :x2 => (x -> x .- minimum(x)) => :x2) # transform columns
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      1

julia> select(df, :x2, :x2 => ByRow(sqrt)) # transform columns by row
2×2 DataFrame
 Row │ x2     x2_sqrt
     │ Int64  Float64
─────┼────────────────
   1 │     3  1.73205
   2 │     4  2.0

julia> select(df, :x1, :x2, [:x1, :x2] => ((x1, x2) -> x1 ./ x2) => :z) # transform multiple columns
2×3 DataFrame
 Row │ x1     x2     z
     │ Int64  Int64  Float64
─────┼────────────────────────
   1 │     1      3  0.333333
   2 │     2      4  0.5

julia> select(df, :x1, :x2, [:x1, :x2] => ByRow((x1, x2) -> x1 / x2) => :z)  # transform multiple columns by row
2×3 DataFrame
 Row │ x1     x2     z
     │ Int64  Int64  Float64
─────┼────────────────────────
   1 │     1      3  0.333333
   2 │     2      4  0.5

julia> select(df, AsTable(:) => ByRow(extrema) => [:lo, :hi]) # return multiple columns
2×2 DataFrame
 Row │ lo     hi
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
```

It is important to note that `select` always returns a data frame,
even if a single column is selected (as opposed to indexing syntax).
```jldoctest dataframe
julia> select(df, :x1)
2×1 DataFrame
 Row │ x1
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> df[:, :x1]
2-element Vector{Int64}:
 1
 2
```

By default `select` copies columns of a passed source data frame.
In order to avoid copying, pass `copycols=false`:
```
julia> df2 = select(df, :x1)
2×1 DataFrame
 Row │ x1
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> df2.x1 === df.x1
false

julia> df2 = select(df, :x1, copycols=false)
2×1 DataFrame
 Row │ x1
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> df2.x1 === df.x1
true
```

To perform the selection operation in-place use `select!`:
```jldoctest dataframe
julia> select!(df, Not(:x1));

julia> df
2×2 DataFrame
 Row │ x2     y
     │ Int64  Int64
─────┼──────────────
   1 │     3      5
   2 │     4      6
```

`transform` and `transform!` functions work identically to `select` and
`select!`, with the only difference that they retain all columns that are present
in the source data frame. Here are some more advanced examples.

First we show how to generate a column that is a sum of all other columns in the
data frame using the `All()` selector:

```jldoctest dataframe
julia> df = DataFrame(x1=[1, 2], x2=[3, 4], y=[5, 6])
2×3 DataFrame
 Row │ x1     x2     y
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      3      5
   2 │     2      4      6

julia> transform(df, All() => +)
2×4 DataFrame
 Row │ x1     x2     y      x1_x2_y_+
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────────
   1 │     1      3      5          9
   2 │     2      4      6         12
```

Using the `ByRow` wrapper, we can easily compute for each row the name of column
with the highest score:

```
julia> using Random

julia> Random.seed!(1);

julia> df = DataFrame(rand(10, 3), [:a, :b, :c])
10×3 DataFrame
 Row │ a           b          c
     │ Float64     Float64    Float64
─────┼──────────────────────────────────
   1 │ 0.236033    0.555751   0.0769509
   2 │ 0.346517    0.437108   0.640396
   3 │ 0.312707    0.424718   0.873544
   4 │ 0.00790928  0.773223   0.278582
   5 │ 0.488613    0.28119    0.751313
   6 │ 0.210968    0.209472   0.644883
   7 │ 0.951916    0.251379   0.0778264
   8 │ 0.999905    0.0203749  0.848185
   9 │ 0.251662    0.287702   0.0856352
  10 │ 0.986666    0.859512   0.553206

julia> transform(df, AsTable(:) => ByRow(argmax) => :prediction)
10×4 DataFrame
 Row │ a           b          c          prediction
     │ Float64     Float64    Float64    Symbol
─────┼──────────────────────────────────────────────
   1 │ 0.236033    0.555751   0.0769509  b
   2 │ 0.346517    0.437108   0.640396   c
   3 │ 0.312707    0.424718   0.873544   c
   4 │ 0.00790928  0.773223   0.278582   b
   5 │ 0.488613    0.28119    0.751313   c
   6 │ 0.210968    0.209472   0.644883   c
   7 │ 0.951916    0.251379   0.0778264  a
   8 │ 0.999905    0.0203749  0.848185   a
   9 │ 0.251662    0.287702   0.0856352  b
  10 │ 0.986666    0.859512   0.553206   a
```

In the most complex example below we compute row-wise sum, number of
elements, and mean, while ignoring missing values.

```
julia> using Statistics

julia> df = DataFrame(x=[1, 2, missing], y=[1, missing, missing])
3×2 DataFrame
 Row │ x        y
     │ Int64?   Int64?
─────┼──────────────────
   1 │       1        1
   2 │       2  missing
   3 │ missing  missing

julia> transform(df, AsTable(:) .=>
                     ByRow.([sum∘skipmissing,
                             x -> count(!ismissing, x),
                             mean∘skipmissing]) .=>
                     [:sum, :n, :mean])
3×5 DataFrame
 Row │ x        y        sum    n      mean
     │ Int64?   Int64?   Int64  Int64  Float64
─────┼─────────────────────────────────────────
   1 │       1        1      2      2      1.0
   2 │       2  missing      2      1      2.0
   3 │ missing  missing      0      0    NaN
```

While the DataFrames.jl package provides basic data manipulation capabilities,
users are encouraged to use querying frameworks for more convenient and powerful
operations:
- the [Query.jl](https://github.com/davidanthoff/Query.jl) package provides a
  [LINQ](https://en.wikipedia.org/wiki/Language_Integrated_Query)-like interface
  to a large number of data sources
- the [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl)
  package provides interfaces similar to LINQ and
  [dplyr](https://dplyr.tidyverse.org)
- the [DataFrameMacros.jl](https://github.com/jkrumbiegel/DataFrameMacros.jl)
  package provides macros for most standard functions from DataFrames.jl,
  with convenient syntax for the manipulation of multiple columns at once.

See the [Data manipulation frameworks](@ref) section for more information.

## Summarizing Data

The `describe` function returns a data frame summarizing the elementary
statistics and information about each column:

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

julia> describe(df)
2×7 DataFrame
 Row │ variable  mean    min  median  max  nmissing  eltype
     │ Symbol    Union…  Any  Union…  Any  Int64     DataType
─────┼────────────────────────────────────────────────────────
   1 │ A         2.5     1    2.5     4           0  Int64
   2 │ B                 F            M           0  String
```

If you are interested in describing only a subset of columns, then the easiest
way to do it is to pass a subset of an original data frame to `describe` like
this:

```jldoctest dataframe
julia> describe(df[!, [:A]])
1×7 DataFrame
 Row │ variable  mean     min    median   max    nmissing  eltype
     │ Symbol    Float64  Int64  Float64  Int64  Int64     DataType
─────┼──────────────────────────────────────────────────────────────
   1 │ A             2.5      1      2.5      4         0  Int64
```

Of course, one can also compute descriptive statistics directly on individual
columns:

```jldoctest dataframe
julia> using Statistics

julia> mean(df.A)
2.5
```

We can also apply a function to each column of a `DataFrame` using `combine`.
For example:

```jldoctest dataframe
julia> df = DataFrame(A=1:4, B=4.0:-1.0:1.0)
4×2 DataFrame
 Row │ A      B
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      3.0
   3 │     3      2.0
   4 │     4      1.0

julia> combine(df, All() .=> sum)
1×2 DataFrame
 Row │ A_sum  B_sum
     │ Int64  Float64
─────┼────────────────
   1 │    10     10.0

julia> combine(df, All() .=> sum, All() .=> prod)
1×4 DataFrame
 Row │ A_sum  B_sum    A_prod  B_prod
     │ Int64  Float64  Int64   Float64
─────┼─────────────────────────────────
   1 │    10     10.0      24     24.0

julia> combine(df, All() .=> [sum prod]) # the same using 2-dimensional broadcasting
1×4 DataFrame
 Row │ A_sum  B_sum    A_prod  B_prod
     │ Int64  Float64  Int64   Float64
─────┼─────────────────────────────────
   1 │    10     10.0      24     24.0
```

If you would prefer the result to have the same number of rows as the source
data frame, use `select` instead of `combine`.

In the remainder of this section we will discuss more advanced topics related
to the operation specification syntax, so you may decide to skip them if you
want to focus on the most common usage patterns.

A `DataFrame` can store values of any type as its columns, for example
below we show how one can store a `Tuple`:

```
julia> df2 = combine(df, All() .=> extrema)
1×2 DataFrame
 Row │ A_extrema  B_extrema
     │ Tuple…     Tuple…
─────┼───────────────────────
   1 │ (1, 4)     (1.0, 4.0)
```

Later you might want to expand the tuples into separate columns storing the computed
minima and maxima. This can be achieved by passing multiple columns for the output.
Here is an example of how this can be done by writing the column names by-hand for a single
input column:

```
julia> combine(df2, "A_extrema" => identity => ["A_min", "A_max"])
1×2 DataFrame
 Row │ A_min  A_max
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
```

You can extend it to handling all columns in `df2` using broadcasting:

```
julia> combine(df2, All() .=> identity .=> [["A_min", "A_max"], ["B_min", "B_max"]])
1×4 DataFrame
 Row │ A_min  A_max  B_min    B_max
     │ Int64  Int64  Float64  Float64
─────┼────────────────────────────────
   1 │     1      4      1.0      4.0
```

This approach works, but can be improved. Instead of writing all the column names
manually we can instead use a function as a way to specify target column names
based on source column names:

```
julia> combine(df2, All() .=> identity .=> c -> first(c) .* ["_min", "_max"])
1×4 DataFrame
 Row │ A_min  A_max  B_min    B_max
     │ Int64  Int64  Float64  Float64
─────┼────────────────────────────────
   1 │     1      4      1.0      4.0
```

Note that in this example we needed to pass `identity` explicitly since with
`All() => (c -> first(c) .* ["_min", "_max"])` the right-hand side part would be
treated as a transformation and not as a rule for target column names generation.

You might want to perform the transformation of the source data frame into the result
we have just shown in one step. This can be achieved with the following expression:

```
julia> combine(df, All() .=> Ref∘extrema .=> c -> c .* ["_min", "_max"])
1×4 DataFrame
 Row │ A_min  A_max  B_min    B_max
     │ Int64  Int64  Float64  Float64
─────┼────────────────────────────────
   1 │     1      4      1.0      4.0
```

Note that in this case we needed to add a `Ref` call in the `Ref∘extrema` operation specification.
Without `Ref`, `combine` iterates the contents of the value returned by the operation specification function,
which in our case is a tuple of numbers, and tries to expand it assuming that each produced value represents one row,
so one gets an error:

```
julia> combine(df, All() .=> extrema .=> [c -> c .* ["_min", "_max"]])
ERROR: ArgumentError: 'Tuple{Int64, Int64}' iterates 'Int64' values,
which doesn't satisfy the Tables.jl `AbstractRow` interface
```

Note that we used `Ref` as it is a container that is typically used in DataFrames.jl when one
wants to store one row, however, in general it could be another iterator (e.g. a tuple).

## Handling of Columns Stored in a `DataFrame`

Functions that transform a `DataFrame` to produce a
new `DataFrame` always perform a copy of the columns by default, for example:

```jldoctest dataframe
julia> df = DataFrame(A=1:4, B=4.0:-1.0:1.0)
4×2 DataFrame
 Row │ A      B
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      3.0
   3 │     3      2.0
   4 │     4      1.0

julia> df2 = copy(df);

julia> df2.A === df.A
false
```

On the other hand, in-place functions, whose names end with `!`, may mutate the
column vectors of the `DataFrame` they take as an argument. For example:

```jldoctest dataframe
julia> x = [3, 1, 2];

julia> df = DataFrame(x=x)
3×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     3
   2 │     1
   3 │     2

julia> sort!(df)
3×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> x
3-element Vector{Int64}:
 3
 1
 2

julia> df.x[1] = 100
100

julia> df
3×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │   100
   2 │     2
   3 │     3

julia> x
3-element Vector{Int64}:
 3
 1
 2
```
Note that in the above example the original `x` vector is not mutated in the
process, as the `DataFrame(x=x)` constructor makes a copy by default.

In-place functions are safe to call, except when a view of the `DataFrame`
(created via a `view`, `@view` or [`groupby`](@ref))
or when a `DataFrame` created with `copycols=false` are in use.

It is possible to have a direct access to a column `col` of a `DataFrame` `df`
using the syntaxes `df.col`, `df[!, :col]`, via the [`eachcol`](@ref) function,
by accessing a `parent` of a `view` of a column of a `DataFrame`,
or simply by storing the reference to the column vector before the `DataFrame`
was created with `copycols=false`.

```jldoctest dataframe
julia> x = [3, 1, 2];

julia> df = DataFrame(x=x)
3×1 DataFrame
 Row │ x
     │ Int64
─────┼───────
   1 │     3
   2 │     1
   3 │     2

julia> df.x == x
true

julia> df[!, 1] !== x
true

julia> eachcol(df)[1] === df.x
true
```

Note that a column obtained from a `DataFrame` using one of these methods should
not be mutated without caution.

The exact rules of handling columns of a `DataFrame` are explained in [The
design of handling of columns of a `DataFrame`](@ref man-columnhandling) section
of the manual.


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
julia> using DataFrames

julia> df = DataFrame(a=["a", "None", "b", "None"], b=1:4,
                      c=["None", "j", "k", "h"], d=["x", "y", "None", "z"])
4×4 DataFrame
 Row │ a       b      c       d
     │ String  Int64  String  String
─────┼───────────────────────────────
   1 │ a           1  None    x
   2 │ None        2  j       y
   3 │ b           3  k       None
   4 │ None        4  h       z

julia> replace!(df.a, "None" => "c")
4-element Vector{String}:
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
