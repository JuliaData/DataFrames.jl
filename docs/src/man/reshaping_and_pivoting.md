# Reshaping and Pivoting Data

Reshape data from wide to long format using the `stack` function:

```jldoctest reshape
julia> using DataFrames, CSV

julia> iris = CSV.read((joinpath(dirname(pathof(DataFrames)),
                                 "..", "docs", "src", "assets", "iris.csv")),
                       DataFrame)
150×5 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼──────────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
   3 │         4.7         3.2          1.3         0.2  Iris-setosa
   4 │         4.6         3.1          1.5         0.2  Iris-setosa
   5 │         5.0         3.6          1.4         0.2  Iris-setosa
   6 │         5.4         3.9          1.7         0.4  Iris-setosa
   7 │         4.6         3.4          1.4         0.3  Iris-setosa
   8 │         5.0         3.4          1.5         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
 144 │         6.8         3.2          5.9         2.3  Iris-virginica
 145 │         6.7         3.3          5.7         2.5  Iris-virginica
 146 │         6.7         3.0          5.2         2.3  Iris-virginica
 147 │         6.3         2.5          5.0         1.9  Iris-virginica
 148 │         6.5         3.0          5.2         2.0  Iris-virginica
 149 │         6.2         3.4          5.4         2.3  Iris-virginica
 150 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                        135 rows omitted

julia> stack(iris, 1:4)
600×3 DataFrame
 Row │ Species         variable     value
     │ String15        String       Float64
─────┼──────────────────────────────────────
   1 │ Iris-setosa     SepalLength      5.1
   2 │ Iris-setosa     SepalLength      4.9
   3 │ Iris-setosa     SepalLength      4.7
   4 │ Iris-setosa     SepalLength      4.6
   5 │ Iris-setosa     SepalLength      5.0
   6 │ Iris-setosa     SepalLength      5.4
   7 │ Iris-setosa     SepalLength      4.6
   8 │ Iris-setosa     SepalLength      5.0
  ⋮  │       ⋮              ⋮          ⋮
 594 │ Iris-virginica  PetalWidth       2.3
 595 │ Iris-virginica  PetalWidth       2.5
 596 │ Iris-virginica  PetalWidth       2.3
 597 │ Iris-virginica  PetalWidth       1.9
 598 │ Iris-virginica  PetalWidth       2.0
 599 │ Iris-virginica  PetalWidth       2.3
 600 │ Iris-virginica  PetalWidth       1.8
                            585 rows omitted
```

The second optional argument to `stack` indicates the columns to be stacked.
These are normally referred to as the measured variables. Column names can also
be given:

```jldoctest reshape
julia> stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth])
600×3 DataFrame
 Row │ Species         variable     value
     │ String15        String       Float64
─────┼──────────────────────────────────────
   1 │ Iris-setosa     SepalLength      5.1
   2 │ Iris-setosa     SepalLength      4.9
   3 │ Iris-setosa     SepalLength      4.7
   4 │ Iris-setosa     SepalLength      4.6
   5 │ Iris-setosa     SepalLength      5.0
   6 │ Iris-setosa     SepalLength      5.4
   7 │ Iris-setosa     SepalLength      4.6
   8 │ Iris-setosa     SepalLength      5.0
  ⋮  │       ⋮              ⋮          ⋮
 594 │ Iris-virginica  PetalWidth       2.3
 595 │ Iris-virginica  PetalWidth       2.5
 596 │ Iris-virginica  PetalWidth       2.3
 597 │ Iris-virginica  PetalWidth       1.9
 598 │ Iris-virginica  PetalWidth       2.0
 599 │ Iris-virginica  PetalWidth       2.3
 600 │ Iris-virginica  PetalWidth       1.8
                            585 rows omitted
```

Note that all columns can be of different types. Type promotion follows the
rules of `vcat`.

The stacked `DataFrame` that results includes all of the columns not specified
to be stacked. These are repeated for each stacked column. These are normally
refered to as identifier (id) columns. In addition to the id columns, two
additional columns labeled `:variable` and `:values` contain the column
identifier and the stacked columns.

A third optional argument to `stack` represents the id columns that are
repeated. This makes it easier to specify which variables you want included in
the long format:

```jldoctest reshape
julia> stack(iris, [:SepalLength, :SepalWidth], :Species)
300×3 DataFrame
 Row │ Species         variable     value
     │ String15        String       Float64
─────┼──────────────────────────────────────
   1 │ Iris-setosa     SepalLength      5.1
   2 │ Iris-setosa     SepalLength      4.9
   3 │ Iris-setosa     SepalLength      4.7
   4 │ Iris-setosa     SepalLength      4.6
   5 │ Iris-setosa     SepalLength      5.0
   6 │ Iris-setosa     SepalLength      5.4
   7 │ Iris-setosa     SepalLength      4.6
   8 │ Iris-setosa     SepalLength      5.0
  ⋮  │       ⋮              ⋮          ⋮
 294 │ Iris-virginica  SepalWidth       3.2
 295 │ Iris-virginica  SepalWidth       3.3
 296 │ Iris-virginica  SepalWidth       3.0
 297 │ Iris-virginica  SepalWidth       2.5
 298 │ Iris-virginica  SepalWidth       3.0
 299 │ Iris-virginica  SepalWidth       3.4
 300 │ Iris-virginica  SepalWidth       3.0
                            285 rows omitted
```

If you prefer to specify the id columns then use `Not` with `stack` like this:

```jldoctest reshape
julia> stack(iris, Not(:Species))
600×3 DataFrame
 Row │ Species         variable     value
     │ String15        String       Float64
─────┼──────────────────────────────────────
   1 │ Iris-setosa     SepalLength      5.1
   2 │ Iris-setosa     SepalLength      4.9
   3 │ Iris-setosa     SepalLength      4.7
   4 │ Iris-setosa     SepalLength      4.6
   5 │ Iris-setosa     SepalLength      5.0
   6 │ Iris-setosa     SepalLength      5.4
   7 │ Iris-setosa     SepalLength      4.6
   8 │ Iris-setosa     SepalLength      5.0
  ⋮  │       ⋮              ⋮          ⋮
 594 │ Iris-virginica  PetalWidth       2.3
 595 │ Iris-virginica  PetalWidth       2.5
 596 │ Iris-virginica  PetalWidth       2.3
 597 │ Iris-virginica  PetalWidth       1.9
 598 │ Iris-virginica  PetalWidth       2.0
 599 │ Iris-virginica  PetalWidth       2.3
 600 │ Iris-virginica  PetalWidth       1.8
                            585 rows omitted
```

`unstack` converts from a long format to a wide format.
The default is requires specifying which columns are an id variable,
column variable names, and column values:

```jldoctest reshape
julia> iris.id = 1:size(iris, 1)
1:150

julia> longdf = stack(iris, Not([:Species, :id]))
600×4 DataFrame
 Row │ Species         id     variable     value
     │ String15        Int64  String       Float64
─────┼─────────────────────────────────────────────
   1 │ Iris-setosa         1  SepalLength      5.1
   2 │ Iris-setosa         2  SepalLength      4.9
   3 │ Iris-setosa         3  SepalLength      4.7
   4 │ Iris-setosa         4  SepalLength      4.6
   5 │ Iris-setosa         5  SepalLength      5.0
   6 │ Iris-setosa         6  SepalLength      5.4
   7 │ Iris-setosa         7  SepalLength      4.6
   8 │ Iris-setosa         8  SepalLength      5.0
  ⋮  │       ⋮           ⋮         ⋮          ⋮
 594 │ Iris-virginica    144  PetalWidth       2.3
 595 │ Iris-virginica    145  PetalWidth       2.5
 596 │ Iris-virginica    146  PetalWidth       2.3
 597 │ Iris-virginica    147  PetalWidth       1.9
 598 │ Iris-virginica    148  PetalWidth       2.0
 599 │ Iris-virginica    149  PetalWidth       2.3
 600 │ Iris-virginica    150  PetalWidth       1.8
                                   585 rows omitted

julia> unstack(longdf, :id, :variable, :value)
150×5 DataFrame
 Row │ id     SepalLength  SepalWidth  PetalLength  PetalWidth
     │ Int64  Float64?     Float64?    Float64?     Float64?
─────┼─────────────────────────────────────────────────────────
   1 │     1          5.1         3.5          1.4         0.2
   2 │     2          4.9         3.0          1.4         0.2
   3 │     3          4.7         3.2          1.3         0.2
   4 │     4          4.6         3.1          1.5         0.2
   5 │     5          5.0         3.6          1.4         0.2
   6 │     6          5.4         3.9          1.7         0.4
   7 │     7          4.6         3.4          1.4         0.3
   8 │     8          5.0         3.4          1.5         0.2
  ⋮  │   ⋮         ⋮           ⋮            ⋮           ⋮
 144 │   144          6.8         3.2          5.9         2.3
 145 │   145          6.7         3.3          5.7         2.5
 146 │   146          6.7         3.0          5.2         2.3
 147 │   147          6.3         2.5          5.0         1.9
 148 │   148          6.5         3.0          5.2         2.0
 149 │   149          6.2         3.4          5.4         2.3
 150 │   150          5.9         3.0          5.1         1.8
                                               135 rows omitted
```

If the remaining columns are unique, you can skip the id variable and use:

```jldoctest reshape
julia> unstack(longdf, :variable, :value)
150×6 DataFrame
 Row │ Species         id     SepalLength  SepalWidth  PetalLength  PetalWidth ⋯
     │ String15        Int64  Float64?     Float64?    Float64?     Float64?   ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ Iris-setosa         1          5.1         3.5          1.4         0.2 ⋯
   2 │ Iris-setosa         2          4.9         3.0          1.4         0.2
   3 │ Iris-setosa         3          4.7         3.2          1.3         0.2
   4 │ Iris-setosa         4          4.6         3.1          1.5         0.2
   5 │ Iris-setosa         5          5.0         3.6          1.4         0.2 ⋯
   6 │ Iris-setosa         6          5.4         3.9          1.7         0.4
   7 │ Iris-setosa         7          4.6         3.4          1.4         0.3
   8 │ Iris-setosa         8          5.0         3.4          1.5         0.2
  ⋮  │       ⋮           ⋮         ⋮           ⋮            ⋮           ⋮      ⋱
 144 │ Iris-virginica    144          6.8         3.2          5.9         2.3 ⋯
 145 │ Iris-virginica    145          6.7         3.3          5.7         2.5
 146 │ Iris-virginica    146          6.7         3.0          5.2         2.3
 147 │ Iris-virginica    147          6.3         2.5          5.0         1.9
 148 │ Iris-virginica    148          6.5         3.0          5.2         2.0 ⋯
 149 │ Iris-virginica    149          6.2         3.4          5.4         2.3
 150 │ Iris-virginica    150          5.9         3.0          5.1         1.8
                                                               135 rows omitted
```

You can even skip passing the `:variable` and `:value` values as positional
arguments, as they will be used by default, and write:
```jldoctest reshape
julia> unstack(longdf)
150×6 DataFrame
 Row │ Species         id     SepalLength  SepalWidth  PetalLength  PetalWidth ⋯
     │ String15        Int64  Float64?     Float64?    Float64?     Float64?   ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ Iris-setosa         1          5.1         3.5          1.4         0.2 ⋯
   2 │ Iris-setosa         2          4.9         3.0          1.4         0.2
   3 │ Iris-setosa         3          4.7         3.2          1.3         0.2
   4 │ Iris-setosa         4          4.6         3.1          1.5         0.2
   5 │ Iris-setosa         5          5.0         3.6          1.4         0.2 ⋯
   6 │ Iris-setosa         6          5.4         3.9          1.7         0.4
   7 │ Iris-setosa         7          4.6         3.4          1.4         0.3
   8 │ Iris-setosa         8          5.0         3.4          1.5         0.2
  ⋮  │       ⋮           ⋮         ⋮           ⋮            ⋮           ⋮      ⋱
 144 │ Iris-virginica    144          6.8         3.2          5.9         2.3 ⋯
 145 │ Iris-virginica    145          6.7         3.3          5.7         2.5
 146 │ Iris-virginica    146          6.7         3.0          5.2         2.3
 147 │ Iris-virginica    147          6.3         2.5          5.0         1.9
 148 │ Iris-virginica    148          6.5         3.0          5.2         2.0 ⋯
 149 │ Iris-virginica    149          6.2         3.4          5.4         2.3
 150 │ Iris-virginica    150          5.9         3.0          5.1         1.8
                                                               135 rows omitted
```

Passing `view=true` to `stack` returns a data frame whose columns are views into
the original wide data frame. Here is an example:

```jldoctest reshape
julia> stack(iris, view=true)
600×4 DataFrame
 Row │ Species         id     variable     value
     │ String15        Int64  String       Float64
─────┼─────────────────────────────────────────────
   1 │ Iris-setosa         1  SepalLength      5.1
   2 │ Iris-setosa         2  SepalLength      4.9
   3 │ Iris-setosa         3  SepalLength      4.7
   4 │ Iris-setosa         4  SepalLength      4.6
   5 │ Iris-setosa         5  SepalLength      5.0
   6 │ Iris-setosa         6  SepalLength      5.4
   7 │ Iris-setosa         7  SepalLength      4.6
   8 │ Iris-setosa         8  SepalLength      5.0
  ⋮  │       ⋮           ⋮         ⋮          ⋮
 594 │ Iris-virginica    144  PetalWidth       2.3
 595 │ Iris-virginica    145  PetalWidth       2.5
 596 │ Iris-virginica    146  PetalWidth       2.3
 597 │ Iris-virginica    147  PetalWidth       1.9
 598 │ Iris-virginica    148  PetalWidth       2.0
 599 │ Iris-virginica    149  PetalWidth       2.3
 600 │ Iris-virginica    150  PetalWidth       1.8
                                   585 rows omitted
```

This saves memory. To create the view, several `AbstractVector`s are defined:

`:variable` column -- `EachRepeatedVector`
This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.

`:value` column -- `StackedVector`
This is provides a view of the original columns stacked together.

Id columns -- `RepeatedVector`
This repeats the original columns N times where N is the number of columns stacked.

None of these reshaping functions perform any aggregation. To do aggregation,
use the split-apply-combine functions in combination with reshaping. Here is an
example:

```jldoctest reshape
julia> using Statistics

julia> d = stack(iris, Not(:Species))
750×3 DataFrame
 Row │ Species         variable     value
     │ String15        String       Float64
─────┼──────────────────────────────────────
   1 │ Iris-setosa     SepalLength      5.1
   2 │ Iris-setosa     SepalLength      4.9
   3 │ Iris-setosa     SepalLength      4.7
   4 │ Iris-setosa     SepalLength      4.6
   5 │ Iris-setosa     SepalLength      5.0
   6 │ Iris-setosa     SepalLength      5.4
   7 │ Iris-setosa     SepalLength      4.6
   8 │ Iris-setosa     SepalLength      5.0
  ⋮  │       ⋮              ⋮          ⋮
 744 │ Iris-virginica  id             144.0
 745 │ Iris-virginica  id             145.0
 746 │ Iris-virginica  id             146.0
 747 │ Iris-virginica  id             147.0
 748 │ Iris-virginica  id             148.0
 749 │ Iris-virginica  id             149.0
 750 │ Iris-virginica  id             150.0
                            735 rows omitted

julia> x = combine(groupby(d, [:variable, :Species]), :value => mean => :vsum)
15×3 DataFrame
 Row │ variable     Species          vsum
     │ String       String15         Float64
─────┼───────────────────────────────────────
   1 │ SepalLength  Iris-setosa        5.006
   2 │ SepalLength  Iris-versicolor    5.936
   3 │ SepalLength  Iris-virginica     6.588
   4 │ SepalWidth   Iris-setosa        3.418
   5 │ SepalWidth   Iris-versicolor    2.77
   6 │ SepalWidth   Iris-virginica     2.974
   7 │ PetalLength  Iris-setosa        1.464
   8 │ PetalLength  Iris-versicolor    4.26
   9 │ PetalLength  Iris-virginica     5.552
  10 │ PetalWidth   Iris-setosa        0.244
  11 │ PetalWidth   Iris-versicolor    1.326
  12 │ PetalWidth   Iris-virginica     2.026
  13 │ id           Iris-setosa       25.5
  14 │ id           Iris-versicolor   75.5
  15 │ id           Iris-virginica   125.5

julia> first(unstack(x, :Species, :vsum), 6)
5×4 DataFrame
 Row │ variable     Iris-setosa  Iris-versicolor  Iris-virginica
     │ String       Float64?     Float64?         Float64?
─────┼───────────────────────────────────────────────────────────
   1 │ SepalLength        5.006            5.936           6.588
   2 │ SepalWidth         3.418            2.77            2.974
   3 │ PetalLength        1.464            4.26            5.552
   4 │ PetalWidth         0.244            1.326           2.026
   5 │ id                25.5             75.5           125.5
```

To turn an `AbstractDataFrame` on its side, use [`permutedims`](@ref).

```jldoctest reshape
julia> df1 = DataFrame(a=["x", "y"], b=[1.0, 2.0], c=[3, 4], d=[true, false])
2×4 DataFrame
 Row │ a       b        c      d
     │ String  Float64  Int64  Bool
─────┼───────────────────────────────
   1 │ x           1.0      3   true
   2 │ y           2.0      4  false

julia> permutedims(df1, 1)
3×3 DataFrame
 Row │ a       x        y
     │ String  Float64  Float64
─────┼──────────────────────────
   1 │ b           1.0      2.0
   2 │ c           3.0      4.0
   3 │ d           1.0      0.0
```

Note that the column indexed by `src_colnames` in the original `df`
becomes the column names in the permuted result,
and the column names of the original become a new column.
Typically, this would be used on columns with homogenous element types,
since the element types of the other columns
are the result of `promote_type` on _all_ the permuted columns.
Note also that, by default, the new column created from the column names
of the original `df` has the same name as `src_namescol`.
An optional positional argument `dest_namescol` can alter this:

```jldoctest reshape
julia> df2 = DataFrame(a=["x", "y"], b=[1, "two"], c=[3, 4], d=[true, false])
2×4 DataFrame
 Row │ a       b    c      d
     │ String  Any  Int64  Bool
─────┼───────────────────────────
   1 │ x       1        3   true
   2 │ y       two      4  false

julia> permutedims(df2, 1, "different_name")
3×3 DataFrame
 Row │ different_name  x     y
     │ String          Any   Any
─────┼─────────────────────────────
   1 │ b               1     two
   2 │ c               3     4
   3 │ d               true  false
```
