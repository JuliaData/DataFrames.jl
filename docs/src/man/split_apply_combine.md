# The Split-Apply-Combine Strategy

Many data analysis tasks involve three steps:
1. splitting a data set into groups,
2. applying some functions to each of the groups,
3. combining the results.

Note that any of the steps 1 and 3 of this general procedure can be dropped,
in which case we just transform a data frame without grouping it and later
combining the result.

A standardized framework for handling this sort of computation is described in
the paper "[The Split-Apply-Combine Strategy for Data
Analysis](http://www.jstatsoft.org/v40/i01)", written by Hadley Wickham.

The DataFrames package supports the split-apply-combine strategy through the
`groupby` function that creates a `GroupedDataFrame`,
followed by `combine`, `select`/`select!` or `transform`/`transform!`.

All operations described in this section of the manual are supported both for
`AbstractDataFrame` (when split and combine steps are skipped) and
`GroupedDataFrame`. Technically, `AbstractDataFrame` is just considered as being
grouped on no columns (meaning it has a single group, or zero groups if it is
empty). The only difference is that in this case the `keepkeys` and `ungroup`
keyword arguments (described below) are not supported and a data frame is always
returned, as there are no split and combine steps in this case.

In order to perform operations by groups you first need to create a `GroupedDataFrame`
object from your data frame using the `groupby` function that takes two arguments:
(1) a data frame to be grouped, and (2) a set of columns to group by.

Operations can then be applied on each group using one of the following functions:
* `combine`: does not put restrictions on number of rows returned, the order of rows
  is specified by the order of groups in `GroupedDataFrame`; it is typically used
  to compute summary statistics by group;
* `select`: return a data frame with the number and order of rows exactly the same
  as the source data frame, including only new calculated columns;
  `select!` is an in-place version of `select`;
* `transform`: return a data frame with the number and order of rows exactly the same
  as the source data frame, including all columns from the source and new calculated columns;
  `transform!` is an in-place version of `transform`.

All these functions take a specification of one or more functions to apply to
each subset of the `DataFrame`. This specification can be of the following forms:
1. standard column selectors (integers, `Symbol`s, strings, vectors of integers,
   vectors of `Symbol`s, vectors of strings,
   `All`, `Cols`, `:`, `Between`, `Not` and regular expressions)
2. a `cols => function` pair indicating that `function` should be called with
   positional arguments holding columns `cols`, which can be any valid column selector;
   in this case target column name is automatically generated and it is assumed that
   `function` returns a single value or a vector; the generated name is created by
   concatenating source column name and `function` name by default (see examples below).
3. a `cols => function => target_cols` form additionally explicitly specifying
   the target column or columns, which must be a single name (as a `Symbol` or a string),
   a vector of names or `AsTable`. Additionally it can be a `Function` which
   takes a string or a vector of strings as an argument containing names of columns
   selected by `cols`, and returns the target columns names (all accepted types
   except `AsTable` are allowed).
4. a `col => target_cols` pair, which renames the column `col` to `target_cols`, which
   must be single name (as a `Symbol` or a string), a vector of names or `AsTable`.
5. a `nrow` or `nrow => target_cols` form which efficiently computes the number of rows
   in a group; without `target_cols` the new column is called `:nrow`, otherwise
   it must be single name (as a `Symbol` or a string).
6. vectors or matrices containing transformations specified by the `Pair` syntax
   described in points 2 to 5
7. a function which will be called with a `SubDataFrame` corresponding to each group
   if a `GroupedDataFrame` is processed, or with the data frame itself if
   an `AbstractDataFrame` is processed;
   this form should be avoided due to its poor performance unless the number of groups
   is small or a very large number of columns are processed
   (in which case `SubDataFrame` avoids excessive compilation)

Note! If the expression of the form `x => y` is passed then except for the special
convenience form `nrow => target_cols` it is always interpreted as
`cols => function`. In particular the following expression `function => target_cols`
is not a valid transformation specification.

Note! If `cols` or `target_cols` are one of `All`, `Cols`, `Between`, or `Not`,
broadcasting using `.=>` is supported and is equivalent to broadcasting
the result of `names(df, cols)` or `names(df, target_cols)`.
This behaves as if broadcasting happened after replacing the selector
with selected column names within the data frame scope.

All functions have two types of signatures. One of them takes a `GroupedDataFrame`
as the first argument and an arbitrary number of transformations described above
as following arguments. The second type of signature is when a `Function` or a `Type`
is passed as the first argument and a `GroupedDataFrame` as the second argument
(similar to `map`).

As a special rule, with the `cols => function` and
`cols => function => target_cols` syntaxes, if `cols` is wrapped in an `AsTable`
object then a `NamedTuple` containing columns selected by `cols` is passed to
`function`.

What is allowed for `function` to return is determined by the `target_cols` value:
1. If both `cols` and `target_cols` are omitted (so only a `function` is passed),
   then returning a data frame, a matrix, a `NamedTuple`, or a `DataFrameRow` will
   produce multiple columns in the result. Returning any other value produces
   a single column.
2. If `target_cols` is a `Symbol` or a string then the function is assumed to return
   a single column. In this case returning a data frame, a matrix, a `NamedTuple`,
   or a `DataFrameRow` raises an error.
3. If `target_cols` is a vector of `Symbol`s or strings or `AsTable` it is assumed
   that `function` returns multiple columns.
   If `function` returns one of `AbstractDataFrame`, `NamedTuple`, `DataFrameRow`,
   `AbstractMatrix` then rules described in point 1 above apply.
   If `function` returns an `AbstractVector` then each element of this vector must
   support the `keys` function, which must return a collection of `Symbol`s, strings
   or integers; the return value of `keys` must be identical for all elements.
   Then as many columns are created as there are elements in the return value
   of the `keys` function. If `target_cols` is `AsTable` then their names
   are set to be equal to the key names except if `keys` returns integers, in
   which case they are prefixed by `x` (so the column names are e.g. `x1`,
   `x2`, ...). If `target_cols` is a vector of `Symbol`s or strings then
   column names produced using the rules above are ignored and replaced by
   `target_cols` (the number of columns must be the same as the length of
   `target_cols` in this case).
   If `fun` returns a value of any other type then it is assumed that it is a
   table conforming to the Tables.jl API and the `Tables.columntable` function
   is called on it to get the resulting columns and their names. The names are
   retained when `target_cols` is `AsTable` and are replaced if
   `target_cols` is a vector of `Symbol`s or strings.

In all of these cases, `function` can return either a single row or multiple
rows. As a particular rule, values wrapped in a `Ref` or a `0`-dimensional
`AbstractArray` are unwrapped and then treated as a single row.

`select`/`select!` and `transform`/`transform!` always return a data frame
with the same number and order of rows as the source (even if `GroupedDataFrame`
had its groups reordered), except when selection results in zero columns
in the resulting data frame (in which case the result has zero rows).

For `combine`, rows in the returned object appear in the order of groups in the
`GroupedDataFrame`. The functions can return an arbitrary number of rows for
each group, but the kind of returned object and the number and names of columns
must be the same for all groups, except when a `DataFrame()` or `NamedTuple()`
is returned, in which case a given group is skipped.

It is allowed to mix single values and vectors if multiple transformations
are requested. In this case single value will be repeated to match the length
of columns specified by returned vectors.

A separate task is spawned for each specified transformation; each transformation
then spawns as many tasks as Julia threads, and splits processing of groups across
them (however, currently transformations with optimized implementations like `sum`
and transformations that return multiple rows use a single task for all groups).
This allows for parallel operation when Julia was started with more than one
thread. Passed transformation functions should therefore not modify global variables
(i.e. they should be pure), or use locks to control parallel accesses.

To apply `function` to each row instead of whole columns, it can be wrapped in a
`ByRow` struct. `cols` can be any column indexing syntax, in which case
`function` will be passed one argument for each of the columns specified by
`cols` or a `NamedTuple` of them if specified columns are wrapped in `AsTable`.
If `ByRow` is used it is allowed for `cols` to select an empty set of columns,
in which case `function` is called for each row without any arguments and an
empty `NamedTuple` is passed if empty set of columns is wrapped in `AsTable`.

The following keyword arguments are supported by the transformation functions
(not all keyword arguments are supported in all cases; in general they are allowed
in situations when they are meaningful, see the documentation of the specific functions
for details):
- `keepkeys` : whether grouping columns should be kept in the returned data frame.
- `ungroup` : whether the return value of the operation should be a data frame or a
  `GroupedDataFrame`.
- `copycols` : whether columns of the source data frame should be copied if no
  transformation is applied to them.
- `renamecols` : whether in the `cols => function` form automatically generated
  column names should include the name of transformation functions or not.

We show several examples of these functions applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

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

julia> gdf = groupby(iris, :Species)
GroupedDataFrame with 3 groups based on key: Species
First Group (50 rows): Species = "Iris-setosa"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼───────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
   3 │         4.7         3.2          1.3         0.2  Iris-setosa
   4 │         4.6         3.1          1.5         0.2  Iris-setosa
   5 │         5.0         3.6          1.4         0.2  Iris-setosa
   6 │         5.4         3.9          1.7         0.4  Iris-setosa
   7 │         4.6         3.4          1.4         0.3  Iris-setosa
   8 │         5.0         3.4          1.5         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮            ⋮
  43 │         4.4         3.2          1.3         0.2  Iris-setosa
  44 │         5.0         3.5          1.6         0.6  Iris-setosa
  45 │         5.1         3.8          1.9         0.4  Iris-setosa
  46 │         4.8         3.0          1.4         0.3  Iris-setosa
  47 │         5.1         3.8          1.6         0.2  Iris-setosa
  48 │         4.6         3.2          1.4         0.2  Iris-setosa
  49 │         5.3         3.7          1.5         0.2  Iris-setosa
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      34 rows omitted
⋮
Last Group (50 rows): Species = "Iris-virginica"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼──────────────────────────────────────────────────────────────────
   1 │         6.3         3.3          6.0         2.5  Iris-virginica
   2 │         5.8         2.7          5.1         1.9  Iris-virginica
   3 │         7.1         3.0          5.9         2.1  Iris-virginica
   4 │         6.3         2.9          5.6         1.8  Iris-virginica
   5 │         6.5         3.0          5.8         2.2  Iris-virginica
   6 │         7.6         3.0          6.6         2.1  Iris-virginica
   7 │         4.9         2.5          4.5         1.7  Iris-virginica
   8 │         7.3         2.9          6.3         1.8  Iris-virginica
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
  43 │         5.8         2.7          5.1         1.9  Iris-virginica
  44 │         6.8         3.2          5.9         2.3  Iris-virginica
  45 │         6.7         3.3          5.7         2.5  Iris-virginica
  46 │         6.7         3.0          5.2         2.3  Iris-virginica
  47 │         6.3         2.5          5.0         1.9  Iris-virginica
  48 │         6.5         3.0          5.2         2.0  Iris-virginica
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         34 rows omitted

julia> combine(gdf, :PetalLength => mean)
3×2 DataFrame
 Row │ Species          PetalLength_mean
     │ String15         Float64
─────┼───────────────────────────────────
   1 │ Iris-setosa                 1.464
   2 │ Iris-versicolor             4.26
   3 │ Iris-virginica              5.552

julia> combine(gdf, nrow)
3×2 DataFrame
 Row │ Species          nrow
     │ String15         Int64
─────┼────────────────────────
   1 │ Iris-setosa         50
   2 │ Iris-versicolor     50
   3 │ Iris-virginica      50

julia> combine(gdf, nrow, :PetalLength => mean => :mean)
3×3 DataFrame
 Row │ Species          nrow   mean
     │ String15         Int64  Float64
─────┼─────────────────────────────────
   1 │ Iris-setosa         50    1.464
   2 │ Iris-versicolor     50    4.26
   3 │ Iris-virginica      50    5.552

julia> combine(gdf, [:PetalLength, :SepalLength] => ((p, s) -> (a=mean(p)/mean(s), b=sum(p))) =>
               AsTable) # multiple columns are passed as arguments
3×3 DataFrame
 Row │ Species          a         b
     │ String15         Float64   Float64
─────┼────────────────────────────────────
   1 │ Iris-setosa      0.292449     73.2
   2 │ Iris-versicolor  0.717655    213.0
   3 │ Iris-virginica   0.842744    277.6

julia> combine(gdf,
               AsTable([:PetalLength, :SepalLength]) =>
               x -> std(x.PetalLength) / std(x.SepalLength)) # passing a NamedTuple
3×2 DataFrame
 Row │ Species          PetalLength_SepalLength_function
     │ String15         Float64
─────┼───────────────────────────────────────────────────
   1 │ Iris-setosa                              0.492245
   2 │ Iris-versicolor                          0.910378
   3 │ Iris-virginica                           0.867923

julia> combine(x -> std(x.PetalLength) / std(x.SepalLength), gdf) # passing a SubDataFrame
3×2 DataFrame
 Row │ Species          x1
     │ String15         Float64
─────┼───────────────────────────
   1 │ Iris-setosa      0.492245
   2 │ Iris-versicolor  0.910378
   3 │ Iris-virginica   0.867923

julia> combine(gdf, 1:2 => cor, nrow)
3×3 DataFrame
 Row │ Species          SepalLength_SepalWidth_cor  nrow
     │ String15         Float64                     Int64
─────┼────────────────────────────────────────────────────
   1 │ Iris-setosa                        0.74678      50
   2 │ Iris-versicolor                    0.525911     50
   3 │ Iris-virginica                     0.457228     50

julia> combine(gdf, :PetalLength => (x -> [extrema(x)]) => [:min, :max])
3×3 DataFrame
 Row │ Species          min      max
     │ String15         Float64  Float64
─────┼───────────────────────────────────
   1 │ Iris-setosa          1.0      1.9
   2 │ Iris-versicolor      3.0      5.1
   3 │ Iris-virginica       4.5      6.9
```

Contrary to `combine`, the `select` and `transform` functions always return
a data frame with the same number and order of rows as the source.
In the example below
the return values in columns `:SepalLength_SepalWidth_cor` and `:nrow` are
broadcasted to match the number of elements in each group:
```
julia> select(gdf, 1:2 => cor)
150×2 DataFrame
 Row │ Species         SepalLength_SepalWidth_cor
     │ String          Float64
─────┼────────────────────────────────────────────
   1 │ Iris-setosa                       0.74678
   2 │ Iris-setosa                       0.74678
   3 │ Iris-setosa                       0.74678
   4 │ Iris-setosa                       0.74678
  ⋮  │       ⋮                     ⋮
 148 │ Iris-virginica                    0.457228
 149 │ Iris-virginica                    0.457228
 150 │ Iris-virginica                    0.457228
                                  143 rows omitted

julia> transform(gdf, :Species => x -> chop.(x, head=5, tail=0))
150×6 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species         Species_function
     │ Float64      Float64     Float64      Float64     String          SubString…
─────┼────────────────────────────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa     setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa     setosa
   3 │         4.7         3.2          1.3         0.2  Iris-setosa     setosa
   4 │         4.6         3.1          1.5         0.2  Iris-setosa     setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮                ⋮
 148 │         6.5         3.0          5.2         2.0  Iris-virginica  virginica
 149 │         6.2         3.4          5.4         2.3  Iris-virginica  virginica
 150 │         5.9         3.0          5.1         1.8  Iris-virginica  virginica
                                                                          143 rows omitted
```

All functions also support the `do` block form. However, as noted above,
this form is slow and should therefore be avoided when performance matters.

```jldoctest sac
julia> combine(gdf) do df
           (m = mean(df.PetalLength), s² = var(df.PetalLength))
       end
3×3 DataFrame
 Row │ Species          m        s²
     │ String15         Float64  Float64
─────┼─────────────────────────────────────
   1 │ Iris-setosa        1.464  0.0301061
   2 │ Iris-versicolor    4.26   0.220816
   3 │ Iris-virginica     5.552  0.304588
```

If you only want to split the data set into subsets, use the [`groupby`](@ref) function:

```jldoctest sac
julia> for subdf in groupby(iris, :Species)
           println(size(subdf, 1))
       end
50
50
50
```

To also get the values of the grouping columns along with each group, use the
`pairs` function:

```jldoctest sac
julia> for (key, subdf) in pairs(groupby(iris, :Species))
           println("Number of data points for $(key.Species): $(nrow(subdf))")
       end
Number of data points for Iris-setosa: 50
Number of data points for Iris-versicolor: 50
Number of data points for Iris-virginica: 50
```

The value of `key` in the previous example is a [`DataFrames.GroupKey`](@ref) object,
which can be used in a similar fashion to a `NamedTuple`.

Grouping a data frame using the `groupby` function can be seen as adding a lookup key
to it. Such lookups can be performed efficiently by indexing the resulting
`GroupedDataFrame` with a `Tuple` or `NamedTuple`:
```jldoctest sac
julia> df = DataFrame(g=repeat(1:1000, inner=5), x=1:5000)
5000×2 DataFrame
  Row │ g      x
      │ Int64  Int64
──────┼──────────────
    1 │     1      1
    2 │     1      2
    3 │     1      3
    4 │     1      4
    5 │     1      5
    6 │     2      6
    7 │     2      7
    8 │     2      8
  ⋮   │   ⋮      ⋮
 4994 │   999   4994
 4995 │   999   4995
 4996 │  1000   4996
 4997 │  1000   4997
 4998 │  1000   4998
 4999 │  1000   4999
 5000 │  1000   5000
    4985 rows omitted

julia> gdf = groupby(df, :g)
GroupedDataFrame with 1000 groups based on key: g
First Group (5 rows): g = 1
 Row │ g      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     1      2
   3 │     1      3
   4 │     1      4
   5 │     1      5
⋮
Last Group (5 rows): g = 1000
 Row │ g      x
     │ Int64  Int64
─────┼──────────────
   1 │  1000   4996
   2 │  1000   4997
   3 │  1000   4998
   4 │  1000   4999
   5 │  1000   5000

julia> gdf[(g=500,)]
5×2 SubDataFrame
 Row │ g      x
     │ Int64  Int64
─────┼──────────────
   1 │   500   2496
   2 │   500   2497
   3 │   500   2498
   4 │   500   2499
   5 │   500   2500

julia> gdf[[(500,), (501,)]]
GroupedDataFrame with 2 groups based on key: g
First Group (5 rows): g = 500
 Row │ g      x
     │ Int64  Int64
─────┼──────────────
   1 │   500   2496
   2 │   500   2497
   3 │   500   2498
   4 │   500   2499
   5 │   500   2500
⋮
Last Group (5 rows): g = 501
 Row │ g      x
     │ Int64  Int64
─────┼──────────────
   1 │   501   2501
   2 │   501   2502
   3 │   501   2503
   4 │   501   2504
   5 │   501   2505
```

In order to apply a function to each non-grouping column of a `GroupedDataFrame` you can write:
```jldoctest sac
julia> gd = groupby(iris, :Species)
GroupedDataFrame with 3 groups based on key: Species
First Group (50 rows): Species = "Iris-setosa"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼───────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
   3 │         4.7         3.2          1.3         0.2  Iris-setosa
   4 │         4.6         3.1          1.5         0.2  Iris-setosa
   5 │         5.0         3.6          1.4         0.2  Iris-setosa
   6 │         5.4         3.9          1.7         0.4  Iris-setosa
   7 │         4.6         3.4          1.4         0.3  Iris-setosa
   8 │         5.0         3.4          1.5         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮            ⋮
  43 │         4.4         3.2          1.3         0.2  Iris-setosa
  44 │         5.0         3.5          1.6         0.6  Iris-setosa
  45 │         5.1         3.8          1.9         0.4  Iris-setosa
  46 │         4.8         3.0          1.4         0.3  Iris-setosa
  47 │         5.1         3.8          1.6         0.2  Iris-setosa
  48 │         4.6         3.2          1.4         0.2  Iris-setosa
  49 │         5.3         3.7          1.5         0.2  Iris-setosa
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      34 rows omitted
⋮
Last Group (50 rows): Species = "Iris-virginica"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼──────────────────────────────────────────────────────────────────
   1 │         6.3         3.3          6.0         2.5  Iris-virginica
   2 │         5.8         2.7          5.1         1.9  Iris-virginica
   3 │         7.1         3.0          5.9         2.1  Iris-virginica
   4 │         6.3         2.9          5.6         1.8  Iris-virginica
   5 │         6.5         3.0          5.8         2.2  Iris-virginica
   6 │         7.6         3.0          6.6         2.1  Iris-virginica
   7 │         4.9         2.5          4.5         1.7  Iris-virginica
   8 │         7.3         2.9          6.3         1.8  Iris-virginica
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
  43 │         5.8         2.7          5.1         1.9  Iris-virginica
  44 │         6.8         3.2          5.9         2.3  Iris-virginica
  45 │         6.7         3.3          5.7         2.5  Iris-virginica
  46 │         6.7         3.0          5.2         2.3  Iris-virginica
  47 │         6.3         2.5          5.0         1.9  Iris-virginica
  48 │         6.5         3.0          5.2         2.0  Iris-virginica
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         34 rows omitted

julia> combine(gd, valuecols(gd) .=> mean)
3×5 DataFrame
 Row │ Species          SepalLength_mean  SepalWidth_mean  PetalLength_mean  P ⋯
     │ String15         Float64           Float64          Float64           F ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ Iris-setosa                 5.006            3.418             1.464    ⋯
   2 │ Iris-versicolor             5.936            2.77              4.26
   3 │ Iris-virginica              6.588            2.974             5.552
                                                                1 column omitted
```

Note that `GroupedDataFrame` is a view: therefore
grouping columns of its parent data frame must not be mutated, and
rows must not be added nor removed from it. If the number or rows
of the parent changes then an error is thrown when a child `GroupedDataFrame`
is used:
```jldoctest sac
julia> df = DataFrame(id=1:2)
2×1 DataFrame
 Row │ id
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> gd = groupby(df, :id)
GroupedDataFrame with 2 groups based on key: id
First Group (1 row): id = 1
 Row │ id
     │ Int64
─────┼───────
   1 │     1
⋮
Last Group (1 row): id = 2
 Row │ id
     │ Int64
─────┼───────
   1 │     2

julia> push!(df, [3])
3×1 DataFrame
 Row │ id
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> gd[1]
ERROR: AssertionError: The current number of rows in the parent data frame is 3 and it does not match the number of rows it contained when GroupedDataFrame was created which was 2. The number of rows in the parent data frame has likely been changed unintentionally (e.g. using subset!, filter!, deleteat!, push!, or append! functions).
```

Sometimes it is useful to append rows to the source data frame of a
`GroupedDataFrame`, without affecting the rows used for grouping.
In such a scenario you can create the grouped data frame using a `view`
of the parent data frame to avoid the error:

```jldoctest sac
julia> df = DataFrame(id=1:2)
2×1 DataFrame
 Row │ id
     │ Int64
─────┼───────
   1 │     1
   2 │     2

julia> gd = groupby(view(df, :, :), :id)
GroupedDataFrame with 2 groups based on key: id
First Group (1 row): id = 1
 Row │ id
     │ Int64
─────┼───────
   1 │     1
⋮
Last Group (1 row): id = 2
 Row │ id
     │ Int64
─────┼───────
   1 │     2

julia> push!(df, [3])
3×1 DataFrame
 Row │ id
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> gd[1]
1×1 SubDataFrame
 Row │ id
     │ Int64
─────┼───────
   1 │     1
```

# Simulating the SQL `where` clause

You can conveniently work on subsets of a data frame by using `SubDataFrame`s.
Operations performed on such objects can either create a new data frame or be
performed in-place. Here are some examples:

```jldoctest sac
julia> df = DataFrame(a=1:5)
5×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     4
   5 │     5

julia> sdf = @view df[2:3, :]
2×1 SubDataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     2
   2 │     3

julia> transform(sdf, :a => ByRow(string)) # create a new data frame
2×2 DataFrame
 Row │ a      a_string
     │ Int64  String
─────┼─────────────────
   1 │     2  2
   2 │     3  3

julia> transform!(sdf, :a => ByRow(string)) # update the source df in-place
2×2 SubDataFrame
 Row │ a      a_string
     │ Int64  String?
─────┼─────────────────
   1 │     2  2
   2 │     3  3

julia> df # new column was created filled with missing in filtered-out rows
5×2 DataFrame
 Row │ a      a_string
     │ Int64  String?
─────┼─────────────────
   1 │     1  missing
   2 │     2  2
   3 │     3  3
   4 │     4  missing
   5 │     5  missing

julia> select!(sdf, :a => -, renamecols=false) # update the source df in-place
2×1 SubDataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │    -2
   2 │    -3

julia> df # the column replaced an existing column; previously stored values are re-used in filtered-out rows
5×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │    -2
   3 │    -3
   4 │     4
   5 │     5
```

Similar operations can be performed on `GroupedDataFrame` as well:
```jldoctest sac
julia> df = DataFrame(a=[1, 1, 1, 2, 2, 3], b=1:6)
6×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     1      2
   3 │     1      3
   4 │     2      4
   5 │     2      5
   6 │     3      6

julia> sdf = @view df[2:4, :]
3×2 SubDataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     1      3
   3 │     2      4

julia> gsdf = groupby(sdf, :a)
GroupedDataFrame with 2 groups based on key: a
First Group (2 rows): a = 1
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     1      3
⋮
Last Group (1 row): a = 2
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      4

julia> transform(gsdf, nrow) # create a new data frame
3×3 DataFrame
 Row │ a      b      nrow
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      2      2
   2 │     1      3      2
   3 │     2      4      1

julia> transform!(gsdf, nrow, :b => :b_copy)
3×4 SubDataFrame
 Row │ a      b      nrow    b_copy
     │ Int64  Int64  Int64?  Int64?
─────┼──────────────────────────────
   1 │     1      2       2       2
   2 │     1      3       2       3
   3 │     2      4       1       4

julia> df
6×4 DataFrame
 Row │ a      b      nrow     b_copy
     │ Int64  Int64  Int64?   Int64?
─────┼────────────────────────────────
   1 │     1      1  missing  missing
   2 │     1      2        2        2
   3 │     1      3        2        3
   4 │     2      4        1        4
   5 │     2      5  missing  missing
   6 │     3      6  missing  missing

julia> select!(gsdf, :b_copy, :b => sum, renamecols=false)
3×3 SubDataFrame
 Row │ a      b_copy  b
     │ Int64  Int64?  Int64
─────┼──────────────────────
   1 │     1       2      5
   2 │     1       3      5
   3 │     2       4      4

julia> df
6×3 DataFrame
 Row │ a      b_copy   b
     │ Int64  Int64?   Int64
─────┼───────────────────────
   1 │     1  missing      1
   2 │     1        2      5
   3 │     1        3      5
   4 │     2        4      4
   5 │     2  missing      5
   6 │     3  missing      6
```
