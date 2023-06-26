# The Split-Apply-Combine Strategy

## Design of the split-apply-combine support

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
* `combine`: does not put restrictions on number of rows returned per group;
  the returned values are vertically concatenated following order of groups in
  `GroupedDataFrame`; it is typically used to compute summary statistics by group;
  for `GroupedDataFrame` if grouping columns are kept they are put as first columns
  in the result;
* `select`: return a data frame with the number and order of rows exactly the same
  as the source data frame, including only new calculated columns;
  `select!` is an in-place version of `select`;
* `transform`: return a data frame with the number and order of rows exactly the same
  as the source data frame, including all columns from the source and new calculated columns;
  `transform!` is an in-place version of `transform`;
  existing columns in the source data frame are put as first columns in the result;

As a special case, if a `GroupedDataFrame` that has zero groups is passed then
the result of the operation is determined by performing a single call to the
transformation function with a 0-row argument passed to it. The output of this
operation is only used to identify the number and type of produced columns, but
the result has zero rows.

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
5. column-independent operations `function => target_cols` or just `function`
   for specific `function`s where the input columns are omitted;
   without `target_cols` the new column has the same name as `function`, otherwise
   it must be single name (as a `Symbol` or a string). Supported `function`s are:
   * `nrow` to efficiently compute the number of rows in each group.
   * `proprow` to efficiently compute the proportion of rows in each group.
   * `eachindex` to return a vector holding the number of each row within each group.
   * `groupindices` to return the group number.
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
   then returning a data frame, a matrix, a `NamedTuple`, a `Tables.AbstractRow`
   or a `DataFrameRow` will
   produce multiple columns in the result. Returning any other value produces
   a single column.
2. If `target_cols` is a `Symbol` or a string then the function is assumed to return
   a single column. In this case returning a data frame, a matrix, a `NamedTuple`,
   a `Tables.AbstractRow`, or a `DataFrameRow` raises an error.
3. If `target_cols` is a vector of `Symbol`s or strings or `AsTable` it is assumed
   that `function` returns multiple columns.
   If `function` returns one of `AbstractDataFrame`, `NamedTuple`, `DataFrameRow`,
   `Tables.AbstractRow`, `AbstractMatrix` then rules described in point 1 above apply.
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

By default (`threads=true`) a separate task is spawned for each
specified transformation; each transformation then spawns as many tasks
as Julia threads, and splits processing of groups across them
(however, currently transformations with optimized implementations like `sum`
and transformations that return multiple rows use a single task for all groups).
This allows for parallel operation when Julia was started with more than one
thread. Passed transformation functions must therefore not modify global variables
(i.e. they must be pure), use locks to control parallel accesses,
or `threads=false` must be passed to disable multithreading.

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
- `threads` : whether transformations may be run in separate tasks which can execute
  in parallel

## Examples of the split-apply-combine operations

We show several examples of these functions applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

julia> path = joinpath(pkgdir(DataFrames), "docs", "src", "assets", "iris.csv");

julia> iris = CSV.read(path, DataFrame)
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

julia> iris_gdf = groupby(iris, :Species)
GroupedDataFrame with 3 groups based on key: Species
First Group (50 rows): Species = "Iris-setosa"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼───────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮            ⋮
  49 │         5.3         3.7          1.5         0.2  Iris-setosa
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      46 rows omitted
⋮
Last Group (50 rows): Species = "Iris-virginica"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼──────────────────────────────────────────────────────────────────
   1 │         6.3         3.3          6.0         2.5  Iris-virginica
   2 │         5.8         2.7          5.1         1.9  Iris-virginica
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         47 rows omitted

julia> combine(iris_gdf, :PetalLength => mean)
3×2 DataFrame
 Row │ Species          PetalLength_mean
     │ String15         Float64
─────┼───────────────────────────────────
   1 │ Iris-setosa                 1.464
   2 │ Iris-versicolor             4.26
   3 │ Iris-virginica              5.552

julia> combine(iris_gdf, nrow, proprow, groupindices)
3×4 DataFrame
 Row │ Species          nrow   proprow   groupindices
     │ String15         Int64  Float64   Int64
─────┼────────────────────────────────────────────────
   1 │ Iris-setosa         50  0.333333             1
   2 │ Iris-versicolor     50  0.333333             2
   3 │ Iris-virginica      50  0.333333             3

julia> combine(iris_gdf, nrow, :PetalLength => mean => :mean)
3×3 DataFrame
 Row │ Species          nrow   mean
     │ String15         Int64  Float64
─────┼─────────────────────────────────
   1 │ Iris-setosa         50    1.464
   2 │ Iris-versicolor     50    4.26
   3 │ Iris-virginica      50    5.552

julia> combine(iris_gdf,
               [:PetalLength, :SepalLength] =>
               ((p, s) -> (a=mean(p)/mean(s), b=sum(p))) =>
               AsTable) # multiple columns are passed as arguments
3×3 DataFrame
 Row │ Species          a         b
     │ String15         Float64   Float64
─────┼────────────────────────────────────
   1 │ Iris-setosa      0.292449     73.2
   2 │ Iris-versicolor  0.717655    213.0
   3 │ Iris-virginica   0.842744    277.6

julia> combine(iris_gdf,
               AsTable([:PetalLength, :SepalLength]) =>
               x -> std(x.PetalLength) / std(x.SepalLength)) # passing a NamedTuple
3×2 DataFrame
 Row │ Species          PetalLength_SepalLength_function
     │ String15         Float64
─────┼───────────────────────────────────────────────────
   1 │ Iris-setosa                              0.492245
   2 │ Iris-versicolor                          0.910378
   3 │ Iris-virginica                           0.867923

julia> combine(x -> std(x.PetalLength) / std(x.SepalLength), iris_gdf) # passing a SubDataFrame
3×2 DataFrame
 Row │ Species          x1
     │ String15         Float64
─────┼───────────────────────────
   1 │ Iris-setosa      0.492245
   2 │ Iris-versicolor  0.910378
   3 │ Iris-virginica   0.867923

julia> combine(iris_gdf, 1:2 => cor, nrow)
3×3 DataFrame
 Row │ Species          SepalLength_SepalWidth_cor  nrow
     │ String15         Float64                     Int64
─────┼────────────────────────────────────────────────────
   1 │ Iris-setosa                        0.74678      50
   2 │ Iris-versicolor                    0.525911     50
   3 │ Iris-virginica                     0.457228     50

julia> combine(iris_gdf, :PetalLength => (x -> [extrema(x)]) => [:min, :max])
3×3 DataFrame
 Row │ Species          min      max
     │ String15         Float64  Float64
─────┼───────────────────────────────────
   1 │ Iris-setosa          1.0      1.9
   2 │ Iris-versicolor      3.0      5.1
   3 │ Iris-virginica       4.5      6.9
```

To get row number for each observation within each group use the `eachindex` function:
```
julia> combine(iris_gdf, eachindex)
150×2 DataFrame
 Row │ Species         eachindex
     │ String15        Int64
─────┼───────────────────────────
   1 │ Iris-setosa             1
   2 │ Iris-setosa             2
   3 │ Iris-setosa             3
  ⋮  │       ⋮             ⋮
 148 │ Iris-virginica         48
 149 │ Iris-virginica         49
 150 │ Iris-virginica         50
                 144 rows omitted
```

Contrary to `combine`, the `select` and `transform` functions always return
a data frame with the same number and order of rows as the source.
In the example below
the return values in columns `:SepalLength_SepalWidth_cor` and `:nrow` are
broadcasted to match the number of elements in each group:
```
julia> select(iris_gdf, 1:2 => cor)
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

julia> transform(iris_gdf, :Species => x -> chop.(x, head=5, tail=0))
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
julia> combine(iris_gdf) do df
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

To apply a function to each non-grouping column of a `GroupedDataFrame` you can write:

```jldoctest sac
julia> combine(iris_gdf, valuecols(iris_gdf) .=> mean)
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

## Using `GroupedDataFrame` as an iterable and indexable object

If you only want to split the data set into subsets, use the [`groupby`](@ref)
function. You can then iterate `SubDataFrame`s that constitute the identified
groups:

```jldoctest sac
julia> for subdf in iris_gdf
           println(size(subdf, 1))
       end
50
50
50
```

To also get the values of the grouping columns along with each group, use the
`pairs` function:

```jldoctest sac
julia> for (key, subdf) in pairs(iris_gdf)
           println("Number of data points for $(key.Species): $(nrow(subdf))")
       end
Number of data points for Iris-setosa: 50
Number of data points for Iris-versicolor: 50
Number of data points for Iris-virginica: 50
```

The value of `key` in the example above where we iterated `pairs(iris_gdf)` is
a [`DataFrames.GroupKey`](@ref) object, which can be used in a similar fashion
to a `NamedTuple`.

Grouping a data frame using the `groupby` function can be seen as adding a
lookup key to it. Such lookups can be performed efficiently by indexing the
resulting `GroupedDataFrame` with [`DataFrames.GroupKey`](@ref) (as it was
presented above) a `Tuple`, a `NamedTuple`, or a dictionary. Here are some
more examples of such indexing.

```jldoctest sac
julia> iris_gdf[(Species="Iris-virginica",)]  # a NamedTuple
50×5 SubDataFrame
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
  44 │         6.8         3.2          5.9         2.3  Iris-virginica
  45 │         6.7         3.3          5.7         2.5  Iris-virginica
  46 │         6.7         3.0          5.2         2.3  Iris-virginica
  47 │         6.3         2.5          5.0         1.9  Iris-virginica
  48 │         6.5         3.0          5.2         2.0  Iris-virginica
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         35 rows omitted

julia> iris_gdf[[("Iris-virginica",), ("Iris-setosa",)]] # a vector of Tuples
GroupedDataFrame with 2 groups based on key: Species
First Group (50 rows): Species = "Iris-virginica"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼──────────────────────────────────────────────────────────────────
   1 │         6.3         3.3          6.0         2.5  Iris-virginica
   2 │         5.8         2.7          5.1         1.9  Iris-virginica
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         46 rows omitted
⋮
Last Group (50 rows): Species = "Iris-setosa"
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼───────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮            ⋮
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      47 rows omitted

julia> key = keys(iris_gdf) |> last # last key in iris_gdf
GroupKey: (Species = String15("Iris-virginica"),)

julia> iris_gdf[key]
50×5 SubDataFrame
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
  44 │         6.8         3.2          5.9         2.3  Iris-virginica
  45 │         6.7         3.3          5.7         2.5  Iris-virginica
  46 │         6.7         3.0          5.2         2.3  Iris-virginica
  47 │         6.3         2.5          5.0         1.9  Iris-virginica
  48 │         6.5         3.0          5.2         2.0  Iris-virginica
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         35 rows omitted

julia> iris_gdf[Dict("Species" => "Iris-setosa")] # a dictionary
50×5 SubDataFrame
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
  44 │         5.0         3.5          1.6         0.6  Iris-setosa
  45 │         5.1         3.8          1.9         0.4  Iris-setosa
  46 │         4.8         3.0          1.4         0.3  Iris-setosa
  47 │         5.1         3.8          1.6         0.2  Iris-setosa
  48 │         4.6         3.2          1.4         0.2  Iris-setosa
  49 │         5.3         3.7          1.5         0.2  Iris-setosa
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      35 rows omitted
```

Note that although `GroupedDataFrame` is iterable and indexable it is not an
`AbstractVector`. For this reason currently it was decided that it does not
support `map` nor broadcasting (to allow for making a decision in the future
what result type they should produce). To apply a function to all groups of a
data frame and get a vector of results either use a comprehension or `collect`
`GroupedDataFrame` into a vector first. Here are examples of both approaches:

```jldoctest sac
julia> sdf_vec = collect(iris_gdf)
3-element Vector{Any}:
 50×5 SubDataFrame
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
  44 │         5.0         3.5          1.6         0.6  Iris-setosa
  45 │         5.1         3.8          1.9         0.4  Iris-setosa
  46 │         4.8         3.0          1.4         0.3  Iris-setosa
  47 │         5.1         3.8          1.6         0.2  Iris-setosa
  48 │         4.6         3.2          1.4         0.2  Iris-setosa
  49 │         5.3         3.7          1.5         0.2  Iris-setosa
  50 │         5.0         3.3          1.4         0.2  Iris-setosa
                                                      35 rows omitted
 50×5 SubDataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     String15
─────┼───────────────────────────────────────────────────────────────────
   1 │         7.0         3.2          4.7         1.4  Iris-versicolor
   2 │         6.4         3.2          4.5         1.5  Iris-versicolor
   3 │         6.9         3.1          4.9         1.5  Iris-versicolor
   4 │         5.5         2.3          4.0         1.3  Iris-versicolor
   5 │         6.5         2.8          4.6         1.5  Iris-versicolor
   6 │         5.7         2.8          4.5         1.3  Iris-versicolor
   7 │         6.3         3.3          4.7         1.6  Iris-versicolor
   8 │         4.9         2.4          3.3         1.0  Iris-versicolor
  ⋮  │      ⋮           ⋮            ⋮           ⋮              ⋮
  44 │         5.0         2.3          3.3         1.0  Iris-versicolor
  45 │         5.6         2.7          4.2         1.3  Iris-versicolor
  46 │         5.7         3.0          4.2         1.2  Iris-versicolor
  47 │         5.7         2.9          4.2         1.3  Iris-versicolor
  48 │         6.2         2.9          4.3         1.3  Iris-versicolor
  49 │         5.1         2.5          3.0         1.1  Iris-versicolor
  50 │         5.7         2.8          4.1         1.3  Iris-versicolor
                                                          35 rows omitted
 50×5 SubDataFrame
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
  44 │         6.8         3.2          5.9         2.3  Iris-virginica
  45 │         6.7         3.3          5.7         2.5  Iris-virginica
  46 │         6.7         3.0          5.2         2.3  Iris-virginica
  47 │         6.3         2.5          5.0         1.9  Iris-virginica
  48 │         6.5         3.0          5.2         2.0  Iris-virginica
  49 │         6.2         3.4          5.4         2.3  Iris-virginica
  50 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                         35 rows omitted

julia> map(nrow, sdf_vec)
3-element Vector{Int64}:
 50
 50
 50

julia> nrow.(sdf_vec)
3-element Vector{Int64}:
 50
 50
 50
```

Since `GroupedDataFrame` is iterable, you can achieve the same result with a
comprehension:

```jldoctest sac
julia> [nrow(sdf) for sdf in iris_gdf]
3-element Vector{Int64}:
 50
 50
 50
```

Note that using the split-apply-combine strategy with the operation specification
syntax in `combine`, `select` or `transform` will usually be faster for large
`GroupedDataFrame` objects than iterating them, with the difference that they
produce a data frame. An operation corresponding to the example above is:

```
julia> combine(iris_gdf, nrow)
3×2 DataFrame
 Row │ Species          nrow
     │ String15         Int64
─────┼────────────────────────
   1 │ Iris-setosa         50
   2 │ Iris-versicolor     50
   3 │ Iris-virginica      50
```

## Simulating the SQL `where` clause

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

## Column-independent operations

The operation specification language used with `combine`, `select` and `transform`
supports the following column-independent operations:

* getting the number of rows in a group (`nrow`);
* getting the proportion of rows in a group (`proprow`);
* getting the group number (`groupindices`);
* getting a vector of indices within groups (`eachindex`).

These operations are column-independent, because they do not require specifying the input column
name in the operation specification syntax.

These four exceptions to the standard operation specification syntax were
introduced for user convenience as these operations are often needed in
practice.

Below each of them is explained by example.

First create a data frame we will work with:

```jldoctest sac
julia> df = DataFrame(customer_id=["a", "b", "b", "b", "c", "c"],
                      transaction_id=[12, 15, 19, 17, 13, 11],
                      volume=[2, 3, 1, 4, 5, 9])
6×3 DataFrame
 Row │ customer_id  transaction_id  volume
     │ String       Int64           Int64
─────┼─────────────────────────────────────
   1 │ a                        12       2
   2 │ b                        15       3
   3 │ b                        19       1
   4 │ b                        17       4
   5 │ c                        13       5
   6 │ c                        11       9

julia> gdf = groupby(df, :customer_id, sort=true);

julia> show(gdf, allgroups=true)
GroupedDataFrame with 3 groups based on key: customer_id
Group 1 (1 row): customer_id = "a"
 Row │ customer_id  transaction_id  volume
     │ String       Int64           Int64
─────┼─────────────────────────────────────
   1 │ a                        12       2
Group 2 (3 rows): customer_id = "b"
 Row │ customer_id  transaction_id  volume
     │ String       Int64           Int64
─────┼─────────────────────────────────────
   1 │ b                        15       3
   2 │ b                        19       1
   3 │ b                        17       4
Group 3 (2 rows): customer_id = "c"
 Row │ customer_id  transaction_id  volume
     │ String       Int64           Int64
─────┼─────────────────────────────────────
   1 │ c                        13       5
   2 │ c                        11       9
```

### Getting the number of rows

You can get the number of rows per group in a `GroupedDataFrame` by just
writing `nrow`, in which case the generated column name with the number of rows
is `:nrow`:

```jldoctest sac
julia> combine(gdf, nrow)
3×2 DataFrame
 Row │ customer_id  nrow
     │ String       Int64
─────┼────────────────────
   1 │ a                1
   2 │ b                3
   3 │ c                2
```

Additionally you are allowed to pass target column name:

```jldoctest sac
julia> combine(gdf, nrow => "transaction_count")
3×2 DataFrame
 Row │ customer_id  transaction_count
     │ String       Int64
─────┼────────────────────────────────
   1 │ a                            1
   2 │ b                            3
   3 │ c                            2
```

Note that in both cases we did not pass source column name as it is not needed
to determine the number of rows per group. This is the reason why column-independent
operations are exceptions to standard operation specification syntax.

The `nrow` expression also works in the operation specification syntax
applied to a data frame. Here is an example:

```jldoctest sac
julia> combine(df, nrow => "transaction_count")
1×1 DataFrame
 Row │ transaction_count
     │ Int64
─────┼───────────────────
   1 │                 6
```

Finally, recall that [`nrow`](@ref) is also a regular function that returns a
number of rows in a data frame:


```jldoctest sac
julia> nrow(df)
6
```

This dual use of `nrow` does not lead to ambiguities, and is meant to make it
easier to remember this exception.

### Getting the proportion of rows

If you want to get a proportion of rows per group in a `GroupedDataFrame`
you can use the `proprow` and `proprow => [target column name]` column-independent
operations. Here are some examples:

```jldoctest sac
julia> combine(gdf, proprow)
3×2 DataFrame
 Row │ customer_id  proprow
     │ String       Float64
─────┼───────────────────────
   1 │ a            0.166667
   2 │ b            0.5
   3 │ c            0.333333

julia> combine(gdf, proprow => "transaction_fraction")
3×2 DataFrame
 Row │ customer_id  transaction_fraction
     │ String       Float64
─────┼───────────────────────────────────
   1 │ a                        0.166667
   2 │ b                        0.5
   3 │ c                        0.333333
```

As opposed to `nrow`, `proprow` cannot be used outside of the operation
specification syntax and is only allowed when processing a `GroupedDataFrame`.

### Getting the group number

Another common operation is getting group number. Use the `groupindices` and
`groupindices => [target column name]` column-independent operations to get it:


```jldoctest sac
julia> combine(gdf, groupindices)
3×2 DataFrame
 Row │ customer_id  groupindices
     │ String       Int64
─────┼───────────────────────────
   1 │ a                       1
   2 │ b                       2
   3 │ c                       3

julia> transform(gdf, groupindices)
6×4 DataFrame
 Row │ customer_id  transaction_id  volume  groupindices
     │ String       Int64           Int64   Int64
─────┼───────────────────────────────────────────────────
   1 │ a                        12       2             1
   2 │ b                        15       3             2
   3 │ b                        19       1             2
   4 │ b                        17       4             2
   5 │ c                        13       5             3
   6 │ c                        11       9             3

julia> combine(gdf, groupindices => "group_number")
3×2 DataFrame
 Row │ customer_id  group_number
     │ String       Int64
─────┼───────────────────────────
   1 │ a                       1
   2 │ b                       2
   3 │ c                       3
```

Outside of the operation specification syntax, [`groupindices`](@ref)
is also a regular function which returns group indices for each row
in the parent data frame of the passed `GroupedDataFrame`:

```jldoctest sac
julia> groupindices(gdf)
6-element Vector{Union{Missing, Int64}}:
 1
 2
 2
 2
 3
 3
```

### Getting a vector of indices within groups

The last column-independent operation supported by the operation
specification syntax is getting the index of each row within each group:


```jldoctest sac
julia> combine(gdf, eachindex)
6×2 DataFrame
 Row │ customer_id  eachindex
     │ String       Int64
─────┼────────────────────────
   1 │ a                    1
   2 │ b                    1
   3 │ b                    2
   4 │ b                    3
   5 │ c                    1
   6 │ c                    2

julia> select(gdf, eachindex, groupindices)
6×3 DataFrame
 Row │ customer_id  eachindex  groupindices
     │ String       Int64      Int64
─────┼──────────────────────────────────────
   1 │ a                    1             1
   2 │ b                    1             2
   3 │ b                    2             2
   4 │ b                    3             2
   5 │ c                    1             3
   6 │ c                    2             3

julia> combine(gdf, eachindex => "transaction_number")
6×2 DataFrame
 Row │ customer_id  transaction_number
     │ String       Int64
─────┼─────────────────────────────────
   1 │ a                             1
   2 │ b                             1
   3 │ b                             2
   4 │ b                             3
   5 │ c                             1
   6 │ c                             2
```

Note that this operation also makes sense in a data frame context,
where all rows are considered to be in the same group:

```jldoctest sac
julia> transform(df, eachindex)
6×4 DataFrame
 Row │ customer_id  transaction_id  volume  eachindex
     │ String       Int64           Int64   Int64
─────┼────────────────────────────────────────────────
   1 │ a                        12       2          1
   2 │ b                        15       3          2
   3 │ b                        19       1          3
   4 │ b                        17       4          4
   5 │ c                        13       5          5
   6 │ c                        11       9          6
```

Finally recall that `eachindex` is a standard function for getting all indices
in an array. This similarity of functionality was the reason why this name was
picked:

```jldoctest sac
julia> collect(eachindex(df.customer_id))
6-element Vector{Int64}:
 1
 2
 3
 4
 5
 6
```

This, for example, means that in the following example the two created columns
have the same contents:

```jldoctest sac
julia> combine(gdf, eachindex, :customer_id => eachindex)
6×3 DataFrame
 Row │ customer_id  eachindex  customer_id_eachindex
     │ String       Int64      Int64
─────┼───────────────────────────────────────────────
   1 │ a                    1                      1
   2 │ b                    1                      1
   3 │ b                    2                      2
   4 │ b                    3                      3
   5 │ c                    1                      1
   6 │ c                    2                      2
```


## Column-independent operations versus functions

When discussing column-independent operations it is important to remember
that operation specification syntax allows you to pass a function (without
source and target column names), in which case such a function gets passed a
`SubDataFrame` that represents a group in a `GroupedDataFrame`. Here is an
example comparing a column-independent operation and a function:

```jldoctest sac
julia> combine(gdf, eachindex, sdf -> axes(sdf, 1))
6×3 DataFrame
 Row │ customer_id  eachindex  x1
     │ String       Int64      Int64
─────┼───────────────────────────────
   1 │ a                    1      1
   2 │ b                    1      1
   3 │ b                    2      2
   4 │ b                    3      3
   5 │ c                    1      1
   6 │ c                    2      2
```

Notice that the column-independent operation `eachindex` produces the same result
as using the anonymous function `sdf -> axes(sdf, 1)` that takes a `SubDataFrame`
as its first argument and returns indices along its first axes.
Importantly if it wasn't defined as a column-independent operation
the `eachindex` function would fail when being passed as you can see here:

```jldoctest sac
julia> combine(gdf, sdf -> eachindex(sdf))
ERROR: MethodError: no method matching keys(::SubDataFrame{DataFrame, DataFrames.Index, Vector{Int64}})
```

The reason for this error is that the `eachindex` function does not allow passing a
`SubDataFrame` as its argument.

The same applies to `proprow` and `groupindices`: they would not work
with a `SubDataFrame` as stand-alone functions.

The `nrow` column-independent operation is a different case, as
the `nrow` function accepts `SubDataFrame` as an argument:

```jldoctest sac
julia> combine(gdf, nrow, sdf -> nrow(sdf))
3×3 DataFrame
 Row │ customer_id  nrow   x1
     │ String       Int64  Int64
─────┼───────────────────────────
   1 │ a                1      1
   2 │ b                3      3
   3 │ c                2      2
```

Notice that columns `:nrow` and `:x1` have identical contents, but the
difference is that they do not have the same names. `nrow` is a
column-independent operation generating the `:nrow` column name by default with
number of rows per group. On the other hand, the `sdf -> nrow(sdf)` anonymous
function does gets a `SubDataFrame` as its argument and returns its number of
rows. The `:x1` column name is the default auto-generated column name when
processing anonymous functions.

Passing a function taking a `SubDataFrame` is a flexible functionality allowing
you to perform complex operations on your data. However, you should bear in mind
two aspects:

* Using the full operation specification syntax (where source and target column
  names are passed) or column-independent operations will lead to faster
  execution of your code (as the Julia compiler is able to better optimize
  execution of such operations) in comparison to passing a function
  taking a `SubDataFrame`.
* Although writing `nrow`, `proprow`, `groupindices`, and `eachindex` looks
  like just passing a function they internally **do not** take a `SubDataFrame`
  as their argument. As we explained in this section, `proprow`,
  `groupindices`, and `eachindex` would not work with `SubDataFrame` as their
  argument, and `nrow` would work, but would produce a different column name.
  Instead, these four operations are special column-independent operations that
  are exceptions to the standard operation specification syntax rules. They
  were added for user convenience.

## Specifying group order in `groupby`

By default order of groups produced by `groupby` is undefined.
If you want the order of groups to follow the order of first appearance in
the source data frame of a grouping key then pass the `sort=false` keyword argument
to `groupby`:

```jldoctest sac
julia> push!(df, ["a", 100, 100]) # push row with large integer values to disable default sorting
7×3 DataFrame
 Row │ customer_id  transaction_id  volume
     │ String       Int64           Int64
─────┼─────────────────────────────────────
   1 │ a                        12       2
   2 │ b                        15       3
   3 │ b                        19       1
   4 │ b                        17       4
   5 │ c                        13       5
   6 │ c                        11       9
   7 │ a                       100     100

julia> keys(groupby(df, :volume))
7-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (volume = 2,)
 GroupKey: (volume = 3,)
 GroupKey: (volume = 1,)
 GroupKey: (volume = 4,)
 GroupKey: (volume = 5,)
 GroupKey: (volume = 9,)
 GroupKey: (volume = 100,)
```

If you want to have them sorted in ascending order pass `sort=true`:

```
julia> keys(groupby(df, :volume, sort=true))
7-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (volume = 1,)
 GroupKey: (volume = 2,)
 GroupKey: (volume = 3,)
 GroupKey: (volume = 4,)
 GroupKey: (volume = 5,)
 GroupKey: (volume = 9,)
 GroupKey: (volume = 100,)
```

You can also use the [`order`](@ref) wrapper when passing a column name to group
by or pass a named tuple as `sort` keyword argument containing one or more of
`alg`, `lt`, `by`, `rev`, and `order` fields that will be treated just like in
[`sortperm`](@ref):

```
julia> keys(groupby(df, [:customer_id, order(:volume, rev=true)]))
6-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (customer_id = "a", volume = 2)
 GroupKey: (customer_id = "b", volume = 4)
 GroupKey: (customer_id = "b", volume = 3)
 GroupKey: (customer_id = "b", volume = 1)
 GroupKey: (customer_id = "c", volume = 9)
 GroupKey: (customer_id = "c", volume = 5)

julia> keys(groupby(df, :customer_id, sort=(rev=true,)))
3-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (customer_id = "c",)
 GroupKey: (customer_id = "b",)
 GroupKey: (customer_id = "a",)
```

