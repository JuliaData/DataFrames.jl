# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying some
functions to each of the groups and then combining the results. A standardized
framework for handling this sort of computation is described in the paper
"[The Split-Apply-Combine Strategy for Data Analysis](http://www.jstatsoft.org/v40/i01)",
written by Hadley Wickham.

The DataFrames package supports the split-apply-combine strategy through the
`groupby` function followed by `combine`, `select`/`select!` or `transform`/`transform!`.

In order to perform operations by groups you first need to create a `GroupedDataFrame`
object from your data frame using the `groupby` function that takes two arguments:
(1) a data frame to be grouped, and (2) a set of columns to group by.

!!! note

    All operations described for `GroupedDataFrame` in this section of the manual
    are also supported for `AbstractDataFrame` in which case it is considered as
    being grouped on no columns (meaning it has a single group, or zero groups if it is empty).
    The only difference is that in this case the `keepkeys` and `ungroup` keyword
    arguments are not supported and a data frame is always returned.

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
1. standard column selectors (integers, `Symbol`s, vectors of integers, vectors of
   `Symbol`s, vectors of strings, `:`, `All`, `Between`, `Not` and regular expressions).
2. a `cols => function` pair indicating that `function` should be called with
   positional arguments holding columns `cols`, which can be a any valid column selector;
   in this case target column name is automatically generated and it is assumed that
   `function` returns a single value or a vector; the generated name is created by
   concatenating source column name and `function` name by default (see examples below).
3. a `cols => function => target_cols` form additionally explicitly specifying
   the target column or columns.
4. a `col => target_cols` pair, which renames the column `col` to `target_cols` which
   must be single column (a `Symbol` or a string).
5. a `nrow` or `nrow => target_cols` form which efficiently computes the number of rows
   in a group; without `target_cols` the new column is called `:nrow`, otherwise
   it must be single column (a `Symbol` or a string).
6. vectors or matrices containing transformations specified by the `Pair` syntax
   described in points 2 to 5
8. a function which will be called with a `SubDataFrame` corresponding to each group;
   this form should be avoided due to its poor performance unless a very large
   number of columns are processed (in which case `SubDataFrame` avoids excessive
   compilation)

All functions have two types of signatures. One of them takes a `GroupedDataFrame`
as a first argument and an arbitrary number of transfomations described above
as following arguments. The second type of signature is when `Function` or `Type`
is passed as a first argument and `GroupedDataFrame` is the second argument
(similar to how it is passed to `map`).

As a special rule, with the `cols => function` and `cols => function =>
target_cols` syntaxes, if `cols` is wrapped in an `AsTable`
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

`select`/`select!` and `transform`/`transform!` always return a `DataFrame`
with the same number and order of rows as the source (even if `GroupedDataFrame`
had its groups reordered).

For `combine`, rows in the returned object appear in the order of
groups in the `GroupedDataFrame`. The functions can return an arbitrary number
of rows for each group, but the kind of returned object and the number
and names of columns must be the same for all groups.

It is allowed to mix single values and vectors if multiple transformations
are requested. In this case single value will be repeated to match the length
of columns specified by returned vectors.

To apply `function` to each row instead of whole columns, it can be wrapped in a
`ByRow` struct. `cols` can be any column indexing syntax, in which case
`function` will be passed one argument for each of the columns specified by
`cols` or a `NamedTuple` of them if specified columns are wrapped in `AsTable`.
If `ByRow` is used it is allowed for `cols` to select an empty set of columns,
in which case `function` is called for each row without any arguments and an
empty `NamedTuple` is passed if empty set of columns is wrapped in `AsTable`.

There the following keyword arguments are supported by the transformation functions
(not all keyword arguments are supported in all cases; in general they are allowed
in situations when they are meaningful, see the documentation of the specific functions
for details):
- `keepkeys` : whether grouping columns should be kept in the returned data frame.
- `ungroup` : whether the return value of the operation should be a data frame or a
  `GroupedDataFrame`.
- `copycols` : whether columns of the source data frame should be copied if no transformation
  is applied to them.
- `renamecols` : whether in the `cols => function` form automatically generated column names
  should include the name of transformation functions or not.

We show several examples of these functions applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

julia> iris = DataFrame(CSV.File(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv")))
150×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String         │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ Iris-setosa    │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa    │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa    │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ Iris-setosa    │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ Iris-setosa    │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa    │
│ 7   │ 4.6         │ 3.4        │ 1.4         │ 0.3        │ Iris-setosa    │
⋮
│ 143 │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ Iris-virginica │
│ 144 │ 6.8         │ 3.2        │ 5.9         │ 2.3        │ Iris-virginica │
│ 145 │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ Iris-virginica │
│ 146 │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ Iris-virginica │
│ 147 │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ Iris-virginica │
│ 148 │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ Iris-virginica │
│ 149 │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │
│ 150 │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ Iris-virginica │

julia> gdf = groupby(iris, :Species)
GroupedDataFrame with 3 groups based on key: Species
First Group (50 rows): Species = "Iris-setosa"
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String      │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ Iris-setosa │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ Iris-setosa │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ Iris-setosa │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa │
│ 7   │ 4.6         │ 3.4        │ 1.4         │ 0.3        │ Iris-setosa │
⋮
│ 43  │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │
│ 44  │ 5.0         │ 3.5        │ 1.6         │ 0.6        │ Iris-setosa │
│ 45  │ 5.1         │ 3.8        │ 1.9         │ 0.4        │ Iris-setosa │
│ 46  │ 4.8         │ 3.0        │ 1.4         │ 0.3        │ Iris-setosa │
│ 47  │ 5.1         │ 3.8        │ 1.6         │ 0.2        │ Iris-setosa │
│ 48  │ 4.6         │ 3.2        │ 1.4         │ 0.2        │ Iris-setosa │
│ 49  │ 5.3         │ 3.7        │ 1.5         │ 0.2        │ Iris-setosa │
│ 50  │ 5.0         │ 3.3        │ 1.4         │ 0.2        │ Iris-setosa │
⋮
Last Group (50 rows): Species = "Iris-virginica"
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String         │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 6.3         │ 3.3        │ 6.0         │ 2.5        │ Iris-virginica │
│ 2   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ Iris-virginica │
│ 3   │ 7.1         │ 3.0        │ 5.9         │ 2.1        │ Iris-virginica │
│ 4   │ 6.3         │ 2.9        │ 5.6         │ 1.8        │ Iris-virginica │
│ 5   │ 6.5         │ 3.0        │ 5.8         │ 2.2        │ Iris-virginica │
│ 6   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ Iris-virginica │
│ 7   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ Iris-virginica │
⋮
│ 43  │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ Iris-virginica │
│ 44  │ 6.8         │ 3.2        │ 5.9         │ 2.3        │ Iris-virginica │
│ 45  │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ Iris-virginica │
│ 46  │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ Iris-virginica │
│ 47  │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ Iris-virginica │
│ 48  │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ Iris-virginica │
│ 49  │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │
│ 50  │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ Iris-virginica │

julia> combine(gdf, :PetalLength => mean)
3×2 DataFrame
│ Row │ Species         │ PetalLength_mean │
│     │ String          │ Float64          │
├─────┼─────────────────┼──────────────────┤
│ 1   │ Iris-setosa     │ 1.464            │
│ 2   │ Iris-versicolor │ 4.26             │
│ 3   │ Iris-virginica  │ 5.552            │

julia> combine(gdf, nrow)
3×2 DataFrame
│ Row │ Species         │ nrow  │
│     │ String          │ Int64 │
├─────┼─────────────────┼───────┤
│ 1   │ Iris-setosa     │ 50    │
│ 2   │ Iris-versicolor │ 50    │
│ 3   │ Iris-virginica  │ 50    │

julia> combine(gdf, nrow, :PetalLength => mean => :mean)
3×3 DataFrame
│ Row │ Species         │ nrow  │ mean    │
│     │ String          │ Int64 │ Float64 │
├─────┼─────────────────┼───────┼─────────┤
│ 1   │ Iris-setosa     │ 50    │ 1.464   │
│ 2   │ Iris-versicolor │ 50    │ 4.26    │
│ 3   │ Iris-virginica  │ 50    │ 5.552   │

julia> combine(gdf, [:PetalLength, :SepalLength] => ((p, s) -> (a=mean(p)/mean(s), b=sum(p))) =>
               AsTable) # multiple columns are passed as arguments
3×3 DataFrame
│ Row │ Species         │ a        │ b       │
│     │ String          │ Float64  │ Float64 │
├─────┼─────────────────┼──────────┼─────────┤
│ 1   │ Iris-setosa     │ 0.292449 │ 73.2    │
│ 2   │ Iris-versicolor │ 0.717655 │ 213.0   │
│ 3   │ Iris-virginica  │ 0.842744 │ 277.6   │

julia> combine(gdf,
               AsTable([:PetalLength, :SepalLength]) =>
               x -> std(x.PetalLength) / std(x.SepalLength)) # passing a NamedTuple
3×2 DataFrame
│ Row │ Species         │ PetalLength_SepalLength_function │
│     │ String          │ Float64                          │
├─────┼─────────────────┼──────────────────────────────────┤
│ 1   │ Iris-setosa     │ 0.492245                         │
│ 2   │ Iris-versicolor │ 0.910378                         │
│ 3   │ Iris-virginica  │ 0.867923                         │

julia> combine(x -> std(x.PetalLength) / std(x.SepalLength), gdf) # passing a SubDataFrame
3×2 DataFrame
│ Row │ Species         │ PetalLength_SepalLength_function │
│     │ String          │ Float64                          │
├─────┼─────────────────┼──────────────────────────────────┤
│ 1   │ Iris-setosa     │ 0.492245                         │
│ 2   │ Iris-versicolor │ 0.910378                         │
│ 3   │ Iris-virginica  │ 0.867923                         │

julia> combine(gdf, 1:2 => cor, nrow)
3×3 DataFrame
│ Row │ Species         │ SepalLength_SepalWidth_cor │ nrow  │
│     │ String          │ Float64                    │ Int64 │
├─────┼─────────────────┼────────────────────────────┼───────┤
│ 1   │ Iris-setosa     │ 0.74678                    │ 50    │
│ 2   │ Iris-versicolor │ 0.525911                   │ 50    │
│ 3   │ Iris-virginica  │ 0.457228                   │ 50    │

julia> combine(gdf, :PetalLength => (x -> [extrema(x)]) => [:min, :max])
3×3 DataFrame
│ Row │ Species         │ min     │ max     │
│     │ String          │ Float64 │ Float64 │
├─────┼─────────────────┼─────────┼─────────┤
│ 1   │ Iris-setosa     │ 1.0     │ 1.9     │
│ 2   │ Iris-versicolor │ 3.0     │ 5.1     │
│ 3   │ Iris-virginica  │ 4.5     │ 6.9     │
```

Contrary to `combine`, the `select` and `transform` functions always return
a data frame with the same number and order of rows as the source.
In the example below
the return values in columns `:SepalLength_SepalWidth_cor` and `:nrow` are
broadcasted to match the number of elements in each group:
```
julia> select(gdf, 1:2 => cor)
150×2 DataFrame
│ Row │ Species        │ SepalLength_SepalWidth_cor │
│     │ String         │ Float64                    │
├─────┼────────────────┼────────────────────────────┤
│ 1   │ Iris-setosa    │ 0.74678                    │
│ 2   │ Iris-setosa    │ 0.74678                    │
│ 3   │ Iris-setosa    │ 0.74678                    │
│ 4   │ Iris-setosa    │ 0.74678                    │
│ 5   │ Iris-setosa    │ 0.74678                    │
│ 6   │ Iris-setosa    │ 0.74678                    │
│ 7   │ Iris-setosa    │ 0.74678                    │
⋮
│ 143 │ Iris-virginica │ 0.457228                   │
│ 144 │ Iris-virginica │ 0.457228                   │
│ 145 │ Iris-virginica │ 0.457228                   │
│ 146 │ Iris-virginica │ 0.457228                   │
│ 147 │ Iris-virginica │ 0.457228                   │
│ 148 │ Iris-virginica │ 0.457228                   │
│ 149 │ Iris-virginica │ 0.457228                   │
│ 150 │ Iris-virginica │ 0.457228                   │

julia> transform(gdf, :Species => x -> chop.(x, head=5, tail=0))
150×6 DataFrame
│ Row │ Species        │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species_function │
│     │ String         │ Float64     │ Float64    │ Float64     │ Float64    │ SubString…       │
├─────┼────────────────┼─────────────┼────────────┼─────────────┼────────────┼──────────────────┤
│ 1   │ Iris-setosa    │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa           │
│ 2   │ Iris-setosa    │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa           │
│ 3   │ Iris-setosa    │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa           │
│ 4   │ Iris-setosa    │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa           │
│ 5   │ Iris-setosa    │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa           │
│ 6   │ Iris-setosa    │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa           │
│ 7   │ Iris-setosa    │ 4.6         │ 3.4        │ 1.4         │ 0.3        │ setosa           │
⋮
│ 143 │ Iris-virginica │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica        │
│ 144 │ Iris-virginica │ 6.8         │ 3.2        │ 5.9         │ 2.3        │ virginica        │
│ 145 │ Iris-virginica │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica        │
│ 146 │ Iris-virginica │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica        │
│ 147 │ Iris-virginica │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica        │
│ 148 │ Iris-virginica │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica        │
│ 149 │ Iris-virginica │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica        │
│ 150 │ Iris-virginica │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica        │
```

All functions also support the `do` block form. However, as noted above,
this form is slow and should therefore be avoided when performance matters.

```jldoctest sac
julia> combine(gdf) do df
           (m = mean(df.PetalLength), s² = var(df.PetalLength))
       end
3×3 DataFrame
│ Row │ Species         │ m       │ s²        │
│     │ String          │ Float64 │ Float64   │
├─────┼─────────────────┼─────────┼───────────┤
│ 1   │ Iris-setosa     │ 1.464   │ 0.0301061 │
│ 2   │ Iris-versicolor │ 4.26    │ 0.220816  │
│ 3   │ Iris-virginica  │ 5.552   │ 0.304588  │
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
```
julia> df = DataFrame(g = repeat(1:1000, inner=5), x = 1:5000);

julia> gdf = groupby(df, :g)
GroupedDataFrame with 1000 groups based on key: g
First Group (5 rows): g = 1
│ Row │ g     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 1     │ 2     │
│ 3   │ 1     │ 3     │
│ 4   │ 1     │ 4     │
│ 5   │ 1     │ 5     │
⋮
Last Group (5 rows): g = 1000
│ Row │ g     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1000  │ 4996  │
│ 2   │ 1000  │ 4997  │
│ 3   │ 1000  │ 4998  │
│ 4   │ 1000  │ 4999  │
│ 5   │ 1000  │ 5000  │

julia> gdf[(g=500,)]
5×2 SubDataFrame
│ Row │ g     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 500   │ 2496  │
│ 2   │ 500   │ 2497  │
│ 3   │ 500   │ 2498  │
│ 4   │ 500   │ 2499  │
│ 5   │ 500   │ 2500  │

julia> gdf[[(500,), (501,)]]
GroupedDataFrame with 2 groups based on key: g
First Group (5 rows): g = 500
│ Row │ g     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 500   │ 2496  │
│ 2   │ 500   │ 2497  │
│ 3   │ 500   │ 2498  │
│ 4   │ 500   │ 2499  │
│ 5   │ 500   │ 2500  │
⋮
Last Group (5 rows): g = 501
│ Row │ g     │ x     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 501   │ 2501  │
│ 2   │ 501   │ 2502  │
│ 3   │ 501   │ 2503  │
│ 4   │ 501   │ 2504  │
│ 5   │ 501   │ 2505  │
```

In order to apply a function to each non-grouping column of a `GroupedDataFrame` you can write:
```jldoctest sac
julia> gd = groupby(iris, :Species);

julia> combine(gd, valuecols(gd) .=> mean)
3×5 DataFrame
│ Row │ Species         │ SepalLength_mean │ SepalWidth_mean │ PetalLength_mean │ PetalWidth_mean │
│     │ String          │ Float64          │ Float64         │ Float64          │ Float64         │
├─────┼─────────────────┼──────────────────┼─────────────────┼──────────────────┼─────────────────┤
│ 1   │ Iris-setosa     │ 5.006            │ 3.418           │ 1.464            │ 0.244           │
│ 2   │ Iris-versicolor │ 5.936            │ 2.77            │ 4.26             │ 1.326           │
│ 3   │ Iris-virginica  │ 6.588            │ 2.974           │ 5.552            │ 2.026           │

julia> combine(gd, valuecols(gd) .=> (x -> (x .- mean(x)) ./ std(x)), renamecols=false)
150×5 DataFrame
│ Row │ Species        │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ String         │ Float64     │ Float64    │ Float64     │ Float64    │
├─────┼────────────────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ Iris-setosa    │ 0.266674    │ 0.215209   │ -0.368852   │ -0.410411  │
│ 2   │ Iris-setosa    │ -0.300718   │ -1.09704   │ -0.368852   │ -0.410411  │
│ 3   │ Iris-setosa    │ -0.868111   │ -0.572142  │ -0.945184   │ -0.410411  │
│ 4   │ Iris-setosa    │ -1.15181    │ -0.834592  │ 0.207479    │ -0.410411  │
│ 5   │ Iris-setosa    │ -0.0170218  │ 0.47766    │ -0.368852   │ -0.410411  │
│ 6   │ Iris-setosa    │ 1.11776     │ 1.26501    │ 1.36014     │ 1.45509    │
│ 7   │ Iris-setosa    │ -1.15181    │ -0.0472411 │ -0.368852   │ 0.522342   │
⋮
│ 143 │ Iris-virginica │ -1.23923    │ -0.849621  │ -0.818997   │ -0.458766  │
│ 144 │ Iris-virginica │ 0.333396    │ 0.700782   │ 0.630555    │ 0.997633   │
│ 145 │ Iris-virginica │ 0.176134    │ 1.01086    │ 0.268167    │ 1.72583    │
│ 146 │ Iris-virginica │ 0.176134    │ 0.080621   │ -0.637803   │ 0.997633   │
│ 147 │ Iris-virginica │ -0.452916   │ -1.46978   │ -1.00019    │ -0.458766  │
│ 148 │ Iris-virginica │ -0.138391   │ 0.080621   │ -0.637803   │ -0.0946659 │
│ 149 │ Iris-virginica │ -0.610178   │ 1.32094    │ -0.275415   │ 0.997633   │
│ 150 │ Iris-virginica │ -1.08197    │ 0.080621   │ -0.818997   │ -0.822865  │
```
