# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper "[The Split-Apply-Combine Strategy for Data Analysis](http://www.jstatsoft.org/v40/i01)", written by Hadley Wickham.

The DataFrames package supports the split-apply-combine strategy through the `by` function, which is a shorthand for `groupby` followed by `map` and/or `combine`. `by` takes in three arguments: (1) a `DataFrame`, (2) one or more columns to split the `DataFrame` on, and (3) a specification of one or more functions to apply to each subset of the `DataFrame`. This specification can be of the following forms:
1. a `col => function` pair indicating that `function` should be called with the vector
   of values for column `col`, which can be a column name or index
2. a `cols => function` pair indicating that `function` should be called with
   positional arguments holding columns `cols`, which can be a any valid column selector
3. a `cols => function => target_col` form additionally
   specifying the name of the target column (this assumes that `function` returns a single value or a vector)
4. a `col => target_col` pair, which renames the column `col` to `target_col`
5. a `nrow` or `nrow => target_col` form which efficiently computes the number of rows in a group
   (without `target_col` the new column is called `:nrow`)
6. several arguments of the forms given above, or vectors thereof
7. a function which will be called with a `SubDataFrame` corresponding to each group;
   this form should be avoided due to its poor performance unless a very large
   number of columns are processed (in which case `SubDataFrame` avoids excessive
   compilation)

All forms except 6 can be also passed as the first argument to `by`.

In all of these cases, `function` can return either a single row or multiple rows.
`function` can always generate a single column by returning a single value or a vector.
Additionally, if `by` is passed exactly one `function` and `target_col` is not specified,
`function` can return multiple columns in the form of an `AbstractDataFrame`,
`AbstractMatrix`, `NamedTuple` or `DataFrameRow`.

Here are the rules specifying the shape of the resulting `DataFrame`:
- a single value produces a single row and column per group
- a named tuple or `DataFrameRow` produces a single row and one column per field
- a vector produces a single column with one row per entry
- a named tuple of vectors produces one column per field with one row per entry in the vectors
- a `DataFrame` or a matrix produces as many rows and columns as it contains;
  note that this option should be avoided due to its poor performance when the number
  of groups is large

The kind of return value and the number and names of columns must be the same for all groups.

If a single value or a vector is returned by the `function` and `target_col` is not
provided, it is generated automatically, by concatenating source column name and
`function` name where possible (see examples below).

We show several examples of the `by` function applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

julia> iris = DataFrame(CSV.File(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv")));

julia> first(iris, 6)
6×6 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │ id    │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String      │ Int64 │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┼───────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ Iris-setosa │ 1     │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa │ 2     │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │ 3     │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ Iris-setosa │ 4     │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ Iris-setosa │ 5     │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa │ 6     │

julia> last(iris, 6)
6×6 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │ id    │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String         │ Int64 │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┼───────┤
│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ Iris-virginica │ 145   │
│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ Iris-virginica │ 146   │
│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ Iris-virginica │ 147   │
│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ Iris-virginica │ 148   │
│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │ 149   │
│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ Iris-virginica │ 150   │

julia> by(iris, :Species, :PetalLength => mean)
3×2 DataFrame
│ Row │ Species         │ PetalLength_mean │
│     │ String          │ Float64          │
├─────┼─────────────────┼──────────────────┤
│ 1   │ Iris-setosa     │ 1.464            │
│ 2   │ Iris-versicolor │ 4.26             │
│ 3   │ Iris-virginica  │ 5.552            │

julia> by(iris, :Species, nrow)
3×2 DataFrame
│ Row │ Species         │ nrow  │
│     │ String          │ Int64 │
├─────┼─────────────────┼───────┤
│ 1   │ Iris-setosa     │ 50    │
│ 2   │ Iris-versicolor │ 50    │
│ 3   │ Iris-virginica  │ 50    │

julia> by(iris, :Species, nrow, :PetalLength => mean => :mean)
3×3 DataFrame
│ Row │ Species         │ nrow  │ mean    │
│     │ String          │ Int64 │ Float64 │
├─────┼─────────────────┼───────┼─────────┤
│ 1   │ Iris-setosa     │ 50    │ 1.464   │
│ 2   │ Iris-versicolor │ 50    │ 4.26    │
│ 3   │ Iris-virginica  │ 50    │ 5.552   │

julia> by(iris, :Species,
          [:PetalLength, :SepalLength] =>
          (p, s) -> (a=mean(p)/mean(s), b=sum(p))) # multiple columns are passed as arguments
3×3 DataFrame
│ Row │ Species         │ a        │ b       │
│     │ String          │ Float64  │ Float64 │
├─────┼─────────────────┼──────────┼─────────┤
│ 1   │ Iris-setosa     │ 0.292449 │ 73.2    │
│ 2   │ Iris-versicolor │ 0.717655 │ 213.0   │
│ 3   │ Iris-virginica  │ 0.842744 │ 277.6   │
```

The `by` function also supports the `do` block form. However, as noted above,
this form is slow and should therefore be avoided when performance matters.

```jldoctest sac
julia> by(iris, :Species) do df
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

A second approach to the Split-Apply-Combine strategy is implemented in the `aggregate` function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) one or more functions that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column that was not used to split the DataFrame, creating new columns of the form `$name_$function` like with `by` (see above). We show several examples of the `aggregate` function applied to the `iris` dataset below:

```jldoctest sac
julia> aggregate(iris, :Species, length)
3×6 DataFrame
│ Row │ Species         │ SepalLength_length │ SepalWidth_length │ PetalLength_length │ PetalWidth_length │ id_length │
│     │ String          │ Int64              │ Int64             │ Int64              │ Int64             │ Int64     │
├─────┼─────────────────┼────────────────────┼───────────────────┼────────────────────┼───────────────────┼───────────┤
│ 1   │ Iris-setosa     │ 50                 │ 50                │ 50                 │ 50                │ 50        │
│ 2   │ Iris-versicolor │ 50                 │ 50                │ 50                 │ 50                │ 50        │
│ 3   │ Iris-virginica  │ 50                 │ 50                │ 50                 │ 50                │ 50        │

julia> aggregate(iris, :Species, [sum, mean])
3×11 DataFrame. Omitted printing of 3 columns
│ Row │ Species         │ SepalLength_sum │ SepalWidth_sum │ PetalLength_sum │ PetalWidth_sum │ id_sum │ SepalLength_mean │ SepalWidth_mean │
│     │ String          │ Float64         │ Float64        │ Float64         │ Float64        │ Int64  │ Float64          │ Float64         │
├─────┼─────────────────┼─────────────────┼────────────────┼─────────────────┼────────────────┼────────┼──────────────────┼─────────────────┤
│ 1   │ Iris-setosa     │ 250.3           │ 170.9          │ 73.2            │ 12.2           │ 1275   │ 5.006            │ 3.418           │
│ 2   │ Iris-versicolor │ 296.8           │ 138.5          │ 213.0           │ 66.3           │ 3775   │ 5.936            │ 2.77            │
│ 3   │ Iris-virginica  │ 329.4           │ 148.7          │ 277.6           │ 101.3          │ 6275   │ 6.588            │ 2.974           │
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
`GroupedDataFrame` with a a `Tuple` or `NamedTuple`:
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
