# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper "[The Split-Apply-Combine Strategy for Data Analysis](http://www.jstatsoft.org/v40/i01)", written by Hadley Wickham.

The DataFrames package supports the split-apply-combine strategy through the `by` function, which is a shorthand for `groupby` followed by `map` and/or `combine`. `by` takes in three arguments: (1) a `DataFrame`, (2) one or more columns to split the `DataFrame` on, and (3) a specification of one or more functions to apply to each subset of the `DataFrame`. This specification can be of the following forms:
1. a `col => function` pair indicating that `function` should be called with the vector of values for column `col`, which can be a column name or index
2. a `cols => function` pair indicating that `function` should be called with a named tuple holding columns `cols`, which can be a tuple or vector of names or indices
3. several such pairs, either as positional arguments or as keyword arguments (mixing is not allowed), producing each a single separate column; keyword argument names are used as column names
4. equivalently, a (named) tuple or vector of such pairs
5. a function which will be called with a `SubDataFrame` corresponding to each group; this form should be avoided due to its poor performance

In all of these cases, the function can return either a single row or multiple rows, with a single or multiple columns:
- a single value produces a single row and column per group
- a named tuple or `DataFrameRow` produces a single row and one column per field
- a vector produces a single column with one row per entry
- a named tuple of vectors produces one column per field with one row per entry in the vectors
- a `DataFrame` or a matrix produces as many rows and columns as it contains; note that returning a `DataFrame` should be avoided due to its poor performance when the number of groups is large

The kind of return value and the number and names of columns must be the same for all groups.

As a special case, if multiple pairs or a tuple of vectors or pairs is passed (forms 3 and 4 above), each function is required to return a single value or vector, which will produce each a separate column.

The name for the resulting column can be chosen either by passing a named tuple of pairs, or by returning a named tuple or a data frame. If no name is provided, it is generated automatically. For functions taking a single column (first form), the input column name is concatenated with the function name: for standard functions like `mean` this will produce columns with names like `SepalLength_mean`; for anonymous functions like `x -> 2 * sqrt(x)`, the produced columns will be `SepalLength_function`. For functions taking multiple columns (second form), names are `x1`, `x2`, etc.

We show several examples of the `by` function applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

julia> iris = DataFrame(CSV.File(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv")));

julia> first(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa        │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa        │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa        │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa        │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │

julia> last(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica     │
│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica     │
│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica     │
│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica     │
│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica     │
│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica     │

julia> by(iris, :Species, :PetalLength => mean)
3×2 DataFrame
│ Row │ Species    │ PetalLength_mean │
│     │ String⍰    │ Float64          │
├─────┼────────────┼──────────────────┤
│ 1   │ setosa     │ 1.462            │
│ 2   │ versicolor │ 4.26             │
│ 3   │ virginica  │ 5.552            │

julia> by(iris, :Species, N = :Species => length) # Chosen column is arbitrary
3×2 DataFrame
│ Row │ Species       │ N     │
│     │ Categorical…⍰ │ Int64 │
├─────┼───────────────┼───────┤
│ 1   │ setosa        │ 50    │
│ 2   │ versicolor    │ 50    │
│ 3   │ virginica     │ 50    │

julia> by(iris, :Species, N = :Species => length, mean = :PetalLength => mean) # Column for length is arbitrary
3×3 DataFrame
│ Row │ Species    │ N     │ mean    │
│     │ String⍰    │ Int64 │ Float64 │
├─────┼────────────┼───────┼─────────┤
│ 1   │ setosa     │ 50    │ 1.462   │
│ 2   │ versicolor │ 50    │ 4.26    │
│ 3   │ virginica  │ 50    │ 5.552   │

julia> by(iris, :Species, [:PetalLength, :SepalLength] =>
              x -> (a=mean(x.PetalLength)/mean(x.SepalLength), b=sum(x.PetalLength)))
3×3 DataFrame
│ Row │ Species    │ a        │ b       │
│     │ String⍰    │ Float64  │ Float64 │
├─────┼────────────┼──────────┼─────────┤
│ 1   │ setosa     │ 0.29205  │ 73.1    │
│ 2   │ versicolor │ 0.717655 │ 213.0   │
│ 3   │ virginica  │ 0.842744 │ 277.6   │
```

The `by` function also supports the `do` block form. However, as noted above, this form is slow and should therefore be avoided when performance matters.

```jldoctest sac
julia> by(iris, :Species) do df
          (m = mean(df.PetalLength), s² = var(df.PetalLength))
       end
3×3 DataFrame
│ Row │ Species       │ m       │ s²        │
│     │ Categorical…⍰ │ Float64 │ Float64   │
├─────┼───────────────┼─────────┼───────────┤
│ 1   │ setosa        │ 1.462   │ 0.0301592 │
│ 2   │ versicolor    │ 4.26    │ 0.220816  │
│ 3   │ virginica     │ 5.552   │ 0.304588  │
```

A second approach to the Split-Apply-Combine strategy is implemented in the `aggregate` function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) one or more functions that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column that was not used to split the DataFrame, creating new columns of the form `$name_$function` like with `by` (see above). We show several examples of the `aggregate` function applied to the `iris` dataset below:

```jldoctest sac
julia> aggregate(iris, :Species, length)
3×5 DataFrame
│ Row │ Species       │ SepalLength_length │ SepalWidth_length │ PetalLength_length │ PetalWidth_length │
│     │ Categorical…⍰ │ Int64              │ Int64             │ Int64              │ Int64             │
├─────┼───────────────┼────────────────────┼───────────────────┼────────────────────┼───────────────────┤
│ 1   │ setosa        │ 50                 │ 50                │ 50                 │ 50                │
│ 2   │ versicolor    │ 50                 │ 50                │ 50                 │ 50                │
│ 3   │ virginica     │ 50                 │ 50                │ 50                 │ 50                │

julia> aggregate(iris, :Species, [sum, mean])
3×9 DataFrame. Omitted printing of 2 columns
│ Row │ Species       │ SepalLength_sum │ SepalWidth_sum │ PetalLength_sum │ PetalWidth_sum │ SepalLength_mean │ SepalWidth_mean │
│     │ Categorical…⍰ │ Float64         │ Float64        │ Float64         │ Float64        │ Float64          │ Float64         │
├─────┼───────────────┼─────────────────┼────────────────┼─────────────────┼────────────────┼──────────────────┼─────────────────┤
│ 1   │ setosa        │ 250.3           │ 171.4          │ 73.1            │ 12.3           │ 5.006            │ 3.428           │
│ 2   │ versicolor    │ 296.8           │ 138.5          │ 213.0           │ 66.3           │ 5.936            │ 2.77            │
│ 3   │ virginica     │ 329.4           │ 148.7          │ 277.6           │ 101.3          │ 6.588            │ 2.974           │
```

If you only want to split the data set into subsets, use the `groupby` function:

```jldoctest sac
julia> for subdf in groupby(iris, :Species)
           println(size(subdf, 1))
       end
50
50
50
```
