# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper "[The Split-Apply-Combine Strategy for Data Analysis](http://www.jstatsoft.org/v40/i01)", written by Hadley Wickham.

The DataFrames package supports the Split-Apply-Combine strategy through the `by` function, which takes in three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) a function or expression to apply to each subset of the DataFrame.

We show several examples of the `by` function applied to the `iris` dataset below:

```jldoctest sac
julia> using DataFrames, CSV, Statistics

julia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), "../test/data/iris.csv"));

julia> head(iris)
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

julia> tail(iris)
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

julia> by(iris, :Species, size)
3×2 DataFrame
│ Row │ Species       │ x1      │
│     │ Categorical…⍰ │ Tuple…  │
├─────┼───────────────┼─────────┤
│ 1   │ setosa        │ (50, 5) │
│ 2   │ versicolor    │ (50, 5) │
│ 3   │ virginica     │ (50, 5) │

julia> by(iris, :Species, df -> mean(df.PetalLength))
3×2 DataFrame
│ Row │ Species       │ x1      │
│     │ Categorical…⍰ │ Float64 │
├─────┼───────────────┼─────────┤
│ 1   │ setosa        │ 1.462   │
│ 2   │ versicolor    │ 4.26    │
│ 3   │ virginica     │ 5.552   │

julia> by(iris, :Species, df -> DataFrame(N = size(df, 1)))
3×2 DataFrame
│ Row │ Species       │ N     │
│     │ Categorical…⍰ │ Int64 │
├─────┼───────────────┼───────┤
│ 1   │ setosa        │ 50    │
│ 2   │ versicolor    │ 50    │
│ 3   │ virginica     │ 50    │
```

The `by` function also support the `do` block form:

```jldoctest sac
julia> by(iris, :Species) do df
          DataFrame(m = mean(df.PetalLength), s² = var(df.PetalLength))
       end
3×3 DataFrame
│ Row │ Species       │ m       │ s²        │
│     │ Categorical…⍰ │ Float64 │ Float64   │
├─────┼───────────────┼─────────┼───────────┤
│ 1   │ setosa        │ 1.462   │ 0.0301592 │
│ 2   │ versicolor    │ 4.26    │ 0.220816  │
│ 3   │ virginica     │ 5.552   │ 0.304588  │
```

A second approach to the Split-Apply-Combine strategy is implemented in the `aggregate` function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) one or more functions that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column that was not used to split the DataFrame, creating new columns of the form `$name_$function`. For named functions like `mean` this will produce columns with names like `SepalLength_mean`. For anonymous functions like `x -> sqrt(x)^e`, which Julia tracks and references by a numerical identifier e.g. `#12`, the produced columns will be `SepalLength_#12`. We show several examples of the `aggregate` function applied to the `iris` dataset below:

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
