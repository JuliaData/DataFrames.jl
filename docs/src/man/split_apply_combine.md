# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper, The Split-Apply-Combine Strategy for Data Analysis \<<http://www.jstatsoft.org/v40/i01>\>, written by Hadley Wickham.

The DataFrames package supports the Split-Apply-Combine strategy through the `by` function, which takes in three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) a function or expression to apply to each subset of the DataFrame.

We show several examples of the `by` function applied to the `iris` dataset below:

```julia
using DataFrames, RDatasets

iris = dataset("datasets", "iris")

by(iris, :Species, size)
by(iris, :Species, df -> mean(df[:PetalLength]))
by(iris, :Species, df -> DataFrame(N = size(df, 1)))
```

The `by` function also support the `do` block form:

```julia
by(iris, :Species) do df
   DataFrame(m = mean(df[:PetalLength]), s² = var(df[:PetalLength]))
end
```

A second approach to the Split-Apply-Combine strategy is implemented in the `aggregate` function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and a (3) function (or several functions) that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column, that was not used to split the DataFrame, creating new columns of the form `$name_$function` e.g. `SepalLength_mean`. Anonymous functions and expressions that do not have a name will be called `λ1`.

We show several examples of the `aggregate` function applied to the `iris` dataset below:

```julia
aggregate(iris, :Species, sum)
aggregate(iris, :Species, [sum, mean])
```

If you only want to split the data set into subsets, use the `groupby` function:

```julia
for subdf in groupby(iris, :Species)
    println(size(subdf, 1))
end
```
