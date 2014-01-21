# The Split-Apply-Combine Strategy

Many data analysis tasks involve splitting a data set into groups, applying
some functions to each of the groups and then combining the results. A
standardized framework for handling this sort of computation is described in
the paper, [The Split-Apply-Combine Strategy for Data Analysis](http://www.jstatsoft.org/v40/i01),
written by Hadley Wickham.

The DataFrames package supports the Split-Apply-Combine strategy through
the `by` function, which takes in three arguments: (1) a DataFrame, (2) a
column to split the DataFrame on, and (3) a function or expression to
apply to each subset of the DataFrame.

We show several examples of the `by` function applied to the `iris` dataset
below:

    using DataFrames, RDatasets

    iris = data("datasets", "iris")

    by(iris, "Species", nrow)
    by(iris, "Species", df -> mean(df["PetalLength"]))
    by(iris, "Species", :(N = size(_DF, 1)))

If you only want to split the data set into subsets, use the `groupby` function:

    for subdf in groupby(iris, "Species")
        println(size(subdf, 1))
    end
