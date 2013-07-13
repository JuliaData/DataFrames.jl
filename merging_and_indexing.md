---

layout: minimal
title: Merging and Indexing

---

# Merging Data Sets Together

Often we have several related data sets that we need to merge together. For example, we might have data about the flowers from Fisher's iris data set:

    require("DataFrames")
    using DataFrames

    require("RDatasets")
    using RDatasets

    iris = data("datasets", "iris")

This data set describes individual flowers from three species, but we might want to incorporate generic knowledge about the typical properties of those species into our analysis. Suppose that we have another data set called `flowers` like that defined below:

    flowers = DataFrame()
    flowers["Species"] = ["virginica", "versicolor", "setosa"]
    flowers["PrimaryColor"] = ["purplish", "purple", "purple"]

How could we merge in the information about primary colors from the `flowers` data set into the `iris` data set?

In Julia, we use a function called `merge` that is inspired by techniques for joining together different database tables. The simplest example of merge is:

    iris_with_colors = merge(iris, flowers)

When called on two data sets, `merge(A, B)` tries to identify a commonly named column that will guide the process of matching rows from `A` with rows from `B`. In this example, that column is the `Species` column. We can help `merge` out by naming this column explicitly:

    iris_with_colors = merge(iris, flowers, "Species")

In this example, it is clear which rows from `iris` should be associated with which rows from `flowers`. But what if `flowers` mentioned a fourth species of flower not found in the `iris` data set? For example, imagine that we added information about daisies to `flowers`:

    flowers = DataFrame()
    flowers["Species"] = ["virginica", "versicolor", "setosa", "daisy"]
    flowers["PrimaryColor"] = ["purplish", "purple", "purple", "yellow"]

What will happen now? We can see by calling `merge` again:

    merge(iris, flowers, "Species")

If you inspect the results, you'll see that nothing has changed. This is because `merge` defaults to a style of merging called an "inner join" which looks at the values of "Species" in both `iris` and `flowers` and only uses the values found in both data sets. We can insure this behavior by explicitly specifying that we want an "inner" join using a fourth argument to `merge`:

    merge(iris, flowers, "Species", "inner")

What other types of merging operations are there? In total, there are four:

* _Inner join_: Use the values of the Species column that are found in both the `iris` and `flowers` data sets.
* _Left join_: Use only the values of the Species column that are found in the `iris` data set.
* _Right join_: Use only the values of the Species column that are found in the `flowers` data set.
* _Outer join_: Use the values of the Species column that are found in either the `iris` or `flowers` data set.

In our current example, it isn't easy to tell these apart. To make it more clear, we'll use a different data set in which `flowers` is missing data about the "setosa" species, but also has unneeded data about the irrelevant "daisy" species:

    flowers = DataFrame()
    flowers["Species"] = ["virginica", "versicolor", "daisy"]
    flowers["PrimaryColor"] = ["purplish", "purple", "yellow"]

In that case, we get quite different results from the four types of joins:

    merge(iris, flowers, "Species", "inner")
    merge(iris, flowers, "Species", "left")
    merge(iris, flowers, "Species", "right")
    merge(iris, flowers, "Species", "outer")

As you'll see, the inner join produces 100 rows and contains no information about the "setosa" Species because that species was not found in the `flowers` data set. The left join contains 150 rows, but is missing color information for "setosa" because it wasn't present in the `flowers` data set. The right join contains 101 rows, including an _almost_ completely empty row describing the "daisy" species that  doesn't appear in the `iris`  data aset. Finally, the outer join contains 151 rows describing all four species, incluing both the "setosa" species from the `iris` data set and the "daisy" species from the `flowers` data set.

# Indexing: Making Subsetting and Mergers Faster

One problem with merging large data sets is that the merging process can take a long time to complete. This is because the merging process has to determine which subset of rows from `A` should be combined with which subset of rows from `B`. Selecting subsets in this way is slow in general for most DataFrames because the entries of each column have to be exhaustively examined.

But it is possible to make subset selection much faster if we allow the `DataFrame` to store indexing information that tells the system where to expect certain subsets to be located inside of the `DataFrame`. If you are familar with database systems, this indexing involves either explicit index metadata that is added to a database or an implicit index defined by a "primary key" for the database.

For the `iris` data set, an indexing step would

_MORE TO BE FILLED IN HERE_
