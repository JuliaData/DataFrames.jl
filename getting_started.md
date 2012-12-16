---
layout: slate
title: Getting Started
---

# Getting Started

## Installation

The DataFrames package is available through the Julia package system. If you've never used the package system before, you'll need to run the following:

	require("pkg")
	Pkg.init()
	Pkg.add("DataFrames")

If you have an existing library of packages, you can pull the DataFrames package into your library using similar commands:

	require("pkg")
	Pkg.add("DataFrames")

## Loading the DataFrames Package

In all of the examples that follow, we're going to assume that you've already loaded the DataFrames package. You can do that by typing the following two commands before trying out any of the examples in this manual:

	load("DataFrames")
	using DataFrames

## Some Basic Examples

As we described in the introduction, the first thing you'll want to do is to confirm that we have a new type that represents a missing value. Type the following into the REPL to see that this is working for you:

	NA

One of the essential properties of `NA` is that it poisons other items. To see this, try to add something to `NA`:

	1 + NA

As we described earlier, you'll get a lot more power out of `NA`'s when they can occur in other data structures. Let's create our first `DataVec` now:

	dv = DataVec([1, 3, 2, 5, 4])
	dv[1] = NA

To see how `NA` poisons even complex calculations, let's try to take the mean of those five numbers:

	mean(dv)

In many cases we're willing to just ignore `NA`'s and remove them from our vector. We can do that using the `removeNA` function:

	removeNA(dv)
	mean(removeNA(dv))

Instead of removing `NA`'s, you can try to ignore them using the `failNA` function. The `failNA` function attempt to convert a `DataVec{T}` to a `Vector{T}` and will throw an error if any `NA`'s are encountered. If we were dealing with a vector like the following, `failNA` will work just right:

	dv = DataVec([1, 3, 2, 5, 4])
	mean(failNA(dv))

In addition to removing or ignoring `NA`'s, it's possible to replace them using the `replaceNA` function:

	dv = DataVec([1, 3, 2, 5, 4])
	dv[1] = NA
	mean(replaceNA(dv, 11))

Which strategy for dealing with `NA`'s is most appropriate will typically depend on the details of your situation.

In modern data analysis `NA`'s don't simply arise in vector-like data. The `DataMatrix` and `DataFrame` structures are also capable of handling `NA`'s. You can confirm for yourself that the presence of `NA`'s poisons matrix operations in the same way that it poisons vector operations by creating a simple `DataMatrix` and trying to perform matrix multiplication:

	dm = DataMatrix([1.0 0.0; 0.0 1.0])
	dm[1, 1] = NA
	dm * dm

## Working with Tabular Data Sets

As we said before, working with simple `DataVec`'s and `DataMatrix`'s gets boring after a while. To express interesting types of tabular data sets, we'll create a simple `DataFrame` piece-by-piece:

	df = DataFrame()
	df["A"] = 1:4
	df["B"] = ["M", "F", "F", "M"]
	df

In practice, we're more likely to use an existing data set than to construct one from scratch. To load a more interesting data set, we can use the `read_table()` function. To make use of it, we'll need a data set stored in a simple format like the comma separated values (CSV) standard. There are some simple examples of CSV files included with the DataFrames package. We can find them using basic file operations in Julia:

	require("pkg")
	mydir = file_path(Pkg.package_directory("DataFrames"), "test", "data")
	filenames = readdir(mydir)
	df = read_table(file_path(mydir, filenames[1]))

The resulting `DataFrame` has a large number of similar rows. We can check its size using the `nrow` and `ncol` commands:

	nrow(df)
	ncol(df)

We can also look at small subsets of the data in a couple of ways:

	head(df)
	tail(df)

	df[1:3, :]

Having seen what some of the rows look like, we can try to summarize the entire data set using:

	summary(df)

To focus our search, we start looking at just the means and medians of the columns:

	colmeans(df)
	colmedians(df)

Or, alternatively, we can look at the columns one-by-one:

	mean(df["E"])
	range(df["E"])

If you'd like to get your hands on more data to play with, we strongly encourage you to try out the RDatasets package. This package supplements the DataFrames package by providing access to 570 classical data sets that will be familiar to R programmers. You can install and load the RDatasets package using the Julia package manager:

	require("pkg")
	Pkg.add("RDatasets")
	load("RDatasets")

Once that's done, you can use the `data()` function from RDatasets to gain access to data sets like Fisher's Iris data:

	iris = RDatasets.data("datasets", "iris")
	head(iris)

The Iris data set is a really interesting testbed for examining simple contrasts between groups. To get at those kind of group differences, we can split apart our data set based on the species of flower being studied and then analyze each group separately. To do that, we'll use the Split-Apply-Combine strategy made popular by R's plyr library. In Julia, we do this using the `by` function:

	function g(df)
		res = DataFrame()
		res["nrows"] = nrow(df)
		res["MeanPetalLength"] = mean(df["Petal.Length"])
		res["MeanPetalWidth"] = mean(df["Petal.Width"])
		return res
	end

	by(iris, "Species", g)

Instead of passing in a function that constructs a `DataFrame` piece-by-piece to summarize each group, you can pass in a Julia expression that will construct columns one-by-one. The simplest example looks like:

	by(iris, "Species", :(NewColumn = 1))

This example is admittedly a little silly. The reason we've started with something trivial is that it's quite difficult to work with our current version of the `iris` `DataFrame` because the current set of column names includes names like `"Petal.Length"`, which are not valid Julia variable names. As such, we can't use these names in Julia expressions. To work around that, the DataFrames package provides a function called `clean_colnames!()` which will replace non-alphanumeric characters with underscores in order to produce valid Julia identifiers:

	clean_colnames!(iris)
	colnames(iris)

Now that the column names are clean, we can put the expression-based

	by(iris, "Species", :(MeanPetalLength = mean(Petal_Length)))
	by(iris, "Species", :(MeanPetalWidth = mean(Petal_Width)))

This style of expression-based manipulation is quite handy once you get used to it. But sometimes you need to summarize groups based on properties of the entire group-level `DataFrame` rather than something describable using just the column names alone. In that case, you can exploit the fact that each group-level DataFrame is temporarily given the name `_DF`:

	by(iris, "Species", :(N = nrow(_DF)))

If none of these ways of working with individual groups of data appeal to you, you can also use the `groupby` function to produce an iterable set of `DataFrame`'s that you can step though one-by-one:

	for df in groupby(iris, "Species")
		println({unique(df["Species"]),
				 mean(df["Petal.Length"]),
				 mean(df["Petal.Width"])})
	end

We hope this brief tutorial introduction has convinced you that you can do quite complex data manipulations using the DataFrames package. To really dig in, we're now going to describe the design of the DataFrames package in greater depth.

# Table of Contents

* [Table of Contents](http://harlanh.github.com/DataFrames.jl/index.html)
* [Getting Started](http://harlanh.github.com/DataFrames.jl/getting_started.html)
