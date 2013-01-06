---
layout: slate
title: DataFrames.jl - Tools for working with data in Julia
---

# DataFrames.jl

DataFrames.jl is a Julia package that provides many essential tools for working with real-world data sets, including

(1) An `NA` value for expressing missing data:

	isna(NA)

(2) A DataFrame type for working with tabular data:

	df = DataFrame()
	df["MyFirstColumn"] = [1, 2, 3, 4]
	show(df)

(3) DataFrame I/O routines for reading and writing DataFrames from/to standard formats:

	df = read_table("my_dataset.csv")

(4) Split-Apply-Combine routines:

	by(iris, "Species", :(MeanPetalLength = mean(Petal_Length)))

To learn more, you can read through the rest of the DataFrames manual. The Table of Contents is shown below. We recommend starting with the aptly-named "Getting Started" section.

# Table of Contents

* [Table of Contents](http://harlanh.github.com/DataFrames.jl/index.html)
* [Why Use the DataFrames Package?](http://harlanh.github.com/DataFrames.jl/introduction.html)
* [Getting Started](http://harlanh.github.com/DataFrames.jl/getting_started.html)
* [Merging and Indexing](http://harlanh.github.com/DataFrames.jl/merging_and_indexing.html)
* [Reshaping and Pivoting](http://harlanh.github.com/DataFrames.jl/reshaping_and_pivoting.html)
* [Split-Apply-Combine Operations](http://harlanh.github.com/DataFrames.jl/split_apply_combine.html)
* [Streaming Data Analysis](http://harlanh.github.com/DataFrames.jl/datastreams.html)

# Offline Reading

If you prefer reading documentation offline, you can download a copy of our [PDF manual](http://harlanh.github.com/DataFrames.jl/downloads/manual.pdf).
