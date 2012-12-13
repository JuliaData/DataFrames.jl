# Table of Contents

* Why Use the DataFrames Package?
	*  Missing Data Points
	*  Data Structures for Storing Missing Data Points
	*  Tabular Data Structures
	*  A Language for Expressing Statistical Models
* Getting Started
* The Design of DataFrames
* Formal Specification of DataFrames
* Function Reference Guide

\newpage

---

# Why Use the DataFrames Package?

We want to use Julia for statistical programming. While Julia is a very powerful tool for general mathematical programming, it is missing several basic features that are essential for statistical applications. This introductory section describes some of the features that are missing from Julia's core. The rest of the manual describes the ways in which the DataFrames package extends Julia to make up for those missing features.

## Missing Data Points

Suppose that we want to calculate the mean of a list of five `Float64` numbers: `x1`, `x2`, `x3`, `x4` and `x5`. We would normally do this in Julia as follows:

* Represent these five numbers as a `Vector`: `v = [x1, x2, x3, x4, x5]`.
* Compute the mean of `v` using the `mean()` function.

_But what if one of the five numbers were missing?_

The concept of a missing data point cannot be directly expressed in Julia because there is no `NULL` value. In the statistically focused languages S and R, missing data points are described using the `NA` value. Adding an `NA` value to Julia is the DataFrames package's first extension of Julia's core type system.

## Data Structures for Storing Missing Data Points

Even if we can express the notion that the value of the number `x2` is unknown by using a new `NA` value, there is little that we can do with this new value because it cannot be directly stored in a standard Julian `Vector` unless that `Vector` has no type constraints on its entries. While we could use a `Vector{Any}`, this produces very inefficient code.

Instead of using generic data structures, we have created extensions of the core Julia `Vector` and `Matrix` data structures. These augmented data structures are called `DataVec`'s and `DataMatrix`'s. Both `DataVec`'s and `DataMatrix`'s can contain either (a) values of any specific type or (b) our new `NA` type.

For example, a standard `Vector{Float64}` can contain `Float64`'s and nothing else. Our new `DataVec{Float64}` can contain `Float64`'s or `NA`'s, but nothing else. This makes the new data types much more efficient than using generic containers like `Vector{Any}` or `Matrix{Any}`.

## Tabular Data Structures

`DataVec`'s and `DataMatrix`'s are very powerful data structures, but they are not sufficient for describing most real world data sets. Although most standard data sets are easily described using a simple table of data, these kinds of tables are generally not like matrices. The table of data shown below highlights some of the ways in which a data set is not like a `DataMatrix`:

![Tabular Data](figures/data.pdf)

We highlight three major differences below:

* The columns of a tabular data set may have different types. A `DataMatrix` can only contain values of one type: these might all be `String`'s or `Int`'s, but we cannot have one column of `String`'s and another column of `Int`'s.
* The values of the entries within a column generally have a consistent type. This means that a single column could be represented using a `DataVec`. Unfortunately, the heterogeneity of types between columns means that we need someway of wrapping a group of columns together into a coherent whole. We could use a `Vector` to wrap up all of the columns of the table, but this will not enforce one constraint imposed by our intuitions: _every column of a tabular data set has the same length as all of the other columns_.
* The columns of a tabular data set are typically named using `String`'s. Most programs for working with data can access the columns of a data set using these names in addition to simple numeric indices. In other words, a tabular data structure can sometimes behave like an `Array` and can sometimes behave like a `Dict`.

We can summarize these concerns by noting that we face four problems when with working with tabular data sets that are not well solved by existing Julian data structures:

* Tabular data sets may have heterogeneous types of columns.
* Each column of a tabular data set has a consistent type.
* Each column of a tabular data set has a consistent length, although some entries may be missing.
* The columns of a tabular data set should be addressable by both name and index.

We solve all of these four problems by adding a `DataFrame` type to Julia. This type will be familiar to anyone who has worked with R's `data.frame` type or with Pandas' `DataFrame` type. Even if you have never used R or Python to work with data, this tabular data structure will be similar to many of the intuitions that you've developed while working with spreadsheet programs like Excel.

## A Language for Expressing Statistical Models

Statistical programming is generally focused on answering substantive questions about the properties of a data set. We are generally not interested in thinking about algorithms, but instead want to spend our time thinking about mathematical models.

Part of the power of the R programming language is that it provides a coherent mini-language for talking about various types of linear models ranging from ANOVA's to GLM's. For example, R describes a regression in which a variable `Z` is regressed against the variables `X` and `Y` using the notation:

    Z ~ X + Y

Julia, by default, provides no similar sort of mini-language for describing the mathematical structure of statistical models. To remedy this, we have added a `Formula` type to Julia that provides a simple DSL for describing linear models in Julia.

\newpage

---

# Getting Started

## Installation

The DataFrames package is available through the Julia package system. If you've never used the package syste, before, you'll need to run the following:

	require("pkg")
	Pkg.init()
	Pkg.add("DataFrames")

If you have an existing library of packages, you can pull the DataFrames package into your library using similar commands:

	require("pkg")
	Pkg.add("DataFrames")

## Loading the DataFrames Package

In all of the examples that follow, we're going to assume that you've already loaded the DataFrames package. You can do that by typing the following two commands before trying any of our examples:

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

Dealing with `NA`'s is a constant challenge. The presence of `NA`'s poisons matrix operations in the same way that it poisons vector operations. To see that, we'll create our first `DataMatrix`:

	dm = DataMatrix([1.0 0.0; 0.0 1.0])
	dm[1, 1] = NA
	dm * dm

As we said before, working with simple `DataVec`'s and `DataMatrix`'s gets boring after a while. To express interesting types of tabular data sets, we'll create a simple `DataFrame` piece-by-piece:

	df = DataFrame()
	df["A"] = 1:4
	df["B"] = ["M", "F", "F", "M"]
	df

In practice, we're more likely to use an existing data set than to construct one from scratch. To load a more interesting data set, we can use the `read_table()` function. To make use of it, we'll need a data set. There are some simple examples included with the DataFrames package. We can find them using basic file operations in Julia:

	require("pkg")
	mydir = file_path(Pkg.package_directory("DataFrames"), "test", "data")
	filenames = readdir(mydir)
	df = read_table(file_path(mydir, filenames[1]))

The resulting `DataFrame` is pretty large. We can check its size using the `nrow` and `ncol` commands:

	nrow(df)
	ncol(df)

We can look at simpler subsets of the data in a couple of ways:

	head(df)
	tail(df)

	df[1:3, :]

We can summarize this data set by looking at means and medians of the columns:

	colmeans(df)
	colmedians(df)

Alternatively, we can look at columns one-by-one:

	mean(df["E"])
	range(df["E"])

If you'd like to get your hands on more data to play with, we strongly encourage you to try out the RDatasets package. This package supplements the DataFrames package by providing access to 570 classical data sets. You can install and load it using the Julia package manager:

	require("pkg")
	Pkg.add("RDatasets")
	load("RDatasets")

Once that's done, you can use the `data()` function to gain access to data sets like Fisher's Iris data:


	iris = RDatasets.data("datasets", "iris")
	head(iris)

The Iris data set is really interesting for examining contrasts between groups. To get at those, we can split apart our data set based on the species of flower being studied and analyze them separately. To do that, we use the Split-Apply-Combine strategy made popular by R's plyr library:

	for df in groupby(iris, "Species")
		println({unique(df["Species"]),
				 mean(df["Petal.Length"]),
				 mean(df["Petal.Width"])})
	end

As you can see, we can do quite complex data manipulations using DataFrames. To really dig in, we're now going to describe the design of the DataFrames package in greater depth.

\newpage

---

# The Design of DataFrames

## The Type Hierarchy

Before we do anything else, let's go through the hierarchy of types introduced by the DataFrames package. This type hierarchy is depicated visually in the figures at the end of this section and can be summarized in a simple nested list:

* NAtype
* AbstractDataVec
	* DataVec
	* PooledDataVec
* AbstractMatrix
	* DataMatrix
* AbstractDataFrame
	* DataFrame
* AbstractDataStream
	* FileDataStream
	* DataFrameDataStream
	* MatrixDataStream

We'll step through each element of this hierarchy in turn in the following sections.

![Scalar and Array Types](figures/types1.pdf)

![Tabular Data Types](figures/types2.pdf)

## Overview of Basic Types for Working with Data

There are four new types introduced by the current generation of the DataFrames package:

* NAType: A scalar value that represents a single missing piece of data. This value behaves much like `NA` in R.
* DataVec: A vector that can contain values of a specific type as well as `NA`'s.
* PooledDataVec: An alternative to DataVec's that can be more memory-efficient if a small number of distinc values are present in the underlying vector of data.
* DataFrame: A tabular data structure that is similar to R's `data.frame` and Pandas' `DataFrame`.

In the future, we will also be introducing generic Arrays of arbitrary dimension. After this, we will provide two new types:

* DataMatrix: A matrix that can contain values of a specific type as well as `NA`'s.
* DataFrame: An array that can contain values of a specific type as well as `NA`'s.

# The NA Type

The core problem with using the data structures built into Julia for data analysis is that there is no mechanism for expressing the absence of data. Traditional database systems express the absence of data using a `NULL` value, while data analysis packages typically follow the tradition set by S and use `NA` for this purpose when referring to data. (NB: _In S and R, `NULL` is present in addition to `NA`, but it refers to the absence of any specific value for a variable in code, rather than the absence of any specific value for something inside of a data set._)

The DataFrames package expresses the absence of data by introducing a new type called `NAtype`. This value is used everywhere to indicate missingness in the underlying data set.

To see this value, you can type

	NAtype

in the Julia REPL. You can learn more about the nature of this new type using standard Julia functions for navigating Julia's type system:

	typeof(NAtype)

	super(NAtype)

	dump(NAtype)

While the `NAtype` provides the essential type needed to express missingness, the practical way that missing data is denoted uses a special constant `NA`, which is an instance of `NAtype`:

	NA
	NAtype()

You can explore this value to confirm that `NA` is just an instance of the `NAtype`:

	typeof(NA)

	dump(NA)

Simply being able to express the notion that a data point is missing is important, but we're ultimately not interested in just expressing data: we want to build tools for interacting with data that may be missing. In a later section, we'll describe the details of interacting with `NA`, but for now we'll state the defining property of `NA`: _because `NA` expresses ignorance about the value of something, every interaction with `NA` corrupts known values and transforms them into `NA`'s. Below we show how this works for addition:

	1 + NA

We'll discuss the subtleties of `NA`'s ability to corrupt known values in a later section. For now the essential point is this: `NA`'s exist to represent missingness that occurs in scalar data.

# The DataVec Type

To express the notion that a complex data structure like an `Array` contains missing entries, we need to construct a new data structure that can contain standard Julia values like `Float64` while also allowing the presence of `NA` values.

Of course, a Julian `Array{Any}` would allow us to do this:

	{1, NA}

But consistently using `Any` arrays would make Julia much less efficient. Instead, we want to provide a new data structure that parallels a standard Julia `Array`, while allowing exactly one additional value: `NA`.

This new data structure is the `DataVec` type. You can construct your first `DataVec` using the following code:

	DataVec[1, NA, 3]

As you'll see when entering this into the REPL, this snippet of code creates a `3-element DataVec{Int64}`. A `DataVec` of type `DataVec{Int64}` can store `Int64` values or `NA`'s. In general, a `DataVec` of type `DataVec{T}` can store values of type `T` or `NA`'s.

This is achieved by a very simple mechanism: a `DataVec{T}` is a new parametric composite type that we've added to Julia that wraps around a standard Julia `Vector` and complements this basic vector with a metadata store that indicates whether any entry of the wrapped vector is missing. In essence, a `DataVec` of type `T` is defined as:

	type DataVec{T}
		data::Vector{T}
		na::BitVector
	end

This allows us to assess whether any entry of the vector is `NA` at the cost of exactly one additional bit per item. We are able to save space by using `BitArray`'s instead of an `Array{Bool}`. At present, we store the non-missing data values in a vector called `data` and we store the metadata that indicates which values are missing in a vector called `na`. But end-users should not worry about these implementation details.

Instead, you can simply focus on the behavior of the `DataVec` type. Let's start off by exploring the basic properties of this new type:

	DataVec

	typeof(DataVec)
	typeof(DataVec{Int64})

	super(DataVec)
	super(super(DataVec))

	DataVec.names

If you want to drill down further, you can always run `dump()`:

	dump(DataVec)

We're quite proud that the definition of `DataVec`'s is so simple: it makes it easier for end-users to start contributing code to the DataFrames package.

# Constructing DataVec's

Let's focus on ways that you can create new `DataVec`'s. The simplest possible constructor requires the end-user to directly specify both the underlying data values and the missingness metadata as a `BitVector`:

	dv = DataVec([1, 2, 3], falses(3))

This is rather ugly, so we've defined many additional constructors that make it easier to create a new `DataVec`. The first simplification is to ignore the distinction between a `BitVector` and an `Array{Bool, 1}` by allowing users to specify `Bool` values directly:

	dv = DataVec([1, 2, 3], [false, false, false])

In practice, this is still a lot of useless typing when all of the values of the new `DataVec` are not missing. In that case, you can just pass a Julian vector:

	dv = DataVec([1, 2, 3])

When the values you wish to store in a `DataVec` are sequential, you can cut down even further on typing by using a Julian `Range`:

	dv = DataVec(1:3)

In contrast to these normal-looking constructors, when some of the values in the new `DataVec` are missing, there is a very special type of constructor you can use:

	dv = DataVec[1, 2, NA, 4]

_Technical Note: This special type of constructor is defined by overloading the `ref()` function to apply to values of type `DataVec`.

# DataVec's with Special Types

One of the virtues of using metadata to represent missingness instead of sentinel values like `NaN` is that we can easily define `DataVec`'s over arbitrary types. For example, we can create `DataVec`'s that store arbitrary Julia types like `ComplexPair`'s and `Bool`'s:

	dv = DataVec([1 + 2im, 3 - 1im])

	dv = DataVec([true, false])

In fact, we can add a new type of our own and then wrap it inside of a new sort of `DataVec`:

	type MyNewType
		a::Int64
		b::Int64
		c::Int64
	end

	dv = DataVec([MyNewType(1, 2, 3), MyNewType(2, 3, 4)])

Of course, specializing the types of `DataVec`'s means that we sometimes need to convert between types. Just as Julia has several specialized conversion functions for doing this, the DataFrames package provides conversion functions as well. For now, we have three such functions:

* `dvint()`
* `dvfloat()`
* `dvbool()`

Using these, we can naturally convert between types:

	dv = DataVec([1.0, 2.0])

	dvint(dv)

In the opposite direction, we sometimes want to create arbitrary length `DataVec`'s that have a specific type before we insert values:

	dv = DataVec(Float64, 5)

	dv[1] = 1

`DataVec`'s created in this way have `NA` in all entries. If you instead wish to initialize a `DataVec` with standard initial values, you can use one of several functions:

* dvzeros()
* dvones()
* dvfalses()
* dvtrues()

Like the similar functions in Julia's Base, we can specify the length and type of these initialized vectors:

	dv = dvzeros(5)
	dv = dvzeros(Int64, 5)

	dv = dvones(5)
	dv = dvones(Int64, 5)

	dv = dvfalses(5)

	dv = dvtrues(5)

# The PooledDataVec Type

_TO BE FILLED IN_

# The DataFrame Type

While `DataVec`'s are a very powerful tool for dealing with missing data, they only bring us part of the way towards representing real-world data in Julia. The final missing data structure is a tabular data structure of the sort used in relational databases and spreadsheet software.

To represent these kinds of tabular data sets, the DataFrames package provides the `DataFrame` type. The `DataFrame` type is a new Julian composite type with just two fields:

* `columns`: A Julia `Vector{Any}`, each element of which will be a single column of the tabular data. The typical column is of type `DataVec{T}`, but this is not strictly required.
* `colindex`: An `Index` object that allows one to access entries in the columns using both numeric indexing (like a standard Julian `Array`) or key-valued indexing (like a standard Julian `Dict`). The details of the `Index` type will be described later; for now, we just note that an `Index` can easily be constructed from any array of `ByteString`'s. This array is assumed to specify the names of the columns. For example, you might create an index as follows: `Index(["ColumnA", "ColumnB"])`.

In the future, we hope that there will be many different types of `DataFrame`-like constructs. But all objects that behave like a `DataFrame` will behave according to the following rules that are enforced by an `AbstractDataFrame` protocol:

* A DataFrame-like object is a table with `M` rows and `N` columns.
* Every column of a DataFrame-like object has its own type. This heterogeneity of types is the reason that a DataFrame cannot simply be represented using a matrix of `DataVec`'s.
* Each columns of a DataFrame-like object is guaranteed to have length `M`.
* Each columns of a DataFrame-like object is guaranteed to be capable of storing an `NA` value if one is ever inserted. NB: _There is ongoing debate about whether the columns of a DataFrame should always be `DataVec`'s or whether the columns should only be converted to `DataVec`'s if an `NA` is introduced by an assignment operation._

# Constructing DataFrame's

Now that you understand what a `DataFrame` is, let's build one:

	df_columns = {dvzeros(5), dvfalses(5)}
	df_colindex = Index(["A", "B"])

	df = DataFrame(df_columns, df_colindex)

In practice, many other constructors are more convenient to use than this basic one. The simplest convenience constructors is to provide only the columns, which will produce default names for all the columns.

	df = DataFrame(df_columns)

One often would like to construct `DataFrame`'s from columns which may not yet be `DataVec`'s. This is possible using the same type of constructor. All columns that are not yet `DataVec`'s will be converted to `DataVec`'s:

	df = DataFrame({ones(5), falses(5)})

Often one wishes to convert an existing matrix into a `DataFrame`. This is also possible:

	df = DataFrame(ones(5, 3))

Like `DataVec`'s, it is possible to create empty `DataFrame`'s in which all of the default values are `NA`. In the simplest version, we specify a type, the number of rows and the number of columns:

	df = DataFrame(Int64, 10, 5)

Alternatively, one can specify a `Vector` of types. This implicitly defines the number of columns, but one must still explicitly specify the number of rows:

	df = DataFrame({Int64, Float64}, 4)

When you know what the names of the columns will be, but not the values, it is possible to specify the column names at the time of construction.

_SHOULD THIS BE `DataFrame(types, nrow, names)` INSTEAD?_

	DataFrame({Int64, Float64}, ["A", "B"], 10)
	DataFrame({Int64, Float64}, Index(["A", "B"]), 10) # STILL NEED TO MAKE THIS WORK

A more uniquely Julian way of creating `DataFrame`'s exploits Julia's ability to quote `Expression`'s in order to produce behavior like R's delayed evaluation strategy.

	df = DataFrame(quote
	 				 A = rand(5)
	 				 B = dvtrues(5)
				   end)

# Accessing and Assigning Elements of DataVec's and DataFrame's

Because a DataVec is a 1-dimensional Array, indexing into it is trivial and behaves exactly like indexing into a standard Julia vector.

	dv = dvones(5)
	dv[1]
	dv[5]
	dv[end]
	dv[1:3]
	dv[[true, true, false, false, false]]

	dv[1] = 3
	dv[5] = 5.3
	dv[end] = 2.1
	dv[1:3] = [3.2, 3.2, 3.1]
	dv[[true, true, false, false, false]] = dvones(2) # SHOULD WE MAKE THIS WORK?


In contrast, a DataFrame is a random-access data structure that can be indexed into and assigned to in many different ways. We walk through many of them below.

## Simple Numeric Indexing

	df = DataFrame(Int64, 5, 3)
	df[1, 3]
	df[1]


## Range-Based Numeric Indexing

	df = DataFrame(Int64, 5, 3)

	df[1, :]
	df[:, 3]
	df[1:2, 3]
	df[1, 1:3]
	df[:, :]

## Column Name Indexing

	df["x1"]
	df[1, "x1"]
	df[1:3, "x1"]

	df[["x1", "x2"]]
	df[1, ["x1", "x2"]]
	df[1:3, ["x1", "x2"]]

# Unary Operators for NA, DataVec's and DataFrame's

In practice, we want to compute with these new types. The first requirement is to define the basic unary operators:

* `+`
* `-`
* `!`
* _MISSING: The transpose unary operator_

You can see these operators in action below:

	+NA
	-NA
	!NA

	+dvones(5)
	-dvones(5)
	!dvfalses(5)

## Binary Operators

* Arithmetic Operators:
	* Scalar Arithmetic: `+`, `-`, `*`, `/`, `^`,
	* Array Arithmetic: `+`, `.+`, `-`, `.-`, `.*`, `./`, `.^`
* Bit Operators: `&`, `|`, `$`
* Comparison Operators:
	* Scalar Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
	* Array Comparisons: `.==`, `.!=`, `.<`, `.<=`, `.>`, `.>=`

The standard arithmetic operators work on DataVec's when they interact with Number's, NA's or other DataVec's.

	dv = dvones(5)
	dv[1] = NA
	df = DataFrame(quote
					 a = 1:5
				   end)

## NA's with NA's

	NA + NA
	NA .+ NA

And so on for -, .- , *, .*, /, ./, ^, .^ 


## NA's with Scalars and Scalars with NA's

	1 + NA
	1 .+ NA
	NA + 1
	NA .+ 1

And so on for -, .- , *, .*, /, ./, ^, .^ 

## NA's with DataVec's

	dv + NA
	dv .+ NA
	NA + dv
	NA .+ dv

And so on for -, .- , *, .*, /, ./, ^, .^ 

## DataVec's with Scalars

	dv + 1
	dv .+ 1

And so on for -, .- , .*, ./, .^ 

## Scalars with DataVec's

	1 + dv
	1 .+ dv

And so on for -, .- , *, .*, /, ./, ^, .^ 

_HOW MUCH SHOULD WE HAVE OPERATIONS W/ DATAFRAMES?_

	NA + df
	df + NA
	1 + df
	df + 1
	dv + df # SHOULD THIS EXIST?
	df + dv # SHOULD THIS EXIST?
	df + df

And so on for -, .- , .*, ./, .^ 

The standard bit operators work on `DataVec`'s:

_TO BE FILLED IN_

The standard comparison operators work on `DataVec`'s:

	NA .< NA
	NA .< "a"
	NA .< 1
	NA .== dv

	dv .< NA
	dv .< "a"
	dv .< 1
	dv .== dv

	df .< NA
	df .< "a"
	df .< 1
	df .== dv # SHOULD THIS EXIST?
	df .== df

## Elementwise Functions

* `abs`
* `sign`
* `acos`
* `acosh`
* `asin`
* `asinh`
* `atan`
* `atan2`
* `atanh`
* `sin`
* `sinh`
* `cos`
* `cosh`
* `tan`
* `tanh`
* `ceil`
* `floor`
* `round`
* `trunc`
* `signif`
* `exp`
* `log`
* `log10`
* `log1p`
* `log2`
* `logb`
* `sqrt`

Standard functions that apply to scalar values of type `Number` return `NA` when applied to `NA`:

	abs(NA)

Standard functions are broadcast to the elements of `DataVec`'s and `DataFrame`'s for elementwise application:

	dv = dvones(5)
	df = DataFrame({dv})

	abs(dv)
	abs(df)

## Pairwise Functions

* `diff`

Functions that operate on pairs of entries of a `Vector` work on `DataVec`'s and insert `NA` where it would be produced by other operator rules:

	diff(dv)

## Cumulative Functions

* `cumprod`
* `cumsum`
* `cumsum_kbn`
* MISSING: `cummin`
* MISSING: `cummax`

Functions that operate cumulatively on the entries of a `Vector` work on `DataVec`'s and insert `NA` where it would be produced by other operator rules:

	cumprod(dv)
	cumsum(dv)
	cumsum_kbn(dv)

## Aggregative Functions

* `min`
* `max`
* `prod`
* `sum`
* `mean`
* `median`
* `std`
* `var`
* `fft`
* `norm`

You can see these in action:

	min(dv)

To broadcast these to individual columns, use the `col*s` versions:

* `colmins`
* `colmaxs`
* `colprods`
* `colsums`
* `colmeans`
* `colmedians`
* `colstds`
* `colvars`
* `colffts`
* `colnorms`

You can see these in action:

	colmins(df)

# Loading Standard Data Sets

The DataFrames package is easiest to explore if you also install the RDatasets package, which provides access to 570 classic data sets:

	load("RDatasets")

	iris = RDatasets.data("datasets", "iris")
	dia = RDatasets.data("ggplot2", "diamonds")

# Split-Apply-Combine

The basic mechanism for spliting data is the `groupby()` function, which will produce a `GroupedDataFrame` object that is easiest to interact with by iterating over its entries:

	for df in groupby(iris, "Species")
		println("A DataFrame with $(nrow(df)) rows")
	end

The `|` (pipe) operator for `GroupedDataFrame`'s allows you to run simple functions on the columns of the induced `DataFrame`'s. You pass a simple function by producing a symbol with its name:
 
	groupby(iris, "Species") | :mean

Another simple way to split-and-apply (without clear combining) is to use the `map()` function:

	map(df -> mean(df[1]), groupby(iris, "Species"))

# Reshaping

If you are looking for the equivalent of the R "Reshape" packages `melt()` and `cast()` functions, you can use `stack()` and `unstack()`. Note that these functions have exactly the oppposite syntax as `melt()` and `cast()`:

	stack(iris, ["Petal.Length", "Petal.Width"])

# Model Formulas

## Design

Once support for missing data and tabular data structures are in place, we need to begin to develop a version of the model formulas "syntax" used by R. In reality, it is better to regard this "syntax" as a complete domain-specific language (DSL) for describing linear models. For those unfamilar with this DSL, we show some examples below and then elaborate upon them to demonstrate ways in which Julia might move beyond R's formula system.

Let's consider the simplest sort of linear regression model: how does the height of a child depend upon the height of the child's mother and father? If we let the variable `C` denote the height of the child, `M` the height of the mother and `F` the height of the father, the standard linear model approach in statistics would try to model their relationship using the following equation: `C = a + bM + cF + epsilon`, where `a`, `b` and `c` are fixed constants and `epsilon` is a normally distributed noise term that accounts for the imperfect match between any specific child's height and the predictions based solely on the heights of that child's mother and father.

In practice, we would fit such a model using a function that performs linear regression for us based on information about the model and the data source. For example, in R we would write `lm(C ~ M + F, data = heights.data)` to fit this model, assuming that `heights.data` refers to a tabular data structure containing the heights of the children, mothers and fathers for which we have data.

If we wanted to see how the child's height depends only on the mother's height, we would write `lm(C ~ M)`. If we were concerned only about dependence on the father's height, we would write `lm(C ~ H)`. As you can see, we can perform many different statistical analyses using a very consise language for describing those analyses.

What is that language? The R formula language allows one to specify linear models by specifying the terms that should be included. The language is defined by a very small number of constructs:

* The `~` operator: The `~` operator separates the pieces of a Formula. For linear models, this means that one specifies the outputs to be predicted on the left-hand side of the `~` and the inputs to be used to make predictions on the right-hand side.
* The `+` operator: If you wish to include multiple predictors in a linear model, you use the `+` operator. To include both the columns `A` and `B` while predicting `C`, you write: `C ~ A + B`.
* The `&` operator: The `&` operator is equivalent to `:` in R. It computes interaction terms, which are really an entirely new column created by combining two existing columns. For example, `C ~ A&B` describes a linear model with only one predictor. The values of this predictor at row `i` is exactly `A[i] * B[i]`, where `*` is the standard arithmetic multiplication operation. Because of the precedence rules for Julia, it was not possible to use a `:` operator without writing a custom parser.
* The `*` operator: The `*` operator is really shorthand because `C ~ A*B` expands to `C ~ A + B + A:B`. In other words, in a DSL with only three operators, the `*` is just syntactic sugar.

In addition to these operators, the model formulas DSL typically allows us to include simple functions of single columns such as in the example, `C ~ A + log(B)`.

For Julia, this DSL will be handled by constructing an object of type `Formula`. It will be possible to generate a `Formula` using explicitly quoted expression. For example, we might write the Julian equivalent of the models above as `lm(:(C ~ M + F), heights_data)`. A `Formula` object describes how one should convert the columns of a `DataFrame` into a `ModelMatrix`, which fully specifies a linear model. [MORE DETAILS NEEDED ABOUT HOW `ModelMatrix` WORKS.]

How can Julia move beyond R? The primary improvement Julia can offer over R's model formula approach involves the use of hierarchical indexing of columns to control the inclusion of groups of columns as predictors. For example, a text regression model that uses word counts for thousands of different words as columns in a `DataFrame` might involve writing `IsSpam ~ Pronouns + Prepositions + Verbs` to exclude most words from the analysis except for those included in the `Pronouns`, `Prepositions` and `Verbs` groups. In addition, we might try to improve upon some of the tricks R provides for writing hierarchical models in which each value of a categorical predictor gets its own coefficients. This occurs, for example, in hierarchical regression models of the sort implemented by R's `lmer` function. In addition, there are plans to support multiple LHS and RHS components of a `Formula` using a `|` operator.

## Implementation

DETAILS NEEDED

# Factors

## Design

As noted above, statistical data often involves that are not quantitative, but qualitative. Such variables are typically called categorical variables and can take on only a finite number of different values. For example, a data set about people might contain demographic information such as gender or nationality for which we can know the entire set of possible values in advance. Both gender and nationality are categorical variables and should not be represented using quantitative codes unless required as this is confusing to the user and mathematically suspect since the numbering used is entirely artificial.

In general, we can require that a `Factor` type allow us to express variables that can take on a known, finite list of values. This finite list is called the levels of a `Factor`. In this sense, a `Factor` is like an enumeration type.

What makes a `Factor` more specialized than an enumeration type is that modeling tools can interpret factors using indicator variables. This is very important for specifying regression models. For example, if we run a regression in which the right-hand side includes a gender `Factor`, the regression function can replace this factor with two dummy variable columns that encode the levels of this factor. (In practice, there are additional complications because of issues of identifiability or collinearity, but we ignore those for the time being and address them in the Implementation section.)

In addition to the general `Factor` type, we might also introduce a subtype of the `Factor` type that encodes ordinal variables, which are categorical variables that encode a definite ordering such as the values, "very unhappy", "unhappy", "indifferent", "happy" and "very happy". By introducing an `OrdinalFactor` type in which the levels of this sort of ordinal factor are represented in their proper ordering, we can provide specialized functionality like ordinal logistic regression that go beyond what is possible with `Factor` types alone.

## Implementation

We have a `Factor` type that handles `NA`s. This type is currently implemented using `PooledDataVec`'s.

# DataStreams

## Specification of DataStream as an Abstract Protocol

A `DataStream` object allows one to abstractly write code that processes streaming data, which can be used for many things:

* Analysis of massive data sets that cannot fit in memory
* Online analysis in which interim answers are required while an analysis is still underway

Before we begin to discuss the use of `DataStream`'s in Julia, we need to distinguish between streaming data and online analysis:

* Streaming data involves low memory usage access to a data source. Typically, one demands that a streaming data algorithm use much less memory than would be required to simply represent the full raw data source in main memory.
* Online analysis involves computations on data for which interim answers must be available. For example, given a list of a trillion numbers, one would like to have access to the estimated mean after seeing only the first _N_ elements of this list. Online estimation is essential for building practical statistical systems that will be deployed in the wild. Online analysis is the _sine qua non_ of active learning, in which a statistical system selects which data points it will observe next.

In Julia, a `DataStream` is really an abstract protocol implemented by all subtypes of the abstract type, `AbstractDataStream`. This protocol assumes the following:

* A `DataStream` provides a connection to an immutable source of data that implements the standard iterator protocol use throughout Julia:
	 * `start(iter)`: Get initial iteration state.
	 * `next(iter, state)`: For a given iterable object and iteration state, return the current item and the next iteration state.
	 * `done(iter, state)`: Test whether we are done iterating.
* Each call to `next()` causes the `DataStream` object to read in a chunk of rows of tabular data from the streaming source and store these in a `DataFrame`. This chunk of data is called a minibatch and its maximum size is specified at the time the DataStream is created. It defaults to _1_ if no size is explicitly specified.
* All rows from the data source must use the same tabular schema. Entries may be missing, but this missingness must be represented explicitly by the `DataStream` using `NA`'s.

Ultimately, we hope to implement a variety of `DataStream` types that wrap access to many different data sources like CSV files and SQL databases. At present, have only implemented the `FileDataStream` type, which wraps access to a delimited file. In the future, we hope to implement:

* MatrixDataStream
* DataFrameDataStream
* SQLDataStream
* Other tabular data sources like Fixed Width Files

Thankfully the abstact `DataStream` protocol allows one to specify algorithms without regard for the specific type of `DataStream` being used. NB: _NoSQL databases are likely to be difficult to support because of their flexible schemas. We will need to think about how to interface with such systems in the future._

## Constructing DataStreams

The easiest way to construct a `DataStream` is to specify a filename:

	ds = DataStream("my_data_set.csv")

You can then iterate over this `DataStream` to see how things work:

	for df in ds
		print(ds)
	end

## Use Cases for DataStreams:

We can compute many useful quantities using `DataStream`'s:

* _Means: `colmeans(ds)`
* _Variances: `colvars(ds)`
* _Covariances: `cov(ds)`
* _Correlations: `cor(ds)`
* _Unique element lists and counts: _MISSING_
* _Linear models: _MISSING_
* _Entropy: _MISSING_

## Advice on Deploying DataStreams

* Many useful computations in statistics can be done online:
  * Estimation of means, including implicit estimation of means in Reinforcement Learning
  * Estimation of entropy
  * Estimation of linear regression models
* But many other computations cannot be done online because they require completing a full pass through the data before quantities can be computed exactly.
* Before writing a DataStream algorith, ask yourself: "what is the performance of this algorithm if I only allow it to make one pass through the data?"

## References

* McGregor: Crash Course on Data Stream Algorithms
* Muthukrishnan : Data Streams - Algorithms and Applications
* Chakrabarti: CS85 - Data Stream Algorithms
* Knuth: Art of Computer Programming

# Ongoing Debates about NA's

* What are the proper rules for the propagation of missingness? It is clear that there is no simple absolute rule we can follow, but we need to formulate some general principles for how to set reasonable defaults. R's strategy seems to be:
    * For operations on vectors, `NA`'s are absolutely poisonous by default.
    * For operations on `data.frames`'s, `NA`'s are absolutely poisonous on a column-by-column basis by default. This stems from a more general which assumes that most operations on `data.frame` reduce to the aggregation of the same operation performed on each column independently.
    * Every function should provide an `na.rm` option that allows one to ignore `NA`'s. Essentially this involves replacing `NA` by the identity element for that function: `sum(na.rm = TRUE)` replaces `NA`'s with `0`, while `prod(na.rm = TRUE)` replaces `NA`'s with `1`.
* Should there be multiple types of missingness?
    * For example, SAS distinguishes between:
        * Numeric missing values
        * Character missing values
        * Special numeric missing values
    * In statistical theory, while the _fact_ of missingness is simple and does not involve multiple types of `NA`'s, the _cause_ of missingness can be different for different data sets, which leads to very different procedures that can appropriately be used. See, for example, the different suggestions in Little and Rubin (2002) about how to treat data that has entries missing completely at random (MCAR) vs. data that has entries missing at random (MAR). Should we be providing tools for handling this? External data sources will almost never provide this information, but multiple dispatch means that Julian statistical functions could insure that the appropriate computations are performed for properly typed data sets without the end-user ever understanding the process that goes on under the hood.
* How is missingness different from `NaN` for `Float`'s? Both share poisonous behavior and `NaN` propagation is very efficient in modern computers. This can provide a clever method for making `NA` fast for `Float`'s, but does not apply to other types and seems potentially problematic as two different concepts are now aliased. For example, we are not uncertain about the value of `0/0` and should not allow any method to impute a value for it -- which any imputation method will do if we treat every `NaN` as equivalent to a `NA`.
* Should cleverness ever be allowed in propagation of `NA`? In section 3.3.4 of the R Language Definition, they note that in cases where the result of an operation would be the same for all possible values that an `NA` value could take on, the operation may return this constant value rather than return `NA`. For example, `FALSE & NA` returns `FALSE` while `TRUE | NA` returns `TRUE`. This sort of cleverness seems like a can-of-worms.

## Ongoing Debates about DataFrame's

* How should RDBMS-like indices be implemented? What is most efficient? How can we avoid the inefficient vector searches that R uses?
* How should `DataFrame`'s be distributed for parallel processing?

\newpage

---

# Formal Specification of DataFrames Data Structures

* Type Definitions and Type Hierarchy
* Constructors
* Indexing (Refs / Assigns)
* Operators
	* Unary Operators:
		* `+`, `-`, `!`, `'`
	* Elementary Unary Functions
		* `abs`, ...
	* Binary Operators:
		* Arithmetic Operators:
			* Scalar Arithmetic: `+`, `-`, `*`, `/`, `^`,
			* Array Arithmetic: `+`, `.+`, `-`, `.-`, `.*`, `./`, `.^`
		* Bit Operators: `&`, `|`, `$`
		* Comparison Operators:
			* Scalar Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
			* Array Comparisons: `.==`, `.!=`, `.<`, `.<=`, `.>`, `.>=`
* Container Operations
* Broadcasting / Recycling
* Type Promotion and Conversion
* String Representations
* IO
* Copying
* Properties
	* size
	* length
	* ndims
	* numel
	* eltype
* Predicates
* Handling NA's
* Iteration
* Miscellaneous

## The NAtype

### Behavior under Unary Operators

The unary operators

### Behavior under Unary Operators

The unary operators

### Behavior under Arithmetic Operators

# Constructors

* NA's
	* Constructor: `NAtype()`
	* Const alias: `NA`
* DataVec's
	* From (Vector, BitVector): `DataVec([1, 2, 3], falses(3))`
	* From (Vector, Vector{Bool}): `DataVec([1, 2, 3], [false, false, false])`
	* From (Vector): `DataVec([1, 2, 3])`
	* From (BitVector, BitVector): `DataVec(trues(3), falses(3))`
	* From (BitVector): `DataVec(trues(3))`
	* From (Range1): `DataVec(1:3)`
	* From (DataVec): `DataVec(DataVec([1, 2, 3]))`
	* From (Type, Int): `DataVec(Int64, 3)`
	* From (Int): `DataVec(3)` (Type defaults to Float64)
	* From (): `DataVec()` (Type defaults to Float64, length defaults to 0)
	* Initialized with Float64 zeros: `dvzeros(3)`
	* Initialized with typed zeros: `dvzeros(Int64, 3)`
	* Initialized with Float64 ones: `dvones(3)`
	* Initialized with typed ones: `dvones(Int64, 3)`
	* Initialized with falses: `dvfalses(3)`
	* Initialized with trues: `dvtrues(3)`
	* Literal syntax: `DataVec[1, 2, NA]`
* PooledDataVec's
	* From (Vector, BitVector): `PooledDataVec([1, 2, 3], falses(3))`
	* From (Vector, Vector{Bool}): `PooledDataVec([1, 2, 3], [false, false, false])`
	* From (Vector): `PooledDataVec([1, 2, 3])`
	* From (BitVector, BitVector): `PooledDataVec(trues(3), falses(3))`
	* From (BitVector, Vector{Bool}): `PooledDataVec(trues(3), [false, false, false])`
	* From (BitVector): `PooledDataVec(trues(3))`
	* From (Range1): `PooledDataVec(1:3)`
	* From (DataVec): `PooledDataVec(DataVec([1, 2, 3]))`
	* From (Type, Int): `PooledDataVec(Int64, 3)`
	* From (Int): `PooledDataVec(3)` (Type defaults to Float64)
	* From (): `PooledDataVec()` (Type defaults to Float64, length defaults to 0)
	* Initialized with Float64 zeros: `pdvzeros(3)`
	* Initialized with typed zeros: `pdvzeros(Int64, 3)`
	* Initialized with Float64 ones: `pdvones(3)`
	* Initialized with typed ones: `pdvones(Int64, 3)`
	* Initialized with falses: `pdvfalses(3)`
	* Initialized with trues: `pdvtrues(3)`
	* Literal syntax: `PooledDataVec[1, 2, NA]`
* DataMatrix
	* From (Array, BitArray): `DataMatrix([1 2; 3 4], falses(2, 2))`
	* From (Array, Array{Bool}): `DataMatrix([1 2; 3 4], [false false; false false])`
	* From (Array): `DataMatrix([1 2; 3 4])`
	* From (BitArray, BitArray): `DataMatrix(trues(2, 2), falses(2, 2))`
	* From (BitArray): `DataMatrix(trues(2, 2))`
	* From (DataVec...): `DataMatrix(DataVec[1, NA], DataVec[NA, 2])`
	* From (Range1...): `DataMatrix(1:3, 1:3)`
	* From (DataMatrix): `DataMatrix(DataVec([1 2; 3 4]))`
	* From (Type, Int, Int): `DataMatrix(Int64, 2, 2)`
	* From (Int, Int): `DataMatrix(2, 2)` (Type defaults to Float64)
	* From (): `DataMatrix()` (Type defaults to Float64, length defaults to (0, 0))
	* Initialized with Float64 zeros: `dmzeros(2, 2)`
	* Initialized with typed zeros: `dmzeros(Int64, 2, 2)`
	* Initialized with Float64 ones: `dmones(2, 2)`
	* Initialized with typed ones: `dmones(Int64, 2, 2)`
	* Initialized with falses: `dmfalses(2, 2)`
	* Initialized with trues: `dmtrues(2, 2)`
	* Initialized identity matrix: `dmeye(2, 2)`
	* Initialized identity matrix: `dmeye(2)`
	* Initialized diagonal matrix: `dmdiagm([2, 1])`
	* Literal syntax: `DataMatrix[1 2; NA 2]`
* DataFrame
	* From (): `DataFrame()`
	* From (Vector{Any}, Index): `DataFrame({dvzeros(3), dvones(3)}, Index(["A", "B"]))`
	* From (Vector{Any}): `DataFrame({dvzeros(3), dvones(3)})
	* From (Expr): `DataFrame(quote A = [1, 2, 3, 4] end)`
	* From (Matrix, Vector{String}): `DataFrame([1 2; 3 4], ["A", "B"])`
	* From (Matrix): `DataFrame([1 2; 3 4])`
	* From (Tuple): `DataFrame(dvones(2), dzfalses(2))`
	* From (Associative): ???
	* From (Vector, Vector, Groupings): ???
	* From (Dict of Vectors): `DataFrame({"A" => [1, 3], "B" => [2, 4]})`
	* From (Dict of Vectors, Vector{String}): `DataFrame({"A" => [1, 3], "B" => [2, 4]}, ["A"])`
	* From (Type, Int, Int): `DataFrame(Int64, 2, 2)`
	* From (Int, Int): `DataFrame(2, 2)`
	* From (Vector{Types}, Vector{String}, Int): `DataFrame({Int64, Float64}, ["A", "B"], 2)`
	* From (Vector{Types}, Int): `DataFrame({Int64, Float64}, 2)`

# Indexing

NA

dv = dvzeros(10)

dv[1]

dv[1:2]

dv[:]

dv[[1, 2 3]]

dv[[false, false, true, false, false]]

dmzeros(10)

Indexers: Int, Range, Colon, Vector{Int}, Vector{Bool}, String, Vector{String}

DataVec's and PooledDataVec's implement:

* Int
* Range
* Colon
* Vector{Int}
* Vector{Bool}

DataMatrix's implement the Cartesian product:

* Int, Int
* Int, Range
* Int, Colon
* Int, Vector{Int}
* Int, Vector{Bool}
...
* Vector{Bool}, Int
* Vector{Bool}, Range
* Vector{Bool}, Colon
* Vector{Bool}, Vector{Int}
* Vector{Bool}, Vector{Bool}

Single Int access?

DataFrame's add two new indexer types:

* String
* Vector{String}

These can only occur as (a) the only indexer or (b) in the second slot of a paired indexer

Anything that can be ref()'d can also be assign()'d

Where do we allow Expr indexing?

\newpage

---

# Function Reference Guide

## DataFrames

#### `DataFrame(cols::Vector, colnames::Vector{ByteString})`

Construct a DataFrame from the columns given by `cols` with the index
generated by `colnames`. A DataFrame inherits from
`Associative{Any,Any}`, so Associative operations should work. Columns
are vector-like objects. Normally these are AbstractDataVecs (DataVecs
or PooledDataVecs), but they can also (currently) include standard
Julia Vectors.

#### `DataFrame(cols::Vector)`

Construct a DataFrame from the columns given by `cols` with default
column names.

#### `DataFrame()`

An empty DataFrame.

#### `copy(df::DataFrame)`

A shallow copy of `df`. Columns are referenced, not copied.

#### `deepcopy(df::DataFrame)`

A deep copy of `df`. Copies of each column are made.

#### `similar(df::DataFrame, nrow)`

A new DataFrame with `nrow` rows and the same column names and types as `df`. 


### Basics

#### `size(df)`, `ndims(df)`

Same meanings as for Arrays.

#### `has(df, key)`, `get(df, key, default)`, `keys(df)`, and `values(df)`

Same meanings as Associative operations. `keys` are column names;
`values` are column contents.

#### `start(df)`, `done(df,i)`, and `next(df,i)`

Methods to iterate over columns.

#### `ncol(df::AbstractDataFrame)`

Number of columns in `df`.

#### `nrow(df::AbstractDataFrame)`

Number of rows in `df`.

#### `length(df::AbstractDataFrame)` or `numel(df::AbstractDataFrame)`

Number of columns in `df`.

#### `isempty(df::AbstractDataFrame)`

Whether the number of columns equals zero.

#### `head(df::AbstractDataFrame)` and `head(df::AbstractDataFrame, i::Int)` 

First `i` rows of `df`. Defaults to 6.

#### `tail(df::AbstractDataFrame)` and `tail(df::AbstractDataFrame, i::Int)` 

Last `i` rows of `df`. Defaults to 6.

#### `show(io, df::AbstractDataFrame)`

Standard pretty-printer of `df`. Called by `print()` and the REPL.

#### `dump(df::AbstractDataFrame)`

Show the structure of `df`. Like R's `str`.

#### `summary(df::AbstractDataFrame)`

Show a summary of each column of `df`.

#### `complete_cases(df::AbstractDataFrame)`

A Vector{Bool} of indexes of complete cases in `df` (rows with no
NA's).

#### `duplicated(df::AbstractDataFrame)`

A Vector{Bool} of indexes indicating rows that are duplicates of prior
rows.

#### `unique(df::AbstractDataFrame)`

DataFrame with unique rows in `df`.


### Indexing, Assignment, and Concatenation

DataFrames are indexed like a Matrix and like an Associative. Columns
may be indexed by column name. Rows do not have names. Referencing
with one argument normally indexes by columns: `df["col"]`,
`df[["col1","col3"]]` or `df[i]`. With two arguments, rows and columns
are selected. Indexing along rows works like Matrix indexing. Indexing
along columns works like Matrix indexing with the addition of column
name access. 

#### `ref(df::DataFrame, ind)`  or `df[ind]`

Returns a subset of the columns of `df` as specified by `ind`, which
may be an `Int`, a `Range`, a `Vector{Int}`, `ByteString`, or
`Vector{ByteString}`. Columns are referenced, not copied. For a
single-element `ind`, the column by itself is returned.

#### `ref(df::DataFrame, irow, icol)`  or `df[irow,icol]`

Returns a subset of `df` as specified by `irow` and `icol`. `irow` may
be an `Int`, a `Range`, or a `Vector{Int}`. `icol` may be an `Int`, a
`Range`, or a `Vector{Int}`, `ByteString`, or, `ByteString`, or
`Vector{ByteString}`. For a single-element `ind`, the column subset by
itself is returned.

#### `index(df::DataFrame)`

Returns the column `Index` for `df`.

#### `set_group(df::DataFrame, newgroup, names::Vector{ByteString})`
#### `get_groups(df::DataFrame)`
#### `set_groups(df::DataFrame, gr::Dict)`

See the Indexing section for these operations on column indexes.

#### `colnames(df::DataFrame)` or `names(df::DataFrame)` 

The column names as an `Array{ByteString}`

#### `assign(df::DataFrame, newcol, colname)` or `df[colname] = newcol`

Replace or add a new column with name `colname` and contents `newcol`.
Arrays are converted to DataVecs. Values are recycled to match the
number of rows in `df`.

#### `insert(df::DataFrame, index::Integer, item, name)`

Insert a column of name `name` and with contents `item` into `df` at
position `index`.

#### `insert(df::DataFrame, df2::DataFrame)`

Insert columns of `df2` into `df1`.

#### `del!(df::DataFrame, cols)`

Delete columns in `df` at positions given by `cols` (noted with any
means that columns can be referenced).

#### `del(df::DataFrame, cols)`

Nondestructive version. Return a DataFrame based on the columns in
`df` after deleting columns specified by `cols`.

#### `cbind(df1, df2, ...)` or `hcat(df1, df2, ...)` or `[df1 df2 ...]`  

Concatenate columns. Duplicated column names are adjusted.

#### `rbind(df1, df2, ...)` or `vcat(df1, df2, ...)` or `[df1, df2, ...]`  

Concatenate rows.

### I/O

#### `csvDataFrame(filename, o::Options)`

Return a DataFrame from file `filename`. Options `o` include
`colnames` [`"true"`, `"false"`, or `"check"` (the default)] and
`poolstrings` [`"check"` (default) or `"never"`].

### Expression/Function Evaluation in a DataFrame

#### `with(df::AbstractDataFrame, ex::Expr)`

Evaluate expression `ex` with the columns in `df`.

#### `within(df::AbstractDataFrame, ex::Expr)`

Return a copy of `df` after evaluating expression `ex` with the
columns in `df`.

#### `within!(df::AbstractDataFrame, ex::Expr)`

Modify `df` by evaluating expression `ex` with the columns in `df`.

#### `based_on(df::AbstractDataFrame, ex::Expr)`

Return a new DataFrame based on evaluating expression `ex` with the
columns in `df`. Often used for summarizing operations.

#### `colwise(f::Function, df::AbstractDataFrame)`
#### `colwise(f::Vector{Function}, df::AbstractDataFrame)`

Apply `f` to each column of `df`, and return the results as an
Array{Any}.

#### `colwise(df::AbstractDataFrame, s::Symbol)`
#### `colwise(df::AbstractDataFrame, s::Vector{Symbol})`

Apply the function specified by Symbol `s` to each column of `df`, and
return the results as a DataFrame.

### SubDataFrames

#### `sub(df::DataFrame, r, c)`
#### `sub(df::DataFrame, r)`

Return a SubDataFrame with references to rows and columns of `df`.


#### `sub(sd::SubDataFrame, r, c)`
#### `sub(sd::SubDataFrame, r)`

Return a SubDataFrame with references to rows and columns of `df`.

#### `ref(sd::SubDataFrame, r, c)` or `sd[r,c]`
#### `ref(sd::SubDataFrame, c)` or `sd[c]`

Referencing should work the same as DataFrames.


### Grouping

#### `groupby(df::AbstractDataFrame, cols)`

Return a GroupedDataFrame based on unique groupings indicated by the
columns with one or more names given in `cols`.

#### `start(gd)`, `done(gd,i)`, and `next(gd,i)`

Methods to iterate over GroupedDataFrame groupings.

#### `ref(gd::GroupedDataFrame, idx)` or `gd[idx]`

Reference a particular grouping. Referencing returns a SubDataFrame.

#### `with(gd::GroupedDataFrame, ex::Expr)`

Evaluate expression `ex` with the columns in `gd` in each grouping.

#### `within(gd::GroupedDataFrame, ex::Expr)`
#### `within!(gd::GroupedDataFrame, ex::Expr)`

Return a DataFrame with the results of evaluating expression `ex` with
the columns in `gd` in each grouping.

#### `based_on(gd::GroupedDataFrame, ex::Expr)`

Sweeps along groups and applies `based_on` to each group. Returns a
DataFrame.

#### `map(f::Function, gd::GroupedDataFrame)`

Apply `f` to each grouping of `gd` and return the results in an Array.

#### `colwise(f::Function, gd::GroupedDataFrame)`
#### `colwise(f::Vector{Function}, gd::GroupedDataFrame)`

Apply `f` to each column in each grouping of `gd`, and return the
results as an Array{Any}.

#### `colwise(gd::GroupedDataFrame, s::Symbol)`
#### `colwise(gd::GroupedDataFrame, s::Vector{Symbol})`

Apply the function specified by Symbol `s` to each column of in each
grouping of `gd`, and return the results as a DataFrame.

#### `by(df::AbstractDataFrame, cols, s::Symbol)` or `groupby(df, cols) | s`
#### `by(df::AbstractDataFrame, cols, s::Vector{Symbol})`

Return a DataFrame with the results of grouping on `cols` and
`colwise` evaluation based on `s`. Equivalent to `colwise(groupby(df,
cols), s)`.

#### `by(df::AbstractDataFrame, cols, e::Expr)` or `groupby(df, cols) | e`

Return a DataFrame with the results of grouping on `cols` and
evaluation of `e` in each grouping. Equivalent to `based_on(groupby(df,
cols), e)`.

### Reshaping / Merge

#### `stack(df::DataFrame, cols)`

For conversion from wide to long format. Returns a DataFrame with
stacked columns indicated by `cols`. The result has column `"key"`
with column names from `df` and column `"value"` with the values from
`df`. Columns in `df` not included in `cols` are duplicated along the
stack.

#### `unstack(df::DataFrame, ikey, ivalue, irefkey)`

For conversion from long to wide format. Returns a DataFrame. `ikey`
indicates the key column--unique values in column `ikey` will be
column names in the result. `ivalue` indicates the value column.
`irefkey` is the column with a unique identifier for that . Columns
not given by `ikey`, `ivalue`, or `irefkey` are currently ignored.

#### `merge(df1::DataFrame, df2::DataFrame, bycol)`
#### `merge(df1::DataFrame, df2::DataFrame, bycol, jointype)`

Return the database join of `df1` and `df2` based on the column `bycol`.
Currently only a single merge key is supported. Supports `jointype` of
"inner" (the default), "left", "right", or "outer".


## Index

#### `Index()`
#### `Index(s::Vector{ByteString})`

An Index with names `s`. An Index is like an Associative type. An
Index is used for column indexing of DataFrames. An Index maps
ByteStrings and Vector{ByteStrings} to Indices.

#### `length(x::Index)`, `copy(x::Index)`, `has(x::Index, key)`, `keys(x::Index)`, `push(x::Index, name)`

Normal meanings.

#### `del(x::Index, idx::Integer)`,  `del(x::Index, s::ByteString)`,  

Delete the name `s` or name at position `idx` in `x`.

#### `names(x::Index)`

A Vector{ByteString} with the names of `x`.

#### `names!(x::Index, nm::Vector{ByteString})`

Set names `nm` in `x`.

#### `replace_names(x::Index, from::Vector, to::Vector)`

Replace names `from` with `to` in `x`.

#### `ref(x::Index, idx)` or `x[idx]`

This does the mapping from name(s) to Indices (positions). `idx` may
be ByteString, Vector{ByteString}, Int, Vector{Int}, Range{Int},
Vector{Bool}, AbstractDataVec{Bool}, or AbstractDataVec{Int}.

#### `set_group(idx::Index, newgroup, names::Vector{ByteString})`

Add a group to `idx` with name `newgroup` that includes the names in
the vector `names`.  

#### `get_groups(idx::Index)`

A Dict that maps the name of each group to the names in the group.

#### `set_groups(idx::Index, gr::Dict)`

Set groups in `idx` based on the mapping given by `gr`.


## Missing Values

Missing value behavior is implemented by instantiations of the `AbstractDataVec`
abstract type. 

#### `NA`

A constant indicating a missing value.
  
#### `isna(x)`

Return a `Bool` or `Array{Bool}` (if `x` is an `AbstractDataVec`)
that is `true` for elements with missing values.

#### `nafilter(x)`

Return a copy of `x` after removing missing values. 

#### `nareplace(x, val)`

Return a copy of `x` after replacing missing values with `val`.

#### `naFilter(x)`

Return an object based on `x` such that future operations like `mean`
will not include missing values. This can be an iterator or other
object.

#### `naReplace(x, val)`

Return an object based on `x` such that future operations like `mean`
will replace NAs with `val`.

#### `na(x)`

Return an `NA` value appropriate for the type of `x`.

#### `nas(x, dim)`

Return an object like `x` filled with `NA`'s with size `dim`.


## DataVecs

#### `DataVec(x::Vector)`
#### `DataVec(x::Vector, m::Vector{Bool})`

Create a DataVec from `x`, with `m` optionally indicating which values
are NA. DataVecs are like Julia Vectors with support for NA's. `x` may
be any type of Vector.

#### `PooledDataVec(x::Vector)`
#### `PooledDataVec(x::Vector, m::Vector{Bool})`

Create a PooledDataVec from `x`, with `m` optionally indicating which
values are NA. PooledDataVecs contain a pool of values with references
to those values. This is useful in a similar manner to an R array of
factors.

#### `size`, `length`, `ndims`, `ref`, `assign`, `start`, `next`, `done`

All normal Vector operations including array referencing should work.

#### `isna(x)`, `nafilter(x)`, `nareplace(x, val)`, `naFilter(x)`, `naReplace(x, val)`

All NA-related methods are supported.

## Utilities

#### `cut(x::Vector, breaks::Vector)`

Returns a PooledDataVec with length equal to `x` that divides values in `x`
based on the divisions given by `breaks`.

## Formulas and Models

#### `Formula(ex::Expr)`

Return a Formula object based on `ex`. Formulas are two-sided
expressions separated by `~`, like `:(y ~ w*x + z + i&v)`.

#### `model_frame(f::Formula, d::AbstractDataFrame)`
#### `model_frame(ex::Expr, d::AbstractDataFrame)`

A ModelFrame.

#### `model_matrix(mf::ModelFrame)`
#### `model_matrix(f::Formula, d::AbstractDataFrame)`
#### `model_matrix(ex::Expr, d::AbstractDataFrame)`

A ModelMatrix based on `mf`, `f` and `d`, or `ex` and `d`.

#### `lm(ex::Expr, df::AbstractDataFrame)`

Linear model results (type OLSResults) based on formula `ex` and `df`.
