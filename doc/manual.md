# Introduction

This manual is meant to introduce new users to the DataFrame package for Julia, which provides the basic data structures needed for real-world data analysis. In everything that follows, we'll assume that you've installed and loaded the DataFrames package using the following commands:

	require("pkg")
	Pkg.add("DataFrames")

	load("DataFrames")
	using DataFrames

 Overview of Basic Types for Working with Data

# The NA Type

The core problem with using the data structures built into Julia for data analysis is that there is no mechanism for expressing the absence of data. Traditional database systems express the absence of data using a `NULL` value, while data analysis packages typically follow the tradition set by S and use `NA` for this purpose.

The DataFrames package expresses the absence of data by introducing a new type called `NAtype`. This value is used everywhere to indicate missingness in the underlying data set.

To see this value, you can type

	NAtype

in the Julia REPL. You can learn more about the nature of this new type using standard Julia functions for navigating Julia's type system:

	typeof(NAtype)
	super(NAtype)
	dump(NAtype)

While the `NAtype` provides the essential type needed to express missingness, the practical way that missing data is denoted using a special constant `NA`, which is an instance of `NAtype`:

	NA
	NAtype()

You can explore this value to confirm that `NA` is just an instance of the `NAtype`:

	typeof(NA)
	dump(NA)

Simply being able to express the notion that a data point is missing is important, but we're ultimately not interested in just expressing data: we want to build tools for interacting with data that may be missing. In a later section, we'll describe the details of interacting with `NA`, but for now we'll state the defining property of `NA`: because it expresses ignorance about the value of something, every interaction with `NA` corrupts known values and transforms them into `NA`'s:

	1 + NA

We'll discuss the subtleties of `NA`'s ability to corrupt known values in a later section. For now the essential point is this: `NA`'s exist to represent missingness that occurs in scalar data.

# The DataVec Type

To express the notion that complex data structure like an `Array` contains missing entries, we need to construct a new data structure that can contain standard Julia values like `Float64` while also allowing the presence of `NA` values.

Of course, a Julian `Array{Any}` would allow us to do this:

	{1, NA}

But consistently using `Any` arrays would make Julia much less efficient. Instead, we want to provide a new data structure that parallels a standard Julia `Array`, while allowing exactly one additional value: `NA`.

This new data structure is the `DataVec` type:

	DataVec[1, NA, 3]

As you'll see when entering this into the REPL, this snippet of code creates a `3-element DataVec{Int64}`. This type of `DataVec` can store `Int64` values or an `NA`.

This is achieved by a very simple mechanism: a `DataVec{T}` is a new parametric composite type that we've added to Julia that wraps around a standard Julia `Vector` and complements this basic vector with a metadata store that indicates whether any entry of the wrapped vector is missing. In essence, a `DataVec` of type `T` is defined as:

	type DataVec{T}
		data::Vector{T}
		na::BitVector{Bool}
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

But let's focus on ways that you can use these `DataVec`'s. The simplest possible constructor requires the end-user to directly specify both the underlying data values and the missingness metadata as a `BitVector`:

	dv = DataVec([1, 2, 3], falses(3))

This is rather ugly, so we've defined many additional constructors that make it easier to create a new `DataVec`. The first simplification is to ignore the distinction between a `BitVector{Bool}` and an `Array{Bool, 1}` by allowing users to specify `Bool` values directly:

	dv = DataVec([1, 2, 3], [false, false, false])

In practice, this is still a lot of useless typing when all of the values of the new `DataVec` are not missing. In that case, you can just pass a Julian vector:

	dv = DataVec([1, 2, 3])

When the values you wish to store in a `DataVec` are sequential, you can cut down even further on typing by using a Julian `Range`:

	dv = DataVec(1:3)

In contrast, when some of the values in the new `DataVec` are missing, we've constructed a very special constructor:

	dv = DataVec[1, 2, NA, 4]

_Technical Note_: This special type of constructor is defined by defining the `ref()` function to apply to values of type `DataVec`.

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

`DataVec`'s created in this way have `NA` in all entries. If you instead wish to initialize a `DataVec` with standard initial values, you can several functions:

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

# The DataFrame Type

While `DataVec`'s are a very powerful tool for dealing with missing data, they only bring us part of the way towards representing real-world data in Julia. The final missing data structure is a simple tabular data structure like those used in relational databases or spreadsheet software.

To represent these kinds of tabular data sets, the DataFrames package provides the `DataFrame` type. The `DataFrame` type is a new Julian composite type with just two fields:

* `columns`: A Julia `Vector{Any}`, each element of which will be a single column of the tabular data. The typical column is of type `DataVec{T}`, but this is not currently enforced.
* `colindex`: An `Index` object that allows one to access entries in the columns using both numeric indexing (like a standard `Array`) or key-valued indexing (like a standard `Dict`). The details of the `Inded` type will be described later; for now, we just note that an `Index` can easily be constructed from any array of `ByteString`'s that specify the names of the columns.

In practice, there are many different types of `DataFrame`-like constructs. But all objects that behave like a `DataFrame` will behave according to the following rules:

* A DataFrame-like object is a table with `M` rows and `N` columns.
* Every column of a DataFrame-like object has its own type. This heterogeneity of types is the reason that a DataFrame cannot simply be represented using a matrix of `DataVec`'s.
* Each columns of a DataFrame-like object is guaranteed to have length `M`.
* Each columns of a DataFrame-like object is guaranteed to be capable of storing an `NA` value if one is ever inserted. NB: _There is ongoing debate about whether the columns of a DataFrame should always be `DataVec`'s or whether the columns should only be converted to `DataVec`'s if an `NA` is introduced by an assignment operation.

	df_columns = {dvzeros(5), dvfalses(5)}
	df_colindex = Index(["A", "B"])

	df = DataFrame(df_columns, df_colindex)

In practice, many other constructors are more convenient to use. The simplest is to provide only the columns, which will produce default names for all the columns.

	df = DataFrame(df_columns)

One often would like to construct DataFrame's from columns which may not yet be DataVec's. This is possible using the same type of constructor. All columns that are not yet DataVec's will be converted to DataVec's. (THIS LAST CLAIM IS NOT YET TRUE AND TOM DOES NOT WANT IT.)


	df = DataFrame({ones(5), falses(5)})

Often one wishes to convert an existing matrix into a DataFrame. This is also possible.

	df = DataFrame(ones(5, 3))

Like DataVec's, it is possible to create empty DataFrame's in which all of the default values are NA.

In the simplest version, we specify a type, the number of rows and the number of columns.

	df = DataFrame(Int64, 10, 5)

Alternatively, one can specify a Vector of types. This implicitly defines the number of columns, but one must still specify the number of rows.

	df = DataFrame({Int64, Float64}, 4)

When you know what the names of the columns will be, but not the values it is possible to specify them as well. SHOULD THIS BE DataFrame(types, nrow, names) INSTEAD?


	DataFrame({Int64, Float64}, ["A", "B"], 10)
	DataFrame({Int64, Float64}, Index(["A", "B"]), 10) NEED TO MAKE THIS WORK

A more uniquely Julian way of creating DataFrame's exploits Julia's ability to quote Expressions in order to produce behavior like R's delayed evaluation strategy.


	df = DataFrame(quote
	 				 A = rand(5)
	 				 B = dvtrues(5)
				   end)


## Accessing and Assigning Elements of DataVec's and DataFrame's

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


In contrast, a DataFrame is a random-access data structure that can be indexing into and assigned to in many different ways. We walk through them one-by-one below.

### Simple Numeric Indexing

	df = DataFrame(Int64, 5, 3)
	df[1, 3]
	df[1]


### Range-Based Numeric Indexing

	df = DataFrame(Int64, 5, 3)

	df[1, :]
	df[:, 3]
	df[1:2, 3]
	df[1, 1:3]
	df[:, :]

### Column Name Indexing

	df["x1"]
	df[1, "x1"]
	df[1:3, "x1"]

	df[["x1", "x2"]]
	df[1, ["x1", "x2"]]
	df[1:3, ["x1", "x2"]]

## Unary Operators for NA, DataVec's and DataFrame's

* `+`
* `-`
* `!`
* MISSING: The transpose unary operator

You can see these in action:

	+NA
	-NA
	!NA

	+dvones(5)
	-dvones(5)
	!dvfalses(5)

## Binary Operators

* Arithmetic Operators: +, .+, -, .-, *, .*, /, ./, ^, .^
* Bit Operators: MISSING
* Comparison Operators: ==, !=, <, <=, >, >=, .==, .!=, .<, .<=, .>, .>=

The standard arithmetic operators work on DataVec's when they interact with Number's, NA's or other DataVec's.

	dv = dvones(5)
	dv[1] = NA
	df = DataFrame(quote
					 a = 1:5
				   end)

### NA's with NA's

	NA + NA
	NA .+ NA

And so on for -, .- , *, .*, /, ./, ^, .^ 


### NA's with Scalars and Scalars with NA's

	1 + NA
	1 .+ NA
	NA + 1
	NA .+ 1

And so on for -, .- , *, .*, /, ./, ^, .^ 

### NA's with DataVec's

	dv + NA
	dv .+ NA
	NA + dv
	NA .+ dv

And so on for -, .- , *, .*, /, ./, ^, .^ 

### DataVec's with Scalars

	dv + 1
	dv .+ 1

And so on for -, .- , .*, ./, .^ 

### Scalars with DataVec's

	1 + dv
	1 .+ dv

And so on for -, .- , *, .*, /, ./, ^, .^ 

HOW MUCH SHOULD WE HAVE OPERATIONS W/ DATAFRAMES?

	NA + df
	df + NA
	1 + df
	df + 1
	dv + df # SHOULD THIS EXIST?
	df + dv # SHOULD THIS EXIST?
	df + df

And so on for -, .- , .*, ./, .^ 

The standard bit operators work on DataVec's: MISSING

The standard comparison operators work on DataVec's:

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

Standard functions that apply to scalar values of type Number return NA when applied to NA:

	abs(NA)

Standard functions are broadcast to the elements of DataVec's and DataFrame's for elementwise application:

	dv = dvones(5)
	df = DataFrame({dv})

	abs(dv)
	abs(df)

## Pairwise Functions

* `diff`

Functions that operate on pairs of entries of a Vector work on DataVec's and insert NA where it would be produced by other operator rules:

	diff(dv)

## Cumulative Functions

* `cumprod`
* `cumsum`
* `cumsum_kbn`
* MISSING: `cummin`
* MISSING: `cummax`

Functions that operate cumulatively on the entries of a Vector work on DataVec's and insert NA where it would be produced by other operator rules:

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

	colmins(df)

# Loading Standard Data Sets

The DataFrames package is easiest to explore if you also install the RDatasets package, which provides access to 570 classic data sets:

	load("RDatasets")

	iris = RDatasets.data("datasets", "iris")
	dia = RDatasets.data("ggplot2", "diamonds")

# Split-Apply-Combine

The basic mechanism for spliting data is the groupby() function, which will produce a GroupedDataFrame() object that is easiest to interact with by iterating over its entries:

	for df in groupby(iris, "Species")
		println("A DataFrame with $(nrow(df)) rows")
	end

The `|` (pipe) operator for GroupedDataFrame's allows you to run simple functions on the columns of the induced DataFrame's. You pass a simple function by producing a symbol with its name:
 
	groupby(iris, "Species") | :mean

Another simple way to split-and-apply (without clear combining) is to use the map() function:

	map(df -> mean(df[1]), groupby(iris, "Species"))

# Reshaping

If you are looking for the equivalent of the R "Reshape" packages melt() and cast() functions, you can use stack() and unstack(). Note that these functions have exactly the oppposite syntax as melt() and cast():

	stack(iris, ["Petal.Length", "Petal.Width"])
