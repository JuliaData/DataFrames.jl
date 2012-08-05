# Statistical Programming in Julia: The DataFrames Project

===

# Introduction

We want to use Julia for statistical programming. Julia is already a very powerful tool for general mathematical programming, but it is missing several features that are essential for statistical programming.

To explain why these missing features are essential and to motivate the changes that need to be made to Julia to make up for their absence (which we believe are possible using only modules/packages rather than changes to the core language), we can consider several examples of increasing complexity that involve calculating means:

* _Case 1_: We want to calculate the mean of a list of 5 numbers: `x1`, `x2`, `x3`, `x4` and `x5`. We can represent these numbers as a vector `v = [x1, x2, x3, x4, x5]`. We can compute the mean of `v` in Julia using the `mean()` function from `base/statistics.jl` by calling `mean(v)`. For this sort of computation, Julia shines.
* _Case 2_: The approach taken above works well unless one or more of the values `x1` through `x5` is missing. Most real world data sets contain missing values, but the approach for computing means used above has no ability to handle even a single missing value. There are two problems:
    * None of the default numeric types in Julia that could be used to encode the values `x1` through `x5` provide any mechanism for encoding missingness.
    * The function `mean()` used above would not know how to handle missing data points even if the values `x1` through `x5` were capable of expressing missingness.
* _Case 3_: Even if we introduce new types for expressing missingness and overload functions like `mean()` to handle missing data, we still have no tools for calculating the mean of some variable `z` conditional on the values of two other variables `x` and `y`. This more general problem of calculating means conditional on other information is called regression and is one of the core analytic tools provided by statistical theory. To express conditional means, we need two things: a data structure in which we can store the values of several different variables simultaneously and a method for performing regressions by specifying which variables are being conditioned on. The data structure we want to use in order to solve the first problem is a tabular, cases-by-variables representation of data that is ubiquitous in statistical computing and is largely equivalent to the concept of a relational database. In order to solve the latter problem, we want to have a type of domain-specific language called model formulas that allows us to concisely specify the variable(s) we want to predict and the variable(s) we want to use to perform predictions. This syntax is as valuable for statistical programming as regular expressions are for processing string.

From these example, we see that performing statistical computation in Julia will require abstract methods for dealing with all of the following:

* Missing Data
* Tabular Data Structures
* Model Formulas

While explaining how tabular data and model formulas should work in greater detail, we'll see that we also need one more change to Julia: a `Factor` type like that found in R. A `Factor` type is valuable for two reasons:

* A `Factor` type can reduce the amount of memory required to represent data by representing a finite set of values using an efficient encoding that is expanded into full values only when necessary. Essentially this means that `Factor`'s behave like enumeration types and that they perform a role similar to database normalization.
* A `Factor` type can be used to inform a statistical algorithm that an input or output variable is categorical. This is particularly important when specifying regression models in which a single categorical variable encoded using `Factor`'s will be replaced with an entire matrix of indicator variables that allows pure numerical computation to be done to estimate a regression model. This conversion of categorical data into numerical data is among the most important tasks that a statistical system performs since it is error-prone if the programmer performs it for themselves each time.

With that overview of our four basic additions to Julia in mind, we now proceed to introduce them in greater detail. We then describe our current design, our current implementations of this design and our long-term goals.

# Missing Data

## Design

As noted above, the most basic piece of statistical programming functionality that Julia is missing is an ability to explicitly represent data that suffers from missingness. A Julian `Int64`, for example, cannot represent missingness: a numeric value simply must be present for every element of a Julian `Vector` of `Int64`'s.

In the abstract, we can handle this by complementing every existing type available in Julia with a paired type that handle missingness. If, for example, `MyType` can encode the values "A", "B" and "C", then "MyTypeAugmented" should be able to encode the values "A", "B", "C" and `NA`, where `NA` is a special value that indicates missingness and is not a value that "MyType" could ever take on. This augmentation procedure could be done for every single type in Julia and we would then have a version of Julia that handles missingness appropriately.

But we need to specify substantially more than methods for encoding missingness, we also need to specify methods for working with data that suffers from missingness. As a starting example, suppose that the value of a variable `x` is missing and we attempt to compute `x + 2`. What should the answer be?

Most statistical enviroments, including R, view `NA` as poisonous: if we are uncertain of the value of `x`, then we should also be uncertain of the value of `x + 2`, which is there set to `NA`. Because uncertainty is poisonous, we need to redefine the addition operation in Julia on our augmented type system to insure that `NA`'s poison the results of all downstream computations and produce `NA`'s.

Unfortunately, dealing with `NA`'s is actually more complex than this simple poisoning rule suggests: most statistical systems do not view `NA` as absolutely poisonous. While `NA + 2` produces `NA`, it is typically possible to instruct more complex functions like `mean()` to ignore `NA` values. In short, the treatment of `NA` must be handled on a case-by-base basis, although there are recurring patterns that we can exploit. In practice, we will need to provide all of the important types of behavior and then set reasonable defaults.

## Implementation

* One approach to providing `NA` support in Julia involves implementing everything using `AbstractArray`'s of `Union` types. This approach is feasible in Julia and could, in principle, be extended to any types we wish to augment with `NA` by using macros. But this approach might be quite inefficient.
* Another different approach is to use appropriate `BitsType`'s to encode missingness. If done appropriately, this could plausibly be much more efficient than the `Union` type, but seems to be more complex to get right.

## Ongoing Debates

* What are the proper rules for the propagation of missingness? It is clear that there is no simple absolute rule we can follow, but we need to formulate some general principles for how to set reasonable defaults. R's strategy seems to be:
    * For operations on vectors, `NA`'s are absolutely poisonous by default.
    * For operations on `data.frames`'s, `NA`'s are absolutely poisonous on a column-by-column basis by default. This stems from a more general which assumes that most operations on `data.frame` reduce to the aggregation of this function performed on each column in isolation.
    * Every function should provide an `na.rm` option that allows one to ignore `NA`'s. Essentially this involves replacing `NA` by the identity element for that function: `sum(na.rm = TRUE)` replaces `NA`'s with `0`, while `prod(na.rm = TRUE)` replaces `NA`'s with `1`.
* Should there be multiple types of missingness?
    * For example, SAS distinguishes between:
        * Numeric missing values
        * Character missing values
        * Special numeric missing values
    * In statistical theory, while the _fact_ of missingness is simple and does not involve multiple types of `NA`'s, the _cause_ of missingness can be different for different data sets, which leads to very different procedures that can appropriately be used. See, for example, the different suggestions in Little and Rubin (2002) about how to treat data that has entries missing completely at random (MCAR) vs. data that has entries missing at random (MAR). Should we be providing tools for handling this? External data sources will almost never provide this information, but multiple dispatch means that Julian statistical functions could insure that the appropriate computations are performed for properly typed data sets without the end-user ever understanding the process that goes on under the hood.
* How is missingness different from `NaN` for `Float`'s? Both share poisonous behavior and `NaN` propagation is very efficient in modern computers. This can be a clever method for making `NA` fast for `Float`'s, but does not apply to other types and seems potentially problematic as two different concepts are now aliased. For example, we are not uncertain about the value of `0/0` and should not allow any method to impute a value -- which the method will do if it treats every `NaN` as equivalent to a `NA`.
* Should cleverness ever be allowed in propagation of `NA`? In section 3.3.4 of the R Language Definition, they note that in cases where the result of an operation would be the same for all possible values that an `NA` value could take on, the operation may return this constant value rather than return `NA`. For example, `FALSE & NA` returns `FALSE` while `TRUE | NA` returns `TRUE`. This seems like a can-of-worms to me.

# Tabular Data Structures: DataFrames

## Design

As noted before, nearly all interesting statistical methods work on objects that are more complex than simple vectors, even if those vectors have been augmented to express missingness. In addition to an `NA` type, one needs the ability to express the notion that a set of variables have all been measured for a specific case so that statistical analysis can proceed by treating each case as an organic whole. For example, in regression this case-by-variables approach allows us to assert that one variable `z`'s value depends _on a case-by-case basis_ on the values of two other variables, `x` and `y`.

In most statistical enviroments, this cases-by-variables approach to data analysis is accomplished using a tabular data structure. Some examples include:

* The data set type in SAS and SPSS.
* The data table type in Python's pandas library. (NB: pandas seems to go further than a simple tabular data structure.)
* The `data.frame` and `data.table` types in R.

In general, such tabular data structures can be viewed as instances of the relational model of data that also underlies the design of SQL. Like SQL, Julia should also provide a method for organizing tabular data, indexing into it and performing computations on it. We propose to call the resulting new type a `DataFrame`.

We note that the relational model as a design makes no stipulation about implementation details like row-orientation or column-orientation of a tabular data structure. And, like the relational model, Julian DataFrame's must allow missing data. What is essential are the following requirements:

* All elements within a column have a constant typ. This is really not a problematic restriction; if you want to allow both strings and characters in Column V, simply insist that the type of elements of V be Any.
* Different columns may have different types. This is why a DataFrame is not a Matrix.
* Optional names for rows and columns.
* Optional groupings of rows and columns.
* Ability to index into DataFrame using row indices / row names and/or column indices / column names.

A DataFrame (or maybe DataTable is a better name) type, of heterogeneous *Data columns, complete with rownames and colnames. We should find out more about what John Chambers thinks about data.frames in S/R and how they should be done better. We should also look at the data.table implementation and also at what Pandas is doing.

The power of reshape2 is severely limited by the asymmetric treatment of row and column variables in a data.drame. New data type should treat column and row variables symmetrically, and may be a better name would be data.matrix or even data.array. A related limitation of R's data.frame is that values in a column must have same type. Pandas corrected for this issue in the implementation of the data frame by have symmetrical treatment for rows and columns.

## Implementation

A data frame is a list of vectors, factors, and/or matrices all having the same length (number of rows in the case of matrices). In addition, a data frame generally has a names attribute labeling the variables and a row.names attribute for labeling the cases.

A data frame can contain a list that is the same length as the other components. The list can contain elements of differing lengths thereby providing a data structure for ragged arrays. However, as of this writing such arrays are not generally handled correctly.

## Ongoing Debates

Indexing
Symmetric rows and columns
* It is said that John Chambers has many complaints about the design of R's `data.frame` type. Yet those complaints are not clear. It would be valuable to have access to his criticisms. Right now we have nothing.

# Model Formulas

## Design

Formulas will probably be explicitly quoted expression in Julia, ala lm(:(y ~ x), dat). So we just need a set of conventions (and maybe an extra operator or two).

Interactions
Hierarchical indexing

model.matrix and related equivalent methods on formulas.

## Implementation

## Ongoing Debates

# Factors

## Design

As noted above, statistical data often involves that are not quantitative, but qualitative. Such variables are typically called categorical and can often take on a finite number of values. For example, a data set about people may contain demographic information such as gender or nationality. These are categorical variables and should not be represented using quantitative codes unless required.

In general, we can require that a `Factor` type allow us to express variables that can take on a known, finite list of values. This finite list is called the levels of a `Factor`.

A subtype of `Factor` is the `OrdinalFactor` in which the levels of the factor are mathematically well-ordered, but do not take on cardinal values that would allow us to use a quantitative coding system.

## Implementation

A FactorData type, supporting optionally ordered enumerations with NAs. Currently implemented as PooledVecs.

Factors are currently implemented using an integer array to specify the actual levels and a second array of names that are mapped to the integers. Rather unfortunately users often make use of the implementation in order to make some calculations easier. This, however, is an implementation issue and is not guaranteed to hold in all implementations of R.

## Ongoing Debates

None at present.

# Future Directions of Work

## DataStreams

Modeling functions need to be able to work with tabular data that cannot be fit in memory. Indeed, one of the defining attributes of large-scale data analysis is that we work with data sets that are too large to fit in memory on the machines we have access to, but do fit in disk on those machines. While parallelization is clearly important for large-scale data analysis, it is generally not sufficient: equally important is the ability to process a tabular data set without storing the entire data set in memory during processing.

Processing data without represnting the entire data set in memory is well-studied in computer science and is described as streaming data procesing. We have begun work on mechanisms for procesing streaming data sets by providing a DataStream.

## Online Data Analysis

For many applications, a method for handling a data stream is not sufficient: in practice, we often require that an algorithm not only be able to work with limited memory, but provide interim results before it has finished analyzing a data stream. Methods that provide interim answers while processing a data stream are called online learning methods and are essential for modern businesses that must process and act upon data in real time. We would like for Julia to be used to build such systems whenever possible.
