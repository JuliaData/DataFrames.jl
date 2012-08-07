# Statistical Programming in Julia: The DataFrames Project

===

# Introduction

We want to use Julia for statistical programming. Julia is already a very powerful tool for general mathematical programming, but it is missing several features that are essential for statistical programming. We believe the changes that must be made to prepare Julia for statistical programming can be implemented using only Julian modules/packages and will not require changes to the core language. As such, we are building a package called `DataFrame` that implements the necessary changes.

To (1) explain why the missing features implemented by `DataFrame` are essential and (2) motivate the changes that need to be made to Julia to make up for their absence in the core language, we describe several examples of increasing complexity below that involve calculating various types of means:

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
* A `Factor` type can be used to inform a statistical algorithm that an input or output variable is categorical. This is particularly important when specifying regression models in which a single categorical variable encoded using `Factor`'s will be replaced with an entire matrix of indicator variables that allows pure numerical computation to be done to estimate a regression model. This conversion of categorical data into numerical data is among the most important tasks that a statistical system performs since it is error-prone if the programmer performs it for themselves using ad hoc code.

Taken together, this means we actually need to add four pieces to Julia to prepare it for statistical programming:

* Missing Data
* Tabular Data Structures
* Model Formulas
* Factors

With that overview of our four basic additions to Julia in mind, we now proceed to introduce them in greater detail. We then describe our current design, our current implementations of this design and our long-term goals.

# Missing Data

## Design

As noted above, the most basic piece of statistical programming functionality that Julia is missing is an ability to explicitly represent data that suffers from missingness. A Julian `Int64`, for example, cannot represent missingness: a definite numeric value must be present for every element of a Julian `Vector` of `Int64`'s.

In the abstract, we can handle this by complementing every existing type available in Julia with an augmented type that handle missingness. If, for example, `MyType` can encode the values "A", "B" and "C", then "MyTypeAugmented" should be able to encode the values "A", "B", "C" and `NA`, where `NA` is a special value that indicates missingness and is not a value that "MyType" could ever take on. This augmentation procedure could be done for every single type in Julia and we would then have a version of Julia that handles missingness appropriately.

But we need to specify substantially more than types that can encode missingness, we also need to specify methods for working with data that suffers from missingness. As a starting example, suppose that the value of a variable `x` is missing and we attempt to compute `x + 2`. What should the answer be?

Most statistical enviroments, including R, view `NA` as poisonous: if we are uncertain of the value of `x`, then we should also be uncertain of the value of `x + 2`, which is therefore set to `NA`. Because uncertainty is poisonous, we need to redefine the addition operation in Julia on our augmented type system to insure that `NA`'s poison the results of all downstream computations and produce `NA`'s.

Unfortunately, dealing with `NA`'s is actually more complex than this simple poisoning rule suggests: most statistical systems do not view `NA` as absolutely poisonous. While `NA + 2` produces `NA`, it is typically possible to instruct more complex functions like `mean()` to ignore `NA` values. In short, the treatment of `NA` must be handled on a case-by-base basis, although there are recurring patterns that we can exploit. In practice, we will need to provide all of the important types of behavior and then set reasonable defaults.

## Implementation

* One approach to providing `NA` support in Julia involves implementing everything using `AbstractArray`'s of `Union` types. This approach is feasible in Julia and could, in principle, be extended to any types we wish to augment with `NA` by using macros. But this approach might be quite inefficient.
* Another different approach is to use appropriate `BitsType`'s to encode missingness. If done appropriately, this could plausibly be much more efficient than the `Union` type, but seems to be more complex to get right.

## Ongoing Debates

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

# Tabular Data Structures: DataFrames

## Design

As noted before, nearly all interesting statistical methods work on objects that are more complex than simple vectors, even if those vectors have been augmented to express missingness. In addition to an `NA` type, one needs the ability to express the notion that a set of variables have all been measured for a specific case so that statistical analysis can proceed by treating each case as an organic whole. For example, in regression this case-by-variables approach allows us to assert that one variable `z`'s value depends _on a case-by-case basis_ on the values of two other variables, `x` and `y`.

In most statistical enviroments, this cases-by-variables approach to data analysis is accomplished using a tabular data structure. Some examples include:

* The data set type in SAS and SPSS.
* The data table type in Python's pandas library. (NB: pandas seems to go further than a simple tabular data structure.)
* The `data.frame` and `data.table` types in R.

In general, such tabular data structures can be viewed as instances of the relational model of data that also underlies the design of SQL. Like SQL, Julia should also provide a method for organizing tabular data, indexing into it and performing computations on it. We propose to call the resulting new type a `DataFrame`.

We note that the relational model as a design makes no stipulation about implementation details like row-orientation or column-orientation of a tabular data structure. And, like the relational model, Julian DataFrame's must allow missing data, which is why we introduced methods for handling missing data before introducing the `DataFrame`. What is essential for the behavior of `DataFrame`'s are the following design requirements:

* A `DataFrame` is a two-dimensional data structure that contains _m_ rows and _n_ columns.
* All elements within one column of a `DataFrame` have a constant type. This is not a substantive restriction, because the type of a column could be the `Any` type.
* Two different columns may contain elements with two different types. This is why a `DataFrame` is not a `Matrix` of any specific type.
* Both the rows and columns of a `DataFrame` may have names in addition to numeric indices.
* One can specify groups of rows and/or groups of columns of a `DataFrame` using specialized indexes as in a RDBMS.
* One can index into the entries of a `DataFrame` using row indices/row names/row groups and/or column indices/column names/column groups.

Thus a `DataFrame` can be viewed as an aggregate of _n_ heterogeneous columns, each of which has length _m_.

## Implementation

DETAILS NEEDED

## Ongoing Debates

* How should RDBMS-like indices be implemented? What is most efficient? How can we avoid the inefficient vector searches that R uses?
* How should `DataFrame`'s be distributed for parallel processing?
* How should we insure a symmetric treatment of rows and columns?

## Ongoing Questions

* It is said that John Chambers has many complaints about the design of R's `data.frame` type and ideas about how to do better. It would be helpful if we knew more about what we would do differently.

# Model Formulas

## Design

Once support for missing data and tabular data structures are in place, we need to begin to develop a version of the model formulas "syntax" used by R. In reality, it is better to regard this "syntax" as a complete domain-specific language (DSL) for describing linear models. For those unfamilar with this DSL, we show some examples below and then elaborate upon them to demonstrate ways in which Julia might move beyond R's formula system.

Let's consider the simplest sort of linear regression model: how does the height of a child depend upon the height of the child's mother and father? If we let the variable `C` denote the height of the child, `M` the height of the mother and `F` the height of the father, the standard linear model approach in statistics would try to model their relationship using the following equation: `C = a + bM + cF + epsilon`, where `a`, `b` and `c` are fixed constants and `epsilon` is a normally distributed noise term that accounts for the imperfect match between any specific child's height and the predictions based solely on the heights of that child's mother and father.

In practice, we would fit such a model using a function that performs linear regression for us based on information about the model and the data source. For example, in R we would write `lm(C ~ M + F, data = heights.data)` to fit this model, assuming that `heights.data` refers to a tabular data structure containing the heights of the children, mothers and fathers for which we have data.

If we wanted to see how the child's height depends only on the mother's height, we would write `lm(C ~ M)`. If we were concerned only about dependence on the father's height, we would write `lm(C ~ H)`. As you can see, we can perform many different statistical analyses using a very consise language for describing those analyses.

What is that language? The R formula language allows one to specify linear models by specifying the terms that should be included. The language is defined by a very small number of constructs:

* The `~` operator: The `~` operator separates the pieces of a Formula. For linear models, this means that one specifies the outputs to be predicted on the left-hand side of the `~` and the inputs to be used to make predictions on the right-hand side.
* The `+` operator: If you wish to include multiple predictors in a linear model, you use the `+` operator. To include both the columns `A` and `B` while predicting `C`, you write: `C ~ A + B`.
* The `:` operator: The `:` operator computes interaction terms, which are really an entirely new column created by combining two existing columns. For example, `C ~ A:B` describes a linear model with only one predictor. The values of this predictor at row `i` is exactly `A[i] * B[i]`, where `*` is the standard arithmetic multiplication operation.
* The `*` operator: The `*` operator is really shorthand because `C ~ A*B` expands to `C ~ A + B + A:B`. In other words, in a DSL with only three operators, the `*` is just syntactic sugar.

In addition to these operators, the model formulas DSL typically allows us to include simple functions of single columns such as in the example, `C ~ A + log(B)`.

For Julia, this DSL will be handled by constructing an object of type `Formula`. It will be possible to generate a `Formula` using explicitly quoted expression. For example, we might write the Julian equivalent of the models above as `lm(:(C ~ M + F), heights_data)`. A `Formula` object describes how one should convert the columns of a `DataFrame` into a `ModelMatrix`, which fully specifies a linear model. [MORE DETAILS NEEDED ABOUT HOW `ModelMatrix` WORKS.]

How can Julia move beyond R? The primary improvement Julia can offer over R's model formula approach involves the use of hierarchical indexing of columns to control the inclusion of groups of columns as predictors. For example, a text regression model that uses word counts for thousands of different words as columns in a `DataFrame` might involve writing `IsSpam ~ Pronouns + Prepositions + Verbs` to exclude most words from the analysis except for those included in the `Pronouns`, `Prepositions` and `Verbs` groups. In addition, we might try to improve upon some of the tricks R provides for writing hierarchical models in which each value of a categorical predictor gets its own coefficients. This occurs, for example, in hierarchical regression models.

## Implementation

DETAILS NEEDED

## Ongoing Debates

None at present.

# Factors

## Design

As noted above, statistical data often involves that are not quantitative, but qualitative. Such variables are typically called categorical variables and can take on only a finite number of different values. For example, a data set about people might contain demographic information such as gender or nationality for which we can know the entire set of possible values in advance. Both gender and nationality are categorical variables and should not be represented using quantitative codes unless required as this is confusing to the user and mathematically suspect since the numbering used is entirely artificial.

In general, we can require that a `Factor` type allow us to express variables that can take on a known, finite list of values. This finite list is called the levels of a `Factor`. In this sense, a `Factor` is like an enumeration type.

What makes a `Factor` more specialized than an enumeration type is that modeling tools can interpret factors using indicator variables. This is very important for specifying regression models. For example, if we run a regression in which the right-hand side includes a gender `Factor`, the regression function can replace this factor with two dummy variable columns that encode the levels of this factor. (In practice, there are additional complications because of issues of identifiability or collinearity, but we ignore those for the time being and address them in the Implementation section.)

In addition to the general `Factor` type, we might also introduce a subtype of the `Factor` type that encodes ordinal variables, which are categorical variables that encode a definite ordering such as the values, "very unhappy", "unhappy", "indifferent", "happy" and "very happy". By introducing an `OrdinalFactor` type in which the levels of this sort of ordinal factor are represented in their proper ordering, we can provide specialized functionality like ordinal logistic regression that go beyond what is possible with `Factor` types alone.

## Implementation

We have a `Factor` type that handles `NA`s. This type is currently implemented using `PooledVec`'s.

## Ongoing Debates

None at present.

# Future Directions of Work

## DataStreams

Modeling functions need to be able to work with tabular data that cannot be fit in memory. Indeed, one of the defining attributes of large-scale data analysis is that we work with data sets that are too large to fit in memory on the machines we have access to, but do fit in disk on those machines. While parallelization is clearly important for large-scale data analysis, it is generally not sufficient: equally important is the ability to process a tabular data set without storing the entire data set in memory during processing.

Processing data without represnting the entire data set in memory is well-studied in computer science and is described as streaming data procesing. We have begun work on mechanisms for procesing streaming data sets by providing a DataStream.

## Online Data Analysis

For many applications, a method for handling a data stream is not sufficient: in practice, we often require that an algorithm not only be able to work with limited memory, but provide interim results before it has finished analyzing a data stream. Methods that provide interim answers while processing a data stream are called online learning methods and are essential for modern businesses that must process and act upon data in real time. We would like for Julia to be used to build such systems whenever possible.

# References and Inspiration

* John Chambers e-mail describing better `data.frame`'s.
* The design of Python's pandas library.
* R's `data.frame`.
* R's `data.table`.
