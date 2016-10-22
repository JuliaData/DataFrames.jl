var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Introduction",
    "title": "Introduction",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#DataFrames-Documentation-Outline-1",
    "page": "Introduction",
    "title": "DataFrames Documentation Outline",
    "category": "section",
    "text": ""
},

{
    "location": "index.html#Package-Manual-1",
    "page": "Introduction",
    "title": "Package Manual",
    "category": "section",
    "text": "Pages = [\"man/getting_started.md\", \"man/io.md\", \"man/joins.md\", \"man/split_apply_combine.md\", \"man/reshaping_and_pivoting.md\", \"man/sorting.md\", \"man/formulas.md\", \"man/pooling.md\"]\nDepth = 2"
},

{
    "location": "index.html#API-1",
    "page": "Introduction",
    "title": "API",
    "category": "section",
    "text": "Pages = [\"lib/maintypes.md\", \"lib/manipulation.md\", \"lib/utilities.md\"]\nDepth = 2"
},

{
    "location": "index.html#Documentation-Index-1",
    "page": "Introduction",
    "title": "Documentation Index",
    "category": "section",
    "text": "Pages = [\"lib/maintypes.md\", \"lib/manipulation.md\", \"lib/utilities.md\", \"man/io.md\"]"
},

{
    "location": "man/getting_started.html#",
    "page": "Getting Started",
    "title": "Getting Started",
    "category": "page",
    "text": ""
},

{
    "location": "man/getting_started.html#Getting-Started-1",
    "page": "Getting Started",
    "title": "Getting Started",
    "category": "section",
    "text": ""
},

{
    "location": "man/getting_started.html#Installation-1",
    "page": "Getting Started",
    "title": "Installation",
    "category": "section",
    "text": "The DataFrames package is available through the Julia package system. Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed using NullableArrays, DataFrames to bring all of the relevant variables into your current namespace. In addition, we will make use of the RDatasets package, which provides access to hundreds of classical data sets."
},

{
    "location": "man/getting_started.html#The-Nullable-Type-1",
    "page": "Getting Started",
    "title": "The Nullable Type",
    "category": "section",
    "text": "To get started, let's examine the Nullable type. Objects of this type can either hold a value, or represent a missing value (null). For example, this is a Nullable holding the integer 1:Nullable(1)And this represents a missing value:Nullable()Nullable objects support all standard operators, which return another Nullable. One of the essential properties of null values is that they poison other items. To see this, try to add something like Nullable(1) to Nullable():Nullable(1) + Nullable()Note that operations mixing Nullable and scalars (e.g. 1 + Nullable()) are not supported."
},

{
    "location": "man/getting_started.html#The-NullableArray-Type-1",
    "page": "Getting Started",
    "title": "The NullableArray Type",
    "category": "section",
    "text": "Nullable objects can be stored in a standard Array just like any value:v = Nullable{Int}[1, 3, 4, 5, 4]But arrays of Nullable are inefficient, both in terms of computation costs and of memory use. NullableArrays provide a more efficient storage, and behave like Array{Nullable} objects.nv = NullableArray(Nullable{Int}[Nullable(), 3, 2, 5, 4])In many cases we're willing to just ignore missing values and remove them from our vector. We can do that using the dropnull function:dropnull(nv)\nmean(dropnull(nv))Instead of removing null values, you can try to convert the NullableArray into a normal Julia Array using convert:convert(Array, nv)This fails in the presence of null values, but will succeed if there are no null values:nv[1] = 3\nconvert(Array, nv)In addition to removing null values and hoping they won't occur, you can also replace any null values using the convert function, which takes a replacement value as an argument:nv = NullableArray(Nullable{Int}[Nullable(), 3, 2, 5, 4])\nmean(convert(Array, nv, 0))Which strategy for dealing with null values is most appropriate will typically depend on the specific details of your data analysis pathway."
},

{
    "location": "man/getting_started.html#The-DataFrame-Type-1",
    "page": "Getting Started",
    "title": "The DataFrame Type",
    "category": "section",
    "text": "The DataFrame type can be used to represent data tables, each column of which is an array (by default, a NullableArray). You can specify the columns using keyword arguments:df = DataFrame(A = 1:4, B = [\"M\", \"F\", \"F\", \"M\"])It is also possible to construct a DataFrame in stages:df = DataFrame()\ndf[:A] = 1:8\ndf[:B] = [\"M\", \"F\", \"F\", \"M\", \"F\", \"M\", \"M\", \"F\"]\ndfThe DataFrame we build in this way has 8 rows and 2 columns. You can check this using size function:nrows = size(df, 1)\nncols = size(df, 2)We can also look at small subsets of the data in a couple of different ways:head(df)\ntail(df)\n\ndf[1:3, :]Having seen what some of the rows look like, we can try to summarize the entire data set using describe:describe(df)To focus our search, we start looking at just the means and medians of specific columns. In the example below, we use numeric indexing to access the columns of the DataFrame:mean(dropnull(df[1]))\nmedian(dropnull(df[1]))We could also have used column names to access individual columns:mean(dropnull(df[:A]))\nmedian(dropnull(df[:A]))We can also apply a function to each column of a DataFrame with the colwise function. For example:df = DataFrame(A = 1:4, B = randn(4))\ncolwise(c->cumsum(dropnull(c)), df)"
},

{
    "location": "man/getting_started.html#Accessing-Classic-Data-Sets-1",
    "page": "Getting Started",
    "title": "Accessing Classic Data Sets",
    "category": "section",
    "text": "To see more of the functionality for working with DataFrame objects, we need a more complex data set to work with. We'll use the RDatasets package, which provides access to many of the classical data sets that are available in R.For example, we can access Fisher's iris data set using the following functions:iris = readtable(joinpath(Pkg.dir(\"DataFrames\"), \"test/data/iris.csv\"))\nhead(iris)In the next section, we'll discuss generic I/O strategy for reading and writing DataFrame objects that you can use to import and export your own data files."
},

{
    "location": "man/getting_started.html#Querying-DataFrames-1",
    "page": "Getting Started",
    "title": "Querying DataFrames",
    "category": "section",
    "text": "While the DataFrames package provides basic data manipulation capabilities, users are encouraged to use the following packages for more powerful and complete data querying functionality in the spirit of dplyr and LINQ:DataFramesMeta.jl provides metaprogramming tools for DataFrames and associative objects. These macros improve performance and provide more convenient syntax.\nQuery.jl provides a LINQ like interface to a large number of data sources, including DataFrame instances."
},

{
    "location": "man/io.html#",
    "page": "IO",
    "title": "IO",
    "category": "page",
    "text": ""
},

{
    "location": "man/io.html#Importing-and-Exporting-(I/O)-1",
    "page": "IO",
    "title": "Importing and Exporting (I/O)",
    "category": "section",
    "text": ""
},

{
    "location": "man/io.html#DataFrames.readtable",
    "page": "IO",
    "title": "DataFrames.readtable",
    "category": "Function",
    "text": "Read data from a tabular-file format (CSV, TSV, ...)\n\nreadtable(filename, [keyword options])\n\nArguments\n\nfilename::AbstractString : the filename to be read\n\nKeyword Arguments\n\nheader::Bool – Use the information from the file's header line to determine column names. Defaults to true.\nseparator::Char – Assume that fields are split by the separator character. If not specified, it will be guessed from the filename: .csv defaults to ',', .tsv defaults to '	', .wsv defaults to ' '.\nquotemark::Vector{Char} – Assume that fields contained inside of two quotemark characters are quoted, which disables processing of separators and linebreaks. Set to Char[] to disable this feature and slightly improve performance. Defaults to ['\"'].\ndecimal::Char – Assume that the decimal place in numbers is written using the decimal character. Defaults to '.'.\nnastrings::Vector{String} – Translate any of the strings into this vector into a NULL value. Defaults to [\"\", \"NULL\", \"NA\"].\ntruestrings::Vector{String} – Translate any of the strings into this vector into a Boolean true. Defaults to [\"T\", \"t\", \"TRUE\", \"true\"].\nfalsestrings::Vector{String} – Translate any of the strings into this vector into a Boolean false. Defaults to [\"F\", \"f\", \"FALSE\", \"false\"].\nmakefactors::Bool – Convert string columns into CategoricalVector's for use as factors. Defaults to false.\nnrows::Int – Read only nrows from the file. Defaults to -1, which indicates that the entire file should be read.\nnames::Vector{Symbol} – Use the values in this array as the names for all columns instead of or in lieu of the names in the file's header. Defaults to [], which indicates that the header should be used if present or that numeric names should be invented if there is no header.\neltypes::Vector – Specify the types of all columns. Defaults to [].\nallowcomments::Bool – Ignore all text inside comments. Defaults to false.\ncommentmark::Char – Specify the character that starts comments. Defaults to '#'.\nignorepadding::Bool – Ignore all whitespace on left and right sides of a field. Defaults to true.\nskipstart::Int – Specify the number of initial rows to skip. Defaults to 0.\nskiprows::Vector{Int} – Specify the indices of lines in the input to ignore. Defaults to [].\nskipblanks::Bool – Skip any blank lines in input. Defaults to true.\nencoding::Symbol – Specify the file's encoding as either :utf8 or :latin1. Defaults to :utf8.\nnormalizenames::Bool – Ensure that column names are valid Julia identifiers. For instance this renames a column named \"a b\" to \"a_b\" which can then be accessed with :a_b instead of Symbol(\"a b\"). Defaults to true.\n\nResult\n\n::DataFrame\n\nExamples\n\ndf = readtable(\"data.csv\")\ndf = readtable(\"data.tsv\")\ndf = readtable(\"data.wsv\")\ndf = readtable(\"data.txt\", separator = '	')\ndf = readtable(\"data.txt\", header = false)\n\n\n\n"
},

{
    "location": "man/io.html#Importing-data-from-tabular-data-files-1",
    "page": "IO",
    "title": "Importing data from tabular data files",
    "category": "section",
    "text": "To read data from a CSV-like file, use the readtable function:readtablereadtable requires that you specify the path of the file that you would like to read as a String. To read data from a non-file source, you may also supply an IO object. It supports many additional keyword arguments: these are documented in the section on advanced I/O operations."
},

{
    "location": "man/io.html#DataFrames.writetable",
    "page": "IO",
    "title": "DataFrames.writetable",
    "category": "Function",
    "text": "Write data to a tabular-file format (CSV, TSV, ...)\n\nwritetable(filename, df, [keyword options])\n\nArguments\n\nfilename::AbstractString : the filename to be created\ndf::AbstractDataFrame : the AbstractDataFrame to be written\n\nKeyword Arguments\n\nseparator::Char – The separator character that you would like to use. Defaults to the output of getseparator(filename), which uses commas for files that end in .csv, tabs for files that end in .tsv and a single space for files that end in .wsv.\nquotemark::Char – The character used to delimit string fields. Defaults to '\"'.\nheader::Bool – Should the file contain a header that specifies the column names from df. Defaults to true.\nnastring::AbstractString – What to write in place of missing data. Defaults to \"NULL\".\n\nResult\n\n::DataFrame\n\nExamples\n\ndf = DataFrame(A = 1:10)\nwritetable(\"output.csv\", df)\nwritetable(\"output.dat\", df, separator = ',', header = false)\nwritetable(\"output.dat\", df, quotemark = '', separator = ',')\nwritetable(\"output.dat\", df, header = false)\n\n\n\n"
},

{
    "location": "man/io.html#Exporting-data-to-a-tabular-data-file-1",
    "page": "IO",
    "title": "Exporting data to a tabular data file",
    "category": "section",
    "text": "To write data to a CSV file, use the writetable function:writetable"
},

{
    "location": "man/io.html#Supplying-DataFrames-inline-with-non-standard-string-literals-1",
    "page": "IO",
    "title": "Supplying DataFrames inline with non-standard string literals",
    "category": "section",
    "text": "You can also provide CSV-like tabular data in a non-standard string literal to construct a new DataFrame, as in the following:df = csv\"\"\"\n    name,  age, squidPerWeek\n    Alice,  36,         3.14\n    Bob,    24,         0\n    Carol,  58,         2.71\n    Eve,    49,         7.77\n    \"\"\"The csv string literal prefix indicates that the data are supplied in standard comma-separated value format. Common alternative formats are also available as string literals. For semicolon-separated values, with comma as a decimal, use csv2:df = csv2\"\"\"\n    name;  age; squidPerWeek\n    Alice;  36;         3,14\n    Bob;    24;         0\n    Carol;  58;         2,71\n    Eve;    49;         7,77\n    \"\"\"For whitespace-separated values, use wsv:df = wsv\"\"\"\n    name  age squidPerWeek\n    Alice  36         3.14\n    Bob    24         0\n    Carol  58         2.71\n    Eve    49         7.77\n    \"\"\"And for tab-separated values, use tsv:df = tsv\"\"\"\n    name	age	squidPerWeek\n    Alice	36	3.14\n    Bob	24	0\n    Carol	58	2.71\n    Eve	49	7.77\n    \"\"\""
},

{
    "location": "man/joins.html#",
    "page": "Joins",
    "title": "Joins",
    "category": "page",
    "text": ""
},

{
    "location": "man/joins.html#Database-Style-Joins-1",
    "page": "Joins",
    "title": "Database-Style Joins",
    "category": "section",
    "text": "We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:names = DataFrame(ID = [1, 2], Name = [\"John Doe\", \"Jane Doe\"])\njobs = DataFrame(ID = [1, 2], Job = [\"Lawyer\", \"Doctor\"])We might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the join function:full = join(names, jobs, on = :ID)Output:Row ID Name Job\n1 1 \"John Doe\" \"Lawyer\"\n2 1 \"Jane Doe\" \"Doctor\"In relational database theory, this operation is generally referred to as a join. The columns used to determine which rows should be combined during a join are called keys.There are seven kinds of joins supported by the DataFrames package:Inner: The output contains rows for values of the key that exist in both the first (left) and second (right) arguments to join.\nLeft: The output contains rows for values of the key that exist in the first (left) argument to join, whether or not that value exists in the second (right) argument.\nRight: The output contains rows for values of the key that exist in the second (right) argument to join, whether or not that value exists in the first (left) argument.\nOuter: The output contains rows for values of the key that exist in the first (left) or second (right) argument to join.\nSemi: Like an inner join, but output is restricted to columns from the first (left) argument to join.\nAnti: The output contains rows for values of the key that exist in the first (left) but not the second (right) argument to join. As with semi joins, output is restricted to columns from the first (left) argument.\nCross: The output is the cartesian product of rows from the first (left) and second (right) arguments to join.You can control the kind of join that join performs using the kind keyword argument:a = DataFrame(ID = [1, 2], Name = [\"A\", \"B\"])\nb = DataFrame(ID = [1, 3], Job = [\"Doctor\", \"Lawyer\"])\njoin(a, b, on = :ID, kind = :inner)\njoin(a, b, on = :ID, kind = :left)\njoin(a, b, on = :ID, kind = :right)\njoin(a, b, on = :ID, kind = :outer)\njoin(a, b, on = :ID, kind = :semi)\njoin(a, b, on = :ID, kind = :anti)Cross joins are the only kind of join that does not use a key:join(a, b, kind = :cross)"
},

{
    "location": "man/split_apply_combine.html#",
    "page": "Split-apply-combine",
    "title": "Split-apply-combine",
    "category": "page",
    "text": ""
},

{
    "location": "man/split_apply_combine.html#The-Split-Apply-Combine-Strategy-1",
    "page": "Split-apply-combine",
    "title": "The Split-Apply-Combine Strategy",
    "category": "section",
    "text": "Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper, The Split-Apply-Combine Strategy for Data Analysis \\<<http://www.jstatsoft.org/v40/i01>\\>, written by Hadley Wickham.The DataFrames package supports the Split-Apply-Combine strategy through the by function, which takes in three arguments: (1) a DataFrame, (2) a column to split the DataFrame on, and (3) a function or expression to apply to each subset of the DataFrame.We show several examples of the by function applied to the iris dataset below:using DataFrames\niris = readtable(joinpath(Pkg.dir(\"DataFrames\"), \"test/data/iris.csv\"))\n\nby(iris, :Species, size)\nby(iris, :Species, df -> mean(dropnull(df[:PetalLength])))\nby(iris, :Species, df -> DataFrame(N = size(df, 1)))The by function also support the do block form:by(iris, :Species) do df\n   DataFrame(m = mean(dropnull(df[:PetalLength])), s² = var(dropnull(df[:PetalLength])))\nendA second approach to the Split-Apply-Combine strategy is implemented in the aggregate function, which also takes three arguments: (1) a DataFrame, (2) a column (or columns) to split the DataFrame on, and a (3) function (or several functions) that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column, that was not used to split the DataFrame, creating new columns of the form $name_$function e.g. SepalLength_mean. Anonymous functions and expressions that do not have a name will be called λ1.We show several examples of the aggregate function applied to the iris dataset below:aggregate(iris, :Species, sum)\naggregate(iris, :Species, [sum, x->mean(dropnull(x))])If you only want to split the data set into subsets, use the groupby function:for subdf in groupby(iris, :Species)\n    println(size(subdf, 1))\nend"
},

{
    "location": "man/reshaping_and_pivoting.html#",
    "page": "Reshaping",
    "title": "Reshaping",
    "category": "page",
    "text": ""
},

{
    "location": "man/reshaping_and_pivoting.html#Reshaping-and-Pivoting-Data-1",
    "page": "Reshaping",
    "title": "Reshaping and Pivoting Data",
    "category": "section",
    "text": "Reshape data from wide to long format using the stack function:using DataFrames\niris = readtable(joinpath(Pkg.dir(\"DataFrames\"), \"test/data/iris.csv\"))\niris[:id] = 1:size(iris, 1)  # this makes it easier to unstack\nd = stack(iris, 1:4)The second optional argument to stack indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth])Note that all columns can be of different types. Type promotion follows the rules of vcat.The stacked DataFrame that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled :variable and :values contain the column identifier and the stacked columns.A third optional argument to stack represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:d = stack(iris, [:SepalLength, :SepalWidth], :Species)melt is an alternative function to reshape from wide to long format. It is based on stack, but it prefers specification of the id columns as:d = melt(iris, :Species)All other columns are assumed to be measured variables (they are stacked).You can also stack an entire DataFrame. The default stacks all floating-point columns:d = stack(iris)unstack converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:longdf = melt(iris, [:Species, :id])\nwidedf = unstack(longdf, :id, :variable, :value)If the remaining columns are unique, you can skip the id variable and use:widedf = unstack(longdf, :variable, :value)stackdf and meltdf are two additional functions that work like stack and melt, but they provide a view into the original wide DataFrame. Here is an example:d = stackdf(iris)This saves memory. To create the view, several AbstractVectors are defined::variable column – EachRepeatedVector   This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.:value column – StackedVector   This is provides a view of the original columns stacked together.Id columns – RepeatedVector   This repeats the original columns N times where N is the number of columns stacked.For more details on the storage representation, see:dump(stackdf(iris))None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:d = stack(iris)\nx = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(dropnull(df[:value]))))\nunstack(x, :Species, :vsum)"
},

{
    "location": "man/sorting.html#",
    "page": "Sorting",
    "title": "Sorting",
    "category": "page",
    "text": ""
},

{
    "location": "man/sorting.html#Sorting-1",
    "page": "Sorting",
    "title": "Sorting",
    "category": "section",
    "text": "Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling sort! will sort all columns, in place:using DataFrames\niris = readtable(joinpath(Pkg.dir(\"DataFrames\"), \"test/data/iris.csv\"))\nsort!(iris)In Sorting DataFrames, you may want to sort different columns with different options. Here are some examples showing most of the possible options:sort!(iris, rev = true)\nsort!(iris, cols = [:SepalWidth, :SepalLength])\n\nsort!(iris, cols = [order(:Species, by = uppercase),\n                    order(:SepalLength, rev = true)])Keywords used above include cols (to specify columns), rev (to sort a column or the whole DataFrame in reverse), and by (to apply a function to a column/DataFrame). Each keyword can either be a single value, or can be a tuple or array, with values corresponding to individual columns.As an alternative to using array or tuple values, order to specify an ordering for a particular column within a set of columnsThe following two examples show two ways to sort the iris dataset with the same result: Species will be ordered in reverse lexicographic order, and within species, rows will be sorted by increasing sepal length and width:sort!(iris, cols = (:Species, :SepalLength, :SepalWidth),\n      rev = (true, false, false))\n\nsort!(iris, cols = (order(:Species, rev = true), :SepalLength, :SepalWidth))"
},

{
    "location": "man/formulas.html#",
    "page": "Formulas",
    "title": "Formulas",
    "category": "page",
    "text": ""
},

{
    "location": "man/formulas.html#The-Formula,-ModelFrame-and-ModelMatrix-Types-1",
    "page": "Formulas",
    "title": "The Formula, ModelFrame and ModelMatrix Types",
    "category": "section",
    "text": "In regression analysis, we often want to describe the relationship between a response variable and one or more input variables in terms of main effects and interactions. To facilitate the specification of a regression model in terms of the columns of a DataFrame, the DataFrames package provides a Formula type, which is created by the ~ binary operator in Julia:fm = Z ~ X + YA Formula object can be used to transform a DataFrame into a ModelFrame object:df = DataFrame(X = randn(10), Y = randn(10), Z = randn(10))\nmf = ModelFrame(Z ~ X + Y, df)A ModelFrame object is just a simple wrapper around a DataFrame. For modeling purposes, one generally wants to construct a ModelMatrix, which constructs a Matrix{Float64} that can be used directly to fit a statistical model:mm = ModelMatrix(ModelFrame(Z ~ X + Y, df))Note that mm contains an additional column consisting entirely of 1.0 values. This is used to fit an intercept term in a regression model.In addition to specifying main effects, it is possible to specify interactions using the & operator inside a Formula:mm = ModelMatrix(ModelFrame(Z ~ X + Y + X&Y, df))If you would like to specify both main effects and an interaction term at once, use the * operator inside a `Formula`:mm = ModelMatrix(ModelFrame(Z ~ X*Y, df))You can control how categorical variables (e.g., CategoricalArray columns) are converted to ModelMatrix columns by specifying _contrasts_ when you construct a ModelFrame:mm = ModelMatrix(ModelFrame(Z ~ X*Y, df, contrasts = Dict(:X => HelmertCoding())))Contrasts can also be modified in an existing ModelFrame:mf = ModelFrame(Z ~ X*Y, df)\ncontrasts!(mf, X = HelmertCoding())The construction of model matrices makes it easy to formulate complex statistical models. These are used to good effect by the GLM Package."
},

{
    "location": "man/pooling.html#",
    "page": "Pooling",
    "title": "Pooling",
    "category": "page",
    "text": ""
},

{
    "location": "man/pooling.html#Categorical-Data-1",
    "page": "Pooling",
    "title": "Categorical Data",
    "category": "section",
    "text": "Often, we have to deal with factors that take on a small number of levels:v = [\"Group A\", \"Group A\", \"Group A\",\n     \"Group B\", \"Group B\", \"Group B\"]The naive encoding used in an Array or in a NullableArray represents every entry of this vector as a full string. In contrast, we can represent the data more efficiently by replacing the strings with indices into a small pool of levels. This is what the CategoricalArray type does:cv = CategoricalArray([\"Group A\", \"Group A\", \"Group A\",\n                       \"Group B\", \"Group B\", \"Group B\"])A companion type, NullableCategoricalArray, allows storing missing values in the array: is to CategoricalArray what NullableArray is to the standard Array type.In addition to representing repeated data efficiently, the CategoricalArray type allows us to determine efficiently the allowed levels of the variable at any time using the levels function (note that levels may or may not be actually used in the data):levels(cv)The levels! function also allows changing the order of appearance of the levels, which can be useful for display purposes or when working with ordered variables.By default, a CategoricalArray is able to represent 2<sup>32</sup>differents levels. You can use less memory by calling the compact function:cv = compact(cv)Often, you will have factors encoded inside a DataFrame with Array or NullableArray columns instead of CategoricalArray or NullableCategoricalArray columns. You can do conversion of a single column using the categorical function:cv = categorical(v)Or you can edit the columns of a DataFrame in-place using the categorical! function:df = DataFrame(A = [1, 1, 1, 2, 2, 2],\n               B = [\"X\", \"X\", \"X\", \"Y\", \"Y\", \"Y\"])\ncategorical!(df, [:A, :B])Using categorical arrays is important for working with the GLM package. When fitting regression models, CategoricalArray and NullableCategoricalArray columns in the input are translated into 0/1 indicator columns in the ModelMatrix with one column for each of the levels of the CategoricalArray/NullableCategoricalArray. This allows one to analyze categorical data efficiently.See the CategoricalArrays package for more information regarding categorical arrays."
},

{
    "location": "lib/maintypes.html#",
    "page": "Main types",
    "title": "Main types",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/maintypes.html#DataFrames.AbstractDataFrame",
    "page": "Main types",
    "title": "DataFrames.AbstractDataFrame",
    "category": "Type",
    "text": "An abstract type for which all concrete types expose a database-like interface.\n\nCommon methods\n\nAn AbstractDataFrame is a two-dimensional table with Symbols for column names. An AbstractDataFrame is also similar to an Associative type in that it allows indexing by a key (the columns).\n\nThe following are normally implemented for AbstractDataFrames:\n\ndescribe : summarize columns\ndump : show structure\nhcat : horizontal concatenation\nvcat : vertical concatenation\nnames : columns names\nnames! : set columns names\nrename! : rename columns names based on keyword arguments\neltypes : eltype of each column\nlength : number of columns\nsize : (nrows, ncols)\nhead : first n rows\ntail : last n rows\nconvert : convert to an array\nNullableArray : convert to a NullableArray\ncomplete_cases : indexes of complete cases (rows with no NA's)\ncomplete_cases! : remove rows with NA's\nnonunique : indexes of duplicate rows\nunique! : remove duplicate rows\nsimilar : a DataFrame with similar columns as d\n\nIndexing\n\nTable columns are accessed (getindex) by a single index that can be a symbol identifier, an integer, or a vector of each. If a single column is selected, just the column object is returned. If multiple columns are selected, some AbstractDataFrame is returned.\n\nd[:colA]\nd[3]\nd[[:colA, :colB]]\nd[[1:3; 5]]\n\nRows and columns can be indexed like a Matrix with the added feature of indexing columns by name.\n\nd[1:3, :colA]\nd[3,3]\nd[3,:]\nd[3,[:colA, :colB]]\nd[:, [:colA, :colB]]\nd[[1:3; 5], :]\n\nsetindex works similarly.\n\n\n\n"
},

{
    "location": "lib/maintypes.html#DataFrames.DataFrame",
    "page": "Main types",
    "title": "DataFrames.DataFrame",
    "category": "Type",
    "text": "An AbstractDataFrame that stores a set of named columns\n\nThe columns are normally AbstractVectors stored in memory, particularly a Vector, NullableVector, or CategoricalVector.\n\nConstructors\n\nDataFrame(columns::Vector{Any}, names::Vector{Symbol})\nDataFrame(kwargs...)\nDataFrame() # an empty DataFrame\nDataFrame(t::Type, nrows::Integer, ncols::Integer) # an empty DataFrame of arbitrary size\nDataFrame(column_eltypes::Vector, names::Vector, nrows::Integer)\nDataFrame(ds::Vector{Associative})\n\nArguments\n\ncolumns : a Vector{Any} with each column as contents\nnames : the column names\nkwargs : the key gives the column names, and the value is the column contents\nt : elemental type of all columns\nnrows, ncols : number of rows and columns\ncolumn_eltypes : elemental type of each column\nds : a vector of Associatives\n\nEach column in columns should be the same length.\n\nNotes\n\nMost of the default constructors convert columns to NullableArray.  The base constructor, DataFrame(columns::Vector{Any}, names::Vector{Symbol}) does not convert to NullableArray.\n\nA DataFrame is a lightweight object. As long as columns are not manipulated, creation of a DataFrame from existing AbstractVectors is inexpensive. For example, indexing on columns is inexpensive, but indexing by rows is expensive because copies are made of each column.\n\nBecause column types can vary, a DataFrame is not type stable. For performance-critical code, do not index into a DataFrame inside of loops.\n\nExamples\n\ndf = DataFrame()\nv = [\"x\",\"y\",\"z\"][rand(1:3, 10)]\ndf1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])  # columns are Arrays\ndf2 = DataFrame(A = 1:10, B = v, C = rand(10))           # columns are NullableArrays\ndump(df1)\ndump(df2)\ndescribe(df2)\nDataFrames.head(df1)\ndf1[:A] + df2[:C]\ndf1[1:4, 1:2]\ndf1[[:A,:C]]\ndf1[1:2, [:A,:C]]\ndf1[:, [:A,:C]]\ndf1[:, [1,3]]\ndf1[1:4, :]\ndf1[1:4, :C]\ndf1[1:4, :C] = 40. * df1[1:4, :C]\n[df1; df2]  # vcat\n[df1  df2]  # hcat\nsize(df1)\n\n\n\n"
},

{
    "location": "lib/maintypes.html#DataFrames.SubDataFrame",
    "page": "Main types",
    "title": "DataFrames.SubDataFrame",
    "category": "Type",
    "text": "A view of row subsets of an AbstractDataFrame\n\nA SubDataFrame is meant to be constructed with sub.  A SubDataFrame is used frequently in split/apply sorts of operations.\n\nsub(d::AbstractDataFrame, rows)\n\nArguments\n\nd : an AbstractDataFrame\nrows : any indexing type for rows, typically an Int, AbstractVector{Int}, AbstractVector{Bool}, or a Range\n\nNotes\n\nA SubDataFrame is an AbstractDataFrame, so expect that most DataFrame functions should work. Such methods include describe, dump, nrow, size, by, stack, and join. Indexing is just like a DataFrame; copies are returned.\n\nTo subset along columns, use standard column indexing as that creates a view to the columns by default. To subset along rows and columns, use column-based indexing with sub.\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\nsdf1 = sub(df, 1:6)\nsdf2 = sub(df, df[:a] .> 1)\nsdf3 = sub(df[[1,3]], df[:a] .> 1)  # row and column subsetting\nsdf4 = groupby(df, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame\nsdf5 = sub(sdf1, 1:3)\nsdf1[:,[:a,:b]]\n\n\n\n"
},

{
    "location": "lib/maintypes.html#Main-Types-1",
    "page": "Main types",
    "title": "Main Types",
    "category": "section",
    "text": "Pages = [\"maintypes.md\"]AbstractDataFrame\nDataFrame\nSubDataFrame"
},

{
    "location": "lib/utilities.html#",
    "page": "Utilities",
    "title": "Utilities",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/utilities.html#DataFrames.eltypes",
    "page": "Utilities",
    "title": "DataFrames.eltypes",
    "category": "Function",
    "text": "Return element types of columns\n\neltypes(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\n\nResult\n\n::Vector{Type} : the element type of each column\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\neltypes(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.head",
    "page": "Utilities",
    "title": "DataFrames.head",
    "category": "Function",
    "text": "Show the first or last part of an AbstractDataFrame\n\nhead(df::AbstractDataFrame, r::Int = 6)\ntail(df::AbstractDataFrame, r::Int = 6)\n\nArguments\n\ndf : the AbstractDataFrame\nr : the number of rows to show\n\nResult\n\n::AbstractDataFrame : the first or last part of df\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nhead(df)\ntail(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.complete_cases",
    "page": "Utilities",
    "title": "DataFrames.complete_cases",
    "category": "Function",
    "text": "Indexes of complete cases (rows without null values)\n\ncomplete_cases(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\n\nResult\n\n::Vector{Bool} : indexes of complete cases\n\nSee also complete_cases!.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf[[1,4,5], :x] = Nullable()\ndf[[9,10], :y] = Nullable()\ncomplete_cases(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.complete_cases!",
    "page": "Utilities",
    "title": "DataFrames.complete_cases!",
    "category": "Function",
    "text": "Delete rows with null values.\n\ncomplete_cases!(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\n\nResult\n\n::AbstractDataFrame : the updated version\n\nSee also complete_cases.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf[[1,4,5], :x] = Nullable()\ndf[[9,10], :y] = Nullable()\ncomplete_cases!(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#StatsBase.describe",
    "page": "Utilities",
    "title": "StatsBase.describe",
    "category": "Function",
    "text": "Summarize the columns of an AbstractDataFrame\n\ndescribe(df::AbstractDataFrame)\ndescribe(io, df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\nio : optional output descriptor\n\nResult\n\nnothing\n\nDetails\n\nIf the column's base type derives from Number, compute the minimum, first quantile, median, mean, third quantile, and maximum. NA's are filtered and reported separately.\n\nFor boolean columns, report trues, falses, and NAs.\n\nFor other types, show column characteristics and number of NAs.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndescribe(df)\n\n\n\ndescribe(a)\n\nPretty-print the summary statistics provided by summarystats: the mean, minimum, 25th percentile, median, 75th percentile, and maximum.\n\n\n\n"
},

{
    "location": "lib/utilities.html#Base.dump",
    "page": "Utilities",
    "title": "Base.dump",
    "category": "Function",
    "text": "Show the structure of an AbstractDataFrame, in a tree-like format\n\ndump(df::AbstractDataFrame, n::Int = 5)\ndump(io::IO, df::AbstractDataFrame, n::Int = 5)\n\nArguments\n\ndf : the AbstractDataFrame\nn : the number of levels to show\nio : optional output descriptor\n\nResult\n\nnothing\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndump(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.names!",
    "page": "Utilities",
    "title": "DataFrames.names!",
    "category": "Function",
    "text": "Set column names\n\nnames!(df::AbstractDataFrame, vals)\n\nArguments\n\ndf : the AbstractDataFrame\nvals : column names, normally a Vector{Symbol} the same length as the number of columns in df\nallow_duplicates : if false (the default), an error will be raised if duplicate names are found; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\n\nResult\n\n::AbstractDataFrame : the updated result\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nnames!(df, [:a, :b, :c])\nnames!(df, [:a, :b, :a])  # throws ArgumentError\nnames!(df, [:a, :b, :a], allow_duplicates=true)  # renames second :a to :a_1\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.nonunique",
    "page": "Utilities",
    "title": "DataFrames.nonunique",
    "category": "Function",
    "text": "Indexes of duplicate rows (a row that is a duplicate of a prior row)\n\nnonunique(df::AbstractDataFrame)\nnonunique(df::AbstractDataFrame, cols)\n\nArguments\n\ndf : the AbstractDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.) specifying the column(s) to compare\n\nResult\n\n::Vector{Bool} : indicates whether the row is a duplicate of some prior row\n\nSee also unique and unique!.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf = vcat(df, df)\nnonunique(df)\nnonunique(df, 1)\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.rename",
    "page": "Utilities",
    "title": "DataFrames.rename",
    "category": "Function",
    "text": "Rename columns\n\nrename!(df::AbstractDataFrame, from::Symbol, to::Symbol)\nrename!(df::AbstractDataFrame, d::Associative)\nrename!(f::Function, df::AbstractDataFrame)\nrename(df::AbstractDataFrame, from::Symbol, to::Symbol)\nrename(f::Function, df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\nd : an Associative type that maps the original name to a new name\nf : a function that has the old column name (a symbol) as input and new column name (a symbol) as output\n\nResult\n\n::AbstractDataFrame : the updated result\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nrename(x -> @compat(Symbol)(uppercase(string(x))), df)\nrename(df, @compat(Dict(:i=>:A, :x=>:X)))\nrename(df, :y, :Y)\nrename!(df, @compat(Dict(:i=>:A, :x=>:X)))\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.rename!",
    "page": "Utilities",
    "title": "DataFrames.rename!",
    "category": "Function",
    "text": "Rename columns\n\nrename!(df::AbstractDataFrame, from::Symbol, to::Symbol)\nrename!(df::AbstractDataFrame, d::Associative)\nrename!(f::Function, df::AbstractDataFrame)\nrename(df::AbstractDataFrame, from::Symbol, to::Symbol)\nrename(f::Function, df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\nd : an Associative type that maps the original name to a new name\nf : a function that has the old column name (a symbol) as input and new column name (a symbol) as output\n\nResult\n\n::AbstractDataFrame : the updated result\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nrename(x -> @compat(Symbol)(uppercase(string(x))), df)\nrename(df, @compat(Dict(:i=>:A, :x=>:X)))\nrename(df, :y, :Y)\nrename!(df, @compat(Dict(:i=>:A, :x=>:X)))\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.tail",
    "page": "Utilities",
    "title": "DataFrames.tail",
    "category": "Function",
    "text": "Show the first or last part of an AbstractDataFrame\n\nhead(df::AbstractDataFrame, r::Int = 6)\ntail(df::AbstractDataFrame, r::Int = 6)\n\nArguments\n\ndf : the AbstractDataFrame\nr : the number of rows to show\n\nResult\n\n::AbstractDataFrame : the first or last part of df\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nhead(df)\ntail(df)\n\n\n\n"
},

{
    "location": "lib/utilities.html#Base.unique",
    "page": "Utilities",
    "title": "Base.unique",
    "category": "Function",
    "text": "unique(A::CategoricalArray)\nunique(A::NullableCategoricalArray)\n\nReturn levels which appear in A, in the same order as levels (and not in their order of appearance). This function is significantly slower than levels since it needs to check whether levels are used or not.\n\n\n\nDelete duplicate rows\n\nunique(df::AbstractDataFrame)\nunique(df::AbstractDataFrame, cols)\nunique!(df::AbstractDataFrame)\nunique!(df::AbstractDataFrame, cols)\n\nArguments\n\ndf : the AbstractDataFrame\ncols :  column indicator (Symbol, Int, Vector{Symbol}, etc.)\n\nspecifying the column(s) to compare.\n\nResult\n\n::AbstractDataFrame : the updated version of df with unique rows.\n\nWhen cols is specified, the return DataFrame contains complete rows, retaining in each case the first instance for which df[cols] is unique.\n\nSee also nonunique.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf = vcat(df, df)\nunique(df)   # doesn't modify df\nunique(df, 1)\nunique!(df)  # modifies df\n\n\n\n"
},

{
    "location": "lib/utilities.html#DataFrames.unique!",
    "page": "Utilities",
    "title": "DataFrames.unique!",
    "category": "Function",
    "text": "Delete duplicate rows\n\nunique(df::AbstractDataFrame)\nunique(df::AbstractDataFrame, cols)\nunique!(df::AbstractDataFrame)\nunique!(df::AbstractDataFrame, cols)\n\nArguments\n\ndf : the AbstractDataFrame\ncols :  column indicator (Symbol, Int, Vector{Symbol}, etc.)\n\nspecifying the column(s) to compare.\n\nResult\n\n::AbstractDataFrame : the updated version of df with unique rows.\n\nWhen cols is specified, the return DataFrame contains complete rows, retaining in each case the first instance for which df[cols] is unique.\n\nSee also nonunique.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf = vcat(df, df)\nunique(df)   # doesn't modify df\nunique(df, 1)\nunique!(df)  # modifies df\n\n\n\n"
},

{
    "location": "lib/utilities.html#Utilities-1",
    "page": "Utilities",
    "title": "Utilities",
    "category": "section",
    "text": "Pages = [\"utilities.md\"]eltypes\nhead\ncomplete_cases\ncomplete_cases!\ndescribe\ndump\nnames!\nnonunique\nrename\nrename!\ntail\nunique\nunique!"
},

{
    "location": "lib/manipulation.html#",
    "page": "Data manipulation",
    "title": "Data manipulation",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/manipulation.html#Data-Manipulation-1",
    "page": "Data manipulation",
    "title": "Data Manipulation",
    "category": "section",
    "text": "Pages = [\"manipulation.md\"]"
},

{
    "location": "lib/manipulation.html#Base.join",
    "page": "Data manipulation",
    "title": "Base.join",
    "category": "Function",
    "text": "Join two DataFrames\n\njoin(df1::AbstractDataFrame,\n     df2::AbstractDataFrame;\n     on::Union{Symbol, Vector{Symbol}} = Symbol[],\n     kind::Symbol = :inner)\n\nArguments\n\ndf1, df2 : the two AbstractDataFrames to be joined\n\nKeyword Arguments\n\non : a Symbol or Vector{Symbol}, the column(s) used as keys when joining; required argument except for kind = :cross\nkind : the type of join, options include:\n:inner : only include rows with keys that match in both df1 and df2, the default\n:outer : include all rows from df1 and df2\n:left : include all rows from df1\n:right : include all rows from df2\n:semi : return rows of df1 that match with the keys in df2\n:anti : return rows of df1 that do not match with the keys in df2\n:cross : a full Cartesian product of the key combinations; every row of df1 is matched with every row of df2\n\nNull values are filled in where needed to complete joins.\n\nResult\n\n::DataFrame : the joined DataFrame\n\nExamples\n\nname = DataFrame(ID = [1, 2, 3], Name = [\"John Doe\", \"Jane Doe\", \"Joe Blogs\"])\njob = DataFrame(ID = [1, 2, 4], Job = [\"Lawyer\", \"Doctor\", \"Farmer\"])\n\njoin(name, job, on = :ID)\njoin(name, job, on = :ID, kind = :outer)\njoin(name, job, on = :ID, kind = :left)\njoin(name, job, on = :ID, kind = :right)\njoin(name, job, on = :ID, kind = :semi)\njoin(name, job, on = :ID, kind = :anti)\njoin(name, job, kind = :cross)\n\n\n\n"
},

{
    "location": "lib/manipulation.html#Joins-1",
    "page": "Data manipulation",
    "title": "Joins",
    "category": "section",
    "text": "join"
},

{
    "location": "lib/manipulation.html#DataFrames.melt",
    "page": "Data manipulation",
    "title": "DataFrames.melt",
    "category": "Function",
    "text": "Stacks a DataFrame; convert from a wide to long format; see stack.\n\n\n\n"
},

{
    "location": "lib/manipulation.html#DataFrames.stack",
    "page": "Data manipulation",
    "title": "DataFrames.stack",
    "category": "Function",
    "text": "Stacks a DataFrame; convert from a wide to long format\n\nstack(df::AbstractDataFrame, measure_vars, id_vars)\nstack(df::AbstractDataFrame, measure_vars)\nstack(df::AbstractDataFrame)\nmelt(df::AbstractDataFrame, id_vars, measure_vars)\nmelt(df::AbstractDataFrame, id_vars)\n\nArguments\n\ndf : the AbstractDataFrame to be stacked\nmeasure_vars : the columns to be stacked (the measurement variables), a normal column indexing type, like a Symbol, Vector{Symbol}, Int, etc.; for melt, defaults to all variables that are not id_vars\nid_vars : the identifier columns that are repeated during stacking, a normal column indexing type; for stack defaults to all variables that are not measure_vars\n\nIf neither measure_vars or id_vars are given, measure_vars defaults to all floating point columns.\n\nResult\n\n::DataFrame : the long-format dataframe with column :value holding the values of the stacked columns (measure_vars), with column :variable a Vector of Symbols with the measure_vars name, and with columns for each of the id_vars.\n\nSee also stackdf and meltdf for stacking methods that return a view into the original DataFrame. See unstack for converting from long to wide format.\n\nExamples\n\nd1 = DataFrame(a = repeat([1:3;], inner = [4]),\n               b = repeat([1:4;], inner = [3]),\n               c = randn(12),\n               d = randn(12),\n               e = map(string, 'a':'l'))\n\nd1s = stack(d1, [:c, :d])\nd1s2 = stack(d1, [:c, :d], [:a])\nd1m = melt(d1, [:a, :b, :e])\n\n\n\n"
},

{
    "location": "lib/manipulation.html#DataFrames.unstack",
    "page": "Data manipulation",
    "title": "DataFrames.unstack",
    "category": "Function",
    "text": "Unstacks a DataFrame; convert from a long to wide format\n\nunstack(df::AbstractDataFrame, rowkey, colkey, value)\nunstack(df::AbstractDataFrame, colkey, value)\nunstack(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame to be unstacked\nrowkey : the column with a unique key for each row, if not given, find a key by grouping on anything not a colkey or value\ncolkey : the column holding the column names in wide format, defaults to :variable\nvalue : the value column, defaults to :value\n\nResult\n\n::DataFrame : the wide-format dataframe\n\nExamples\n\nwide = DataFrame(id = 1:12,\n                 a  = repeat([1:3;], inner = [4]),\n                 b  = repeat([1:4;], inner = [3]),\n                 c  = randn(12),\n                 d  = randn(12))\n\nlong = stack(wide)\nwide0 = unstack(long)\nwide1 = unstack(long, :variable, :value)\nwide2 = unstack(long, :id, :variable, :value)\n\nNote that there are some differences between the widened results above.\n\n\n\n"
},

{
    "location": "lib/manipulation.html#DataFrames.stackdf",
    "page": "Data manipulation",
    "title": "DataFrames.stackdf",
    "category": "Function",
    "text": "A stacked view of a DataFrame (long format)\n\nLike stack and melt, but a view is returned rather than data copies.\n\nstackdf(df::AbstractDataFrame, measure_vars, id_vars)\nstackdf(df::AbstractDataFrame, measure_vars)\nmeltdf(df::AbstractDataFrame, id_vars, measure_vars)\nmeltdf(df::AbstractDataFrame, id_vars)\n\nArguments\n\ndf : the wide AbstractDataFrame\nmeasure_vars : the columns to be stacked (the measurement variables), a normal column indexing type, like a Symbol, Vector{Symbol}, Int, etc.; for melt, defaults to all variables that are not id_vars\nid_vars : the identifier columns that are repeated during stacking, a normal column indexing type; for stack defaults to all variables that are not measure_vars\n\nResult\n\n::DataFrame : the long-format dataframe with column :value holding the values of the stacked columns (measure_vars), with column :variable a Vector of Symbols with the measure_vars name, and with columns for each of the id_vars.\n\nThe result is a view because the columns are special AbstractVectors that return indexed views into the original DataFrame.\n\nExamples\n\nd1 = DataFrame(a = repeat([1:3;], inner = [4]),\n               b = repeat([1:4;], inner = [3]),\n               c = randn(12),\n               d = randn(12),\n               e = map(string, 'a':'l'))\n\nd1s = stackdf(d1, [:c, :d])\nd1s2 = stackdf(d1, [:c, :d], [:a])\nd1m = meltdf(d1, [:a, :b, :e])\n\n\n\n"
},

{
    "location": "lib/manipulation.html#DataFrames.meltdf",
    "page": "Data manipulation",
    "title": "DataFrames.meltdf",
    "category": "Function",
    "text": "A stacked view of a DataFrame (long format); see stackdf\n\n\n\n"
},

{
    "location": "lib/manipulation.html#Reshaping-1",
    "page": "Data manipulation",
    "title": "Reshaping",
    "category": "section",
    "text": "melt\nstack\nunstack\nstackdf\nmeltdf"
},

{
    "location": "NEWS.html#",
    "page": "Release Notes",
    "title": "Release Notes",
    "category": "page",
    "text": ""
},

{
    "location": "NEWS.html#DataFrames-v0.6.11-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.6.11 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#Changes-1",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "New documentation based on Documenter ([#929])\nSupport new fit indicator functions for statistical models ([#921]).\nAdd string literals csv, csv2, wsv, and tsv ([#918]) \nAdd a readtable argument for optional name normalization ([#896]) "
},

{
    "location": "NEWS.html#DataFrames-v0.6.6-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.6.6 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#Deprecations-1",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates array(df, ...) in favor of convert(Array, df, ...) ([#806])\nDeprecates DataArray(df, T) in favor of convert(DataArray{T}, df) ([#806])"
},

{
    "location": "NEWS.html#DataFrames-v0.6.3-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.6.3 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#Deprecations-2",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Removes save and loaddf, since the format was not compatible across Julia and DataFrames versions ([#790]). Use writetable or JLD to save DataFrames"
},

{
    "location": "NEWS.html#DataFrames-v0.6.1-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.6.1 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-1",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "writetable supports append option ([#755])"
},

{
    "location": "NEWS.html#Changes-2",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "Faster read_rda ([#754], [#759])"
},

{
    "location": "NEWS.html#DataFrames-v0.6.0-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.6.0 Release Notes",
    "category": "section",
    "text": "Focus on performance improvements and rooting out bugs in corner cases."
},

{
    "location": "NEWS.html#New-features-2",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "Constructor for empty DataFrames allows specifying PDAs ([#725])\nstack(df) and melt(df), which take FloatingPoint vars as measure vars ([#734])\nNew convenience methods for unstack ([#734])\nconvertdataframes option added to read_rda ([#751])"
},

{
    "location": "NEWS.html#Changes-3",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "vcat(dfs) handles container and eltype promotion ([#747])\njoin finally handles DataFrames with no non-key columns ([#749])\nsorting methods throw an error when args meant for cols are passed to by ([#749])\nrename! and rename throw when column to be renamed does not exist ([#749])\nnames!, rename!, and rename for DataFrames now return DataFrames ([#749])"
},

{
    "location": "NEWS.html#Deprecations-3",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates by(df, cols, [symbol(s)]) in favor of aggregate(df, cols, [function(s)]) ([#726])\nRemoves pivottable in favor of other reshaping methods ([#734])\nDeprecates nullable!(..., ::AbstractDataFrame) in favor of nullable!(::DataFrame, ...) ([#752])\nDeprecates keys(df), values(df) ([#752])\nRenames insert!(df, df) to merge!(df, dfs...) ([#752])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.12-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.12 Release Notes",
    "category": "section",
    "text": "Track changes to JuliaLang/julia"
},

{
    "location": "NEWS.html#DataFrames-v0.5.11-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.11 Release Notes",
    "category": "section",
    "text": "Track changes to JuliaLang/julia"
},

{
    "location": "NEWS.html#DataFrames-v0.5.10-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.10 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-3",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "Formulas handle three-way (and higher) interactions ([#700])"
},

{
    "location": "NEWS.html#Changes-4",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "Now using ReadTheDocs for documentation"
},

{
    "location": "NEWS.html#DataFrames-v0.5.9-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.9 Release Notes",
    "category": "section",
    "text": "Track changes to JuliaLang/julia"
},

{
    "location": "NEWS.html#DataFrames-v0.5.8-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.8 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-4",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "Extends StatsBase.predict to take a DataFrame as a predictor ([#679])\ncoefnames handles random-effect terms ([#662])"
},

{
    "location": "NEWS.html#Deprecations-4",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates DataFrame(::Dict, ...) in favor of convert ([#626])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.7-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.7 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-5",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "deleterows!(df::DataFrame, inds) ([#635])"
},

{
    "location": "NEWS.html#Changes-5",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "empty!(::DataFrame) and insert!(::DataFrame, ...) now operate in place ([#634])\nAll exported higher-order functions now handle do-block syntax ([#643])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.6-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.6 Release Notes",
    "category": "section",
    "text": "Track changes to JuliaLang/julia"
},

{
    "location": "NEWS.html#DataFrames-v0.5.5-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.5 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-6",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "Support fitting arbitrary StatisticalModels ([#571])\nTest coverage now tracked via Coveralls.io ([#597])"
},

{
    "location": "NEWS.html#Changes-6",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "show(::AbstractDataFrame) now shows all columns by default"
},

{
    "location": "NEWS.html#Deprecations-5",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates DataFrame(::Any...), DataFrame(::Associative) ([#610])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.4-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.4 Release Notes",
    "category": "section",
    "text": ""
},

{
    "location": "NEWS.html#New-features-7",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "push! methods add a row to a DataFrame ([#621])\nTest coverage now tracked via Coveralls.io ([#597])"
},

{
    "location": "NEWS.html#Changes-7",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "IO functions ensure column names are valid symbols ([#563])\nsetindex! methods now return the updated DataFrame"
},

{
    "location": "NEWS.html#Deprecations-6",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates DataFrame(::Int, ::Int) ([#561])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.3-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.3 Release Notes",
    "category": "section",
    "text": "Internal changes to adjust to [JuliaLang/julia#5897]"
},

{
    "location": "NEWS.html#DataFrames-v0.5.2-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.2 Release Notes",
    "category": "section",
    "text": "Continues trend of stripping down features and improving core functionality."
},

{
    "location": "NEWS.html#New-features-8",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "append!(::AbstractDataFrame, ::AbstractDataFrame) ([#506])\njoin supports :semi-, :anti- and :cross-joins ([#524], [#536])\nImplement eltypes argument in readtable ([#497])\nRead from generic IO objects ([#499])"
},

{
    "location": "NEWS.html#Changes-8",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "Convert to using only symbols (no more strings) for column names ([#509])\nRenames stack_df, melt_df, pivot_table to stackdf, meltdf, pivottable ([#538])\nRenames duplicated, drop_duplicates! to nonunique, unique! ([#538])\nRenames load_df to loaddf ([#538])\nRenames types to eltypes ([#539])\nRenames readtable argument colnames to names ([#497])"
},

{
    "location": "NEWS.html#Deprecations-7",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Removes expression-based indexing, including with, within!, based_on, etc. ([#492])\nRemoves DataStream ([#492])\nRemoves NamedArray ([#492])\nRemoves column groupings (set_groups, get_groups, etc.)  ([#492])\nRemoves specific colwise and rowwise functions (rowsums, colnorms, etc.) ([#492])\nRemoves @DataFrame and @transform ([#492])\nDeprecates natural joins: the key must be specified now ([#536])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.1-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.1 Release Notes",
    "category": "section",
    "text": "Removing prototype features until core functionality is farther along."
},

{
    "location": "NEWS.html#Changes-9",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "Write Formulas without quoting, thanks to the @~ macro ([JuliaLang/julia#4882])\nRenames EachCol, EachRow to eachcol, eachrow ([#474])\neachrow returns a DataFrameRow ([#474])\nSubDataFrames are now immutable ([#474])"
},

{
    "location": "NEWS.html#Deprecations-8",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Removes IndexedVector ([#483])\nRemoves Blocks.jl functionality ([#483])\nRemoves methods that treat DataFrame like a matrix, e.g round, sin ([#484])\nDeprecates sub's alias subset ([#474])"
},

{
    "location": "NEWS.html#DataFrames-v0.5.0-Release-Notes-1",
    "page": "Release Notes",
    "title": "DataFrames v0.5.0 Release Notes",
    "category": "section",
    "text": "Improved I/O and more-Julian idioms."
},

{
    "location": "NEWS.html#New-features-9",
    "page": "Release Notes",
    "title": "New features",
    "category": "section",
    "text": "Write HTML tables via writemime ([#433])\nRead whitespace-delimited input ([#443])\nRead input with C-style escapes ([#454])"
},

{
    "location": "NEWS.html#Changes-10",
    "page": "Release Notes",
    "title": "Changes",
    "category": "section",
    "text": "sort interface updated to better match mainline Julia ([#389])\nnames!, rename!, and delete! now return the updated Index, rather than the names in the Index ([#445])\nRenames coltypes, colnames, clean_colnames! to types, names, cleannames! ([#469])\nVarious improvements to print/show methods"
},

{
    "location": "NEWS.html#Deprecations-9",
    "page": "Release Notes",
    "title": "Deprecations",
    "category": "section",
    "text": "Deprecates rbind, cbind and vecbind deprecated in favor of hcat and vcat ([#453])[#389]: https://github.com/JuliaStats/DataFrames.jl/issues/389 [#433]: https://github.com/JuliaStats/DataFrames.jl/issues/433 [#443]: https://github.com/JuliaStats/DataFrames.jl/issues/443 [#445]: https://github.com/JuliaStats/DataFrames.jl/issues/445 [#453]: https://github.com/JuliaStats/DataFrames.jl/issues/453 [#454]: https://github.com/JuliaStats/DataFrames.jl/issues/454 [#469]: https://github.com/JuliaStats/DataFrames.jl/issues/469 [#474]: https://github.com/JuliaStats/DataFrames.jl/issues/474 [#483]: https://github.com/JuliaStats/DataFrames.jl/issues/483 [#484]: https://github.com/JuliaStats/DataFrames.jl/issues/484 [#492]: https://github.com/JuliaStats/DataFrames.jl/issues/492 [#497]: https://github.com/JuliaStats/DataFrames.jl/issues/497 [#499]: https://github.com/JuliaStats/DataFrames.jl/issues/499 [#506]: https://github.com/JuliaStats/DataFrames.jl/issues/506 [#509]: https://github.com/JuliaStats/DataFrames.jl/issues/509 [#524]: https://github.com/JuliaStats/DataFrames.jl/issues/524 [#536]: https://github.com/JuliaStats/DataFrames.jl/issues/536 [#538]: https://github.com/JuliaStats/DataFrames.jl/issues/538 [#539]: https://github.com/JuliaStats/DataFrames.jl/issues/539 [#561]: https://github.com/JuliaStats/DataFrames.jl/issues/561 [#563]: https://github.com/JuliaStats/DataFrames.jl/issues/563 [#571]: https://github.com/JuliaStats/DataFrames.jl/issues/571 [#597]: https://github.com/JuliaStats/DataFrames.jl/issues/597 [#610]: https://github.com/JuliaStats/DataFrames.jl/issues/610 [#621]: https://github.com/JuliaStats/DataFrames.jl/issues/621 [#626]: https://github.com/JuliaStats/DataFrames.jl/issues/626 [#634]: https://github.com/JuliaStats/DataFrames.jl/issues/634 [#635]: https://github.com/JuliaStats/DataFrames.jl/issues/635 [#643]: https://github.com/JuliaStats/DataFrames.jl/issues/643 [#662]: https://github.com/JuliaStats/DataFrames.jl/issues/662 [#679]: https://github.com/JuliaStats/DataFrames.jl/issues/679 [#700]: https://github.com/JuliaStats/DataFrames.jl/issues/700 [#725]: https://github.com/JuliaStats/DataFrames.jl/issues/725 [#726]: https://github.com/JuliaStats/DataFrames.jl/issues/726 [#734]: https://github.com/JuliaStats/DataFrames.jl/issues/734 [#747]: https://github.com/JuliaStats/DataFrames.jl/issues/747 [#749]: https://github.com/JuliaStats/DataFrames.jl/issues/749 [#751]: https://github.com/JuliaStats/DataFrames.jl/issues/751 [#752]: https://github.com/JuliaStats/DataFrames.jl/issues/752 [#754]: https://github.com/JuliaStats/DataFrames.jl/issues/754 [#755]: https://github.com/JuliaStats/DataFrames.jl/issues/755 [#759]: https://github.com/JuliaStats/DataFrames.jl/issues/759 [#790]: https://github.com/JuliaStats/DataFrames.jl/issues/790 [#806]: https://github.com/JuliaStats/DataFrames.jl/issues/806[JuliaLang/julia#4882]: https://github.com/JuliaLang/julia/issues/4882 [JuliaLang/julia#5897]: https://github.com/JuliaLang/julia/issues/5897"
},

{
    "location": "LICENSE.html#",
    "page": "License",
    "title": "License",
    "category": "page",
    "text": "DataFrames.jl is licensed under the MIT License:Copyright (c) 2012-2015: Harlan Harris, EPRI (Tom Short's code), Chris DuBois, John Myles White, and other contributors.Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."
},

]}
