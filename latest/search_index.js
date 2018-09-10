var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Introduction",
    "title": "Introduction",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#DataFrames.jl-1",
    "page": "Introduction",
    "title": "DataFrames.jl",
    "category": "section",
    "text": "Welcome to the DataFrames documentation! This resource aims to teach you everything you need to know to get up and running with tabular data manipulation using the DataFrames.jl package and the Julia language. If there is something you expect DataFrames to be capable of, but cannot figure out how to do, please reach out with questions in Domains/Data on Discourse. Please report bugs by opening an issue. You can follow the source links throughout the documentation to jump right to the source files on GitHub to make pull requests for improving the documentation and function capabilities. Please review DataFrames contributing guidelines before submitting your first PR! Information on specific versions can be found on the Release page."
},

{
    "location": "index.html#Package-Manual-1",
    "page": "Introduction",
    "title": "Package Manual",
    "category": "section",
    "text": "Pages = [\"man/getting_started.md\",\n         \"man/joins.md\",\n         \"man/split_apply_combine.md\",\n         \"man/reshaping_and_pivoting.md\",\n         \"man/sorting.md\",\n         \"man/categorical.md\",\n         \"man/querying_frameworks.md\"]\nDepth = 2"
},

{
    "location": "index.html#API-1",
    "page": "Introduction",
    "title": "API",
    "category": "section",
    "text": "Pages = [\"lib/types.md\", \"lib/functions.md\"]\nDepth = 2"
},

{
    "location": "index.html#Index-1",
    "page": "Introduction",
    "title": "Index",
    "category": "section",
    "text": "Pages = [\"lib/types.md\", \"lib/functions.md\"]"
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
    "text": "The DataFrames package is available through the Julia package system and can be installed using the following command:Pkg.add(\"DataFrames\")Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed using DataFrames to bring all of the relevant variables into your current namespace."
},

{
    "location": "man/getting_started.html#The-DataFrame-Type-1",
    "page": "Getting Started",
    "title": "The DataFrame Type",
    "category": "section",
    "text": "The DataFrame type can be used to represent data tables, each column of which is a vector. You can specify the columns using keyword arguments or pairs:julia> using DataFrames\n\njulia> DataFrame(A = 1:4, B = [\"M\", \"F\", \"F\", \"M\"])\n4×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │\n│ 2   │ 2 │ F │\n│ 3   │ 3 │ F │\n│ 4   │ 4 │ M │\n"
},

{
    "location": "man/getting_started.html#Constructing-Column-by-Column-1",
    "page": "Getting Started",
    "title": "Constructing Column by Column",
    "category": "section",
    "text": "It is also possible to construct a DataFrame one column at a time.julia> df = DataFrame()\n0×0 DataFrames.DataFrame\n\n\njulia> df[:A] = 1:8\n1:8\n\njulia> df[:B] = [\"M\", \"F\", \"F\", \"M\", \"F\", \"M\", \"M\", \"F\"]\n8-element Array{String,1}:\n \"M\"\n \"F\"\n \"F\"\n \"M\"\n \"F\"\n \"M\"\n \"M\"\n \"F\"\n\njulia> df\n8×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │\n│ 2   │ 2 │ F │\n│ 3   │ 3 │ F │\n│ 4   │ 4 │ M │\n│ 5   │ 5 │ F │\n│ 6   │ 6 │ M │\n│ 7   │ 7 │ M │\n│ 8   │ 8 │ F │\nThe DataFrame we build in this way has 8 rows and 2 columns. You can check this using the size function:julia> size(df, 1) == 8\ntrue\n\njulia> size(df, 2) == 2\ntrue\n\njulia> size(df) == (8, 2)\ntrue\n"
},

{
    "location": "man/getting_started.html#Constructing-Row-by-Row-1",
    "page": "Getting Started",
    "title": "Constructing Row by Row",
    "category": "section",
    "text": "It is also possible to construct a DataFrame row by row.First a DataFrame with empty columns is constructed:julia> df = DataFrame(A = Int[], B = String[])\n0×2 DataFrames.DataFrameRows can then be added as Vectors, where the row order matches the columns order:julia> push!(df, [1, \"M\"])\n1×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │Rows can also be added as Dicts, where the dictionary keys match the column names:julia> push!(df, Dict(:B => \"F\", :A => 2))\n2×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │\n│ 2   │ 2 │ F │Note that constructing a DataFrame row by row is significantly less performant than constructing it all at once, or column by column. For many use-cases this will not matter, but for very large DataFrames  this may be a consideration."
},

{
    "location": "man/getting_started.html#Working-with-Data-Frames-1",
    "page": "Getting Started",
    "title": "Working with Data Frames",
    "category": "section",
    "text": ""
},

{
    "location": "man/getting_started.html#Taking-a-Subset-1",
    "page": "Getting Started",
    "title": "Taking a Subset",
    "category": "section",
    "text": "We can also look at small subsets of the data in a couple of different ways:julia> head(df)\n6×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │\n│ 2   │ 2 │ F │\n│ 3   │ 3 │ F │\n│ 4   │ 4 │ M │\n│ 5   │ 5 │ F │\n│ 6   │ 6 │ M │\n\njulia> tail(df)\n6×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 3 │ F │\n│ 2   │ 4 │ M │\n│ 3   │ 5 │ F │\n│ 4   │ 6 │ M │\n│ 5   │ 7 │ M │\n│ 6   │ 8 │ F │\n\njulia> df[1:3, :]\n3×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ 1 │ M │\n│ 2   │ 2 │ F │\n│ 3   │ 3 │ F │\n"
},

{
    "location": "man/getting_started.html#Summarizing-with-describe-1",
    "page": "Getting Started",
    "title": "Summarizing with describe",
    "category": "section",
    "text": "Having seen what some of the rows look like, we can try to summarize the entire data set using describe:julia> describe(df)\n2×8 DataFrames.DataFrame\n│ Row │ variable │ mean │ min │ median │ max │ nunique │ nmissing │ eltype │\n├─────┼──────────┼──────┼─────┼────────┼─────┼─────────┼──────────┼────────┤\n│ 1   │ A        │ 4.5  │ 1   │ 4.5    │ 8   │         │          │ Int64  │\n│ 2   │ B        │      │ F   │        │ M   │ 2       │          │ String │\nTo access individual columns of the dataset, you refer to the column names by their symbol or by their numerical index. Here we extract the first column, :A, and use it to compute the mean and variance.julia> mean(df[:A]) == mean(df[1]) == 4.5\ntrue\n\njulia> var(df[:A]) ==  var(df[1]) == 6.0\ntrue\n"
},

{
    "location": "man/getting_started.html#Column-Wise-Operations-1",
    "page": "Getting Started",
    "title": "Column-Wise Operations",
    "category": "section",
    "text": "We can also apply a function to each column of a DataFrame with the colwise function. For example:julia> df = DataFrame(A = 1:4, B = 4.0:-1.0:1.0)\n4×2 DataFrames.DataFrame\n│ Row │ A │ B   │\n├─────┼───┼─────┤\n│ 1   │ 1 │ 4.0 │\n│ 2   │ 2 │ 3.0 │\n│ 3   │ 3 │ 2.0 │\n│ 4   │ 4 │ 1.0 │\n\njulia> colwise(sum, df)\n2-element Array{Real,1}:\n 10\n 10.0"
},

{
    "location": "man/getting_started.html#Importing-and-Exporting-Data-(I/O)-1",
    "page": "Getting Started",
    "title": "Importing and Exporting Data (I/O)",
    "category": "section",
    "text": "For reading and writing tabular data from CSV and other delimited text files, use the CSV.jl package.If you have not used the CSV.jl package before then you may need to install it first:Pkg.add(\"CSV\")The CSV.jl functions are not loaded automatically and must be imported into the session.using CSVA dataset can now be read from a CSV file at path input usingCSV.read(input)A DataFrame can be written to a CSV file at path output usingdf = DataFrame(x = 1, y = 2)\nCSV.write(output, df)The behavior of CSV functions can be adapted via keyword arguments. For more information, use the REPL help-mode or checkout the online CSV.jl documentation."
},

{
    "location": "man/getting_started.html#Loading-a-Classic-Data-Set-1",
    "page": "Getting Started",
    "title": "Loading a Classic Data Set",
    "category": "section",
    "text": "To see more of the functionality for working with DataFrame objects, we need a more complex data set to work with. We can access Fisher\'s iris data set using the following functions:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa  │\n│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa  │\n│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa  │\n│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │\n│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa  │\n"
},

{
    "location": "man/getting_started.html#Querying-DataFrames-1",
    "page": "Getting Started",
    "title": "Querying DataFrames",
    "category": "section",
    "text": "While the DataFrames package provides basic data manipulation capabilities, users are encouraged to use the Query.jl, which provides a LINQ-like interface to a large number of data sources, including DataFrame instances. See the Querying frameworks  section for more information."
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
    "text": "We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:julia> using DataFrames\n\njulia> names = DataFrame(ID = [20, 40], Name = [\"John Doe\", \"Jane Doe\"])\n2×2 DataFrames.DataFrame\n│ Row │ ID │ Name     │\n├─────┼────┼──────────┤\n│ 1   │ 20 │ John Doe │\n│ 2   │ 40 │ Jane Doe │\n\njulia> jobs = DataFrame(ID = [20, 40], Job = [\"Lawyer\", \"Doctor\"])\n2×2 DataFrames.DataFrame\n│ Row │ ID │ Job    │\n├─────┼────┼────────┤\n│ 1   │ 20 │ Lawyer │\n│ 2   │ 40 │ Doctor │\nWe might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the join function:julia> join(names, jobs, on = :ID)\n2×3 DataFrames.DataFrame\n│ Row │ ID │ Name     │ Job    │\n├─────┼────┼──────────┼────────┤\n│ 1   │ 20 │ John Doe │ Lawyer │\n│ 2   │ 40 │ Jane Doe │ Doctor │\nIn relational database theory, this operation is generally referred to as a join. The columns used to determine which rows should be combined during a join are called keys.There are seven kinds of joins supported by the DataFrames package:Inner: The output contains rows for values of the key that exist in both the first (left) and second (right) arguments to join.\nLeft: The output contains rows for values of the key that exist in the first (left) argument to join, whether or not that value exists in the second (right) argument.\nRight: The output contains rows for values of the key that exist in the second (right) argument to join, whether or not that value exists in the first (left) argument.\nOuter: The output contains rows for values of the key that exist in the first (left) or second (right) argument to join.\nSemi: Like an inner join, but output is restricted to columns from the first (left) argument to join.\nAnti: The output contains rows for values of the key that exist in the first (left) but not the second (right) argument to join. As with semi joins, output is restricted to columns from the first (left) argument.\nCross: The output is the cartesian product of rows from the first (left) and second (right) arguments to join.See the Wikipedia page on SQL joins for more information.You can control the kind of join that join performs using the kind keyword argument:julia> jobs = DataFrame(ID = [20, 60], Job = [\"Lawyer\", \"Astronaut\"])\n2×2 DataFrames.DataFrame\n│ Row │ ID │ Job       │\n├─────┼────┼───────────┤\n│ 1   │ 20 │ Lawyer    │\n│ 2   │ 60 │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :inner)\n1×3 DataFrames.DataFrame\n│ Row │ ID │ Name     │ Job    │\n├─────┼────┼──────────┼────────┤\n│ 1   │ 20 │ John Doe │ Lawyer │\n\njulia> join(names, jobs, on = :ID, kind = :left)\n2×3 DataFrames.DataFrame\n│ Row │ ID │ Name     │ Job     │\n├─────┼────┼──────────┼─────────┤\n│ 1   │ 20 │ John Doe │ Lawyer  │\n│ 2   │ 40 │ Jane Doe │ missing │\n\njulia> join(names, jobs, on = :ID, kind = :right)\n2×3 DataFrames.DataFrame\n│ Row │ ID │ Name     │ Job       │\n├─────┼────┼──────────┼───────────┤\n│ 1   │ 20 │ John Doe │ Lawyer    │\n│ 2   │ 60 │ missing  │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :outer)\n3×3 DataFrames.DataFrame\n│ Row │ ID │ Name        │ Job       │\n├─────┼────┼─────────────┼───────────┤\n│ 1   │ 20 │ John Doe    │ Lawyer    │\n│ 2   │ 40 │ Jane Doe    │ missing   │\n│ 3   │ 60 │ missing     │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :semi)\n1×2 DataFrames.DataFrame\n│ Row │ ID │ Name     │\n├─────┼────┼──────────┤\n│ 1   │ 20 │ John Doe │\n\njulia> join(names, jobs, on = :ID, kind = :anti)\n1×2 DataFrames.DataFrame\n│ Row │ ID │ Name     │\n├─────┼────┼──────────┤\n│ 1   │ 40 │ Jane Doe │\nCross joins are the only kind of join that does not use a key:julia> join(names, jobs, kind = :cross)\n4×4 DataFrames.DataFrame\n│ Row │ ID │ Name     │ ID_1 │ Job       │\n├─────┼────┼──────────┼──────┼───────────┤\n│ 1   │ 20 │ John Doe │ 20   │ Lawyer    │\n│ 2   │ 20 │ John Doe │ 60   │ Astronaut │\n│ 3   │ 40 │ Jane Doe │ 20   │ Lawyer    │\n│ 4   │ 40 │ Jane Doe │ 60   │ Astronaut │\nIn order to join data tables on keys which have different names, you must first rename them so that they match. This can be done using rename!:julia> a = DataFrame(ID = [20, 40], Name = [\"John Doe\", \"Jane Doe\"])\n2×2 DataFrames.DataFrame\n│ Row │ ID │ Name     │\n├─────┼────┼──────────┤\n│ 1   │ 20 │ John Doe │\n│ 2   │ 40 │ Jane Doe │\n\njulia> b = DataFrame(IDNew = [20, 40], Job = [\"Lawyer\", \"Doctor\"])\n2×2 DataFrames.DataFrame\n│ Row │ IDNew │ Job    │\n├─────┼───────┼────────┤\n│ 1   │ 20    │ Lawyer │\n│ 2   │ 40    │ Doctor │\n\njulia> rename!(b, :IDNew => :ID)\n2×2 DataFrames.DataFrame\n│ Row │ ID │ Job    │\n├─────┼────┼────────┤\n│ 1   │ 20 │ Lawyer │\n│ 2   │ 40 │ Doctor │\n\njulia> join(a, b, on = :ID, kind = :inner)\n2×3 DataFrames.DataFrame\n│ Row │ ID │ Name     │ Job    │\n├─────┼────┼──────────┼────────┤\n│ 1   │ 20 │ John Doe │ Lawyer │\n│ 2   │ 40 │ Jane Doe │ Doctor │\nOr renaming multiple columns at a time:julia> a = DataFrame(City = [\"Amsterdam\", \"London\", \"London\", \"New York\", \"New York\"],\n                     Job = [\"Lawyer\", \"Lawyer\", \"Lawyer\", \"Doctor\", \"Doctor\"],\n                     Category = [1, 2, 3, 4, 5])\n5×3 DataFrames.DataFrame\n│ Row │ City      │ Job    │ Category │\n├─────┼───────────┼────────┼──────────┤\n│ 1   │ Amsterdam │ Lawyer │ 1        │\n│ 2   │ London    │ Lawyer │ 2        │\n│ 3   │ London    │ Lawyer │ 3        │\n│ 4   │ New York  │ Doctor │ 4        │\n│ 5   │ New York  │ Doctor │ 5        │\n\njulia> b = DataFrame(Location = [\"Amsterdam\", \"London\", \"London\", \"New York\", \"New York\"],\n                     Work = [\"Lawyer\", \"Lawyer\", \"Lawyer\", \"Doctor\", \"Doctor\"],\n                     Name = [\"a\", \"b\", \"c\", \"d\", \"e\"])\n5×3 DataFrames.DataFrame\n│ Row │ Location  │ Work   │ Name │\n├─────┼───────────┼────────┼──────┤\n│ 1   │ Amsterdam │ Lawyer │ a    │\n│ 2   │ London    │ Lawyer │ b    │\n│ 3   │ London    │ Lawyer │ c    │\n│ 4   │ New York  │ Doctor │ d    │\n│ 5   │ New York  │ Doctor │ e    │\n\njulia> rename!(b, :Location => :City, :Work => :Job)\n5×3 DataFrames.DataFrame\n│ Row │ City      │ Job    │ Name │\n├─────┼───────────┼────────┼──────┤\n│ 1   │ Amsterdam │ Lawyer │ a    │\n│ 2   │ London    │ Lawyer │ b    │\n│ 3   │ London    │ Lawyer │ c    │\n│ 4   │ New York  │ Doctor │ d    │\n│ 5   │ New York  │ Doctor │ e    │\n\njulia> join(a, b, on = [:City, :Job])\n9×4 DataFrames.DataFrame\n│ Row │ City      │ Job    │ Category │ Name │\n├─────┼───────────┼────────┼──────────┼──────┤\n│ 1   │ Amsterdam │ Lawyer │ 1        │ a    │\n│ 2   │ London    │ Lawyer │ 2        │ b    │\n│ 3   │ London    │ Lawyer │ 2        │ c    │\n│ 4   │ London    │ Lawyer │ 3        │ b    │\n│ 5   │ London    │ Lawyer │ 3        │ c    │\n│ 6   │ New York  │ Doctor │ 4        │ d    │\n│ 7   │ New York  │ Doctor │ 4        │ e    │\n│ 8   │ New York  │ Doctor │ 5        │ d    │\n│ 9   │ New York  │ Doctor │ 5        │ e    │\n"
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
    "text": "Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper \"The Split-Apply-Combine Strategy for Data Analysis\", written by Hadley Wickham.The DataFrames package supports the Split-Apply-Combine strategy through the by function, which takes in three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) a function or expression to apply to each subset of the DataFrame.We show several examples of the by function applied to the iris dataset below:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa  │\n│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa  │\n│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa  │\n│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │\n│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa  │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica │\n│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica │\n│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica │\n│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica │\n│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica │\n│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica │\n\n\njulia> by(iris, :Species, size)\n3×2 DataFrames.DataFrame\n│ Row │ Species    │ x1      │\n├─────┼────────────┼─────────┤\n│ 1   │ setosa     │ (50, 5) │\n│ 2   │ versicolor │ (50, 5) │\n│ 3   │ virginica  │ (50, 5) │\n\njulia> by(iris, :Species, df -> mean(df[:PetalLength]))\n3×2 DataFrames.DataFrame\n│ Row │ Species    │ x1    │\n├─────┼────────────┼───────┤\n│ 1   │ setosa     │ 1.462 │\n│ 2   │ versicolor │ 4.26  │\n│ 3   │ virginica  │ 5.552 │\n\njulia> by(iris, :Species, df -> DataFrame(N = size(df, 1)))\n3×2 DataFrames.DataFrame\n│ Row │ Species    │ N  │\n├─────┼────────────┼────┤\n│ 1   │ setosa     │ 50 │\n│ 2   │ versicolor │ 50 │\n│ 3   │ virginica  │ 50 │\nThe by function also support the do block form:julia> by(iris, :Species) do df\n          DataFrame(m = mean(df[:PetalLength]), s² = var(df[:PetalLength]))\n       end\n3×3 DataFrames.DataFrame\n│ Row │ Species    │ m     │ s²        │\n├─────┼────────────┼───────┼───────────┤\n│ 1   │ setosa     │ 1.462 │ 0.0301592 │\n│ 2   │ versicolor │ 4.26  │ 0.220816  │\n│ 3   │ virginica  │ 5.552 │ 0.304588  │\nA second approach to the Split-Apply-Combine strategy is implemented in the aggregate function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) one or more functions that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column that was not used to split the DataFrame, creating new columns of the form $name_$function. For named functions like mean this will produce columns with names like SepalLength_mean. For anonymous functions like x -> sqrt(x)^e, which Julia tracks and references by a numerical identifier e.g. #12, the produced columns will be SepalLength_#12. We show several examples of the aggregate function applied to the iris dataset below:julia> aggregate(iris, :Species, length)\n3×5 DataFrames.DataFrame\n│ Row │ Species    │ SepalLength_length │ SepalWidth_length │ PetalLength_length │ PetalWidth_length │\n├─────┼────────────┼────────────────────┼───────────────────┼────────────────────┼───────────────────┤\n│ 1   │ setosa     │ 50                 │ 50                │ 50                 │ 50                │\n│ 2   │ versicolor │ 50                 │ 50                │ 50                 │ 50                │\n│ 3   │ virginica  │ 50                 │ 50                │ 50                 │ 50                │\n\njulia> aggregate(iris, :Species, [sum, mean])\n3×9 DataFrames.DataFrame\n│ Row │ Species    │ SepalLength_sum │ SepalWidth_sum │ PetalLength_sum │ PetalWidth_sum │ SepalLength_mean │ SepalWidth_mean │ PetalLength_mean │ PetalWidth_mean │\n├─────┼────────────┼─────────────────┼────────────────┼─────────────────┼────────────────┼──────────────────┼─────────────────┼──────────────────┼─────────────────┤\n│ 1   │ setosa     │ 250.3           │ 171.4          │ 73.1            │ 12.3           │ 5.006            │ 3.428           │ 1.462            │ 0.246           │\n│ 2   │ versicolor │ 296.8           │ 138.5          │ 213.0           │ 66.3           │ 5.936            │ 2.77            │ 4.26             │ 1.326           │\n│ 3   │ virginica  │ 329.4           │ 148.7          │ 277.6           │ 101.3          │ 6.588            │ 2.974           │ 5.552            │ 2.026           │\nIf you only want to split the data set into subsets, use the groupby function:julia> for subdf in groupby(iris, :Species)\n           println(size(subdf, 1))\n       end\n50\n50\n50\n"
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
    "text": "Reshape data from wide to long format using the stack function:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa  │\n│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa  │\n│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa  │\n│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │\n│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa  │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica │\n│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica │\n│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica │\n│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica │\n│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica │\n│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica │\n\n\njulia> d = stack(iris, 1:4);\n\njulia> head(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │\n├─────┼─────────────┼───────┼─────────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │\n\njulia> tail(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │\n├─────┼────────────┼───────┼───────────┤\n│ 1   │ PetalWidth │ 2.5   │ virginica │\n│ 2   │ PetalWidth │ 2.3   │ virginica │\n│ 3   │ PetalWidth │ 1.9   │ virginica │\n│ 4   │ PetalWidth │ 2.0   │ virginica │\n│ 5   │ PetalWidth │ 2.3   │ virginica │\n│ 6   │ PetalWidth │ 1.8   │ virginica │\nThe second optional argument to stack indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:julia> d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth]);\n\njulia> head(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │\n├─────┼─────────────┼───────┼─────────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │\n\njulia> tail(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │\n├─────┼────────────┼───────┼───────────┤\n│ 1   │ PetalWidth │ 2.5   │ virginica │\n│ 2   │ PetalWidth │ 2.3   │ virginica │\n│ 3   │ PetalWidth │ 1.9   │ virginica │\n│ 4   │ PetalWidth │ 2.0   │ virginica │\n│ 5   │ PetalWidth │ 2.3   │ virginica │\n│ 6   │ PetalWidth │ 1.8   │ virginica │\nNote that all columns can be of different types. Type promotion follows the rules of vcat.The stacked DataFrame that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled :variable and :values contain the column identifier and the stacked columns.A third optional argument to stack represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:julia> d = stack(iris, [:SepalLength, :SepalWidth], :Species);\n\njulia> head(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │\n├─────┼─────────────┼───────┼─────────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │\n\njulia> tail(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │\n├─────┼────────────┼───────┼───────────┤\n│ 1   │ SepalWidth │ 3.3   │ virginica │\n│ 2   │ SepalWidth │ 3.0   │ virginica │\n│ 3   │ SepalWidth │ 2.5   │ virginica │\n│ 4   │ SepalWidth │ 3.0   │ virginica │\n│ 5   │ SepalWidth │ 3.4   │ virginica │\n│ 6   │ SepalWidth │ 3.0   │ virginica │\nmelt is an alternative function to reshape from wide to long format. It is based on stack, but it prefers specification of the id columns as:julia> d = melt(iris, :Species);\n\njulia> head(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │\n├─────┼─────────────┼───────┼─────────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │\n\njulia> tail(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │\n├─────┼────────────┼───────┼───────────┤\n│ 1   │ PetalWidth │ 2.5   │ virginica │\n│ 2   │ PetalWidth │ 2.3   │ virginica │\n│ 3   │ PetalWidth │ 1.9   │ virginica │\n│ 4   │ PetalWidth │ 2.0   │ virginica │\n│ 5   │ PetalWidth │ 2.3   │ virginica │\n│ 6   │ PetalWidth │ 1.8   │ virginica │\nunstack converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:julia> iris[:id] = 1:size(iris, 1)\n1:150\n\njulia> longdf = melt(iris, [:Species, :id]);\n\njulia> head(longdf)\n6×4 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │ id │\n├─────┼─────────────┼───────┼─────────┼────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │ 1  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │ 2  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │ 3  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │ 4  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │ 5  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │ 6  │\n\njulia> tail(longdf)\n6×4 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │ id  │\n├─────┼────────────┼───────┼───────────┼─────┤\n│ 1   │ PetalWidth │ 2.5   │ virginica │ 145 │\n│ 2   │ PetalWidth │ 2.3   │ virginica │ 146 │\n│ 3   │ PetalWidth │ 1.9   │ virginica │ 147 │\n│ 4   │ PetalWidth │ 2.0   │ virginica │ 148 │\n│ 5   │ PetalWidth │ 2.3   │ virginica │ 149 │\n│ 6   │ PetalWidth │ 1.8   │ virginica │ 150 │\n\njulia> widedf = unstack(longdf, :id, :variable, :value);\n\njulia> head(widedf)\n6×5 DataFrames.DataFrame\n│ Row │ id │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n├─────┼────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ 1  │ 1.4         │ 0.2        │ 5.1         │ 3.5        │\n│ 2   │ 2  │ 1.4         │ 0.2        │ 4.9         │ 3.0        │\n│ 3   │ 3  │ 1.3         │ 0.2        │ 4.7         │ 3.2        │\n│ 4   │ 4  │ 1.5         │ 0.2        │ 4.6         │ 3.1        │\n│ 5   │ 5  │ 1.4         │ 0.2        │ 5.0         │ 3.6        │\n│ 6   │ 6  │ 1.7         │ 0.4        │ 5.4         │ 3.9        │\n\njulia> tail(widedf)\n6×5 DataFrames.DataFrame\n│ Row │ id  │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n├─────┼─────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ 145 │ 5.7         │ 2.5        │ 6.7         │ 3.3        │\n│ 2   │ 146 │ 5.2         │ 2.3        │ 6.7         │ 3.0        │\n│ 3   │ 147 │ 5.0         │ 1.9        │ 6.3         │ 2.5        │\n│ 4   │ 148 │ 5.2         │ 2.0        │ 6.5         │ 3.0        │\n│ 5   │ 149 │ 5.4         │ 2.3        │ 6.2         │ 3.4        │\n│ 6   │ 150 │ 5.1         │ 1.8        │ 5.9         │ 3.0        │\nIf the remaining columns are unique, you can skip the id variable and use:julia> longdf = melt(iris, [:Species, :id]);\n\njulia> head(longdf)\n6×4 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │ id │\n├─────┼─────────────┼───────┼─────────┼────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │ 1  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │ 2  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │ 3  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │ 4  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │ 5  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │ 6  │\n\njulia> widedf = unstack(longdf, :variable, :value);\n\njulia> head(widedf)\n6×6 DataFrames.DataFrame\n│ Row │ Species │ id │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n├─────┼─────────┼────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ setosa  │ 1  │ 1.4         │ 0.2        │ 5.1         │ 3.5        │\n│ 2   │ setosa  │ 2  │ 1.4         │ 0.2        │ 4.9         │ 3.0        │\n│ 3   │ setosa  │ 3  │ 1.3         │ 0.2        │ 4.7         │ 3.2        │\n│ 4   │ setosa  │ 4  │ 1.5         │ 0.2        │ 4.6         │ 3.1        │\n│ 5   │ setosa  │ 5  │ 1.4         │ 0.2        │ 5.0         │ 3.6        │\n│ 6   │ setosa  │ 6  │ 1.7         │ 0.4        │ 5.4         │ 3.9        │\nstackdf and meltdf are two additional functions that work like stack and melt, but they provide a view into the original wide DataFrame. Here is an example:julia> d = stackdf(iris);\n\njulia> head(d)\n6×4 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │ id │\n├─────┼─────────────┼───────┼─────────┼────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │ 1  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │ 2  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │ 3  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │ 4  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │ 5  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │ 6  │\n\njulia> tail(d)\n6×4 DataFrames.DataFrame\n│ Row │ variable   │ value │ Species   │ id  │\n├─────┼────────────┼───────┼───────────┼─────┤\n│ 1   │ PetalWidth │ 2.5   │ virginica │ 145 │\n│ 2   │ PetalWidth │ 2.3   │ virginica │ 146 │\n│ 3   │ PetalWidth │ 1.9   │ virginica │ 147 │\n│ 4   │ PetalWidth │ 2.0   │ virginica │ 148 │\n│ 5   │ PetalWidth │ 2.3   │ virginica │ 149 │\n│ 6   │ PetalWidth │ 1.8   │ virginica │ 150 │\nThis saves memory. To create the view, several AbstractVectors are defined::variable column – EachRepeatedVector   This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.:value column – StackedVector   This is provides a view of the original columns stacked together.Id columns – RepeatedVector   This repeats the original columns N times where N is the number of columns stacked.None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:julia> d = melt(iris, :Species);\n\njulia> head(d)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ value │ Species │\n├─────┼─────────────┼───────┼─────────┤\n│ 1   │ SepalLength │ 5.1   │ setosa  │\n│ 2   │ SepalLength │ 4.9   │ setosa  │\n│ 3   │ SepalLength │ 4.7   │ setosa  │\n│ 4   │ SepalLength │ 4.6   │ setosa  │\n│ 5   │ SepalLength │ 5.0   │ setosa  │\n│ 6   │ SepalLength │ 5.4   │ setosa  │\n\njulia> x = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(df[:value])));\n\njulia> head(x)\n6×3 DataFrames.DataFrame\n│ Row │ variable    │ Species    │ vsum  │\n├─────┼─────────────┼────────────┼───────┤\n│ 1   │ SepalLength │ setosa     │ 5.006 │\n│ 2   │ SepalLength │ versicolor │ 5.936 │\n│ 3   │ SepalLength │ virginica  │ 6.588 │\n│ 4   │ SepalWidth  │ setosa     │ 3.428 │\n│ 5   │ SepalWidth  │ versicolor │ 2.77  │\n│ 6   │ SepalWidth  │ virginica  │ 2.974 │\n\njulia> head(unstack(x, :Species, :vsum))\n5×4 DataFrames.DataFrame\n│ Row │ variable    │ setosa │ versicolor │ virginica │\n├─────┼─────────────┼────────┼────────────┼───────────┤\n│ 1   │ PetalLength │ 1.462  │ 4.26       │ 5.552     │\n│ 2   │ PetalWidth  │ 0.246  │ 1.326      │ 2.026     │\n│ 3   │ SepalLength │ 5.006  │ 5.936      │ 6.588     │\n│ 4   │ SepalWidth  │ 3.428  │ 2.77       │ 2.974     │\n│ 5   │ id          │ 25.5   │ 75.5       │ 125.5     │\n"
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
    "text": "Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling sort! will sort all columns, in place:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> sort!(iris);\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa  │\n│ 2   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa  │\n│ 3   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa  │\n│ 4   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa  │\n│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa  │\n│ 6   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica │\n│ 2   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica │\n│ 3   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica │\n│ 4   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica │\n│ 5   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica │\n│ 6   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica │\nIn Sorting DataFrames, you may want to sort different columns with different options. Here are some examples showing most of the possible options:julia> sort!(iris, rev = true);\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica │\n│ 2   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica │\n│ 3   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica │\n│ 4   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica │\n│ 5   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica │\n│ 6   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa  │\n│ 2   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa  │\n│ 3   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa  │\n│ 4   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa  │\n│ 5   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa  │\n\njulia> sort!(iris, (:SepalWidth, :SepalLength));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species    │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────┤\n│ 1   │ 5.0         │ 2.0        │ 3.5         │ 1.0        │ versicolor │\n│ 2   │ 6.0         │ 2.2        │ 5.0         │ 1.5        │ virginica  │\n│ 3   │ 6.0         │ 2.2        │ 4.0         │ 1.0        │ versicolor │\n│ 4   │ 6.2         │ 2.2        │ 4.5         │ 1.5        │ versicolor │\n│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa     │\n│ 6   │ 5.0         │ 2.3        │ 3.3         │ 1.0        │ versicolor │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa  │\n│ 2   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa  │\n│ 3   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa  │\n│ 4   │ 5.2         │ 4.1        │ 1.5         │ 0.1        │ setosa  │\n│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa  │\n\njulia> sort!(iris, (order(:Species, by = uppercase),\n                    order(:SepalLength, rev = true)));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa  │\n│ 2   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa  │\n│ 3   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa  │\n│ 4   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa  │\n│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa  │\n│ 6   │ 5.4         │ 3.4        │ 1.7         │ 0.2        │ setosa  │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 2   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 3   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica │\n│ 4   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica │\n│ 5   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica │\n│ 6   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica │\nKeywords used above include rev (to sort a column or the whole DataFrame in reverse), and by (to apply a function to a column/DataFrame). Each keyword can either be a single value, or can be a tuple or array, with values corresponding to individual columns.As an alternative to using array or tuple values, order to specify an ordering for a particular column within a set of columnsThe following two examples show two ways to sort the iris dataset with the same result: Species will be ordered in reverse lexicographic order, and within species, rows will be sorted by increasing sepal length and width:julia> sort!(iris, (:Species, :SepalLength, :SepalWidth),\n                    rev = (true, false, false));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica │\n│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica │\n│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica │\n│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa  │\n│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa  │\n│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa  │\n│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa  │\n│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa  │\n│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa  │\n\njulia> sort!(iris, (order(:Species, rev = true), :SepalLength, :SepalWidth));\n\njulia> head(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species   │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────┤\n│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica │\n│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica │\n│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica │\n│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica │\n│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica │\n\njulia> tail(iris)\n6×5 DataFrames.DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa  │\n│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa  │\n│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa  │\n│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa  │\n│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa  │\n│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa  │\n"
},

{
    "location": "man/categorical.html#",
    "page": "Categorical Data",
    "title": "Categorical Data",
    "category": "page",
    "text": ""
},

{
    "location": "man/categorical.html#Categorical-Data-1",
    "page": "Categorical Data",
    "title": "Categorical Data",
    "category": "section",
    "text": "Often, we have to deal with factors that take on a small number of levels:julia> v = [\"Group A\", \"Group A\", \"Group A\", \"Group B\", \"Group B\", \"Group B\"]\n6-element Array{String,1}:\n \"Group A\"\n \"Group A\"\n \"Group A\"\n \"Group B\"\n \"Group B\"\n \"Group B\"\nThe naive encoding used in an Array represents every entry of this vector as a full string. In contrast, we can represent the data more efficiently by replacing the strings with indices into a small pool of levels. This is what the CategoricalArray type does:julia> using CategoricalArrays\n\njulia> cv = CategoricalArray(v)\n6-element CategoricalArrays.CategoricalArray{String,1,UInt32}:\n \"Group A\"\n \"Group A\"\n \"Group A\"\n \"Group B\"\n \"Group B\"\n \"Group B\"\nCategoricalArrays support missing values via the Missings package.julia> using Missings\n\njulia> cv = CategoricalArray([\"Group A\", missing, \"Group A\",\n                              \"Group B\", \"Group B\", missing])\n6-element CategoricalArrays.CategoricalArray{Union{Missings.Missing, String},1,UInt32}:\n \"Group A\"\n missing\n \"Group A\"\n \"Group B\"\n \"Group B\"\n missingIn addition to representing repeated data efficiently, the CategoricalArray type allows us to determine efficiently the allowed levels of the variable at any time using the levels function (note that levels may or may not be actually used in the data):julia> levels(cv)\n2-element Array{String,1}:\n \"Group A\"\n \"Group B\"\nThe levels! function also allows changing the order of appearance of the levels, which can be useful for display purposes or when working with ordered variables.julia> levels!(cv, [\"Group B\", \"Group A\"]);\n\njulia> levels(cv)\n2-element Array{String,1}:\n \"Group B\"\n \"Group A\"\n\njulia> sort(cv)\n6-element CategoricalArrays.CategoricalArray{Union{Missings.Missing, String},1,UInt32}:\n \"Group B\"\n \"Group B\"\n \"Group A\"\n \"Group A\"\n missing\n missing\nBy default, a CategoricalArray is able to represent 2<sup>32</sup>differents levels. You can use less memory by calling the compress function:julia> cv = compress(cv)\n6-element CategoricalArrays.CategoricalArray{Union{Missings.Missing, String},1,UInt8}:\n \"Group A\"\n missing\n \"Group A\"\n \"Group B\"\n \"Group B\"\n missing\nOften, you will have factors encoded inside a DataFrame with Array columns instead of CategoricalArray columns. You can convert one or more columns of the DataFrame using the categorical! function, which modifies the input DataFrame in-place.julia> using DataFrames\n\njulia> df = DataFrame(A = [\"A\", \"B\", \"C\", \"D\", \"D\", \"A\"],\n                      B = [\"X\", \"X\", \"X\", \"Y\", \"Y\", \"Y\"])\n6×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ A │ X │\n│ 2   │ B │ X │\n│ 3   │ C │ X │\n│ 4   │ D │ Y │\n│ 5   │ D │ Y │\n│ 6   │ A │ Y │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n String\n String\n\njulia> categorical!(df, :A) # change the column `:A` to be categorical\n6×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ A │ X │\n│ 2   │ B │ X │\n│ 3   │ C │ X │\n│ 4   │ D │ Y │\n│ 5   │ D │ Y │\n│ 6   │ A │ Y │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n CategoricalArrays.CategoricalString{UInt32}\n String\n\njulia> categorical!(df) # change all columns to be categorical\n6×2 DataFrames.DataFrame\n│ Row │ A │ B │\n├─────┼───┼───┤\n│ 1   │ A │ X │\n│ 2   │ B │ X │\n│ 3   │ C │ X │\n│ 4   │ D │ Y │\n│ 5   │ D │ Y │\n│ 6   │ A │ Y │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n CategoricalArrays.CategoricalString{UInt32}\n CategoricalArrays.CategoricalString{UInt32}\nUsing categorical arrays is important for working with the GLM package. When fitting regression models, CategoricalArray columns in the input are translated into 0/1 indicator columns in the ModelMatrix with one column for each of the levels of the CategoricalArray. This allows one to analyze categorical data efficiently.See the CategoricalArrays package for more information regarding categorical arrays."
},

{
    "location": "man/missing.html#",
    "page": "Missing Data",
    "title": "Missing Data",
    "category": "page",
    "text": ""
},

{
    "location": "man/missing.html#The-Missing-Type-1",
    "page": "Missing Data",
    "title": "The Missing Type",
    "category": "section",
    "text": "Missing is a type implemented by the Missings.jl package to represent missing data. missing is an instance of the type Missing used to represent a missing value.julia> using DataFrames\r\n\r\njulia> missing\r\nmissing\r\n\r\njulia> typeof(missing)\r\nMissings.Missing\r\nThe Missing type lets users create Vectors and DataFrame columns with missing values. Here we create a vector with a missing value and the element-type of the returned vector is Union{Missings.Missing, Int64}.julia> x = [1, 2, missing]\r\n3-element Array{Union{Missings.Missing, Int64},1}:\r\n 1\r\n 2\r\n  missing\r\n\r\njulia> eltype(x)\r\nUnion{Missings.Missing, Int64}\r\n\r\njulia> Union{Missing, Int}\r\nUnion{Missings.Missing, Int64}\r\n\r\njulia> eltype(x) == Union{Missing, Int}\r\ntrue\r\nmissing values can be excluded when performing operations by using skipmissing, which returns a memory-efficient iterator.julia> skipmissing(x)\r\nMissings.EachSkipMissing{Array{Union{$Int, Missings.Missing},1}}(Union{$Int, Missings.Missing}[1, 2, missing])\r\nThe output of skipmissing can be passed directly into functions as an argument. For example, we can find the sum of all non-missing values or collect the non-missing values into a new missing-free vector.julia> sum(skipmissing(x))\r\n3\r\n\r\njulia> collect(skipmissing(x))\r\n2-element Array{Int64,1}:\r\n 1\r\n 2\r\nmissing elements can be replaced with other values via Missings.replace.julia> collect(Missings.replace(x, 1))\r\n3-element Array{Int64,1}:\r\n 1\r\n 2\r\n 1\r\nThe function Missings.T returns the element-type T in Union{T, Missing}.julia> eltype(x)\r\nUnion{Int64, Missings.Missing}\r\n\r\njulia> Missings.T(eltype(x))\r\nInt64\r\nUse missings to generate Vectors and Arrays supporting missing values, using the optional first argument to specify the element-type.julia> missings(1)\r\n1-element Array{Missings.Missing,1}:\r\n missing\r\n\r\njulia> missings(3)\r\n3-element Array{Missings.Missing,1}:\r\n missing\r\n missing\r\n missing\r\n\r\njulia> missings(1, 3)\r\n1×3 Array{Missings.Missing,2}:\r\n missing  missing  missing\r\n\r\njulia> missings(Int, 1, 3)\r\n1×3 Array{Union{Missings.Missing, Int64},2}:\r\n missing  missing  missing\r\n"
},

{
    "location": "man/querying_frameworks.html#",
    "page": "Querying frameworks",
    "title": "Querying frameworks",
    "category": "page",
    "text": ""
},

{
    "location": "man/querying_frameworks.html#Querying-frameworks-1",
    "page": "Querying frameworks",
    "title": "Querying frameworks",
    "category": "section",
    "text": "The Query.jl package provides advanced data manipulation capabilities for DataFrames (and many other data structures). This section provides a short introduction to the package, the Query.jl documentation has a more comprehensive documentation of the package.To get started, install the Query.jl package:Pkg.add(\"Query\")A query is started with the @from macro and consists of a series of query commands. Query.jl provides commands that can filter, project, join, flatten and group data from a DataFrame. A query can return an iterator, or one can materialize the results of a query into a variety of data structures, including a new DataFrame.A simple example of a query looks like this:julia> using DataFrames, Query\n\njulia> df = DataFrame(name=[\"John\", \"Sally\", \"Roger\"], age=[54., 34., 79.], children=[0, 2, 4])\n3×3 DataFrames.DataFrame\n│ Row │ name  │ age  │ children │\n├─────┼───────┼──────┼──────────┤\n│ 1   │ John  │ 54.0 │ 0        │\n│ 2   │ Sally │ 34.0 │ 2        │\n│ 3   │ Roger │ 79.0 │ 4        │\n\njulia> q1 = @from i in df begin\n            @where i.age > 40\n            @select {number_of_children=i.children, i.name}\n            @collect DataFrame\n       end\n2×2 DataFrames.DataFrame\n│ Row │ number_of_children │ name  │\n├─────┼────────────────────┼───────┤\n│ 1   │ 0                  │ John  │\n│ 2   │ 4                  │ Roger │\nThe query starts with the @from macro. The first argument i is the name of the range variable that will be used to refer to an individual row in later query commands. The next argument df is the data source that one wants to query. The @where command in this query will filter the source data by applying the filter condition i.age > 40. This filters out any rows in which the age column is not larger than 40. The @select command then projects the columns of the source data onto a new column structure. The example here applies three specific modifications: 1) it only keeps a subset of the columns in the source DataFrame, i.e. the age column will not be part of the transformed data; 2) it changes the order of the two columns that are selected; and 3) it renames one of the columns that is selected from children to number_of_children. The example query uses the {} syntax to achieve this. A {} in a Query.jl expression instantiates a new NamedTuple, i.e. it is a shortcut for writing @NT(number_of_children=>i.children, name=>i.name). The @collect statement determines the data structure that the query returns. In this example the results are returned as a DataFrame.A query without a @collect statement returns a standard julia iterator that can be used with any normal julia language construct that can deal with iterators. The following code returns a julia iterator for the query results:julia> q2 = @from i in df begin\n                   @where i.age > 40\n                   @select {number_of_children=i.children, i.name}\n              end; # suppress printing the iterator type\nOne can loop over the results using a standard julia for statement:julia> total_children = 0\n0\n\njulia> for i in q2\n           total_children += i.number_of_children\n       end\n\njulia> total_children\n4\nOr one can use a comprehension to extract the name of a subset of rows:julia> y = [i.name for i in q2 if i.number_of_children > 0]\n1-element Array{String,1}:\n \"Roger\"\nThe last example (extracting only the name and applying a second filter) could of course be completely expressed as a query expression:julia> q3 = @from i in df begin\n            @where i.age > 40 && i.children > 0\n            @select i.name\n            @collect\n       end\n1-element Array{String,1}:\n \"Roger\"\nA query that ends with a @collect statement without a specific type will materialize the query results into an array. Note also the difference in the @select statement: The previous queries all used the {} syntax in the @select statement to project results into a tabular format. The last query instead just selects a single value from each row in the @select statement.These examples only scratch the surface of what one can do with Query.jl, and the interested reader is referred to the Query.jl documentation for more information."
},

{
    "location": "lib/types.html#",
    "page": "Types",
    "title": "Types",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/types.html#Types-1",
    "page": "Types",
    "title": "Types",
    "category": "section",
    "text": "Pages = [\"types.md\"]AbstractDataFrame\nDataFrame\nDataFrameRow\nGroupApplied\nGroupedDataFrame\nSubDataFrame"
},

{
    "location": "lib/functions.html#",
    "page": "Functions",
    "title": "Functions",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/functions.html#Functions-1",
    "page": "Functions",
    "title": "Functions",
    "category": "section",
    "text": "Pages = [\"functions.md\"]"
},

{
    "location": "lib/functions.html#DataFrames.aggregate",
    "page": "Functions",
    "title": "DataFrames.aggregate",
    "category": "function",
    "text": "Split-apply-combine that applies a set of functions over columns of an AbstractDataFrame or GroupedDataFrame\n\naggregate(d::AbstractDataFrame, cols, fs)\naggregate(gd::GroupedDataFrame, fs)\n\nArguments\n\nd : an AbstractDataFrame\ngd : a GroupedDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.)\nfs : a function or vector of functions to be applied to vectors within groups; expects each argument to be a column vector\n\nEach fs should return a value or vector. All returns must be the same length.\n\nReturns\n\n::DataFrame\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\naggregate(df, :a, sum)\naggregate(df, :a, [sum, x->mean(skipmissing(x))])\naggregate(groupby(df, :a), [sum, x->mean(skipmissing(x))])\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.by",
    "page": "Functions",
    "title": "DataFrames.by",
    "category": "function",
    "text": "Split-apply-combine in one step; apply f to each grouping in d based on columns col\n\nby(d::AbstractDataFrame, cols, f::Function; sort::Bool = false)\nby(f::Function, d::AbstractDataFrame, cols; sort::Bool = false)\n\nArguments\n\nd : an AbstractDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.)\nf : a function to be applied to groups; expects each argument to be an AbstractDataFrame\nsort: sort row groups (no sorting by default)\n\nf can return a value, a vector, or a DataFrame. For a value or vector, these are merged into a column along with the cols keys. For a DataFrame, cols are combined along columns with the resulting DataFrame. Returning a DataFrame is the clearest because it allows column labeling.\n\nA method is defined with f as the first argument, so do-block notation can be used.\n\nby(d, cols, f) is equivalent to combine(map(f, groupby(d, cols))).\n\nReturns\n\n::DataFrame\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\nby(df, :a, d -> sum(d[:c]))\nby(df, :a, d -> 2 * skipmissing(d[:c]))\nby(df, :a, d -> DataFrame(c_sum = sum(d[:c]), c_mean = mean(skipmissing(d[:c]))))\nby(df, :a, d -> DataFrame(c = d[:c], c_mean = mean(skipmissing(d[:c]))))\nby(df, [:a, :b]) do d\n    DataFrame(m = mean(skipmissing(d[:c])), v = var(skipmissing(d[:c])))\nend\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.colwise",
    "page": "Functions",
    "title": "DataFrames.colwise",
    "category": "function",
    "text": "Apply a function to each column in an AbstractDataFrame or GroupedDataFrame\n\ncolwise(f::Function, d)\ncolwise(d)\n\nArguments\n\nf : a function or vector of functions\nd : an AbstractDataFrame of GroupedDataFrame\n\nIf d is not provided, a curried version of groupby is given.\n\nReturns\n\nvarious, depending on the call\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ncolwise(sum, df)\ncolwise([sum, length], df)\ncolwise((minimum, maximum), df)\ncolwise(sum, groupby(df, :a))\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.groupby",
    "page": "Functions",
    "title": "DataFrames.groupby",
    "category": "function",
    "text": "A view of an AbstractDataFrame split into row groups\n\ngroupby(d::AbstractDataFrame, cols; sort = false, skipmissing = false)\ngroupby(cols; sort = false, skipmissing = false)\n\nArguments\n\nd : an AbstractDataFrame to split (optional, see Returns)\ncols : data table columns to group by\nsort: whether to sort rows according to the values of the grouping columns cols\nskipmissing: whether to skip rows with missing values in one of the grouping columns cols\n\nReturns\n\n::GroupedDataFrame : a grouped view into d\n::Function: a function x -> groupby(x, cols) (if d is not specified)\n\nDetails\n\nAn iterator over a GroupedDataFrame returns a SubDataFrame view for each grouping into d. A GroupedDataFrame also supports indexing by groups and map.\n\nSee the following for additional split-apply-combine operations:\n\nby : split-apply-combine using functions\naggregate : split-apply-combine; applies functions in the form of a cross product\ncombine : combine (obviously)\ncolwise : apply a function to each column in an AbstractDataFrame or GroupedDataFrame\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ngd = groupby(df, :a)\ngd[1]\nlast(gd)\nvcat([g[:b] for g in gd]...)\nfor g in gd\n    println(g)\nend\nmap(d -> mean(skipmissing(d[:c])), gd)   # returns a GroupApplied object\ncombine(map(d -> mean(skipmissing(d[:c])), gd))\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.join",
    "page": "Functions",
    "title": "Base.join",
    "category": "function",
    "text": "join(df1, df2; on = Symbol[], kind = :inner, makeunique = false,\n     indicator = nothing, validate = (false, false))\n\nJoin two DataFrame objects\n\nArguments\n\ndf1, df2 : the two AbstractDataFrames to be joined\n\nKeyword Arguments\n\non : A column, or vector of columns to join df1 and df2 on. If the column(s)   that df1 and df2 will be joined on have different names, then the columns   should be (left, right) tuples or left => right pairs, or a vector of   such tuples or pairs. on is a required argument for all joins except for   kind = :cross\nkind : the type of join, options include:\n:inner : only include rows with keys that match in both df1 and df2, the default\n:outer : include all rows from df1 and df2\n:left : include all rows from df1\n:right : include all rows from df2\n:semi : return rows of df1 that match with the keys in df2\n:anti : return rows of df1 that do not match with the keys in df2\n:cross : a full Cartesian product of the key combinations; every row of df1 is matched with every row of df2\n\nmakeunique : if false (the default), an error will be raised if duplicate names are found in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\nindicator : Default: nothing. If a Symbol, adds categorical indicator  column named Symbol for whether a row appeared in only df1 (\"left_only\"),  only df2 (\"right_only\") or in both (\"both\"). If Symbol is already in use,  the column name will be modified if makeunique=true.\nvalidate : whether to check that columns passed as the on argument  define unique keys in each input data frame (according to isequal).  Can be a tuple or a pair, with the first element indicating whether to  run check for df1 and the second element for df2.  By default no check is performed.\n\nFor the three join operations that may introduce missing values (:outer, :left, and :right), all columns of the returned data table will support missing values.\n\nWhen merging on categorical columns that differ in the ordering of their levels, the ordering of the left DataFrame takes precedence over the ordering of the right DataFrame\n\nResult\n\n::DataFrame : the joined DataFrame\n\nExamples\n\nname = DataFrame(ID = [1, 2, 3], Name = [\"John Doe\", \"Jane Doe\", \"Joe Blogs\"])\njob = DataFrame(ID = [1, 2, 4], Job = [\"Lawyer\", \"Doctor\", \"Farmer\"])\n\njoin(name, job, on = :ID)\njoin(name, job, on = :ID, kind = :outer)\njoin(name, job, on = :ID, kind = :left)\njoin(name, job, on = :ID, kind = :right)\njoin(name, job, on = :ID, kind = :semi)\njoin(name, job, on = :ID, kind = :anti)\njoin(name, job, kind = :cross)\n\njob2 = DataFrame(identifier = [1, 2, 4], Job = [\"Lawyer\", \"Doctor\", \"Farmer\"])\njoin(name, job2, on = (:ID, :identifier))\njoin(name, job2, on = :ID => :identifier)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.melt",
    "page": "Functions",
    "title": "DataFrames.melt",
    "category": "function",
    "text": "Stacks a DataFrame; convert from a wide to long format; see stack.\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.stack",
    "page": "Functions",
    "title": "DataFrames.stack",
    "category": "function",
    "text": "Stacks a DataFrame; convert from a wide to long format\n\nstack(df::AbstractDataFrame, [measure_vars], [id_vars];\n      variable_name::Symbol=:variable, value_name::Symbol=:value)\nmelt(df::AbstractDataFrame, [id_vars], [measure_vars];\n     variable_name::Symbol=:variable, value_name::Symbol=:value)\n\nArguments\n\ndf : the AbstractDataFrame to be stacked\nmeasure_vars : the columns to be stacked (the measurement variables), a normal column indexing type, like a Symbol, Vector{Symbol}, Int, etc.; for melt, defaults to all variables that are not id_vars. If neither measure_vars or id_vars are given, measure_vars defaults to all floating point columns.\nid_vars : the identifier columns that are repeated during stacking, a normal column indexing type; for stack defaults to all variables that are not measure_vars\nvariable_name : the name of the new stacked column that shall hold the names of each of measure_vars\nvalue_name : the name of the new stacked column containing the values from each of measure_vars\n\nResult\n\n::DataFrame : the long-format DataFrame with column :value holding the values of the stacked columns (measure_vars), with column :variable a Vector of Symbols with the measure_vars name, and with columns for each of the id_vars.\n\nSee also stackdf and meltdf for stacking methods that return a view into the original DataFrame. See unstack for converting from long to wide format.\n\nExamples\n\nd1 = DataFrame(a = repeat([1:3;], inner = [4]),\n               b = repeat([1:4;], inner = [3]),\n               c = randn(12),\n               d = randn(12),\n               e = map(string, \'a\':\'l\'))\n\nd1s = stack(d1, [:c, :d])\nd1s2 = stack(d1, [:c, :d], [:a])\nd1m = melt(d1, [:a, :b, :e])\nd1s_name = melt(d1, [:a, :b, :e], variable_name=:somemeasure)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.unstack",
    "page": "Functions",
    "title": "DataFrames.unstack",
    "category": "function",
    "text": "Unstacks a DataFrame; convert from a long to wide format\n\nunstack(df::AbstractDataFrame, rowkeys::Union{Symbol, Integer},\n        colkey::Union{Symbol, Integer}, value::Union{Symbol, Integer})\nunstack(df::AbstractDataFrame, rowkeys::AbstractVector{<:Union{Symbol, Integer}},\n        colkey::Union{Symbol, Integer}, value::Union{Symbol, Integer})\nunstack(df::AbstractDataFrame, colkey::Union{Symbol, Integer},\n        value::Union{Symbol, Integer})\nunstack(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame to be unstacked\nrowkeys : the column(s) with a unique key for each row, if not given, find a key by grouping on anything not a colkey or value\ncolkey : the column holding the column names in wide format, defaults to :variable\nvalue : the value column, defaults to :value\n\nResult\n\n::DataFrame : the wide-format DataFrame\n\nIf colkey contains missing values then they will be skipped and a warning will be printed.\n\nIf combination of rowkeys and colkey contains duplicate entries then last value will be retained and a warning will be printed.\n\nExamples\n\nwide = DataFrame(id = 1:12,\n                 a  = repeat([1:3;], inner = [4]),\n                 b  = repeat([1:4;], inner = [3]),\n                 c  = randn(12),\n                 d  = randn(12))\n\nlong = stack(wide)\nwide0 = unstack(long)\nwide1 = unstack(long, :variable, :value)\nwide2 = unstack(long, :id, :variable, :value)\nwide3 = unstack(long, [:id, :a], :variable, :value)\n\nNote that there are some differences between the widened results above.\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.stackdf",
    "page": "Functions",
    "title": "DataFrames.stackdf",
    "category": "function",
    "text": "A stacked view of a DataFrame (long format)\n\nLike stack and melt, but a view is returned rather than data copies.\n\nstackdf(df::AbstractDataFrame, [measure_vars], [id_vars];\n        variable_name::Symbol=:variable, value_name::Symbol=:value)\nmeltdf(df::AbstractDataFrame, [id_vars], [measure_vars];\n       variable_name::Symbol=:variable, value_name::Symbol=:value)\n\nArguments\n\ndf : the wide AbstractDataFrame\nmeasure_vars : the columns to be stacked (the measurement variables), a normal column indexing type, like a Symbol, Vector{Symbol}, Int, etc.; for melt, defaults to all variables that are not id_vars\nid_vars : the identifier columns that are repeated during stacking, a normal column indexing type; for stack defaults to all variables that are not measure_vars\n\nResult\n\n::DataFrame : the long-format DataFrame with column :value holding the values of the stacked columns (measure_vars), with column :variable a Vector of Symbols with the measure_vars name, and with columns for each of the id_vars.\n\nThe result is a view because the columns are special AbstractVectors that return indexed views into the original DataFrame.\n\nExamples\n\nd1 = DataFrame(a = repeat([1:3;], inner = [4]),\n               b = repeat([1:4;], inner = [3]),\n               c = randn(12),\n               d = randn(12),\n               e = map(string, \'a\':\'l\'))\n\nd1s = stackdf(d1, [:c, :d])\nd1s2 = stackdf(d1, [:c, :d], [:a])\nd1m = meltdf(d1, [:a, :b, :e])\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.meltdf",
    "page": "Functions",
    "title": "DataFrames.meltdf",
    "category": "function",
    "text": "A stacked view of a DataFrame (long format); see stackdf\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Grouping,-Joining,-and-Split-Apply-Combine-1",
    "page": "Functions",
    "title": "Grouping, Joining, and Split-Apply-Combine",
    "category": "section",
    "text": "aggregate\nby\ncolwise\ngroupby\njoin\nmelt\nstack\nunstack\nstackdf\nmeltdf"
},

{
    "location": "lib/functions.html#Basics-1",
    "page": "Functions",
    "title": "Basics",
    "category": "section",
    "text": "allowmissing!\ncategorical!\ncombine\ncompletecases\ndeleterows!\ndescribe\ndisallowmissing!\ndropmissing\ndropmissing!\neachcol\neachrow\neltypes\nfilter\nfilter!\nhead\nnames\nnames!\nnonunique\norder\nrename!\nrename\nshow\nshowcols\nsimilar\nsize\nsort\nsort!\ntail\nunique!\npermutecols!"
},

]}
