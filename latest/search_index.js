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
    "text": "Pages = [\"man/getting_started.md\",\n         \"man/joins.md\",\n         \"man/split_apply_combine.md\",\n         \"man/reshaping_and_pivoting.md\",\n         \"man/sorting.md\",\n         \"man/categorical.md\",\n         \"man/missing.md\",\n         \"man/querying_frameworks.md\"]\nDepth = 2"
},

{
    "location": "index.html#API-1",
    "page": "Introduction",
    "title": "API",
    "category": "section",
    "text": "Pages = [\"lib/types.md\", \"lib/functions.md\", \"lib/indexing.md\"]\nDepth = 2"
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
    "text": "The DataFrames package is available through the Julia package system and can be installed using the following commands:using Pkg\nPkg.add(\"DataFrames\")Throughout the rest of this tutorial, we will assume that you have installed the DataFrames package and have already typed using DataFrames to bring all of the relevant variables into your current namespace."
},

{
    "location": "man/getting_started.html#The-DataFrame-Type-1",
    "page": "Getting Started",
    "title": "The DataFrame Type",
    "category": "section",
    "text": "Objects of the DataFrame type represent a data table as a series of vectors, each corresponding to a column or variable. The simplest way of constructing a DataFrame is to pass column vectors using keyword arguments or pairs:julia> using DataFrames\n\njulia> df = DataFrame(A = 1:4, B = [\"M\", \"F\", \"F\", \"M\"])\n4×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ M      │\n│ 2   │ 2     │ F      │\n│ 3   │ 3     │ F      │\n│ 4   │ 4     │ M      │\nColumns can be accessed via df.col or df[:col]. The latter syntax is more flexible as it allows passing a variable holding the name of the column, and not only a literal name. Note that column names are symbols (:col or Symbol(\"col\")) rather than strings (\"col\"). Columns can also be accessed using an integer index specifying their position.julia> df.A\n4-element Array{Int64,1}:\n 1\n 2\n 3\n 4\n\njulia> df.A === df[:A]\ntrue\n\njulia> df.A === df[1]\ntrue\n\njulia> firstcolumn = :A\n:A\n\njulia> df[firstcolumn] === df.A\ntrueColumn names can be obtained using the names function:julia> names(df)\n2-element Array{Symbol,1}:\n :A\n :B"
},

{
    "location": "man/getting_started.html#Constructing-Column-by-Column-1",
    "page": "Getting Started",
    "title": "Constructing Column by Column",
    "category": "section",
    "text": "It is also possible to start with an empty DataFrame and add columns to it one by one:julia> df = DataFrame()\n0×0 DataFrame\n\n\njulia> df.A = 1:8\n1:8\n\njulia> df.B = [\"M\", \"F\", \"F\", \"M\", \"F\", \"M\", \"M\", \"F\"]\n8-element Array{String,1}:\n \"M\"\n \"F\"\n \"F\"\n \"M\"\n \"F\"\n \"M\"\n \"M\"\n \"F\"\n\njulia> df\n8×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ M      │\n│ 2   │ 2     │ F      │\n│ 3   │ 3     │ F      │\n│ 4   │ 4     │ M      │\n│ 5   │ 5     │ F      │\n│ 6   │ 6     │ M      │\n│ 7   │ 7     │ M      │\n│ 8   │ 8     │ F      │\nThe DataFrame we build in this way has 8 rows and 2 columns. This can be checked using the size function:julia> size(df, 1)\n8\n\njulia> size(df, 2)\n2\n\njulia> size(df)\n(8, 2)\n"
},

{
    "location": "man/getting_started.html#Constructing-Row-by-Row-1",
    "page": "Getting Started",
    "title": "Constructing Row by Row",
    "category": "section",
    "text": "It is also possible to fill a DataFrame row by row. Let us construct an empty data frame with two columns (note that the first column can only contain integers and the second one can only contain strings):julia> df = DataFrame(A = Int[], B = String[])\n0×2 DataFrameRows can then be added as tuples or vectors, where the order of elements matches that of columns:julia> push!(df, (1, \"M\"))\n1×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ M      │\n\njulia> push!(df, [2, \"N\"])\n2×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ M      │\n│ 2   │ 2     │ N      │Rows can also be added as Dicts, where the dictionary keys match the column names:julia> push!(df, Dict(:B => \"F\", :A => 3))\n3×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ M      │\n│ 2   │ 2     │ N      │\n│ 3   │ 3     │ F      │Note that constructing a DataFrame row by row is significantly less performant than constructing it all at once, or column by column. For many use-cases this will not matter, but for very large DataFrames  this may be a consideration."
},

{
    "location": "man/getting_started.html#Constructing-from-another-table-type-1",
    "page": "Getting Started",
    "title": "Constructing from another table type",
    "category": "section",
    "text": "DataFrames supports the Tables.jl interface for interacting with tabular data. This means that a DataFrame can be used as a \"source\" to any package that expects a Tables.jl interface input, (file format packages, data manipulation packages, etc.). A DataFrame can also be a sink for any Tables.jl interface input. Some example uses are:df = DataFrame(a=[1, 2, 3], b=[:a, :b, :c])\n\n# write DataFrame out to CSV file\nCSV.write(\"dataframe.csv\", df)\n\n# store DataFrame in an SQLite database table\nSQLite.load!(df, db, \"dataframe_table\")\n\n# transform a DataFrame through Query.jl package\ndf = df |> @map({a=_.a + 1, _.b}) |> DataFrame"
},

{
    "location": "man/getting_started.html#Working-with-Data-Frames-1",
    "page": "Getting Started",
    "title": "Working with Data Frames",
    "category": "section",
    "text": ""
},

{
    "location": "man/getting_started.html#Examining-the-Data-1",
    "page": "Getting Started",
    "title": "Examining the Data",
    "category": "section",
    "text": "The default printing of DataFrame objects only includes a sample of rows and columns that fits on screen:julia> df = DataFrame(A = 1:2:1000, B = repeat(1:10, inner=50), C = 1:500)\n500×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 1     │ 1     │\n│ 2   │ 3     │ 1     │ 2     │\n│ 3   │ 5     │ 1     │ 3     │\n│ 4   │ 7     │ 1     │ 4     │\n⋮\n│ 496 │ 991   │ 10    │ 496   │\n│ 497 │ 993   │ 10    │ 497   │\n│ 498 │ 995   │ 10    │ 498   │\n│ 499 │ 997   │ 10    │ 499   │\n│ 500 │ 999   │ 10    │ 500   │Printing options can be adjusted by calling the show function manually: show(df, allrows=true) prints all rows even if they do not fit on screen and show(df, allcols=true) does the same for columns.The first and last functions can be used to look at the first and last rows of a data frame (respectively):julia> first(df, 6)\n6×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 1     │ 1     │\n│ 2   │ 3     │ 1     │ 2     │\n│ 3   │ 5     │ 1     │ 3     │\n│ 4   │ 7     │ 1     │ 4     │\n│ 5   │ 9     │ 1     │ 5     │\n│ 6   │ 11    │ 1     │ 6     │\n\njulia> last(df, 6)\n6×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 989   │ 10    │ 495   │\n│ 2   │ 991   │ 10    │ 496   │\n│ 3   │ 993   │ 10    │ 497   │\n│ 4   │ 995   │ 10    │ 498   │\n│ 5   │ 997   │ 10    │ 499   │\n│ 6   │ 999   │ 10    │ 500   │Also notice that when DataFrame is printed to the console or rendered in HTML (e.g. in Jupyter Notebook) you get an information about type of elements held in its columns. For example in this case:julia> DataFrame(a = 1:2, b = [1.0, missing],\n                 c = categorical(\'a\':\'b\'), d = [1//2, missing])\n2×4 DataFrame\n│ Row │ a     │ b        │ c            │ d         │\n│     │ Int64 │ Float64⍰ │ Categorical… │ Rationa…⍰ │\n├─────┼───────┼──────────┼──────────────┼───────────┤\n│ 1   │ 1     │ 1.0      │ \'a\'          │ 1//2      │\n│ 2   │ 2     │ missing  │ \'b\'          │ missing   │we can observe that:the first column :a can hold elements of type Int64;\nthe second column :b can hold Float64 or Missing, which is indicated by ⍰ printed after the name of type;\nthe third column :c can hold categorical data; here we notice …, which indicates that the actual name of the type was long and got truncated;\nthe type information in fourth column :d presents a situation where the name is both truncated and the type allows Missing."
},

{
    "location": "man/getting_started.html#Taking-a-Subset-1",
    "page": "Getting Started",
    "title": "Taking a Subset",
    "category": "section",
    "text": "Specific subsets of a data frame can be extracted using the indexing syntax, similar to matrices. The colon : indicates that all items (rows or columns depending on its position) should be retained:julia> df[1:3, :]\n3×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 1     │ 1     │\n│ 2   │ 3     │ 1     │ 2     │\n│ 3   │ 5     │ 1     │ 3     │\n\njulia> df[[1, 5, 10], :]\n3×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 1     │ 1     │\n│ 2   │ 9     │ 1     │ 5     │\n│ 3   │ 19    │ 1     │ 10    │\n\njulia> df[:, [:A, :B]]\n500×2 DataFrame\n│ Row │ A     │ B     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 1     │\n│ 2   │ 3     │ 1     │\n│ 3   │ 5     │ 1     │\n│ 4   │ 7     │ 1     │\n⋮\n│ 496 │ 991   │ 10    │\n│ 497 │ 993   │ 10    │\n│ 498 │ 995   │ 10    │\n│ 499 │ 997   │ 10    │\n│ 500 │ 999   │ 10    │\n\njulia> df[1:3, [:B, :A]]\n3×2 DataFrame\n│ Row │ B     │ A     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 1     │\n│ 2   │ 1     │ 3     │\n│ 3   │ 1     │ 5     │\n\njulia> df[[3, 1], [:C]]\n2×1 DataFrame\n│ Row │ C     │\n│     │ Int64 │\n├─────┼───────┤\n│ 1   │ 3     │\n│ 2   │ 1     │Do note that df[[:A]] and df[:, [:A]] return a DataFrame object, while df[:A] and df[:, :A] return a vector:julia> df[[:A]]\n500×1 DataFrame\n│ Row │ A     │\n│     │ Int64 │\n├─────┼───────┤\n│ 1   │ 1     │\n│ 2   │ 3     │\n│ 3   │ 5     │\n│ 4   │ 7     │\n⋮\n│ 496 │ 991   │\n│ 497 │ 993   │\n│ 498 │ 995   │\n│ 499 │ 997   │\n│ 500 │ 999   │\n\njulia> df[[:A]] == df[:, [:A]]\ntrue\n\njulia> df[:A]\n500-element Array{Int64,1}:\n   1\n   3\n   5\n   7\n   9\n  11\n   ⋮\n 991\n 993\n 995\n 997\n 999\n\njulia> df[:A] == df[:, :A]\ntrueIn the first cases, [:A] is a vector, indicating that the resulting object should be a DataFrame, since a vector can contain one or more column names. On the other hand, :A is a single symbol, indicating that a single column vector should be extracted.The indexing syntax can also be used to select rows based on conditions on variables:julia> df[df.A .> 500, :]\n250×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 501   │ 6     │ 251   │\n│ 2   │ 503   │ 6     │ 252   │\n│ 3   │ 505   │ 6     │ 253   │\n│ 4   │ 507   │ 6     │ 254   │\n⋮\n│ 246 │ 991   │ 10    │ 496   │\n│ 247 │ 993   │ 10    │ 497   │\n│ 248 │ 995   │ 10    │ 498   │\n│ 249 │ 997   │ 10    │ 499   │\n│ 250 │ 999   │ 10    │ 500   │\n\njulia> df[(df.A .> 500) .& (300 .< df.C .< 400), :]\n99×3 DataFrame\n│ Row │ A     │ B     │ C     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 601   │ 7     │ 301   │\n│ 2   │ 603   │ 7     │ 302   │\n│ 3   │ 605   │ 7     │ 303   │\n│ 4   │ 607   │ 7     │ 304   │\n⋮\n│ 95  │ 789   │ 8     │ 395   │\n│ 96  │ 791   │ 8     │ 396   │\n│ 97  │ 793   │ 8     │ 397   │\n│ 98  │ 795   │ 8     │ 398   │\n│ 99  │ 797   │ 8     │ 399   │While the DataFrames package provides basic data manipulation capabilities, users are encouraged to use querying frameworks for more convenient and powerful operations:the Query.jl package provides a LINQ-like interface to a large number of data sources\nthe DataFramesMeta.jl package provides interfaces similar to LINQ and dplyrSee the Querying frameworks section for more information."
},

{
    "location": "man/getting_started.html#Summarizing-Data-1",
    "page": "Getting Started",
    "title": "Summarizing Data",
    "category": "section",
    "text": "The describe function returns a data frame summarizing the elementary statistics and information about each column:julia> df = DataFrame(A = 1:4, B = [\"M\", \"F\", \"F\", \"M\"])\n\njulia> describe(df)\n2×8 DataFrame\n│ Row │ variable │ mean   │ min │ median │ max │ nunique │ nmissing │ eltype   │\n│     │ Symbol   │ Union… │ Any │ Union… │ Any │ Union…  │ Nothing  │ DataType │\n├─────┼──────────┼────────┼─────┼────────┼─────┼─────────┼──────────┼──────────┤\n│ 1   │ A        │ 2.5    │ 1   │ 2.5    │ 4   │         │          │ Int64    │\n│ 2   │ B        │        │ F   │        │ M   │ 2       │          │ String   │\nOf course, one can also compute descrptive statistics directly on individual columns:julia> using Statistics\n\njulia> mean(df.A)\n2.5"
},

{
    "location": "man/getting_started.html#Column-Wise-Operations-1",
    "page": "Getting Started",
    "title": "Column-Wise Operations",
    "category": "section",
    "text": "We can also apply a function to each column of a DataFrame with the colwise function. For example:julia> df = DataFrame(A = 1:4, B = 4.0:-1.0:1.0)\n4×2 DataFrame\n│ Row │ A     │ B       │\n│     │ Int64 │ Float64 │\n├─────┼───────┼─────────┤\n│ 1   │ 1     │ 4.0     │\n│ 2   │ 2     │ 3.0     │\n│ 3   │ 3     │ 2.0     │\n│ 4   │ 4     │ 1.0     │\n\njulia> colwise(sum, df)\n2-element Array{Real,1}:\n 10\n 10.0"
},

{
    "location": "man/getting_started.html#Importing-and-Exporting-Data-(I/O)-1",
    "page": "Getting Started",
    "title": "Importing and Exporting Data (I/O)",
    "category": "section",
    "text": "For reading and writing tabular data from CSV and other delimited text files, use the CSV.jl package.If you have not used the CSV.jl package before then you may need to install it first:using Pkg\nPkg.add(\"CSV\")The CSV.jl functions are not loaded automatically and must be imported into the session.using CSVA dataset can now be read from a CSV file at path input usingCSV.read(input)A DataFrame can be written to a CSV file at path output usingdf = DataFrame(x = 1, y = 2)\nCSV.write(output, df)The behavior of CSV functions can be adapted via keyword arguments. For more information, see ?CSV.read and ?CSV.write, or checkout the online CSV.jl documentation."
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
    "text": "We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:julia> using DataFrames\n\njulia> names = DataFrame(ID = [20, 40], Name = [\"John Doe\", \"Jane Doe\"])\n2×2 DataFrame\n│ Row │ ID    │ Name     │\n│     │ Int64 │ String   │\n├─────┼───────┼──────────┤\n│ 1   │ 20    │ John Doe │\n│ 2   │ 40    │ Jane Doe │\n\njulia> jobs = DataFrame(ID = [20, 40], Job = [\"Lawyer\", \"Doctor\"])\n2×2 DataFrame\n│ Row │ ID    │ Job    │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 20    │ Lawyer │\n│ 2   │ 40    │ Doctor │\nWe might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the join function:julia> join(names, jobs, on = :ID)\n2×3 DataFrame\n│ Row │ ID    │ Name     │ Job    │\n│     │ Int64 │ String   │ String │\n├─────┼───────┼──────────┼────────┤\n│ 1   │ 20    │ John Doe │ Lawyer │\n│ 2   │ 40    │ Jane Doe │ Doctor │\nIn relational database theory, this operation is generally referred to as a join. The columns used to determine which rows should be combined during a join are called keys.There are seven kinds of joins supported by the DataFrames package:Inner: The output contains rows for values of the key that exist in both the first (left) and second (right) arguments to join.\nLeft: The output contains rows for values of the key that exist in the first (left) argument to join, whether or not that value exists in the second (right) argument.\nRight: The output contains rows for values of the key that exist in the second (right) argument to join, whether or not that value exists in the first (left) argument.\nOuter: The output contains rows for values of the key that exist in the first (left) or second (right) argument to join.\nSemi: Like an inner join, but output is restricted to columns from the first (left) argument to join.\nAnti: The output contains rows for values of the key that exist in the first (left) but not the second (right) argument to join. As with semi joins, output is restricted to columns from the first (left) argument.\nCross: The output is the cartesian product of rows from the first (left) and second (right) arguments to join.See the Wikipedia page on SQL joins for more information.You can control the kind of join that join performs using the kind keyword argument:julia> jobs = DataFrame(ID = [20, 60], Job = [\"Lawyer\", \"Astronaut\"])\n2×2 DataFrame\n│ Row │ ID    │ Job       │\n│     │ Int64 │ String    │\n├─────┼───────┼───────────┤\n│ 1   │ 20    │ Lawyer    │\n│ 2   │ 60    │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :inner)\n1×3 DataFrame\n│ Row │ ID    │ Name     │ Job    │\n│     │ Int64 │ String   │ String │\n├─────┼───────┼──────────┼────────┤\n│ 1   │ 20    │ John Doe │ Lawyer │\n\njulia> join(names, jobs, on = :ID, kind = :left)\n2×3 DataFrame\n│ Row │ ID    │ Name     │ Job     │\n│     │ Int64 │ String   │ String⍰ │\n├─────┼───────┼──────────┼─────────┤\n│ 1   │ 20    │ John Doe │ Lawyer  │\n│ 2   │ 40    │ Jane Doe │ missing │\n\njulia> join(names, jobs, on = :ID, kind = :right)\n2×3 DataFrame\n│ Row │ ID    │ Name     │ Job       │\n│     │ Int64 │ String⍰  │ String    │\n├─────┼───────┼──────────┼───────────┤\n│ 1   │ 20    │ John Doe │ Lawyer    │\n│ 2   │ 60    │ missing  │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :outer)\n3×3 DataFrame\n│ Row │ ID    │ Name     │ Job       │\n│     │ Int64 │ String⍰  │ String⍰   │\n├─────┼───────┼──────────┼───────────┤\n│ 1   │ 20    │ John Doe │ Lawyer    │\n│ 2   │ 40    │ Jane Doe │ missing   │\n│ 3   │ 60    │ missing  │ Astronaut │\n\njulia> join(names, jobs, on = :ID, kind = :semi)\n1×2 DataFrame\n│ Row │ ID    │ Name     │\n│     │ Int64 │ String   │\n├─────┼───────┼──────────┤\n│ 1   │ 20    │ John Doe │\n\njulia> join(names, jobs, on = :ID, kind = :anti)\n1×2 DataFrame\n│ Row │ ID    │ Name     │\n│     │ Int64 │ String   │\n├─────┼───────┼──────────┤\n│ 1   │ 40    │ Jane Doe │\nCross joins are the only kind of join that does not use a key:julia> join(names, jobs, kind = :cross, makeunique = true)\n4×4 DataFrame\n│ Row │ ID    │ Name     │ ID_1  │ Job       │\n│     │ Int64 │ String   │ Int64 │ String    │\n├─────┼───────┼──────────┼───────┼───────────┤\n│ 1   │ 20    │ John Doe │ 20    │ Lawyer    │\n│ 2   │ 20    │ John Doe │ 60    │ Astronaut │\n│ 3   │ 40    │ Jane Doe │ 20    │ Lawyer    │\n│ 4   │ 40    │ Jane Doe │ 60    │ Astronaut │\nIn order to join data tables on keys which have different names, you must first rename them so that they match. This can be done using rename!:julia> a = DataFrame(ID = [20, 40], Name = [\"John Doe\", \"Jane Doe\"])\n2×2 DataFrame\n│ Row │ ID    │ Name     │\n│     │ Int64 │ String   │\n├─────┼───────┼──────────┤\n│ 1   │ 20    │ John Doe │\n│ 2   │ 40    │ Jane Doe │\n\njulia> b = DataFrame(IDNew = [20, 40], Job = [\"Lawyer\", \"Doctor\"])\n2×2 DataFrame\n│ Row │ IDNew │ Job    │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 20    │ Lawyer │\n│ 2   │ 40    │ Doctor │\n\njulia> rename!(b, :IDNew => :ID)\n2×2 DataFrame\n│ Row │ ID    │ Job    │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 20    │ Lawyer │\n│ 2   │ 40    │ Doctor │\n\njulia> join(a, b, on = :ID, kind = :inner)\n2×3 DataFrame\n│ Row │ ID    │ Name     │ Job    │\n│     │ Int64 │ String   │ String │\n├─────┼───────┼──────────┼────────┤\n│ 1   │ 20    │ John Doe │ Lawyer │\n│ 2   │ 40    │ Jane Doe │ Doctor │\nOr renaming multiple columns at a time:julia> a = DataFrame(City = [\"Amsterdam\", \"London\", \"London\", \"New York\", \"New York\"],\n                     Job = [\"Lawyer\", \"Lawyer\", \"Lawyer\", \"Doctor\", \"Doctor\"],\n                     Category = [1, 2, 3, 4, 5])\n5×3 DataFrame\n│ Row │ City      │ Job    │ Category │\n│     │ String    │ String │ Int64    │\n├─────┼───────────┼────────┼──────────┤\n│ 1   │ Amsterdam │ Lawyer │ 1        │\n│ 2   │ London    │ Lawyer │ 2        │\n│ 3   │ London    │ Lawyer │ 3        │\n│ 4   │ New York  │ Doctor │ 4        │\n│ 5   │ New York  │ Doctor │ 5        │\n\njulia> b = DataFrame(Location = [\"Amsterdam\", \"London\", \"London\", \"New York\", \"New York\"],\n                     Work = [\"Lawyer\", \"Lawyer\", \"Lawyer\", \"Doctor\", \"Doctor\"],\n                     Name = [\"a\", \"b\", \"c\", \"d\", \"e\"])\n5×3 DataFrame\n│ Row │ Location  │ Work   │ Name   │\n│     │ String    │ String │ String │\n├─────┼───────────┼────────┼────────┤\n│ 1   │ Amsterdam │ Lawyer │ a      │\n│ 2   │ London    │ Lawyer │ b      │\n│ 3   │ London    │ Lawyer │ c      │\n│ 4   │ New York  │ Doctor │ d      │\n│ 5   │ New York  │ Doctor │ e      │\n\njulia> rename!(b, :Location => :City, :Work => :Job)\n5×3 DataFrame\n│ Row │ City      │ Job    │ Name   │\n│     │ String    │ String │ String │\n├─────┼───────────┼────────┼────────┤\n│ 1   │ Amsterdam │ Lawyer │ a      │\n│ 2   │ London    │ Lawyer │ b      │\n│ 3   │ London    │ Lawyer │ c      │\n│ 4   │ New York  │ Doctor │ d      │\n│ 5   │ New York  │ Doctor │ e      │\n\njulia> join(a, b, on = [:City, :Job])\n9×4 DataFrame\n│ Row │ City      │ Job    │ Category │ Name   │\n│     │ String    │ String │ Int64    │ String │\n├─────┼───────────┼────────┼──────────┼────────┤\n│ 1   │ Amsterdam │ Lawyer │ 1        │ a      │\n│ 2   │ London    │ Lawyer │ 2        │ b      │\n│ 3   │ London    │ Lawyer │ 2        │ c      │\n│ 4   │ London    │ Lawyer │ 3        │ b      │\n│ 5   │ London    │ Lawyer │ 3        │ c      │\n│ 6   │ New York  │ Doctor │ 4        │ d      │\n│ 7   │ New York  │ Doctor │ 4        │ e      │\n│ 8   │ New York  │ Doctor │ 5        │ d      │\n│ 9   │ New York  │ Doctor │ 5        │ e      │\n"
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
    "text": "Many data analysis tasks involve splitting a data set into groups, applying some functions to each of the groups and then combining the results. A standardized framework for handling this sort of computation is described in the paper \"The Split-Apply-Combine Strategy for Data Analysis\", written by Hadley Wickham.The DataFrames package supports the Split-Apply-Combine strategy through the by function, which takes in three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) a function or expression to apply to each subset of the DataFrame.We show several examples of the by function applied to the iris dataset below:julia> using DataFrames, CSV, Statistics\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa        │\n│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa        │\n│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa        │\n│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │\n│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa        │\n│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica     │\n│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica     │\n│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica     │\n│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica     │\n│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica     │\n│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica     │\n\njulia> by(iris, :Species, size)\n3×2 DataFrame\n│ Row │ Species       │ x1      │\n│     │ Categorical…⍰ │ Tuple…  │\n├─────┼───────────────┼─────────┤\n│ 1   │ setosa        │ (50, 5) │\n│ 2   │ versicolor    │ (50, 5) │\n│ 3   │ virginica     │ (50, 5) │\n\njulia> by(iris, :Species, df -> mean(df.PetalLength))\n3×2 DataFrame\n│ Row │ Species       │ x1      │\n│     │ Categorical…⍰ │ Float64 │\n├─────┼───────────────┼─────────┤\n│ 1   │ setosa        │ 1.462   │\n│ 2   │ versicolor    │ 4.26    │\n│ 3   │ virginica     │ 5.552   │\n\njulia> by(iris, :Species, df -> DataFrame(N = size(df, 1)))\n3×2 DataFrame\n│ Row │ Species       │ N     │\n│     │ Categorical…⍰ │ Int64 │\n├─────┼───────────────┼───────┤\n│ 1   │ setosa        │ 50    │\n│ 2   │ versicolor    │ 50    │\n│ 3   │ virginica     │ 50    │The by function also support the do block form:julia> by(iris, :Species) do df\n          DataFrame(m = mean(df.PetalLength), s² = var(df.PetalLength))\n       end\n3×3 DataFrame\n│ Row │ Species       │ m       │ s²        │\n│     │ Categorical…⍰ │ Float64 │ Float64   │\n├─────┼───────────────┼─────────┼───────────┤\n│ 1   │ setosa        │ 1.462   │ 0.0301592 │\n│ 2   │ versicolor    │ 4.26    │ 0.220816  │\n│ 3   │ virginica     │ 5.552   │ 0.304588  │A second approach to the Split-Apply-Combine strategy is implemented in the aggregate function, which also takes three arguments: (1) a DataFrame, (2) one or more columns to split the DataFrame on, and (3) one or more functions that are used to compute a summary of each subset of the DataFrame. Each function is applied to each column that was not used to split the DataFrame, creating new columns of the form $name_$function. For named functions like mean this will produce columns with names like SepalLength_mean. For anonymous functions like x -> sqrt(x)^e, which Julia tracks and references by a numerical identifier e.g. #12, the produced columns will be SepalLength_#12. We show several examples of the aggregate function applied to the iris dataset below:julia> aggregate(iris, :Species, length)\n3×5 DataFrame\n│ Row │ Species       │ SepalLength_length │ SepalWidth_length │ PetalLength_length │ PetalWidth_length │\n│     │ Categorical…⍰ │ Int64              │ Int64             │ Int64              │ Int64             │\n├─────┼───────────────┼────────────────────┼───────────────────┼────────────────────┼───────────────────┤\n│ 1   │ setosa        │ 50                 │ 50                │ 50                 │ 50                │\n│ 2   │ versicolor    │ 50                 │ 50                │ 50                 │ 50                │\n│ 3   │ virginica     │ 50                 │ 50                │ 50                 │ 50                │\n\njulia> aggregate(iris, :Species, [sum, mean])\n3×9 DataFrame. Omitted printing of 2 columns\n│ Row │ Species       │ SepalLength_sum │ SepalWidth_sum │ PetalLength_sum │ PetalWidth_sum │ SepalLength_mean │ SepalWidth_mean │\n│     │ Categorical…⍰ │ Float64         │ Float64        │ Float64         │ Float64        │ Float64          │ Float64         │\n├─────┼───────────────┼─────────────────┼────────────────┼─────────────────┼────────────────┼──────────────────┼─────────────────┤\n│ 1   │ setosa        │ 250.3           │ 171.4          │ 73.1            │ 12.3           │ 5.006            │ 3.428           │\n│ 2   │ versicolor    │ 296.8           │ 138.5          │ 213.0           │ 66.3           │ 5.936            │ 2.77            │\n│ 3   │ virginica     │ 329.4           │ 148.7          │ 277.6           │ 101.3          │ 6.588            │ 2.974           │If you only want to split the data set into subsets, use the groupby function:julia> for subdf in groupby(iris, :Species)\n           println(size(subdf, 1))\n       end\n50\n50\n50"
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
    "text": "Reshape data from wide to long format using the stack function:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa        │\n│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa        │\n│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa        │\n│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │\n│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa        │\n│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica     │\n│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica     │\n│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica     │\n│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica     │\n│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica     │\n│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica     │\n\njulia> d = stack(iris, 1:4);\n\njulia> first(d, 6)\n6×3 DataFrame\n│ Row │ variable    │ value    │ Species       │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │\n├─────┼─────────────┼──────────┼───────────────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │\n│ 2   │ SepalLength │ 4.9      │ setosa        │\n│ 3   │ SepalLength │ 4.7      │ setosa        │\n│ 4   │ SepalLength │ 4.6      │ setosa        │\n│ 5   │ SepalLength │ 5.0      │ setosa        │\n│ 6   │ SepalLength │ 5.4      │ setosa        │\n\njulia> last(d, 6)\n6×3 DataFrame\n│ Row │ variable   │ value    │ Species       │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │\n├─────┼────────────┼──────────┼───────────────┤\n│ 1   │ PetalWidth │ 2.5      │ virginica     │\n│ 2   │ PetalWidth │ 2.3      │ virginica     │\n│ 3   │ PetalWidth │ 1.9      │ virginica     │\n│ 4   │ PetalWidth │ 2.0      │ virginica     │\n│ 5   │ PetalWidth │ 2.3      │ virginica     │\n│ 6   │ PetalWidth │ 1.8      │ virginica     │The second optional argument to stack indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:julia> d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth]);\n\njulia> first(d, 6)\n6×3 DataFrame\n│ Row │ variable    │ value    │ Species       │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │\n├─────┼─────────────┼──────────┼───────────────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │\n│ 2   │ SepalLength │ 4.9      │ setosa        │\n│ 3   │ SepalLength │ 4.7      │ setosa        │\n│ 4   │ SepalLength │ 4.6      │ setosa        │\n│ 5   │ SepalLength │ 5.0      │ setosa        │\n│ 6   │ SepalLength │ 5.4      │ setosa        │\n\njulia> last(d, 6)\n6×3 DataFrame\n│ Row │ variable   │ value    │ Species       │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │\n├─────┼────────────┼──────────┼───────────────┤\n│ 1   │ PetalWidth │ 2.5      │ virginica     │\n│ 2   │ PetalWidth │ 2.3      │ virginica     │\n│ 3   │ PetalWidth │ 1.9      │ virginica     │\n│ 4   │ PetalWidth │ 2.0      │ virginica     │\n│ 5   │ PetalWidth │ 2.3      │ virginica     │\n│ 6   │ PetalWidth │ 1.8      │ virginica     │\nNote that all columns can be of different types. Type promotion follows the rules of vcat.The stacked DataFrame that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled :variable and :values contain the column identifier and the stacked columns.A third optional argument to stack represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:julia> d = stack(iris, [:SepalLength, :SepalWidth], :Species);\n\njulia> first(d, 6)\n6×3 DataFrame\n│ Row │ variable    │ value    │ Species       │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │\n├─────┼─────────────┼──────────┼───────────────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │\n│ 2   │ SepalLength │ 4.9      │ setosa        │\n│ 3   │ SepalLength │ 4.7      │ setosa        │\n│ 4   │ SepalLength │ 4.6      │ setosa        │\n│ 5   │ SepalLength │ 5.0      │ setosa        │\n│ 6   │ SepalLength │ 5.4      │ setosa        │\n\njulia> last(d, 6)\n6×3 DataFrame\n│ Row │ variable   │ value    │ Species       │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │\n├─────┼────────────┼──────────┼───────────────┤\n│ 1   │ SepalWidth │ 3.3      │ virginica     │\n│ 2   │ SepalWidth │ 3.0      │ virginica     │\n│ 3   │ SepalWidth │ 2.5      │ virginica     │\n│ 4   │ SepalWidth │ 3.0      │ virginica     │\n│ 5   │ SepalWidth │ 3.4      │ virginica     │\n│ 6   │ SepalWidth │ 3.0      │ virginica     │melt is an alternative function to reshape from wide to long format. It is based on stack, but it prefers specification of the id columns as:julia> d = melt(iris, :Species);\n\njulia> first(d, 6)\n6×3 DataFrame\n│ Row │ variable    │ value    │ Species       │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │\n├─────┼─────────────┼──────────┼───────────────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │\n│ 2   │ SepalLength │ 4.9      │ setosa        │\n│ 3   │ SepalLength │ 4.7      │ setosa        │\n│ 4   │ SepalLength │ 4.6      │ setosa        │\n│ 5   │ SepalLength │ 5.0      │ setosa        │\n│ 6   │ SepalLength │ 5.4      │ setosa        │\n\njulia> last(d, 6)\n6×3 DataFrame\n│ Row │ variable   │ value    │ Species       │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │\n├─────┼────────────┼──────────┼───────────────┤\n│ 1   │ PetalWidth │ 2.5      │ virginica     │\n│ 2   │ PetalWidth │ 2.3      │ virginica     │\n│ 3   │ PetalWidth │ 1.9      │ virginica     │\n│ 4   │ PetalWidth │ 2.0      │ virginica     │\n│ 5   │ PetalWidth │ 2.3      │ virginica     │\n│ 6   │ PetalWidth │ 1.8      │ virginica     │unstack converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:julia> iris[:id] = 1:size(iris, 1)\n1:150\n\njulia> longdf = melt(iris, [:Species, :id]);\n\njulia> first(longdf, 6)\n6×4 DataFrame\n│ Row │ variable    │ value    │ Species       │ id    │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │\n├─────┼─────────────┼──────────┼───────────────┼───────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │\n│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │\n│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │\n│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │\n│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │\n│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │\n\njulia> last(longdf, 6)\n6×4 DataFrame\n│ Row │ variable   │ value    │ Species       │ id    │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │ Int64 │\n├─────┼────────────┼──────────┼───────────────┼───────┤\n│ 1   │ PetalWidth │ 2.5      │ virginica     │ 145   │\n│ 2   │ PetalWidth │ 2.3      │ virginica     │ 146   │\n│ 3   │ PetalWidth │ 1.9      │ virginica     │ 147   │\n│ 4   │ PetalWidth │ 2.0      │ virginica     │ 148   │\n│ 5   │ PetalWidth │ 2.3      │ virginica     │ 149   │\n│ 6   │ PetalWidth │ 1.8      │ virginica     │ 150   │\n\njulia> widedf = unstack(longdf, :id, :variable, :value);\n\njulia> first(widedf, 6)\n6×5 DataFrame\n│ Row │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n│     │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │\n├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ 1     │ 1.4         │ 0.2        │ 5.1         │ 3.5        │\n│ 2   │ 2     │ 1.4         │ 0.2        │ 4.9         │ 3.0        │\n│ 3   │ 3     │ 1.3         │ 0.2        │ 4.7         │ 3.2        │\n│ 4   │ 4     │ 1.5         │ 0.2        │ 4.6         │ 3.1        │\n│ 5   │ 5     │ 1.4         │ 0.2        │ 5.0         │ 3.6        │\n│ 6   │ 6     │ 1.7         │ 0.4        │ 5.4         │ 3.9        │\n\njulia> last(widedf, 6)\n6×5 DataFrame\n│ Row │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n│     │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │\n├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ 145   │ 5.7         │ 2.5        │ 6.7         │ 3.3        │\n│ 2   │ 146   │ 5.2         │ 2.3        │ 6.7         │ 3.0        │\n│ 3   │ 147   │ 5.0         │ 1.9        │ 6.3         │ 2.5        │\n│ 4   │ 148   │ 5.2         │ 2.0        │ 6.5         │ 3.0        │\n│ 5   │ 149   │ 5.4         │ 2.3        │ 6.2         │ 3.4        │\n│ 6   │ 150   │ 5.1         │ 1.8        │ 5.9         │ 3.0        │If the remaining columns are unique, you can skip the id variable and use:julia> longdf = melt(iris, [:Species, :id]);\n\njulia> first(longdf, 6)\n6×4 DataFrame\n│ Row │ variable    │ value    │ Species       │ id    │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │\n├─────┼─────────────┼──────────┼───────────────┼───────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │\n│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │\n│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │\n│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │\n│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │\n│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │\n\njulia> widedf = unstack(longdf, :variable, :value);\n\njulia> first(widedf, 6)\n6×6 DataFrame\n│ Row │ Species       │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │\n│     │ Categorical…⍰ │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │\n├─────┼───────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤\n│ 1   │ setosa        │ 1     │ 1.4         │ 0.2        │ 5.1         │ 3.5        │\n│ 2   │ setosa        │ 2     │ 1.4         │ 0.2        │ 4.9         │ 3.0        │\n│ 3   │ setosa        │ 3     │ 1.3         │ 0.2        │ 4.7         │ 3.2        │\n│ 4   │ setosa        │ 4     │ 1.5         │ 0.2        │ 4.6         │ 3.1        │\n│ 5   │ setosa        │ 5     │ 1.4         │ 0.2        │ 5.0         │ 3.6        │\n│ 6   │ setosa        │ 6     │ 1.7         │ 0.4        │ 5.4         │ 3.9        │stackdf and meltdf are two additional functions that work like stack and melt, but they provide a view into the original wide DataFrame. Here is an example:julia> d = stackdf(iris);\n\njulia> first(d, 6)\n6×4 DataFrame\n│ Row │ variable    │ value    │ Species       │ id    │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │\n├─────┼─────────────┼──────────┼───────────────┼───────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │\n│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │\n│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │\n│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │\n│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │\n│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │\n\njulia> last(d, 6)\n6×4 DataFrame\n│ Row │ variable   │ value    │ Species       │ id    │\n│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │ Int64 │\n├─────┼────────────┼──────────┼───────────────┼───────┤\n│ 1   │ PetalWidth │ 2.5      │ virginica     │ 145   │\n│ 2   │ PetalWidth │ 2.3      │ virginica     │ 146   │\n│ 3   │ PetalWidth │ 1.9      │ virginica     │ 147   │\n│ 4   │ PetalWidth │ 2.0      │ virginica     │ 148   │\n│ 5   │ PetalWidth │ 2.3      │ virginica     │ 149   │\n│ 6   │ PetalWidth │ 1.8      │ virginica     │ 150   │This saves memory. To create the view, several AbstractVectors are defined::variable column – EachRepeatedVector   This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.:value column – StackedVector   This is provides a view of the original columns stacked together.Id columns – RepeatedVector   This repeats the original columns N times where N is the number of columns stacked.None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:julia> d = melt(iris, :Species);\n\njulia> first(d, 6)\n6×3 DataFrame\n│ Row │ variable    │ value    │ Species       │\n│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │\n├─────┼─────────────┼──────────┼───────────────┤\n│ 1   │ SepalLength │ 5.1      │ setosa        │\n│ 2   │ SepalLength │ 4.9      │ setosa        │\n│ 3   │ SepalLength │ 4.7      │ setosa        │\n│ 4   │ SepalLength │ 4.6      │ setosa        │\n│ 5   │ SepalLength │ 5.0      │ setosa        │\n│ 6   │ SepalLength │ 5.4      │ setosa        │\n\njulia> x = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(df[:value])));\n\njulia> first(x, 6)\n\n6×3 DataFrame\n│ Row │ variable    │ Species       │ vsum    │\n│     │ Symbol      │ Categorical…⍰ │ Float64 │\n├─────┼─────────────┼───────────────┼─────────┤\n│ 1   │ SepalLength │ setosa        │ 5.006   │\n│ 2   │ SepalLength │ versicolor    │ 5.936   │\n│ 3   │ SepalLength │ virginica     │ 6.588   │\n│ 4   │ SepalWidth  │ setosa        │ 3.428   │\n│ 5   │ SepalWidth  │ versicolor    │ 2.77    │\n│ 6   │ SepalWidth  │ virginica     │ 2.974   │\n\njulia> first(unstack(x, :Species, :vsum), 6)\n5×4 DataFrame\n│ Row │ variable    │ setosa   │ versicolor │ virginica │\n│     │ Symbol      │ Float64⍰ │ Float64⍰   │ Float64⍰  │\n├─────┼─────────────┼──────────┼────────────┼───────────┤\n│ 1   │ PetalLength │ 1.462    │ 4.26       │ 5.552     │\n│ 2   │ PetalWidth  │ 0.246    │ 1.326      │ 2.026     │\n│ 3   │ SepalLength │ 5.006    │ 5.936      │ 6.588     │\n│ 4   │ SepalWidth  │ 3.428    │ 2.77       │ 2.974     │\n│ 5   │ id          │ 25.5     │ 75.5       │ 125.5     │"
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
    "text": "Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling sort! will sort all columns, in place:julia> using DataFrames, CSV\n\njulia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), \"../test/data/iris.csv\"));\n\njulia> sort!(iris);\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa        │\n│ 2   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa        │\n│ 3   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa        │\n│ 4   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa        │\n│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │\n│ 6   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica     │\n│ 2   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica     │\n│ 3   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica     │\n│ 4   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica     │\n│ 5   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica     │\n│ 6   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica     │In Sorting DataFrames, you may want to sort different columns with different options. Here are some examples showing most of the possible options:julia> sort!(iris, rev = true);\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica     │\n│ 2   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica     │\n│ 3   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica     │\n│ 4   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica     │\n│ 5   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica     │\n│ 6   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica     │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │\n│ 2   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │\n│ 3   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa        │\n│ 4   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa        │\n│ 5   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa        │\n│ 6   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa        │\n\njulia> sort!(iris, (:SepalWidth, :SepalLength));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.0         │ 2.0        │ 3.5         │ 1.0        │ versicolor    │\n│ 2   │ 6.0         │ 2.2        │ 5.0         │ 1.5        │ virginica     │\n│ 3   │ 6.0         │ 2.2        │ 4.0         │ 1.0        │ versicolor    │\n│ 4   │ 6.2         │ 2.2        │ 4.5         │ 1.5        │ versicolor    │\n│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │\n│ 6   │ 5.0         │ 2.3        │ 3.3         │ 1.0        │ versicolor    │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │\n│ 2   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │\n│ 3   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │\n│ 4   │ 5.2         │ 4.1        │ 1.5         │ 0.1        │ setosa        │\n│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │\n│ 6   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │\n\njulia> sort!(iris, (order(:Species, by = uppercase),\n                    order(:SepalLength, rev = true)));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │\n│ 2   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │\n│ 3   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │\n│ 4   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │\n│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │\n│ 6   │ 5.4         │ 3.4        │ 1.7         │ 0.2        │ setosa        │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 2   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 3   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │\n│ 4   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │\n│ 5   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │\n│ 6   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │Keywords used above include rev (to sort a column or the whole DataFrame in reverse), and by (to apply a function to a column/DataFrame). Each keyword can either be a single value, or can be a tuple or array, with values corresponding to individual columns.As an alternative to using array or tuple values, order to specify an ordering for a particular column within a set of columnsThe following two examples show two ways to sort the iris dataset with the same result: Species will be ordered in reverse lexicographic order, and within species, rows will be sorted by increasing sepal length and width:julia> sort!(iris, (:Species, :SepalLength, :SepalWidth),\n                    rev = (true, false, false));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │\n│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │\n│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │\n│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │\n│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │\n│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │\n│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │\n│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │\n│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │\n\njulia> sort!(iris, (order(:Species, rev = true), :SepalLength, :SepalWidth));\n\njulia> first(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │\n│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │\n│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │\n│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │\n│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │\n\njulia> last(iris, 6)\n6×5 DataFrame\n│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │\n│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │\n├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤\n│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │\n│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │\n│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │\n│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │\n│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │\n│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │"
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
    "text": "Often, we have to deal with factors that take on a small number of levels:julia> v = [\"Group A\", \"Group A\", \"Group A\", \"Group B\", \"Group B\", \"Group B\"]\n6-element Array{String,1}:\n \"Group A\"\n \"Group A\"\n \"Group A\"\n \"Group B\"\n \"Group B\"\n \"Group B\"\nThe naive encoding used in an Array represents every entry of this vector as a full string. In contrast, we can represent the data more efficiently by replacing the strings with indices into a small pool of levels. This is what the CategoricalArray type does:julia> using CategoricalArrays\n\njulia> cv = CategoricalArray(v)\n6-element CategoricalArrays.CategoricalArray{String,1,UInt32}:\n \"Group A\"\n \"Group A\"\n \"Group A\"\n \"Group B\"\n \"Group B\"\n \"Group B\"\nCategoricalArrays support missing values via the Missings package.julia> using Missings\n\njulia> cv = CategoricalArray([\"Group A\", missing, \"Group A\",\n                              \"Group B\", \"Group B\", missing])\n6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt32}:\n \"Group A\"\n missing\n \"Group A\"\n \"Group B\"\n \"Group B\"\n missingIn addition to representing repeated data efficiently, the CategoricalArray type allows us to determine efficiently the allowed levels of the variable at any time using the levels function (note that levels may or may not be actually used in the data):julia> levels(cv)\n2-element Array{String,1}:\n \"Group A\"\n \"Group B\"\nThe levels! function also allows changing the order of appearance of the levels, which can be useful for display purposes or when working with ordered variables.julia> levels!(cv, [\"Group B\", \"Group A\"]);\n\njulia> levels(cv)\n2-element Array{String,1}:\n \"Group B\"\n \"Group A\"\n\njulia> sort(cv)\n6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt32}:\n \"Group B\"\n \"Group B\"\n \"Group A\"\n \"Group A\"\n missing\n missing\nBy default, a CategoricalArray is able to represent 2<sup>32</sup>differents levels. You can use less memory by calling the compress function:julia> cv = compress(cv)\n6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt8}:\n \"Group A\"\n missing\n \"Group A\"\n \"Group B\"\n \"Group B\"\n missing\nOften, you will have factors encoded inside a DataFrame with Array columns instead of CategoricalArray columns. You can convert one or more columns of the DataFrame using the categorical! function, which modifies the input DataFrame in-place.julia> using DataFrames\n\njulia> df = DataFrame(A = [\"A\", \"B\", \"C\", \"D\", \"D\", \"A\"],\n                      B = [\"X\", \"X\", \"X\", \"Y\", \"Y\", \"Y\"])\n6×2 DataFrame\n│ Row │ A      │ B      │\n│     │ String │ String │\n├─────┼────────┼────────┤\n│ 1   │ A      │ X      │\n│ 2   │ B      │ X      │\n│ 3   │ C      │ X      │\n│ 4   │ D      │ Y      │\n│ 5   │ D      │ Y      │\n│ 6   │ A      │ Y      │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n String\n String\n\njulia> categorical!(df, :A) # change the column `:A` to be categorical\n6×2 DataFrame\n│ Row │ A            │ B      │\n│     │ Categorical… │ String │\n├─────┼──────────────┼────────┤\n│ 1   │ A            │ X      │\n│ 2   │ B            │ X      │\n│ 3   │ C            │ X      │\n│ 4   │ D            │ Y      │\n│ 5   │ D            │ Y      │\n│ 6   │ A            │ Y      │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n CategoricalArrays.CategoricalString{UInt32}\n String\n\njulia> categorical!(df) # change all columns to be categorical\n6×2 DataFrame\n│ Row │ A            │ B            │\n│     │ Categorical… │ Categorical… │\n├─────┼──────────────┼──────────────┤\n│ 1   │ A            │ X            │\n│ 2   │ B            │ X            │\n│ 3   │ C            │ X            │\n│ 4   │ D            │ Y            │\n│ 5   │ D            │ Y            │\n│ 6   │ A            │ Y            │\n\njulia> eltypes(df)\n2-element Array{Type,1}:\n CategoricalArrays.CategoricalString{UInt32}\n CategoricalArrays.CategoricalString{UInt32}\nUsing categorical arrays is important for working with the GLM package. When fitting regression models, CategoricalArray columns in the input are translated into 0/1 indicator columns in the ModelMatrix with one column for each of the levels of the CategoricalArray. This allows one to analyze categorical data efficiently.See the CategoricalArrays package for more information regarding categorical arrays."
},

{
    "location": "man/missing.html#",
    "page": "Missing Data",
    "title": "Missing Data",
    "category": "page",
    "text": ""
},

{
    "location": "man/missing.html#Missing-Data-1",
    "page": "Missing Data",
    "title": "Missing Data",
    "category": "section",
    "text": "In Julia, missing values in data are represented using the special object missing, which is the single instance of the type Missing.julia> missing\r\nmissing\r\n\r\njulia> typeof(missing)\r\nMissing\r\nThe Missing type lets users create Vectors and DataFrame columns with missing values. Here we create a vector with a missing value and the element-type of the returned vector is Union{Missing, Int64}.julia> x = [1, 2, missing]\r\n3-element Array{Union{Missing, Int64},1}:\r\n 1\r\n 2\r\n  missing\r\n\r\njulia> eltype(x)\r\nUnion{Missing, Int64}\r\n\r\njulia> Union{Missing, Int}\r\nUnion{Missing, Int64}\r\n\r\njulia> eltype(x) == Union{Missing, Int}\r\ntrue\r\nmissing values can be excluded when performing operations by using skipmissing, which returns a memory-efficient iterator.julia> skipmissing(x)\r\nBase.SkipMissing{Array{Union{Missing, Int64},1}}(Union{Missing, Int64}[1, 2, missing])\r\nThe output of skipmissing can be passed directly into functions as an argument. For example, we can find the sum of all non-missing values or collect the non-missing values into a new missing-free vector.julia> sum(skipmissing(x))\r\n3\r\n\r\njulia> collect(skipmissing(x))\r\n2-element Array{Int64,1}:\r\n 1\r\n 2\r\nThe function coalesce can be used to replace missing values with another value (note the dot, indicating that the replacement should be applied to all entries in x):julia> coalesce.(x, 0)\r\n3-element Array{Int64,1}:\r\n 1\r\n 2\r\n 0\r\nThe Missings.jl package provides a few convenience functions to work with missing values.The function Missings.replace returns an iterator which replaces missing elements with another value:julia> using Missings\r\n\r\njulia> Missings.replace(x, 1)\r\nMissings.EachReplaceMissing{Array{Union{Missing, Int64},1},Int64}(Union{Missing, Int64}[1, 2, missing], 1)\r\n\r\njulia> collect(Missings.replace(x, 1))\r\n3-element Array{Int64,1}:\r\n 1\r\n 2\r\n 1\r\n\r\njulia> collect(Missings.replace(x, 1)) == coalesce.(x, 1)\r\ntrue\r\nThe function Missings.T returns the element-type T in Union{T, Missing}.julia> eltype(x)\r\nUnion{Int64, Missing}\r\n\r\njulia> Missings.T(eltype(x))\r\nInt64\r\nThe missings function constructs Vectors and Arrays supporting missing values, using the optional first argument to specify the element-type.julia> missings(1)\r\n1-element Array{Missing,1}:\r\n missing\r\n\r\njulia> missings(3)\r\n3-element Array{Missing,1}:\r\n missing\r\n missing\r\n missing\r\n\r\njulia> missings(1, 3)\r\n1×3 Array{Missing,2}:\r\n missing  missing  missing\r\n\r\njulia> missings(Int, 1, 3)\r\n1×3 Array{Union{Missing, Int64},2}:\r\n missing  missing  missing\r\nSee the Julia manual for more information about missing values."
},

{
    "location": "man/querying_frameworks.html#",
    "page": "Querying frameworks",
    "title": "Querying frameworks",
    "category": "page",
    "text": ""
},

{
    "location": "man/querying_frameworks.html#Data-manipulation-frameworks-1",
    "page": "Querying frameworks",
    "title": "Data manipulation frameworks",
    "category": "section",
    "text": "Two popular frameworks provide convenience methods to manipulate DataFrames: DataFramesMeta.jl and Query.jl. They implement a functionality similar to dplyr or LINQ."
},

{
    "location": "man/querying_frameworks.html#DataFramesMeta.jl-1",
    "page": "Querying frameworks",
    "title": "DataFramesMeta.jl",
    "category": "section",
    "text": "The DataFramesMeta.jl package provides a convenient yet fast macro-based interface to work with DataFrames.First install the DataFramesMeta.jl package:using Pkg\nPkg.add(\"DataFramesMeta\")The major benefit of the package is that it allows you to refer to columns of a DataFrame as Symbols. Therefore instead of writing verylongdataframename.variable you can simply write :variable in expressions. Additionally you can chain a sequence of transformations of a DataFrame using the @linq macro.Here is a minimal example of usage of the package. Observe that we refer to names of columns using only their names and that chaining is performed using the @link macro and the |> operator:julia> using DataFrames, DataFramesMeta\n\njulia> df = DataFrame(name=[\"John\", \"Sally\", \"Roger\"],\n                      age=[54., 34., 79.],\n                      children=[0, 2, 4])\n3×3 DataFrame\n│ Row │ name   │ age     │ children │\n│     │ String │ Float64 │ Int64    │\n├─────┼────────┼─────────┼──────────┤\n│ 1   │ John   │ 54.0    │ 0        │\n│ 2   │ Sally  │ 34.0    │ 2        │\n│ 3   │ Roger  │ 79.0    │ 4        │\n\njulia> @linq df |>\n           where(:age .> 40) |>\n           select(number_of_children=:children, :name)\n2×2 DataFrame\n│ Row │ number_of_children │ name   │\n│     │ Int64              │ String │\n├─────┼────────────────────┼────────┤\n│ 1   │ 0                  │ John   │\n│ 2   │ 4                  │ Roger  │In the following examples we show that DataFramesMeta.jl also supports the split-apply-combine pattern:julia> df = DataFrame(key=repeat(1:3, 4), value=1:12)\n12×2 DataFrame\n│ Row │ key   │ value │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 1     │\n│ 2   │ 2     │ 2     │\n│ 3   │ 3     │ 3     │\n│ 4   │ 1     │ 4     │\n│ 5   │ 2     │ 5     │\n│ 6   │ 3     │ 6     │\n│ 7   │ 1     │ 7     │\n│ 8   │ 2     │ 8     │\n│ 9   │ 3     │ 9     │\n│ 10  │ 1     │ 10    │\n│ 11  │ 2     │ 11    │\n│ 12  │ 3     │ 12    │\n\njulia> @linq df |>\n           where(:value .> 3) |>\n           by(:key, min=minimum(:value), max=maximum(:value)) |>\n           select(:key, range=:max - :min)\n3×2 DataFrame\n│ Row │ key   │ range │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 6     │\n│ 2   │ 2     │ 6     │\n│ 3   │ 3     │ 6     │\n\njulia> @linq df |>\n           groupby(:key) |>\n           transform(value0 = :value .- minimum(:value))\n12×3 DataFrame\n│ Row │ key   │ value │ value0 │\n│     │ Int64 │ Int64 │ Int64  │\n├─────┼───────┼───────┼────────┤\n│ 1   │ 1     │ 1     │ 0      │\n│ 2   │ 1     │ 4     │ 3      │\n│ 3   │ 1     │ 7     │ 6      │\n│ 4   │ 1     │ 10    │ 9      │\n│ 5   │ 2     │ 2     │ 0      │\n│ 6   │ 2     │ 5     │ 3      │\n│ 7   │ 2     │ 8     │ 6      │\n│ 8   │ 2     │ 11    │ 9      │\n│ 9   │ 3     │ 3     │ 0      │\n│ 10  │ 3     │ 6     │ 3      │\n│ 11  │ 3     │ 9     │ 6      │\n│ 12  │ 3     │ 12    │ 9      │You can find more details about how this package can be used on the DataFramesMeta.jl GitHub page."
},

{
    "location": "man/querying_frameworks.html#Query.jl-1",
    "page": "Querying frameworks",
    "title": "Query.jl",
    "category": "section",
    "text": "The Query.jl package provides advanced data manipulation capabilities for DataFrames (and many other data structures). This section provides a short introduction to the package, the Query.jl documentation has a more comprehensive documentation of the package.To get started, install the Query.jl package:using Pkg\nPkg.add(\"Query\")A query is started with the @from macro and consists of a series of query commands. Query.jl provides commands that can filter, project, join, flatten and group data from a DataFrame. A query can return an iterator, or one can materialize the results of a query into a variety of data structures, including a new DataFrame.A simple example of a query looks like this:julia> using DataFrames, Query\n\njulia> df = DataFrame(name=[\"John\", \"Sally\", \"Roger\"],\n                      age=[54., 34., 79.],\n                      children=[0, 2, 4])\n3×3 DataFrame\n│ Row │ name   │ age     │ children │\n│     │ String │ Float64 │ Int64    │\n├─────┼────────┼─────────┼──────────┤\n│ 1   │ John   │ 54.0    │ 0        │\n│ 2   │ Sally  │ 34.0    │ 2        │\n│ 3   │ Roger  │ 79.0    │ 4        │\n\njulia> q1 = @from i in df begin\n            @where i.age > 40\n            @select {number_of_children=i.children, i.name}\n            @collect DataFrame\n       end\n2×2 DataFrame\n│ Row │ number_of_children │ name   │\n│     │ Int64              │ String │\n├─────┼────────────────────┼────────┤\n│ 1   │ 0                  │ John   │\n│ 2   │ 4                  │ Roger  │The query starts with the @from macro. The first argument i is the name of the range variable that will be used to refer to an individual row in later query commands. The next argument df is the data source that one wants to query. The @where command in this query will filter the source data by applying the filter condition i.age > 40. This filters out any rows in which the age column is not larger than 40. The @select command then projects the columns of the source data onto a new column structure. The example here applies three specific modifications: 1) it only keeps a subset of the columns in the source DataFrame, i.e. the age column will not be part of the transformed data; 2) it changes the order of the two columns that are selected; and 3) it renames one of the columns that is selected from children to number_of_children. The example query uses the {} syntax to achieve this. A {} in a Query.jl expression instantiates a new NamedTuple, i.e. it is a shortcut for writing @NT(number_of_children=>i.children, name=>i.name). The @collect statement determines the data structure that the query returns. In this example the results are returned as a DataFrame.A query without a @collect statement returns a standard julia iterator that can be used with any normal julia language construct that can deal with iterators. The following code returns a julia iterator for the query results:julia> q2 = @from i in df begin\n                   @where i.age > 40\n                   @select {number_of_children=i.children, i.name}\n              end; # suppress printing the iterator type\nOne can loop over the results using a standard julia for statement:julia> total_children = 0\n0\n\njulia> for i in q2\n           global total_children += i.number_of_children\n       end\n\njulia> total_children\n4\nOr one can use a comprehension to extract the name of a subset of rows:julia> y = [i.name for i in q2 if i.number_of_children > 0]\n1-element Array{String,1}:\n \"Roger\"\nThe last example (extracting only the name and applying a second filter) could of course be completely expressed as a query expression:julia> q3 = @from i in df begin\n            @where i.age > 40 && i.children > 0\n            @select i.name\n            @collect\n       end\n1-element Array{String,1}:\n \"Roger\"\nA query that ends with a @collect statement without a specific type will materialize the query results into an array. Note also the difference in the @select statement: The previous queries all used the {} syntax in the @select statement to project results into a tabular format. The last query instead just selects a single value from each row in the @select statement.These examples only scratch the surface of what one can do with Query.jl, and the interested reader is referred to the Query.jl documentation for more information."
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
    "text": "Pages = [\"types.md\"]"
},

{
    "location": "lib/types.html#Type-hierarchy-design-1",
    "page": "Types",
    "title": "Type hierarchy design",
    "category": "section",
    "text": "AbstractDataFrame is an abstract type that provides an interface for data frame types. It is not intended as a fully generic interface for working with tabular data, which is the role of interfaces defined by Tables.jl instead.DataFrame is the most fundamental subtype of AbstractDataFrame, which stores a set of columns as AbstractVector objects.SubDataFrame is an AbstractDataFrame subtype representing a view into a DataFrame. It stores only a reference to the parent DataFrame and information about which rows from the parent are selected. Typically it is created using the view function or is returned by indexing into a GroupedDataFrame object.GroupedDataFrame is a type that stores the result of a  grouping operation performed on an AbstractDataFrame. It is intended to be created as a result of a call to the groupby function.DataFrameRow is a view into a single row of an AbstractDataFrame. It stores only a reference to a parent AbstractDataFrame and information about which row from the parent is selected. The DataFrameRow type supports iteration over columns of the row and is similar in functionality to the NamedTuple type, but allows for modification of data stored in the parent AbstractDataFrame and reflects changes done to the parent after the creation of the view. Typically objects of the DataFrameRow type are encountered when returned by the eachrow function. In the future accessing a single row of a data frame via getindex or view will return a DataFrameRow.Additionally, the eachrow function returns a value of the DataFrameRows type, which serves as an iterator over rows of an AbstractDataFrame, returning DataFrameRow objects.Similarly, the eachcol and columns functions return a value of the DataFrameColumns type, which serves as an iterator over columns of an AbstractDataFrame. The difference between the return value of eachcol and columns is the following:The eachcol function returns a value of the DataFrameColumns{<:AbstractDataFrame, true} type, which is an iterator returning a pair containing the column name and the column vector.\nThe columns function returns a value of the DataFrameColumns{<:AbstractDataFrame, false} type, which is an iterator returning the column vector only.The DataFrameRows and DataFrameColumns types are subtypes of AbstractVector and support its interface with the exception that they are read only. Note that they are not exported and should not be constructed directly, but using the eachrow, eachcol and columns functions."
},

{
    "location": "lib/types.html#DataFrames.AbstractDataFrame",
    "page": "Types",
    "title": "DataFrames.AbstractDataFrame",
    "category": "type",
    "text": "AbstractDataFrame\n\nAn abstract type for which all concrete types expose an interface for working with tabular data.\n\nCommon methods\n\nAn AbstractDataFrame is a two-dimensional table with Symbols for column names. An AbstractDataFrame is also similar to an Associative type in that it allows indexing by a key (the columns).\n\nThe following are normally implemented for AbstractDataFrames:\n\ndescribe : summarize columns\ndump : show structure\nhcat : horizontal concatenation\nvcat : vertical concatenation\nrepeat : repeat rows\nnames : columns names\nnames! : set columns names\nrename! : rename columns names based on keyword arguments\neltypes : eltype of each column\nlength : number of columns\nsize : (nrows, ncols)\nfirst : first n rows\nlast : last n rows\nconvert : convert to an array\ncompletecases : boolean vector of complete cases (rows with no missings)\ndropmissing : remove rows with missing values\ndropmissing! : remove rows with missing values in-place\nnonunique : indexes of duplicate rows\nunique! : remove duplicate rows\nsimilar : a DataFrame with similar columns as d\n\nIndexing\n\nTable columns are accessed (getindex) by a single index that can be a symbol identifier, an integer, or a vector of each. If a single column is selected, just the column object is returned. If multiple columns are selected, some AbstractDataFrame is returned.\n\nd[:colA]\nd[3]\nd[[:colA, :colB]]\nd[[1:3; 5]]\n\nRows and columns can be indexed like a Matrix with the added feature of indexing columns by name.\n\nd[1:3, :colA]\nd[3,3]\nd[3,:]\nd[3,[:colA, :colB]]\nd[:, [:colA, :colB]]\nd[[1:3; 5], :]\n\nsetindex works similarly.\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.DataFrame",
    "page": "Types",
    "title": "DataFrames.DataFrame",
    "category": "type",
    "text": "DataFrame <: AbstractDataFrame\n\nAn AbstractDataFrame that stores a set of named columns\n\nThe columns are normally AbstractVectors stored in memory, particularly a Vector or CategoricalVector.\n\nConstructors\n\nDataFrame(columns::Vector, names::Vector{Symbol}; makeunique::Bool=false)\nDataFrame(columns::Matrix, names::Vector{Symbol}; makeunique::Bool=false)\nDataFrame(kwargs...)\nDataFrame(pairs::Pair{Symbol}...; makeunique::Bool=false)\nDataFrame() # an empty DataFrame\nDataFrame(t::Type, nrows::Integer, ncols::Integer) # an empty DataFrame of arbitrary size\nDataFrame(column_eltypes::Vector, names::Vector, nrows::Integer; makeunique::Bool=false)\nDataFrame(column_eltypes::Vector, cnames::Vector, categorical::Vector, nrows::Integer;\n          makeunique::Bool=false)\nDataFrame(ds::AbstractDict)\nDataFrame(table; makeunique::Bool=false)\n\nArguments\n\ncolumns : a Vector with each column as contents or a Matrix\nnames : the column names\nmakeunique : if false (the default), an error will be raised if duplicates in names are found; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\nkwargs : the key gives the column names, and the value is the column contents\nt : elemental type of all columns\nnrows, ncols : number of rows and columns\ncolumn_eltypes : elemental type of each column\ncategorical : Vector{Bool} indicating which columns should be converted to                 CategoricalVector\nds : AbstractDict of columns\ntable: any type that implements the Tables.jl interface\n\nEach column in columns should be the same length.\n\nNotes\n\nA DataFrame is a lightweight object. As long as columns are not manipulated, creation of a DataFrame from existing AbstractVectors is inexpensive. For example, indexing on columns is inexpensive, but indexing by rows is expensive because copies are made of each column.\n\nIf a column is passed to a DataFrame constructor or is assigned as a whole using setindex! then its reference is stored in the DataFrame. An exception to this rule is assignment of an AbstractRange as a column, in which case the range is collected to a Vector.\n\nBecause column types can vary, a DataFrame is not type stable. For performance-critical code, do not index into a DataFrame inside of loops.\n\nExamples\n\ndf = DataFrame()\nv = [\"x\",\"y\",\"z\"][rand(1:3, 10)]\ndf1 = DataFrame(Any[collect(1:10), v, rand(10)], [:A, :B, :C])\ndf2 = DataFrame(A = 1:10, B = v, C = rand(10))\ndump(df1)\ndump(df2)\ndescribe(df2)\nfirst(df1, 10)\ndf1[:A] + df2[:C]\ndf1[1:4, 1:2]\ndf1[[:A,:C]]\ndf1[1:2, [:A,:C]]\ndf1[:, [:A,:C]]\ndf1[:, [1,3]]\ndf1[1:4, :]\ndf1[1:4, :C]\ndf1[1:4, :C] = 40. * df1[1:4, :C]\n[df1; df2]  # vcat\n[df1  df2]  # hcat\nsize(df1)\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.DataFrameRow",
    "page": "Types",
    "title": "DataFrames.DataFrameRow",
    "category": "type",
    "text": "DataFrameRow{<:AbstractDataFrame}\n\nA view of one row of an AbstractDataFrame.\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.GroupedDataFrame",
    "page": "Types",
    "title": "DataFrames.GroupedDataFrame",
    "category": "type",
    "text": "GroupedDataFrame\n\nThe result of a groupby operation on an AbstractDataFrame; a view into the AbstractDataFrame grouped by rows.\n\nNot meant to be constructed directly, see groupby.\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.SubDataFrame",
    "page": "Types",
    "title": "DataFrames.SubDataFrame",
    "category": "type",
    "text": "SubDataFrame{<:AbstractVector{Int}} <: AbstractDataFrame\n\nA view of row subsets of an AbstractDataFrame\n\nA SubDataFrame is meant to be constructed with view.  A SubDataFrame is used frequently in split/apply sorts of operations.\n\nview(d::AbstractDataFrame, rows)\n\nArguments\n\nd : an AbstractDataFrame\nrows : any indexing type for rows, typically an Int, AbstractVector{Int}, AbstractVector{Bool}, or a Range\n\nNotes\n\nA SubDataFrame is an AbstractDataFrame, so expect that most DataFrame functions should work. Such methods include describe, dump, nrow, size, by, stack, and join. Indexing is just like a DataFrame; copies are returned.\n\nTo subset along columns, use standard column indexing as that creates a view to the columns by default. To subset along rows and columns, use column-based indexing with view.\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\nsdf1 = view(df, 1:6)\nsdf2 = view(df, df[:a] .> 1)\nsdf3 = view(df[[1,3]], df[:a] .> 1)  # row and column subsetting\nsdf4 = groupby(df, :a)[1]  # indexing a GroupedDataFrame returns a SubDataFrame\nsdf5 = view(sdf1, 1:3)\nsdf1[:,[:a,:b]]\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.DataFrameRows",
    "page": "Types",
    "title": "DataFrames.DataFrameRows",
    "category": "type",
    "text": "DataFrameRows{T<:AbstractDataFrame} <: AbstractVector{DataFrameRow{T}}\n\nIterator over rows of an AbstractDataFrame, with each row represented as a DataFrameRow.\n\nA value of this type is returned by the eachrow function.\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#DataFrames.DataFrameColumns",
    "page": "Types",
    "title": "DataFrames.DataFrameColumns",
    "category": "type",
    "text": "DataFrameColumns{<:AbstractDataFrame, V} <: AbstractVector{V}\n\nIterator over columns of an AbstractDataFrame. If V is Pair{Symbol,AbstractVector} (which is the case when calling eachcol) then each returned value is a pair consisting of column name and column vector. If V is AbstractVector (a value returned by the columns function) then each returned value is a column vector.\n\n\n\n\n\n"
},

{
    "location": "lib/types.html#Types-specification-1",
    "page": "Types",
    "title": "Types specification",
    "category": "section",
    "text": "AbstractDataFrame\nDataFrame\nDataFrameRow\nGroupedDataFrame\nSubDataFrame\nDataFrameRows\nDataFrameColumns"
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
    "text": "Split-apply-combine that applies a set of functions over columns of an AbstractDataFrame or GroupedDataFrame\n\naggregate(d::AbstractDataFrame, cols, fs)\naggregate(gd::GroupedDataFrame, fs)\n\nArguments\n\nd : an AbstractDataFrame\ngd : a GroupedDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.)\nfs : a function or vector of functions to be applied to vectors within groups; expects each argument to be a column vector\n\nEach fs should return a value or vector. All returns must be the same length.\n\nReturns\n\n::DataFrame\n\nExamples\n\nusing Statistics\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\naggregate(df, :a, sum)\naggregate(df, :a, [sum, x->mean(skipmissing(x))])\naggregate(groupby(df, :a), [sum, x->mean(skipmissing(x))])\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.by",
    "page": "Functions",
    "title": "DataFrames.by",
    "category": "function",
    "text": "Split-apply-combine in one step; apply f to each grouping in d based on columns col\n\nby(d::AbstractDataFrame, cols, f::Function; sort::Bool = false)\nby(f::Function, d::AbstractDataFrame, cols; sort::Bool = false)\n\nArguments\n\nd : an AbstractDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.)\nf : a function to be applied to groups; expects each argument to be an AbstractDataFrame\nsort: sort row groups (no sorting by default)\n\nDetails\n\nFor each group in gd, f is passed a SubDataFrame view with the corresponding rows. f can return a single value, a named tuple, a vector, or a data frame. This determines the shape of the resulting data frame:\n\nA single value gives a data frame with a single column and one row per group.\nA named tuple or a DataFrameRow gives a data frame with one column for each field\n\nand one row per group.\n\nA vector gives a data frame with a single column and as many rows for each group as the length of the returned vector for that group.\nA data frame gives a data frame with the same columns and as many rows for each group as the rows returned for that group.\n\nIn all cases, the resulting data frame contains all the grouping columns in addition to those listed above. Note that f must always return the same type of object for all groups, and (if a named tuple or data frame) with the same fields or columns. Returning a single value or a named tuple is significantly faster than returning a vector or a data frame.\n\nA method is defined with f as the first argument, so do-block notation can be used.\n\nby(d, cols, f) is equivalent to combine(f, groupby(d, cols)).\n\nReturns\n\n::DataFrame\n\nExamples\n\nusing Statistics\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\nby(df, :a, d -> sum(d[:c]))\nby(df, :a, d -> 2 * skipmissing(d[:c]))\nby(df, :a, d -> (c_sum = sum(d[:c]), c_mean = mean(skipmissing(d[:c]))))\nby(df, :a, d -> DataFrame(c = d[:c], c_mean = mean(skipmissing(d[:c]))))\nby(df, [:a, :b]) do d\n    (m = mean(skipmissing(d[:c])), v = var(skipmissing(d[:c])))\nend\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.colwise",
    "page": "Functions",
    "title": "DataFrames.colwise",
    "category": "function",
    "text": "Apply a function to each column in an AbstractDataFrame or GroupedDataFrame\n\ncolwise(f::Function, d)\ncolwise(d)\n\nArguments\n\nf : a function or vector of functions\nd : an AbstractDataFrame of GroupedDataFrame\n\nIf d is not provided, a curried version of groupby is given.\n\nReturns\n\nvarious, depending on the call\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ncolwise(sum, df)\ncolwise([sum, length], df)\ncolwise((minimum, maximum), df)\ncolwise(sum, groupby(df, :a))\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.combine",
    "page": "Functions",
    "title": "DataFrames.combine",
    "category": "function",
    "text": "combine(gd::GroupedDataFrame)\ncombine(f::Function, gd::GroupedDataFrame)\n\nTransform a GroupedDataFrame into a DataFrame. If a function f is provided, it is called for each group in gd with a SubDataFrame view holding the corresponding rows, and the returned DataFrame then consists of the returned rows plus the grouping columns.\n\nf can return a single value, a row or multiple rows. The type of the returned value determines the shape of the resulting data frame:\n\nA single value gives a data frame with a single column and one row per group.\nA named tuple or a DataFrameRow gives a data frame with one column for each field and one row per group.\nA vector gives a data frame with a single column and as many rows for each group as the length of the returned vector for that group.\nA data frame or a matrix gives a data frame with the same columns and as many rows for each group as the rows returned for that group.\n\nIn all cases, the resulting data frame contains all the grouping columns in addition to those listed above. Note that f must always return the same type of object for all groups, and (if a named tuple or data frame) with the same fields or columns. Returning a single value or a named tuple is significantly faster than returning a vector or a data frame.\n\nThe resulting data frame will be sorted if sort=true was passed to the groupby call from which gd was constructed. Otherwise, ordering of rows is undefined.\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ngd = groupby(df, :a)\ncombine(d -> sum(skipmissing(d[:c])), gd)\n\nSee also\n\nby(f, df, cols) is a shorthand for combine(f, groupby(df, cols)).\n\nmap: combine(f, groupby(df, cols)) is a more efficient equivalent of combine(map(f, groupby(df, cols))).\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.groupby",
    "page": "Functions",
    "title": "DataFrames.groupby",
    "category": "function",
    "text": "A view of an AbstractDataFrame split into row groups\n\ngroupby(d::AbstractDataFrame, cols; sort = false, skipmissing = false)\ngroupby(cols; sort = false, skipmissing = false)\n\nArguments\n\nd : an AbstractDataFrame to split (optional, see Returns)\ncols : data table columns to group by\nsort: whether to sort rows according to the values of the grouping columns cols\nskipmissing: whether to skip rows with missing values in one of the grouping columns cols\n\nReturns\n\n::GroupedDataFrame : a grouped view into d\n::Function: a function x -> groupby(x, cols) (if d is not specified)\n\nDetails\n\nAn iterator over a GroupedDataFrame returns a SubDataFrame view for each grouping into d. A GroupedDataFrame also supports indexing by groups, map (which applies a function to each group) and combine (which applies a function to each group and combines the result into a data frame).\n\nSee the following for additional split-apply-combine operations:\n\nby : split-apply-combine using functions\naggregate : split-apply-combine; applies functions in the form of a cross product\ncolwise : apply a function to each column in an AbstractDataFrame or GroupedDataFrame\nmap : apply a function to each group of a GroupedDataFrame (without combining)\ncombine : combine a GroupedDataFrame, optionally applying a function to each group\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ngd = groupby(df, :a)\ngd[1]\nlast(gd)\nvcat([g[:b] for g in gd]...)\nfor g in gd\n    println(g)\nend\nmap(d -> sum(skipmissing(d[:c])), gd)\ncombine(d -> sum(skipmissing(d[:c])), gd)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.join",
    "page": "Functions",
    "title": "Base.join",
    "category": "function",
    "text": "join(df1, df2; on = Symbol[], kind = :inner, makeunique = false,\n     indicator = nothing, validate = (false, false))\n\nJoin two DataFrame objects\n\nArguments\n\ndf1, df2 : the two AbstractDataFrames to be joined\n\nKeyword Arguments\n\non : A column, or vector of columns to join df1 and df2 on. If the column(s)   that df1 and df2 will be joined on have different names, then the columns   should be (left, right) tuples or left => right pairs, or a vector of   such tuples or pairs. on is a required argument for all joins except for   kind = :cross\nkind : the type of join, options include:\n:inner : only include rows with keys that match in both df1 and df2, the default\n:outer : include all rows from df1 and df2\n:left : include all rows from df1\n:right : include all rows from df2\n:semi : return rows of df1 that match with the keys in df2\n:anti : return rows of df1 that do not match with the keys in df2\n:cross : a full Cartesian product of the key combinations; every row of df1 is matched with every row of df2\n\nmakeunique : if false (the default), an error will be raised if duplicate names are found in columns not joined on; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\nindicator : Default: nothing. If a Symbol, adds categorical indicator  column named Symbol for whether a row appeared in only df1 (\"left_only\"),  only df2 (\"right_only\") or in both (\"both\"). If Symbol is already in use,  the column name will be modified if makeunique=true.\nvalidate : whether to check that columns passed as the on argument  define unique keys in each input data frame (according to isequal).  Can be a tuple or a pair, with the first element indicating whether to  run check for df1 and the second element for df2.  By default no check is performed.\n\nFor the three join operations that may introduce missing values (:outer, :left, and :right), all columns of the returned data table will support missing values.\n\nWhen merging on categorical columns that differ in the ordering of their levels, the ordering of the left DataFrame takes precedence over the ordering of the right DataFrame\n\nResult\n\n::DataFrame : the joined DataFrame\n\nExamples\n\nname = DataFrame(ID = [1, 2, 3], Name = [\"John Doe\", \"Jane Doe\", \"Joe Blogs\"])\njob = DataFrame(ID = [1, 2, 4], Job = [\"Lawyer\", \"Doctor\", \"Farmer\"])\n\njoin(name, job, on = :ID)\njoin(name, job, on = :ID, kind = :outer)\njoin(name, job, on = :ID, kind = :left)\njoin(name, job, on = :ID, kind = :right)\njoin(name, job, on = :ID, kind = :semi)\njoin(name, job, on = :ID, kind = :anti)\njoin(name, job, kind = :cross)\n\njob2 = DataFrame(identifier = [1, 2, 4], Job = [\"Lawyer\", \"Doctor\", \"Farmer\"])\njoin(name, job2, on = (:ID, :identifier))\njoin(name, job2, on = :ID => :identifier)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.map",
    "page": "Functions",
    "title": "Base.map",
    "category": "function",
    "text": "map(f::Union{Function, Type}, gd::GroupedDataFrame)\n\nApply a function to each group of rows and return a GroupedDataFrame.\n\nFor each group in gd, f is passed a SubDataFrame view holding the corresponding rows. f can return a single value, a row or multiple rows. The type of the returned value determines the shape of the resulting data frame:\n\nA single value gives a data frame with a single column and one row per group.\nA named tuple or a DataFrameRow gives a data frame with one column for each field and one row per group.\nA vector gives a data frame with a single column and as many rows for each group as the length of the returned vector for that group.\nA data frame or a matrix gives a data frame with the same columns and as many rows for each group as the rows returned for that group.\n\nIn all cases, the resulting GroupedDataFrame contains all the grouping columns in addition to those listed above. Note that f must always return the same type of object for all groups, and (if a named tuple or data frame) with the same fields or columns. Returning a single value or a named tuple is significantly faster than returning a vector or a data frame.\n\nExamples\n\ndf = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),\n               b = repeat([2, 1], outer=[4]),\n               c = randn(8))\ngd = groupby(df, :a)\nmap(d -> sum(skipmissing(d[:c])), gd)\n\nSee also\n\ncombine(f, gd) returns a DataFrame rather than a GroupedDataFrame\n\n\n\n\n\n"
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
    "text": "aggregate\nby\ncolwise\ncombine\ngroupby\njoin\nmap\nmelt\nstack\nunstack\nstackdf\nmeltdf"
},

{
    "location": "lib/functions.html#DataFrames.allowmissing!",
    "page": "Functions",
    "title": "DataFrames.allowmissing!",
    "category": "function",
    "text": "allowmissing!(df::DataFrame)\n\nConvert all columns of a df from element type T to Union{T, Missing} to support missing values.\n\nallowmissing!(df::DataFrame, col::Union{Integer, Symbol})\n\nConvert a single column of a df from element type T to Union{T, Missing} to support missing values.\n\nallowmissing!(df::DataFrame, cols::AbstractVector{<:Union{Integer, Symbol}})\n\nConvert multiple columns of a df from element type T to Union{T, Missing} to support missing values.\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.columns",
    "page": "Functions",
    "title": "DataFrames.columns",
    "category": "function",
    "text": "columns(df::AbstractDataFrame)\n\nReturn a DataFrameColumns that iterates an AbstractDataFrame column by column, yielding column vectors.\n\nExamples\n\njulia> df = DataFrame(x=1:4, y=11:14)\n4×2 DataFrame\n│ Row │ x     │ y     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 11    │\n│ 2   │ 2     │ 12    │\n│ 3   │ 3     │ 13    │\n│ 4   │ 4     │ 14    │\n\njulia> collect(columns(df))\n2-element Array{AbstractArray{T,1} where T,1}:\n [1, 2, 3, 4]\n [11, 12, 13, 14]\n\njulia> sum.(columns(df))\n2-element Array{Int64,1}:\n 10\n 50\n\njulia> map(columns(df)) do col\n           maximum(col) - minimum(col)\n       end\n2-element Array{Int64,1}:\n 3\n 3\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.completecases",
    "page": "Functions",
    "title": "DataFrames.completecases",
    "category": "function",
    "text": "completecases(df::AbstractDataFrame)\ncompletecases(df::AbstractDataFrame, cols::AbstractVector)\ncompletecases(df::AbstractDataFrame, cols::Union{Integer, Symbol})\n\nReturn a Boolean vector with true entries indicating rows without missing values (complete cases) in data frame df. If cols is provided, only missing values in the corresponding columns are considered.\n\nSee also: dropmissing and dropmissing!. Use findall(completecases(df)) to get the indices of the rows.\n\nExamples\n\njulia> df = DataFrame(i = 1:5,\n                      x = [missing, 4, missing, 2, 1],\n                      y = [missing, missing, \"c\", \"d\", \"e\"])\n5×3 DataFrame\n│ Row │ i     │ x       │ y       │\n│     │ Int64 │ Int64⍰  │ String⍰ │\n├─────┼───────┼─────────┼─────────┤\n│ 1   │ 1     │ missing │ missing │\n│ 2   │ 2     │ 4       │ missing │\n│ 3   │ 3     │ missing │ c       │\n│ 4   │ 4     │ 2       │ d       │\n│ 5   │ 5     │ 1       │ e       │\n\njulia> completecases(df)\n5-element BitArray{1}:\n false\n false\n false\n  true\n  true\n\njulia> completecases(df, :x)\n5-element BitArray{1}:\n false\n  true\n false\n  true\n  true\n\njulia> completecases(df, [:x, :y])\n5-element BitArray{1}:\n false\n false\n false\n  true\n  true\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#StatsBase.describe",
    "page": "Functions",
    "title": "StatsBase.describe",
    "category": "function",
    "text": "Report descriptive statistics for a data frame\n\ndescribe(df::AbstractDataFrame; stats = [:mean, :min, :median, :max, :nmissing, :nunique, :eltype])\n\nArguments\n\ndf : the AbstractDataFrame\nstats::Union{Symbol,AbstractVector{Symbol}} : the summary statistics to report. If a vector, allowed fields are :mean, :std, :min, :q25, :median, :q75, :max, :eltype, :nunique, :first, :last, and :nmissing. If set to :all, all summary statistics are reported.\n\nResult\n\nA DataFrame where each row represents a variable and each column a summary statistic.\n\nDetails\n\nFor Real columns, compute the mean, standard deviation, minimum, first quantile, median, third quantile, and maximum. If a column does not derive from Real, describe will attempt to calculate all statistics, using nothing as a fall-back in the case of an error.\n\nWhen stats contains :nunique, describe will report the number of unique values in a column. If a column\'s base type derives from Real, :nunique will return nothings.\n\nMissing values are filtered in the calculation of all statistics, however the column :nmissing will report the number of missing values of that variable. If the column does not allow missing values, nothing is returned. Consequently, nmissing = 0 indicates that the column allows missing values, but does not currently contain any.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndescribe(df)\ndescribe(df, stats = :all)\ndescribe(df, stats = [:min, :max])\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.disallowmissing!",
    "page": "Functions",
    "title": "DataFrames.disallowmissing!",
    "category": "function",
    "text": "disallowmissing!(df::DataFrame)\n\nConvert all columns of a df from element type Union{T, Missing} to T to drop support for missing values.\n\ndisallowmissing!(df::DataFrame, col::Union{Integer, Symbol})\n\nConvert a single column of a df from element type Union{T, Missing} to T to drop support for missing values.\n\ndisallowmissing!(df::DataFrame, cols::AbstractVector{<:Union{Integer, Symbol}})\n\nConvert multiple columns of a df from element type Union{T, Missing} to T to drop support for missing values.\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.dropmissing",
    "page": "Functions",
    "title": "DataFrames.dropmissing",
    "category": "function",
    "text": "dropmissing(df::AbstractDataFrame)\ndropmissing(df::AbstractDataFrame, cols::AbstractVector)\ndropmissing(df::AbstractDataFrame, cols::Union{Integer, Symbol})\n\nReturn a copy of data frame df excluding rows with missing values. If cols is provided, only missing values in the corresponding columns are considered.\n\nSee also: completecases and dropmissing!.\n\nExamples\n\njulia> df = DataFrame(i = 1:5,\n                      x = [missing, 4, missing, 2, 1],\n                      y = [missing, missing, \"c\", \"d\", \"e\"])\n5×3 DataFrame\n│ Row │ i     │ x       │ y       │\n│     │ Int64 │ Int64⍰  │ String⍰ │\n├─────┼───────┼─────────┼─────────┤\n│ 1   │ 1     │ missing │ missing │\n│ 2   │ 2     │ 4       │ missing │\n│ 3   │ 3     │ missing │ c       │\n│ 4   │ 4     │ 2       │ d       │\n│ 5   │ 5     │ 1       │ e       │\n\njulia> dropmissing(df)\n2×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 4     │ 2      │ d       │\n│ 2   │ 5     │ 1      │ e       │\n\njulia> dropmissing(df, :x)\n3×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 2     │ 4      │ missing │\n│ 2   │ 4     │ 2      │ d       │\n│ 3   │ 5     │ 1      │ e       │\n\njulia> dropmissing(df, [:x, :y])\n2×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 4     │ 2      │ d       │\n│ 2   │ 5     │ 1      │ e       │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.dropmissing!",
    "page": "Functions",
    "title": "DataFrames.dropmissing!",
    "category": "function",
    "text": "dropmissing!(df::AbstractDataFrame)\ndropmissing!(df::AbstractDataFrame, cols::AbstractVector)\ndropmissing!(df::AbstractDataFrame, cols::Union{Integer, Symbol})\n\nRemove rows with missing values from data frame df and return it. If cols is provided, only missing values in the corresponding columns are considered.\n\nSee also: dropmissing and completecases.\n\nExamples\n\njulia> df = DataFrame(i = 1:5,\n                      x = [missing, 4, missing, 2, 1],\n                      y = [missing, missing, \"c\", \"d\", \"e\"])\n5×3 DataFrame\n│ Row │ i     │ x       │ y       │\n│     │ Int64 │ Int64⍰  │ String⍰ │\n├─────┼───────┼─────────┼─────────┤\n│ 1   │ 1     │ missing │ missing │\n│ 2   │ 2     │ 4       │ missing │\n│ 3   │ 3     │ missing │ c       │\n│ 4   │ 4     │ 2       │ d       │\n│ 5   │ 5     │ 1       │ e       │\n\njulia> df1 = copy(df);\n\njulia> dropmissing!(df1);\n\njulia> df1\n2×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 4     │ 2      │ d       │\n│ 2   │ 5     │ 1      │ e       │\n\njulia> df2 = copy(df);\n\njulia> dropmissing!(df2, :x);\n\njulia> df2\n3×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 2     │ 4      │ missing │\n│ 2   │ 4     │ 2      │ d       │\n│ 3   │ 5     │ 1      │ e       │\n\njulia> df3 = copy(df);\n\njulia> dropmissing!(df3, [:x, :y]);\n\n\njulia> df3\n2×3 DataFrame\n│ Row │ i     │ x      │ y       │\n│     │ Int64 │ Int64⍰ │ String⍰ │\n├─────┼───────┼────────┼─────────┤\n│ 1   │ 4     │ 2      │ d       │\n│ 2   │ 5     │ 1      │ e       │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.eachrow",
    "page": "Functions",
    "title": "DataFrames.eachrow",
    "category": "function",
    "text": "eachrow(df::AbstractDataFrame)\n\nReturn a DataFrameRows that iterates an AbstractDataFrame row by row, with each row represented as a DataFrameRow.\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.eachcol",
    "page": "Functions",
    "title": "DataFrames.eachcol",
    "category": "function",
    "text": "eachcol(df::AbstractDataFrame)\n\nReturn a DataFrameColumns that iterates an AbstractDataFrame column by column. Iteration returns a pair consisting of column name and column vector.\n\nExamples\n\njulia> df = DataFrame(x=1:4, y=11:14)\n4×2 DataFrame\n│ Row │ x     │ y     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 11    │\n│ 2   │ 2     │ 12    │\n│ 3   │ 3     │ 13    │\n│ 4   │ 4     │ 14    │\n\njulia> collect(eachcol(df))\n2-element Array{Pair{Symbol,AbstractArray{T,1} where T},1}:\n :x => [1, 2, 3, 4]\n :y => [11, 12, 13, 14]\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.eltypes",
    "page": "Functions",
    "title": "DataFrames.eltypes",
    "category": "function",
    "text": "Return element types of columns\n\neltypes(df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\n\nResult\n\n::Vector{Type} : the element type of each column\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\neltypes(df)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.filter",
    "page": "Functions",
    "title": "Base.filter",
    "category": "function",
    "text": "filter(function, df::AbstractDataFrame)\n\nReturn a copy of data frame df containing only rows for which function returns true. The function is passed a DataFrameRow as its only argument.\n\nExamples\n\njulia> df = DataFrame(x = [3, 1, 2, 1], y = [\"b\", \"c\", \"a\", \"b\"])\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 1     │ b      │\n\njulia> filter(row -> row[:x] > 1, df)\n2×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 2     │ a      │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.filter!",
    "page": "Functions",
    "title": "Base.filter!",
    "category": "function",
    "text": "filter!(function, df::AbstractDataFrame)\n\nRemove rows from data frame df for which function returns false. The function is passed a DataFrameRow as its only argument.\n\nExamples\n\njulia> df = DataFrame(x = [3, 1, 2, 1], y = [\"b\", \"c\", \"a\", \"b\"])\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 1     │ b      │\n\njulia> filter!(row -> row[:x] > 1, df);\n\njulia> df\n2×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 2     │ a      │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.insertcols!",
    "page": "Functions",
    "title": "DataFrames.insertcols!",
    "category": "function",
    "text": "Insert a column into a data frame in place.\n\ninsertcols!(df::DataFrame, ind::Int; name=col,\n            makeunique::Bool=false)\ninsertcols!(df::DataFrame, ind::Int, (:name => col)::Pair{Symbol,<:AbstractVector};\n            makeunique::Bool=false)\n\nArguments\n\ndf : the DataFrame to which we want to add a column\nind : a position at which we want to insert a column\nname : the name of the new column\ncol : an AbstractVector giving the contents of the new column\nmakeunique : Defines what to do if name already exists in df; if it is false an error will be thrown; if it is true a new unique name will be generated by adding a suffix\n\nResult\n\n::DataFrame : a DataFrame with added column.\n\nExamples\n\njulia> d = DataFrame(a=1:3)\n3×1 DataFrame\n│ Row │ a     │\n│     │ Int64 │\n├─────┼───────┤\n│ 1   │ 1     │\n│ 2   │ 2     │\n│ 3   │ 3     │\n\njulia> insertcols!(d, 1, b=[\'a\', \'b\', \'c\'])\n3×2 DataFrame\n│ Row │ b    │ a     │\n│     │ Char │ Int64 │\n├─────┼──────┼───────┤\n│ 1   │ \'a\'  │ 1     │\n│ 2   │ \'b\'  │ 2     │\n│ 3   │ \'c\'  │ 3     │\n\njulia> insertcols!(d, 1, :c => [2, 3, 4])\n3×3 DataFrame\n│ Row │ c     │ b    │ a     │\n│     │ Int64 │ Char │ Int64 │\n├─────┼───────┼──────┼───────┤\n│ 1   │ 2     │ \'a\'  │ 1     │\n│ 2   │ 3     │ \'b\'  │ 2     │\n│ 3   │ 4     │ \'c\'  │ 3     │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.mapcols",
    "page": "Functions",
    "title": "DataFrames.mapcols",
    "category": "function",
    "text": "mapcols(f::Union{Function,Type}, df::AbstractDataFrame)\n\nReturn a DataFrame where each column of df is transformed using function f. f must return AbstractVector objects all with the same length or scalars.\n\nExamples\n\njulia> df = DataFrame(x=1:4, y=11:14)\n4×2 DataFrame\n│ Row │ x     │ y     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 11    │\n│ 2   │ 2     │ 12    │\n│ 3   │ 3     │ 13    │\n│ 4   │ 4     │ 14    │\n\njulia> mapcols(x -> x.^2, df)\n4×2 DataFrame\n│ Row │ x     │ y     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 121   │\n│ 2   │ 4     │ 144   │\n│ 3   │ 9     │ 169   │\n│ 4   │ 16    │ 196   │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.names!",
    "page": "Functions",
    "title": "DataFrames.names!",
    "category": "function",
    "text": "Set column names\n\nnames!(df::AbstractDataFrame, vals)\n\nArguments\n\ndf : the AbstractDataFrame\nvals : column names, normally a Vector{Symbol} the same length as the number of columns in df\nmakeunique : if false (the default), an error will be raised if duplicate names are found; if true, duplicate names will be suffixed with _i (i starting at 1 for the first duplicate).\n\nResult\n\n::AbstractDataFrame : the updated result\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nnames!(df, [:a, :b, :c])\nnames!(df, [:a, :b, :a])  # throws ArgumentError\nnames!(df, [:a, :b, :a], makeunique=true)  # renames second :a to :a_1\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.nonunique",
    "page": "Functions",
    "title": "DataFrames.nonunique",
    "category": "function",
    "text": "Indexes of duplicate rows (a row that is a duplicate of a prior row)\n\nnonunique(df::AbstractDataFrame)\nnonunique(df::AbstractDataFrame, cols)\n\nArguments\n\ndf : the AbstractDataFrame\ncols : a column indicator (Symbol, Int, Vector{Symbol}, etc.) specifying the column(s) to compare\n\nResult\n\n::Vector{Bool} : indicates whether the row is a duplicate of some prior row\n\nSee also unique and unique!.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf = vcat(df, df)\nnonunique(df)\nnonunique(df, 1)\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.rename!",
    "page": "Functions",
    "title": "DataFrames.rename!",
    "category": "function",
    "text": "Rename columns\n\nrename!(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)\nrename!(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})\nrename!(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})\nrename!(f::Function, df::AbstractDataFrame)\nrename(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)\nrename(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})\nrename(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})\nrename(f::Function, df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\nd : an Associative type or an AbstractArray of pairs that maps the original names to new names\nf : a function which for each column takes the old name (a Symbol) and returns the new name (a Symbol)\n\nResult\n\n::AbstractDataFrame : the updated result\n\nNew names are processed sequentially. A new name must not already exist in the DataFrame at the moment an attempt to rename a column is performed.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nrename(df, :i => :A, :x => :X)\nrename(df, [:i => :A, :x => :X])\nrename(df, Dict(:i => :A, :x => :X))\nrename(x -> Symbol(uppercase(string(x))), df)\nrename(df) do x\n    Symbol(uppercase(string(x)))\nend\nrename!(df, Dict(:i =>: A, :x => :X))\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.rename",
    "page": "Functions",
    "title": "DataFrames.rename",
    "category": "function",
    "text": "Rename columns\n\nrename!(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)\nrename!(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})\nrename!(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})\nrename!(f::Function, df::AbstractDataFrame)\nrename(df::AbstractDataFrame, (from => to)::Pair{Symbol, Symbol}...)\nrename(df::AbstractDataFrame, d::AbstractDict{Symbol,Symbol})\nrename(df::AbstractDataFrame, d::AbstractArray{Pair{Symbol,Symbol}})\nrename(f::Function, df::AbstractDataFrame)\n\nArguments\n\ndf : the AbstractDataFrame\nd : an Associative type or an AbstractArray of pairs that maps the original names to new names\nf : a function which for each column takes the old name (a Symbol) and returns the new name (a Symbol)\n\nResult\n\n::AbstractDataFrame : the updated result\n\nNew names are processed sequentially. A new name must not already exist in the DataFrame at the moment an attempt to rename a column is performed.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\nrename(df, :i => :A, :x => :X)\nrename(df, [:i => :A, :x => :X])\nrename(df, Dict(:i => :A, :x => :X))\nrename(x -> Symbol(uppercase(string(x))), df)\nrename(df) do x\n    Symbol(uppercase(string(x)))\nend\nrename!(df, Dict(:i =>: A, :x => :X))\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.repeat",
    "page": "Functions",
    "title": "Base.repeat",
    "category": "function",
    "text": "repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)\n\nConstruct a data frame by repeating rows in df. inner specifies how many times each row is repeated, and outer specifies how many times the full set of rows is repeated.\n\nExample\n\njulia> df = DataFrame(a = 1:2, b = 3:4)\n2×2 DataFrame\n│ Row │ a     │ b     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 3     │\n│ 2   │ 2     │ 4     │\n\njulia> repeat(df, inner = 2, outer = 3)\n12×2 DataFrame\n│ Row │ a     │ b     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 3     │\n│ 2   │ 1     │ 3     │\n│ 3   │ 2     │ 4     │\n│ 4   │ 2     │ 4     │\n│ 5   │ 1     │ 3     │\n│ 6   │ 1     │ 3     │\n│ 7   │ 2     │ 4     │\n│ 8   │ 2     │ 4     │\n│ 9   │ 1     │ 3     │\n│ 10  │ 1     │ 3     │\n│ 11  │ 2     │ 4     │\n│ 12  │ 2     │ 4     │\n\n\n\n\n\nrepeat(df::AbstractDataFrame, count::Integer)\n\nConstruct a data frame by repeating each row in df the number of times specified by count.\n\nExample\n\njulia> df = DataFrame(a = 1:2, b = 3:4)\n2×2 DataFrame\n│ Row │ a     │ b     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 3     │\n│ 2   │ 2     │ 4     │\n\njulia> repeat(df, 2)\n4×2 DataFrame\n│ Row │ a     │ b     │\n│     │ Int64 │ Int64 │\n├─────┼───────┼───────┤\n│ 1   │ 1     │ 3     │\n│ 2   │ 2     │ 4     │\n│ 3   │ 1     │ 3     │\n│ 4   │ 2     │ 4     │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.show",
    "page": "Functions",
    "title": "Base.show",
    "category": "function",
    "text": "show([io::IO,] df::AbstractDataFrame;\n     allrows::Bool = !get(io, :limit, false),\n     allcols::Bool = !get(io, :limit, false),\n     allgroups::Bool = !get(io, :limit, false),\n     splitcols::Bool = get(io, :limit, false),\n     rowlabel::Symbol = :Row,\n     summary::Bool = true)\n\nRender a data frame to an I/O stream. The specific visual representation chosen depends on the width of the display.\n\nIf io is omitted, the result is printed to stdout, and allrows, allcols and allgroups default to false while splitcols defaults to true.\n\nArguments\n\nio::IO: The I/O stream to which df will be printed.\ndf::AbstractDataFrame: The data frame to print.\nallrows::Bool: Whether to print all rows, rather than a subset that fits the device height. By default this is the case only if io does not have the IOContext property limit set.\nallcols::Bool: Whether to print all columns, rather than a subset that fits the device width. By default this is the case only if io does not have the IOContext property limit set.\nallgroups::Bool: Whether to print all groups rather than the first and last, when df is a GroupedDataFrame. By default this is the case only if io does not have the IOContext property limit set.\nsplitcols::Bool: Whether to split printing in chunks of columns fitting the screen width rather than printing all columns in the same block. Only applies if allcols is true. By default this is the case only if io has the IOContext property limit set.\nrowlabel::Symbol = :Row: The label to use for the column containing row numbers.\nsummary::Bool = true: Whether to print a brief string summary of the data frame.\n\nExamples\n\njulia> using DataFrames\n\njulia> df = DataFrame(A = 1:3, B = [\"x\", \"y\", \"z\"]);\n\njulia> show(df, allcols=true)\n3×2 DataFrame\n│ Row │ A     │ B      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ x      │\n│ 2   │ 2     │ y      │\n│ 3   │ 3     │ z      │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.sort",
    "page": "Functions",
    "title": "Base.sort",
    "category": "function",
    "text": "sort(df::AbstractDataFrame, cols;\n     alg::Union{Algorithm, Nothing}=nothing, lt=isless, by=identity,\n     rev::Bool=false, order::Ordering=Forward)\n\nReturn a copy of data frame df sorted by column(s) cols. cols can be either a Symbol or Integer column index, or a tuple or vector of such indices.\n\nIf alg is nothing (the default), the most appropriate algorithm is chosen automatically among TimSort, MergeSort and RadixSort depending on the type of the sorting columns and on the number of rows in df. If rev is true, reverse sorting is performed. To enable reverse sorting only for some columns, pass order(c, rev=true) in cols, with c the corresponding column index (see example below). See sort! for a description of other keyword arguments.\n\nExamples\n\njulia> df = DataFrame(x = [3, 1, 2, 1], y = [\"b\", \"c\", \"a\", \"b\"])\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 1     │ b      │\n\njulia> sort(df, :x)\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ c      │\n│ 2   │ 1     │ b      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\njulia> sort(df, (:x, :y))\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\njulia> sort(df, (:x, :y), rev=true)\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 2     │ a      │\n│ 3   │ 1     │ c      │\n│ 4   │ 1     │ b      │\n\njulia> sort(df, (:x, order(:y, rev=true)))\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ c      │\n│ 2   │ 1     │ b      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.sort!",
    "page": "Functions",
    "title": "Base.sort!",
    "category": "function",
    "text": "sort!(df::AbstractDataFrame, cols;\n      alg::Union{Algorithm, Nothing}=nothing, lt=isless, by=identity,\n      rev::Bool=false, order::Ordering=Forward)\n\nSort data frame df by column(s) cols. cols can be either a Symbol or Integer column index, or a tuple or vector of such indices.\n\nIf alg is nothing (the default), the most appropriate algorithm is chosen automatically among TimSort, MergeSort and RadixSort depending on the type of the sorting columns and on the number of rows in df. If rev is true, reverse sorting is performed. To enable reverse sorting only for some columns, pass order(c, rev=true) in cols, with c the corresponding column index (see example below). See other methods for a description of other keyword arguments.\n\nExamples\n\njulia> df = DataFrame(x = [3, 1, 2, 1], y = [\"b\", \"c\", \"a\", \"b\"])\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 1     │ b      │\n\njulia> sort!(df, :x)\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ c      │\n│ 2   │ 1     │ b      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\njulia> sort!(df, (:x, :y))\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ b      │\n│ 2   │ 1     │ c      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\njulia> sort!(df, (:x, :y), rev=true)\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 3     │ b      │\n│ 2   │ 2     │ a      │\n│ 3   │ 1     │ c      │\n│ 4   │ 1     │ b      │\n\njulia> sort!(df, (:x, order(:y, rev=true)))\n4×2 DataFrame\n│ Row │ x     │ y      │\n│     │ Int64 │ String │\n├─────┼───────┼────────┤\n│ 1   │ 1     │ c      │\n│ 2   │ 1     │ b      │\n│ 3   │ 2     │ a      │\n│ 4   │ 3     │ b      │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Base.unique!",
    "page": "Functions",
    "title": "Base.unique!",
    "category": "function",
    "text": "Delete duplicate rows\n\nunique(df::AbstractDataFrame)\nunique(df::AbstractDataFrame, cols)\nunique!(df::AbstractDataFrame)\nunique!(df::AbstractDataFrame, cols)\n\nArguments\n\ndf : the AbstractDataFrame\ncols :  column indicator (Symbol, Int, Vector{Symbol}, etc.)\n\nspecifying the column(s) to compare.\n\nResult\n\n::AbstractDataFrame : the updated version of df with unique rows.\n\nWhen cols is specified, the return DataFrame contains complete rows, retaining in each case the first instance for which df[cols] is unique.\n\nSee also nonunique.\n\nExamples\n\ndf = DataFrame(i = 1:10, x = rand(10), y = rand([\"a\", \"b\", \"c\"], 10))\ndf = vcat(df, df)\nunique(df)   # doesn\'t modify df\nunique(df, 1)\nunique!(df)  # modifies df\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#DataFrames.permutecols!",
    "page": "Functions",
    "title": "DataFrames.permutecols!",
    "category": "function",
    "text": "permutecols!(df::DataFrame, p::AbstractVector)\n\nPermute the columns of df in-place, according to permutation p. Elements of p may be either column indices (Int) or names (Symbol), but cannot be a combination of both. All columns must be listed.\n\nExamples\n\njulia> df = DataFrame(a=1:5, b=2:6, c=3:7)\n5×3 DataFrame\n│ Row │ a     │ b     │ c     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 2     │ 3     │\n│ 2   │ 2     │ 3     │ 4     │\n│ 3   │ 3     │ 4     │ 5     │\n│ 4   │ 4     │ 5     │ 6     │\n│ 5   │ 5     │ 6     │ 7     │\n\njulia> permutecols!(df, [2, 1, 3]);\n\njulia> df\n5×3 DataFrame\n│ Row │ b     │ a     │ c     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 2     │ 1     │ 3     │\n│ 2   │ 3     │ 2     │ 4     │\n│ 3   │ 4     │ 3     │ 5     │\n│ 4   │ 5     │ 4     │ 6     │\n│ 5   │ 6     │ 5     │ 7     │\n\njulia> permutecols!(df, [:c, :a, :b]);\n\njulia> df\n5×3 DataFrame\n│ Row │ c     │ a     │ b     │\n│     │ Int64 │ Int64 │ Int64 │\n├─────┼───────┼───────┼───────┤\n│ 1   │ 3     │ 1     │ 2     │\n│ 2   │ 4     │ 2     │ 3     │\n│ 3   │ 5     │ 3     │ 4     │\n│ 4   │ 6     │ 4     │ 5     │\n│ 5   │ 7     │ 5     │ 6     │\n\n\n\n\n\n"
},

{
    "location": "lib/functions.html#Basics-1",
    "page": "Functions",
    "title": "Basics",
    "category": "section",
    "text": "allowmissing!\ncolumns\ncompletecases\ndescribe\ndisallowmissing!\ndropmissing\ndropmissing!\neachrow\neachcol\neltypes\nfilter\nfilter!\ninsertcols!\nmapcols\nnames!\nnonunique\nrename!\nrename\nrepeat\nshow\nsort\nsort!\nunique!\npermutecols!"
},

{
    "location": "lib/indexing.html#",
    "page": "Indexing",
    "title": "Indexing",
    "category": "page",
    "text": "CurrentModule = DataFrames"
},

{
    "location": "lib/indexing.html#Indexing-1",
    "page": "Indexing",
    "title": "Indexing",
    "category": "section",
    "text": "Pages = [\"indexing.md\"]"
},

{
    "location": "lib/indexing.html#General-rules-1",
    "page": "Indexing",
    "title": "General rules",
    "category": "section",
    "text": "The following rules explain target functionality of how getindex, setindex!, and view are intended to work with DataFrame, SubDataFrame and DataFrameRow objects.The rules for a valid type of index into a column are the following:a value, later denoted as col:\na Symbol;\nan Integer that is not Bool;\na vector, later denoted as cols:\na vector of Symbol (does not have to be a subtype of AbstractVector{Symbol});\na vector of Integer other than Bool (does not have to be a subtype of AbstractVector{<:Integer});\na vector of Bool that has to be a subtype of AbstractVector{Bool}.\na colon.The rules for a valid type of index into a row are the following:a value, later denoted as row:\nan Integer that is not Bool;\na vector, later denoted as rows:\na vector of Integer other than Bool (does not have to be a subtype of AbstractVector{<:Integer});\na vector of Bool that has to be a subtype of AbstractVector{Bool};\na colon.In the descriptions below df represents a DataFrame, sdf is a SubDataFrame and dfr is a DataFrameRow."
},

{
    "location": "lib/indexing.html#getindex-1",
    "page": "Indexing",
    "title": "getindex",
    "category": "section",
    "text": "The following list specifies return types of getindex operations depending on argument types.In all operations copying vectors is avoided where possible. If it is performed a description explicitly mentions that the data is copied.For performance reasons, accessing, via getindex or view, a single row and multiple cols of a DataFrame, a SubDataFrame or a DataFrameRow always returns a DataFrameRow (which is a view-like type).DataFrame:df[col] -> the vector contained in column col;\ndf[cols] -> a freshly allocated DataFrame containing the vectors contained in columns cols;\ndf[row, col] -> the value contained in row row of column col, the same as df[col][row];\ndf[row, cols] -> a DataFrameRow with parent df if cols is a colon and df[cols] otherwise;\ndf[rows, col] -> a copy of the vector df[col] with only the entries corresponding to rows selected, the same as df[col][rows];\ndf[rows, cols] -> a DataFrame containing copies of columns cols with only the entries corresponding to rows selected.\n@view df[col] -> the vector contained in column col (this is equivalent to df[col]);\n@view df[cols] -> a SubDataFrame with parent df if cols is a colon and df[cols] otherwise;\n@view df[row, col] -> a 0-dimensional view into df[col], the same as view(df[col], row);\n@view df[row, cols] -> a DataFrameRow with parent df if cols is a colon and df[cols] otherwise;\n@view df[rows, col] -> a view into df[col] with rows selected, the same as view(df[col], rows);\n@view df[rows, cols] -> a SubDataFrame with rows selected with parent df if cols is a colon and df[cols] otherwise.SubDataFrame:sdf[col] -> a view of the vector contained in column col of parent(sdf) with DataFrames.rows(sdf) as a selector;\nsdf[cols] -> a SubDataFrame, with parent parent(sdf) if cols is a colon and parent(sdf)[cols] otherwise;\nsdf[row, col] -> a value contained in row row of column col;\nsdf[row, cols] -> a DataFrameRow with parent parent(sdf) if cols is a colon and parent(sdf)[cols] otherwise;\nsdf[rows, col] -> a copy of a vector sdf[col] with only rows rows selected;\nsdf[rows, cols] -> a DataFrame containing columns cols and df[rows, col] as a vector in each col in cols.\n@view sdf[col] -> a view of vector contained in column col of parent(sdf) with DataFrames.rows(sdf) as selector;\n@view sdf[cols] -> a SubDataFrame with parent parent(sdf) if cols is a colon and parent(sdf)[cols] otherwise;\n@view sdf[row, col] -> translates to view(sdf[col], row) (a 0-dimensional view into df[col]);\n@view sdf[row, cols] -> a DataFrameRow with parent parent(sdf) if cols is a colon and parent(sdf)[cols] otherwise;\n@view sdf[rows, col] -> translates to view(sdf[col], rows) (a standard view into sdf[col] vector);\n@view sdf[rows, cols] -> a SubDataFrame with parent parent(sdf) if cols is a colon and sdf[cols] otherwise.DataFrameRow:dfr[col] -> the value contained in column col of dfr;\ndfr[cols] -> a DataFrameRow with parent parent(dfr) if cols is a colon and parent(dfr)[cols] otherwise;\n@view dfr[col] -> a 0-dimensional view into parent(dfr)[DataFrames.row(dfr), col];\n@view dfr[cols] -> a DataFrameRow with parent parent(dfr) if cols is a colon and parent(dfr)[cols] otherwise;"
},

{
    "location": "lib/indexing.html#setindex!-1",
    "page": "Indexing",
    "title": "setindex!",
    "category": "section",
    "text": "Under construction"
},

]}
