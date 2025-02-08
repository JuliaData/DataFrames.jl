# Data manipulation frameworks

Four frameworks provide convenience methods to manipulate `DataFrame`s:
TidierData.jl, DataFramesMeta.jl, DataFrameMacros.jl and Query.jl. They implement a functionality similar to
[dplyr](https://dplyr.tidyverse.org/) or
[LINQ](https://en.wikipedia.org/wiki/Language_Integrated_Query).

These frameworks are designed both to make it easier for new users to start working with data frames in Julia
and to allow advanced users to write more compact code.

## TidierData.jl
[TidierData.jl](https://tidierorg.github.io/TidierData.jl/latest/), part of 
the [Tidier](https://tidierorg.github.io/Tidier.jl/dev/) ecosystem, is a macro-based 
data analysis interface that wraps DataFrames.jl.  The instructions below are for version 
0.16.0 of TidierData.jl.

First, install the TidierData.jl package:

```julia
using Pkg
Pkg.add("TidierData")
```

TidierData.jl enables clean, readable, and fast code for all major data transformation 
functions including 
[aggregating](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/summarize/), 
[pivoting](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/pivots/), 
[nesting](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/nesting/), 
and [joining](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/joins/) 
data frames. TidierData re-exports `DataFrame` from DataFrames.jl, `@chain` from Chain.jl, and 
Statistics.jl to streamline data operations. 

TidierData.jl is heavily inspired by the `dplyr` and `tidyr` R packages (part of the R 
`tidyverse`), which it aims to implement using pure Julia by wrapping DataFrames.jl. While
TidierData.jl borrows conventions from the `tidyverse`, it is important to note that the 
`tidyverse` itself is often not considered idiomatic R code. TidierData.jl brings 
data analysis conventions from `tidyverse` into Julia to have the best of both worlds: 
tidy syntax and the speed and flexibility of the Julia language.

TidierData.jl has two major differences from other macro-based packages. First, TidierData.jl 
uses tidy expressions. An example of a tidy expression is `a = mean(b)`, where `b` refers 
to an existing column in the data frame, and `a` refers to either a new or existing column. 
Referring to variables outside of the data frame requires prefixing variables with `!!`. 
For example, `a = mean(!!b)` refers to a variable `b` outside the data frame. Second, 
TidierData.jl aims to make broadcasting mostly invisible through 
[auto-vectorization](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/autovec/). TidierData.jl currently uses a lookup table to decide which functions not to 
vectorize; all other functions are automatically vectorized. This allows for 
writing of concise expressions: `@mutate(df, a = a - mean(a))` transforms the `a` column 
by subtracting each value by the mean of the column. Behind the scenes, the right-hand 
expression is converted to `a .- mean(a)` because `mean()` is in the lookup table as a 
function that should not be vectorized. Take a look at the 
[auto-vectorization](https://tidierorg.github.io/TidierData.jl/latest/examples/generated/UserGuide/autovec/) documentation for details.

One major benefit of combining tidy expressions with auto-vectorization is that 
TidierData.jl code (which uses DataFrames.jl as its backend) can work directly on 
databases using [TidierDB.jl](https://github.com/TidierOrg/TidierDB.jl), 
which converts tidy expressions into SQL, supporting DuckDB and several other backends.

```jldoctest tidierdata
julia> using TidierData

julia> df = DataFrame(
                name = ["John", "Sally", "Roger"],
                age = [54.0, 34.0, 79.0],
                children = [0, 2, 4]
            )
3×3 DataFrame
 Row │ name    age      children
     │ String  Float64  Int64
─────┼───────────────────────────
   1 │ John       54.0         0
   2 │ Sally      34.0         2
   3 │ Roger      79.0         4

julia> @chain df begin
           @filter(children != 2)
           @select(name, num_children = children)
       end
2×2 DataFrame
 Row │ name    num_children 
     │ String  Int64        
─────┼──────────────────────
   1 │ John               0
   2 │ Roger              4
```

Below are examples showcasing `@group_by` with `@summarize` or `@mutate` - analagous to the split, apply, combine pattern.

```jldoctest tidierdata
julia> df = DataFrame(
                groups = repeat('a':'e', inner = 2), 
                b_col = 1:10, 
                c_col = 11:20, 
                d_col = 111:120
            )
10×4 DataFrame
 Row │ groups  b_col  c_col  d_col 
     │ Char    Int64  Int64  Int64 
─────┼─────────────────────────────
   1 │ a           1     11    111
   2 │ a           2     12    112
   3 │ b           3     13    113
   4 │ b           4     14    114
   5 │ c           5     15    115
   6 │ c           6     16    116
   7 │ d           7     17    117
   8 │ d           8     18    118
   9 │ e           9     19    119
  10 │ e          10     20    120

julia> @chain df begin
           @filter(b_col > 2)
           @group_by(groups)
           @summarise(median_b = median(b_col), 
                      across((b_col:d_col), mean))   
       end
4×5 DataFrame
 Row │ groups  median_b  b_col_mean  c_col_mean  d_col_mean 
     │ Char    Float64   Float64     Float64     Float64    
─────┼──────────────────────────────────────────────────────
   1 │ b            3.5         3.5        13.5       113.5
   2 │ c            5.5         5.5        15.5       115.5
   3 │ d            7.5         7.5        17.5       117.5
   4 │ e            9.5         9.5        19.5       119.5

julia> @chain df begin
           @filter(b_col > 4 && c_col <= 18)
           @group_by(groups)
           @mutate(
               new_col = b_col + maximum(d_col),
               new_col2 = c_col - maximum(d_col),
               new_col3 = case_when(c_col >= 18  => "high",
                                    c_col > 15   => "medium",
                                    true         => "low"))
           @select(starts_with("new"))
           @ungroup # required because `@mutate` does not ungroup
       end
4×4 DataFrame
 Row │ groups  new_col  new_col2  new_col3 
     │ Char    Int64    Int64     String   
─────┼─────────────────────────────────────
   1 │ c           121      -101  low
   2 │ c           122      -100  medium
   3 │ d           125      -101  medium
   4 │ d           126      -100  high
```

For more examples, please visit the [TidierData.jl](https://tidierorg.github.io/TidierData.jl/latest/) documentation.

## DataFramesMeta.jl

The [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl) package
provides a convenient yet fast macro-based interface to work with `DataFrame`s.
The instructions below are for version 0.10.0 of DataFramesMeta.jl.

First install the DataFramesMeta.jl package:

```julia
using Pkg
Pkg.add("DataFramesMeta")
```

The major benefit of the package is it provides a more convenient syntax
for the transformation functions `transform`, `select`, and `combine` 
via the macros `@transform`, `@select`, `@combine`, and more.

DataFramesMeta.jl also reexports the `@chain` macro from 
[Chain.jl](https://github.com/jkrumbiegel/Chain.jl), allowing users to
pipe the output of one transformation as an input to another, as with 
`|>` and `%>%` in R. 

Below we present several selected examples of usage of the package.

First we subset rows of the source data frame using a logical condition
and select two of its columns, renaming one of them:

```jldoctest dataframesmeta
julia> using DataFramesMeta

julia> df = DataFrame(name=["John", "Sally", "Roger"],
                      age=[54.0, 34.0, 79.0],
                      children=[0, 2, 4])
3×3 DataFrame
 Row │ name    age      children
     │ String  Float64  Int64
─────┼───────────────────────────
   1 │ John       54.0         0
   2 │ Sally      34.0         2
   3 │ Roger      79.0         4

julia> @chain df begin
           @rsubset :age > 40 
           @select(:number_of_children = :children, :name)
       end
2×2 DataFrame
 Row │ number_of_children  name
     │ Int64               String
─────┼────────────────────────────
   1 │                  0  John
   2 │                  4  Roger
```

In the following examples we show that DataFramesMeta.jl also supports the split-apply-combine pattern:

```jldoctest dataframesmeta
julia> df = DataFrame(key=repeat(1:3, 4), value=1:12)
12×2 DataFrame
 Row │ key    value
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      4
   5 │     2      5
   6 │     3      6
   7 │     1      7
   8 │     2      8
   9 │     3      9
  10 │     1     10
  11 │     2     11
  12 │     3     12

julia> @chain df begin
           @rsubset :value > 3 
           @by(:key, :min = minimum(:value), :max = maximum(:value))
           @select(:key, :range = :max - :min)
        end
3×2 DataFrame
 Row │ key    range
     │ Int64  Int64
─────┼──────────────
   1 │     1      6
   2 │     2      6
   3 │     3      6

julia> @chain df begin
           groupby(:key)
           @transform :value0 = :value .- minimum(:value)
       end
12×3 DataFrame
 Row │ key    value  value0
     │ Int64  Int64  Int64
─────┼──────────────────────
   1 │     1      1       0
   2 │     2      2       0
   3 │     3      3       0
   4 │     1      4       3
   5 │     2      5       3
   6 │     3      6       3
   7 │     1      7       6
   8 │     2      8       6
   9 │     3      9       6
  10 │     1     10       9
  11 │     2     11       9
  12 │     3     12       9
```

You can find more details about how this package can be used on the
[DataFramesMeta.jl GitHub page](https://github.com/JuliaData/DataFramesMeta.jl).

## DataFrameMacros.jl

[DataFrameMacros.jl](https://github.com/jkrumbiegel/DataFrameMacros.jl) is
an alternative to DataFramesMeta.jl with an additional focus on convenient
solutions for the transformation of multiple columns at once.
The instructions below are for version 0.3 of DataFrameMacros.jl.

First, install the DataFrameMacros.jl package:

```julia
using Pkg
Pkg.add("DataFrameMacros")
```

In DataFrameMacros.jl, all but the `@combine` macro are row-wise by default.
There is also a `@groupby` which allows creating grouping columns on the fly
using the same syntax as `@transform`, for grouping by new columns
without writing them out twice.

In the example below, you can also see some of DataFrameMacros.jl's multi-column
features, where `mean` is applied to both age columns at once by selecting
them with the `r"age"` regex. The new column names are then derived using the
`"{}"` shortcut which splices the transformed column names into a string.

```jldoctest dataframemacros
julia> using DataFrames, DataFrameMacros, Chain, Statistics

julia> df = DataFrame(name=["John", "Sally", "Roger"],
                      age=[54.0, 34.0, 79.0],
                      children=[0, 2, 4])
3×3 DataFrame
 Row │ name    age      children 
     │ String  Float64  Int64    
─────┼───────────────────────────
   1 │ John       54.0         0
   2 │ Sally      34.0         2
   3 │ Roger      79.0         4

julia> @chain df begin
           @transform :age_months = :age * 12
           @groupby :has_child = :children > 0
           @combine "mean_{}" = mean({r"age"})
       end
2×3 DataFrame
 Row │ has_child  mean_age  mean_age_months 
     │ Bool       Float64   Float64         
─────┼──────────────────────────────────────
   1 │     false      54.0            648.0
   2 │      true      56.5            678.0
```

There's also the capability to reference a group of multiple columns as a single unit,
for example to run aggregations over them, with the `{{ }}` syntax.
In the following example, the first quarter is compared to the maximum of the other three:

```jldoctest dataframemacros
julia> df = DataFrame(q1 = [12.0, 0.4, 42.7],
                      q2 = [6.4, 2.3, 40.9],
                      q3 = [9.5, 0.2, 13.6],
                      q4 = [6.3, 5.4, 39.3])
3×4 DataFrame
 Row │ q1       q2       q3       q4      
     │ Float64  Float64  Float64  Float64 
─────┼────────────────────────────────────
   1 │    12.0      6.4      9.5      6.3
   2 │     0.4      2.3      0.2      5.4
   3 │    42.7     40.9     13.6     39.3

julia> @transform df :q1_best = :q1 > maximum({{Not(:q1)}})
3×5 DataFrame
 Row │ q1       q2       q3       q4       q1_best 
     │ Float64  Float64  Float64  Float64  Bool    
─────┼─────────────────────────────────────────────
   1 │    12.0      6.4      9.5      6.3     true
   2 │     0.4      2.3      0.2      5.4    false
   3 │    42.7     40.9     13.6     39.3     true
```

## Query.jl

The [Query.jl](https://github.com/queryverse/Query.jl) package provides advanced
data manipulation capabilities for `DataFrame`s (and many other data
structures). This section provides a short introduction to the package, the
[Query.jl documentation](http://www.queryverse.org/Query.jl/stable/) has a more
comprehensive documentation of the package. The instructions here are for version
1.0.0 of Query.jl.

To get started, install the Query.jl package:

```julia
using Pkg
Pkg.add("Query")
```

A query is started with the `@from` macro and consists of a series of query
commands. Query.jl provides commands that can filter, project, join, flatten and
group data from a `DataFrame`. A query can return an iterator, or one can
materialize the results of a query into a variety of data structures, including
a new `DataFrame`.

A simple example of a query looks like this:

```jldoctest query
julia> using DataFrames, Query

julia> df = DataFrame(name=["John", "Sally", "Roger"],
                      age=[54.0, 34.0, 79.0],
                      children=[0, 2, 4])
3×3 DataFrame
 Row │ name    age      children
     │ String  Float64  Int64
─────┼───────────────────────────
   1 │ John       54.0         0
   2 │ Sally      34.0         2
   3 │ Roger      79.0         4

julia> q1 = @from i in df begin
            @where i.age > 40
            @select {number_of_children=i.children, i.name}
            @collect DataFrame
       end
2×2 DataFrame
 Row │ number_of_children  name
     │ Int64               String
─────┼────────────────────────────
   1 │                  0  John
   2 │                  4  Roger
```

The query starts with the `@from` macro. The first argument `i` is the name of
the range variable that will be used to refer to an individual row in later
query commands. The next argument `df` is the data source that one wants to
query. The `@where` command in this query will filter the source data by
applying the filter condition `i.age > 40`. This filters out any rows in which
the `age` column is not larger than 40. The `@select` command then projects the
columns of the source data onto a new column structure. The example here applies
three specific modifications: 1) it only keeps a subset of the columns in the
source `DataFrame`, i.e. the `age` column will not be part of the transformed
data; 2) it changes the order of the two columns that are selected; and 3) it
renames one of the columns that is selected from `children` to
`number_of_children`. The example query uses the `{}` syntax to achieve this. A
`{}` in a Query.jl expression instantiates a new
[NamedTuple](https://github.com/blackrock/NamedTuples.jl), i.e. it is a shortcut
for writing `@NT(number_of_children=>i.children, name=>i.name)`. The `@collect`
statement determines the data structure that the query returns. In this example
the results are returned as a `DataFrame`.

A query without a `@collect` statement returns a standard julia iterator that
can be used with any normal julia language construct that can deal with
iterators. The following code returns a julia iterator for the query results:

```jldoctest query
julia> q2 = @from i in df begin
                   @where i.age > 40
                   @select {number_of_children=i.children, i.name}
              end; # suppress printing the iterator type

```

One can loop over the results using a standard julia `for` statement:

```jldoctest query
julia> total_children = 0
0

julia> for i in q2
           global total_children += i.number_of_children
       end

julia> total_children
4

```

Or one can use a comprehension to extract the name of a subset of rows:

```jldoctest query
julia> y = [i.name for i in q2 if i.number_of_children > 0]
1-element Vector{String}:
 "Roger"

```

The last example (extracting only the name and applying a second filter) could
of course be completely expressed as a query expression:

```jldoctest query
julia> q3 = @from i in df begin
            @where i.age > 40 && i.children > 0
            @select i.name
            @collect
       end
1-element Vector{String}:
 "Roger"

```

A query that ends with a `@collect` statement without a specific type will
materialize the query results into an array. Note also the difference in the
`@select` statement: The previous queries all used the `{}` syntax in the
`@select` statement to project results into a tabular format. The last query
instead just selects a single value from each row in the `@select` statement.

These examples only scratch the surface of what one can do with
[Query.jl](https://github.com/queryverse/Query.jl), and the interested reader is
referred to the [Query.jl
documentation](http://www.queryverse.org/Query.jl/stable/) for more
information.
