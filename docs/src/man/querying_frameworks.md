# Data manipulation frameworks

Two popular frameworks provide convenience methods to manipulate `DataFrame`s: DataFramesMeta.jl and Query.jl. They implement a functionality similar to [dplyr](https://dplyr.tidyverse.org/) or [LINQ](https://en.wikipedia.org/wiki/Language_Integrated_Query).

## DataFramesMeta.jl

The [DataFramesMeta.jl](https://github.com/JuliaStats/DataFramesMeta.jl) package provides a convenient yet fast macro-based interface to work with `DataFrame`s.

First install the DataFramesMeta.jl package:

```julia
using Pkg
Pkg.add("DataFramesMeta")
```

The major benefit of the package is that it allows you to refer to columns of a `DataFrame` as `Symbol`s. Therefore instead of writing `verylongdataframename.variable` you can simply write `:variable` in expressions. Additionally you can chain a sequence of transformations of a `DataFrame` using the `@linq` macro.

Here is a minimal example of usage of the package. Observe that we refer to names of columns using only their names and that chaining is performed using the `@linq` macro and the `|>` operator:

```jldoctest dataframesmeta
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(name=["John", "Sally", "Roger"],
                      age=[54., 34., 79.],
                      children=[0, 2, 4])
3×3 DataFrame
│ Row │ name   │ age     │ children │
│     │ String │ Float64 │ Int64    │
├─────┼────────┼─────────┼──────────┤
│ 1   │ John   │ 54.0    │ 0        │
│ 2   │ Sally  │ 34.0    │ 2        │
│ 3   │ Roger  │ 79.0    │ 4        │

julia> @linq df |>
           where(:age .> 40) |>
           select(number_of_children=:children, :name)
2×2 DataFrame
│ Row │ number_of_children │ name   │
│     │ Int64              │ String │
├─────┼────────────────────┼────────┤
│ 1   │ 0                  │ John   │
│ 2   │ 4                  │ Roger  │
```

In the following examples we show that DataFramesMeta.jl also supports the split-apply-combine pattern:

```jldoctest dataframesmeta
julia> df = DataFrame(key=repeat(1:3, 4), value=1:12)
12×2 DataFrame
│ Row │ key   │ value │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 3     │
│ 4   │ 1     │ 4     │
│ 5   │ 2     │ 5     │
│ 6   │ 3     │ 6     │
│ 7   │ 1     │ 7     │
│ 8   │ 2     │ 8     │
│ 9   │ 3     │ 9     │
│ 10  │ 1     │ 10    │
│ 11  │ 2     │ 11    │
│ 12  │ 3     │ 12    │

julia> @linq df |>
           where(:value .> 3) |>
           by(:key, min=minimum(:value), max=maximum(:value)) |>
           select(:key, range=:max - :min)
3×2 DataFrame
│ Row │ key   │ range │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 6     │
│ 2   │ 2     │ 6     │
│ 3   │ 3     │ 6     │

julia> @linq df |>
           groupby(:key) |>
           transform(value0 = :value .- minimum(:value))
12×3 DataFrame
│ Row │ key   │ value │ value0 │
│     │ Int64 │ Int64 │ Int64  │
├─────┼───────┼───────┼────────┤
│ 1   │ 1     │ 1     │ 0      │
│ 2   │ 1     │ 4     │ 3      │
│ 3   │ 1     │ 7     │ 6      │
│ 4   │ 1     │ 10    │ 9      │
│ 5   │ 2     │ 2     │ 0      │
│ 6   │ 2     │ 5     │ 3      │
│ 7   │ 2     │ 8     │ 6      │
│ 8   │ 2     │ 11    │ 9      │
│ 9   │ 3     │ 3     │ 0      │
│ 10  │ 3     │ 6     │ 3      │
│ 11  │ 3     │ 9     │ 6      │
│ 12  │ 3     │ 12    │ 9      │
```

You can find more details about how this package can be used on the [DataFramesMeta.jl GitHub page](https://github.com/JuliaData/DataFramesMeta.jl).

## Query.jl

The [Query.jl](https://github.com/queryverse/Query.jl) package provides advanced data manipulation capabilities for `DataFrame`s (and many other data structures). This section provides a short introduction to the package, the [Query.jl documentation](http://www.queryverse.org/Query.jl/stable/) has a more comprehensive documentation of the package.

To get started, install the Query.jl package:

```julia
using Pkg
Pkg.add("Query")
```

A query is started with the `@from` macro and consists of a series of query commands. Query.jl provides commands that can filter, project, join, flatten and group data from a `DataFrame`. A query can return an iterator, or one can materialize the results of a query into a variety of data structures, including a new `DataFrame`.

A simple example of a query looks like this:

```jldoctest query
julia> using DataFrames, Query

julia> df = DataFrame(name=["John", "Sally", "Roger"],
                      age=[54., 34., 79.],
                      children=[0, 2, 4])
3×3 DataFrame
│ Row │ name   │ age     │ children │
│     │ String │ Float64 │ Int64    │
├─────┼────────┼─────────┼──────────┤
│ 1   │ John   │ 54.0    │ 0        │
│ 2   │ Sally  │ 34.0    │ 2        │
│ 3   │ Roger  │ 79.0    │ 4        │

julia> q1 = @from i in df begin
            @where i.age > 40
            @select {number_of_children=i.children, i.name}
            @collect DataFrame
       end
2×2 DataFrame
│ Row │ number_of_children │ name   │
│     │ Int64              │ String │
├─────┼────────────────────┼────────┤
│ 1   │ 0                  │ John   │
│ 2   │ 4                  │ Roger  │
```

The query starts with the `@from` macro. The first argument `i` is the name of the range variable that will be used to refer to an individual row in later query commands. The next argument `df` is the data source that one wants to query. The `@where` command in this query will filter the source data by applying the filter condition `i.age > 40`. This filters out any rows in which the `age` column is not larger than 40. The `@select` command then projects the columns of the source data onto a new column structure. The example here applies three specific modifications: 1) it only keeps a subset of the columns in the source `DataFrame`, i.e. the `age` column will not be part of the transformed data; 2) it changes the order of the two columns that are selected; and 3) it renames one of the columns that is selected from `children` to `number_of_children`. The example query uses the `{}` syntax to achieve this. A `{}` in a Query.jl expression instantiates a new [NamedTuple](https://github.com/blackrock/NamedTuples.jl), i.e. it is a shortcut for writing `@NT(number_of_children=>i.children, name=>i.name)`. The `@collect` statement determines the data structure that the query returns. In this example the results are returned as a `DataFrame`.

A query without a `@collect` statement returns a standard julia iterator that can be used with any normal julia language construct that can deal with iterators. The following code returns a julia iterator for the query results:

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
1-element Array{String,1}:
 "Roger"

```

The last example (extracting only the name and applying a second filter) could of course be completely expressed as a query expression:

```jldoctest query
julia> q3 = @from i in df begin
            @where i.age > 40 && i.children > 0
            @select i.name
            @collect
       end
1-element Array{String,1}:
 "Roger"

```

A query that ends with a `@collect` statement without a specific type will materialize the query results into an array. Note also the difference in the `@select` statement: The previous queries all used the `{}` syntax in the `@select` statement to project results into a tabular format. The last query instead just selects a single value from each row in the `@select` statement.

These examples only scratch the surface of what one can do with [Query.jl](https://github.com/queryverse/Query.jl), and the interested reader is referred to the [Query.jl documentation](http://www.queryverse.org/Query.jl/stable/) for more information.
