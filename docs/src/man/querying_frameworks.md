# Querying frameworks

The [Query.jl](https://github.com/queryverse/Query.jl) package provides advanced data manipulation capabilities for `DataFrames` (and many other data structures). This section provides a short introduction to the package, the [Query.jl documentation](http://www.queryverse.org/Query.jl/stable/) has a more comprehensive documentation of the package.

To get started, install the Query.jl package:

```julia
Pkg.add("Query")
```

A query is started with the `@from` macro and consists of a series of query commands. Query.jl provides commands that can filter, project, join, flatten and group data from a `DataFrame`. A query can return an iterator, or one can materialize the results of a query into a variety of data structures, including a new `DataFrame`.

A simple example of a query looks like this:

```jldoctest query
julia> using DataFrames, Query

julia> df = DataFrame(name=["John", "Sally", "Roger"], age=[54., 34., 79.], children=[0, 2, 4])
3×3 DataFrames.DataFrame
│ Row │ name  │ age  │ children │
├─────┼───────┼──────┼──────────┤
│ 1   │ John  │ 54.0 │ 0        │
│ 2   │ Sally │ 34.0 │ 2        │
│ 3   │ Roger │ 79.0 │ 4        │

julia> q1 = @from i in df begin
            @where i.age > 40
            @select {number_of_children=i.children, i.name}
            @collect DataFrame
       end
2×2 DataFrames.DataFrame
│ Row │ number_of_children │ name  │
├─────┼────────────────────┼───────┤
│ 1   │ 0                  │ John  │
│ 2   │ 4                  │ Roger │

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
           total_children += i.number_of_children
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
