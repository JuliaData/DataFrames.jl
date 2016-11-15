# Querying frameworks

## Query.jl

The [Query.jl](https://github.com/davidanthoff/Query.jl) package provides advanced data manipulation capabilities for `DataFrames` (and many other data structures). This section provides a short introduction to the package, the [Query.jl documentation](http://www.david-anthoff.com/Query.jl/stable/) has a more comprehensive documentation of the package.

To get started, install the Query.jl package:

```julia
Pkg.add("Query")
```

A query is started with the `@from` macro and consists of a series of query commands. Query.jl provides commands that can filter, project, join, group, flatten and group data from a `DataFrame`. A query can return an iterator, or one can materialize the results of a query into a variety of data structures, including a new `DataFrame`.

A simple example of a query looks like this:

```@setup 1
using DataFrames, Query
```

```@example 1
using DataFrames, Query

df = DataFrame(name=["John", "Sally", "Roger"], age=[54., 34., 79.], children=[0, 2, 4])

q1 = @from i in df begin
     @where i.age > 40
     @select {number_of_children=i.children, i.name}
     @collect DataFrame
end
```

The query starts with the `@from` macro. The first argument `i` is the name of the range variable that will be used to refer to an individual row in later query commands. The next argument `df` is the data source that one wants to query. The `@where` command in this query will filter the source data by applying the filter condition `i.age > 40`. This filters out any rows in which the `age` column is not larger than 40. The `@select` command then projects the columns of the source data onto a new column structure. The example here applies three specific modifications: 1) it only keeps a subset of the columns in the source `DataFrame`, i.e. the `age` column will not be part of the transformed data; 2) it changes the order of the two columns that are selected; and 3) it renames one of the columns that is selected from `children` to `number_of_children`. The example query uses the `{}` syntax to achieve this. A `{}` in a Query.jl expression instantiates a new [NamedTuple](https://github.com/blackrock/NamedTuples.jl), i.e. it is a shortcut for writing `@NT(number_of_children=>i.children, name=>i.name)`. The `@collect` statement determines the data structure that the query returns. In this example the results are returned as a `DataFrame`.

A query without a `@collect` statement returns a standard julia iterator that can be used with any normal julia language construct that can deal with iterators. The following code returns a julia iterator for the query results:

```@example 1
q2 = @from i in df begin
     @where i.age > 40
     @select {number_of_children=i.children, i.name}
end
nothing # hide
```

One can loop over the results using a standard julia `for` statement:

```@example 1
total_children = 0
for i in q2
    total_children += i.number_of_children
end

println("Total number of children: $(get(total_children))")
```

Or one can use a comprehension to extract the name of a subset of rows:

```@example 1
y = [i.name for i in q2 if i.number_of_children > 0]
```

The last example (extracting only the name and applying a second filter) could of course be completely expressed as a query expression:

```@example 1
q3 = @from i in df begin
     @where i.age > 40 && i.children > 0
     @select i.name
     @collect
end
```

A query that ends with a `@collect` statement without a specific type will materialize the query results into an array. Note also the difference in the `@select` statement: The previous queries all used the `{}` syntax in the `@select` statement to project results into a tabular format. The last query instead just selects a single value from each row in the `@select` statement.

These examples only scratch the surface of what one can do with [Query.jl](https://github.com/davidanthoff/Query.jl), and the interested reader is referred to the [Query.jl documentation](http://www.david-anthoff.com/Query.jl/stable/) for more information.
