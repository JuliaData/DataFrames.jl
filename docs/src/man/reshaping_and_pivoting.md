# Reshaping and Pivoting Data

Reshape data from wide to long format using the `stack` function:

```julia
using DataFrames, RDatasets
iris = dataset("datasets", "iris")
iris[:id] = 1:size(iris, 1)  # this makes it easier to unstack
d = stack(iris, 1:4)
```

The second optional argument to `stack` indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:

```julia
d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth])
```

Note that all columns can be of different types. Type promotion follows the rules of `vcat`.

The stacked DataFrame that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled `:variable` and `:values` contain the column identifier and the stacked columns.

A third optional argument to `stack` represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:

```julia
d = stack(iris, [:SepalLength, :SepalWidth], :Species)
```

`melt` is an alternative function to reshape from wide to long format. It is based on `stack`, but it prefers specification of the id columns as:

```julia
d = melt(iris, :Species)
```

All other columns are assumed to be measured variables (they are stacked).

You can also stack an entire DataFrame. The default stacks all floating-point columns:

```julia
d = stack(iris)
```

`unstack` converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:

```julia
longdf = melt(iris, [:Species, :id])
widedf = unstack(longdf, :id, :variable, :value)
```

If the remaining columns are unique, you can skip the id variable and use:

```julia
widedf = unstack(longdf, :variable, :value)
```

`stackdf` and `meltdf` are two additional functions that work like `stack` and `melt`, but they provide a view into the original wide DataFrame. Here is an example:

```julia
d = stackdf(iris)
```

This saves memory. To create the view, several AbstractVectors are defined:

`:variable` column -- `EachRepeatedVector`  
This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.

`:value` column -- `StackedVector`  
This is provides a view of the original columns stacked together.

Id columns -- `RepeatedVector`  
This repeats the original columns N times where N is the number of columns stacked.

For more details on the storage representation, see:

```julia
dump(stackdf(iris))
```

None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:

```julia
d = stack(iris)
x = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(df[:value])))
unstack(x, :Species, :vsum)
```
