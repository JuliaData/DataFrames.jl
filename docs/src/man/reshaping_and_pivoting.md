# Reshaping and Pivoting Data

Reshape data from wide to long format using the `stack` function:

```jldoctest reshape
julia> using DataFrames, CSV

julia> iris = DataFrame(CSV.File(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv")));

julia> first(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String      │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ Iris-setosa │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ Iris-setosa │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ Iris-setosa │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa │

julia> last(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64     │ Float64    │ Float64     │ Float64    │ String         │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ Iris-virginica │
│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ Iris-virginica │
│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ Iris-virginica │
│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ Iris-virginica │
│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │
│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ Iris-virginica │

julia> d = stack(iris, 1:4);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value   │ Species     │
│     │ Cat…        │ Float64 │ String      │
├─────┼─────────────┼─────────┼─────────────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value   │ Species        │
│     │ Cat…       │ Float64 │ String         │
├─────┼────────────┼─────────┼────────────────┤
│ 1   │ PetalWidth │ 2.5     │ Iris-virginica │
│ 2   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 3   │ PetalWidth │ 1.9     │ Iris-virginica │
│ 4   │ PetalWidth │ 2.0     │ Iris-virginica │
│ 5   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 6   │ PetalWidth │ 1.8     │ Iris-virginica │
```

The second optional argument to `stack` indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:

```jldoctest reshape
julia> d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth]);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value   │ Species     │
│     │ Cat…        │ Float64 │ String      │
├─────┼─────────────┼─────────┼─────────────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value   │ Species        │
│     │ Cat…       │ Float64 │ String         │
├─────┼────────────┼─────────┼────────────────┤
│ 1   │ PetalWidth │ 2.5     │ Iris-virginica │
│ 2   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 3   │ PetalWidth │ 1.9     │ Iris-virginica │
│ 4   │ PetalWidth │ 2.0     │ Iris-virginica │
│ 5   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 6   │ PetalWidth │ 1.8     │ Iris-virginica │
```

Note that all columns can be of different types. Type promotion follows the rules of `vcat`.

The stacked `DataFrame` that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled `:variable` and `:values` contain the column identifier and the stacked columns.

A third optional argument to `stack` represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:

```jldoctest reshape
julia> d = stack(iris, [:SepalLength, :SepalWidth], :Species);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value   │ Species     │
│     │ Cat…        │ Float64 │ String      │
├─────┼─────────────┼─────────┼─────────────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value   │ Species        │
│     │ Cat…       │ Float64 │ String         │
├─────┼────────────┼─────────┼────────────────┤
│ 1   │ SepalWidth │ 3.3     │ Iris-virginica │
│ 2   │ SepalWidth │ 3.0     │ Iris-virginica │
│ 3   │ SepalWidth │ 2.5     │ Iris-virginica │
│ 4   │ SepalWidth │ 3.0     │ Iris-virginica │
│ 5   │ SepalWidth │ 3.4     │ Iris-virginica │
│ 6   │ SepalWidth │ 3.0     │ Iris-virginica │
```

If you prefer to specify the id columns then use `Not` with `stack` like this:

```jldoctest reshape
julia> d = stack(iris, Not(:Species));

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value   │ Species     │
│     │ Cat…        │ Float64 │ String      │
├─────┼─────────────┼─────────┼─────────────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value   │ Species        │
│     │ Cat…       │ Float64 │ String         │
├─────┼────────────┼─────────┼────────────────┤
│ 1   │ PetalWidth │ 2.5     │ Iris-virginica │
│ 2   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 3   │ PetalWidth │ 1.9     │ Iris-virginica │
│ 4   │ PetalWidth │ 2.0     │ Iris-virginica │
│ 5   │ PetalWidth │ 2.3     │ Iris-virginica │
│ 6   │ PetalWidth │ 1.8     │ Iris-virginica │
```

`unstack` converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:

```jldoctest reshape
julia> iris.id = 1:size(iris, 1)
1:150

julia> longdf = stack(iris, Not([:Species, :id]));

julia> first(longdf, 6)
6×4 DataFrame
│ Row │ variable    │ value   │ Species     │ id    │
│     │ Cat…        │ Float64 │ String      │ Int64 │
├─────┼─────────────┼─────────┼─────────────┼───────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │ 1     │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │ 2     │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │ 3     │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │ 4     │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │ 5     │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │ 6     │

julia> last(longdf, 6)
6×4 DataFrame
│ Row │ variable   │ value   │ Species        │ id    │
│     │ Cat…       │ Float64 │ String         │ Int64 │
├─────┼────────────┼─────────┼────────────────┼───────┤
│ 1   │ PetalWidth │ 2.5     │ Iris-virginica │ 145   │
│ 2   │ PetalWidth │ 2.3     │ Iris-virginica │ 146   │
│ 3   │ PetalWidth │ 1.9     │ Iris-virginica │ 147   │
│ 4   │ PetalWidth │ 2.0     │ Iris-virginica │ 148   │
│ 5   │ PetalWidth │ 2.3     │ Iris-virginica │ 149   │
│ 6   │ PetalWidth │ 1.8     │ Iris-virginica │ 150   │

julia> widedf = unstack(longdf, :id, :variable, :value);

julia> first(widedf, 6)
6×5 DataFrame
│ Row │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ 1     │ 5.1         │ 3.5        │ 1.4         │ 0.2        │
│ 2   │ 2     │ 4.9         │ 3.0        │ 1.4         │ 0.2        │
│ 3   │ 3     │ 4.7         │ 3.2        │ 1.3         │ 0.2        │
│ 4   │ 4     │ 4.6         │ 3.1        │ 1.5         │ 0.2        │
│ 5   │ 5     │ 5.0         │ 3.6        │ 1.4         │ 0.2        │
│ 6   │ 6     │ 5.4         │ 3.9        │ 1.7         │ 0.4        │

julia> last(widedf, 6)
6×5 DataFrame
│ Row │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ 145   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │
│ 2   │ 146   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │
│ 3   │ 147   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │
│ 4   │ 148   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │
│ 5   │ 149   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │
│ 6   │ 150   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │
```

If the remaining columns are unique, you can skip the id variable and use:

```jldoctest reshape
julia> longdf = stack(iris, Not([:Species, :id]));

julia> first(longdf, 6)
6×4 DataFrame
│ Row │ variable    │ value   │ Species     │ id    │
│     │ Cat…        │ Float64 │ String      │ Int64 │
├─────┼─────────────┼─────────┼─────────────┼───────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │ 1     │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │ 2     │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │ 3     │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │ 4     │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │ 5     │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │ 6     │

julia> last(longdf, 6)
6×4 DataFrame
│ Row │ variable   │ value   │ Species        │ id    │
│     │ Cat…       │ Float64 │ String         │ Int64 │
├─────┼────────────┼─────────┼────────────────┼───────┤
│ 1   │ PetalWidth │ 2.5     │ Iris-virginica │ 145   │
│ 2   │ PetalWidth │ 2.3     │ Iris-virginica │ 146   │
│ 3   │ PetalWidth │ 1.9     │ Iris-virginica │ 147   │
│ 4   │ PetalWidth │ 2.0     │ Iris-virginica │ 148   │
│ 5   │ PetalWidth │ 2.3     │ Iris-virginica │ 149   │
│ 6   │ PetalWidth │ 1.8     │ Iris-virginica │ 150   │

julia> widedf = unstack(longdf, :variable, :value);

julia> first(widedf, 6)
6×6 DataFrame
│ Row │ Species     │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ String      │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼─────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ Iris-setosa │ 1     │ 5.1         │ 3.5        │ 1.4         │ 0.2        │
│ 2   │ Iris-setosa │ 2     │ 4.9         │ 3.0        │ 1.4         │ 0.2        │
│ 3   │ Iris-setosa │ 3     │ 4.7         │ 3.2        │ 1.3         │ 0.2        │
│ 4   │ Iris-setosa │ 4     │ 4.6         │ 3.1        │ 1.5         │ 0.2        │
│ 5   │ Iris-setosa │ 5     │ 5.0         │ 3.6        │ 1.4         │ 0.2        │
│ 6   │ Iris-setosa │ 6     │ 5.4         │ 3.9        │ 1.7         │ 0.4        │

julia> last(widedf, 6)
6×6 DataFrame
│ Row │ Species        │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ String         │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼────────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ Iris-virginica │ 145   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │
│ 2   │ Iris-virginica │ 146   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │
│ 3   │ Iris-virginica │ 147   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │
│ 4   │ Iris-virginica │ 148   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │
│ 5   │ Iris-virginica │ 149   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │
│ 6   │ Iris-virginica │ 150   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │
```

You can even skip passing the `:variable` and `:value` values as positional arguments, as they will be used by default, and write:
```jldoctest reshape
julia> widedf = unstack(longdf);

julia> first(widedf, 6)
6×6 DataFrame
│ Row │ Species     │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ String      │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼─────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ Iris-setosa │ 1     │ 5.1         │ 3.5        │ 1.4         │ 0.2        │
│ 2   │ Iris-setosa │ 2     │ 4.9         │ 3.0        │ 1.4         │ 0.2        │
│ 3   │ Iris-setosa │ 3     │ 4.7         │ 3.2        │ 1.3         │ 0.2        │
│ 4   │ Iris-setosa │ 4     │ 4.6         │ 3.1        │ 1.5         │ 0.2        │
│ 5   │ Iris-setosa │ 5     │ 5.0         │ 3.6        │ 1.4         │ 0.2        │
│ 6   │ Iris-setosa │ 6     │ 5.4         │ 3.9        │ 1.7         │ 0.4        │

julia> last(widedf, 6)
6×6 DataFrame
│ Row │ Species        │ id    │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │
│     │ String         │ Int64 │ Float64?    │ Float64?   │ Float64?    │ Float64?   │
├─────┼────────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ Iris-virginica │ 145   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │
│ 2   │ Iris-virginica │ 146   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │
│ 3   │ Iris-virginica │ 147   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │
│ 4   │ Iris-virginica │ 148   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │
│ 5   │ Iris-virginica │ 149   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │
│ 6   │ Iris-virginica │ 150   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │
```

Passing `view=true` to `stack` returns a data frame whose columns are views into the original wide data frame. Here is an example:

```jldoctest reshape
julia> d = stack(iris, view=true);

julia> first(d, 6)
6×4 DataFrame
│ Row │ variable    │ value │ Species     │ id    │
│     │ Cat…        │ Any   │ String      │ Int64 │
├─────┼─────────────┼───────┼─────────────┼───────┤
│ 1   │ SepalLength │ 5.1   │ Iris-setosa │ 1     │
│ 2   │ SepalLength │ 4.9   │ Iris-setosa │ 2     │
│ 3   │ SepalLength │ 4.7   │ Iris-setosa │ 3     │
│ 4   │ SepalLength │ 4.6   │ Iris-setosa │ 4     │
│ 5   │ SepalLength │ 5.0   │ Iris-setosa │ 5     │
│ 6   │ SepalLength │ 5.4   │ Iris-setosa │ 6     │

julia> last(d, 6)
6×4 DataFrame
│ Row │ variable   │ value │ Species        │ id    │
│     │ Cat…       │ Any   │ String         │ Int64 │
├─────┼────────────┼───────┼────────────────┼───────┤
│ 1   │ PetalWidth │ 2.5   │ Iris-virginica │ 145   │
│ 2   │ PetalWidth │ 2.3   │ Iris-virginica │ 146   │
│ 3   │ PetalWidth │ 1.9   │ Iris-virginica │ 147   │
│ 4   │ PetalWidth │ 2.0   │ Iris-virginica │ 148   │
│ 5   │ PetalWidth │ 2.3   │ Iris-virginica │ 149   │
│ 6   │ PetalWidth │ 1.8   │ Iris-virginica │ 150   │
```

This saves memory. To create the view, several `AbstractVector`s are defined:

`:variable` column -- `EachRepeatedVector`
This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.

`:value` column -- `StackedVector`
This is provides a view of the original columns stacked together.

Id columns -- `RepeatedVector`
This repeats the original columns N times where N is the number of columns stacked.

None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:

```jldoctest reshape
julia> using Statistics

julia> d = stack(iris, Not(:Species));

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value   │ Species     │
│     │ Cat…        │ Float64 │ String      │
├─────┼─────────────┼─────────┼─────────────┤
│ 1   │ SepalLength │ 5.1     │ Iris-setosa │
│ 2   │ SepalLength │ 4.9     │ Iris-setosa │
│ 3   │ SepalLength │ 4.7     │ Iris-setosa │
│ 4   │ SepalLength │ 4.6     │ Iris-setosa │
│ 5   │ SepalLength │ 5.0     │ Iris-setosa │
│ 6   │ SepalLength │ 5.4     │ Iris-setosa │

julia> x = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(df.value)));

julia> first(x, 6)
│ Row │ variable    │ Species         │ vsum    │
│     │ Cat…        │ String          │ Float64 │
├─────┼─────────────┼─────────────────┼─────────┤
│ 1   │ SepalLength │ Iris-setosa     │ 5.006   │
│ 2   │ SepalLength │ Iris-versicolor │ 5.936   │
│ 3   │ SepalLength │ Iris-virginica  │ 6.588   │
│ 4   │ SepalWidth  │ Iris-setosa     │ 3.418   │
│ 5   │ SepalWidth  │ Iris-versicolor │ 2.77    │
│ 6   │ SepalWidth  │ Iris-virginica  │ 2.974   │

julia> first(unstack(x, :Species, :vsum), 6)
5×4 DataFrame
│ Row │ variable    │ Iris-setosa │ Iris-versicolor │ Iris-virginica │
│     │ Cat…        │ Float64?    │ Float64?        │ Float64?       │
├─────┼─────────────┼─────────────┼─────────────────┼────────────────┤
│ 1   │ SepalLength │ 5.006       │ 5.936           │ 6.588          │
│ 2   │ SepalWidth  │ 3.418       │ 2.77            │ 2.974          │
│ 3   │ PetalLength │ 1.464       │ 4.26            │ 5.552          │
│ 4   │ PetalWidth  │ 0.244       │ 1.326           │ 2.026          │
│ 5   │ id          │ 25.5        │ 75.5            │ 125.5          │
```
