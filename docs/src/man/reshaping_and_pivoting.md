# Reshaping and Pivoting Data

Reshape data from wide to long format using the `stack` function:

```jldoctest reshape
julia> using DataFrames, CSV

julia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv"));

julia> first(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ setosa        │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ setosa        │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ setosa        │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │
│ 5   │ 5.0         │ 3.6        │ 1.4         │ 0.2        │ setosa        │
│ 6   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │

julia> last(iris, 6)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 6.7         │ 3.3        │ 5.7         │ 2.5        │ virginica     │
│ 2   │ 6.7         │ 3.0        │ 5.2         │ 2.3        │ virginica     │
│ 3   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ virginica     │
│ 4   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ virginica     │
│ 5   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ virginica     │
│ 6   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ virginica     │

julia> d = stack(iris, 1:4);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value    │ Species       │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │
├─────┼─────────────┼──────────┼───────────────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │
│ 2   │ SepalLength │ 4.9      │ setosa        │
│ 3   │ SepalLength │ 4.7      │ setosa        │
│ 4   │ SepalLength │ 4.6      │ setosa        │
│ 5   │ SepalLength │ 5.0      │ setosa        │
│ 6   │ SepalLength │ 5.4      │ setosa        │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value    │ Species       │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │
├─────┼────────────┼──────────┼───────────────┤
│ 1   │ PetalWidth │ 2.5      │ virginica     │
│ 2   │ PetalWidth │ 2.3      │ virginica     │
│ 3   │ PetalWidth │ 1.9      │ virginica     │
│ 4   │ PetalWidth │ 2.0      │ virginica     │
│ 5   │ PetalWidth │ 2.3      │ virginica     │
│ 6   │ PetalWidth │ 1.8      │ virginica     │
```

The second optional argument to `stack` indicates the columns to be stacked. These are normally referred to as the measured variables. Column names can also be given:

```jldoctest reshape
julia> d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth]);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value    │ Species       │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │
├─────┼─────────────┼──────────┼───────────────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │
│ 2   │ SepalLength │ 4.9      │ setosa        │
│ 3   │ SepalLength │ 4.7      │ setosa        │
│ 4   │ SepalLength │ 4.6      │ setosa        │
│ 5   │ SepalLength │ 5.0      │ setosa        │
│ 6   │ SepalLength │ 5.4      │ setosa        │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value    │ Species       │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │
├─────┼────────────┼──────────┼───────────────┤
│ 1   │ PetalWidth │ 2.5      │ virginica     │
│ 2   │ PetalWidth │ 2.3      │ virginica     │
│ 3   │ PetalWidth │ 1.9      │ virginica     │
│ 4   │ PetalWidth │ 2.0      │ virginica     │
│ 5   │ PetalWidth │ 2.3      │ virginica     │
│ 6   │ PetalWidth │ 1.8      │ virginica     │

```

Note that all columns can be of different types. Type promotion follows the rules of `vcat`.

The stacked `DataFrame` that results includes all of the columns not specified to be stacked. These are repeated for each stacked column. These are normally refered to as identifier (id) columns. In addition to the id columns, two additional columns labeled `:variable` and `:values` contain the column identifier and the stacked columns.

A third optional argument to `stack` represents the id columns that are repeated. This makes it easier to specify which variables you want included in the long format:

```jldoctest reshape
julia> d = stack(iris, [:SepalLength, :SepalWidth], :Species);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value    │ Species       │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │
├─────┼─────────────┼──────────┼───────────────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │
│ 2   │ SepalLength │ 4.9      │ setosa        │
│ 3   │ SepalLength │ 4.7      │ setosa        │
│ 4   │ SepalLength │ 4.6      │ setosa        │
│ 5   │ SepalLength │ 5.0      │ setosa        │
│ 6   │ SepalLength │ 5.4      │ setosa        │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value    │ Species       │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │
├─────┼────────────┼──────────┼───────────────┤
│ 1   │ SepalWidth │ 3.3      │ virginica     │
│ 2   │ SepalWidth │ 3.0      │ virginica     │
│ 3   │ SepalWidth │ 2.5      │ virginica     │
│ 4   │ SepalWidth │ 3.0      │ virginica     │
│ 5   │ SepalWidth │ 3.4      │ virginica     │
│ 6   │ SepalWidth │ 3.0      │ virginica     │
```

`melt` is an alternative function to reshape from wide to long format. It is based on `stack`, but it prefers specification of the id columns as:

```jldoctest reshape
julia> d = melt(iris, :Species);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value    │ Species       │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │
├─────┼─────────────┼──────────┼───────────────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │
│ 2   │ SepalLength │ 4.9      │ setosa        │
│ 3   │ SepalLength │ 4.7      │ setosa        │
│ 4   │ SepalLength │ 4.6      │ setosa        │
│ 5   │ SepalLength │ 5.0      │ setosa        │
│ 6   │ SepalLength │ 5.4      │ setosa        │

julia> last(d, 6)
6×3 DataFrame
│ Row │ variable   │ value    │ Species       │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │
├─────┼────────────┼──────────┼───────────────┤
│ 1   │ PetalWidth │ 2.5      │ virginica     │
│ 2   │ PetalWidth │ 2.3      │ virginica     │
│ 3   │ PetalWidth │ 1.9      │ virginica     │
│ 4   │ PetalWidth │ 2.0      │ virginica     │
│ 5   │ PetalWidth │ 2.3      │ virginica     │
│ 6   │ PetalWidth │ 1.8      │ virginica     │
```

`unstack` converts from a long format to a wide format. The default is requires specifying which columns are an id variable, column variable names, and column values:

```jldoctest reshape
julia> iris.id = 1:size(iris, 1)
1:150

julia> longdf = melt(iris, [:Species, :id]);

julia> first(longdf, 6)
6×4 DataFrame
│ Row │ variable    │ value    │ Species       │ id    │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │
├─────┼─────────────┼──────────┼───────────────┼───────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │
│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │
│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │
│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │
│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │
│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │

julia> last(longdf, 6)
6×4 DataFrame
│ Row │ variable   │ value    │ Species       │ id    │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │ Int64 │
├─────┼────────────┼──────────┼───────────────┼───────┤
│ 1   │ PetalWidth │ 2.5      │ virginica     │ 145   │
│ 2   │ PetalWidth │ 2.3      │ virginica     │ 146   │
│ 3   │ PetalWidth │ 1.9      │ virginica     │ 147   │
│ 4   │ PetalWidth │ 2.0      │ virginica     │ 148   │
│ 5   │ PetalWidth │ 2.3      │ virginica     │ 149   │
│ 6   │ PetalWidth │ 1.8      │ virginica     │ 150   │

julia> widedf = unstack(longdf, :id, :variable, :value);

julia> first(widedf, 6)
6×5 DataFrame
│ Row │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │
│     │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │
├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ 1     │ 1.4         │ 0.2        │ 5.1         │ 3.5        │
│ 2   │ 2     │ 1.4         │ 0.2        │ 4.9         │ 3.0        │
│ 3   │ 3     │ 1.3         │ 0.2        │ 4.7         │ 3.2        │
│ 4   │ 4     │ 1.5         │ 0.2        │ 4.6         │ 3.1        │
│ 5   │ 5     │ 1.4         │ 0.2        │ 5.0         │ 3.6        │
│ 6   │ 6     │ 1.7         │ 0.4        │ 5.4         │ 3.9        │

julia> last(widedf, 6)
6×5 DataFrame
│ Row │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │
│     │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │
├─────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ 145   │ 5.7         │ 2.5        │ 6.7         │ 3.3        │
│ 2   │ 146   │ 5.2         │ 2.3        │ 6.7         │ 3.0        │
│ 3   │ 147   │ 5.0         │ 1.9        │ 6.3         │ 2.5        │
│ 4   │ 148   │ 5.2         │ 2.0        │ 6.5         │ 3.0        │
│ 5   │ 149   │ 5.4         │ 2.3        │ 6.2         │ 3.4        │
│ 6   │ 150   │ 5.1         │ 1.8        │ 5.9         │ 3.0        │
```

If the remaining columns are unique, you can skip the id variable and use:

```jldoctest reshape
julia> longdf = melt(iris, [:Species, :id]);

julia> first(longdf, 6)
6×4 DataFrame
│ Row │ variable    │ value    │ Species       │ id    │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │
├─────┼─────────────┼──────────┼───────────────┼───────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │
│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │
│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │
│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │
│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │
│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │

julia> widedf = unstack(longdf, :variable, :value);

julia> first(widedf, 6)
6×6 DataFrame
│ Row │ Species       │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │
│     │ Categorical…⍰ │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │
├─────┼───────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ setosa        │ 1     │ 1.4         │ 0.2        │ 5.1         │ 3.5        │
│ 2   │ setosa        │ 2     │ 1.4         │ 0.2        │ 4.9         │ 3.0        │
│ 3   │ setosa        │ 3     │ 1.3         │ 0.2        │ 4.7         │ 3.2        │
│ 4   │ setosa        │ 4     │ 1.5         │ 0.2        │ 4.6         │ 3.1        │
│ 5   │ setosa        │ 5     │ 1.4         │ 0.2        │ 5.0         │ 3.6        │
│ 6   │ setosa        │ 6     │ 1.7         │ 0.4        │ 5.4         │ 3.9        │
```

You can even skip passing the `:variable` and `:value` values as positional arguments, as they will be used by default, and write:
```jldoctest reshape
julia> widedf = unstack(longdf);

julia> first(widedf, 6)
6×6 DataFrame
│ Row │ Species       │ id    │ PetalLength │ PetalWidth │ SepalLength │ SepalWidth │
│     │ Categorical…⍰ │ Int64 │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │
├─────┼───────────────┼───────┼─────────────┼────────────┼─────────────┼────────────┤
│ 1   │ setosa        │ 1     │ 1.4         │ 0.2        │ 5.1         │ 3.5        │
│ 2   │ setosa        │ 2     │ 1.4         │ 0.2        │ 4.9         │ 3.0        │
│ 3   │ setosa        │ 3     │ 1.3         │ 0.2        │ 4.7         │ 3.2        │
│ 4   │ setosa        │ 4     │ 1.5         │ 0.2        │ 4.6         │ 3.1        │
│ 5   │ setosa        │ 5     │ 1.4         │ 0.2        │ 5.0         │ 3.6        │
│ 6   │ setosa        │ 6     │ 1.7         │ 0.4        │ 5.4         │ 3.9        │
```

`stackdf` and `meltdf` are two additional functions that work like `stack` and `melt`, but they provide a view into the original wide DataFrame. Here is an example:

```jldoctest reshape
julia> d = stackdf(iris);

julia> first(d, 6)
6×4 DataFrame
│ Row │ variable    │ value    │ Species       │ id    │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │ Int64 │
├─────┼─────────────┼──────────┼───────────────┼───────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │ 1     │
│ 2   │ SepalLength │ 4.9      │ setosa        │ 2     │
│ 3   │ SepalLength │ 4.7      │ setosa        │ 3     │
│ 4   │ SepalLength │ 4.6      │ setosa        │ 4     │
│ 5   │ SepalLength │ 5.0      │ setosa        │ 5     │
│ 6   │ SepalLength │ 5.4      │ setosa        │ 6     │

julia> last(d, 6)
6×4 DataFrame
│ Row │ variable   │ value    │ Species       │ id    │
│     │ Symbol     │ Float64⍰ │ Categorical…⍰ │ Int64 │
├─────┼────────────┼──────────┼───────────────┼───────┤
│ 1   │ PetalWidth │ 2.5      │ virginica     │ 145   │
│ 2   │ PetalWidth │ 2.3      │ virginica     │ 146   │
│ 3   │ PetalWidth │ 1.9      │ virginica     │ 147   │
│ 4   │ PetalWidth │ 2.0      │ virginica     │ 148   │
│ 5   │ PetalWidth │ 2.3      │ virginica     │ 149   │
│ 6   │ PetalWidth │ 1.8      │ virginica     │ 150   │
```

This saves memory. To create the view, several AbstractVectors are defined:

`:variable` column -- `EachRepeatedVector`
This repeats the variables N times where N is the number of rows of the original AbstractDataFrame.

`:value` column -- `StackedVector`
This is provides a view of the original columns stacked together.

Id columns -- `RepeatedVector`
This repeats the original columns N times where N is the number of columns stacked.

None of these reshaping functions perform any aggregation. To do aggregation, use the split-apply-combine functions in combination with reshaping. Here is an example:

```jldoctest reshape
julia> d = melt(iris, :Species);

julia> first(d, 6)
6×3 DataFrame
│ Row │ variable    │ value    │ Species       │
│     │ Symbol      │ Float64⍰ │ Categorical…⍰ │
├─────┼─────────────┼──────────┼───────────────┤
│ 1   │ SepalLength │ 5.1      │ setosa        │
│ 2   │ SepalLength │ 4.9      │ setosa        │
│ 3   │ SepalLength │ 4.7      │ setosa        │
│ 4   │ SepalLength │ 4.6      │ setosa        │
│ 5   │ SepalLength │ 5.0      │ setosa        │
│ 6   │ SepalLength │ 5.4      │ setosa        │

julia> x = by(d, [:variable, :Species], df -> DataFrame(vsum = mean(df.value)));

julia> first(x, 6)

6×3 DataFrame
│ Row │ variable    │ Species       │ vsum    │
│     │ Symbol      │ Categorical…⍰ │ Float64 │
├─────┼─────────────┼───────────────┼─────────┤
│ 1   │ SepalLength │ setosa        │ 5.006   │
│ 2   │ SepalLength │ versicolor    │ 5.936   │
│ 3   │ SepalLength │ virginica     │ 6.588   │
│ 4   │ SepalWidth  │ setosa        │ 3.428   │
│ 5   │ SepalWidth  │ versicolor    │ 2.77    │
│ 6   │ SepalWidth  │ virginica     │ 2.974   │

julia> first(unstack(x, :Species, :vsum), 6)
5×4 DataFrame
│ Row │ variable    │ setosa   │ versicolor │ virginica │
│     │ Symbol      │ Float64⍰ │ Float64⍰   │ Float64⍰  │
├─────┼─────────────┼──────────┼────────────┼───────────┤
│ 1   │ PetalLength │ 1.462    │ 4.26       │ 5.552     │
│ 2   │ PetalWidth  │ 0.246    │ 1.326      │ 2.026     │
│ 3   │ SepalLength │ 5.006    │ 5.936      │ 6.588     │
│ 4   │ SepalWidth  │ 3.428    │ 2.77       │ 2.974     │
│ 5   │ id          │ 25.5     │ 75.5       │ 125.5     │
```
