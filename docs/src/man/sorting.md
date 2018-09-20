# Sorting

Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling `sort!` will sort all columns, in place:

```jldoctest sort
julia> using DataFrames, CSV

julia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), "../test/data/iris.csv"));

julia> sort!(iris);

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa        │
│ 2   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa        │
│ 3   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa        │
│ 4   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa        │
│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │
│ 6   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica     │
│ 2   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica     │
│ 3   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica     │
│ 4   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica     │
│ 5   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica     │
│ 6   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica     │
```

In Sorting `DataFrame`s, you may want to sort different columns with different options. Here are some examples showing most of the possible options:

```jldoctest sort
julia> sort!(iris, rev = true);

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ virginica     │
│ 2   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ virginica     │
│ 3   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ virginica     │
│ 4   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ virginica     │
│ 5   │ 7.7         │ 2.6        │ 6.9         │ 2.3        │ virginica     │
│ 6   │ 7.6         │ 3.0        │ 6.6         │ 2.1        │ virginica     │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ setosa        │
│ 2   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │
│ 3   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ setosa        │
│ 4   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ setosa        │
│ 5   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ setosa        │
│ 6   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ setosa        │

julia> sort!(iris, (:SepalWidth, :SepalLength));

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.0         │ 2.0        │ 3.5         │ 1.0        │ versicolor    │
│ 2   │ 6.0         │ 2.2        │ 5.0         │ 1.5        │ virginica     │
│ 3   │ 6.0         │ 2.2        │ 4.0         │ 1.0        │ versicolor    │
│ 4   │ 6.2         │ 2.2        │ 4.5         │ 1.5        │ versicolor    │
│ 5   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ setosa        │
│ 6   │ 5.0         │ 2.3        │ 3.3         │ 1.0        │ versicolor    │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ setosa        │
│ 2   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │
│ 3   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │
│ 4   │ 5.2         │ 4.1        │ 1.5         │ 0.1        │ setosa        │
│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │
│ 6   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │

julia> sort!(iris, (order(:Species, by = uppercase),
                    order(:SepalLength, rev = true)));

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │
│ 2   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │
│ 3   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │
│ 4   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │
│ 5   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │
│ 6   │ 5.4         │ 3.4        │ 1.7         │ 0.2        │ setosa        │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 2   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 3   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │
│ 4   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │
│ 5   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │
│ 6   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │
```

Keywords used above include `rev` (to sort a column or the whole `DataFrame` in reverse), and `by` (to apply a function to a column/`DataFrame`). Each keyword can either be a single value, or can be a tuple or array, with values corresponding to individual columns.

As an alternative to using array or tuple values, `order` to specify an ordering for a particular column within a set of columns

The following two examples show two ways to sort the `iris` dataset with the same result: `Species` will be ordered in reverse lexicographic order, and within species, rows will be sorted by increasing sepal length and width:

```jldoctest sort
julia> sort!(iris, (:Species, :SepalLength, :SepalWidth),
                    rev = (true, false, false));

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │
│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │
│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │
│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │
│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │
│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │
│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │
│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │
│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │

julia> sort!(iris, (order(:Species, rev = true), :SepalLength, :SepalWidth));

julia> head(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ virginica     │
│ 2   │ 5.6         │ 2.8        │ 4.9         │ 2.0        │ virginica     │
│ 3   │ 5.7         │ 2.5        │ 5.0         │ 2.0        │ virginica     │
│ 4   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ virginica     │
│ 6   │ 5.8         │ 2.8        │ 5.1         │ 2.4        │ virginica     │

julia> tail(iris)
6×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species       │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ Categorical…⍰ │
├─────┼─────────────┼────────────┼─────────────┼────────────┼───────────────┤
│ 1   │ 5.4         │ 3.9        │ 1.3         │ 0.4        │ setosa        │
│ 2   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ setosa        │
│ 3   │ 5.5         │ 4.2        │ 1.4         │ 0.2        │ setosa        │
│ 4   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ setosa        │
│ 5   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ setosa        │
│ 6   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ setosa        │
```
