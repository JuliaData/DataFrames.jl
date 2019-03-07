# Sorting

Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling `sort!` will sort all columns, in place:

```jldoctest sort
julia> using DataFrames, CSV

julia> iris = CSV.read(joinpath(dirname(pathof(DataFrames)), "../docs/src/assets/iris.csv"));

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.1         │ 3.5        │ 1.4         │ 0.2        │ Iris-setosa │
│ 2   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa │
│ 3   │ 4.7         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │
│ 4   │ 4.6         │ 3.1        │ 1.5         │ 0.2        │ Iris-setosa │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 6.3         │ 2.5        │ 5.0         │ 1.9        │ Iris-virginica │
│ 2   │ 6.5         │ 3.0        │ 5.2         │ 2.0        │ Iris-virginica │
│ 3   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │
│ 4   │ 5.9         │ 3.0        │ 5.1         │ 1.8        │ Iris-virginica │

julia> sort!(iris);

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ Iris-setosa │
│ 2   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ Iris-setosa │
│ 3   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ Iris-setosa │
│ 4   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ Iris-virginica │
│ 2   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ Iris-virginica │
│ 3   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ Iris-virginica │
│ 4   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ Iris-virginica │
```

Observe that all columns are taken into account lexicographically when sorting the `DataFrame`.

You can also call the `sort` function to create a new `DataFrame` with freshly allocated sorted vectors.

In sorting `DataFrame`s, you may want to sort different columns with different options.
Here are some examples showing most of the possible options:

```jldoctest sort
julia> sort!(iris, rev = true);

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ Iris-virginica │
│ 2   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ Iris-virginica │
│ 3   │ 7.7         │ 3.0        │ 6.1         │ 2.3        │ Iris-virginica │
│ 4   │ 7.7         │ 2.8        │ 6.7         │ 2.0        │ Iris-virginica │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 4.4         │ 3.2        │ 1.3         │ 0.2        │ Iris-setosa │
│ 2   │ 4.4         │ 3.0        │ 1.3         │ 0.2        │ Iris-setosa │
│ 3   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ Iris-setosa │
│ 4   │ 4.3         │ 3.0        │ 1.1         │ 0.1        │ Iris-setosa │

julia> sort!(iris, [:Species, :SepalWidth]);

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 4.5         │ 2.3        │ 1.3         │ 0.3        │ Iris-setosa │
│ 2   │ 4.4         │ 2.9        │ 1.4         │ 0.2        │ Iris-setosa │
│ 3   │ 5.0         │ 3.0        │ 1.6         │ 0.2        │ Iris-setosa │
│ 4   │ 4.9         │ 3.0        │ 1.4         │ 0.2        │ Iris-setosa │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 6.2         │ 3.4        │ 5.4         │ 2.3        │ Iris-virginica │
│ 2   │ 7.2         │ 3.6        │ 6.1         │ 2.5        │ Iris-virginica │
│ 3   │ 7.9         │ 3.8        │ 6.4         │ 2.0        │ Iris-virginica │
│ 4   │ 7.7         │ 3.8        │ 6.7         │ 2.2        │ Iris-virginica │

julia> sort!(iris, (order(:Species, by=length), order(:SepalLength, rev=true)));

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.8         │ 4.0        │ 1.2         │ 0.2        │ Iris-setosa │
│ 2   │ 5.7         │ 3.8        │ 1.7         │ 0.3        │ Iris-setosa │
│ 3   │ 5.7         │ 4.4        │ 1.5         │ 0.4        │ Iris-setosa │
│ 4   │ 5.5         │ 3.5        │ 1.3         │ 0.2        │ Iris-setosa │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species         │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰         │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────────┤
│ 1   │ 5.1         │ 2.5        │ 3.0         │ 1.1        │ Iris-versicolor │
│ 2   │ 5.0         │ 2.0        │ 3.5         │ 1.0        │ Iris-versicolor │
│ 3   │ 5.0         │ 2.3        │ 3.3         │ 1.0        │ Iris-versicolor │
│ 4   │ 4.9         │ 2.4        │ 3.3         │ 1.0        │ Iris-versicolor │
```

Keywords used above include `rev` (to sort in reverse),
and `by` (to apply a function to values before comparing them).
Each keyword can either be a single value, or can be a tuple or a vector,
with values corresponding to individual columns.

As an alternative to using a vector or tuple values you can use `order` to specify
an ordering for a particular column within a set of columns.

The following two examples show two ways to sort the `iris` dataset with the same result:
`:Species` will be ordered in reverse order, and within groups, rows will be sorted by increasing `:PetalLength`:

```jldoctest sort
julia> sort!(iris, (:Species, :PetalLength), rev=(true, false));

julia> julia> sort!(iris, (:Species, :PetalLength), rev=(true, false));

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ Iris-virginica │
│ 2   │ 6.2         │ 2.8        │ 4.8         │ 1.8        │ Iris-virginica │
│ 3   │ 6.0         │ 3.0        │ 4.8         │ 1.8        │ Iris-virginica │
│ 4   │ 6.3         │ 2.7        │ 4.9         │ 1.8        │ Iris-virginica │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa │
│ 2   │ 5.1         │ 3.3        │ 1.7         │ 0.5        │ Iris-setosa │
│ 3   │ 5.1         │ 3.8        │ 1.9         │ 0.4        │ Iris-setosa │
│ 4   │ 4.8         │ 3.4        │ 1.9         │ 0.2        │ Iris-setosa │

julia> sort!(iris, (order(:Species, rev=true), :PetalLength));

julia> first(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species        │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰        │
├─────┼─────────────┼────────────┼─────────────┼────────────┼────────────────┤
│ 1   │ 4.9         │ 2.5        │ 4.5         │ 1.7        │ Iris-virginica │
│ 2   │ 6.2         │ 2.8        │ 4.8         │ 1.8        │ Iris-virginica │
│ 3   │ 6.0         │ 3.0        │ 4.8         │ 1.8        │ Iris-virginica │
│ 4   │ 6.3         │ 2.7        │ 4.9         │ 1.8        │ Iris-virginica │

julia> last(iris, 4)
4×5 DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species     │
│     │ Float64⍰    │ Float64⍰   │ Float64⍰    │ Float64⍰   │ String⍰     │
├─────┼─────────────┼────────────┼─────────────┼────────────┼─────────────┤
│ 1   │ 5.4         │ 3.9        │ 1.7         │ 0.4        │ Iris-setosa │
│ 2   │ 5.1         │ 3.3        │ 1.7         │ 0.5        │ Iris-setosa │
│ 3   │ 5.1         │ 3.8        │ 1.9         │ 0.4        │ Iris-setosa │
│ 4   │ 4.8         │ 3.4        │ 1.9         │ 0.2        │ Iris-setosa │
```
