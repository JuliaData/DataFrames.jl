import StatsBase: sample

"""
    sample(df[, n; replace=true, ordered=false])

Draw a random sample of `n` rows from a data frame `df` and return the result as a data frame.

# Arguments

- `replace::Bool=true`: Should sampling be performed with replacement?
- `ordered::Bool=false`: Should an ordered sample be taken?

# Example
```
julia> using RDatasets
julia> iris = dataset("datasets", "iris")
julia> srand(1)
julia> sample(iris, 5)
5×5 DataFrames.DataFrame
│ Row │ SepalLength │ SepalWidth │ PetalLength │ PetalWidth │ Species      │
│ 1   │ 5.0         │ 2.0        │ 3.5         │ 1.0        │ "versicolor" │
│ 2   │ 6.2         │ 2.9        │ 4.3         │ 1.3        │ "versicolor" │
│ 3   │ 6.7         │ 3.1        │ 4.7         │ 1.5        │ "versicolor" │
│ 4   │ 5.5         │ 2.3        │ 4.0         │ 1.3        │ "versicolor" │
│ 5   │ 5.8         │ 2.7        │ 5.1         │ 1.9        │ "virginica"  │
```
"""
function sample(df::AbstractDataFrame, n::Integer=1; replace::Bool=true, ordered::Bool=false)
    df[sample(1:size(df, 1), n, replace=replace, ordered=ordered), :]
end
