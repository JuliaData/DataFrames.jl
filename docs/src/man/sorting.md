# Sorting

Sorting is a fundamental component of data analysis. Basic sorting is trivial: just calling `sort!` will sort all columns, in place:

```jldoctest sort
julia> using DataFrames

julia> df = DataFrame(a=repeat(1:2, 5), b=1.4:-0.3:-1.3, c=1:10)
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 1     │ 1.4     │ 1     │
│ 2   │ 2     │ 1.1     │ 2     │
│ 3   │ 1     │ 0.8     │ 3     │
│ 4   │ 2     │ 0.5     │ 4     │
│ 5   │ 1     │ 0.2     │ 5     │
│ 6   │ 2     │ -0.1    │ 6     │
│ 7   │ 1     │ -0.4    │ 7     │
│ 8   │ 2     │ -0.7    │ 8     │
│ 9   │ 1     │ -1.0    │ 9     │
│ 10  │ 2     │ -1.3    │ 10    │

julia> sort!(df)
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 1     │ -1.0    │ 9     │
│ 2   │ 1     │ -0.4    │ 7     │
│ 3   │ 1     │ 0.2     │ 5     │
│ 4   │ 1     │ 0.8     │ 3     │
│ 5   │ 1     │ 1.4     │ 1     │
│ 6   │ 2     │ -1.3    │ 10    │
│ 7   │ 2     │ -0.7    │ 8     │
│ 8   │ 2     │ -0.1    │ 6     │
│ 9   │ 2     │ 0.5     │ 4     │
│ 10  │ 2     │ 1.1     │ 2     │
```

Observe that all columns are taken into account lexicographically when sorting the `DataFrame`.

You can also call the `sort` function to create a new `DataFrame` with freshly allocated sorted vectors.

In sorting `DataFrame`s, you may want to sort different columns with different options. Here are some examples showing most of the possible options:

```jldoctest sort
julia> sort!(df, rev = true)
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 2     │ 1.1     │ 2     │
│ 2   │ 2     │ 0.5     │ 4     │
│ 3   │ 2     │ -0.1    │ 6     │
│ 4   │ 2     │ -0.7    │ 8     │
│ 5   │ 2     │ -1.3    │ 10    │
│ 6   │ 1     │ 1.4     │ 1     │
│ 7   │ 1     │ 0.8     │ 3     │
│ 8   │ 1     │ 0.2     │ 5     │
│ 9   │ 1     │ -0.4    │ 7     │
│ 10  │ 1     │ -1.0    │ 9     │

julia> sort!(df, [:a, :c])
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 1     │ 1.4     │ 1     │
│ 2   │ 1     │ 0.8     │ 3     │
│ 3   │ 1     │ 0.2     │ 5     │
│ 4   │ 1     │ -0.4    │ 7     │
│ 5   │ 1     │ -1.0    │ 9     │
│ 6   │ 2     │ 1.1     │ 2     │
│ 7   │ 2     │ 0.5     │ 4     │
│ 8   │ 2     │ -0.1    │ 6     │
│ 9   │ 2     │ -0.7    │ 8     │
│ 10  │ 2     │ -1.3    │ 10    │

julia> sort!(df, (order(:b, by=abs), order(:a, rev=true)))
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 2     │ -0.1    │ 6     │
│ 2   │ 1     │ 0.2     │ 5     │
│ 3   │ 1     │ -0.4    │ 7     │
│ 4   │ 2     │ 0.5     │ 4     │
│ 5   │ 2     │ -0.7    │ 8     │
│ 6   │ 1     │ 0.8     │ 3     │
│ 7   │ 1     │ -1.0    │ 9     │
│ 8   │ 2     │ 1.1     │ 2     │
│ 9   │ 2     │ -1.3    │ 10    │
│ 10  │ 1     │ 1.4     │ 1     │
```

Keywords used above include `rev` (to sort a column or the whole `DataFrame` in reverse), and `by` (to apply a function to rows of a column/`DataFrame`). Each keyword can either be a single value, or can be a tuple or a vector, with values corresponding to individual columns.

As an alternative to using a vector or tuple values you can use `order` to specify an ordering for a particular column within a set of columns.

The following two examples show two ways to sort the `df` dataset with the same result: `:a` will be ordered in reverse order, and within groups, rows will be sorted by increasing `:c`:

```jldoctest sort
julia> sort!(df, (:a, :c), rev=(true, false))
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 2     │ 1.1     │ 2     │
│ 2   │ 2     │ 0.5     │ 4     │
│ 3   │ 2     │ -0.1    │ 6     │
│ 4   │ 2     │ -0.7    │ 8     │
│ 5   │ 2     │ -1.3    │ 10    │
│ 6   │ 1     │ 1.4     │ 1     │
│ 7   │ 1     │ 0.8     │ 3     │
│ 8   │ 1     │ 0.2     │ 5     │
│ 9   │ 1     │ -0.4    │ 7     │
│ 10  │ 1     │ -1.0    │ 9     │

julia> sort!(df, (order(:a, rev=true), :c))
10×3 DataFrame
│ Row │ a     │ b       │ c     │
│     │ Int64 │ Float64 │ Int64 │
├─────┼───────┼─────────┼───────┤
│ 1   │ 2     │ 1.1     │ 2     │
│ 2   │ 2     │ 0.5     │ 4     │
│ 3   │ 2     │ -0.1    │ 6     │
│ 4   │ 2     │ -0.7    │ 8     │
│ 5   │ 2     │ -1.3    │ 10    │
│ 6   │ 1     │ 1.4     │ 1     │
│ 7   │ 1     │ 0.8     │ 3     │
│ 8   │ 1     │ 0.2     │ 5     │
│ 9   │ 1     │ -0.4    │ 7     │
│ 10  │ 1     │ -1.0    │ 9     │
```
