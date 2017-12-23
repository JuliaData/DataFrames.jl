# Subsets

A `DataFrame` supports many forms of indexing.

```jldoctest subsets
julia> using DataFrames

julia> df = DataFrame(A = 1:10, B = 2:2:20)
10×2 DataFrames.DataFrame
│ Row │ A  │ B  │
├─────┼────┼────┤
│ 1   │ 1  │ 2  │
│ 2   │ 2  │ 4  │
│ 3   │ 3  │ 6  │
│ 4   │ 4  │ 8  │
│ 5   │ 5  │ 10 │
│ 6   │ 6  │ 12 │
│ 7   │ 7  │ 14 │
│ 8   │ 8  │ 16 │
│ 9   │ 9  │ 18 │
│ 10  │ 10 │ 20 │

```

Referring to the first column by index or name:

```jldoctest subsets
julia> df[1]
10-element Array{Int64,1}:
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10

julia> df[:A]
10-element Array{Int64,1}:
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10

 ```

Refering to the first element of the first column:

```jldoctest subsets
julia> df[1, 1]
1

julia> df[1, :A]
1

```

Selecting a subset of rows by index and an (ordered) subset of columns by name:

```jldoctest subsets
julia> df[1:3, [:A, :B]]
3×2 DataFrames.DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ 2 │
│ 2   │ 2 │ 4 │
│ 3   │ 3 │ 6 │

julia> df[1:3, [:B, :A]]
3×2 DataFrames.DataFrame
│ Row │ B │ A │
├─────┼───┼───┤
│ 1   │ 2 │ 1 │
│ 2   │ 4 │ 2 │
│ 3   │ 6 │ 3 │

```

Selecting a subset of rows matching a condition:

```jldoctest subsets
julia> filter(row -> row[:A] > 5, df)
5×2 DataFrames.DataFrame
│ Row │ A  │ B  │
├─────┼────┼────┤
│ 1   │ 6  │ 12 │
│ 2   │ 7  │ 14 │
│ 3   │ 8  │ 16 │
│ 4   │ 9  │ 18 │
│ 5   │ 10 │ 20 │

julia> filter(row -> (row[:A] > 5) && (row[:B] < 15), df)
2×2 DataFrames.DataFrame
│ Row │ A │ B  │
├─────┼───┼────┤
│ 1   │ 6 │ 12 │
│ 2   │ 7 │ 14 │
```

`filter!` behaves the same way, but modifies the data frame in place.