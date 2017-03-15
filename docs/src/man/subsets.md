# Subsets

## DataArrays

The `DataArray` type is meant to behave like a standard Julia `Array` and tries to implement identical indexing rules:

One dimensional `DataArray`:

```julia
julia> using DataArrays

julia> dv = data([1, 2, 3])
3-element DataArray{Int64,1}:
 1
 2
 3

julia> dv[1]
1

julia> dv[2] = NA
NA

julia> dv[2]
NA
```

Two dimensional `DataArray`:

```julia
julia> using DataArrays

julia> dm = data([1 2; 3 4])
2×2 DataArray{Int64,2}:
 1  2
 3  4

julia> dm[1, 1]
1

julia> dm[2, 1] = NA
NA

julia> dm[2, 1]
NA
```

DataFrames

In contrast, a `DataFrame` offers substantially more forms of indexing because columns can be referred to by name:

```julia
julia> using DataFrames

julia> df = DataFrame(A = 1:10, B = 2:2:20)
10×2 DataFrame
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

Refering to the first column by index or name:

```julia
julia> df[1]
10-element DataArray{Int64,1}:
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
10-element DataArray{Int64,1}:
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

```julia
julia> df[1, 1]
1

julia> df[1, :A]
1
```

Selecting a subset of rows by index and an (ordered) subset of columns by name:

```julia
julia> df[1:3, [:A, :B]]
3×2 DataFrame
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ 2 │
│ 2   │ 2 │ 4 │
│ 3   │ 3 │ 6 │

julia> df[1:3, [:B, :A]]
3×2 DataFrame
│ Row │ B │ A │
├─────┼───┼───┤
│ 1   │ 2 │ 1 │
│ 2   │ 4 │ 2 │
│ 3   │ 6 │ 3 │
```

Selecting a subset of rows by using a condition:

```julia
julia> df[df[:A] % 2 .== 0, :]
5×2 DataFrame
│ Row │ A  │ B  │
├─────┼────┼────┤
│ 1   │ 2  │ 4  │
│ 2   │ 4  │ 8  │
│ 3   │ 6  │ 12 │
│ 4   │ 8  │ 16 │
│ 5   │ 10 │ 20 │

julia> df[df[:B] % 2 .== 0, :]
10×2 DataFrame
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
