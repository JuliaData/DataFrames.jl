# Subsets

A `DataTable` supports many forms of indexing.

```julia
julia> using DataTables

julia> dt = DataTable(A = 1:10, B = 2:2:20)
10×2 DataTables.DataTable
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

```julia
julia> dt[1]
10-element NullableArrays.NullableArray{Int64,1}:
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

julia> dt[:A]
10-element NullableArrays.NullableArray{Int64,1}:
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
julia> dt[1, 1]
Nullable{Int64}(1)

julia> dt[1, :A]
Nullable{Int64}(1)
```

Selecting a subset of rows by index and an (ordered) subset of columns by name:

```julia
julia> dt[1:3, [:A, :B]]
3×2 DataTables.DataTable
│ Row │ A │ B │
├─────┼───┼───┤
│ 1   │ 1 │ 2 │
│ 2   │ 2 │ 4 │
│ 3   │ 3 │ 6 │

julia> dt[1:3, [:B, :A]]
3×2 DataTables.DataTable
│ Row │ B │ A │
├─────┼───┼───┤
│ 1   │ 2 │ 1 │
│ 2   │ 4 │ 2 │
│ 3   │ 6 │ 3 │
```
