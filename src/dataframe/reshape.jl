"""
    permutecols!(df::DataFrame, p::AbstractVector)

Permute the columns of `df` in-place, according to permutation `p`.

### Examples

```julia
julia> df = DataFrame(; a=1:5, b=2:6, c=3:7)
5×3 DataFrames.DataFrame
│ Row │ a │ b │ c │
├─────┼───┼───┼───┤
│ 1   │ 1 │ 2 │ 3 │
│ 2   │ 2 │ 3 │ 4 │
│ 3   │ 3 │ 4 │ 5 │
│ 4   │ 4 │ 5 │ 6 │
│ 5   │ 5 │ 6 │ 7 │

julia> permutecols!(df, [2, 1, 3]);

julia> df
5×3 DataFrames.DataFrame
│ Row │ b │ a │ c │
├─────┼───┼───┼───┤
│ 1   │ 2 │ 1 │ 3 │
│ 2   │ 3 │ 2 │ 4 │
│ 3   │ 4 │ 3 │ 5 │
│ 4   │ 5 │ 4 │ 6 │
│ 5   │ 6 │ 5 │ 7 │

julia> permutecols!(df, [:c, :a, :b]);

julia> df
5×3 DataFrames.DataFrame
│ Row │ c │ a │ b │
├─────┼───┼───┼───┤
│ 1   │ 3 │ 1 │ 2 │
│ 2   │ 4 │ 2 │ 3 │
│ 3   │ 5 │ 3 │ 4 │
│ 4   │ 6 │ 4 │ 5 │
│ 5   │ 7 │ 5 │ 6 │
```
"""
function permutecols!(df::DataFrame, p::AbstractVector)
    length(p) == size(df, 2) && isperm(p) || error("$p is not a valid column permutation")
    permute!(DataFrames.columns(df), p)
    df.colindex = DataFrames.Index(
        Dict(names(df)[j] => i for (i, j) in enumerate(p)), [names(df)[j] for j in p]
    )
end

function permutecols!(df::DataFrame, p::AbstractVector{Symbol})
    permutecols!(df, indexin(p,names(df)))
end
