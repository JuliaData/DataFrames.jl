# Categorical Data

Often, we have to deal with factors that take on a small number of levels:

```jldoctest categorical
julia> v = ["Group A", "Group A", "Group A", "Group B", "Group B", "Group B"]
6-element Array{String,1}:
 "Group A"
 "Group A"
 "Group A"
 "Group B"
 "Group B"
 "Group B"

```

The naive encoding used in an `Array` represents every entry of this vector as a full string. In contrast, we can represent the data more efficiently by replacing the strings with indices into a small pool of levels. This is what the `CategoricalArray` type does:

```jldoctest categorical
julia> using CategoricalArrays

julia> cv = CategoricalArray(v)
6-element CategoricalArrays.CategoricalArray{String,1,UInt32}:
 "Group A"
 "Group A"
 "Group A"
 "Group B"
 "Group B"
 "Group B"

```

`CategoricalArrays` support missing values via the `Missings` package.

```jldoctest categorical
julia> using Missings

julia> cv = CategoricalArray(["Group A", missing, "Group A",
                              "Group B", "Group B", missing])
6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing
```

In addition to representing repeated data efficiently, the `CategoricalArray` type allows us to determine efficiently the allowed levels of the variable at any time using the `levels` function (note that levels may or may not be actually used in the data):

```jldoctest categorical
julia> levels(cv)
2-element Array{String,1}:
 "Group A"
 "Group B"

```

The `levels!` function also allows changing the order of appearance of the levels, which can be useful for display purposes or when working with ordered variables.

```jldoctest categorical
julia> levels!(cv, ["Group B", "Group A"]);

julia> levels(cv)
2-element Array{String,1}:
 "Group B"
 "Group A"

julia> sort(cv)
6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group B"
 "Group B"
 "Group A"
 "Group A"
 missing
 missing

```

By default, a `CategoricalArray` is able to represent 2<sup>32</sup>differents levels. You can use less memory by calling the `compress` function:

```jldoctest categorical
julia> cv = compress(cv)
6-element CategoricalArrays.CategoricalArray{Union{Missing, String},1,UInt8}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing

```

Often, you will have factors encoded inside a DataFrame with `Array` columns instead of
`CategoricalArray` columns. You can convert one or more columns of the DataFrame using the
`categorical!` function, which modifies the input DataFrame in-place.

```jldoctest categorical
julia> using DataFrames

julia> df = DataFrame(A = ["A", "B", "C", "D", "D", "A"],
                      B = ["X", "X", "X", "Y", "Y", "Y"])
6×2 DataFrame
│ Row │ A      │ B      │
│     │ String │ String │
├─────┼────────┼────────┤
│ 1   │ A      │ X      │
│ 2   │ B      │ X      │
│ 3   │ C      │ X      │
│ 4   │ D      │ Y      │
│ 5   │ D      │ Y      │
│ 6   │ A      │ Y      │

julia> eltypes(df)
2-element Array{Type,1}:
 String
 String

julia> categorical!(df, :A) # change the column `:A` to be categorical
6×2 DataFrame
│ Row │ A            │ B      │
│     │ Categorical… │ String │
├─────┼──────────────┼────────┤
│ 1   │ A            │ X      │
│ 2   │ B            │ X      │
│ 3   │ C            │ X      │
│ 4   │ D            │ Y      │
│ 5   │ D            │ Y      │
│ 6   │ A            │ Y      │

julia> eltypes(df)
2-element Array{Type,1}:
 CategoricalArrays.CategoricalString{UInt32}
 String

julia> categorical!(df) # change all columns to be categorical
6×2 DataFrame
│ Row │ A            │ B            │
│     │ Categorical… │ Categorical… │
├─────┼──────────────┼──────────────┤
│ 1   │ A            │ X            │
│ 2   │ B            │ X            │
│ 3   │ C            │ X            │
│ 4   │ D            │ Y            │
│ 5   │ D            │ Y            │
│ 6   │ A            │ Y            │

julia> eltypes(df)
2-element Array{Type,1}:
 CategoricalArrays.CategoricalString{UInt32}
 CategoricalArrays.CategoricalString{UInt32}

```

Using categorical arrays is important for working with the [GLM package](https://github.com/JuliaStats/GLM.jl). When fitting regression models, `CategoricalArray` columns in the input are translated into 0/1 indicator columns in the `ModelMatrix` with one column for each of the levels of the `CategoricalArray`. This allows one to analyze categorical data efficiently.

See the [CategoricalArrays package](https://github.com/JuliaData/CategoricalArrays.jl) for more information regarding categorical arrays.
