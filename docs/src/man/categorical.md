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

The naive encoding used in an `Array` represents every entry of this vector as a full string.
In contrast, we can represent the data more efficiently by replacing the strings with indices
into a small pool of levels. This is what the `CategoricalArray` type does:

```jldoctest categorical
julia> using CategoricalArrays

julia> cv = CategoricalArray(v)
6-element CategoricalArray{String,1,UInt32}:
 "Group A"
 "Group A"
 "Group A"
 "Group B"
 "Group B"
 "Group B"

```

`CategoricalArrays` support missing values.

```jldoctest categorical
julia> cv = CategoricalArray(["Group A", missing, "Group A",
                              "Group B", "Group B", missing])
6-element CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing
```

In addition to representing repeated data efficiently, the `CategoricalArray` type
allows us to determine efficiently the allowed levels of the variable at any time using
the `levels` function (note that levels may or may not be actually used in the data):

```jldoctest categorical
julia> levels(cv)
2-element Array{String,1}:
 "Group A"
 "Group B"

```

The `levels!` function also allows changing the order of appearance of the levels,
which can be useful for display purposes or when working with ordered variables.

```jldoctest categorical
julia> levels!(cv, ["Group B", "Group A"]);

julia> levels(cv)
2-element Array{String,1}:
 "Group B"
 "Group A"

julia> sort(cv)
6-element CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group B"
 "Group B"
 "Group A"
 "Group A"
 missing
 missing

```

By default, a `CategoricalArray` is able to represent 2<sup>32</sup> different levels.
You can use less memory by calling the `compress` function:

```jldoctest categorical
julia> cv = compress(cv)
6-element CategoricalArray{Union{Missing, String},1,UInt8}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing

```

Instead of using the `CategoricalArray` constructor directly you can use `categorical`
function. It additionally accepts one positional argument `compress` which when set to `true`
is equivalent to calling `compress` on the new vector:
```jldoctest categorical
julia> cv1 = categorical(["A", "B"], true)
2-element CategoricalArray{String,1,UInt8}:
 "A"
 "B"
```

If the `ordered` keyword argument is set to `true`, the resulting `CategoricalArray` will be
ordered, which means that its levels can be tested for order (rather than throwing an error):
```jldoctest categorical
julia> cv2 = categorical(["A", "B"], true, ordered=true)
2-element CategoricalArray{String,1,UInt8}:
 "A"
 "B"

julia> cv1[1] < cv1[2]
ERROR: ArgumentError: Unordered CategoricalValue objects cannot be tested for order using <. Use isless instead, or call the ordered! function on the parent array to change this

julia> cv2[1] < cv2[2]
true
```

You can check if a `CategoricalArray` is ordered using the `isordered` function
and change between ordered and unordered using `ordered!` function.

```jldoctest categorical
julia> isordered(cv1)
false

julia> ordered!(cv1, true)
2-element CategoricalArray{String,1,UInt8}:
 "A"
 "B"

julia> isordered(cv1)
true

julia> cv1[1] < cv1[2]
true
```

Often, you will have factors encoded inside a `DataFrame` with `Vector` columns instead
of `CategoricalVector` columns. You can convert one or more columns of the `DataFrame`
using the `categorical!` function, which modifies the input `DataFrame` in-place.
Compression can be applied by setting the `compress` keyword argument to `true`.

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
2-element Array{DataType,1}:
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
2-element Array{DataType,1}:
 CategoricalString{UInt32}
 String
```

If columns are not specified, all columns with an `AbstractString` element type
are converted to be categorical. In the example below we also enable compression:

```jldoctest categorical
julia> categorical!(df, compress=true)
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
2-element Array{DataType,1}:
 CategoricalString{UInt8}
 CategoricalString{UInt8}

```

Using categorical arrays is important for working with the [GLM package](https://github.com/JuliaStats/GLM.jl).
When fitting regression models, `CategoricalVector` columns in the input are translated
into 0/1 indicator columns in the `ModelMatrix` with one column for each of the levels of
the `CategoricalVector`. This allows one to analyze categorical data efficiently.

See the [CategoricalArrays package](https://github.com/JuliaData/CategoricalArrays.jl)
for more information regarding categorical arrays.
