# Categorical Data

Often, we have to deal with columns in a data frame that take on a small number
of levels:

```jldoctest categorical
julia> v = ["Group A", "Group A", "Group A", "Group B", "Group B", "Group B"]
6-element Vector{String}:
 "Group A"
 "Group A"
 "Group A"
 "Group B"
 "Group B"
 "Group B"
```

The naive encoding used in a `Vector` represents every entry of this vector as a
full string. In contrast, we can represent the data more efficiently by
replacing the strings with indices into a small pool of levels. There are two
benefits of doing this. The first is that such vectors will tend to use less
memory. The second is that they can be efficiently grouped using the `groupby`
function.

There are two common types that allow to perform level pooling:
* the `PooledVector` from PooledArrays.jl;
* the `CategoricalVector` from CategoricalArrays.jl.

The difference between `PooledVector` and `CategoricalVector` is the following:
* the `PooledVector` is used if data compression is the only objective of the
  user;
* the `CategoricalVector` is designed to additionally provide a full
  functionality for working with categorical variables, both with unordered
  (nominal variables) and ordered categories (ordinal variables) at the expense
  of allowing only `AbstractString`, `AbstractChar`, or `Number` element types
  (optionally in a union with `Missing`).

Using `CategoricalVector` is important for working with the
[GLM.jl](https://github.com/JuliaStats/GLM.jl) package. When fitting regression
models, `CategoricalVector` columns in the input are translated into 0/1 indicator
columns in the `ModelMatrix` with one column for each of the levels of the
`CategoricalVector`. This allows one to analyze categorical data efficiently.
Therefore below we show selected examples of working with CategoricalArrays.jl
package.

See the [CategoricalArrays.jl](https://github.com/JuliaData/CategoricalArrays.jl)
package for more information regarding categorical arrays and the
[PooledArrays.jl](https://github.com/JuliaData/PooledArrays.jl) for examples
of working with pooled data. Also note that in this section we discuss only
vectors because we are considering a data frame context of the data. However, in
general both packages allow to work with arrays of any dimensionality.

In order to follow the examples below you need to install the
CategoricalArrays.jl package first.

```jldoctest categorical
julia> using CategoricalArrays

julia> cv = categorical(v)
6-element CategoricalArray{String,1,UInt32}:
 "Group A"
 "Group A"
 "Group A"
 "Group B"
 "Group B"
 "Group B"
```

`CategoricalVectors`s support missing values.

```jldoctest categorical
julia> cv = categorical(["Group A", missing, "Group A",
                         "Group B", "Group B", missing])
6-element CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing
```

In addition to representing repeated data efficiently, the `CategoricalArray`
type allows us to determine efficiently the allowed levels of the variable at
any time using the `levels` function (note that levels may or may not be
actually used in the data):

```jldoctest categorical
julia> levels(cv)
2-element Vector{String}:
 "Group A"
 "Group B"
```

The `levels!` function also allows changing the order of appearance of the
levels, which can be useful for display purposes or when working with ordered
variables.

```jldoctest categorical
julia> levels!(cv, ["Group B", "Group A"])
6-element CategoricalArray{Union{Missing, String},1,UInt32}:
 "Group A"
 missing
 "Group A"
 "Group B"
 "Group B"
 missing

julia> levels(cv)
2-element Vector{String}:
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

By default, a `CategoricalVector` is able to represent ``2^{32}`` different
levels. You can use less memory by calling the `compress` function:

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

The `categorical` function additionally accepts a keyword argument `compress`
which when set to `true` is equivalent to calling `compress` on the new vector:

```jldoctest categorical
julia> cv1 = categorical(["A", "B"], compress=true)
2-element CategoricalArray{String,1,UInt8}:
 "A"
 "B"
```

If the `ordered` keyword argument is set to `true`, the resulting
`CategoricalVector` will be ordered, which means that its levels can be tested
for order (rather than throwing an error):

```jldoctest categorical
julia> cv2 = categorical(["A", "B"], ordered=true)
2-element CategoricalArray{String,1,UInt32}:
 "A"
 "B"

julia> cv1[1] < cv1[2]
ERROR: ArgumentError: Unordered CategoricalValue objects cannot be tested for order using <. Use isless instead, or call the ordered! function on the parent array to change this

julia> cv2[1] < cv2[2]
true
```

You can check if a `CategoricalVector` is ordered using the `isordered` function
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
