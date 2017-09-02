# Categorical Data

Often, we have to deal with factors that take on a small number of levels:

```julia
v = ["Group A", "Group A", "Group A",
     "Group B", "Group B", "Group B"]
```

The naive encoding used in an `Array` represents every entry of this vector as a full string. In contrast, we can represent the data more efficiently by replacing the strings with indices into a small pool of levels. This is what the `CategoricalArray` type does:

```julia
cv = CategoricalArray(["Group A", "Group A", "Group A",
                       "Group B", "Group B", "Group B"])
```

`CategoricalArrays` support missing values via the `Nulls` package.

```julia
using Nulls
cv = CategoricalArray(["Group A", null, "Group A",
                       "Group B", "Group B", null])
```

In addition to representing repeated data efficiently, the `CategoricalArray` type allows us to determine efficiently the allowed levels of the variable at any time using the `levels` function (note that levels may or may not be actually used in the data):

```julia
levels(cv)
```

The `levels!` function also allows changing the order of appearance of the levels, which can be useful for display purposes or when working with ordered variables.

By default, a `CategoricalArray` is able to represent 2<sup>32</sup>differents levels. You can use less memory by calling the `compact` function:

```julia
cv = compact(cv)
```

Often, you will have factors encoded inside a DataFrame with `Array` columns instead of `CategoricalArray` columns. You can do conversion of a single column using the `categorical` function:

```julia
cv = categorical(v)
```

Or you can edit the columns of a `DataFrame` in-place using the `categorical!` function:

```julia
df = DataFrame(A = [1, 1, 1, 2, 2, 2],
               B = ["X", "X", "X", "Y", "Y", "Y"])
categorical!(df, [:A, :B])
```

Using categorical arrays is important for working with the [GLM package](https://github.com/JuliaStats/GLM.jl). When fitting regression models, `CategoricalArray` columns in the input are translated into 0/1 indicator columns in the `ModelMatrix` with one column for each of the levels of the `CategoricalArray`. This allows one to analyze categorical data efficiently.

See the [CategoricalArrays package](https://github.com/JuliaData/CategoricalArrays.jl) for more information regarding categorical arrays.
