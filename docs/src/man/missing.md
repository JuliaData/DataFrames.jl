# Missing Data

In Julia, missing values in data are represented using the special object
`missing`, which is the single instance of the type `Missing`.

```jldoctest
julia> missing
missing

julia> typeof(missing)
Missing
```

The `Missing` type lets users create vectors and `DataFrame` columns with
missing values. Here we create a vector with a missing value and the
element-type of the returned vector is `Union{Missing, Int64}`.

```jldoctest missings
julia> x = [1, 2, missing]
3-element Vector{Union{Missing, Int64}}:
 1
 2
  missing

julia> eltype(x)
Union{Missing, Int64}

julia> Union{Missing, Int}
Union{Missing, Int64}

julia> eltype(x) == Union{Missing, Int}
true
```

`missing` values can be excluded when performing operations by using
`skipmissing`, which returns a memory-efficient iterator.

```jldoctest missings
julia> skipmissing(x)
skipmissing(Union{Missing, Int64}[1, 2, missing])
```

The output of `skipmissing` can be passed directly into functions as an
argument. For example, we can find the `sum` of all non-missing values or
`collect` the non-missing values into a new missing-free vector.

```jldoctest missings
julia> sum(skipmissing(x))
3

julia> collect(skipmissing(x))
2-element Vector{Int64}:
 1
 2
```

The function `coalesce` can be used to replace missing values with another value
(note the dot, indicating that the replacement should be applied to all entries
in `x`):

```jldoctest missings
julia> coalesce.(x, 0)
3-element Vector{Int64}:
 1
 2
 0
```

The functions [`dropmissing`](@ref) and [`dropmissing!`](@ref) can be used to
remove the rows containing `missing` values from a data frame and either create
a new `DataFrame` or mutate the original in-place respectively.

```jldoctest missings
julia> using DataFrames

julia> df = DataFrame(i=1:5,
                      x=[missing, 4, missing, 2, 1],
                      y=[missing, missing, "c", "d", "e"])
5×3 DataFrame
 Row │ i      x        y
     │ Int64  Int64?   String?
─────┼─────────────────────────
   1 │     1  missing  missing
   2 │     2        4  missing
   3 │     3  missing  c
   4 │     4        2  d
   5 │     5        1  e

julia> dropmissing(df)
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```

One can specify the column(s) in which to search for rows containing `missing`
values to be removed.

```jldoctest missings
julia> dropmissing(df, :x)
3×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     2      4  missing
   2 │     4      2  d
   3 │     5      1  e
```

By default the [`dropmissing`](@ref) and [`dropmissing!`](@ref) functions keep
the `Union{T, Missing}` element type in columns selected for row removal. To
remove the `Missing` part, if present, set the `disallowmissing` keyword
argument to `true` (it will become the default behavior in the future).

```jldoctest missings
julia> dropmissing(df, disallowmissing=true)
2×3 DataFrame
 Row │ i      x      y
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     4      2  d
   2 │     5      1  e
```

Sometimes it is useful to allow or disallow support of missing values in some
columns of a data frame. These operations are supported by the
[`allowmissing`](@ref), [`allowmissing!`](@ref), [`disallowmissing`](@ref), and
[`disallowmissing!`](@ref) functions. Here is an example:

```jldoctest missings
julia> df = DataFrame(x=1:3, y=4:6)
3×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> allowmissing!(df)
3×2 DataFrame
 Row │ x       y
     │ Int64?  Int64?
─────┼────────────────
   1 │      1       4
   2 │      2       5
   3 │      3       6
```

Now `df` allows missing values in all its columns. We can take advantage of this
fact and set some of the values in `df` to `missing`, e.g.:

```jldoctest missings
julia> df[1, 1] = missing
missing

julia> df
3×2 DataFrame
 Row │ x        y
     │ Int64?   Int64?
─────┼─────────────────
   1 │ missing       4
   2 │       2       5
   3 │       3       6
```

Note that a column selector can be passed as the second positional argument to
[`allowmissing`](@ref) and [`allowmissing!`](@ref) to restrict the change to
only some columns in our data frame.

Now let us perform the reverse operation by disallowing missing values in `df`. We
know that column `:y` does not contain missing values so we can use the
[`disallowmissing`](@ref) function passing a column selector as the second
positional argument:

```jldoctest missings
julia> disallowmissing(df, :y)
3×2 DataFrame
 Row │ x        y
     │ Int64?   Int64
─────┼────────────────
   1 │ missing      4
   2 │       2      5
   3 │       3      6
```

This operation created a new `DataFrame`. If we wanted to update the `df`
in-place the [`disallowmissing!`](@ref) function should be used.

If we tried to disallow missings in the whole data frame using
`disallowmissing(df)` we would get an error. However, it is often useful to
disallow missings in all columns that actually do not contain them but keep the
columns that have some `missing` values unchanged without having to list them
explicitly. This can be accomplished by passing the `error=false` keyword argument:

```jldoctest missings
julia> disallowmissing(df, error=false)
3×2 DataFrame
 Row │ x        y
     │ Int64?   Int64
─────┼────────────────
   1 │ missing      4
   2 │       2      5
   3 │       3      6
```

The [Missings.jl](https://github.com/JuliaData/Missings.jl) package provides a
few convenience functions to work with missing values.

One of the most commonly used is `passmissing`. It is a higher order function
that takes some function `f` as its argument and returns a new function
which returns `missing` if any of its positional arguments are `missing`
and otherwise applies the function `f` to these arguments. This functionality
is useful in combination with functions that do not support passing `missing`
values as their arguments. For example, trying `uppercase(missing)` would
produce an error, while the following works:

```jldoctest missings
julia> passmissing(uppercase)("a")
"A"

julia> passmissing(uppercase)(missing)
missing
```

The function `Missings.replace` returns an iterator which replaces `missing`
elements with another value:

```jldoctest missings
julia> using Missings

julia> Missings.replace(x, 1)
Missings.EachReplaceMissing{Vector{Union{Missing, Int64}}, Int64}(Union{Missing, Int64}[1, 2, missing], 1)

julia> collect(Missings.replace(x, 1))
3-element Vector{Int64}:
 1
 2
 1

julia> collect(Missings.replace(x, 1)) == coalesce.(x, 1)
true
```

The function `nonmissingtype` returns the element-type `T` in `Union{T, Missing}`.

```jldoctest missings
julia> eltype(x)
Union{Missing, Int64}

julia> nonmissingtype(eltype(x))
Int64
```

The `missings` function constructs `Vector`s and `Array`s supporting missing
values, using the optional first argument to specify the element-type.

```jldoctest missings
julia> missings(1)
1-element Vector{Missing}:
 missing

julia> missings(3)
3-element Vector{Missing}:
 missing
 missing
 missing

julia> missings(1, 3)
1×3 Matrix{Missing}:
 missing  missing  missing

julia> missings(Int, 1, 3)
1×3 Matrix{Union{Missing, Int64}}:
 missing  missing  missing
```

See the [Julia manual](https://docs.julialang.org/en/v1/manual/missing/) for
more information about missing values.
