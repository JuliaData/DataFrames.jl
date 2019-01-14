# Missing Data

In Julia, missing values in data are represented using the special object `missing`, which is the single instance of the type `Missing`.

```jldoctest
julia> missing
missing

julia> typeof(missing)
Missing

```

The `Missing` type lets users create `Vector`s and `DataFrame` columns with missing values. Here we create a vector with a missing value and the element-type of the returned vector is `Union{Missing, Int64}`.

```jldoctest missings
julia> x = [1, 2, missing]
3-element Array{Union{Missing, Int64},1}:
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

`missing` values can be excluded when performing operations by using `skipmissing`, which returns a memory-efficient iterator.

```jldoctest missings
julia> skipmissing(x)
Base.SkipMissing{Array{Union{Missing, Int64},1}}(Union{Missing, Int64}[1, 2, missing])

```

The output of `skipmissing` can be passed directly into functions as an argument. For example, we can find the `sum` of all non-missing values or `collect` the non-missing values into a new missing-free vector.

```jldoctest missings
julia> sum(skipmissing(x))
3

julia> collect(skipmissing(x))
2-element Array{Int64,1}:
 1
 2

```

The function `coalesce` can be used to replace missing values with another value (note the dot, indicating that the replacement should be applied to all entries in `x`):

```jldoctest missings
julia> coalesce.(x, 0)
3-element Array{Int64,1}:
 1
 2
 0

```

The functions `dropmissing` and `dropmissing!` can be used to remove the rows containing `missing` values from a `DataFrame` and either create a new `DataFrame` or mutate the original in-place respectively.

```jldoctest missings
julia> df = DataFrame(i = 1:5,
                      x = [missing, 4, missing, 2, 1],
                      y = [missing, missing, "c", "d", "e"])

julia> dropmissing(df)
2×3 DataFrame
│ Row │ i     │ x      │ y       │
│     │ Int64 │ Int64⍰ │ String⍰ │
├─────┼───────┼────────┼─────────┤
│ 1   │ 4     │ 2      │ d       │
│ 2   │ 5     │ 1      │ e       │
```

One can specify the column(s) in which to search for rows containing `missing` values to be removed.

```jldoctest missings
julia> dropmissing(df, :x)
3×3 DataFrame
│ Row │ i     │ x      │ y       │
│     │ Int64 │ Int64⍰ │ String⍰ │
├─────┼───────┼────────┼─────────┤
│ 1   │ 2     │ 4      │ missing │
│ 2   │ 4     │ 2      │ d       │
│ 3   │ 5     │ 1      │ e       │
```

By default the `dropmissing` and `dropmissing!` functions keep the `Union{T,Missing}` element type in columns selected for row removal. To remove the `Missing` part, if present, set the `disallowmissing` option to `true` (it will become the default behavior in the future).

```jldoctest missings
julia> dropmissing(df, disallowmissing=true)
2×3 DataFrame
│ Row │ i     │ x     │ y      │
│     │ Int64 │ Int64 │ String │
├─────┼───────┼───────┼────────┤
│ 1   │ 4     │ 2     │ d      │
│ 2   │ 5     │ 1     │ e      │
```


The [Missings.jl](https://github.com/JuliaData/Missings.jl) package provides a few convenience functions to work with missing values.

The function `Missings.replace` returns an iterator which replaces `missing` elements with another value:

```jldoctest missings
julia> using Missings

julia> Missings.replace(x, 1)
Missings.EachReplaceMissing{Array{Union{Missing, Int64},1},Int64}(Union{Missing, Int64}[1, 2, missing], 1)

julia> collect(Missings.replace(x, 1))
3-element Array{Int64,1}:
 1
 2
 1

julia> collect(Missings.replace(x, 1)) == coalesce.(x, 1)
true

```

The function `Missings.T` returns the element-type `T` in `Union{T, Missing}`.

```jldoctest missings
julia> eltype(x)
Union{Int64, Missing}

julia> Missings.T(eltype(x))
Int64

```

The `missings` function constructs `Vector`s and `Array`s supporting missing values, using the optional first argument to specify the element-type.

```jldoctest missings
julia> missings(1)
1-element Array{Missing,1}:
 missing

julia> missings(3)
3-element Array{Missing,1}:
 missing
 missing
 missing

julia> missings(1, 3)
1×3 Array{Missing,2}:
 missing  missing  missing

julia> missings(Int, 1, 3)
1×3 Array{Union{Missing, Int64},2}:
 missing  missing  missing

```

See the [Julia manual](https://docs.julialang.org/en/v1/manual/missing/) for more information about missing values.
