# The `Missing` Type

`Missing` is a type implemented by the [Missings.jl](https://github.com/JuliaData/Missings.jl) package to represent missing data. `missing` is an instance of the type `Missing` used to represent a missing value.

```jldoctest missings
julia> using DataFrames

julia> missing
missing

julia> typeof(missing)
Missings.Missing

```

The `Missing` type lets users create `Vector`s and `DataFrame` columns with missing values. Here we create a vector with a missing value and the element-type of the returned vector is `Union{Missings.Missing, Int64}`.

```jldoctest missings
julia> x = [1, 2, missing]
3-element Array{Union{Missings.Missing, Int64},1}:
 1
 2
  missing

julia> eltype(x)
Union{Missings.Missing, Int64}

julia> Union{Missing, Int}
Union{Missings.Missing, Int64}

julia> eltype(x) == Union{Missing, Int}
true

```

`missing` values can be excluded when performing operations by using `skipmissing`, which returns a memory-efficient iterator.

```jldoctest missings
julia> skipmissing(x)
Missings.EachSkipMissing{Array{Union{$Int, Missings.Missing},1}}(Union{$Int, Missings.Missing}[1, 2, missing])

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

`missing` elements can be replaced with other values via `Missings.replace`.

```jldoctest missings
julia> collect(Missings.replace(x, 1))
3-element Array{Int64,1}:
 1
 2
 1

```

The function `Missings.T` returns the element-type `T` in `Union{T, Missing}`.

```jldoctest missings
julia> eltype(x)
Union{Int64, Missings.Missing}

julia> Missings.T(eltype(x))
Int64

```

Use `missings` to generate `Vector`s and `Array`s supporting missing values, using the optional first argument to specify the element-type.

```jldoctest missings
julia> missings(1)
1-element Array{Missings.Missing,1}:
 missing

julia> missings(3)
3-element Array{Missings.Missing,1}:
 missing
 missing
 missing

julia> missings(1, 3)
1×3 Array{Missings.Missing,2}:
 missing  missing  missing

julia> missings(Int, 1, 3)
1×3 Array{Union{Missings.Missing, Int64},2}:
 missing  missing  missing

```