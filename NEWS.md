# DataFrames v0.22 Release Notes

## Breaking changes

* `isless` for `DataFrameRow`s now checks column names
([#2292](https://github.com/JuliaData/DataFrames.jl/pull/2292))
* `DataFrameColumns` is now not a subtype of `AbstractVector`
  ([#2291](https://github.com/JuliaData/DataFrames.jl/pull/2291))
* `nunique` is not reported now by `describe` by default
  ([#2339](https://github.com/JuliaData/DataFrames.jl/pull/2339))
* stop reordering columns of the parent in `transform` and `transform!`;
  always generate columns that were specified to be computed even for
  `GroupedDataFrame` with zero rows
  ([#2324](https://github.com/JuliaData/DataFrames.jl/pull/2324))
* improve the rule for automatically generated column names in
  `combine`/`select(!)`/`transform(!)` with composed functions
  ([#2274](https://github.com/JuliaData/DataFrames.jl/pull/2274))
* `:nmissing` in `describe` now produces `0` if the column does not allow
  missing values; earlier `nothing` was produced in this case
  ([#2360](https://github.com/JuliaData/DataFrames.jl/pull/2360))
* fast aggregation functions in for `GroupedDataFrame` now correctly
  choose the fast path only when it is safe; this resolves inconsistencies
  with what the same functions not using fast path produce
  ([#2357](https://github.com/JuliaData/DataFrames.jl/pull/2357))

## New functionalities

* add `filter` to `GroupedDataFrame` ([#2279](https://github.com/JuliaData/DataFrames.jl/pull/2279))
* add `empty` and `empty!` function for `DataFrame` that remove all rows from it,
  but keep columns ([#2262](https://github.com/JuliaData/DataFrames.jl/pull/2262))
* make `indicator` keyword argument in joins allow passing a string
  ([#2284](https://github.com/JuliaData/DataFrames.jl/pull/2284),
   [#2296](https://github.com/JuliaData/DataFrames.jl/pull/2296))
* add new functions to `GroupKey` API to make it more consistent with `DataFrameRow`
  ([#2308](https://github.com/JuliaData/DataFrames.jl/pull/2308))
* allow column renaming in joins
  ([#2313](https://github.com/JuliaData/DataFrames.jl/pull/2313)

## Deprecated

* `DataFrame!` is now deprecated ([#2338](https://github.com/JuliaData/DataFrames.jl/pull/2338))
* all old deprecations now throw an error
  ([#2350](https://github.com/JuliaData/DataFrames.jl/pull/2350))

## Dependency changes

## Other relevant changes

* Documentation is now available also in *Dark* mode
  ([#2315](https://github.com/JuliaData/DataFrames.jl/pull/2315))
