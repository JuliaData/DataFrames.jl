# DataFrames.jl v1.2.2 Patch Release Notes

## Bug fixes

* fix a bug in `crossjoin` if the first argument is `SubDataFrame` and
  `makeunique=true`
  ([#2826](https://github.com/JuliaData/DataFrames.jl/issues/2826))

# DataFrames.jl v1.2.1 Patch Release Notes

## Bug fixes

* Add workaround for `deleteat!` bug in Julia Base in `delete!` function
  ([#2820](https://github.com/JuliaData/DataFrames.jl/issues/2820))

# DataFrames.jl v1.2 Release Notes

## New functionalities

* add option `matchmissing=:notequal` in joins;
  in `leftjoin`, `semijoin` and `antijoin` missings are dropped in right data frame,
  but preserved in left; in `rightjoin` missings are dropped in left data frame,
  but preserved in right; in `innerjoin` missings are dropped in both data frames;
  in `outerjoin` this value of keyword argument is not supported
  ([#2724](https://github.com/JuliaData/DataFrames.jl/pull/2724))
* correctly handle selectors of the form `:col => AsTable` and `:col => cols`
  by expanding a single column into multiple columns
  ([#2780](https://github.com/JuliaData/DataFrames.jl/pull/2780))
* if `subset!` is passed a `GroupedDataFrame` the grouping in the passed object
  gets updated to reflect rows removed from the parent data frame
  ([#2809](https://github.com/JuliaData/DataFrames.jl/pull/2809))

## Bug fixes

* fix bug in how `groupby` handles grouping of float columns;
  now `-0.0` is treated as *not integer* when deciding on which
  grouping algorithm should be used
  ([#2791](https://github.com/JuliaData/DataFrames.jl/pull/2791))
* fix bug in how `issorted` handles custom orderings and improve performance
  of sorting when complex custom orderings are passed
  ([#2746](https://github.com/JuliaData/DataFrames.jl/pull/2746))
* fix bug in `combine`, `select`, `select!`, `transform`, and `transform!`
  that incorrectly disallowed matrices of `Pair`s in `GroupedDataFrame` processing
  ([#2782](https://github.com/JuliaData/DataFrames.jl/pull/2782))
* fix location of summary in `text/html` output
  ([#2801](https://github.com/JuliaData/DataFrames.jl/pull/2801))

## Performance improvements

* `SubDataFrame`, `filter!`, `unique!`, `getindex`, `delete!`, `leftjoin`,
  `rightjoin`, and `outerjoin` are now more efficient if rows selected
  in internal operations form a continuous block
  ([#2727](https://github.com/JuliaData/DataFrames.jl/pull/2727),
   [#2769](https://github.com/JuliaData/DataFrames.jl/pull/2769))

## Deprecated

* `hcat` of a data frame with a vector is now deprecated to allow consistent
  handling of horizontal concatenation of data frame with Tables.jl tables
  in the future
  ([#2777](https://github.com/JuliaData/DataFrames.jl/pull/2777))

## Other changes

* `text/plain` rendering of columns containing complex numbers is now improved
  ([#2756](https://github.com/JuliaData/DataFrames.jl/pull/2756))
* in `text/html` display of a data frame show full type information when
  hovering over the shortened type with a mouse
  ([#2774](https://github.com/JuliaData/DataFrames.jl/pull/2774))

# DataFrames.jl v1.1.1 Patch Release Notes

## Performance improvements

* fix performance issue when aggregation function produces multiple rows
  in split-apply-combine
  ([2749](https://github.com/JuliaData/DataFrames.jl/pull/2749))
* `completecases` is now optimized and only processes columns that
  can contain missing values; additionally it is now type stable and
  always returns a `BitVector`
  ([#2726](https://github.com/JuliaData/DataFrames.jl/pull/2726))
* fix performance bottleneck when displaying wide tables
  ([#2750](https://github.com/JuliaData/DataFrames.jl/pull/2750))

# DataFrames.jl v1.1 Release Notes

## Functionality changes

* make sure `subset` checks if the passed condition function
  returns a vector of values (in the 1.0 release also returning scalar `true`,
  `false`, or `missing` was allowed which was unintended and error prone)
  ([#2744](https://github.com/JuliaData/DataFrames.jl/pull/2744))


# DataFrames.jl v1.0.2 Patch Release Notes

## Performance improvements

* fix of performance issue of `groupby` when using multi-threading
  ([#2736](https://github.com/JuliaData/DataFrames.jl/pull/2736))
* fix of performance issue of `groupby` when using `PooledVector`
  ([2733](https://github.com/JuliaData/DataFrames.jl/pull/2733))

# DataFrames.jl v1.0 Release Notes

## Breaking changes

* No breaking changes are planned for v1.0 release

## Bug fixes

* DataFrames.jl now checks that passed columns are 1-based as this is a current
  design assumption ([#2594](https://github.com/JuliaData/DataFrames.jl/pull/2594))
* `mapcols!` makes sure not to create columns being `AbstractRange` consistently
  with other methods that add columns to a `DataFrame`
  ([#2594](https://github.com/JuliaData/DataFrames.jl/pull/2594))
* `transform` and `transform!` always copy columns when column renaming transformation
  is passed. If similar issues are identified after 1.0 release (i.e. that a
  copy of data is not made in scenarios where it normally should be made these
  will be considered bugs and fixed as non-breaking changes)
  ([#2721](https://github.com/JuliaData/DataFrames.jl/pull/2721))

## New functionalities

* `firstindex`, `lastindex`, `size`, `ndims`, and `axes` are now consistently defined
  and documented in the manual for `AbstractDataFrame`, `DataFrameRow`,
  `DataFrameRows`, `DataFrameColumns`, `GroupedDataFrame`, `GroupKeys`, and `GroupKey`
  ([#2573](https://github.com/JuliaData/DataFrames.jl/pull/2573))
* add `subset` and `subset!` functions that allow to subset rows
  ([#2496](https://github.com/JuliaData/DataFrames.jl/pull/2496))
* `names` now allows passing a predicate as a column selector
  ([#2417](https://github.com/JuliaData/DataFrames.jl/pull/2417))
* `vcat` now allows a `source` keyword argument that specifies the
  additional column to be added in the last position in the resulting data frame
  that will identify the source data frame.
  ([#2649](https://github.com/JuliaData/DataFrames.jl/pull/2649))
* `GroupKey` and `DataFrameRow` are consistently behaving like `NamedTuple`
  in comparisons and they now implement: `hash`, `==`, `isequal`, `<`, `isless`
  ([#2669](https://github.com/JuliaData/DataFrames.jl/pull/2669)])
* since Julia 1.7 using broadcasting assignment on a `DataFrame` column
  selected as a property (e.g. `df.col .= 1`) is allowed when column does not
  exist and it allocates a fresh column
  ([#2655](https://github.com/JuliaData/DataFrames.jl/pull/2655))
* `delete!` now correctly handles the case when columns of a data frame are aliased
  ([#2690](https://github.com/JuliaData/DataFrames.jl/pull/2690))

## Deprecated

* in `leftjoin`, `rightjoin`, and `outerjoin` the `indicator` keyword argument
  is deprecated in favor of `source` keyword argument; `indicator` will be removed
  in 2.0 release ([2649](https://github.com/JuliaData/DataFrames.jl/pull/2649))
* Using broadcasting assignment on a `SubDataFrames` column selected as a property
  (e.g. `sdf.col .= 1`) is deprecated; it will be disallowed in the future.
  ([#2655](https://github.com/JuliaData/DataFrames.jl/pull/2655))
* Broadcasting assignment to an existing column of a `DataFrame`
  selected as a property (e.g. `df.col .= 1`) being an in-place
  operation is deprecated. It will allocate a fresh column in the future
  ([#2655](https://github.com/JuliaData/DataFrames.jl/pull/2655))
* all deprecations present in 0.22 release now throw an error
  ([#2554](https://github.com/JuliaData/DataFrames.jl/pull/2554));
  in particular `convert` methods, `map` on `GroupedDataFrame`
  that were deprecated in 0.22.6 release now throw an error
  ([#2679](https://github.com/JuliaData/DataFrames.jl/pull/2679))

## Other relevant changes

* `innerjoin`, `leftjoin`, `rightjoin`, `outerjoin`, `semijoin`, and `antijoin`
  are now much faster and check if passed data frames are sorted by the `on`
  columns and take into account if shorter data frame that is joined has unique
  values in `on` columns. These aspects of input data frames might affect the
  order of rows produced in the output
  ([#2612](https://github.com/JuliaData/DataFrames.jl/pull/2612),
   [#2622](https://github.com/JuliaData/DataFrames.jl/pull/2622))
* `DataFrame` constructor, `copy`, `getindex`, `select`, `select!`, `transform`,
  `transform!`, `combine`, `sort`, and join functions now use multiple threads
  in selected operations
  ([#2647](https://github.com/JuliaData/DataFrames.jl/pull/2647),
   [#2588](https://github.com/JuliaData/DataFrames.jl/pull/2588),
   [#2574](https://github.com/JuliaData/DataFrames.jl/pull/2574),
   [#2664](https://github.com/JuliaData/DataFrames.jl/pull/2664))

# DataFrames.jl v0.22.7 Release notes

* `convert` methods from `AbstractDataFrame`, `DataFrameRow` and `GroupKey`
  to `Array`, `Matrix`, `Vector` and `Tuple`, as well as from `AbstractDict` to
  `DataFrame`, are now deprecated: use corresponding
  constructors instead. The only conversions that are
  retained are `convert(::Type{NamedTuple}, dfr::DataFrameRow)`,
  `convert(::Type{NamedTuple}, key::GroupKey)`, and
  `convert(::Type{DataFrame}, sdf::SubDataFrame)`; the deprecated methods will be
  removed in 1.0 release
* as a bug fix `eltype` of vector returned by `eachrow` is now `DataFrameRow`
  ([#2662](https://github.com/JuliaData/DataFrames.jl/pull/2662))
* applying `map` to `GroupedDataFrame` is now deprecated. It will
  be an error in 1.0 release.
  ([#2662](https://github.com/JuliaData/DataFrames.jl/pull/2662))
* `copycols` keyword argument is now respected when building a `DataFrame` from
  `Tables.CopiedColumns`
  ([#2656](https://github.com/JuliaData/DataFrames.jl/pull/2656))

# DataFrames.jl v0.22 Release Notes

## Breaking changes

* the rules for transformations passed to `select`/`select!`, `transform`/`transform!`,
  and `combine` have been made more flexible; in particular now it is allowed to
  return multiple columns from a transformation function
  ([#2461](https://github.com/JuliaData/DataFrames.jl/pull/2461) and
  [#2481](https://github.com/JuliaData/DataFrames.jl/pull/2481))
* CategoricalArrays.jl is no longer reexported: call `using CategoricalArrays`
  to use it [#2404]((https://github.com/JuliaData/DataFrames.jl/pull/2404)).
  In the same vein, the `categorical` and `categorical!` functions
  have been deprecated in favor of
  `transform(df, cols .=> categorical .=> cols)` and similar syntaxes
  [#2394]((https://github.com/JuliaData/DataFrames.jl/pull/2394)).
  `stack` now creates a `PooledVector{String}` variable column rather than
  a `CategoricalVector{String}` column by default;
  pass `variable_eltype=CategoricalValue{String}` to get the previous behavior
  ([#2391](https://github.com/JuliaData/DataFrames.jl/pull/2391))
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
* joins now return `PooledVector` not `CategoricalVector` in indicator column
  ([#2505](https://github.com/JuliaData/DataFrames.jl/pull/2505))
* `GroupKeys` now supports `in` for `GroupKey`, `Tuple`, `NamedTuple` and dictionaries
  ([2392](https://github.com/JuliaData/DataFrames.jl/pull/2392))
* in `describe` the specification of custom aggregation is now `function => name`;
  old `name => function` order is now deprecated
  ([#2401](https://github.com/JuliaData/DataFrames.jl/pull/2401))
* in joins passing `NaN` or real or imaginary `-0.0` in `on` column now throws an
  error; passing `missing` thows an error unless `matchmissing=:equal` keyword argument
  is passed ([#2504](https://github.com/JuliaData/DataFrames.jl/pull/2504))
* `unstack` now produces row and column keys in the order of their first appearance
   and has two new keyword arguments `allowmissing` and `allowduplicates`
  ([#2494](https://github.com/JuliaData/DataFrames.jl/pull/2494))
* [PrettyTables.jl](https://github.com/ronisbr/PrettyTables.jl) is now the
  default back-end to print DataFrames to text/plain; the print option
  `splitcols` was removed and the output format was changed
  ([#2429](https://github.com/JuliaData/DataFrames.jl/pull/2429))

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
  ([#2313](https://github.com/JuliaData/DataFrames.jl/pull/2313) and
  ([#2398](https://github.com/JuliaData/DataFrames.jl/pull/2398))
* add `rownumber` to `DataFrameRow` ([#2356](https://github.com/JuliaData/DataFrames.jl/pull/2356))
* allow passing column name to specify the position where a new columns should be
  inserted in `insertcols!` ([#2365](https://github.com/JuliaData/DataFrames.jl/pull/2365))
* allow `GroupedDataFrame`s to be indexed using a dictionary, which can use `Symbol` or string keys and
  are not dependent on the order of keys. ([#2281](https://github.com/JuliaData/DataFrames.jl/pull/2281))
* add `isapprox` method to check for approximate equality between two dataframes
  ([#2373](https://github.com/JuliaData/DataFrames.jl/pull/2373))
* add `columnindex` for `DataFrameRow`
  ([#2380](https://github.com/JuliaData/DataFrames.jl/pull/2380))
* `names` now accepts `Type` as a column selector
  ([#2400](https://github.com/JuliaData/DataFrames.jl/pull/2400))
* `select`, `select!`, `transform`, `transform!` and `combine` now allow `renamecols`
  keyword argument that makes it possible to avoid adding transformation function name
  as a suffix in automatically generated column names
  ([#2397](https://github.com/JuliaData/DataFrames.jl/pull/2397))
* `filter`, `sort`, `dropmissing`, and `unique` now support a `view` keyword argument
  which if set to `true` makes them retun a `SubDataFrame` view into the passed
  data frame.
* add `only` method for `AbstractDataFrame` ([#2449](https://github.com/JuliaData/DataFrames.jl/pull/2449))
* passing empty sets of columns in `filter`/`filter!` and in `select`/`transform`/`combine`
  with `ByRow` is now accepted ([#2476](https://github.com/JuliaData/DataFrames.jl/pull/2476))
* add `permutedims` method for `AbstractDataFrame` ([#2447](https://github.com/JuliaData/DataFrames.jl/pull/2447))
* add support for `Cols` from DataAPI.jl ([#2495](https://github.com/JuliaData/DataFrames.jl/pull/2495))

## Deprecated

* `DataFrame!` is now deprecated ([#2338](https://github.com/JuliaData/DataFrames.jl/pull/2338))
* several in-standard `DataFrame` constructors are now deprecated
  ([#2464](https://github.com/JuliaData/DataFrames.jl/pull/2464))
* all old deprecations now throw an error
  ([#2350](https://github.com/JuliaData/DataFrames.jl/pull/2350))

## Dependency changes

* Tables.jl version 1.2 is now required.
* DataAPI.jl version 1.4 is now required. It implies that `All(args...)` is
  deprecated and `Cols(args...)` is recommended instead. `All()` is still supported.

## Other relevant changes

* Documentation is now available also in *Dark* mode
  ([#2315](https://github.com/JuliaData/DataFrames.jl/pull/2315))
* add rich display support for Markdown cell entries in HTML and LaTeX
  ([#2346](https://github.com/JuliaData/DataFrames.jl/pull/2346))
* limit the maximal display width the output can use in `text/plain` before
  being truncated (in the `textwidth` sense, excluding `…`) to `32` per column
  by default and fix a corner case when no columns are printed in situations when
  they are too wide ([#2403](https://github.com/JuliaData/DataFrames.jl/pull/2403))
* Common methods are now precompiled to improve responsiveness the first time a method
  is called in a Julia session. Precompilation takes up to 30 seconds
  after installing the package
  ([#2456](https://github.com/JuliaData/DataFrames.jl/pull/2456)).
