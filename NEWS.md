DataFrames v0.5.3 Release Notes
===============================

Internal changes to adjust to [JuliaLang/julia#5897]

DataFrames v0.5.2 Release Notes
===============================

Continues trend of stripping down features and improving core functionality.

New features
------------
  * append!(::AbstractDataFrame, ::AbstractDataFrame) ([#506])
  * `join` supports `:semi`-, `:anti`- and `:cross`-joins ([#524], [#536])
  * Implement `eltypes` argument in `readtable` ([#497])
  * Read from generic IO objects ([#499])

Changes
-------
  * Convert to using only symbols (no more strings) for column names ([#509])
  * Renames `stack_df`, `melt_df`, `pivot_table` to `stackdf`, `meltdf`, `pivottable` ([#538])
  * Renames `duplicated`, `drop_duplicates!` to `nonunique`, `unique!` ([#538])
  * Renames `load_df` to `loaddf` ([#538])
  * Renames `types` to `eltypes` ([#539])
  * Renames `readtable` argument `colnames` to `names` ([#497])

Deprecations
------------
  * Removes expression-based indexing, including `with`, `within!`, `based_on`, etc. ([#492])
  * Removes `DataStream` ([#492])
  * Removes `NamedArray` ([#492])
  * Removes column groupings (`set_groups`, `get_groups`, etc.)  ([#492])
  * Removes specific colwise and rowwise functions (`rowsums`, `colnorms`, etc.) ([#492])
  * Removes `@DataFrame` and `@transform` ([#492])
  * Deprecates natural `join`s: the key must be specified now ([#536])

DataFrames v0.5.1 Release Notes
===============================

Removing prototype features until core functionality is farther along.

Changes
-------
  * Write `Formula`s without quoting, thanks to the `@~` macro ([JuliaLang/julia#4882])
  * Renames `EachCol`, `EachRow` to `eachcol`, `eachrow` ([#474])
  * `eachrow` returns a `DataFrameRow` ([#474])
  * `SubDataFrames` are now immutable ([#474])

Deprecations
------------
  * Removes `IndexedVector` ([#483])
  * Removes `Blocks.jl` functionality ([#483])
  * Removes methods that treat DataFrame like a matrix, e.g `round`, `sin` ([#484])
  * Deprecates `sub`'s alias `subset` ([#474])

DataFrames v0.5.0 Release Notes
===============================

Improved I/O and more-Julian idioms.

New features
------------
  * Write HTML tables via writemime ([#433])
  * Read whitespace-delimited input ([#443])
  * Read input with C-style escapes ([#454])

Changes
-------
  * `sort` interface updated to better match mainline Julia ([#389])
  * `names!`, `rename!`, and `delete!` now return the updated Index, rather than the names in the Index ([#445])
  * Renames `coltypes`, `colnames`, `clean_colnames!` to `types`, `names`, `cleannames!` ([#469])
  * Various improvements to `print`/`show` methods

Deprecations
------------
  * Deprecates `rbind`, `cbind` and `vecbind` deprecated in favor of `hcat` and `vcat` ([#453])

[#389]: https://github.com/JuliaStats/DataFrames.jl/issues/389
[#433]: https://github.com/JuliaStats/DataFrames.jl/issues/433
[#443]: https://github.com/JuliaStats/DataFrames.jl/issues/443
[#445]: https://github.com/JuliaStats/DataFrames.jl/issues/445
[#453]: https://github.com/JuliaStats/DataFrames.jl/issues/453
[#454]: https://github.com/JuliaStats/DataFrames.jl/issues/454
[#469]: https://github.com/JuliaStats/DataFrames.jl/issues/469
[#474]: https://github.com/JuliaStats/DataFrames.jl/issues/474
[#483]: https://github.com/JuliaStats/DataFrames.jl/issues/483
[#484]: https://github.com/JuliaStats/DataFrames.jl/issues/484
[#492]: https://github.com/JuliaStats/DataFrames.jl/issues/492
[#497]: https://github.com/JuliaStats/DataFrames.jl/issues/497
[#499]: https://github.com/JuliaStats/DataFrames.jl/issues/499
[#506]: https://github.com/JuliaStats/DataFrames.jl/issues/506
[#509]: https://github.com/JuliaStats/DataFrames.jl/issues/509
[#524]: https://github.com/JuliaStats/DataFrames.jl/issues/524
[#536]: https://github.com/JuliaStats/DataFrames.jl/issues/536
[#538]: https://github.com/JuliaStats/DataFrames.jl/issues/538
[#539]: https://github.com/JuliaStats/DataFrames.jl/issues/539

[JuliaLang/julia#4882]: https://github.com/JuliaLang/julia/issues/4882
[JuliaLang/julia#5897]: https://github.com/JuliaLang/julia/issues/5897
