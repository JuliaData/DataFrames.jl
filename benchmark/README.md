# Planned benchmarks

## Split-apply-combine

Dimensions:
- number of grouping columns: 1, 2
- grouping column types (for two columns test all combinations): `String`, `Union{String, Missing}`, `Int`, `Union{Int, Missing}`, `CategoricalValue` (no missing), `CategoricalValue` (with missing), `CategoricalString` (no missing), `CategoricalString` (with missing)
- number of levels: 10, 1_000, 100_000
- number of rows: 10_000, 1_000_000, 100_000_000
- is data frame sorted: yes, no
- grouping column nature: vector, view
- grouped object type: `DataFrame`, `SubDataFrame`
- number of columns in the data frame: 5, 10
- aggregation operations:
    - `nrow`, `sum`, `mean` (`Int`, `Union{Int, Missing}`, `Float64`, `Union{Float64, Missing}`)
    - number of unique values (`Int`, `Union{Int, Missing}`, `String`, `Union{String, Missing}`, `CategoricalValue` (no missing), `CategoricalValue` (with missing), `CategoricalString` (no missing), `CategoricalString` (with missing))
    - number of missing values (`Union{Int, Missing}`, `Union{String, Missing}`, `CategoricalValue` (with missing), `CategoricalString` (with missing))
- aggregation input approaches: `SubDataFrame` (slow) vs `NamedTuple` (fast)
- aggregation output approaches: `DataFrame` (slow) vs `NamedTuple` (fast)

## Sorting

TODO

## Wide vs long format conversion

TODO

## Joins

TODO