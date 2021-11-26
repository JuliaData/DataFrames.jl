# Comparisons

This section compares DataFrames.jl with other data manipulation frameworks in Python, R, and Stata.

A sample data set can be created using the following code:

```julia
using DataFrames
using Statistics

df = DataFrame(grp=repeat(1:2, 3), x=6:-1:1, y=4:9, z=[3:7; missing], id='a':'f')
df2 = DataFrame(grp=[1, 3], w=[10, 11])
```

!!! note

    Some of the operations mutate the tables so every operation assumes that it is done on the original data frame.

Note that in the comparisons presented below predicates like `x -> x >= 1` can
be more compactly written as `=>(1)`. The latter form has an additional benefit
that it is compiled only once per Julia session (as opposed to `x -> x >= 1`
which defines a new anonymous function every time it is introduced).

## Comparison with the Python package pandas

The following table compares the main functions of DataFrames.jl with the Python package pandas (version 1.1.0):

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({'grp': [1, 2, 1, 2, 1, 2],
                   'x': range(6, 0, -1),
                   'y': range(4, 10),
                   'z': [3, 4, 5, 6, 7, None]},
                   index = list('abcdef'))
df2 = pd.DataFrame({'grp': [1, 3], 'w': [10, 11]})
```

Because pandas supports multi-index, this example data frame is set up with `a` to `f`
as row indices rather than a separate `id` column.

### Accessing data

| Operation                  | pandas                  | DataFrames.jl                      |
|:---------------------------|:------------------------|:-----------------------------------|
| Cell indexing by location  | `df.iloc[1, 1]`         | `df[2, 2]`                         |
| Row slicing by location    | `df.iloc[1:3]`          | `df[2:3, :]`                       |
| Column slicing by location | `df.iloc[:, 1:]`        | `df[:, 2:end]`                     |
| Row indexing by label      | `df.loc['c']`           | `df[findfirst(==('c'), df.id), :]` |
| Column indexing by label   | `df.loc[:, 'x']`        | `df[:, :x]`                        |
| Column slicing by label    | `df.loc[:, ['x', 'z']]` | `df[:, [:x, :z]]`                  |
|                            | `df.loc[:, 'x':'z']`    | `df[:, Between(:x, :z)]`           |
| Mixed indexing             | `df.loc['c'][1]`        | `df[findfirst(==('c'), df.id), 2]` |

Note that Julia uses 1-based indexing, inclusive on both ends. A special keyword `end` can be used to
indicate the last index. Likewise, the `begin` keyword can be used to indicate the first index.

In addition, when indexing a data frame with the `findfirst` function, a single
`DataFrameRow` object is returned. In the case that `id` is not unique, you can use the `findall` function
or boolean indexing instead. It would then return a `DataFrame` object containing all matched rows. The following
two lines of code are functionally equivalent:

```julia
df[findall(==('c'), df.id), :]
df[df.id .== 'c', :]
```

DataFrames.jl's indexing always produces a consistent and predictable return type.
By contrast, pandas' `loc` function returns a `Series` object when there is exactly
one `'c'` value in the index, and it returns a `DataFrame` object when there are multiple
rows having the index value of `'c'`.

### Common operations

| Operation                | pandas                                                         | DataFrames.jl                               |
|:-------------------------|:---------------------------------------------------------------|:--------------------------------------------|
| Reduce multiple values   | `df['z'].mean(skipna = False)`                                 | `mean(df.z)`                                |
|                          | `df['z'].mean()`                                               | `mean(skipmissing(df.z))`                   |
|                          | `df[['z']].agg(['mean'])`                                      | `combine(df, :z => mean ∘ skipmissing)`     |
| Add new columns          | `df.assign(z1 = df['z'] + 1)`                                  | `transform(df, :z => (v -> v .+ 1) => :z1)` |
| Rename columns           | `df.rename(columns = {'x': 'x_new'})`                          | `rename(df, :x => :x_new)`                  |
| Pick & transform columns | `df.assign(x_mean = df['x'].mean())[['x_mean', 'y']]`          | `select(df, :x => mean, :y)`                |
| Sort rows                | `df.sort_values(by = 'x')`                                     | `sort(df, :x)`                              |
|                          | `df.sort_values(by = ['grp', 'x'], ascending = [True, False])` | `sort(df, [:grp, order(:x, rev = true)])`   |
| Drop missing rows        | `df.dropna()`                                                  | `dropmissing(df)`                           |
| Select unique rows       | `df.drop_duplicates()`                                         | `unique(df)`                                |

Note that pandas skips `NaN` values in its analytic functions by default. By contrast,
Julia functions do not skip `NaN`'s. If necessary, you can filter out
the `NaN`'s before processing, for example, `mean(Iterators.filter(!isnan, x))`.

Pandas uses `NaN` for representing both missing data and the floating point "not a number" value.
Julia defines a special value `missing` for representing missing data. DataFrames.jl respects
general rules in Julia in propagating `missing` values by default. If necessary,
the `skipmissing` function can be used to remove missing data.
See the [Missing Data](@ref) section for more information.

In addition, pandas keeps the original column name after applying a function.
DataFrames.jl appends a suffix to the column name by default. To keep it simple, the
examples above do not synchronize the column names between pandas and DataFrames.jl
(you can pass `renamecols=false` keyword argument to `select`, `transform` and
`combine` functions to retain old column names).

### Mutating operations

| Operation          | pandas                                                | DataFrames.jl                                |
|:-------------------|:------------------------------------------------------|:---------------------------------------------|
| Add new columns    | `df['z1'] = df['z'] + 1`                              | `df.z1 = df.z .+ 1`                          |
|                    |                                                       | `transform!(df, :z => (x -> x .+ 1) => :z1)` |
|                    | `df.insert(1, 'const', 10)`                           | `insertcols!(df, 2, :const => 10)`           |
| Rename columns     | `df.rename(columns = {'x': 'x_new'}, inplace = True)` | `rename!(df, :x => :x_new)`                  |
| Sort rows          | `df.sort_values(by = 'x', inplace = True)`            | `sort!(df, :x)`                              |
| Drop missing rows  | `df.dropna(inplace = True)`                           | `dropmissing!(df)`                           |
| Select unique rows | `df.drop_duplicates(inplace = True)`                  | `unique!(df)`                                |

Generally speaking, DataFrames.jl follows the Julia convention of using `!` in the
function name to indicate mutation behavior.

### Grouping data and aggregation

DataFrames.jl provides a `groupby` function to apply operations
over each group independently. The result of `groupby` is a `GroupedDataFrame` object
which may be processed using the `combine`, `transform`, or `select` functions.
The following table illustrates some common grouping and aggregation usages.


| Operation                       | pandas                                                                                 | DataFrames.jl                                        |
|:--------------------------------|:---------------------------------------------------------------------------------------|:-----------------------------------------------------|
| Aggregate by groups             | `df.groupby('grp')['x'].mean()`                                                        | `combine(groupby(df, :grp), :x => mean)`             |
| Rename column after aggregation | `df.groupby('grp')['x'].mean().rename("my_mean")`                                      | `combine(groupby(df, :grp), :x => mean => :my_mean)` |
| Add aggregated data as column   | `df.join(df.groupby('grp')['x'].mean(), on='grp', rsuffix='_mean')`                    | `transform(groupby(df, :grp), :x => mean)`           |
| ...and select output columns    | `df.join(df.groupby('grp')['x'].mean(), on='grp', rsuffix='_mean')[['grp', 'x_mean']]` | `select(groupby(df, :grp), :id, :x => mean)`         |

Note that pandas returns a `Series` object for 1-dimensional result unless `reset_index` is called afterwards.
The corresponding DataFrames.jl examples return an equivalent `DataFrame` object.
Consider the first example:

```python
>>> df.groupby('grp')['x'].mean()
grp
1    4
2    3
Name: x, dtype: int64
```

For DataFrames.jl, it looks like this:

```julia
julia> combine(groupby(df, :grp), :x => mean)
2×2 DataFrame
 Row │ grp    x_mean
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      3.0
```

In DataFrames.jl, the `GroupedDataFrame` object supports an efficient key lookup.
Hence, it performs well when you need to perform lookups repeatedly.

### More advanced commands

This section includes more complex examples.

| Operation                              | pandas                                                                       | DataFrames.jl                                             |
|:---------------------------------------|:-----------------------------------------------------------------------------|:------------------------------------------------------------------------|
| Complex Function                       | `df[['z']].agg(lambda v: np.mean(np.cos(v)))`                                | `combine(df, :z => v -> mean(cos, skipmissing(v)))`                     |
| Aggregate multiple columns             | `df.agg({'x': max, 'y': min})`                                               | `combine(df, :x => maximum, :y => minimum)`                             |
|                                        | `df[['x', 'y']].mean()`                                                      | `combine(df, [:x, :y] .=> mean)`                                        |
|                                        | `df.filter(regex=("^x")).mean()`                                             | `combine(df, names(df, r"^x") .=> mean)`                                |
| Apply function over multiple variables | `df.assign(x_y_cor = np.corrcoef(df.x, df.y)[0, 1])`                         | `transform(df, [:x, :y] => cor)`                                        |
| Row-wise operation                     | `df.assign(x_y_min = df.apply(lambda v: min(v.x, v.y), axis=1))`             | `transform(df, [:x, :y] => ByRow(min))`                                 |
|                                        | `df.assign(x_y_argmax = df.apply(lambda v: df.columns[v.argmax()], axis=1))` | `transform(df, AsTable([:x, :y]) => ByRow(argmax))`                     |
| DataFrame as input                     | `df.groupby('grp').head(2)`                                                  | `combine(d -> first(d, 2), groupby(df, :grp))`                          |
| DataFrame as output                    | `df[['x']].agg(lambda x: [min(x), max(x)])`                                  | `combine(df, :x => (x -> (x=[minimum(x), maximum(x)],)) => AsTable)`  |

Note that pandas preserves the same row order after `groupby` whereas DataFrames.jl
shows them grouped by the provided keys after the `combine` operation,
but `select` and `transform` retain an original row ordering.

### Joining data frames

DataFrames.jl supports join operations similar to a relational database.

| Operation             | pandas                                         | DataFrames.jl                   |
|:----------------------|:-----------------------------------------------|:--------------------------------|
| Inner join            | `pd.merge(df, df2, how = 'inner', on = 'grp')` | `innerjoin(df, df2, on = :grp)` |
| Outer join            | `pd.merge(df, df2, how = 'outer', on = 'grp')` | `outerjoin(df, df2, on = :grp)` |
| Left join             | `pd.merge(df, df2, how = 'left', on = 'grp')`  | `leftjoin(df, df2, on = :grp)`  |
| Right join            | `pd.merge(df, df2, how = 'right', on = 'grp')` | `rightjoin(df, df2, on = :grp)` |
| Semi join (filtering) | `df[df.grp.isin(df2.grp)]`                     | `semijoin(df, df2, on = :grp)`  |
| Anti join (filtering) | `df[~df.grp.isin(df2.grp)]`                    | `antijoin(df, df2, on = :grp)`  |

For multi-column joins, both pandas and DataFrames.jl accept an array for the `on` keyword argument.

In the cases of semi joins and anti joins, the `isin` function in pandas can still be used as long as
the join keys are [combined in a tuple](https://stackoverflow.com/questions/63660610/how-to-perform-semi-join-with-multiple-columns-in-pandas).
In DataFrames.jl, it just works normally with an array of join keys specified in the `on` keyword argument.

## Comparison with the R package dplyr

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1):

```R
df <- tibble(grp = rep(1:2, 3), x = 6:1, y = 4:9,
             z = c(3:7, NA), id = letters[1:6])
```

| Operation                | dplyr                          | DataFrames.jl                          |
|:-------------------------|:-------------------------------|:---------------------------------------|
| Reduce multiple values   | `summarize(df, mean(x))`       | `combine(df, :x => mean)`              |
| Add new columns          | `mutate(df, x_mean = mean(x))` | `transform(df, :x => mean => :x_mean)` |
| Rename columns           | `rename(df, x_new = x)`        | `rename(df, :x => :x_new)`             |
| Pick columns             | `select(df, x, y)`             | `select(df, :x, :y)`                   |
| Pick & transform columns | `transmute(df, mean(x), y)`    | `select(df, :x => mean, :y)`           |
| Pick rows                | `filter(df, x >= 1)`           | `subset(df, :x => ByRow(x -> x >= 1))` |
| Sort rows                | `arrange(df, x)`               | `sort(df, :x)`                         |

As in dplyr, some of these functions can be applied to grouped data frames, in which case they operate by group:

| Operation                | dplyr                                      | DataFrames.jl                               |
|:-------------------------|:-------------------------------------------|:--------------------------------------------|
| Reduce multiple values   | `summarize(group_by(df, grp), mean(x))`    | `combine(groupby(df, :grp), :x => mean)`    |
| Add new columns          | `mutate(group_by(df, grp), mean(x))`       | `transform(groupby(df, :grp), :x => mean)`  |
| Pick & transform columns | `transmute(group_by(df, grp), mean(x), y)` | `select(groupby(df, :grp), :x => mean, :y)` |

The table below compares more advanced commands:

| Operation                 | dplyr                                                     | DataFrames.jl                                                              |
|:--------------------------|:----------------------------------------------------------|:---------------------------------------------------------------------------|
| Complex Function          | `summarize(df, mean(x, na.rm = T))`                       | `combine(df, :x => x -> mean(skipmissing(x)))`                             |
| Transform several columns | `summarize(df, max(x), min(y))`                           | `combine(df, :x => maximum,  :y => minimum)`                               |
|                           | `summarize(df, across(c(x, y), mean))`                    | `combine(df, [:x, :y] .=> mean)`                                           |
|                           | `summarize(df, across(starts_with("x"), mean))`           | `combine(df, names(df, r"^x") .=> mean)`                                   |
|                           | `summarize(df, across(c(x, y), list(max, min)))`          | `combine(df, ([:x, :y] .=> [maximum minimum])...)`                         |
| Multivariate function     | `mutate(df, cor(x, y))`                                   | `transform(df, [:x, :y] => cor)`                                           |
| Row-wise                  | `mutate(rowwise(df), min(x, y))`                          | `transform(df, [:x, :y] => ByRow(min))`                                    |
|                           | `mutate(rowwise(df), which.max(c_across(matches("^x"))))` | `transform(df, AsTable(r"^x") => ByRow(argmax))`                           |
| DataFrame as input        | `summarize(df, head(across(), 2))`                        | `combine(d -> first(d, 2), df)`                                            |
| DataFrame as output       | `summarize(df, tibble(value = c(min(x), max(x))))`        | `combine(df, :x => (x -> (value = [minimum(x), maximum(x)],)) => AsTable)` |


## Comparison with the R package data.table

The following table compares the main functions of DataFrames.jl with the R package data.table (version 1.14.1).

```R
library(data.table)
df  <- data.table(grp = rep(1:2, 3), x = 6:1, y = 4:9,
                  z = c(3:7, NA), id = letters[1:6])
df2 <- data.table(grp=c(1,3), w = c(10,11))
```

| Operation                          | data.table                                       | DataFrames.jl                                |
|:-----------------------------------|:-------------------------------------------------|:---------------------------------------------|
| Reduce multiple values             | `df[, .(mean(x))]`                               | `combine(df, :x => mean)`                    |
| Add new columns                    | `df[, x_mean:=mean(x) ]`                         | `transform!(df, :x => mean => :x_mean)`      |
| Rename column (in place)           | `setnames(df, "x", "x_new")`                     | `rename!(df, :x => :x_new)`                  |
| Rename multiple columns (in place) | `setnames(df, c("x", "y"), c("x_new", "y_new"))` | `rename!(df, [:x, :y] .=> [:x_new, :y_new])` |
| Pick columns as dataframe          | `df[, .(x, y)]`                                  | `select(df, :x, :y)`                         |
| Pick column as a vector            | `df[, x]`                                        | `df[!, :x]`                                  |
| Remove columns                     | `df[, -"x"]`                                     | `select(df, Not(:x))`                        |
| Remove columns (in place)          | `df[, x:=NULL]`                                  | `select!(df, Not(:x))`                       |
| Remove columns (in place)          | `df[, c("x", "y"):=NULL]`                        | `select!(df, Not([:x, :y]))`                 |
| Pick & transform columns           | `df[, .(mean(x), y)]`                            | `select(df, :x => mean, :y)`                 |
| Pick rows                          | `df[ x >= 1 ]`                                   | `filter(:x => >=(1), df)`                    |
| Sort rows (in place)               | `setorder(df, x)`                                | `sort!(df, :x)`                              |
| Sort rows                          | `df[ order(x) ]`                                 | `sort(df, :x)`                               |

### Grouping data and aggregation

| Operation                   | data.table                                        | DataFrames.jl                             |
|:----------------------------|:--------------------------------------------------|:------------------------------------------|
| Reduce multiple values      | `df[, mean(x), by=id ]`                           | `combine(groupby(df, :id), :x => mean)`   |
| Add new columns (in place)  | `df[, x_mean:=mean(x), by=id]`                    | `transform!(groupby(df, :id), :x => mean)`|
| Pick & transform columns    | `df[, .(x_mean = mean(x), y), by=id]`             | `select(groupby(df, :id), :x => mean, :y)`|

### More advanced commands

| Operation                         | data.table                                                                                 | DataFrames.jl                                                               |
|:----------------------------------|:-------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------|
| Complex Function                  | `df[, .(mean(x, na.rm=TRUE)) ]`                                                            | `combine(df, :x => x -> mean(skipmissing(x)))`                              |
| Transform certain rows (in place) | `df[x<=0, x:=0]`                                                                           | `df.x[df.x .<= 0] .= 0`                                                     |
| Transform several columns         | `df[, .(max(x), min(y)) ]`                                                                 | `combine(df, :x => maximum, :y => minimum)`                                 |
|                                   | `df[, lapply(.SD, mean), .SDcols = c("x", "y") ]`                                          | `combine(df, [:x, :y] .=> mean)`                                            |
|                                   | `df[, lapply(.SD, mean), .SDcols = patterns("*x") ]`                                       | `combine(df, names(df, r"^x") .=> mean)`                                    |
|                                   | `df[, unlist(lapply(.SD, function(x) c(max=max(x), min=min(x)))), .SDcols = c("x", "y") ]` | `combine(df, ([:x, :y] .=> [maximum minimum])...)`                          |
| Multivariate function             | `df[, .(cor(x,y)) ]`                                                                       | `transform(df, [:x, :y] => cor)`                                            |
| Row-wise                          | `df[, min_xy := min(x, y), by = 1:nrow(df)]`                                               | `transform!(df, [:x, :y] => ByRow(min))`                                    |
|                                   | `df[, argmax_xy := which.max(.SD) , .SDcols = patterns("*x"), by = 1:nrow(df) ]`           | `transform!(df, AsTable(r"^x") => ByRow(argmax))`                           |
| DataFrame as output               | `df[, .SD[1], by=grp]`                                                                     | `combine(groupby(df, :grp), first)`                                         |
| DataFrame as output               | `df[, .SD[which.max(x)], by=grp]`                                                          | `combine(groupby(df, :grp), sdf -> sdf[argmax(sdf.x), :])`                  |

### Joining data frames

| Operation             | data.table                                      | DataFrames.jl                   |
|:----------------------|:------------------------------------------------|:--------------------------------|
| Inner join            | `merge(df, df2, on = "grp")`                    | `innerjoin(df, df2, on = :grp)` |
| Outer join            | `merge(df, df2, all = TRUE, on = "grp")`        | `outerjoin(df, df2, on = :grp)` |
| Left join             | `merge(df, df2, all.x = TRUE, on = "grp")`      | `leftjoin(df, df2, on = :grp)`  |
| Right join            | `merge(df, df2, all.y = TRUE, on = "grp")`      | `rightjoin(df, df2, on = :grp)` |
| Anti join (filtering) | `df[!df2, on = "grp" ]`                         | `antijoin(df, df2, on = :grp)`  |
| Semi join (filtering) | `merge(df1, df2[, .(grp)])   `                  | `semijoin(df, df2, on = :grp)`  |


## Comparison with Stata (version 8 and above)

The following table compares the main functions of DataFrames.jl with Stata:

| Operation              | Stata                   | DataFrames.jl                           |
|:-----------------------|:------------------------|:----------------------------------------|
| Reduce multiple values | `collapse (mean) x`     | `combine(df, :x => mean)`               |
| Add new columns        | `egen x_mean = mean(x)` | `transform!(df, :x => mean => :x_mean)` |
| Rename columns         | `rename x x_new`        | `rename!(df, :x => :x_new)`             |
| Pick columns           | `keep x y`              | `select!(df, :x, :y)`                   |
| Pick rows              | `keep if x >= 1`        | `subset!(df, :x => ByRow(x -> x >= 1))` |
| Sort rows              | `sort x`                | `sort!(df, :x)`                         |

Note that the suffix `!` (i.e. `transform!`, `select!`, etc) ensures that the operation transforms the dataframe in place, as in Stata

Some of these functions can be applied to grouped data frames, in which case they operate by group:

| Operation              | Stata                            | DataFrames.jl                               |
|:-----------------------|:---------------------------------|:--------------------------------------------|
| Add new columns        | `egen x_mean = mean(x), by(grp)` | `transform!(groupby(df, :grp), :x => mean)` |
| Reduce multiple values | `collapse (mean) x, by(grp)`     | `combine(groupby(df, :grp), :x => mean)`    |

The table below compares more advanced commands:

| Operation                 | Stata                          | DataFrames.jl                                              |
|:--------------------------|:-------------------------------|:-----------------------------------------------------------|
| Transform certain rows    | `replace x = 0 if x <= 0`      | `transform(df, :x => (x -> ifelse.(x .<= 0, 0, x)) => :x)` |
| Transform several columns | `collapse (max) x (min) y`     | `combine(df, :x => maximum,  :y => minimum)`               |
|                           | `collapse (mean) x y`          | `combine(df, [:x, :y] .=> mean)`                           |
|                           | `collapse (mean) x*`           | `combine(df, names(df, r"^x") .=> mean)`                   |
|                           | `collapse (max) x y (min) x y` | `combine(df, ([:x, :y] .=> [maximum minimum])...)`         |
| Multivariate function     | `egen z = corr(x y)`           | `transform!(df, [:x, :y] => cor => :z)`                    |
| Row-wise                  | `egen z = rowmin(x y)`         | `transform!(df, [:x, :y] => ByRow(min) => :z)`             |
