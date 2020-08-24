# Comparisons

This section compares DataFrames.jl with other data manipulation frameworks.

A sample data set can be created using the following code:

```julia
df = DataFrame(id = 'a':'f', grp = [1,2,1,2,1,2], x = 6:-1:1, y = 4:9, z = 3:8)
```

## Comparison with the Python package pandas

The following table compares the main functions of DataFrames.jl with the Python package pandas (version 1.1.0):

```python
df = pd.DataFrame(
        {'grp': [1,2,1,2,1,2], 'x': range(6,0,-1), 'y': range(4,10), 'z': range(3,9)},
        index = list('abcdef'))
```

Note that pandas allows row indexing by labels. For DataFrames.jl, you can achieve the same result via row filtering as shown below.

How to access data:

| Operation                  | Example                     | pandas                 | DataFrames.jl                                              |
|:---------------------------|:----------------------------|:-----------------------|:-----------------------------------------------------------|
| Cell indexing by location  | Cell at row 2, column 2     | `df.iloc[1,1]`         | `df[2,2]` (1-based index)*                                 |
| Row slicing by location    | Rows 2 and 3                | `df.iloc[1:3]`         | `df[2:3, :]`                                               |
| Column slicing by location | Column 2 and after          | `df.iloc[:, 1:]`       | `df[:, 2:end]`*                                            |
| Row indexing by label      | Row 'c'                     | `df.loc['c']`          | `df[df.id .== 'c', :]`                                     |
| Column indexing by label   | Column 'x'                  | `df.loc[:, 'x']`       | `df[:, :x]`                                                |
| Column slicing by label    | Columns 'x' and 'z'         | `df.loc[:, ['x','z']]` | `df[:, [:x, :z]]`  of `select(df, [:x, :y])`               |
|                            | Columns between 'x' and 'z' | `df.loc[:, 'x':'z']`   | `df[:, Between(:x, :z)]`  of `select(df, Between(:x, :y))` |
| Mixed indexing             | Cell at row 'c', column 2   | `df.loc['c'][1]`       | `df[df.id .== 'c', 2]`*                                    |

Note: (*) The column index for DataFrames.jl are off by 1 due to an additional `id` column.

Common operations:

| Operation                | pandas                                             | DataFrames.jl                                    |
|:-------------------------|:---------------------------------------------------|:-------------------------------------------------|
| Reduce multiple values   | `df.mean()['x']`                                   | `combine(df, :x => mean)`                        |
| Add new columns          | `df.assign(x_mean = df.mean().x)`                  | `transform(df, :x => mean => :x_mean)`           |
| Rename columns           | `df.rename(columns = {'x': 'x_new'})`              | `rename(df, :x => :x_new)`                       |
| Pick & transform columns | `df.assign(x_mean = df.mean().x)[['x_mean', 'y']]` | `select(df, :x => mean, :y)`                     |
| Sort rows                | `df.sort_values(by = ['x'])`                       | `sort(df, :x)`                                   |

Grouping data and aggregation:

| Operation                    | pandas                                                                                    | DataFrames.jl                               |
|:-----------------------------|:------------------------------------------------------------------------------------------|:--------------------------------------------|
| Aggregate by groups          | `df.groupby('grp')['x'].mean().reset_index()`                                             | `combine(groupby(df, :grp), :x => mean)`    |
| Aggregate and add columns    | `df.join(df.groupby('grp')['x'].mean(), on='grp', rsuffix='_mean')`                       | `transform(groupby(df, :grp), :x => mean)`  |
| Aggregate and select columns | `df.join(df.groupby('grp')['x'].mean(), on='grp', rsuffix='_mean')[['grp','x_mean','y']]` | `select(groupby(df, :grp), :x => mean, :y)` |

More advanced commands:

| Operation                 | pandas                                                                       | DataFrames.jl                                                 |
|:--------------------------|:-----------------------------------------------------------------------------|:--------------------------------------------------------------|
| Complex Function          | `df[['x']].agg(lambda v: np.mean(np.cos(v)))`                                | `combine(df, :x => (x -> mean(cos, x)) => :x_mean)`           |
| Transform several columns | `df.agg({'x': max, 'y': min})`                                               | `combine(df, :x => maximum, :y => minimum)`                   |
|                           | `df[['x','y']].mean()`                                                       | `combine(df, [:x, :y] .=> mean)`                              |
|                           | `df.filter(regex=("^x")).mean()`                                             | `combine(df, r"^x" .=> mean)`                                 |
|                           | `df[['x', 'y']].agg([max, min])`                                             | `combine(df, ([:x, :y] .=> [maximum minimum])...)`            |
| Multivariate function     | `df.assign(x_y_cor = np.corrcoef(df.x, df.y)[0,1])`                          | `transform(df, [:x, :y] => cor)`                              |
| Row-wise                  | `df.assign(x_y_min = df.apply(lambda v: min(v.x, v.y), axis=1))`             | `transform(df, [:x, :y] => ByRow(min))`                       |
|                           | `df.assign(x_y_argmax = df.apply(lambda v: df.columns[v.argmax()], axis=1))` | `transform(df, AsTable([:x,:y]) => ByRow(argmax))`            |
| DataFrame as input        | `df.groupby('grp').head(2)`                                                  | `combine(d -> first(d, 2), groupby(df, :grp))`                |
| DataFrame as output       | `df[['x']].agg(['max','min'])`                                               | `combine(:x => x -> (value = [minimum(x), maximum(x)],), df)` |

Joining data frames:

| Operation             | pandas                                                  | DataFrames.jl                           |
|:----------------------|:--------------------------------------------------------|:----------------------------------------|
| Inner join            | `pd.merge(df1, df2, how = 'inner', on = ['id', 'grp'])` | `innerjoin(df1, df2, on = [:id, :grp])` |
| Outer join            | `pd.merge(df1, df2, how = 'outer', on = ['id', 'grp'])` | `outerjoin(df1, df2, on = [:id, :grp])` |
| Left join             | `pd.merge(df1, df2, how = 'left', on = ['id', 'grp'])`  | `leftjoin(df1, df2, on = [:id, :grp])`  |
| Right join            | `pd.merge(df1, df2, how = 'right', on = ['id', 'grp'])` | `rightjoin(df1, df2, on = [:id, :grp])` |
| Semi join (filtering) | `df1[df1.id.isin(df2.id)]`                              | `semijoin(df1, df2, on = :id)`          |
| Anti join (filtering) | `df1[~df1.id.isin(df2.id)]`                             | `antijoin(df1, df2, on = :id)`          |

Additional Differences
1. Pandas skips `NaN` values in analytic functions by default.  In Julia `NaN` is just a normal floating point value, and instead a special `missing` value is used to indicate missing data. DataFrames.jl respects general rules in Julia in propagating `missing` values by default.
2. Pandas keeps original column name after performing aggregation. DataFrames.jl appends a suffix automatically.

## Comparison with the R package dplyr

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1):

```R
df <- tibble(id = c('a','b','c','d','e','f'),
        grp = c(1,2,1,2,1,2),
        x = c(6,5,4,3,2,1),
        y = c(4,5,6,7,8,9),
        z = c(3,4,5,6,7,8))
```

| Operation                | dplyr                          | DataFrames.jl                          |
|:-------------------------|:-------------------------------|:---------------------------------------|
| Reduce multiple values   | `summarize(df, mean(x))`       | `combine(df, :x => mean)`              |
| Add new columns          | `mutate(df, x_mean = mean(x))` | `transform(df, :x => mean => :x_mean)` |
| Rename columns           | `rename(df, x_new = x)`        | `rename(df, :x => :x_new)`             |
| Pick columns             | `select(df, x, y)`             | `select(df, :x, :y)`                   |
| Pick & transform columns | `transmute(df, mean(x), y)`    | `select(df, :x => mean, :y)`           |
| Pick rows                | `filter(df, x >= 1)`           | `filter(:x => >=(1), df)`              |
| Sort rows                | `arrange(df, x)`               | `sort(df, :x)`                         |

As in dplyr, some of these functions can be applied to grouped data frames, in which case they operate by group:

| Operation                | dplyr                                      | DataFrames.jl                               |
|:-------------------------|:-------------------------------------------|:--------------------------------------------|
| Reduce multiple values   | `summarize(group_by(df, grp), mean(x))`    | `combine(groupby(df, :grp), :x => mean)`    |
| Add new columns          | `mutate(group_by(df, grp), mean(x))`       | `transform(groupby(df, :grp), :x => mean)`  |
| Pick & transform columns | `transmute(group_by(df, grp), mean(x), y)` | `select(groupby(df, :grp), :x => mean, :y)` |

The table below compares more advanced commands:

| Operation                 | dplyr                                                     | DataFrames.jl                                                |
|:--------------------------|:----------------------------------------------------------|:-------------------------------------------------------------|
| Complex Function          | `summarize(df, mean(x, na.rm = T))`                       | `combine(df, :x => x -> mean(skipmissing(x)))`               |
| Transform several columns | `summarize(df, max(x), min(y))`                           | `combine(df, :x => maximum,  :y => minimum)`                 |
|                           | `summarize(df, across(c(x, y), mean))`                    | `combine(df, [:x, :y] .=> mean)`                             |
|                           | `summarize(df, across(starts_with("x"), mean))`           | `combine(df, names(df, r"^x") .=> mean)`                     |
|                           | `summarize(df, across(c(x, y), list(max, min)))`          | `combine(df, ([:x, :y] .=> [maximum minimum])...)`           |
| Multivariate function     | `mutate(df, cor(x, y))`                                   | `transform(df, [:x, :y] => cor)`                             |
| Row-wise                  | `mutate(rowwise(df), min(x, y))`                          | `transform(df, [:x, :y] => ByRow(min))`                      |
|                           | `mutate(rowwise(df), which.max(c_across(matches("^x"))))` | `transform(df, AsTable(r"^x") => ByRow(argmax))`             |
| DataFrame as input        | `summarize(df, head(across(), 2))`                        | `combine(d -> first(d, 2), df)`                              |
| DataFrame as output       | `summarize(df, tibble(value = c(min(x), max(x))))`        | `combine(:x => x -> (value = [minimum(x), maximum(x)],), df)` |

## Comparison with Stata (version 8 and above)

The following table compares the main functions of DataFrames.jl with Stata:

| Operation              | Stata                   | DataFrames.jl                           |
|:-----------------------|:------------------------|:----------------------------------------|
| Reduce multiple values | `collapse (mean) x`     | `combine(df, :x => mean)`               |
| Add new columns        | `egen x_mean = mean(x)` | `transform!(df, :x => mean => :x_mean)` |
| Rename columns         | `rename x x_new`        | `rename!(df, :x => :x_new)`             |
| Pick columns           | `keep x y`              | `select!(df, :x, :y)`                   |
| Pick rows              | `keep if x >= 1`        | `filter!(:x => >=(1), df)`              |
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
