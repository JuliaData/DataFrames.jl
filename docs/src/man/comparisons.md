# Comparisons

This section compares DataFrames.jl with other data manipulation frameworks.

A sample data set can be created using the following code:
```julia
df = DataFrame(id = [1,2,1,2,1,2], x = 6:-1:1, y = 4:9)
```

## Comparison with the Python package pandas

The following table compares the main functions of DataFrames.jl with the Python package pandas (version 1.1.0):

```python
df = pd.DataFrame({'id': [1,2,1,2,1,2], 'x': range(6,0,-1), 'y': range(4,10)})
```

| Operations               | pandas                                             | DataFrames.jl                                    |
|:-------------------------|:---------------------------------------------------|:-------------------------------------------------|
| Reduce multiple values   | `df.mean()['x']`                                   | `combine(df, :x => mean)`                        |
| Add new columns          | `df.assign(x_mean = df.mean().x)`                  | `transform(df, :x => mean => :x_mean)`           |
| Rename columns           | `df.rename(columns = {'x': 'x_new'})`              | `rename(df, :x => :x_new)`                       |
| Pick columns             | `df[['x', 'y']]`                                   | `select(df, :x, :y)` or `df[:, [:x, :y]]`        |
| Pick & transform columns | `df.assign(x_mean = df.mean().x)[['x_mean', 'y']]` | `select(df, :x => mean, :y)`                     |
| Pick rows                | `df[df.x >= 3]`                                    | `filter(:x => >=(3), df)` or `df[df.x .>= 3, :]` |
| Sort rows                | `df.sort_values(by = ['x'])`                       | `sort(df, :x)`                                   |

As in pandas, some of these functions can be applied to grouped data frames, in which case they operate by group:

| Operations               | pandas                                                                                 | DataFrames.jl                              |
|:-------------------------|:---------------------------------------------------------------------------------------|:-------------------------------------------|
| Reduce multiple values   | `df.groupby('id')['x'].mean().reset_index()`                                           | `combine(groupby(df, :id), :x => mean)`    |
| Add new columns          | `df.join(df.groupby('id')['x'].mean(), on='id', rsuffix='_mean')`                      | `transform(groupby(df, :id), :x => mean)`  |
| Pick & transform columns | `df.join(df.groupby('id')['x'].mean(), on='id', rsuffix='_mean')[['id','x_mean','y']]` | `select(groupby(df, :id), :x => mean, :y)` |

The table below compares more advanced commands:

| Operations                | pandas                                                                       | DataFrames.jl                                                |
|:--------------------------|:-----------------------------------------------------------------------------|:-------------------------------------------------------------|
| Complex Function          | `df[['x']].agg(lambda v: np.mean(np.cos(v)))`                                | `combine(df, :x => (x -> mean(cos, x)) => :x_mean)`          |
| Transform several columns | `df.agg({'x': max, 'y': min})`                                               | `combine(df, :x => maximum, :y => minimum)`                  |
|                           | `df[['x','y']].mean()`                                                       | `combine(df, [:x, :y] .=> mean)`                             |
|                           | `df.filter(regex=("^x")).mean()`                                             | `combine(df, r"^x" .=> mean)`                                |
|                           | `df[['x', 'y']].agg([max, min]`                                              | `combine(df, ([:x, :y] .=> [maximum minimum])...)`           |
| Multivariate function     | `df.assign(x_y_cor = np.corrcoef(df.x, df.y)[0,1])`                          | `transform(df, [:x, :y] => cor)`                             |
| Row-wise                  | `df.assign(x_y_min = df.apply(lambda v: min(v.x, v.y), axis=1))`             | `transform(df, [:x, :y] => ByRow(min))`                      |
|                           | `df.assign(x_y_argmax = df.apply(lambda v: df.columns[v.argmax()], axis=1))` | `transform(df, AsTable([:x,:y]) => ByRow(argmax))`           |
| DataFrame as input        | `df.groupby('id').head(2)`                                                   | `combine(d -> first(d, 2), groupby(df, :id))`                |
| DataFrame as output       | `df[['x']].agg(['max','min'])`                                               | `combine(:x => x -> (value = [minimum(x), maximum(x)]), df)` |

Notes:
1. Pandas skips `NaN` values in analytic functions by default. DataFrames.jl respect general rules in Julia in propgating missing values.
2. Pandas keeps original column name after performing aggregation. DataFrames.jl appends a suffix automatically.
3. DataFrames.jl currently does not support row index.

## Comparison with the R package dplyr

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1):

|Operations     | dplyr | DataFrames.jl|
|:------------|:------------|:------------|
|Reduce multiple values|`summarize(df, mean(x))`|`combine(df, :x => mean)`|
|Add new columns|`mutate(df, x_mean = mean(x))`|`transform(df, :x => mean => :x_mean)`|
|Rename columns|`rename(df, x_new = x)`|`rename(df, :x => :x_new)`|
|Pick columns|`select(df, x, y)`|`select(df, :x, :y)`|
|Pick & transform columns|`transmute(df, mean(x), y)`|`select(df, :x => mean, :y)`|
|Pick rows |`filter(df, x >= 1)`|`filter(:x => >=(1), df)`|
|Sort rows|`arrange(df, x)`|`sort(df, :x)`|

As in dplyr, some of these functions can be applied to grouped data frames, in which case they operate by group:

|Operations  | dplyr | DataFrames.jl     |
|:------------|:------------|:------------|
|Reduce multiple values|`summarize(group_by(df, id), mean(x))`|`combine(groupby(df, :id), :x => mean)`|
|Add new columns|`mutate(group_by(df, id), mean(x))`|`transform(groupby(df, :id), :x => mean)`|
|Pick & transform columns|`transmute(group_by(df, id), mean(x), y)`|`select(groupby(df, :id), :x => mean, :y)`|


The table below compares more advanced commands:

|Operations|dplyr| DataFrames.jl       |
|:------------|:------------|:------------|
|Complex Function |`summarize(df, mean(x, na.rm = T))`|`combine(df, :x => x -> mean(skipmissing(x)))`|
|Transform several columns |`summarize(df, max(x), min(y))`|`combine(df, :x => maximum,  :y => minimum)`|
||`summarize(df, across(c(x, y), mean))`|`combine(df, [:x, :y] .=> mean)`|
||`summarize(df, across(starts_with("x"), mean))`|`combine(df, names(df, r"^x") .=> mean)`|
||`summarize(df, across(c(x, y), list(max, min)))`|`combine(df, ([:x, :y] .=> [maximum minimum])...)`|
|Multivariate function|`mutate(df, cor(x, y))`|`transform(df, [:x, :y] => cor)`|
|Row-wise|`mutate(rowwise(df), min(x, y))`|`transform(df, [:x, :y] => ByRow(min))`|
||`mutate(rowwise(df), which.max(c_across(matches("^x"))))`|`transform(df, AsTable(r"^x") => ByRow(argmax))`|
|DataFrame as input|`summarize(df, head(across(), 2))`|`combine(d -> first(d, 2), df)`|
|DataFrame as output|`summarize(df, tibble(value = min(x), max(x)))`|`combine(:x => x -> (value = [minimum(x), maximum(x)]), df)`|


## Comparison with Stata (version 8 and above)

The following table compares the main functions of DataFrames.jl with Stata:

|Operations | Stata| DataFrames.jl |
|:------------|:------------|:------------|
|Reduce multiple values|`collapse (mean) x`|`combine(df, :x => mean)`|
|Add new columns|`egen x_mean = mean(x)`|`transform!(df, :x => mean => :x_mean)`|
|Rename columns|`rename x x_new`|`rename!(df, :x => :x_new)`|
|Pick columns|`keep x y`|`select!(df, :x, :y)`|
|Pick rows |`keep if x >= 1`|`filter!(:x => >=(1), df)`|
|Sort rows|`sort x`|`sort!(df, :x)`|

Note that the suffix `!` (i.e. `transform!`, `select!`, etc) ensures that the operation transforms the dataframe in place, as in Stata

Some of these functions can be applied to grouped data frames, in which case they operate by group:

|Operations| Stata| DataFrames.jl |
|:------------|:------------|:------------|
|Add new columns|`egen x_mean = mean(x), by(id)`|`transform!(groupby(df, :id), :x => mean)`|
|Reduce multiple values|`collapse (mean) x, by(id)`|`combine(groupby(df, :id), :x => mean)`|


The table below compares more advanced commands:

|Operations| Stata| DataFrames.jl |
|:------------|:------------|:------------|
|Transform certain rows |`replace x = 0 if x <= 0`|`transform(df, :x => (x -> ifelse.(x .<= 0, 0, x)) => :x)`|
|Transform several columns |`collapse (max) x (min) y`|`combine(df, :x => maximum,  :y => minimum)`|
||`collapse (mean) x y`|`combine(df, [:x, :y] .=> mean)`|
||`collapse (mean) x*`|`combine(df, names(df, r"^x") .=> mean)`|
||`collapse (max) x y (min) x y`|`combine(df, ([:x, :y] .=> [maximum minimum])...)`|
|Multivariate function|`egen z = corr(x y)`|`transform!(df, [:x, :y] => cor => :z)`|
|Row-wise|`egen z = rowmin(x y)`|`transform!(df, [:x, :y] => ByRow(min) => :z)`|
