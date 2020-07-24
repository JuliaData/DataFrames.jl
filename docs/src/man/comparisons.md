The table compares the main functions of DataFrames.jl with the R package dplyr (v1) and Stata (>=v8)
| DataFrames       | dplyr | Stata|
|:------------|:------------|:------------|
|`combine(df, :x => mean)`  | `summarize(df, mean(x))`    | `collapse (mean) x `|
|`transform(df, :x => mean)`   | `mutate(df, mean(x))`    | `egen x_mean = mean(x)`|
|`select(df, :x, :y)`   | `select(df, x, y)`  | `keep x y` |
|`sort(df, :x)`   | `arrange(df, x)`    | `sort x`|
|`filter(:x => x -> x >= 1, df)`   | `filter(df, x >= 1)`  | `keep if x >= 1` |

These functions create a new dataframe, like in dplyr. To mutate the dataframe in place, like in Stata, use the suffix `!` (e.g. `transform!`, `select!`...)

The functions `select`, `transform`, `combine` can also be used on grouped dataframes:
| DataFrames       | dplyr | Stata|
|:------------|:------------|:------------|
|`combine(groupby(df, :id), :x => mean)`  | `summarize(group_by(df, id), mean(x))`    | `collapse (mean) x, by(id)`|
|`transform(groupby(df, :id), :x => mean)`   | `mutate(group_by(df, id), mean(x))`    | `egen x_mean = mean(x), by(id)`|
|`select(groupby(df, :id), :x => mean)`   | `transmute(group_by(df, id), mean(x))`    | |


Here are more complicated examples for `combine`/`transform`/`select`:
| DataFrames       | dplyr | Stata|
|:------------|:------------|:------------|
|`combine(df, :x => mean => :x_mean)`   | `summarize(df, x_mean = mean(x))`    | `collapse (mean) x_mean = x`|
|`combine(df, :x => x -> maximum(x) - minimum(x))`   | `summarize(df, max(x) - min(x))`    | |
|`combine(df, [:x, :y] => cov)`   | `summarize(df, cov(x, y))`    | |
|`combine(df, :x => maximum,  :y => minimum)`   | `summarize(df, max(x), min(y))`    | `collapse (max) x (min) y` |
|`combine(:x => x -> (name = ["minimum", "maximum"], value = [minimum(x), maximum(x)]), df)`   | `summarize(df, tibble(name = c("minimum", "maximum"), value = range(x)))`    | |
|`combine(d -> head(d, 1), df)` | `summarize(df, head(across(), 1))`||
|`combine(df, [:x, :y] .=> mean)`   | `summarize(df, across(c(x, y), mean))`    | `collapse (mean) x y` |
|`combine(df, names(df, r"^x") .=> mean)`   | `summarize(df, across(starts_with("x"), mean))`    | `collapse (mean) x*` |


