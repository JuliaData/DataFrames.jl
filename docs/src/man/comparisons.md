# Comparisons with other languages

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1) and Stata (version 8 and above)
|Operations| DataFrames.jl       | dplyr | Stata|
|:------------|:------------|:------------|:------------|
|Reduce multiple values|`combine(df, :x => mean)`|`summarize(df, mean(x))`|`collapse (mean) x =`|
|Add new columns|`transform(df, :x => mean => :x_mean)`|`mutate(df, x_mean = mean(x))`|`egen x_mean = mean(x)`|
|Rename columns|`rename(df, :x => :v)`|`rename(df, v = x)`|`rename x v`|
|Pick columns|`select(df, :x, :y)`|`select(df, x, y)`|`keep x y`|
|Pick & transform columns|`select(df, :x => mean)`|`transmute(df, mean(x), y)`||
|Pick rows |`filter(:x => >=(1), df)`|`filter(df, x >= 1)`|`keep if x >= 1`|
|Sort rows|`sort(df, :x)`|`arrange(df, x)`|`sort x`|

These functions create new data frames (like in dplyr). To mutate data frames in place (like in Stata), use the suffix `!` (e.g. `transform!`, `select!`, etc)

The functions `select`, `transform` and `combine` can be applied on grouped data frames, in which case they operate by group:
|Operations| DataFrames.jl       | dplyr | Stata|
|:------------|:------------|:------------|:------------|
|Reduce multiple values|`combine(groupby(df, :id), :x => mean)`|`summarize(group_by(df, id), mean(x))`|`collapse (mean) x, by(id)`|
|Add new columns|`transform(groupby(df, :id), :x => mean)`|`mutate(group_by(df, id), mean(x))`|`egen x_mean = mean(x), by(id)`|
|Pick columns|`select(groupby(df, :id), :x => mean)`|`transmute(group_by(df, id), mean(x))`||


Finally, the table below compares more complicated syntaxes:
Operations| DataFrames       | dplyr| Stata|
|:------------|:------------|:------------|:------------|
|Transform several columns |`combine(df, :x => maximum,  :y => minimum)`|`summarize(df, max(x), min(y))`|`collapse (max) x (min) y`|
||`combine(df, [:x, :y] .=> mean)`|`summarize(df, across(c(x, y), mean))`|`collapse (mean) x y`|
||`combine(df, ([:x, :y] .=> [maximum minimum])...)`|`summarize(df, across(c(x, y), list(max, min)))`|`collapse (max) x y (min) x y`|
||`combine(df, names(df, r"^x") .=> mean)`|`summarize(df, across(starts_with("x"), mean))`|`collapse (mean) x*`|
|Multivariate function|`transform(df, [:x, :y] => cor)`|`mutate(df, cor(x, y))`|`egen z = corr(x y)`|
|Row-wise|`transform(df, [:x, :y] => ByRow(min))`|`mutate(rowwise(df), min(x, y))`|`egen z = rowmin(x y)`|
||`transform(df, AsTable(r"^x") => ByRow(argmax))`|`mutate(rowwise(df), which.max(c_across(starts_with("x"))))`||
|DataFrame as input|`combine(d -> first(d, 2), df)`|`summarize(df, head(across(), 2))`||
|DataFrame as output|`combine(:x => x -> (value = [minimum(x), maximum(x)]), df)`|`summarize(df, tibble(value = min(x), max(x)))`||
