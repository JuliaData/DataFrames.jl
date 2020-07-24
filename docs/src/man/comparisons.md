The table compares the main functions of DataFrames.jl with the R package dplyr (v1) and Stata (>=v8)
|Operations| DataFrames.jl       | dplyr | Stata|
|:------------|:------------|:------------|:------------|
|Reduce multiple values|`combine(df, :x => mean)`|`summarize(df, mean(x))`|`collapse (mean) x =`|
|Add new columns|`transform(df, :x => mean => :x_mean)`|`mutate(df, x_mean = mean(x))`|`egen x_mean = mean(x)`|
|Pick columns|`select(df, :x, :y)`|`select(df, x, y)`|`keep x y`|
|Pick rows |`filter(:x => x -> x >= 1, df)`|`filter(df, x >= 1)`|`keep if x >= 1`|
|Sort rows|`sort(df, :x)`|`arrange(df, x)`|`sort x`|
|Rename columns|`rename(df, :x => :v)`|`rename(df, v = x)`|`rename x v`|

These functions create new dataframes (like in dplyr). To mutate dataframes in place (like in Stata), use the suffix `!` (e.g. `transform!`, `select!`, etc)

The functions `select`, `transform`, `combine` can be applied on grouped dataframes:
| DataFrames.jl       | dplyr | Stata|
|:------------|:------------|:------------|
|`combine(groupby(df, :id), :x => mean)`|`summarize(group_by(df, id), mean(x))`|`collapse (mean) x, by(id)`|
|`transform(groupby(df, :id), :x => mean)`|`mutate(group_by(df, id), mean(x))`|`egen x_mean = mean(x), by(id)`|
|`select(groupby(df, :id), :x => mean)`|`transmute(group_by(df, id), mean(x))`||


The table compares more complicated syntaxes 
Operations| DataFrames       | dplyr| Stata|
|:------------|:------------|:------------|:------------|
|Several columns |`combine(df, :x => maximum,  :y => minimum)`|`summarize(df, max(x), min(y))`|`collapse (max) x (min) y`|
||`combine(df, [:x, :y] .=> mean)`|`summarize(df, across(c(x, y), mean))`|`collapse (mean) x y`|
||`combine(df, names(df, r"^x") .=> mean)`|`summarize(df, across(starts_with("x"), mean))`|`collapse (mean) x*`|
|Multivariate function|`transform(df, [:x, :y] => cov)`|`mutate(df, cov(x, y))`|`egen z = corr(x y)`|
|Row-wise|`transform(df, [:x, :y] => ByRow(min)`|`mutate(rowwise(df), min(x, y))`|`egen z = rowmin(x y)`|
||`transform(df, AsTable(names(df, r"^x")) => ByRow(sum))`|`mutate(rowwise(df), sum(c_across(starts_with("x"))))`|`egen z = rowtotal(x*)`|
|DataFrame as output|`combine(:x => x -> (name = ["minimum", "maximum"], value = [minimum(x), maximum(x)]), df)`|`summarize(df, tibble(name = c("minimum", "maximum"), value = range(x)))`||
|DataFrame as input|`combine(d -> first(d, 2), df)`|`summarize(df, head(across(), 2))`||
