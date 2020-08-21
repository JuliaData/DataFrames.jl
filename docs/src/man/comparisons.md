# Comparisons

This section compares DataFrames.jl with other data manipulation frameworks.

## Comparison with the R package dplyr

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1)

|Operations| DataFrames.jl       | dplyr |
|:------------|:------------|:------------|
|Reduce multiple values|`combine(df, :x => mean)`|`summarize(df, mean(x))`|
|Add new columns|`transform(df, :x => mean => :x_mean)`|`mutate(df, x_mean = mean(x))`|
|Rename columns|`rename(df, :x => :x_new)`|`rename(df, x_new = x)`|
|Pick columns|`select(df, :x, :y)`|`select(df, x, y)`|
|Pick & transform columns|`select(df, :x => mean, :y)`|`transmute(df, mean(x), y)`|
|Pick rows |`filter(:x => >=(1), df)`|`filter(df, x >= 1)`|
|Sort rows|`sort(df, :x)`|`arrange(df, x)`|

As in dplyr, some of these functions can be applied to grouped data frames, in which case they operate by group:
|Operations| DataFrames.jl       | dplyr |
|:------------|:------------|:------------|
|Reduce multiple values|`combine(grouby(df, :id), :x => mean)`|`summarize(group_by(df, id), mean(x))`
|Add new columns|`transform(grouby(df, :id), :x => mean)`|`mutate(group_by(df, id), mean(x))`
|Pick & transform columns|`select(grouby(df, :id), :x => mean, :y)`|`transmute(group_by(df, id), mean(x), y)`|


The table below compares more complicated syntaxes:

Operations| DataFrames.jl       | dplyr|
|:------------|:------------|:------------|
|Complex Function |`combine(df, :x => x -> mean(skipmissing(x)))`|`summarize(df, mean(x, na.rm = T))`|
|Transform several columns |`combine(df, :x => maximum,  :y => minimum)`|`summarize(df, max(x), min(y))`|
||`combine(df, [:x, :y] .=> mean)`|`summarize(df, across(c(x, y), mean))`|
||`combine(df, names(df, r"^x") .=> mean)`|`summarize(df, across(starts_with("x"), mean))`|
||`combine(df, ([:x, :y] .=> [maximum minimum])...)`|`summarize(df, across(c(x, y), list(max, min)))`|
|Multivariate function|`transform(df, [:x, :y] => cor)`|`mutate(df, cor(x, y))`|
|Row-wise|`transform(df, [:x, :y] => ByRow(min))`|`mutate(rowwise(df), min(x, y))`|
||`transform(df, AsTable(r"^x") => ByRow(argmax))`|`mutate(rowwise(df), which.max(c_across(matches("^x"))))`|
|DataFrame as input|`combine(d -> first(d, 2), df)`|`summarize(df, head(across(), 2))`|
|DataFrame as output|`combine(:x => x -> (value = [minimum(x), maximum(x)]), df)`|`summarize(df, tibble(value = min(x), max(x)))`|


## Comparison with Stata (version 8 and above)

The following table compares the main functions of DataFrames.jl with Stata 

|Operations| DataFrames.jl | Stata|
|:------------|:------------|:------------|
|Reduce multiple values|`combine(df, :x => mean)`|`collapse (mean) x`|
|Add new columns|`transform!(df, :x => mean => :x_mean)`|`egen x_mean = mean(x)`|
|Rename columns|`rename!(df, :x => :x_new)`|`rename x x_new`|
|Pick columns|`select!(df, :x, :y)`|`keep x y`|
|Pick rows |`filter!(:x => >=(1), df)`|`keep if x >= 1`|
|Sort rows|`sort!(df, :x)`|`sort x`|

Note that the suffix `!` (i.e. `transform!`, `select!`, etc) ensures that the operation transforms the dataframe in place, as in Stata

Some of these functions can be applied to grouped data frames, in which case they operate by group:
|Operations| DataFrames.jl     | Stata|
|:------------|:------------|:------------|
|Add new columns|`transform!(groupby(df, :id), :x => mean)`|`egen x_mean = mean(x), by(id)`|
|Reduce multiple values|`combine(groupby(df, :id), :x => mean)`|`collapse (mean) x, by(id)`|


The table below compares more complicated syntaxes:

Operations| DataFrames.jl | Stata|
|:------------|:------------|:------------|
|Transform certain rows |`transform(df, [:x, :y] => ((x, y)-> ifelse.(y .>= 1, 1, x)) => :x)`|`replace x = 1 if y >= 1`|
|Transform several columns |`combine(df, :x => maximum,  :y => minimum)`|`collapse (max) x (min) y`|
||`combine(df, [:x, :y] .=> mean)`|`collapse (mean) x y`|
||`combine(df, names(df, r"^x") .=> mean)`|`collapse (mean) x*`|
||`combine(df, ([:x, :y] .=> [maximum minimum])...)`|`collapse (max) x y (min) x y`|
|Multivariate function|`transform!(df, [:x, :y] => cor => :z)`|`egen z = corr(x y)`|
|Row-wise|`transform!(df, [:x, :y] => ByRow(min) => :z)`|`egen z = rowmin(x y)`|
