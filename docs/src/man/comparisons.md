# Comparisons

This section compares DataFrames.jl with other data manipulation frameworks.

## Comparison with the R package dplyr

The following table compares the main functions of DataFrames.jl with the R package dplyr (version 1)

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
|Reduce multiple values|`summarize(group_by(df, id), mean(x))`|`combine(grouby(df, :id), :x => mean)`|
|Add new columns|`mutate(group_by(df, id), mean(x))`|`transform(grouby(df, :id), :x => mean)`|
|Pick & transform columns|`transmute(group_by(df, id), mean(x), y)`|`select(grouby(df, :id), :x => mean, :y)`|


The table below compares more advanced commands:

Operations|dplyr| DataFrames.jl       | 
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

The following table compares the main functions of DataFrames.jl with Stata 

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
