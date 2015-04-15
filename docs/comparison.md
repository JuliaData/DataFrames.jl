# Comparison of `DataFrames` with R and Pandas

R and Pandas (with Python) are commonly used data analysis tools. Both allow defining dataframes and provide a range of convenient ways of using them. Here, we translate some of the essential functionality of both languages. This is a good place to start if you know how to work with dataframes in R or Pandas and quickly want to get going using Julia.

If you are instead interested in a general comparison of the languages, you can find that in the [base Julia documentation](http://docs.julialang.org/en/release-0.3/manual/noteworthy-differences/).

For the examples below, we are following the [Pandas documentation](http://pandas.pydata.org/pandas-docs/stable/comparison_with_r.html) as much as possible.

# R

## Indexing

In R, columns of a `data.frame` can be accessed by name:

```R
df <- data.frame(a=rnorm(5), b=rnorm(5), c=rnorm(5), d=rnorm(5), e=rnorm(5))
df[, c("a", "c")]

df <- data.frame(matrix(runiform(1000), ncol=100))
df[, c(1:10, 25:30, 40, 50:100)]
```

Using DataFrames, the equivalent would be the following:

```julia
df = DataFrame(a=randn(5), b=randn(5), c=randn(5), d=randn(5), e=randn(5))
df[[:a, :c]]

data = rand(1000, 100)
df = convert(DataFrame, data)
df[:, [1:10, 25:30, 40, 50:100]]
```

## Aggregating data using `aggregate`

```R
df <- data.frame(
  v1 = c(1,3,5,7,8,3,5,NA,4,5,7,9),
  v2 = c(11,33,55,77,88,33,55,NA,44,55,77,99),
  by1 = c("red", "blue", 1, 2, NA, "big", 1, 2, "red", 1, NA, 12),
  by2 = c("wet", "dry", 99, 95, NA, "damp", 95, 99, "red", 99, NA, NA))
aggregate(x=df[, c("v1", "v2")], by=list(mydf2$by1, mydf2$by2), FUN = mean)
```

In Julia, you would use ``aggregate``

```julia
df = DataFrame(
  v1 = @data([1,3,5,7,8,3,5,NA,4,5,7,9]),
  v2 = @data([11,33,55,77,88,33,55,NA,44,55,77,99]),
  by1 = @data(["red", "blue", 1, 2, NA, "big", 1, 2, "red", 1, NA, 12]),
  by2 = @data(["wet", "dry", 99, 95, NA, "damp", 95, 99, "red", 99, NA, NA]))

aggregate(df, [:by1, :by2], mean)
```

Note that we are using the `@data` macro to correctly handle columns containing missing values (`NA`). If a column does not contain `NA`, the `@data` macro is not needed.

## Using `with` to evaluate an expression within a `data.frame`

In R, you can use `with` to simplify many expressions involving a `data.frame`:

```R
df <- data.frame(a=runiform(10), b=runiform(10)) # a and b are exponentially distributed
with(df, a + b)
df$a + df$b  # same as the previous expression
```

DataFrames does not currently support evaluations inside a DataFrame, so you would write the following:

```julia

df = data.frame(a=rand(10), b=rand(10))
df[:a] + df[:b]
```
To stay closer to the functionality of R's `with`, you can use the experimental [DataFramesMeta](https://github.com/JuliaStats/DataFramesMeta.jl) package. Using this package, you would write:

```julia
using DataFramesMeta
@with(df, :a + :b)
```

Note that this syntax allows you to distinguish between a variable `a` and a column `:a`.

# Pandas

## Indexing

Selecting multiple columns by name:

```python
df = pd.DataFrame(np.random.randn(10, 3), columns=list('abc'))
df[['a', 'c']]
```

In Julia, you would write:

```julia
df = DataFrame(a=randn(10), b=randn(10), c=randn(10))
df[[:a, :c]]
```

The biggest difference is that columns are accessed using symbols like `:a` instead of strings like `"a"`.

Further, note that `DataFrames` does not support named (indexed) rows. For instance, appending to Dataframes using Pandas will, by default, align these dataframes by their row index, which is related to a database join. Instead, `DataFrames` will never re-align DataFrames, because there are no row indices.

## Aggregating using `groupby`

```python
df = pd.DataFrame({
    'v1': [1,3,5,7,8,3,5,np.nan,4,5,7,9],
    'v2': [11,33,55,77,88,33,55,np.nan,44,55,77,99],
    'by1': ["red", "blue", 1, 2, np.nan, "big", 1, 2, "red", 1, np.nan, 12],
    'by2': ["wet", "dry", 99, 95, np.nan, "damp", 95, 99, "red", 99, np.nan, np.nan]
    })

g = df.groupby(['by1','by2'])
g[['v1','v2']].mean()
```

In Julia, you would use ``aggregate``, which you simply pass the function you want to aggregate by:

```julia
df = DataFrame(
  v1 = @data([1,3,5,7,8,3,5,NA,4,5,7,9]),
  v2 = @data([11,33,55,77,88,33,55,NA,44,55,77,99]),
  by1 = @data(["red", "blue", 1, 2, NA, "big", 1, 2, "red", 1, NA, 12]),
  by2 = @data(["wet", "dry", 99, 95, NA, "damp", 95, 99, "red", 99, NA, NA]))

aggregate(df, [:by1, :by2], mean)
```

