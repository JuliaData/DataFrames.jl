DataFrames.jl
=========

Package for working with tabular data in Julia using `DataFrame`'s.

# Installation

DataFrames.jl is now an installable package. If you have not
initialized the package system before, you will need to do the
following:

```julia
load("pkg.jl")
Pkg.init()
```

To install DataFrames.jl, use the following:

```julia
load("pkg.jl")   # if not done previously
Pkg.add("DataFrames")
```

DataFrames.jl has one main module named `DataFrames`. You can load it as:

```julia
load("DataFrames")
using DataFrames
```

# Features

* `DataFrame` for efficient tabular storage of two-dimensional data
* Minimized data copying
* Default columns can handle missing values (NA's) of any type
* `PooledDataFrame` for efficient storage of factor-like arrays for
  characters, integers, and other types
* Flexible indexing
* `SubDataFrame` for efficient subset referencing without copies
* Grouping operations inspired by [plyr](http://plyr.had.co.nz/),
  [pandas](http://pandas.pydata.org/), and
  [data.table](http://cran.r-project.org/web/packages/data.table/index.html)
* Basic `merge` functionality
* `stack` and `unstack` for long/wide conversions
* Pipelining support (`|`) for many operations
* Several typical R-style functions, including `head`, `tail`, `summary`,
  `unique`, `duplicated`, `with`, `within`, and more
* Formula and design matrix implementation

# Demos

Here's a minimal demo showing some grouping operations:

```julia
julia> load("DataFrames")

julia> using DataFrames

julia> d = DataFrame(quote     # expressions are one way to create a DataFrame
           x = randn(10)
           y = randn(10)
           i = randi(3,10)
           j = randi(3,10)
       end);

julia> dump(d)    # dump() is like R's str()
DataFrame  10 observations of 4 variables
  x: DataVec{Float64}(10) [-0.22496343871037897,-0.4033933555989207,0.6027847717547058,0.06671669747901597]
  y: DataVec{Float64}(10) [0.21904975091285417,-1.3275512477731726,2.266353546459277,-0.19840910239041679]
  i: DataVec{Int64}(10) [2,1,3,1]
  j: DataVec{Int64}(10) [3,2,1,2]

julia> head(d)
DataFrame  (6,4)
                x         y i j
[1,]    -0.224963   0.21905 2 3
[2,]    -0.403393  -1.32755 1 2
[3,]     0.602785   2.26635 3 1
[4,]    0.0667167 -0.198409 1 2
[5,]      1.68303  -1.11183 1 3
[6,]     0.346034   1.68227 2 1

julia> d[1:3, ["x","y"]]     # indexing is similar to R's
DataFrame  (3,2)
                x        y
[1,]    -0.224963  0.21905
[2,]    -0.403393 -1.32755
[3,]     0.602785  2.26635

julia> # Group on column i, and pipe (|) that result to an expression
julia> # that creates the column x_sum. 
julia> groupby(d, "i") | :(x_sum = sum(x))     
DataFrame  (3,2)
        i    x_sum
[1,]    1  2.06822
[2,]    2 -1.80867
[3,]    3 0.319517

julia> groupby(d, "i") | :sum   # Another way to operate on a grouping
DataFrame  (3,4)
        i    x_sum    y_sum j_sum
[1,]    1  2.06822 -2.73985     8
[2,]    2 -1.80867  1.83489     7
[3,]    3 0.319517  1.03072     2
```

See [demo/workflow_demo.jl](https://github.com/HarlanH/DataFrames.jl/blob/master/demo/workflow_demo.jl) for a basic demo of the parts of a Julian data workflow.

See [demo/design_demo.jl](https://github.com/HarlanH/DataFrames.jl/blob/master/demo/design_demo.jl) for a more in-depth demo of DataFrame and related types and
library.


# Documentation

* [Library-style function reference](https://github.com/HarlanH/DataFrames.jl/blob/master/spec/FunctionReference.md)
* [Background and motivation](https://github.com/HarlanH/DataFrames.jl/blob/master/spec/Motivation.md)


# Development work

The [Issues](https://github.com/HarlanH/DataFrames.jl/issues) highlight a
number of issues and ideas for enhancements. Here are some particular
enhancements under way or under discussion:

* _Data Streams:_
[issue 34](https://github.com/HarlanH/DataFrames.jl/issues/34), [doc](https://github.com/HarlanH/DataFrames.jl/blob/master/spec/DataStream.md)

* _Distributed DataFrames:_ [issue 26](https://github.com/HarlanH/DataFrames.jl/issues/26)

* _Fast vector indexing:_
  [issue 24](https://github.com/HarlanH/DataFrames.jl/issues/24), [commit](https://github.com/HarlanH/DataFrames.jl/commit/268faa1c3b9fa2aa3e0c1199d626fe5a83ad1604)

* _Bitstypes with NA support:_ [issue 45](https://github.com/HarlanH/DataFrames.jl/issues/45), [doc](https://github.com/tshort/DataFrames.jl/blob/bitstypeNA/spec/MissingValues.md)

# Possible changes to Julia

DataFrames fit well with Julia's syntax, but some features would
improve the user experience, including keyword function arguments
[(Julia issue 485)](https://github.com/JuliaLang/julia/issues/485),
`"~"` for easier expression syntax, and overloading `"."` for easier
column access (df.colA). See
[here](https://github.com/HarlanH/DataFrames.jl/blob/master/spec/JuliaChanges.md)
for a bit more information.

# Current status

Please consider this a development preview. Many things work, but
expect some rough edges. We hope that this can become a standard Julia
package.
