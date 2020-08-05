# DataFrames.jl

Welcome to the DataFrames documentation!

This resource aims to teach you everything you need
to know to get up and running with tabular data manipulation using the DataFrames.jl package.
For more illustrations of its usage in conjunction with other packages, the
[DataFrames Tutorial using Jupyter Notebooks](https://github.com/bkamins/Julia-DataFrames-Tutorial/)
is a good complementary resource.

If there is something you expect DataFrames to be capable of, but
cannot figure out how to do, please reach out with questions in Domains/Data on
[Discourse](https://discourse.julialang.org/new-topic?title=[DataFrames%20Question]:%20&body=%23%20Question:%0A%0A%23%20Dataset%20(if%20applicable):%0A%0A%23%20Minimal%20Working%20Example%20(if%20applicable):%0A&category=Domains/Data&tags=question). Additionally you might want to listen to an introduction to DataFrames.jl on [JuliaAcademy](https://juliaacademy.com/p/introduction-to-dataframes-jl).

Please report bugs by
[opening an issue](https://github.com/JuliaData/DataFrames.jl/issues/new).

You can follow
the **source** links throughout the documentation to jump right to the
source files on GitHub to make pull requests for improving the documentation and function
capabilities.

Please review
[DataFrames contributing guidelines](https://github.com/JuliaData/DataFrames.jl/blob/master/CONTRIBUTING.md)
before submitting your first PR!

Information on specific versions can be found on the [Release page](https://github.com/JuliaData/DataFrames.jl/releases).

## Package Manual

```@contents
Pages = ["man/getting_started.md",
         "man/joins.md",
         "man/split_apply_combine.md",
         "man/reshaping_and_pivoting.md",
         "man/sorting.md",
         "man/categorical.md",
         "man/missing.md",
         "man/querying_frameworks.md"]
Depth = 2
```

## API

Only exported (i.e. available for use without `DataFrames.` qualifier after
loading the DataFrames.jl package with `using DataFrames`) types and functions
are considered a part of the public API of the DataFrames.jl package. In general
all such objects are documented in this manual (in case some documentation is
missing please kindly report an issue
[here](https://github.com/JuliaData/DataFrames.jl/issues/new)).

All types and functions that are part of public API are guaranteed to go through
a deprecation period before being changed or removed.

Please be warned that while Julia allows you to access internal functions or
types of DataFrames.jl these can change without warning between versions of
DataFrames.jl. In particular it is not safe to directly access fields of types
that are a part of public API of the DataFrames.jl package using e.g. the
`getfield` function. Whenever some operation on fields of defined types is
considered allowed an appropriate exported function should be used instead.

```@contents
Pages = ["lib/types.md", "lib/functions.md", "lib/indexing.md"]
Depth = 2
```

## Index

```@index
Pages = ["lib/types.md", "lib/functions.md"]
```
