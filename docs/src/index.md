# DataFrames.jl

Welcome to the DataFrames.jl documentation!

This resource aims to teach you everything you need to know to get up and
running with tabular data manipulation using the DataFrames.jl package.

For more illustrations of DataFrames.jl usage, in particular in conjunction with
other packages you can check-out the following resources
(they are kept up to date with the released version of DataFrames.jl):
* [DataFrames.jl: Flexible and Fast Tabular Data in Julia](https://www.jstatsoft.org/article/view/v107i04) article published in the *Journal of Statistical Software*
* [Data Wrangling with DataFrames.jl Cheat Sheet](https://www.ahsmart.com/pub/data-wrangling-with-data-frames-jl-cheat-sheet/)
* [DataFrames Tutorial using Jupyter Notebooks](https://github.com/bkamins/Julia-DataFrames-Tutorial/)
* [Julia Academy DataFrames.jl tutorial](https://github.com/JuliaAcademy/DataFrames)
* [JuliaCon 2023](https://github.com/bkamins/JuliaCon2023-Tutorial),
  [JuliaCon 2022](https://github.com/bkamins/JuliaCon2022-DataFrames-Tutorial),
  [JuliaCon 2021](https://github.com/bkamins/JuliaCon2021-DataFrames-Tutorial),
  [JuliaCon 2020](https://github.com/bkamins/JuliaCon2020-DataFrames-Tutorial),
  [JuliaCon 2019](https://github.com/bkamins/JuliaCon2019-DataFrames-Tutorial),
  [ODSC Europe 2021](https://github.com/bkamins/ODSC-EUROPE-2021) tutorials,
  and [PyData Global 2020](https://github.com/bkamins/PyDataGlobal2020)
* [DataFrames.jl showcase](https://github.com/bkamins/DataFrames-Showcase)

If you prefer to learn DataFrames.jl from a book you can consider reading:
* [Julia for Data Analysis](https://github.com/bkamins/JuliaForDataAnalysis);
* [Julia Data Science](https://juliadatascience.io/).

## What is DataFrames.jl?

DataFrames.jl provides a set of tools for working with tabular data in Julia.
Its design and functionality are similar to those of
[pandas](https://pandas.pydata.org/) (in Python) and `data.frame`,
[`data.table`](https://rdatatable.gitlab.io/data.table/)
and [dplyr](https://dplyr.tidyverse.org/) (in R),
making it  a great general purpose data science tool.

DataFrames.jl plays a central role in the Julia Data ecosystem, and has tight
integrations with a range of different libraries. DataFrames.jl isn't the only
tool for working with tabular data in Julia -- as noted below, there are some
other great libraries for certain use-cases -- but it provides great data
wrangling functionality through a familiar interface.

To understand the toolchain in more detail, have a look at the tutorials in this manual.
New users can start with the [First Steps with DataFrames.jl](@ref) section.

You may find the
[DataFramesMeta.jl](https://juliadata.github.io/DataFramesMeta.jl/stable/)
package or one of the other convenience packages discussed in the [Data
manipulation frameworks](@ref) section of this manual helpful when writing more
advanced data transformations, especially if you do not have a significant
programming experience. These packages provide convenience syntax similar to
[dplyr](https://dplyr.tidyverse.org/) in R.

If you use metadata when working with DataFrames.jl you might find the
[TableMetadataTools.jl](https://github.com/JuliaData/TableMetadataTools.jl)
package useful. This package defines several convenience functions for
performing typical metadata operations.

## DataFrames.jl and the Julia Data Ecosystem

The Julia data ecosystem can be a difficult space for new users to navigate, in
part because the Julia ecosystem tends to distribute functionality across
different libraries more than some other languages. Because many people coming
to DataFrames.jl are just starting to explore the Julia data ecosystem, below is
a list of well-supported libraries that provide different data science tools,
along with a few notes about what makes each library special, and how well
integrated they are with DataFrames.jl.


- **Statistics**
    - [StatsKit.jl](https://github.com/JuliaStats/StatsKit.jl): A convenience
      meta-package which loads a set of essential packages for statistics,
      including those mentioned below in this section and DataFrames.jl itself.
    - [Statistics](https://docs.julialang.org/en/v1/stdlib/Statistics/):
      The Julia standard library comes with a wide range of statistics functionality,
      but to gain access to these functions you must call `using Statistics`.
    - [LinearAlgebra](https://docs.julialang.org/en/v1/stdlib/LinearAlgebra/):
      Like `Statistics`, many linear algebra features (factorizations, inversions, etc.)
      live in a library you have to load to use.
    - [SparseArrays](https://docs.julialang.org/en/v1/stdlib/SparseArrays/)
      are also in the standard library but must be loaded to be used.
    - [FreqTables.jl](https://github.com/nalimilan/FreqTables.jl): Create
      frequency tables / cross-tabulations. Tightly integrated with DataFrames.jl.
    - [HypothesisTests.jl](https://juliastats.org/HypothesisTests.jl/stable/):
      A range of hypothesis testing tools.
    - [GLM.jl](https://juliastats.org/GLM.jl/stable/manual/): Tools for estimating
      linear and generalized linear models. Tightly integrated with DataFrames.jl.
    - [StatsModels.jl](https://juliastats.org/StatsModels.jl/stable/):
      For converting heterogeneous `DataFrame` into homogeneous matrices for use
      with linear algebra libraries or machine learning applications that don't
      directly support `DataFrame`s. Will do things like convert categorical
      variables into indicators/one-hot-encodings, create interaction terms, etc.
    - [MultivariateStats.jl](https://multivariatestatsjl.readthedocs.io/en/stable/index.html):
      linear regression, ridge regression, PCA, component analyses tools.
      Not well integrated with DataFrames.jl,
      but easily used in combination with `StatsModels`.
- **Machine Learning**
    - [MLJ.jl](https://github.com/alan-turing-institute/MLJ.jl):
      if you're more of an applied user, there is a single package the pulls
      from all these different libraries and provides a single, scikit-learn
      inspired API: MLJ.jl. MLJ.jl provides a common interface for a wide range
      of machine learning algorithms.
    - [ScikitLearn.jl](https://cstjean.github.io/ScikitLearn.jl/stable/):
      A Julia wrapper around the full Python scikit-learn machine learning library.
      Not well integrated with DataFrames.jl, but can be combined using StatsModels.jl.
    - [AutoMLPipeline](https://github.com/IBM/AutoMLPipeline.jl):
      A package that makes it trivial to create complex ML
      pipeline structures using simple expressions. It leverages
      on the built-in macro programming features of Julia to
      symbolically process, manipulate pipeline expressions,
      and makes it easy to discover optimal structures for
      machine learning regression and classification.
    - Deep learning:
      [KNet.jl](https://denizyuret.github.io/Knet.jl/stable/tutorial/#Introduction-to-Knet-1)
      and [Flux.jl](https://github.com/FluxML/Flux.jl).
- **Plotting**
    - [Plots.jl](http://docs.juliaplots.org/latest/): Powerful, modern plotting
      library with a syntax akin to that of [matplotlib](https://matplotlib.org/)
      (in Python) or `plot` (in R).
      [StatsPlots.jl](http://docs.juliaplots.org/latest/tutorial/#Using-Plot-Recipes-1)
      provides Plots.jl with recipes for many standard statistical plots.
    - [Gadfly.jl](http://gadflyjl.org/stable/): High-level plotting library with
      a "grammar of graphics" syntax akin to that of
      [ggplot](https://ggplot2.tidyverse.org/reference/ggplot.html) (in R).
    - [AlgebraOfGraphics.jl](http://juliaplots.org/AlgebraOfGraphics.jl/stable/): A
      "grammar of graphics" library build upon [Makie.jl](https://docs.makie.org/stable/).
    - [VegaLite.jl](https://www.queryverse.org/VegaLite.jl/stable/): High-level
      plotting library that uses a different "grammar of graphics" syntax and has
      an emphasis on interactive graphics.
- **Data Wrangling**:
    - [Impute.jl](https://github.com/invenia/Impute.jl):
      various methods for handling missing data in vectors, matrices and tables.
    - [DataFramesMeta.jl](https://github.com/JuliaData/DataFramesMeta.jl):
      A range of convenience functions for DataFrames.jl that augment `select` and
      `transform` to provide a user experience similar to that provided by
      [dplyr](https://dplyr.tidyverse.org/) in R.
    - [DataFrameMacros.jl](https://github.com/jkrumbiegel/DataFrameMacros.jl):
      Provides macro versions of the common DataFrames.jl functions similar to DataFramesMeta.jl,
      with convenient syntax for the manipulation of multiple columns at once.
    - [Query.jl](https://github.com/queryverse/Query.jl): Query.jl provides a single
      framework for data wrangling that works with a range of libraries, including
      DataFrames.jl, other tabular data libraries (more on those below), and even
      non-tabular data. Provides many convenience functions analogous to those in
      dplyr in R or [LINQ](https://en.wikipedia.org/wiki/Language_Integrated_Query).
    - You can find more information on these packages in the
      [Data manipulation frameworks](@ref) section of this manual.
- **And More!**
    - [Graphs.jl](https://github.com/JuliaGraphs/Graphs.jl): A pure-Julia,
      high performance network analysis library. Edgelists in `DataFrame`s can be
      easily converted into graphs using the
      [GraphDataFrameBridge.jl](https://github.com/JuliaGraphs/GraphDataFrameBridge.jl)
      package.
- **IO**:
    - DataFrames.jl work well with a range of formats, including:
        - CSV files (using [CSV.jl](https://github.com/JuliaData/CSV.jl)),
        - Apache Arrow (using [Arrow.jl](https://github.com/JuliaData/Arrow.jl))
        - reading Stata, SAS and SPSS files (using [ReadStatTables.jl](https://github.com/junyuan-chen/ReadStatTables.jl);
          alternatively [Queryverse](https://www.queryverse.org/) users
          can choose [StatFiles.jl](https://github.com/queryverse/StatFiles.jl)),
        - Parquet files
          (using [Parquet2.jl](https://gitlab.com/ExpandingMan/Parquet2.jl)),
        - reading R data files (.rda, .RData) (using [RData.jl](https://github.com/JuliaData/RData.jl)).

While not all of these libraries are tightly integrated with DataFrames.jl,
because `DataFrame`s are essentially collections of aligned Julia vectors, so it
is easy to (a) pull out a vector for use with a non-DataFrames-integrated
library, or (b) convert your table into a homogeneously-typed matrix using the
`Matrix` constructor or StatsModels.jl.

### Other Julia Tabular Libraries

DataFrames.jl is a great general purpose tool for data manipulation and
wrangling, but it's not ideal for all applications. For users with more
specialized needs, consider using:

- [TypedTables.jl](https://juliadata.github.io/TypedTables.jl/stable/):
  Type-stable heterogeneous tables. Useful for improved performance when the
  structure of your table is relatively stable and does not feature thousands of
  columns.
- [JuliaDB.jl](https://juliadata.github.io/JuliaDB.jl/stable/): For users
  working with data that is too large to fit in memory, we suggest JuliaDB.jl,
  which offers better performance for large datasets, and can handle out-of-core
  data manipulations (Python users can think of JuliaDB.jl as the Julia version of
  [dask](https://dask.org/)).

Note that most tabular data libraries in the Julia ecosystem (including
DataFrames.jl) support a common interface (defined in the
[Tables.jl](https://github.com/JuliaData/Tables.jl) package). As a result, some
libraries are capable or working with a range of tabular data structures, making
it easy to move between tabular libraries as your needs change. A user of
[Query.jl](https://github.com/queryverse/Query.jl), for example, can use the
same code to manipulate data in a `DataFrame`, a `Table` (defined by
TypedTables.jl), or a JuliaDB table.

## Questions?

If there is something you expect DataFrames to be capable of, but
cannot figure out how to do, please reach out with questions in Domains/Data on
[Discourse](https://discourse.julialang.org/new-topic?title=[DataFrames%20Question]:%20&body=%23%20Question:%0A%0A%23%20Dataset%20(if%20applicable):%0A%0A%23%20Minimal%20Working%20Example%20(if%20applicable):%0A&category=Domains/Data&tags=question).
Additionally you might want to listen to an introduction to DataFrames.jl on
[JuliaAcademy](https://juliaacademy.com/p/introduction-to-dataframes-jl).

Please report bugs by
[opening an issue](https://github.com/JuliaData/DataFrames.jl/issues/new).

You can follow the **source** links throughout the documentation to jump right
to the source files on GitHub to make pull requests for improving the
documentation and function capabilities.

Please review [DataFrames contributing
guidelines](https://github.com/JuliaData/DataFrames.jl/blob/main/CONTRIBUTING.md)
before submitting your first PR!

Information on specific versions can be found on the [Release
page](https://github.com/JuliaData/DataFrames.jl/releases).

## Package Manual

```@contents
Pages = ["man/basics.md",
         "man/getting_started.md",
         "man/joins.md",
         "man/split_apply_combine.md",
         "man/reshaping_and_pivoting.md",
         "man/sorting.md",
         "man/categorical.md",
         "man/missing.md",
         "man/comparisons.md",
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

!!! note

    Breaking changes to public and documented API are avoided in
    DataFrames.jl where possible.

    The following changes are not considered breaking:

    * specific floating point values computed by operations may change at any
      time; users should rely only on approximate accuracy;
    * in functions that use the default random number generator provided by
      Base Julia the specific random numbers computed may change across Julia versions;
    * if the changed functionality is classified as a bug;
    * if the changed behavior was not documented; two major cases are:
      1. in its implementation some function accepted a wider range of arguments
         that it was documented to handle - changes in handling of undocumented
         arguments are not considered as breaking;
      2. the type of the value returned by a function changes, but it still follows
         the contract specified in the documentation; for example if a function
         is documented to return a vector then changing its type from `Vector`
         to `PooledVector` is not considered as breaking;
    * error behavior: code that threw an exception can change exception type
      thrown or stop throwing an exception;
    * changes in display (how objects are printed);
    * changes to the state of global objects from Base Julia whose state
      normally is considered volatile (e.g. state of global random number
      generator).

    All types and functions that are part of public API are guaranteed to go
    through a deprecation period before a breaking change is made to them
    or they would be removed.

    The standard practice is that breaking changes are implemented when a major
    release of DataFrames.jl is made (e.g. functionalities deprecated in a 1.x release
    would be changed in the 2.0 release).

    In rare cases a breaking change might be introduced in a minor release.
    In such a case the changed behavior still goes through one minor release
    during which it is deprecated. The situations where such a breaking change
    might be allowed are (still such breaking changes will be avoided if
    possible):

    * the affected functionality was previously clearly identified in the
      documentation as being subject to changes (for example in DataFrames.jl 1.4
      release propagation rules of `:note`-style metadata are documented as such);
    * the change is on the border of being classified as a bug (in rare cases
      even if a behavior of some function was documented its consequences for
      certain argument combinations could be decided to be unintended and not
      wanted);
    * the change is needed to adjust DataFrames.jl functionality to changes in
      Base Julia.

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
