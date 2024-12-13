# First Steps with DataFrames.jl

## Setting up the Environment

If want to use the DataFrames.jl package you need to install it first.
You can do it using the following commands:

```julia
julia> using Pkg

julia> Pkg.add("DataFrames")
```

or

```julia
julia> ] # ']' should be pressed

(@v1.9) pkg> add DataFrames
```

If you want to make sure everything works as expected you can run the tests
bundled with DataFrames.jl, but be warned that it will take more than 30
minutes:

```julia
julia> using Pkg

julia> Pkg.test("DataFrames") # Warning! This will take more than 30 minutes.
```

Additionally, it is recommended to check the version of DataFrames.jl that
you have installed with the `status` command.

```julia
julia> ]

(@v1.9) pkg> status DataFrames
      Status `~\v1.6\Project.toml`
  [a93c6f00] DataFrames v1.5.0
```

Throughout the rest of the tutorial we will assume that you have installed the
DataFrames.jl package and have already typed `using DataFrames` which loads the
package:

```jldoctest dataframe
julia> using DataFrames
```

The most fundamental type provided by DataFrames.jl is `DataFrame`, where
typically each row is interpreted as an observation and each column as a
feature.

!!! note "Advanced installation configuration"

    DataFrames.jl puts in extra time and effort when the package is being built
    (precompiled) to make sure it is more responsive when you are using it.
    However, in some scenarios users might want to avoid this extra
    precompilaion effort to reduce the time needed to build the package and
    later to load it. To disable precompilation of DataFrames.jl in your current
    project follow the instructions given in the
    [PrecompileTools.jl documentation](https://julialang.github.io/PrecompileTools.jl/stable/#Package-developers:-reducing-the-cost-of-precompilation-during-development)

## Constructors and Basic Utility Functions

### Constructors

In this section you will see several ways to create a `DataFrame` using the
constructor. You can find a detailed list of supported constructors along with
more examples in the documentation of the [`DataFrame`](@ref) object.

We start by creating an empty `DataFrame`:

```jldoctest dataframe
julia> DataFrame()
0×0 DataFrame
```

Now let us initialize a `DataFrame` with several columns. This is a basic way to
do it is the following:

```jldoctest dataframe
julia> DataFrame(A=1:3, B=5:7, fixed=1)
3×3 DataFrame
 Row │ A      B      fixed
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      1
   3 │     3      7      1
```

Observe that using this constructor scalars, like `1` for the column `:fixed`
get automatically broadcasted to fill all rows of the created `DataFrame`.

Sometimes one needs to create a data frame whose column names are not valid
Julia identifiers. In such a case the following form, where `=` is replaced by
`=>` is handy:

```jldoctest dataframe
julia> DataFrame("customer age" => [15, 20, 25],
                 "first name" => ["Rohit", "Rahul", "Akshat"])
3×2 DataFrame
 Row │ customer age  first name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat
```

Notice that this time we have passed column names as strings.

Often you have your source data stored in a dictionary.
Provided that the keys of the dictionary are strings or `Symbol`s
you can also easily create a `DataFrame` from it:

```jldoctest dataframe
julia> dict = Dict("customer age" => [15, 20, 25],
                   "first name" => ["Rohit", "Rahul", "Akshat"])
Dict{String, Vector} with 2 entries:
  "first name"   => ["Rohit", "Rahul", "Akshat"]
  "customer age" => [15, 20, 25]

julia> DataFrame(dict)
3×2 DataFrame
 Row │ customer age  first name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat

julia> dict = Dict(:customer_age => [15, 20, 25],
                   :first_name => ["Rohit", "Rahul", "Akshat"])
Dict{Symbol, Vector} with 2 entries:
  :customer_age => [15, 20, 25]
  :first_name   => ["Rohit", "Rahul", "Akshat"]

julia> DataFrame(dict)
3×2 DataFrame
 Row │ customer_age  first_name
     │ Int64         String
─────┼──────────────────────────
   1 │           15  Rohit
   2 │           20  Rahul
   3 │           25  Akshat
```

Using `Symbol`s, e.g. `:customer_age` rather than strings, e.g. `"customer age"`
to denote column names is preferred as it is faster. However, as you can see
in the example above if our column name contains a space it is not very
convenient to pass it as a `Symbol` (you would have to write `Symbol("customer age")`,
which is verbose) so using a string is more convenient.

It is also quite common to create a `DataFrame` from a `NamedTuple` of vectors
or a vector of `NamedTuple`s. Here are some examples of these operations:

```jldoctest dataframe
julia> DataFrame((a=[1, 2], b=[3, 4]))
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> DataFrame([(a=1, b=0), (a=2, b=0)])
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```

Sometimes your source data might have a heterogeneous set of columns for each observation.
Here is an example:

```
julia> source = [(type="circle", radius=10), (type="square", side=20)]
2-element Vector{NamedTuple{names, Tuple{String, Int64}} where names}:
 (type = "circle", radius = 10)
 (type = "square", side = 20)
```

If you want to create a data frame from such data containing all columns present in at least
one of the source observations, with a `missing` entry if some column is not present then
you can use `Tables.dictcolumntable` function to help you create the desired data frame:

```
julia> DataFrame(Tables.dictcolumntable(source))
2×3 DataFrame
 Row │ type    radius   side
     │ String  Int64?   Int64?
─────┼──────────────────────────
   1 │ circle       10  missing
   2 │ square  missing       20
```

The role of `Tables.dictcolumntable` is to make sure that the `DataFrame` constructor gets information
about all columns present in the source data and properly instantiates them. If we did not use
this function the `DataFrame` constructor would assume that the first row of data contains the set
of columns present in the source, which would lead to an error in our example:

```
julia> DataFrame(source)
ERROR: type NamedTuple has no field radius
```

Let us finish our review of constructors by showing how to create a `DataFrame`
from a matrix. In this case you pass a matrix as a first argument. If the second
argument is just `:auto` then column names `x1`, `x2`, ... will be auto generated.

```jldoctest dataframe
julia> DataFrame([1 0; 2 0], :auto)
2×2 DataFrame
 Row │ x1     x2
     │ Int64  Int64
─────┼──────────────
   1 │     1      0
   2 │     2      0
```

Alternatively you can pass a vector of column names as a second argument to the
`DataFrame` constructor:

```jldoctest dataframe
julia> mat = [1 2 4 5; 15 58 69 41; 23 21 26 69]
3×4 Matrix{Int64}:
  1   2   4   5
 15  58  69  41
 23  21  26  69

julia> nms = ["a", "b", "c", "d"]
4-element Vector{String}:
 "a"
 "b"
 "c"
 "d"

julia> DataFrame(mat, nms)
3×4 DataFrame
 Row │ a      b      c      d
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      2      4      5
   2 │    15     58     69     41
   3 │    23     21     26     69
```

You now know how to create a `DataFrame` from data that you already have in your
Julia session. In the next section we show how to load data to a `DataFrame`
from disk.

### Reading Data From CSV Files

Here we focus on one of the most common scenarios, where one has data stored on
disk in the CSV format.

First make sure you have CSV.jl installed. You can do it using the following
instructions:

```julia
julia> using Pkg

julia> Pkg.add("CSV")
```

In order to read the file in we will use the `CSV.read` function.

```jldoctest dataframe
julia> using CSV

julia> path = joinpath(pkgdir(DataFrames), "docs", "src", "assets", "german.csv");

julia> german_ref = CSV.read(path, DataFrame)
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little         ⋯
    2 │     1     22  female       2  own      little           moderate
    3 │     2     49  male         1  own      little           NA
    4 │     3     45  male         2  free     little           little
    5 │     4     53  male         2  free     little           little         ⋯
    6 │     5     35  male         1  free     NA               NA
    7 │     6     53  male         2  own      quite rich       NA
    8 │     7     35  male         3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮       ⋱
  994 │   993     30  male         3  own      little           little         ⋯
  995 │   994     50  male         2  own      NA               NA
  996 │   995     31  female       1  own      little           NA
  997 │   996     40  male         3  own      little           little
  998 │   997     38  male         2  own      little           NA             ⋯
  999 │   998     23  male         2  free     little           little
 1000 │   999     27  male         2  own      moderate         moderate
                                                  4 columns and 985 rows omitted
```

As you can see the data frame is wider and taller than the display width, so it
got cropped and its 4 rightmost columns and middle 985 rows were not printed.
Later in the tutorial we will discuss how to force Julia to show the whole
data frame if we wanted so.

Also observe that DataFrames.jl displays the data type of the column
below its name. In our case, it is an `Int64`, or `String7` and `String15`.

Let us mention here the difference between the standard `String` type in Julia
and e.g. the `String7` or `String15` types. The types with number suffix denote
strings that have a fixed width (similar `CHAR(N)` type provided by many data
bases). Such strings are much faster to work with (especially if you have many
of them) than the standard `String` type because their instances are not heap
allocated. For this reason `CSV.read` by default reads in narrow string columns
using these fixed-width types.

Let us now explain in detail the following code block:
```julia
path = joinpath(pkgdir(DataFrames), "docs", "src", "assets", "german.csv");

german_ref = CSV.read(path, DataFrame)
```
- we are storing the `german.csv` file in the DataFrames.jl repository to make
  user's life easier and avoid having to download it each time;
- `pkgdir(DataFrames)` gives us the full path to the root of the DataFrames.jl
  package.
- then from this directory we need to move to the directory where the
  `german.csv` file is stored; we use `joinpath` as this is a recommended way to
  compose paths to resources stored on disk in an operating system independent
  way (remember that Windows and Unix differ as they use either `/` or `\` as
  path separator; the `joinpath` function ensures we are not running into issues
  with this);
- then we read the CSV file; the second argument to `CSV.read` is `DataFrame` to
  indicate that we want to read in the file into a `DataFrame` (as `CSV.read`
  allows for many different target formats of data it can read-into).

Before proceeding copy the reference data frame:

```jldoctest dataframe
julia> german = copy(german_ref); # we copy the data frame
```

In this way we can always easily restore our data even if we mess up the
`german` data frame by modifying it.

### Basic Operations on Data Frames

To extract the columns of a data frame directly (i.e. without copying)
you can use one of the following syntaxes:
`german.Sex`, `german."Sex"`, `german[!, :Sex]` or `german[!, "Sex"]`.

The two latter syntaxes using indexing are more flexible as they allow
us passing a variable holding the name of the column, and not only a literal name
as in the case of the syntax using a `.`.

```jldoctest dataframe
julia> german.Sex
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"

julia> colname = "Sex"
"Sex"

julia> german[!, colname]
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
```

Since `german.Sex` does not make a copy when extracting a column from the data
frame, changing the elements of the vector returned by this operation will
affect the values stored in the original `german` data frame. To get a *copy* of
the column you can use `german[:, :Sex]` or `german[:, "Sex"]`. In this case
changing the vector returned by this operation does not affect the data stored
in the `german` data frame.

The `===` function allows us to check if both expressions produce the same object
and confirm the behavior described above:

```jldoctest dataframe
julia> german.Sex === german[!, :Sex]
true

julia> german.Sex === german[:, :Sex]
false
```

You can obtain a vector of column names of the data frame as `String`s using the
`names` function:

```jldoctest dataframe
julia> names(german)
10-element Vector{String}:
 "id"
 "Age"
 "Sex"
 "Job"
 "Housing"
 "Saving accounts"
 "Checking account"
 "Credit amount"
 "Duration"
 "Purpose"
```

Sometimes you are interested in names of columns that meet a particular condition.

For example you can get column names with a given element type by passing this
type as a second argument to the `names` function:

```jldoctest dataframe
julia> names(german, AbstractString)
5-element Vector{String}:
 "Sex"
 "Housing"
 "Saving accounts"
 "Checking account"
 "Purpose"
```

You can explore more options of filtering column names in the documentation of
the [`names`](@ref) function.

If instead you wanted to get column names of a data frame as `Symbol`s use the
`propertynames` function:

```jldoctest dataframe
julia> propertynames(german)
10-element Vector{Symbol}:
 :id
 :Age
 :Sex
 :Job
 :Housing
 Symbol("Saving accounts")
 Symbol("Checking account")
 Symbol("Credit amount")
 :Duration
 :Purpose
```

As you can see the column names containing spaces are not very convenient to work with
as `Symbol`s because they require more typing and introduce some visual noise.

If you were interested in element types of the columns instead. You can use the
`eachcol(german)` function to get an iterator over the columns of the data frame.
Then you can broadcast the `eltype` function over it to get the desired result:

```jldoctest dataframe
julia> eltype.(eachcol(german))
10-element Vector{DataType}:
 Int64
 Int64
 String7
 Int64
 String7
 String15
 String15
 Int64
 Int64
 String31
```

!!! note

    Remember that DataFrames.jl allows to use `Symbol`s (like `:id`) and strings
    (like `"id"`) for all column indexing operations for convenience.
    However, using `Symbol`s is slightly faster, but strings are simpler to work
    with when non standard characters are present in column names or one wants
    to manipulate them.

Before we wrap up let us discuss the `empty` and `empty!` functions that
remove all rows from a `DataFrame`. Understanding the difference between the
behavior of these two functions will help you to understand the function naming
scheme in DataFrames.jl in general.

Let us start with the example of using the `empty` and `empty!` functions:

```jldoctest dataframe
julia> empty(german)
0×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┴──────────────────────────────────────────────────────────────────────────
                                                               4 columns omitted

julia> german
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little         ⋯
    2 │     1     22  female       2  own      little           moderate
    3 │     2     49  male         1  own      little           NA
    4 │     3     45  male         2  free     little           little
    5 │     4     53  male         2  free     little           little         ⋯
    6 │     5     35  male         1  free     NA               NA
    7 │     6     53  male         2  own      quite rich       NA
    8 │     7     35  male         3  rent     little           moderate
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮       ⋱
  994 │   993     30  male         3  own      little           little         ⋯
  995 │   994     50  male         2  own      NA               NA
  996 │   995     31  female       1  own      little           NA
  997 │   996     40  male         3  own      little           little
  998 │   997     38  male         2  own      little           NA             ⋯
  999 │   998     23  male         2  free     little           little
 1000 │   999     27  male         2  own      moderate         moderate
                                                  4 columns and 985 rows omitted

julia> empty!(german)
0×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┴──────────────────────────────────────────────────────────────────────────
                                                               4 columns omitted

julia> german
0×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┴──────────────────────────────────────────────────────────────────────────
                                                               4 columns omitted
```

In the above example `empty` function created a new `DataFrame` with the same
column names and column element types as `german` but with zero rows. On the
other hand `empty!` function removed all rows from `german` in-place and made
each of its columns empty.

The difference between the behavior of the `empty` and `empty!` functions is an
application of the
[stylistic convention](https://docs.julialang.org/en/v1/manual/variables/#Stylistic-Conventions)
employed in the Julia language. This convention is followed in all functions
provided by the DataFrames.jl package.

### Getting Basic Information about a Data Frame

In this section we will learn about how to get basic information on our `german`
`DataFrame`:

The `size` function returns the dimensions of the data frame.
First we restore the `german` data frame, as we have just emptied it above.

```jldoctest dataframe
julia> german = copy(german_ref);

julia> size(german)
(1000, 10)

julia> size(german, 1)
1000

julia> size(german, 2)
10
```

Additionally the `nrow` and `ncol` functions can be used to get the number of rows
and columns in a data frame:
```jldoctest dataframe
julia> nrow(german)
1000

julia> ncol(german)
10
```

To get basic statistics of data in your data frame use the `describe` function
(check out the help of [`describe`](@ref) for information on how to customize
the shown statistics).

```jldoctest dataframe
julia> describe(german)
10×7 DataFrame
 Row │ variable          mean     min       median  max              nmissing  ⋯
     │ Symbol            Union…   Any       Union…  Any              Int64     ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ id                499.5    0         499.5   999                     0  ⋯
   2 │ Age               35.546   19        33.0    75                      0
   3 │ Sex                        female            male                    0
   4 │ Job               1.904    0         2.0     3                       0
   5 │ Housing                    free              rent                    0  ⋯
   6 │ Saving accounts            NA                rich                    0
   7 │ Checking account           NA                rich                    0
   8 │ Credit amount     3271.26  250       2319.5  18424                   0
   9 │ Duration          20.903   4         18.0    72                      0  ⋯
  10 │ Purpose                    business          vacation/others         0
                                                                1 column omitted
```

To limit the columns processed by `describe` use `cols` keyword argument, e.g.:

```jldoctest dataframe
julia> describe(german, cols=1:3)
3×7 DataFrame
 Row │ variable  mean    min     median  max   nmissing  eltype
     │ Symbol    Union…  Any     Union…  Any   Int64     DataType
─────┼────────────────────────────────────────────────────────────
   1 │ id        499.5   0       499.5   999          0  Int64
   2 │ Age       35.546  19      33.0    75           0  Int64
   3 │ Sex               female          male         0  String7
```

The default statistics reported are mean, min, median, max, number of missing
values, and element type of the column. `missing` values are skipped when
computing the summary statistics.

You can adjust how data frame is displayed by calling the `show` function manually:
`show(german, allrows=true)` prints all rows even if they do not fit on screen and
`show(german, allcols=true)` does the same for columns, e.g.:

```jldoctest dataframe
julia> show(german, allcols=true)
1000×10 DataFrame
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking account  Credit amount  Duration  Purpose
      │ Int64  Int64  String7  Int64  String7  String15         String15          Int64          Int64     String31
──────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    1 │     0     67  male         2  own      NA               little                     1169         6  radio/TV
    2 │     1     22  female       2  own      little           moderate                   5951        48  radio/TV
    3 │     2     49  male         1  own      little           NA                         2096        12  education
    4 │     3     45  male         2  free     little           little                     7882        42  furniture/equipment
    5 │     4     53  male         2  free     little           little                     4870        24  car
    6 │     5     35  male         1  free     NA               NA                         9055        36  education
    7 │     6     53  male         2  own      quite rich       NA                         2835        24  furniture/equipment
    8 │     7     35  male         3  rent     little           moderate                   6948        36  car
  ⋮   │   ⋮      ⋮       ⋮       ⋮       ⋮            ⋮                ⋮                ⋮           ⋮               ⋮
  994 │   993     30  male         3  own      little           little                     3959        36  furniture/equipment
  995 │   994     50  male         2  own      NA               NA                         2390        12  car
  996 │   995     31  female       1  own      little           NA                         1736        12  furniture/equipment
  997 │   996     40  male         3  own      little           little                     3857        30  car
  998 │   997     38  male         2  own      little           NA                          804        12  radio/TV
  999 │   998     23  male         2  free     little           little                     1845        45  radio/TV
 1000 │   999     27  male         2  own      moderate         moderate                   4576        45  car
                                                                                                               985 rows omitted
```

It is easy to compute descriptive statistics directly on individual columns using
the functions defined in the `Statistics` module:

```jldoctest dataframe
julia> using Statistics

julia> mean(german.Age)
35.546
```

If instead we want to apply some function to all columns of a data frame we can
use the `mapcols` function. It returns a `DataFrame` where each column of the
source data frame is transformed using a function passed as a first argument.
Note that `mapcols` guarantees not to reuse the columns from `german` in the
returned `DataFrame`. If the transformation returns its argument then it gets
copied before being stored.

```jldoctest dataframe
julia> mapcols(id -> id .^ 2, german)
1000×10 DataFrame
  Row │ id      Age    Sex           Job    Housing   Saving accounts       Ch ⋯
      │ Int64   Int64  String        Int64  String    String                St ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │      0   4489  malemale          4  ownown    NANA                  li ⋯
    2 │      1    484  femalefemale      4  ownown    littlelittle          mo
    3 │      4   2401  malemale          1  ownown    littlelittle          NA
    4 │      9   2025  malemale          4  freefree  littlelittle          li
    5 │     16   2809  malemale          4  freefree  littlelittle          li ⋯
    6 │     25   1225  malemale          1  freefree  NANA                  NA
    7 │     36   2809  malemale          4  ownown    quite richquite rich  NA
    8 │     49   1225  malemale          9  rentrent  littlelittle          mo
  ⋮   │   ⋮       ⋮         ⋮          ⋮       ⋮               ⋮               ⋱
  994 │ 986049    900  malemale          9  ownown    littlelittle          li ⋯
  995 │ 988036   2500  malemale          4  ownown    NANA                  NA
  996 │ 990025    961  femalefemale      1  ownown    littlelittle          NA
  997 │ 992016   1600  malemale          9  ownown    littlelittle          li
  998 │ 994009   1444  malemale          4  ownown    littlelittle          NA ⋯
  999 │ 996004    529  malemale          4  freefree  littlelittle          li
 1000 │ 998001    729  malemale          4  ownown    moderatemoderate      mo
                                                  4 columns and 985 rows omitted
```

If you want to look at first and last rows of a data frame then you can do this
using the `first` and `last` functions respectively:

```jldoctest dataframe
julia> first(german, 6)
6×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     1     22  female       2  own      little           moderate
   3 │     2     49  male         1  own      little           NA
   4 │     3     45  male         2  free     little           little
   5 │     4     53  male         2  free     little           little          ⋯
   6 │     5     35  male         1  free     NA               NA
                                                               4 columns omitted

julia> last(german, 5)
5×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │   995     31  female       1  own      little           NA              ⋯
   2 │   996     40  male         3  own      little           little
   3 │   997     38  male         2  own      little           NA
   4 │   998     23  male         2  free     little           little
   5 │   999     27  male         2  own      moderate         moderate        ⋯
                                                               4 columns omitted
```

Using `first` and `last` without passing the number of rows will return a
first/last `DataFrameRow` in the data frame. `DataFrameRow` is a view into a
single row of an `AbstractDataFrame`. It stores a reference to a parent
`DataFrame` and information about which row and columns from the parent are
selected. You can think of `DataFrameRow` as a `NamedTuple` that is mutable,
i.e. allows to update the source data frame, which is often useful.

```jldoctest dataframe
julia> first(german)
DataFrameRow
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
                                                               4 columns omitted

julia> last(german)
DataFrameRow
  Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accou ⋯
      │ Int64  Int64  String7  Int64  String7  String15         String15       ⋯
──────┼─────────────────────────────────────────────────────────────────────────
 1000 │   999     27  male         2  own      moderate         moderate       ⋯
                                                               4 columns omitted
```

## Getting and Setting Data in a Data Frame

### Indexing Syntax

Data frame can be indexed in a similar way to matrices.
In the [Indexing](@ref) section of the manual you can find all details about all
the available options. Here we highlight the basic ones.

The general syntax for indexing is `data_frame[selected_rows, selected_columns]`.
Observe that, as opposed to matrices in Julia Base, it is required to always pass
both row and column selector. The colon `:` indicates that all items (rows or
columns depending on its position) should be retained. Here are a few examples:

```jldoctest dataframe
julia> german[1:5, [:Sex, :Age]]
5×2 DataFrame
 Row │ Sex      Age
     │ String7  Int64
─────┼────────────────
   1 │ male        67
   2 │ female      22
   3 │ male        49
   4 │ male        45
   5 │ male        53

julia> german[1:5, :]
5×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     1     22  female       2  own      little           moderate
   3 │     2     49  male         1  own      little           NA
   4 │     3     45  male         2  free     little           little
   5 │     4     53  male         2  free     little           little          ⋯
                                                               4 columns omitted

julia> german[[1, 6, 15], :]
3×10 DataFrame
 Row │ id     Age    Sex      Job    Housing  Saving accounts  Checking accoun ⋯
     │ Int64  Int64  String7  Int64  String7  String15         String15        ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │     0     67  male         2  own      NA               little          ⋯
   2 │     5     35  male         1  free     NA               NA
   3 │    14     28  female       2  rent     little           little
                                                               4 columns omitted

julia> german[:, [:Age, :Sex]]
1000×2 DataFrame
  Row │ Age    Sex
      │ Int64  String7
──────┼────────────────
    1 │    67  male
    2 │    22  female
    3 │    49  male
    4 │    45  male
    5 │    53  male
    6 │    35  male
    7 │    53  male
    8 │    35  male
  ⋮   │   ⋮       ⋮
  994 │    30  male
  995 │    50  male
  996 │    31  female
  997 │    40  male
  998 │    38  male
  999 │    23  male
 1000 │    27  male
       985 rows omitted
```

Pay attention that `german[!, [:Sex]]` and `german[:, [:Sex]]` returns a data
frame object, while `german[!, :Sex]` and `german[:, :Sex]` returns a vector. In
the first case, `[:Sex]` is a vector, indicating that the resulting object
should be a data frame. On the other hand, `:Sex` is a single `Symbol`,
indicating that a single column vector should be extracted. Note that in the
first case a vector is required to be passed (not just any iterable), so e.g.
`german[:, (:Age, :Sex)]` is not allowed, but `german[:, [:Age, :Sex]]` is
valid. Below we show both operations to highlight this difference:

```jldoctest dataframe
julia> german[!, [:Sex]]
1000×1 DataFrame
  Row │ Sex
      │ String7
──────┼─────────
    1 │ male
    2 │ female
    3 │ male
    4 │ male
    5 │ male
    6 │ male
    7 │ male
    8 │ male
  ⋮   │    ⋮
  994 │ male
  995 │ male
  996 │ female
  997 │ male
  998 │ male
  999 │ male
 1000 │ male
985 rows omitted

julia> german[!, :Sex]
1000-element PooledArrays.PooledVector{String7, UInt32, Vector{UInt32}}:
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 "male"
 ⋮
 "male"
 "male"
 "male"
 "male"
 "female"
 "male"
 "male"
 "male"
 "male"
```

As it was explained earlier in this tutorial the difference between using `!`
and `:` when passing a row index is that `!` does not perform a copy of columns,
while `:` does when reading data from a data frame. Therefore
`german[!, [:Sex]]` data frame stores the same vector as the source `german`
data frame, while `german[:, [:Sex]]` stores its copy.

The `!` selector normally should be avoided as using it can lead to hard to
catch bugs. However, when working with very large data frames it can be useful
to save memory and improve performance of operations.

Recapping what we have already learned,
To get the column `:Age` from the `german` data frame you can do the following:

- to copy the vector: `german[:, :Age]`, `german[:, "Age"]` or `german[:, 2]`;
- to get a vector without copying: `german.Age`, `german."Age"`, `german[!, :Age]`,
  `german[!, "Age"]` or `german[!, 2]`.

To get the first two columns as a `DataFrame`, we can index as follows:
- to get the copied columns: `german[:, 1:2]`, `german[:, [:id, :Age]]`,
  or `german[:, ["id", "Age"]]`;
- to reuse the columns without copying: `german[!, 1:2]`, `german[!, [:id, :Age]]`,
  or `german[!, ["id", "Age"]]`.

If you want to can get a single cell of a data frame use the same syntax as the
one that gets a cell of a matrix:

```jldoctest dataframe
julia> german[4, 4]
2
```

### Views

We can also create a `view` of a data frame. It is often useful as it is more
memory efficient than creating a materialized selection. You can create it using
a `view` function:

```jldoctest dataframe
julia> view(german, :, 2:5)
1000×4 SubDataFrame
  Row │ Age    Sex      Job    Housing
      │ Int64  String7  Int64  String7
──────┼────────────────────────────────
    1 │    67  male         2  own
    2 │    22  female       2  own
    3 │    49  male         1  own
    4 │    45  male         2  free
    5 │    53  male         2  free
    6 │    35  male         1  free
    7 │    53  male         2  own
    8 │    35  male         3  rent
  ⋮   │   ⋮       ⋮       ⋮       ⋮
  994 │    30  male         3  own
  995 │    50  male         2  own
  996 │    31  female       1  own
  997 │    40  male         3  own
  998 │    38  male         2  own
  999 │    23  male         2  free
 1000 │    27  male         2  own
                       985 rows omitted
```

or using a `@view` macro:

```jldoctest dataframe
julia> @view german[end:-1:1, [1, 4]]
1000×2 SubDataFrame
  Row │ id     Job
      │ Int64  Int64
──────┼──────────────
    1 │   999      2
    2 │   998      2
    3 │   997      2
    4 │   996      3
    5 │   995      1
    6 │   994      2
    7 │   993      3
    8 │   992      1
  ⋮   │   ⋮      ⋮
  994 │     6      2
  995 │     5      1
  996 │     4      2
  997 │     3      2
  998 │     2      1
  999 │     1      2
 1000 │     0      2
     985 rows omitted
```

Similarly we can get a view of one column of a data frame:

```jldoctest dataframe
julia> @view german[1:5, 1]
5-element view(::Vector{Int64}, 1:5) with eltype Int64:
 0
 1
 2
 3
 4
```

its single cell:

```jldoctest dataframe
julia> @view german[2, 2]
0-dimensional view(::Vector{Int64}, 2) with eltype Int64:
22
```

or a single row:

```jldoctest dataframe
julia> @view german[3, 2:5]
DataFrameRow
 Row │ Age    Sex      Job    Housing
     │ Int64  String7  Int64  String7
─────┼────────────────────────────────
   3 │    49  male         1  own
```

As you can see the row and column indexing syntax is exactly the same as for
indexing. The only difference is that we do not create a new object, but a view
into an existing one.

In order to compare the performance of indexing vs creation of a view let us
run the following benchmark using the BenchmarkTools.jl package (please install
it if you want to re-run this comparison):

```julia
julia> using BenchmarkTools

julia> @btime $german[1:end-1, 1:end-1];
  9.900 μs (44 allocations: 57.56 KiB)

julia> @btime @view $german[1:end-1, 1:end-1];
  67.332 ns (2 allocations: 32 bytes)
```

As you can see creation of a view is:
- an order of magnitude faster;
- allocates much less memory.

The downside of the view is that:
- it points to the same memory as its parent (so changing a view changes the
  parent, which is sometimes undesirable);
- some operations might be a bit slower (as DataFrames.jl needs to perform a
  mapping of indices of a view to indices of the parent).

### Changing the Data Stored in a Data Frame

In order to show how to perform mutating operations on a data frame we make a
subset of a `german` data frame first:

```jldoctest dataframe
julia> df1 = german[1:6, 2:4]
6×3 DataFrame
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    67  male         2
   2 │    22  female       2
   3 │    49  male         1
   4 │    45  male         2
   5 │    53  male         2
   6 │    35  male         1
```

In the following example we replace the column `:Age` in our `df1` data frame
with a new vector:

```jldoctest dataframe
julia> val = [80, 85, 98, 95, 78, 89]
6-element Vector{Int64}:
 80
 85
 98
 95
 78
 89

julia> df1.Age = val
6-element Vector{Int64}:
 80
 85
 98
 95
 78
 89

julia> df1
6×3 DataFrame
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    80  male         2
   2 │    85  female       2
   3 │    98  male         1
   4 │    95  male         2
   5 │    78  male         2
   6 │    89  male         1
```

This is a non-copying operation. One can perform it only if `val` vector has the
same length as number of rows of `df1` or as a special case if `df1` would not
have any columns.

```jldoctest dataframe
julia> df1.Age === val # no copy is performed
true
```

If in indexing you select a subset of rows from a data frame the mutation is
performed in place, i.e. writing to an existing vector.
Below setting values of column `:Job` in rows `1:3` to values `[2, 3, 2]`:

```jldoctest dataframe
julia> df1[1:3, :Job] = [2, 3, 2]
3-element Vector{Int64}:
 2
 3
 2

julia> df1
6×3 DataFrame
 Row │ Age    Sex      Job
     │ Int64  String7  Int64
─────┼───────────────────────
   1 │    80  male         2
   2 │    85  female       3
   3 │    98  male         2
   4 │    95  male         2
   5 │    78  male         2
   6 │    89  male         1
```

As a special rule using `!` as row selector replaces column without copying
(just like in the `df1.Age = val` example above).
For example below we replace the `:Sex` column:

```jldoctest dataframe
julia> df1[!, :Sex] = ["male", "female", "female", "transgender", "female", "male"]
6-element Vector{String}:
 "male"
 "female"
 "female"
 "transgender"
 "female"
 "male"

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    80  male             2
   2 │    85  female           3
   3 │    98  female           2
   4 │    95  transgender      2
   5 │    78  female           2
   6 │    89  male             1
```

Similarly to setting selected rows of a single column we can also set
selected columns of a given row of a data frame:

```jldoctest dataframe
julia> df1[3, 1:3] = [78, "male", 4]
3-element Vector{Any}:
 78
   "male"
  4

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    80  male             2
   2 │    85  female           3
   3 │    78  male             4
   4 │    95  transgender      2
   5 │    78  female           2
   6 │    89  male             1
```

We have already mentioned that `DataFrameRow` can be used to mutate its parent
data frame. Here are a few examples:

```jldoctest dataframe
julia> dfr = df1[2, :] # DataFrameRow with the second row and all columns of df1
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    85  female      3

julia> dfr.Age = 98 # set value of col `:Age` in row `2` to `98` in-place
98

julia> dfr
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    98  female      3

julia> dfr[2:3] = ["male", 2] # set values of entries in columns `:Sex` and `:Job`
2-element Vector{Any}:
  "male"
 2

julia> dfr
DataFrameRow
 Row │ Age    Sex     Job
     │ Int64  String  Int64
─────┼──────────────────────
   2 │    98  male        2
```

These operations updated the data stored in the `df1` data frame.

In a similar fashion views can be used to update data stored in their parent
data frame. Here are some examples:

```jldoctest dataframe
julia> sdf = view(df1, :, 2:3)
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ male             2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ male             1

julia> sdf[2, :Sex] = "female" # set value of col `:Sex` in second row to `female` in-place
"female"

julia> sdf
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ female           2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ male             1

julia> sdf[6, 1:2] = ["female", 3]
2-element Vector{Any}:
  "female"
 3

julia> sdf
6×2 SubDataFrame
 Row │ Sex          Job
     │ String       Int64
─────┼────────────────────
   1 │ male             2
   2 │ female           2
   3 │ male             4
   4 │ transgender      2
   5 │ female           2
   6 │ female           3
```

In all these cases the parent of `sdf` view was also updated.

### Broadcasting Assignment

Apart from normal assignment one can perform broadcasting assignment using the
`.=` operation.

Before we move forward let us explain how broadcasting works in Julia.
The standard syntax to perform
[broadcasting](https://docs.julialang.org/en/v1/manual/mathematical-operations/#man-dot-operators)
is to use `.`. For example, as opposed to R this operation fails:

```jldoctest dataframe
julia> s = [25, 26, 35, 56]
4-element Vector{Int64}:
 25
 26
 35
 56

julia> s[2:3] = 0
ERROR: ArgumentError: indexed assignment with a single value to possibly many locations is not supported; perhaps use broadcasting `.=` instead?
```

Instead we have to write:

```jldoctest dataframe
julia> s[2:3] .= 0
2-element view(::Vector{Int64}, 2:3) with eltype Int64:
 0
 0

julia> s
4-element Vector{Int64}:
 25
  0
  0
 56
```

Similar syntax is fully supported in DataFrames.jl. Here, Column `:Age` is
replaced freshly allocated vector because of broadcasting assignment:

```jldoctest dataframe
julia> df1[!, :Age] .= [85, 89, 78, 58, 96, 68] # col `:Age` is replaced freshly allocated vector
6-element Vector{Int64}:
 85
 89
 78
 58
 96
 68

julia> df1
6×3 DataFrame
 Row │ Age    Sex          Job
     │ Int64  String       Int64
─────┼───────────────────────────
   1 │    85  male             2
   2 │    89  female           2
   3 │    78  male             4
   4 │    58  transgender      2
   5 │    96  female           2
   6 │    68  female           3
```

Using the `:` instead of `!` above would perform a broadcasting assignment
in-place into an existing column. The major difference between in-place and
replace operations is that replacing columns is needed if new values have a
different type than the old ones.

In the examples below we operate on columns `:Customers` and `:City` that are not
present in `df1`. In this case using `!` and `:` are equivalent and a new column
is allocated:

```jldoctest dataframe
julia> df1[!, :Customers] .= ["Rohit", "Akshat", "Rahul", "Aayush", "Prateek", "Anam"]
6-element Vector{String}:
 "Rohit"
 "Akshat"
 "Rahul"
 "Aayush"
 "Prateek"
 "Anam"

julia> df1[:, :City] .= ["Kanpur", "Lucknow", "Bhuvneshwar", "Jaipur", "Ranchi", "Dehradoon"]
6-element Vector{String}:
 "Kanpur"
 "Lucknow"
 "Bhuvneshwar"
 "Jaipur"
 "Ranchi"
 "Dehradoon"

julia> df1
6×5 DataFrame
 Row │ Age    Sex          Job    Customers  City
     │ Int64  String       Int64  String     String
─────┼───────────────────────────────────────────────────
   1 │    85  male             2  Rohit      Kanpur
   2 │    89  female           2  Akshat     Lucknow
   3 │    78  male             4  Rahul      Bhuvneshwar
   4 │    58  transgender      2  Aayush     Jaipur
   5 │    96  female           2  Prateek    Ranchi
   6 │    68  female           3  Anam       Dehradoon
```

A most common broadcasting assignment operation is when a scalar is used on the
right hand side, e.g:

```jldoctest dataframe
julia> df1[:, 3] .= 4 # an in-place replacement of values stored in column number 3 by 4
6-element view(::Vector{Int64}, :) with eltype Int64:
 4
 4
 4
 4
 4
 4

julia> df1
6×5 DataFrame
 Row │ Age    Sex          Job    Customers  City
     │ Int64  String       Int64  String     String
─────┼───────────────────────────────────────────────────
   1 │    85  male             4  Rohit      Kanpur
   2 │    89  female           4  Akshat     Lucknow
   3 │    78  male             4  Rahul      Bhuvneshwar
   4 │    58  transgender      4  Aayush     Jaipur
   5 │    96  female           4  Prateek    Ranchi
   6 │    68  female           4  Anam       Dehradoon
```

For `:` row selector the broadcasting assignment operation works in-place,
so the following operation throws an error:

```jldoctest dataframe
julia> df1[:, :Age] .= "Economics"
ERROR: MethodError: Cannot `convert` an object of type String to an object of type Int64
```

We need to use `!` instead as it replaces the old vector with a freshly allocated one:

```jldoctest dataframe
julia> df1[!, :Age] .= "Economics"
6-element Vector{String}:
 "Economics"
 "Economics"
 "Economics"
 "Economics"
 "Economics"
 "Economics"

julia> df1
6×5 DataFrame
 Row │ Age        Sex          Job    Customers  City
     │ String     String       Int64  String     String
─────┼───────────────────────────────────────────────────────
   1 │ Economics  male             4  Rohit      Kanpur
   2 │ Economics  female           4  Akshat     Lucknow
   3 │ Economics  male             4  Rahul      Bhuvneshwar
   4 │ Economics  transgender      4  Aayush     Jaipur
   5 │ Economics  female           4  Prateek    Ranchi
   6 │ Economics  female           4  Anam       Dehradoon
```

There are some scenarios in DataFrames.jl, when we naturally want a
broadcasting-like behaviour, but do not allow for the use of `.` operation. In
such cases a so-called pseudo-broadcasting is performed for user convenience. We
have already seen it in examples of `DataFrame` constructor. Below we show
pseudo-broadcasting at work in the `insertcols!` function, that inserts a column
into a data frame in an arbitrary position.

In the example below we are creating a column `:Country` with the `insertcols!`
function. Since we pass a scalar `"India"` value of the column it is broadcasted
to all rows in the output data frame:

```jldoctest dataframe
julia> insertcols!(df1, 1, :Country => "India")
6×6 DataFrame
 Row │ Country  Age        Sex          Job    Customers  City
     │ String   String     String       Int64  String     String
─────┼────────────────────────────────────────────────────────────────
   1 │ India    Economics  male             4  Rohit      Kanpur
   2 │ India    Economics  female           4  Akshat     Lucknow
   3 │ India    Economics  male             4  Rahul      Bhuvneshwar
   4 │ India    Economics  transgender      4  Aayush     Jaipur
   5 │ India    Economics  female           4  Prateek    Ranchi
   6 │ India    Economics  female           4  Anam       Dehradoon
```

You can pass a column location where you want to put the inserted column as a
second argument to the `insertcols!` function:
```
julia> insertcols!(df1, 4, :b => exp(4))
6×7 DataFrame
 Row │ Country  Age        Sex          b        Job    Customers  City        ⋯
     │ String   String     String       Float64  Int64  String     String      ⋯
─────┼──────────────────────────────────────────────────────────────────────────
   1 │ India    Economics  male         54.5982      4  Rohit      Kanpur      ⋯
   2 │ India    Economics  female       54.5982      4  Akshat     Lucknow
   3 │ India    Economics  male         54.5982      4  Rahul      Bhuvneshwar
   4 │ India    Economics  transgender  54.5982      4  Aayush     Jaipur
   5 │ India    Economics  female       54.5982      4  Prateek    Ranchi      ⋯
   6 │ India    Economics  female       54.5982      4  Anam       Dehradoon
```

### Not, Between, Cols, and All Column Selectors

You can use `Not`, `Between`, `Cols`, and `All` selectors in more complex column
selection scenarios:
- `Not` selector (from the [InvertedIndices.jl](https://github.com/mbauman/InvertedIndices.jl)
  package) allows us to specify the columns we want to exclude from the resulting
  data frame. We can put any valid other column selector inside `Not`;
- `Between` selector allows us to specify a range of columns (we can pass the
  start and stop column using any of the single column selector syntaxes);
- `Cols(...)` selector picks a union of other selectors passed as its arguments;
- `All()` allows us to select all columns of `DataFrame`; this is the same as passing `:`;
- regular expression to select columns whose names match it.

Let us give some examples of these selectors.

Drop `:Age` column:

```jldoctest dataframe
julia> german[:, Not(:Age)]
1000×9 DataFrame
  Row │ id     Sex      Job    Housing  Saving accounts  Checking account  Cre ⋯
      │ Int64  String7  Int64  String7  String15         String15          Int ⋯
──────┼─────────────────────────────────────────────────────────────────────────
    1 │     0  male         2  own      NA               little                ⋯
    2 │     1  female       2  own      little           moderate
    3 │     2  male         1  own      little           NA
    4 │     3  male         2  free     little           little
    5 │     4  male         2  free     little           little                ⋯
    6 │     5  male         1  free     NA               NA
    7 │     6  male         2  own      quite rich       NA
    8 │     7  male         3  rent     little           moderate
  ⋮   │   ⋮       ⋮       ⋮       ⋮            ⋮                ⋮              ⋱
  994 │   993  male         3  own      little           little                ⋯
  995 │   994  male         2  own      NA               NA
  996 │   995  female       1  own      little           NA
  997 │   996  male         3  own      little           little
  998 │   997  male         2  own      little           NA                    ⋯
  999 │   998  male         2  free     little           little
 1000 │   999  male         2  own      moderate         moderate
                                                  3 columns and 985 rows omitted
```

Select columns starting from `:Sex` and ending at `:Housing`:

```
julia> german[:, Between(:Sex, :Housing)]
1000×3 DataFrame
  Row │ Sex     Job    Housing
      │ String  Int64  String
──────┼────────────────────────
    1 │ male        2  own
    2 │ female      2  own
    3 │ male        1  own
    4 │ male        2  free
    5 │ male        2  free
    6 │ male        1  free
    7 │ male        2  own
    8 │ male        3  rent
  ⋮   │   ⋮       ⋮       ⋮
  994 │ male        3  own
  995 │ male        2  own
  996 │ female      1  own
  997 │ male        3  own
  998 │ male        2  own
  999 │ male        2  free
 1000 │ male        2  own
               985 rows omitted
```

In the example below `Cols` selector is picking a union of `"Age"` and
`Between("Sex", "Job")` selectors passed as its arguments:

```jldoctest dataframe
julia> german[:, Cols("Age", Between("Sex", "Job"))]
1000×3 DataFrame
  Row │ Age    Sex      Job
      │ Int64  String7  Int64
──────┼───────────────────────
    1 │    67  male         2
    2 │    22  female       2
    3 │    49  male         1
    4 │    45  male         2
    5 │    53  male         2
    6 │    35  male         1
    7 │    53  male         2
    8 │    35  male         3
  ⋮   │   ⋮       ⋮       ⋮
  994 │    30  male         3
  995 │    50  male         2
  996 │    31  female       1
  997 │    40  male         3
  998 │    38  male         2
  999 │    23  male         2
 1000 │    27  male         2
              985 rows omitted
```

You can also use `Regex` (regular expressions) to select columns. In the example
below we select columns that have `"S"` in their name and also we use `Not` to
drop row number 5:

```jldoctest dataframe
julia> german[Not(5), r"S"]
999×2 DataFrame
 Row │ Sex      Saving accounts
     │ String7  String15
─────┼──────────────────────────
   1 │ male     NA
   2 │ female   little
   3 │ male     little
   4 │ male     little
   5 │ male     NA
   6 │ male     quite rich
   7 │ male     little
   8 │ male     rich
  ⋮  │    ⋮            ⋮
 993 │ male     little
 994 │ male     NA
 995 │ female   little
 996 │ male     little
 997 │ male     little
 998 │ male     little
 999 │ male     moderate
                984 rows omitted
```

## Manipulation Functions

The seven functions below can be used to manipulate data frames
by applying operations to them.

The functions without a `!` in their name
will create a new data frame based on the source data frame,
so you will probably want to store the new data frame to a new variable name,
e.g. `new_df = transform(source_df, operation)`.
The functions with a `!` at the end of their name
will modify an existing data frame in-place,
so there is typically no need to assign the result to a variable,
e.g. `transform!(source_df, operation)` instead of
`source_df = transform(source_df, operation)`.

The number of columns and rows in the resultant data frame varies
depending on the manipulation function employed.

| Function     | Memory Usage                     | Column Retention                        | Row Retention                                       |
| ------------ | -------------------------------- | --------------------------------------- | --------------------------------------------------- |
| `transform`  | Creates a new data frame.        | Retains original and resultant columns. | Retains same number of rows as original data frame. |
| `transform!` | Modifies an existing data frame. | Retains original and resultant columns. | Retains same number of rows as original data frame. |
| `select`     | Creates a new data frame.        | Retains only resultant columns.         | Retains same number of rows as original data frame. |
| `select!`    | Modifies an existing data frame. | Retains only resultant columns.         | Retains same number of rows as original data frame. |
| `subset`     | Creates a new data frame.        | Retains original columns.               | Retains only rows where condition is true.          |
| `subset!`    | Modifies an existing data frame. | Retains original columns.               | Retains only rows where condition is true.          |
| `combine`    | Creates a new data frame.        | Retains only resultant columns.         | Retains only resultant rows.                        |

### Constructing Operations

All of the functions above use the same syntax which is commonly
`manipulation_function(dataframe, operation)`.
The `operation` argument defines the
operation to be applied to the source `dataframe`,
and it can take any of the following common forms explained below:

* `source_column_selector`

  selects source column(s) without manipulating or renaming them

  Examples: `:a`, `[:a, :b]`, `All()`, `Not(:a)`

* `source_column_selector => operation_function`

  passes source column(s) as arguments to a function
  and automatically names the resulting column(s)

  Examples: `:a => sum`, `[:a, :b] => +`, `:a => ByRow(==(3))`

* `source_column_selector => operation_function => new_column_names`

  passes source column(s) as arguments to a function
  and names the resulting column(s) `new_column_names`

  Examples: `:a => sum => :sum_of_a`, `[:a, :b] => (+) => :a_plus_b`

  *(Not available for `subset` or `subset!`)*

* `source_column_selector => new_column_names`

  renames a source column,
  or splits a column containing collection elements into multiple new columns

  Examples: `:a => :new_a`, `:a_b => [:a, :b]`, `:nt => AsTable`

  (*Not available for `subset` or `subset!`*)

The `=>` operator constructs a
[Pair](https://docs.julialang.org/en/v1/base/collections/#Core.Pair),
which is a type to link one object to another.
(Pairs are commonly used to create elements of a
[Dictionary](https://docs.julialang.org/en/v1/base/collections/#Dictionaries).)
In DataFrames.jl manipulation functions,
`Pair` arguments are used to define column `operations` to be performed.
The examples shown above will be explained in more detail later.

*The manipulation functions also have methods for applying multiple operations.
See the later sections [Applying Multiple Operations per Manipulation](@ref)
and [Broadcasting Operation Pairs](@ref) for more information.*

#### `source_column_selector`
Inside an `operation`, `source_column_selector` is usually a column name
or column index which identifies a data frame column.

`source_column_selector` may be used as the entire `operation`
with `select` or `select!` to isolate or reorder columns.

```julia
julia> df = DataFrame(a = [1, 2, 3], b = [4, 5, 6], c = [7, 8, 9])
3×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      7
   2 │     2      5      8
   3 │     3      6      9

julia> select(df, :b)
3×1 DataFrame
 Row │ b
     │ Int64
─────┼───────
   1 │     4
   2 │     5
   3 │     6

julia> select(df, "b")
3×1 DataFrame
 Row │ b
     │ Int64
─────┼───────
   1 │     4
   2 │     5
   3 │     6

julia> select(df, 2)
3×1 DataFrame
 Row │ b
     │ Int64
─────┼───────
   1 │     4
   2 │     5
   3 │     6
```

`source_column_selector` may also be used as the entire `operation`
with `subset` or `subset!` if the source column contains `Bool` values.

```julia
julia> df = DataFrame(
           name = ["Scott", "Jill", "Erica", "Jimmy"],
           minor = [false, true, false, true],
       )
4×2 DataFrame
 Row │ name    minor
     │ String  Bool
─────┼───────────────
   1 │ Scott   false
   2 │ Jill     true
   3 │ Erica   false
   4 │ Jimmy    true

julia> subset(df, :minor)
2×2 DataFrame
 Row │ name    minor
     │ String  Bool
─────┼───────────────
   1 │ Jill     true
   2 │ Jimmy    true
```

`source_column_selector` may instead be a collection of columns such as a vector,
a [regular expression](https://docs.julialang.org/en/v1/manual/strings/#man-regex-literals),
a `Not`, `Between`, `All`, or `Cols` expression,
or a `:`.
See the [Indexing](@ref) API for the full list of possible values with references.

!!! note

    The Julia parser sometimes prevents `:` from being used by itself.
    If you get
    `ERROR: syntax: whitespace not allowed after ":" used for quoting`,
    try using `All()`, `Cols(:)`, or `(:)` instead to select all columns.

```julia
julia> df = DataFrame(
           id = [1, 2, 3],
           first_name = ["José", "Emma", "Nathan"],
           last_name = ["Garcia", "Marino", "Boyer"],
           age = [61, 24, 33]
       )
3×4 DataFrame
 Row │ id     first_name  last_name  age
     │ Int64  String      String     Int64
─────┼─────────────────────────────────────
   1 │     1  José        Garcia        61
   2 │     2  Emma        Marino        24
   3 │     3  Nathan      Boyer         33

julia> select(df, [:last_name, :first_name])
3×2 DataFrame
 Row │ last_name  first_name
     │ String     String
─────┼───────────────────────
   1 │ Garcia     José
   2 │ Marino     Emma
   3 │ Boyer      Nathan

julia> select(df, r"name")
3×2 DataFrame
 Row │ first_name  last_name
     │ String      String
─────┼───────────────────────
   1 │ José        Garcia
   2 │ Emma        Marino
   3 │ Nathan      Boyer

julia> select(df, Not(:id))
3×3 DataFrame
 Row │ first_name  last_name  age
     │ String      String     Int64
─────┼──────────────────────────────
   1 │ José        Garcia        61
   2 │ Emma        Marino        24
   3 │ Nathan      Boyer         33

julia> select(df, Between(2,4))
3×3 DataFrame
 Row │ first_name  last_name  age
     │ String      String     Int64
─────┼──────────────────────────────
   1 │ José        Garcia        61
   2 │ Emma        Marino        24
   3 │ Nathan      Boyer         33

julia> df2 = DataFrame(
           name = ["Scott", "Jill", "Erica", "Jimmy"],
           minor = [false, true, false, true],
           male = [true, false, false, true],
       )
4×3 DataFrame
 Row │ name    minor  male
     │ String  Bool   Bool
─────┼──────────────────────
   1 │ Scott   false   true
   2 │ Jill     true  false
   3 │ Erica   false  false
   4 │ Jimmy    true   true

julia> subset(df2, [:minor, :male])
1×3 DataFrame
 Row │ name    minor  male
     │ String  Bool   Bool
─────┼─────────────────────
   1 │ Jimmy    true  true
```

!!! note

    Using `Symbol` in `source_column_selector` will perform slightly faster than using string.
    However, a string is convenient when column names contain spaces.

    All elements of `source_column_selector` must be the same type
    (unless wrapped in `Cols`),
    e.g. `subset(df2, [:minor, "male"])` will error
    since `Symbol` and string are used simultaneously.

#### `operation_function`
Inside an `operation` pair, `operation_function` is a function
which operates on data frame columns passed as vectors.
When multiple columns are selected by `source_column_selector`,
the `operation_function` will receive the columns as separate positional arguments
in the order they were selected, e.g. `f(column1, column2, column3)`.

```julia
julia> df = DataFrame(a = [1, 2, 3], b = [4, 5, 4])
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      4

julia> combine(df, :a => sum)
1×1 DataFrame
 Row │ a_sum
     │ Int64
─────┼───────
   1 │     6

julia> transform(df, :b => maximum) # `transform` and `select` copy scalar result to all rows
3×3 DataFrame
 Row │ a      b      b_maximum
     │ Int64  Int64  Int64
─────┼─────────────────────────
   1 │     1      4          5
   2 │     2      5          5
   3 │     3      4          5

julia> transform(df, [:b, :a] => -) # vector subtraction is okay
3×3 DataFrame
 Row │ a      b      b_a_-
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      3
   2 │     2      5      3
   3 │     3      4      1

julia> transform(df, [:a, :b] => *) # vector multiplication is not defined
ERROR: MethodError: no method matching *(::Vector{Int64}, ::Vector{Int64})
```

Don't worry! There is a quick fix for the previous error.
If you want to apply a function to each element in a column
instead of to the entire column vector,
then you can wrap your element-wise function in `ByRow` like
`ByRow(my_elementwise_function)`.
This will apply `my_elementwise_function` to every element in the column
and then collect the results back into a vector.

```julia
julia> transform(df, [:a, :b] => ByRow(*))
3×3 DataFrame
 Row │ a      b      a_b_*
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      4
   2 │     2      5     10
   3 │     3      4     12

julia> transform(df, Cols(:) => ByRow(max))
3×3 DataFrame
 Row │ a      b      a_b_max
     │ Int64  Int64  Int64
─────┼───────────────────────
   1 │     1      4        4
   2 │     2      5        5
   3 │     3      4        4

julia> f(x) = x + 1
f (generic function with 1 method)

julia> transform(df, :a => ByRow(f))
3×3 DataFrame
 Row │ a      b      a_f
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      2
   2 │     2      5      3
   3 │     3      4      4
```

Alternatively, you may just want to define the function itself so it
[broadcasts](https://docs.julialang.org/en/v1/manual/arrays/#Broadcasting)
over vectors.

```julia
julia> g(x) = x .+ 1
g (generic function with 1 method)

julia> transform(df, :a => g)
3×3 DataFrame
 Row │ a      b      a_g
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      2
   2 │     2      5      3
   3 │     3      4      4

julia> h(x, y) = x .+ y .+ 1
h (generic function with 1 method)

julia> transform(df, [:a, :b] => h)
3×3 DataFrame
 Row │ a      b      a_b_h
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      6
   2 │     2      5      8
   3 │     3      4      8
```

[Anonymous functions](https://docs.julialang.org/en/v1/manual/functions/#man-anonymous-functions)
are a convenient way to define and use an `operation_function`
all within the manipulation function call.

```julia
julia> select(df, :a => ByRow(x -> x + 1))
3×1 DataFrame
 Row │ a_function
     │ Int64
─────┼────────────
   1 │          2
   2 │          3
   3 │          4

julia> transform(df, [:a, :b] => ByRow((x, y) -> 2x + y))
3×3 DataFrame
 Row │ a      b      a_b_function
     │ Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      4             6
   2 │     2      5             9
   3 │     3      4            10

julia> subset(df, :b => ByRow(x -> x < 5))
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      4

julia> subset(df, :b => ByRow(<(5))) # shorter version of the previous
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      4
```

!!! note

    `operation_functions` within `subset` or `subset!` function calls
    must return a Boolean vector.
    `true` elements in the Boolean vector will determine
    which rows are retained in the resulting data frame.

As demonstrated above, `DataFrame` columns are usually passed
from `source_column_selector` to `operation_function` as one or more
vector arguments.
However, when `AsTable(source_column_selector)` is used,
the selected columns are collected and passed as a single `NamedTuple`
to `operation_function`.

This is often useful when your `operation_function` is defined to operate
on a single collection argument rather than on multiple positional arguments.
The distinction is somewhat similar to the difference between the built-in
`min` and `minimum` functions.
`min` is defined to find the minimum value among multiple positional arguments,
while `minimum` is defined to find the minimum value
among the elements of a single collection argument.

```julia
julia> df = DataFrame(a = 1:2, b = 3:4, c = 5:6, d = 2:-1:1)
2×4 DataFrame
 Row │ a      b      c      d
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      3      5      2
   2 │     2      4      6      1

julia> select(df, Cols(:) => ByRow(min)) # min operates on multiple arguments
2×1 DataFrame
 Row │ a_b_etc_min
     │ Int64
─────┼─────────────
   1 │           1
   2 │           1

julia> select(df, AsTable(:) => ByRow(minimum)) # minimum operates on a collection
2×1 DataFrame
 Row │ a_b_etc_minimum
     │ Int64
─────┼─────────────────
   1 │               1
   2 │               1

julia> select(df, [:a,:b] => ByRow(+)) # `+` operates on a multiple arguments
2×1 DataFrame
 Row │ a_b_+
     │ Int64
─────┼───────
   1 │     4
   2 │     6

julia> select(df, AsTable([:a,:b]) => ByRow(sum)) # `sum` operates on a collection
2×1 DataFrame
 Row │ a_b_sum
     │ Int64
─────┼─────────
   1 │       4
   2 │       6

julia> using Statistics # contains the `mean` function

julia> select(df, AsTable(Between(:b, :d)) => ByRow(mean)) # `mean` operates on a collection
2×1 DataFrame
 Row │ b_c_d_mean
     │ Float64
─────┼────────────
   1 │    3.33333
   2 │    3.66667
```

`AsTable` can also be used to pass columns to a function which operates
on fields of a `NamedTuple`.

```julia
julia> df = DataFrame(a = 1:2, b = 3:4, c = 5:6, d = 7:8)
2×4 DataFrame
 Row │ a      b      c      d
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      3      5      7
   2 │     2      4      6      8

julia> f(nt) = nt.a + nt.d
f (generic function with 1 method)

julia> transform(df, AsTable(:) => ByRow(f))
2×5 DataFrame
 Row │ a      b      c      d      a_b_etc_f
     │ Int64  Int64  Int64  Int64  Int64
─────┼───────────────────────────────────────
   1 │     1      3      5      7          8
   2 │     2      4      6      8         10
```

As demonstrated above,
in the `source_column_selector => operation_function` operation pair form,
the results of an operation will be placed into a new column with an
automatically-generated name based on the operation;
the new column name will be the `operation_function` name
appended to the source column name(s) with an underscore.

This automatic column naming behavior can be avoided in two ways.
First, the operation result can be placed back into the original column
with the original column name by switching the keyword argument `renamecols`
from its default value (`true`) to `renamecols=false`.
This option prevents the function name from being appended to the column name
as it usually would be.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> transform(df, :a => ByRow(x->x+10), renamecols=false) # add 10 in-place
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │    11      5
   2 │    12      6
   3 │    13      7
   4 │    14      8
```

The second method to avoid the default manipulation column naming is to
specify your own `new_column_names`.

#### `new_column_names`

`new_column_names` can be included at the end of an `operation` pair to specify
the name of the new column(s).
`new_column_names` may be a symbol, string, function, vector of symbols, vector of strings, or `AsTable`.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> transform(df, Cols(:) => ByRow(+) => :c)
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      6
   2 │     2      6      8
   3 │     3      7     10
   4 │     4      8     12

julia> transform(df, Cols(:) => ByRow(+) => "a+b")
4×3 DataFrame
 Row │ a      b      a+b
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      6
   2 │     2      6      8
   3 │     3      7     10
   4 │     4      8     12

julia> transform(df, :a => ByRow(x->x+10) => "a+10")
4×3 DataFrame
 Row │ a      b      a+10
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5     11
   2 │     2      6     12
   3 │     3      7     13
   4 │     4      8     14
```

The `source_column_selector => new_column_names` operation form
can be used to rename columns without an intermediate function.
However, there are `rename` and `rename!` functions,
which accept similar syntax,
that tend to be more useful for this operation.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> transform(df, :a => :apple) # adds column `apple`
4×3 DataFrame
 Row │ a      b      apple
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      2
   3 │     3      7      3
   4 │     4      8      4

julia> select(df, :a => :apple) # retains only column `apple`
4×1 DataFrame
 Row │ apple
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     4

julia> rename(df, :a => :apple) # renames column `a` to `apple` in-place
4×2 DataFrame
 Row │ apple  b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8
```

If `new_column_names` already exist in the source data frame,
those columns will be replaced in the existing column location
rather than being added to the end.
This can be done by manually specifying an existing column name
or by using the `renamecols=false` keyword argument.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> transform(df, :b => (x -> x .+ 10))  # automatic new column and column name
4×3 DataFrame
 Row │ a      b      b_function
     │ Int64  Int64  Int64
─────┼──────────────────────────
   1 │     1      5          15
   2 │     2      6          16
   3 │     3      7          17
   4 │     4      8          18

julia> transform(df, :b => (x -> x .+ 10), renamecols=false)  # transform column in-place
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1     15
   2 │     2     16
   3 │     3     17
   4 │     4     18

julia> transform(df, :b => (x -> x .+ 10) => :a)  # replace column :a
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │    15      5
   2 │    16      6
   3 │    17      7
   4 │    18      8
```

Actually, `renamecols=false` just prevents the function name from being appended
to the final column name such that the operation is *usually* returned to the same column.

```julia
julia> transform(df, [:a, :b] => +)  # new column name is all source columns and function name
4×3 DataFrame
 Row │ a      b      a_b_+
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      6
   2 │     2      6      8
   3 │     3      7     10
   4 │     4      8     12

julia> transform(df, [:a, :b] => +, renamecols=false)  # same as above but with no function name
4×3 DataFrame
 Row │ a      b      a_b
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      6
   2 │     2      6      8
   3 │     3      7     10
   4 │     4      8     12

julia> transform(df, [:a, :b] => (+) => :a)  # manually overwrite column :a (see Note below about parentheses)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     6      5
   2 │     8      6
   3 │    10      7
   4 │    12      8
```

In the `source_column_selector => operation_function => new_column_names` operation form,
`new_column_names` may also be a renaming function which operates on a string
to create the destination column names programmatically.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> add_prefix(s) = "new_" * s
add_prefix (generic function with 1 method)

julia> transform(df, :a => (x -> 10 .* x) => add_prefix) # with named renaming function
4×3 DataFrame
 Row │ a      b      new_a
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5     10
   2 │     2      6     20
   3 │     3      7     30
   4 │     4      8     40

julia> transform(df, :a => (x -> 10 .* x) => (s -> "new_" * s)) # with anonymous renaming function
4×3 DataFrame
 Row │ a      b      new_a
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5     10
   2 │     2      6     20
   3 │     3      7     30
   4 │     4      8     40
```

!!! note

    It is a good idea to wrap anonymous functions in parentheses
    to avoid the `=>` operator accidently becoming part of the anonymous function.
    The examples above do not work correctly without the parentheses!
    ```julia
    julia> transform(df, :a => x -> 10 .* x => add_prefix)  # Not what we wanted!
    4×3 DataFrame
     Row │ a      b      a_function
         │ Int64  Int64  Pair…
    ─────┼────────────────────────────────────────────
       1 │     1      5  [10, 20, 30, 40]=>add_prefix
       2 │     2      6  [10, 20, 30, 40]=>add_prefix
       3 │     3      7  [10, 20, 30, 40]=>add_prefix
       4 │     4      8  [10, 20, 30, 40]=>add_prefix
    julia> transform(df, :a => x -> 10 .* x => s -> "new_" * s)  # Not what we wanted!
    4×3 DataFrame
     Row │ a      b      a_function
         │ Int64  Int64  Pair…
    ─────┼─────────────────────────────────────
       1 │     1      5  [10, 20, 30, 40]=>#18
       2 │     2      6  [10, 20, 30, 40]=>#18
       3 │     3      7  [10, 20, 30, 40]=>#18
       4 │     4      8  [10, 20, 30, 40]=>#18
    ```

A renaming function will not work in the
`source_column_selector => new_column_names` operation form
because a function in the second element of the operation pair is assumed to take
the `source_column_selector => operation_function` operation form.
To work around this limitation, use the
`source_column_selector => operation_function => new_column_names` operation form
with `identity` as the `operation_function`.

```julia
julia> transform(df, :a => add_prefix)
ERROR: MethodError: no method matching *(::String, ::Vector{Int64})

julia> transform(df, :a => identity => add_prefix)
4×3 DataFrame
 Row │ a      b      new_a
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      5      1
   2 │     2      6      2
   3 │     3      7      3
   4 │     4      8      4
```

In this case though,
it is probably again more useful to use the `rename` or `rename!` function
rather than one of the manipulation functions
in order to rename in-place and avoid the intermediate `operation_function`.
```julia
julia> rename(add_prefix, df)  # rename all columns with a function
4×2 DataFrame
 Row │ new_a  new_b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> rename(add_prefix, df; cols=:a)  # rename some columns with a function
4×2 DataFrame
 Row │ new_a  b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8
```

In the `source_column_selector => new_column_names` operation form,
only a single source column may be selected per operation,
so why is `new_column_names` plural?
It is possible to split the data contained inside a single column
into multiple new columns by supplying a vector of strings or symbols
as `new_column_names`.

```julia
julia> df = DataFrame(data = [(1,2), (3,4)]) # vector of tuples
2×1 DataFrame
 Row │ data
     │ Tuple…
─────┼────────
   1 │ (1, 2)
   2 │ (3, 4)

julia> transform(df, :data => [:first, :second]) # manual naming
2×3 DataFrame
 Row │ data    first  second
     │ Tuple…  Int64  Int64
─────┼───────────────────────
   1 │ (1, 2)      1       2
   2 │ (3, 4)      3       4
```

This kind of data splitting can even be done automatically with `AsTable`.

```julia
julia> transform(df, :data => AsTable) # default automatic naming with tuples
2×3 DataFrame
 Row │ data    x1     x2
     │ Tuple…  Int64  Int64
─────┼──────────────────────
   1 │ (1, 2)      1      2
   2 │ (3, 4)      3      4
```

If a data frame column contains `NamedTuple`s,
then `AsTable` will preserve the field names.
```julia
julia> df = DataFrame(data = [(a=1,b=2), (a=3,b=4)]) # vector of named tuples
2×1 DataFrame
 Row │ data
     │ NamedTup…
─────┼────────────────
   1 │ (a = 1, b = 2)
   2 │ (a = 3, b = 4)

julia> transform(df, :data => AsTable) # keeps names from named tuples
2×3 DataFrame
 Row │ data            a      b
     │ NamedTup…       Int64  Int64
─────┼──────────────────────────────
   1 │ (a = 1, b = 2)      1      2
   2 │ (a = 3, b = 4)      3      4
```

!!! note

    To pack multiple columns into a single column of `NamedTuple`s
    (reverse of the above operation)
    apply the `identity` function `ByRow`, e.g.
    `transform(df, AsTable([:a, :b]) => ByRow(identity) => :data)`.

Renaming functions also work for multi-column transformations,
but they must operate on a vector of strings.

```julia
julia> df = DataFrame(data = [(1,2), (3,4)])
2×1 DataFrame
 Row │ data
     │ Tuple…
─────┼────────
   1 │ (1, 2)
   2 │ (3, 4)

julia> new_names(v) = ["primary ", "secondary "] .* v
new_names (generic function with 1 method)

julia> transform(df, :data => identity => new_names)
2×3 DataFrame
 Row │ data    primary data  secondary data
     │ Tuple…  Int64         Int64
─────┼──────────────────────────────────────
   1 │ (1, 2)             1               2
   2 │ (3, 4)             3               4
```

### Applying Multiple Operations per Manipulation
All data frame manipulation functions can accept multiple `operation` pairs
at once using any of the following methods:
- `manipulation_function(dataframe, operation1, operation2)`   : multiple arguments
- `manipulation_function(dataframe, [operation1, operation2])` : vector argument
- `manipulation_function(dataframe, [operation1 operation2])`  : matrix argument

Passing multiple operations is especially useful for the `select`, `select!`,
and `combine` manipulation functions,
since they only retain columns which are a result of the passed operations.

```julia
julia> df = DataFrame(a = 1:4, b = [50,50,60,60], c = ["hat","bat","cat","dog"])
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     1     50  hat
   2 │     2     50  bat
   3 │     3     60  cat
   4 │     4     60  dog

julia> combine(df, :a => maximum, :b => sum, :c => join) # 3 combine operations
1×3 DataFrame
 Row │ a_maximum  b_sum  c_join
     │ Int64      Int64  String
─────┼────────────────────────────────
   1 │         4    220  hatbatcatdog

julia> select(df, :c, :b, :a) # re-order columns
4×3 DataFrame
 Row │ c       b      a
     │ String  Int64  Int64
─────┼──────────────────────
   1 │ hat        50      1
   2 │ bat        50      2
   3 │ cat        60      3
   4 │ dog        60      4

ulia> select(df, :b, :) # `:` here means all other columns
4×3 DataFrame
 Row │ b      a      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │    50      1  hat
   2 │    50      2  bat
   3 │    60      3  cat
   4 │    60      4  dog

julia> select(
           df,
           :c => (x -> "a " .* x) => :one_c,
           :a => (x -> 100x),
           :b,
           renamecols=false
       ) # can mix operation forms
4×3 DataFrame
 Row │ one_c   a      b
     │ String  Int64  Int64
─────┼──────────────────────
   1 │ a hat     100     50
   2 │ a bat     200     50
   3 │ a cat     300     60
   4 │ a dog     400     60

julia> select(
           df,
           :c => ByRow(reverse),
           :c => ByRow(uppercase)
       ) # multiple operations on same column
4×2 DataFrame
 Row │ c_reverse  c_uppercase
     │ String     String
─────┼────────────────────────
   1 │ tah        HAT
   2 │ tab        BAT
   3 │ tac        CAT
   4 │ god        DOG
```

In the last two examples,
the manipulation function arguments were split across multiple lines.
This is a good way to make manipulations with many operations more readable.

Passing multiple operations to `subset` or `subset!` is an easy way to narrow in
on a particular row of data.

```julia
julia> subset(
           df,
           :b => ByRow(==(60)),
           :c => ByRow(contains("at"))
       ) # rows with 60 and "at"
1×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  String
─────┼──────────────────────
   1 │     3     60  cat
```

Note that all operations within a single manipulation must use the data
as it existed before the function call
i.e. you cannot use newly created columns for subsequent operations
within the same manipulation.

```julia
julia> transform(
           df,
           [:a, :b] => ByRow(+) => :d,
           :d => (x -> x ./ 2),
       ) # requires two separate transformations
ERROR: ArgumentError: column name :d not found in the data frame; existing most similar names are: :a, :b and :c

julia> new_df = transform(df, [:a, :b] => ByRow(+) => :d)
4×4 DataFrame
 Row │ a      b      c       d
     │ Int64  Int64  String  Int64
─────┼─────────────────────────────
   1 │     1     50  hat        51
   2 │     2     50  bat        52
   3 │     3     60  cat        63
   4 │     4     60  dog        64

julia> transform!(new_df, :d => (x -> x ./ 2) => :d_2)
4×5 DataFrame
 Row │ a      b      c       d      d_2
     │ Int64  Int64  String  Int64  Float64
─────┼──────────────────────────────────────
   1 │     1     50  hat        51     25.5
   2 │     2     50  bat        52     26.0
   3 │     3     60  cat        63     31.5
   4 │     4     60  dog        64     32.0
```


### Broadcasting Operation Pairs

[Broadcasting](https://docs.julialang.org/en/v1/manual/arrays/#Broadcasting)
pairs with `.=>` is often a convenient way to generate multiple
similar `operation`s to be applied within a single manipulation.
Broadcasting within the `Pair` of an `operation` is no different than
broadcasting in base Julia.
The broadcasting `.=>` will be expanded into a vector of pairs
(`[operation1, operation2, ...]`),
and this expansion will occur before the manipulation function is invoked.
Then the manipulation function will use the
`manipulation_function(dataframe, [operation1, operation2, ...])` method.
This process will be explained in more detail below.

To illustrate these concepts, let us first examine the `Type` of a basic `Pair`.
In DataFrames.jl, a symbol, string, or integer
may be used to select a single column.
Some `Pair`s with these types are below.

```julia
julia> typeof(:x => :a)
Pair{Symbol, Symbol}

julia> typeof("x" => "a")
Pair{String, String}

julia> typeof(1 => "a")
Pair{Int64, String}
```

Any of the `Pair`s above could be used to rename the first column
of the data frame below to `a`.

```julia
julia> df = DataFrame(x = 1:3, y = 4:6)
3×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> select(df, :x => :a)
3×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> select(df, 1 => "a")
3×1 DataFrame
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
```

What should we do if we want to keep and rename both the `x` and `y` column?
One option is to supply a `Vector` of operation `Pair`s to `select`.
`select` will process all of these operations in order.

```julia
julia> ["x" => "a", "y" => "b"]
2-element Vector{Pair{String, String}}:
 "x" => "a"
 "y" => "b"

julia> select(df, ["x" => "a", "y" => "b"])
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6
```

We can use broadcasting to simplify the syntax above.

```julia
julia> ["x", "y"] .=> ["a", "b"]
2-element Vector{Pair{String, String}}:
 "x" => "a"
 "y" => "b"

julia> select(df, ["x", "y"] .=> ["a", "b"])
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6
```

Notice that `select` sees the same `Vector{Pair{String, String}}` operation
argument whether the individual pairs are written out explicitly or
constructed with broadcasting.
The broadcasting is applied before the call to `select`.

```julia
julia> ["x" => "a", "y" => "b"] == (["x", "y"] .=> ["a", "b"])
true
```

!!! note

    These operation pairs (or vector of pairs) can be given variable names.
    This is uncommon in practice but could be helpful for intermediate
    inspection and testing.
    ```julia
    df = DataFrame(x = 1:3, y = 4:6)       # create data frame
    operation = ["x", "y"] .=> ["a", "b"]  # save operation to variable
    typeof(operation)                      # check type of operation
    first(operation)                       # check first pair in operation
    last(operation)                        # check last pair in operation
    select(df, operation)                  # manipulate `df` with `operation`
    ```

In Julia,
a non-vector broadcasted with a vector will be repeated in each resultant pair element.

```julia
julia> ["x", "y"] .=> :a    # :a is repeated
2-element Vector{Pair{String, Symbol}}:
 "x" => :a
 "y" => :a

julia> 1 .=> [:a, :b]       # 1 is repeated
2-element Vector{Pair{Int64, Symbol}}:
 1 => :a
 1 => :b
```

We can use this fact to easily broadcast an `operation_function` to multiple columns.

```julia
julia> f(x) = 2 * x
f (generic function with 1 method)

julia> ["x", "y"] .=> f  # f is repeated
2-element Vector{Pair{String, typeof(f)}}:
 "x" => f
 "y" => f

julia> select(df, ["x", "y"] .=> f)  # apply f with automatic column renaming
3×2 DataFrame
 Row │ x_f    y_f
     │ Int64  Int64
─────┼──────────────
   1 │     2      8
   2 │     4     10
   3 │     6     12

julia> ["x", "y"] .=> f .=> ["a", "b"]  # f is repeated
2-element Vector{Pair{String, Pair{typeof(f), String}}}:
 "x" => (f => "a")
 "y" => (f => "b")

julia> select(df, ["x", "y"] .=> f .=> ["a", "b"])  # apply f with manual column renaming
3×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      8
   2 │     4     10
   3 │     6     12
```

A renaming function can be applied to multiple columns in the same way.
It will also be repeated in each operation `Pair`.

```julia
julia> newname(s::String) = s * "_new"
newname (generic function with 1 method)

julia> ["x", "y"] .=> f .=> newname  # both f and newname are repeated
2-element Vector{Pair{String, Pair{typeof(f), typeof(newname)}}}:
 "x" => (f => newname)
 "y" => (f => newname)

julia> select(df, ["x", "y"] .=> f .=> newname)  # apply f then rename column with newname
3×2 DataFrame
 Row │ x_new  y_new
     │ Int64  Int64
─────┼──────────────
   1 │     2      8
   2 │     4     10
   3 │     6     12
```

You can see from the type output above
that a three element pair does not actually exist.
A `Pair` (as the name implies) can only contain two elements.
Thus, `:x => :y => :z` becomes a nested `Pair`,
where `:x` is the first element and points to the `Pair` `:y => :z`,
which is the second element.

```julia
julia> p = :x => :y => :z
:x => (:y => :z)

julia> p[1]
:x

julia> p[2]
:y => :z

julia> p[2][1]
:y

julia> p[2][2]
:z

julia> p[3] # there is no index 3 for a pair
ERROR: BoundsError: attempt to access Pair{Symbol, Pair{Symbol, Symbol}} at index [3]
```

In the previous examples, the source columns have been individually selected.
When broadcasting multiple columns to the same function,
often similarities in the column names or position can be exploited to avoid
tedious selection.
Consider a data frame with temperature data at three different locations
taken over time.
```julia
julia> df = DataFrame(Time = 1:4,
                      Temperature1 = [20, 23, 25, 28],
                      Temperature2 = [33, 37, 41, 44],
                      Temperature3 = [15, 10, 4, 0])
4×4 DataFrame
 Row │ Time   Temperature1  Temperature2  Temperature3
     │ Int64  Int64         Int64         Int64
─────┼─────────────────────────────────────────────────
   1 │     1            20            33            15
   2 │     2            23            37            10
   3 │     3            25            41             4
   4 │     4            28            44             0
```

To convert all of the temperature data in one transformation,
we just need to define a conversion function and broadcast
it to all of the "Temperature" columns.

```julia
julia> celsius_to_kelvin(x) = x + 273
celsius_to_kelvin (generic function with 1 method)

julia> transform(
           df,
           Cols(r"Temp") .=> ByRow(celsius_to_kelvin),
           renamecols = false
       )
4×4 DataFrame
 Row │ Time   Temperature1  Temperature2  Temperature3
     │ Int64  Int64         Int64         Int64
─────┼─────────────────────────────────────────────────
   1 │     1           293           306           288
   2 │     2           296           310           283
   3 │     3           298           314           277
   4 │     4           301           317           273
```
Or, simultaneously changing the column names:

```julia
julia> rename_function(s) = "Temperature $(last(s)) (K)"
rename_function (generic function with 1 method)

julia> select(
           df,
           "Time",
           Cols(r"Temp") .=> ByRow(celsius_to_kelvin) .=> rename_function
       )
4×4 DataFrame
 Row │ Time   Temperature 1 (K)  Temperature 2 (K)  Temperature 3 (K)
     │ Int64  Int64              Int64              Int64
─────┼────────────────────────────────────────────────────────────────
   1 │     1                293                306                288
   2 │     2                296                310                283
   3 │     3                298                314                277
   4 │     4                301                317                273
```

!!! note "Notes"

    * `Not("Time")` or `2:4` would have been equally good choices
      for `source_column_selector` in the above operations.

    * Don't forget `ByRow` if your function is to be applied to elements
      rather than entire column vectors.
      Without `ByRow`, the manipulations above would have thrown
      `ERROR: MethodError: no method matching +(::Vector{Int64}, ::Int64)`.

    * Regular expression (`r""`) and `:` `source_column_selectors`
      must be wrapped in `Cols` to be properly broadcasted
      because otherwise the broadcasting occurs before the expression
      is expanded into a vector of matches.

You could also broadcast different columns to different functions
by supplying a vector of functions.

```julia
julia> df = DataFrame(a=1:4, b=5:8)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      5
   2 │     2      6
   3 │     3      7
   4 │     4      8

julia> f1(x) = x .+ 1
f1 (generic function with 1 method)

julia> f2(x) = x ./ 10
f2 (generic function with 1 method)

julia> transform(df, [:a, :b] .=> [f1, f2])
4×4 DataFrame
 Row │ a      b      a_f1   b_f2
     │ Int64  Int64  Int64  Float64
─────┼──────────────────────────────
   1 │     1      5      2      0.5
   2 │     2      6      3      0.6
   3 │     3      7      4      0.7
   4 │     4      8      5      0.8
```

However, this form is not much more convenient than supplying
multiple individual operations.

```julia
julia> transform(df, [:a => f1, :b => f2]) # same manipulation as previous
4×4 DataFrame
 Row │ a      b      a_f1   b_f2
     │ Int64  Int64  Int64  Float64
─────┼──────────────────────────────
   1 │     1      5      2      0.5
   2 │     2      6      3      0.6
   3 │     3      7      4      0.7
   4 │     4      8      5      0.8
```

Perhaps more useful for broadcasting syntax
is to apply multiple functions to multiple columns
by changing the vector of functions to a 1-by-x matrix of functions.
(Recall that a list, a vector, or a matrix of operation pairs are all valid
for passing to the manipulation functions.)

```julia
julia> [:a, :b] .=> [f1 f2] # No comma `,` between f1 and f2
2×2 Matrix{Pair{Symbol}}:
 :a=>f1  :a=>f2
 :b=>f1  :b=>f2

julia> transform(df, [:a, :b] .=> [f1 f2]) # No comma `,` between f1 and f2
4×6 DataFrame
 Row │ a      b      a_f1   b_f1   a_f2     b_f2
     │ Int64  Int64  Int64  Int64  Float64  Float64
─────┼──────────────────────────────────────────────
   1 │     1      5      2      6      0.1      0.5
   2 │     2      6      3      7      0.2      0.6
   3 │     3      7      4      8      0.3      0.7
   4 │     4      8      5      9      0.4      0.8
```

In this way, every combination of selected columns and functions will be applied.

Pair broadcasting is a simple but powerful tool
that can be used in any of the manipulation functions listed under
[Manipulation Functions](@ref).
Experiment for yourself to discover other useful operations.

### Additional Resources
More details and examples of operation pair syntax can be found in
[this blog post](https://bkamins.github.io/julialang/2020/12/24/minilanguage.html).
(The official wording describing the syntax has changed since the blog post was written,
but the examples are still illustrative.
The operation pair syntax is sometimes referred to as the DataFrames.jl mini-language
or Domain-Specific Language.)

For additional syntax niceties,
many users find the [Chain.jl](https://github.com/jkrumbiegel/Chain.jl)
and [DataFramesMeta.jl](https://github.com/JuliaData/DataFramesMeta.jl)
packages useful
to help simplify manipulations that may be tedious with operation pairs alone.

## Approach Comparison

After that deep dive into [Manipulation Functions](@ref),
it is a good idea to review the alternative approaches covered in
[Getting and Setting Data in a Data Frame](@ref).
Let us compare the approaches with a few examples.

For simple operations,
often getting/setting data with dot syntax
is simpler than the equivalent data frame manipulation.
Here we will add the two columns of our data frame together
and place the result in a new third column.

**Setup:**

```julia
julia> df = DataFrame(x = 1:3, y = 4:6)  # define a data frame
3×2 DataFrame
 Row │ x      y
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6
```

**Manipulation:**

```julia
julia> transform!(df, [:x, :y] => (+) => :z)
3×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      5
   2 │     2      5      7
   3 │     3      6      9
```

**Dot Syntax:**

```julia
julia> df.z = df.x + df.y
3-element Vector{Int64}:
 5
 7
 9

julia> df  # see that the previous expression updated the data frame `df`
3×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      5
   2 │     2      5      7
   3 │     3      6      9
```

Recall that the return type from a data frame manipulation function call
is always a data frame.
The return type of a data frame column accessed with dot syntax is a `Vector`.
Thus the expression `df.x + df.y` gets the column data as vectors
and returns the result of the vector addition.
However, in that same line,
we assigned the resultant `Vector` to a new column `z` in the data frame `df`.
We could have instead assigned the resultant `Vector` to some other variable,
and then `df` would not have been altered.
The approach with dot syntax is very versatile
since the data getting, mathematics, and data setting can be separate steps.

```julia
julia> df.x  # dot syntax returns a vector
3-element Vector{Int64}:
 1
 2
 3

julia> v = df.x + df.y  # assign mathematical result to a vector `v`
3-element Vector{Int64}:
 5
 7
 9

julia> df.z = v  # place `v` into the data frame `df` with the column name `z`
3-element Vector{Int64}:
 5
 7
 9
```

However, one way in which dot syntax is less versatile
is that the column name must be explicitly written in the code.
Indexing syntax is a good alternative in these cases
which is only slightly longer to write than dot syntax.
Both indexing syntax and manipulation functions can operate on dynamic column names
stored in variables.

**Setup:**

Imagine this setup data was read from a file and/or entered by a user at runtime.

```julia
julia> df = DataFrame("My First Column" => 1:3, "My Second Column" => 4:6)  # define a data frame
3×2 DataFrame
 Row │ My First Column  My Second Column
     │ Int64            Int64
─────┼───────────────────────────────────
   1 │               1                 4
   2 │               2                 5
   3 │               3                 6

julia> c1 = "My First Column"; c2 = "My Second Column"; c3 = "My Third Column";  # define column names
```

**Dot Syntax:**

```julia
julia> df.c1  # dot syntax expects an explicit column name and cannot be used to access variable column name
ERROR: ArgumentError: column name :c1 not found in the data frame
```

**Indexing:**

```julia
julia> df[:, c3] = df[:, c1] + df[:, c2]  # access columns with names stored in variables
3-element Vector{Int64}:
 5
 7
 9

julia> df  # see that the previous expression updated the data frame `df`
3×3 DataFrame
 Row │ My First Column  My Second Column  My Third Column
     │ Int64            Int64             Int64
─────┼────────────────────────────────────────────────────
   1 │               1                 4                5
   2 │               2                 5                7
   3 │               3                 6                9
```

**Manipulation:**

```julia
julia> transform!(df, [c1, c2] => (+) => c3)  # access columns with names stored in variables
3×3 DataFrame
 Row │ My First Column  My Second Column  My Third Column
     │ Int64            Int64             Int64
─────┼────────────────────────────────────────────────────
   1 │               1                 4                5
   2 │               2                 5                7
   3 │               3                 6                9
```

Additionally, manipulation functions only require
the name of the data frame to be written once.
This can be helpful when dealing with long variable and column names.

**Setup:**

```julia
julia> my_very_long_data_frame_name = DataFrame(
           "My First Column" => 1:3,
           "My Second Column" => 4:6
       )  # define a data frame
3×2 DataFrame
 Row │ My First Column  My Second Column
     │ Int64            Int64
─────┼───────────────────────────────────
   1 │               1                 4
   2 │               2                 5
   3 │               3                 6

julia> c1 = "My First Column"; c2 = "My Second Column"; c3 = "My Third Column";  # define column names
```
**Manipulation:**

```julia

julia> transform!(my_very_long_data_frame_name, [c1, c2] => (+) => c3)
3×3 DataFrame
 Row │ My First Column  My Second Column  My Third Column
     │ Int64            Int64             Int64
─────┼────────────────────────────────────────────────────
   1 │               1                 4                5
   2 │               2                 5                7
   3 │               3                 6                9
```

**Indexing:**

```julia
julia> my_very_long_data_frame_name[:, c3] = my_very_long_data_frame_name[:, c1] + my_very_long_data_frame_name[:, c2]
3-element Vector{Int64}:
 5
 7
 9

julia> df  # see that the previous expression updated the data frame `df`
3×3 DataFrame
 Row │ My First Column  My Second Column  My Third Column
     │ Int64            Int64             Int64
─────┼────────────────────────────────────────────────────
   1 │               1                 4                5
   2 │               2                 5                7
   3 │               3                 6                9
```

Another benefit of manipulation functions and indexing over dot syntax is that
it is easier to operate on a subset of columns.

**Setup:**

```julia
julia> df = DataFrame(x = 1:3, y = 4:6, z = 7:9)  # define data frame
3×3 DataFrame
 Row │ x      y      z
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      7
   2 │     2      5      8
   3 │     3      6      9
```

**Dot Syntax:**

```julia
julia> df.Not(:x)  # will not work; requires a literal column name
ERROR: ArgumentError: column name :Not not found in the data frame
```

**Manipulation:**

```julia
julia> transform!(df, Not(:x) => ByRow(max))  # find maximum value across all rows except for column `x`
3×4 DataFrame
 Row │ x      y      z      y_z_max
     │ Int64  Int64  Int64  Int64
─────┼──────────────────────────────
   1 │     1      4      7        7
   2 │     2      5      8        8
   3 │     3      6      9        9
```

**Indexing:**

```julia
julia> df[:, :y_z_max] = maximum.(eachrow(df[:, Not(:x)]))  # find maximum value across all rows except for column `x`
3-element Vector{Int64}:
 7
 8
 9

julia> df  # see that the previous expression updated the data frame `df`
3×4 DataFrame
 Row │ x      y      z      y_z_max
     │ Int64  Int64  Int64  Int64
─────┼──────────────────────────────
   1 │     1      4      7        7
   2 │     2      5      8        8
   3 │     3      6      9        9
```

Moreover, indexing can operate on a subset of columns *and* rows.

**Indexing:**

```julia
julia> y_z_max_row3 = maximum(df[3, Not(:x)])  # find maximum value across row 3 except for column `x`
9
```

Hopefully this small comparison has illustrated some of the benefits and drawbacks
of the various syntaxes available in DataFrames.jl.
The best syntax to use depends on the situation.
