# Data Frame Manipulation Functions

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

## Constructing Operations

All of the functions above use the same syntax which is commonly
`manipulation_function(dataframe, operation)`.
The `operation` argument defines the
operation to be applied to the source `dataframe`,
and it can take any of the following common forms explained below:

`source_column_selector`
: selects source column(s) without manipulating or renaming them

   Examples: `:a`, `[:a, :b]`, `All()`, `Not(:a)`

`source_column_selector => operation_function`
: passes source column(s) as arguments to a function
and automatically names the resulting column(s)

   Examples: `:a => sum`, `[:a, :b] => +`, `:a => ByRow(==(3))`

`source_column_selector => operation_function => new_column_names`
: passes source column(s) as arguments to a function
and names the resulting column(s) `new_column_names`

   Examples: `:a => sum => :sum_of_a`, `[:a, :b] => + => :a_plus_b`

   *(Not available for `subset` or `subset!`)*

`source_column_selector => new_column_names`
: renames a source column,
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
The provided examples will be explained in more detail below.

The manipulation functions also have methods for applying multiple operations.
See the later sections [Multiple Operations per Manipulation](@ref)
and [Broadcasting Operation Pairs](@ref) for more information.

### `source_column_selector`
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
a [regular expression](https://docs.julialang.org/en/v1/manual/strings/#Regular-Expressions),
a `Not`, `Between`, `All`, or `Cols` expression,
or a `:`.
See the [Indexing](@ref) API for the full list of possible values with references.

!!! Note
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

### `operation_function`
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

julia> h(x, y) = 2x .+ y
h (generic function with 1 method)

julia> transform(df, [:a, :b] => h)
3×3 DataFrame
 Row │ a      b      a_b_h
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      4      6
   2 │     2      5      9
   3 │     3      4     10
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

!!! Note
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

### `new_column_names`

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

Additionally, in the
`source_column_selector => operation_function => new_column_names` operation form,
`new_column_names` may be a renaming function which operates on a string
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

!!! Note
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

!!! Note
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

## Applying Multiple Operations per Manipulation
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


## Broadcasting Operation Pairs

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

!!! Note
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

!!! Note Notes
      * `Not("Time")` or `2:4` would have been equally good choices for `source_column_selector` in the above operations.
      * Don't forget `ByRow` if your function is to be applied to elements rather than entire column vectors.
      Without `ByRow`, the manipulations above would have thrown
      `ERROR: MethodError: no method matching +(::Vector{Int64}, ::Int64)`.
      * Regular expression (`r""`) and `:` `source_column_selectors`
      must be wrapped in `Cols` to be properly broadcasted
      because otherwise the broadcasting occurs before the expression is expanded into a vector of matches.

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
[Basic Usage of Manipulation Functions](@ref).
Experiment for yourself to discover other useful operations.

## Additional Resources
More details and examples of operation pair syntax can be found in
[this blog post](https://bkamins.github.io/julialang/2020/12/24/minilanguage.html).
(The official wording describing the syntax has changed since the blog post was written,
but the examples are still illustrative.
The operation pair syntax is sometimes referred to as the DataFrames.jl mini-language
or Domain-Specific Language.)

For additional practice,
an interactive tutorial is provided on a variety of introductory topics
by the DataFrames.jl package author
[here](https://github.com/bkamins/Julia-DataFrames-Tutorial).


For additional syntax niceties,
many users find the [Chain.jl](https://github.com/jkrumbiegel/Chain.jl)
and [DataFramesMeta.jl](https://github.com/JuliaData/DataFramesMeta.jl)
packages useful
to help simplify manipulations that may be tedious with operation pairs alone.