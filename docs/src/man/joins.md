# Database-Style Joins

## Introduction to joins

We often need to combine two or more data sets together to provide a complete
picture of the topic we are studying. For example, suppose that we have the
following two data sets:

```jldoctest joins
julia> using DataFrames

julia> people = DataFrame(ID=[20, 40], Name=["John Doe", "Jane Doe"])
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │    20  John Doe
   2 │    40  Jane Doe

julia> jobs = DataFrame(ID=[20, 40], Job=["Lawyer", "Doctor"])
2×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │    20  Lawyer
   2 │    40  Doctor
```

We might want to work with a larger data set that contains both the names and
jobs for each ID. We can do this using the `innerjoin` function:

```jldoctest joins
julia> innerjoin(people, jobs, on = :ID)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String
─────┼─────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    40  Jane Doe  Doctor
```

In relational database theory, this operation is generally referred to as a
join. The columns used to determine which rows should be combined during a join
are called keys.

The following functions are provided to perform seven kinds of joins:

- `innerjoin`: the output contains rows for values of the key that exist in all
  passed data frames.
- `leftjoin`: the output contains rows for values of the key that exist in the
  first (left) argument, whether or not that value exists in the second (right)
  argument.
- `rightjoin`: the output contains rows for values of the key that exist in the
  second (right) argument, whether or not that value exists in the first (left)
  argument.
- `outerjoin`: the output contains rows for values of the key that exist in any
  of the passed data frames.
- `semijoin`: Like an inner join, but output is restricted to columns from the
  first (left) argument.
- `antijoin`: The output contains rows for values of the key that exist in the
  first (left) but not the second (right) argument. As with `semijoin`, output
  is restricted to columns from the first (left) argument.
- `crossjoin`: The output is the cartesian product of rows from all passed data
  frames.

See [the Wikipedia page on SQL joins](https://en.wikipedia.org/wiki/Join_(SQL)) for more information.

Here are examples of different kinds of join:

```jldoctest joins
julia> jobs = DataFrame(ID=[20, 60], Job=["Lawyer", "Astronaut"])
2×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼──────────────────
   1 │    20  Lawyer
   2 │    60  Astronaut

julia> innerjoin(people, jobs, on = :ID)
1×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String
─────┼─────────────────────────
   1 │    20  John Doe  Lawyer

julia> leftjoin(people, jobs, on = :ID)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String?
─────┼──────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    40  Jane Doe  missing

julia> rightjoin(people, jobs, on = :ID)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String?   String
─────┼────────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    60  missing   Astronaut

julia> outerjoin(people, jobs, on = :ID)
3×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String?   String?
─────┼────────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    40  Jane Doe  missing
   3 │    60  missing   Astronaut

julia> semijoin(people, jobs, on = :ID)
1×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │    20  John Doe

julia> antijoin(people, jobs, on = :ID)
1×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │    40  Jane Doe
```

Cross joins are the only kind of join that does not use a `on` key:

```jldoctest joins
julia> crossjoin(people, jobs, makeunique = true)
4×4 DataFrame
 Row │ ID     Name      ID_1   Job
     │ Int64  String    Int64  String
─────┼───────────────────────────────────
   1 │    20  John Doe     20  Lawyer
   2 │    20  John Doe     60  Astronaut
   3 │    40  Jane Doe     20  Lawyer
   4 │    40  Jane Doe     60  Astronaut
```

## Key value comparisons and floating point values

Key values from the two or more data frames are compared using the `isequal`
function. This is consistent with the `Set` and `Dict` types in Julia Base.

It is not recommended to use floating point numbers as keys: floating point
comparisons can be surprising and unpredictable. If you do use floating point
keys, note that by default an error is raised when keys include `-0.0`
(negative zero) or `NaN` values.
Here is an example:

```jldoctest joins
julia> innerjoin(DataFrame(id=[-0.0]), DataFrame(id=[0.0]), on=:id)
ERROR: ArgumentError: Currently for numeric values `NaN` and `-0.0` in their real or imaginary components are not allowed. Such value was found in column :id in left data frame. Use CategoricalArrays.jl to wrap these values in a CategoricalVector to perform the requested join.
```

This can be overridden by wrapping the key
values in a [categorical](@ref man-categorical) vector.

## Joining on key columns with different names

In order to join data frames on keys which have different names in the left and
right tables, you may pass `left => right` pairs as `on` argument:

```jldoctest joins
julia> a = DataFrame(ID=[20, 40], Name=["John Doe", "Jane Doe"])
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼─────────────────
   1 │    20  John Doe
   2 │    40  Jane Doe

julia> b = DataFrame(IDNew=[20, 40], Job=["Lawyer", "Doctor"])
2×2 DataFrame
 Row │ IDNew  Job
     │ Int64  String
─────┼───────────────
   1 │    20  Lawyer
   2 │    40  Doctor

julia> innerjoin(a, b, on = :ID => :IDNew)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String
─────┼─────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    40  Jane Doe  Doctor
```

Here is another example with multiple columns:

```jldoctest joins
julia> a = DataFrame(City=["Amsterdam", "London", "London", "New York", "New York"],
                     Job=["Lawyer", "Lawyer", "Lawyer", "Doctor", "Doctor"],
                     Category=[1, 2, 3, 4, 5])
5×3 DataFrame
 Row │ City       Job     Category
     │ String     String  Int64
─────┼─────────────────────────────
   1 │ Amsterdam  Lawyer         1
   2 │ London     Lawyer         2
   3 │ London     Lawyer         3
   4 │ New York   Doctor         4
   5 │ New York   Doctor         5

julia> b = DataFrame(Location=["Amsterdam", "London", "London", "New York", "New York"],
                     Work=["Lawyer", "Lawyer", "Lawyer", "Doctor", "Doctor"],
                     Name=["a", "b", "c", "d", "e"])
5×3 DataFrame
 Row │ Location   Work    Name
     │ String     String  String
─────┼───────────────────────────
   1 │ Amsterdam  Lawyer  a
   2 │ London     Lawyer  b
   3 │ London     Lawyer  c
   4 │ New York   Doctor  d
   5 │ New York   Doctor  e

julia> innerjoin(a, b, on = [:City => :Location, :Job => :Work])
9×4 DataFrame
 Row │ City       Job     Category  Name
     │ String     String  Int64     String
─────┼─────────────────────────────────────
   1 │ Amsterdam  Lawyer         1  a
   2 │ London     Lawyer         2  b
   3 │ London     Lawyer         3  b
   4 │ London     Lawyer         2  c
   5 │ London     Lawyer         3  c
   6 │ New York   Doctor         4  d
   7 │ New York   Doctor         5  d
   8 │ New York   Doctor         4  e
   9 │ New York   Doctor         5  e
```

## Handling of duplicate keys and tracking source data frame

Additionally, notice that in the last join rows 2 and 3 had the same values on
`on` variables in both joined `DataFrame`s. In such a situation `innerjoin`,
`outerjoin`, `leftjoin` and `rightjoin` will produce all combinations of
matching rows. In our example rows from 2 to 5 were created as a result. The
same behavior can be observed for rows 4 and 5 in both joined `DataFrame`s.

In order to check that columns passed as the `on` argument define unique keys
(according to `isequal`) in each input data frame you can set the `validate`
keyword argument to a two-element tuple or a pair of `Bool` values, with each
element indicating whether to run check for the corresponding data frame. Here
is an example for the join operation described above:

```jldoctest joins
julia> innerjoin(a, b, on = [(:City => :Location), (:Job => :Work)], validate=(true, true))
ERROR: ArgumentError: Merge key(s) are not unique in both df1 and df2. df1 contains 2 duplicate keys: (City = "London", Job = "Lawyer") and (City = "New York", Job = "Doctor"). df2 contains 2 duplicate keys: (Location = "London", Work = "Lawyer") and (Location = "New York", Work = "Doctor").
```

Finally, using the `source` keyword argument you can add a column to the
resulting data frame indicating whether the given row appeared only in the left,
the right or both data frames. Here is an example:

```jldoctest joins
julia> a = DataFrame(ID=[20, 40], Name=["John", "Jane"])
2×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼───────────────
   1 │    20  John
   2 │    40  Jane

julia> b = DataFrame(ID=[20, 60], Job=["Lawyer", "Doctor"])
2×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │    20  Lawyer
   2 │    60  Doctor

julia> outerjoin(a, b, on=:ID, validate=(true, true), source=:source)
3×4 DataFrame
 Row │ ID     Name     Job      source
     │ Int64  String?  String?  String
─────┼─────────────────────────────────────
   1 │    20  John     Lawyer   both
   2 │    40  Jane     missing  left_only
   3 │    60  missing  Doctor   right_only
```

Note that this time we also used the `validate` keyword argument and it did not
produce errors as the keys defined in both source data frames were unique.

## Renaming joined columns

Often you want to keep track of the source data frame.
This feature is supported with the `renamecols` keyword argument:

```jldoctest joins
julia> innerjoin(a, b, on=:ID, renamecols = "_left" => "_right")
1×3 DataFrame
 Row │ ID     Name_left  Job_right
     │ Int64  String     String
─────┼─────────────────────────────
   1 │    20  John       Lawyer
```

In the above example we added the `"_left"` suffix to the non-key columns from
the left table and the `"_right"` suffix to the non-key columns from the right
table.

Alternatively it is allowed to pass a function transforming column names:
```jldoctest joins
julia> innerjoin(a, b, on=:ID, renamecols = lowercase => uppercase)
1×3 DataFrame
 Row │ ID     name    JOB
     │ Int64  String  String
─────┼───────────────────────
   1 │    20  John    Lawyer

```

## Matching missing values in joins

By default when you try to to perform a join on a key that has `missing` values
you get an error:

```jldoctest joins
julia> df1 = DataFrame(id=[1, missing, 3], a=1:3)
3×2 DataFrame
 Row │ id       a
     │ Int64?   Int64
─────┼────────────────
   1 │       1      1
   2 │ missing      2
   3 │       3      3

julia> df2 = DataFrame(id=[1, 2, missing], b=1:3)
3×2 DataFrame
 Row │ id       b
     │ Int64?   Int64
─────┼────────────────
   1 │       1      1
   2 │       2      2
   3 │ missing      3

julia> innerjoin(df1, df2, on=:id)
ERROR: ArgumentError: Missing values in key columns are not allowed when matchmissing == :error. `missing` found in column :id in left data frame.
```

If you would prefer `missing` values to be treated as equal pass
the `matchmissing=:equal` keyword argument:

```jldoctest joins
julia> innerjoin(df1, df2, on=:id, matchmissing=:equal)
2×3 DataFrame
 Row │ id       a      b
     │ Int64?   Int64  Int64
─────┼───────────────────────
   1 │       1      1      1
   2 │ missing      2      3
```

Alternatively you might want to drop all rows with `missing` values. In this
case pass `matchmissing=:notequal`:

```jldoctest joins
julia> innerjoin(df1, df2, on=:id, matchmissing=:notequal)
1×3 DataFrame
 Row │ id      a      b
     │ Int64?  Int64  Int64
─────┼──────────────────────
   1 │      1      1      1
```

## Specifying row order in the join result

By default the order of rows produced by the join operation is undefined:

```jldoctest joins
julia> df_left = DataFrame(id=[1, 2, 4, 5], left=1:4)
4×2 DataFrame
 Row │ id     left
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     4      3
   4 │     5      4

julia> df_right = DataFrame(id=[2, 1, 3, 6, 7], right=1:5)
5×2 DataFrame
 Row │ id     right
     │ Int64  Int64
─────┼──────────────
   1 │     2      1
   2 │     1      2
   3 │     3      3
   4 │     6      4
   5 │     7      5

julia> outerjoin(df_left, df_right, on=:id)
7×3 DataFrame
 Row │ id     left     right
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     2        2        1
   2 │     1        1        2
   3 │     4        3  missing
   4 │     5        4  missing
   5 │     3  missing        3
   6 │     6  missing        4
   7 │     7  missing        5
```

If you would like the result to keep the row order of the left table pass
the `order=:left` keyword argument:

```jldoctest joins
julia> outerjoin(df_left, df_right, on=:id, order=:left)
7×3 DataFrame
 Row │ id     left     right
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     1        1        2
   2 │     2        2        1
   3 │     4        3  missing
   4 │     5        4  missing
   5 │     3  missing        3
   6 │     6  missing        4
   7 │     7  missing        5
```

Note that in this case keys missing from the left table are put after the keys
present in it.

Similarly `order=:right` keeps the order of the right table (and puts keys
not present in it at the end):

```jldoctest joins
julia> outerjoin(df_left, df_right, on=:id, order=:right)
7×3 DataFrame
 Row │ id     left     right
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     2        2        1
   2 │     1        1        2
   3 │     3  missing        3
   4 │     6  missing        4
   5 │     7  missing        5
   6 │     4        3  missing
   7 │     5        4  missing
```

## In-place left join

A common operation is adding data from a reference table to some main table.
It is possible to perform such an in-place update using the `leftjoin!`
function. In this case the left table is updated in place with matching rows from
the right table.

```jldoctest joins
julia> main = DataFrame(id=1:4, main=1:4)
4×2 DataFrame
 Row │ id     main
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4

julia> leftjoin!(main, DataFrame(id=[2, 4], info=["a", "b"]), on=:id);

julia> main
4×3 DataFrame
 Row │ id     main   info
     │ Int64  Int64  String?
─────┼───────────────────────
   1 │     1      1  missing
   2 │     2      2  a
   3 │     3      3  missing
   4 │     4      4  b
```

Note that in this case the order and number of rows in the left table is not
changed. Therefore, in particular, it is not allowed to have duplicate keys
in the right table:

```
julia> leftjoin!(main, DataFrame(id=[2, 2], info_bad=["a", "b"]), on=:id)
ERROR: ArgumentError: duplicate rows found in right table
```
