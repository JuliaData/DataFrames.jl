# Database-Style Joins

We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:

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

We might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the `innerjoin` function:

```jldoctest joins
julia> innerjoin(people, jobs, on = :ID)
2×3 DataFrame
 Row │ ID     Name      Job
     │ Int64  String    String
─────┼─────────────────────────
   1 │    20  John Doe  Lawyer
   2 │    40  Jane Doe  Doctor
```

In relational database theory, this operation is generally referred to as a join.
The columns used to determine which rows should be combined during a join are called keys.

The following functions are provided to perform seven kinds of joins:

-   `innerjoin`: the output contains rows for values of the key that exist in all passed data frames.
-   `leftjoin`: the output contains rows for values of the key that exist in the first (left) argument,
    whether or not that value exists in the second (right) argument.
-   `rightjoin`: the output contains rows for values of the key that exist in the second (right) argument,
    whether or not that value exists in the first (left) argument.
-   `outerjoin`: the output contains rows for values of the key that exist in any of the passed data frames.
-   `semijoin`: Like an inner join, but output is restricted to columns from the first (left) argument.
-   `antijoin`: The output contains rows for values of the key that exist in the first (left) but not the second (right) argument.
    As with `semijoin`, output is restricted to columns from the first (left) argument.
-   `crossjoin`: The output is the cartesian product of rows from all passed data frames.

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

In order to join data frames on keys which have different names in the left and right tables,
you may pass `left => right` pairs as `on` argument:

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
