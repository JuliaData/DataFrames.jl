# Database-Style Joins

We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:

```julia
names = DataFrame(ID = [20, 40], Name = ["John Doe", "Jane Doe"])
jobs = DataFrame(ID = [20, 40], Job = ["Lawyer", "Doctor"])
```

We might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the `join` function:

```julia
full = join(names, jobs, on = :ID)
```

Output:

| Row | ID | Name       | Job      | 
|-----|----|------------|----------|
| 1   | 20 | "John Doe" | "Lawyer" |
| 2   | 40 | "Jane Doe" | "Doctor" |

In relational database theory, this operation is generally referred to as a join. The columns used to determine which rows should be combined during a join are called keys.

There are seven kinds of joins supported by the DataFrames package:

-   Inner: The output contains rows for values of the key that exist in both the first (left) and second (right) arguments to `join`.
-   Left: The output contains rows for values of the key that exist in the first (left) argument to `join`, whether or not that value exists in the second (right) argument.
-   Right: The output contains rows for values of the key that exist in the second (right) argument to `join`, whether or not that value exists in the first (left) argument.
-   Outer: The output contains rows for values of the key that exist in the first (left) or second (right) argument to `join`.
-   Semi: Like an inner join, but output is restricted to columns from the first (left) argument to `join`.
-   Anti: The output contains rows for values of the key that exist in the first (left) but not the second (right) argument to `join`. As with semi joins, output is restricted to columns from the first (left) argument.
-   Cross: The output is the cartesian product of rows from the first (left) and second (right) arguments to `join`.

You can control the kind of join that `join` performs using the `kind` keyword argument:

```julia
a = DataFrame(ID = [20, 40], Name = ["John Doe", "Jane Doe"])
b = DataFrame(ID = [20, 60], Job = ["Lawyer", "Astronaut"])
join(a, b, on = :ID, kind = :inner)
join(a, b, on = :ID, kind = :left)
join(a, b, on = :ID, kind = :right)
join(a, b, on = :ID, kind = :outer)
join(a, b, on = :ID, kind = :semi)
join(a, b, on = :ID, kind = :anti)
```

Cross joins are the only kind of join that does not use a key:

```julia
join(a, b, kind = :cross)
```
