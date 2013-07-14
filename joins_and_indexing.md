---

layout: minimal
title: Joins and Indexing

---

# Database-Style Joins and Indexing

## Joining Data Sets Together

We often need to combine two or more data sets together to provide a complete picture of the topic we are studying. For example, suppose that we have the following two data sets:

    names = DataFrame(ID = [1, 2], Name = ["John Doe", "Jane Doe"])
    jobs = DataFrame(ID = [1, 2], Job = ["Lawyer", "Doctor"])

We might want to work with a larger data set that contains both the names and jobs for each ID. We can do this using the `join` function:

    full = join(names, jobs, on = "ID")

In relational database theory, this operation is generally referred to as a join. The column used to determine which rows should be combined during a join is called the key. If you do not specify the key when calling `join`, the `join` function will attempt to identity a commonly named column in the DataFrame arguments for use as a key:

    full = join(names, jobs)

There are four sorts of joins supported by the DataFrames package:

* Inner: The output contains rows for values of the key that exist in both the first (left) and second (right) arguments to `join`.
* Left: The output contains rows for values of the key that exist in the first (left) argument to `join`, whether or not that value exists in the second (right) argument.
* Right: The output contains rows for values of the key that exist in the second (right) argument to `join`, whether or not that value exists in the first (left) argument.
* Outer: The output contains rows for values of the key that exist in the first (left) or second (right) argument to `join`.

You can control the kind of join that `join` performs using the `kind` keyword argument:

    a = DataFrame(ID = [1, 2], Name = ["A", "B"])
    b = DataFrame(ID = [1, 3], Job = ["Doctor", "Lawyer"])
    join(a, b, on = "ID", kind = :inner)
    join(a, b, on = "ID", kind = :left)
    join(a, b, on = "ID", kind = :right)
    join(a, b, on = "ID", kind = :outer)

## Indexing: Making Joins and Subsetting Faster

One problem with merging large data sets is that determining which subset of rows from the left argument should be combined with which subset of rows from the right argument takes a long time. Selecting subsets in this way is slow for most DataFrames because the entries of each column have to be exhaustively examined.

It is possible to make subset selection much faster if we allow the `DataFrame` to store indexing information that tells the system where to expect certain subsets to be located inside of the `DataFrame`. If you are familar with database systems, this indexing involves either explicit index metadata that is added to a database or an implicit index defined by a "primary key" for the database.

We are currently developing a method for indexing DataFrames, but the prototype is not yet ready for widespread use.
