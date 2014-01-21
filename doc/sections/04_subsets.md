# Accessing and Modifying Entries of DataArray and DataFrame Objects

The `DataArray` type is meant to behave like a standard Julia `Array` and
tries to implement identical indexing rules:

    dv = data([1, 2, 3])
    dv[1]
    dv[2] = NA
    dv[2]

    dm = data([1 2; 3 4])
    dm[1, 1]
    dm[2, 1] = NA
    dm[2, 1]

In contrast, a `DataFrame` offers substantially more forms of indexing
because columns can be referred to by name:

    df = DataFrame(A = 1:10, B = 2:2:20)

    df[1]
    df["A"]

    df[1, 1]
    df[1, "A"]

    df[1:3, ["A", "B"]]
    df[1:3, ["B", "A"]]

    df[df["A"] % 2 .== 0, :]
    df[df["B"] % 2 .== 0, :]

To simplify the last example (in which we examined the properties of column of `df`
to determine which rows to return), you can also index rows using quoted expressions
that will be evaluated using the columns of the `DataFrame`:

    df[:(A % 2 .== 0), :]

Expression indexing makes it easier to build up complex subsets:

    df[:((A % 2 .== 0) & (B % 4 .== 0)), :]

This kind of indexing can also be accomplished using `select`:

    select(:((A % 2 .== 0) & (B % 4 .== 0)), df)
