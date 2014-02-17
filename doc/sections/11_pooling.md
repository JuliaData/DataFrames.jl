# Representing Factors using the PooledDataArray Type

Often, we have to deal with factors that take on a small
number of levels:

    dv = @data(["Group A", "Group A", "Group A",
                "Group B", "Group B", "Group B"])

The naive encoding used in a `DataArray` represents every
entry of this vector as a full string. In contrast, we
can represent the data more efficiently by replacing the
strings with indices into a small pool of levels. This is
what the `PooledDataArray` does:

    pdv = @pdata(["Group A", "Group A", "Group A",
                  "Group B", "Group B", "Group B"])

In addition to representing repeated data efficiently,
the `PooledDataArray` allows us to determine the levels
of the factor at any time using the `levels` function:

    levels(pdv)

By default, a `PooledDataArray` is able to represent
`2^32` differents levels. You can use less memory by
calling the `compact` function:

    pdv = compact(pdv)

Often, you will have factors encoded inside a DataFrame
with `DataArray` columns instead of `PooledDataArray`
columns. You can do conversion of a single column using
the `pool` function:

    pdv = pool(dv)

Or you can edit the columns of a `DataFrame` in-place
using the `pool!` function:

    df = DataFrame(A = [1, 1, 1, 2, 2, 2],
                   B = ["X", "X", "X", "Y", "Y", "Y"])
    pool!(df, [:A, :B])

Pooling columns is important for working with the
[GLM package](https://github.com/JuliaStats/GLM.jl).
When fitting regression models, `PooledDataArray` columns
in the input are translated into 0/1 indicator columns
in the `ModelMatrix` -- with one column for each of the levels
of the `PooledDataArray`. This allows one to analyze categorical
data efficiently.
