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
    df[:A]

    df[1, 1]
    df[1, :A]

    df[1:3, [:A, :B]]
    df[1:3, [:B, :A]]

    df[df[:A] % 2 .== 0, :]
    df[df[:B] % 2 .== 0, :]
