# Importing and Exporting Data (I/O)

For reading and writing tabular data from CSV and other delimited text files,
use the [CSV.jl](https://github.com/JuliaData/CSV.jl) package.

If you have not used the CSV.jl package before then you may need to install it first:
```jldoctest
using Pkg
Pkg.add("CSV")
```

The CSV.jl functions are not loaded automatically and must be imported into the session.
```jldoctest
using CSV
```

A dataset can now be read from a CSV file at path `input` using
```jldoctest
DataFrame(CSV.File(input))
```

A `DataFrame` can be written to a CSV file at path `output` using
```jldcotest
df = DataFrame(x = 1, y = 2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more
information, see `?CSV.File`, `?CSV.read` and `?CSV.write`, or checkout the
online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).

For reading and writing tabular data in Apache Arrow format use
[Arrow.jl](https://github.com/JuliaData/Arrow.jl)
