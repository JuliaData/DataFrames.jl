# Importing and Exporting Data (I/O)

## CSV Files

For reading and writing tabular data from CSV and other delimited text files,
use the [CSV.jl](https://github.com/JuliaData/CSV.jl) package.

If you have not used the CSV.jl package before then you may need to install it first:
```julia
using Pkg
Pkg.add("CSV")
```

The CSV.jl functions are not loaded automatically and must be imported into the session.
```julia
using CSV
```

A dataset can now be read from a CSV file at path `input` using
```julia
DataFrame(CSV.File(input))
```

A `DataFrame` can be written to a CSV file at path `output` using
```julia
df = DataFrame(x=1, y=2)
CSV.write(output, df)
```

The behavior of CSV functions can be adapted via keyword arguments. For more
information, see `?CSV.File`, `?CSV.read` and `?CSV.write`, or checkout the
online [CSV.jl documentation](https://juliadata.github.io/CSV.jl/stable/).

In simple cases, when compilation latency of CSV.jl might be an issue,
using the `DelimitedFiles` module from the Julia standard library can be considered.
Here is an example showing how to read in the data and perform its
post-processing:

```jldoctest readdlm
julia> using DelimitedFiles, DataFrames

julia> path = joinpath(pkgdir(DataFrames), "docs", "src", "assets", "iris.csv");

julia> data, header = readdlm(path, ',', header=true);

julia> iris_raw = DataFrame(data, vec(header))
150×5 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Any          Any         Any          Any         Any
─────┼──────────────────────────────────────────────────────────────────
   1 │ 5.1          3.5         1.4          0.2         Iris-setosa
   2 │ 4.9          3.0         1.4          0.2         Iris-setosa
   3 │ 4.7          3.2         1.3          0.2         Iris-setosa
   4 │ 4.6          3.1         1.5          0.2         Iris-setosa
   5 │ 5.0          3.6         1.4          0.2         Iris-setosa
   6 │ 5.4          3.9         1.7          0.4         Iris-setosa
   7 │ 4.6          3.4         1.4          0.3         Iris-setosa
   8 │ 5.0          3.4         1.5          0.2         Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
 144 │ 6.8          3.2         5.9          2.3         Iris-virginica
 145 │ 6.7          3.3         5.7          2.5         Iris-virginica
 146 │ 6.7          3.0         5.2          2.3         Iris-virginica
 147 │ 6.3          2.5         5.0          1.9         Iris-virginica
 148 │ 6.5          3.0         5.2          2.0         Iris-virginica
 149 │ 6.2          3.4         5.4          2.3         Iris-virginica
 150 │ 5.9          3.0         5.1          1.8         Iris-virginica
                                                        135 rows omitted

julia> iris = identity.(iris_raw)
150×5 DataFrame
 Row │ SepalLength  SepalWidth  PetalLength  PetalWidth  Species
     │ Float64      Float64     Float64      Float64     SubStrin…
─────┼──────────────────────────────────────────────────────────────────
   1 │         5.1         3.5          1.4         0.2  Iris-setosa
   2 │         4.9         3.0          1.4         0.2  Iris-setosa
   3 │         4.7         3.2          1.3         0.2  Iris-setosa
   4 │         4.6         3.1          1.5         0.2  Iris-setosa
   5 │         5.0         3.6          1.4         0.2  Iris-setosa
   6 │         5.4         3.9          1.7         0.4  Iris-setosa
   7 │         4.6         3.4          1.4         0.3  Iris-setosa
   8 │         5.0         3.4          1.5         0.2  Iris-setosa
  ⋮  │      ⋮           ⋮            ⋮           ⋮             ⋮
 144 │         6.8         3.2          5.9         2.3  Iris-virginica
 145 │         6.7         3.3          5.7         2.5  Iris-virginica
 146 │         6.7         3.0          5.2         2.3  Iris-virginica
 147 │         6.3         2.5          5.0         1.9  Iris-virginica
 148 │         6.5         3.0          5.2         2.0  Iris-virginica
 149 │         6.2         3.4          5.4         2.3  Iris-virginica
 150 │         5.9         3.0          5.1         1.8  Iris-virginica
                                                        135 rows omitted
```

Observe that in our example:
* `header` is a `Matrix` therefore we had to pass `vec(header)` to the `DataFrame`
  constructor;
* we broadcasted the `identity` function over the `iris_raw` data frame to perform
  narrowing of `eltype` of columns of `iris_raw`; the reason is that read in by
  the `readdlm` function is stored into a `data` `Matrix` so all columns in
  `iris_raw` initially have the same `eltype` -- in this case it had to be `Any`
  as some of the columns are numeric and some are string.

All such operations (and many more) are automatically handled by CSV.jl.

Similarly, you can use the `writedlm` function from the `DelimitedFiles` module to
save a data frame like this:

```julia
writedlm("test.csv", Iterators.flatten(([names(iris)], eachrow(iris))), ',')
```

As you can see the code required to transform `iris` into a proper input to the
`writedlm` function so that you can create the CSV file having the expected
format is not easy. Therefore CSV.jl is the preferred package to write CSV files
for data stored in data frames.

### `DataFrame` Definition in Source Code
Sometimes, you might want to define a `DataFrame` with data directly in the source
code. For better readability you probably want to define it in a way which resembles
a table. This can be done with `CSV.jl` like this:

```julia
df = CSV.read("""
    name,age,children
    John,54.0,0
    Sally,34.0,2
    Roger,79.0,4
    """ |> IOBuffer, DataFrame)
```

This will result in the following `DataFrame`:

```
3×3 DataFrame
 Row │ name     age      children 
     │ String7  Float64  Int64    
─────┼────────────────────────────
   1 │ John        54.0         0
   2 │ Sally       34.0         2
   3 │ Roger       79.0         4
```

As the readability in the source code of the above format is not great, you can make
use of [`CSV.jl`'s options](https://csv.juliadata.org/stable/examples.html#csv_string)
to find a format which works for you, e.g. using
[Unicode box drawing characters](https://en.wikipedia.org/wiki/Box-drawing_characters#Unicode)

```julia
data = """
┌───────┬──────┬──────────┐
│ name  │ age  │ children │
├───────┼──────┼──────────┤
│ John  │ 54.0 │    0     │
│ Sally │ 34.0 │    2     │
│ Roger │ 79.0 │    4     │
└───────┴──────┴──────────┘
"""
```

You can then create the above `DataFrame` object with

```julia
df = CSV.read(data |> IOBuffer, DataFrame, normalizenames=true,
              header=2, skipto=4, footerskip=1, delim='│',
              drop=(i, name) -> startswith(name |> String, "Column")
     ) .|> x -> x isa AbstractString ? strip(x) : x
```

## Other formats

Other data formats are supported for reading and writing in the following packages
(non exhaustive list):
* Apache Arrow (including Feather v2): [Arrow.jl](https://github.com/JuliaData/Arrow.jl)
* Apache Feather (v1): [Feather.jl](https://github.com/JuliaData/Feather.jl)
* Apache Avro: [Avro.jl](https://github.com/JuliaData/Avro.jl)
* JSON: [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl)
* Parquet: [Parquet2.jl](https://gitlab.com/ExpandingMan/Parquet2.jl)
* Stata, SAS and SPSS: [ReadStatTables.jl](https://github.com/junyuan-chen/ReadStatTables.jl)
  (alternatively [Queryverse](https://www.queryverse.org/)
   users can choose [StatFiles.jl](https://github.com/queryverse/StatFiles.jl))
* reading R data files (.rda, .RData): [RData.jl](https://github.com/JuliaData/RData.jl)
* Microsoft Excel (XLSX): [XLSX.jl](https://github.com/felipenoris/XLSX.jl)
* Copying/pasting to clipboard, for sending data to and from spreadsheets: [ClipData.jl](https://github.com/pdeffebach/ClipData.jl)
