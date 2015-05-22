# Importing and Exporting (I/O)

Currently, `DataFrames` provides functions to read tabular (e.g. CSV) data, and R data files (in RDA2 or RDX2 format).

In addition, a number of additional packages allow to obtain a `DataFrame` from external data, for instance databases, or Statistics software. Below, we describe interfaces for

* PostgreSQL
* MySQL
* Stata
* SPSS
* SAS

## CSV files

### Importing

#### Basic usage

To read data from a CSV-like file, use the `readtable` function:

```julia
df = readtable("data.csv")
df = readtable("data.tsv")
df = readtable("data.wsv")
df = readtable("data.txt", separator = '\t')
df = readtable("data.txt", header = false)
```

`readtable` requires that you specify the path of the file that you would like to read as a `String`.

#### Advanced Options

`readtable` accepts the following optional keyword arguments:

-   `header::Bool` -- Use the information from the file's header line to determine column names. Defaults to `true`.
-   `separator::Char` -- Assume that fields are split by the `separator` character. If not specified, it will be guessed from the filename: `.csv` defaults to `','`, `.tsv` defaults to `'\t'`, `.wsv` defaults to `' '`.
-   `quotemark::Vector{Char}` -- Assume that fields contained inside of two `quotemark` characters are quoted, which disables processing of separators and linebreaks. Set to `Char[]` to disable this feature and slightly improve performance. Defaults to `['"']`.
-   `decimal::Char` -- Assume that the decimal place in numbers is written using the `decimal` character. Defaults to `'.'`.
-   `nastrings::Vector{ASCIIString}` -- Translate any of the strings into this vector into an `NA`. Defaults to `["", "NA"]`.
-   `truestrings::Vector{ASCIIString}` -- Translate any of the strings into this vector into a Boolean `true`. Defaults to `["T", "t", "TRUE", "true"]`.
-   `falsestrings::Vector{ASCIIString}` -- Translate any of the strings into this vector into a Boolean `true`. Defaults to `["F", "f", "FALSE", "false"]`.
-   `makefactors::Bool` -- Convert string columns into `PooledDataVector`'s for use as factors. Defaults to `false`.
-   `nrows::Int` -- Read only `nrows` from the file. Defaults to `-1`, which indicates that the entire file should be read.
-   `names::Vector{Symbol}` -- Use the values in this array as the names for all columns instead of or in lieu of the names in the file's header. Defaults to `[]`, which indicates that the header should be used if present or that numeric names should be invented if there is no header.
-   `eltypes::Vector{DataType}` -- Specify the types of all columns. Defaults to `[]`.
-   `allowcomments::Bool` -- Ignore all text inside comments. Defaults to `false`.
-   `commentmark::Char` -- Specify the character that starts comments. Defaults to `'#'`.
-   `ignorepadding::Bool` -- Ignore all whitespace on left and right sides of a field. Defaults to `true`.
-   `skipstart::Int` -- Specify the number of initial rows to skip. Defaults to `0`.
-   `skiprows::Vector{Int}` -- Specify the indices of lines in the input to ignore. Defaults to `[]`.
-   `skipblanks::Bool` -- Skip any blank lines in input. Defaults to `true`.
-   `encoding::Symbol` -- Specify the file's encoding as either `:utf8` or `:latin1`. Defaults to `:utf8`.

### Exporting

#### Basic usage

To write data to a CSV file, use the `writetable` function:

```julia
df = DataFrame(A = 1:10)
writetable("output.csv", df)
writetable("output.dat", df, separator = ',', header = false)
writetable("output.dat", df, quotemark = '\'', separator = ',')
writetable("output.dat", df, header = false)
```

`writetable` requires the following arguments:

-   `filename::String` -- The path of the file that you wish to write to.
-   `df::DataFrame` -- The DataFrame you wish to write to disk.

#### Advanced Options

`writetable` accepts the following optional keyword arguments:

-   `separator::Char` -- The separator character that you would like to use. Defaults to the output of `getseparator(filename)`, which uses commas for files that end in `.csv`, tabs for files that end in `.tsv` and a single space for files that end in `.wsv`.
-   `quotemark::Char` -- The character used to delimit string fields. Defaults to `'"'`.
-   `header::Bool` -- Should the file contain a header that specifies the column names from `df`. Defaults to `true`.

## R files

## Using other packages

### Databases

To read a `DataFrame` from a database, you can use the database-independent API provided by [DBI.jl](https://github.com/JuliaDB/DBI.jl). This API can be implemented by specific database drivers, for instance PostgreSQL:

```julia
using DBI
using PostgreSQL

conn = connect(Postgres, "localhost", "username", "password", "dbname", 5432)

stmt = prepare(conn, "SELECT 1::bigint, 2.0::double precision, 'foo'::character varying, " *
                     "'foo'::character(10);")
result = execute(stmt)
df = fetchdf(stmt) # This is a DataFrame
```

Note that, while `fetchdf` allows obtaining a `DataFrame`, `DBI` allows for other return formats.

Currently, the API of `DBI` is implemented for

* [PostgreSQL](https://github.com/iamed2/PostgreSQL.jl)
* [MySQL](https://github.com/johnmyleswhite/MySQL.jl)

### Stata, SPSS, and SAS: [DataRead.jl](https://github.com/WizardMac/DataRead.jl)

This package is a wrapper around a C library that reads these file formats. It requires that `libreadstat.dylib` (obtained by following the link above) is in Julia's load path. Example usage:

```julia
using DataRead
read_dta("/path/to/something.dta") #Stata
read_por("/path/to/something.por") #SPSS
read_sav("/path/to/something.sav") #SPSS
read_sas7bdat("/path/to/something.sas7bdat") #SAS
```
