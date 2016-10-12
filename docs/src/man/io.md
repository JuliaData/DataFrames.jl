# Importing and Exporting (I/O)

## Importing data from tabular data files

To read data from a CSV-like file, use the `readtable` function:

```@docs
readtable
```

`readtable` requires that you specify the path of the file that you would like to read as a `String`. 
It supports many additional keyword arguments: these are documented in the section on advanced I/O operations.

## Importing data from tabular string

To read data from a non-file source, you may also supply an `IO` object.

```julia
dat = "Date,Stock,Open,High,Low,Close,Volume
2016-09-29,KESM,7.92,7.98,7.92,7.97,149400
2016-09-30,KESM,7.96,7.97,7.84,7.9,29900
2016-10-04,KESM,7.8,7.94,7.8,7.93,99900
2016-10-05,KESM,7.93,7.95,7.89,7.93,77500
2016-10-06,KESM,7.93,7.93,7.89,7.92,130600
2016-10-07,KESM,7.91,7.94,7.91,7.92,103000"
io = IOBuffer(dat)
df = readtable(io)
```

## Exporting data to a tabular data file

To write data to a CSV file, use the `writetable` function:

```@docs
writetable
```

## Supplying `DataFrame`s inline with non-standard string literals

You can also provide CSV-like tabular data in a non-standard string literal to construct a new `DataFrame`, as in the following:

```julia
df = csv"""
    name,  age, squidPerWeek
    Alice,  36,         3.14
    Bob,    24,         0
    Carol,  58,         2.71
    Eve,    49,         7.77
    """
```

The `csv` string literal prefix indicates that the data are supplied in standard comma-separated value format. Common alternative formats are also available as string literals. For semicolon-separated values, with comma as a decimal, use `csv2`:

```julia
df = csv2"""
    name;  age; squidPerWeek
    Alice;  36;         3,14
    Bob;    24;         0
    Carol;  58;         2,71
    Eve;    49;         7,77
    """
```

For whitespace-separated values, use `wsv`:

```julia
df = wsv"""
    name  age squidPerWeek
    Alice  36         3.14
    Bob    24         0
    Carol  58         2.71
    Eve    49         7.77
    """
```

And for tab-separated values, use `tsv`:

```julia
df = tsv"""
    name	age	squidPerWeek
    Alice	36	3.14
    Bob	24	0
    Carol	58	2.71
    Eve	49	7.77
    """
```
