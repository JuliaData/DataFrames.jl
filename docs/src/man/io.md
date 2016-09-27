# Importing and Exporting (I/O)

## Importing data from tabular data files

To read data from a CSV-like file, use the `readtable` function:

```@docs
readtable
```

`readtable` requires that you specify the path of the file that you would like to read as a `String`. To read data from a non-file source, you may also supply an `IO` object. It supports many additional keyword arguments: these are documented in the section on advanced I/O operations.

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
