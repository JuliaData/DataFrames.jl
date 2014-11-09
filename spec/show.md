# Printing DataFrames

AbstractDataFrames are rendered to the REPL window using a best-effort strategy that attempts to ensure that the output is always readable within the horizontal and vertical boundaries of the REPL window.

Admittedly, this does not always produce a visually appealing output. In many ways, the static window offered by a standard 80 character by 24 character REPL is a bad medium in which to render DataFrames: there is not enough vertical space to show all rows and there is also not enough horizontal space to show all columns.

Unlike Arrays, whose homogeneity of type makes horizontal truncation a reasonable default, a lot of important information about the schema for an AbstractDataFrame is lost if a large number of the AbstractDataFrame's columns are not displayed in the REPL.

# Printing Strategy

The DataFrames.jl package therefore employs the following best-effort strategy for rendering an AbstractDataFrame to the REPL window.

* Determine how much horizontal width would be required to render every column of the AbstractDataFrame, including the implicit "Row #" column contained in every AbstractDataFrame.
* If the horizontal width fits within the visible REPL window, render the AbstractDataFrame as a table that shows the head and tail in complete detail. Under this default behavior, the number of rows shown is guaranteed to fit in the vertical height of the REPL window.
* If the horizontal width required for complete output exceeds the amount of space available in the REPL window, we do not attempt to print out all of the columns. Instead, we print out a summary of the DataFrame's schema in three parts: (1) the name of each column, (2) the type of each column and (3) the number of missing entries in each column. This summary is **not** guaranteed to fit in the vertical height of the REPL window.
* If the full set of columns of the AbstractDataFrame would not fit in the horizontal width of the REPL window, the user may request that the text representation of the AbstractDataFrame will be paginated into chunks that exhausitively display every column. Each of these chunks is guaranteed to fit within the horizontal width of the REPL.
* If the full set of rows of the AbstractDataFrame would not fit in the vertical height of the REPL window, the user may request that all rows be shown in the REPL using the `showall` function. The output of this is **not** guaranteed to fit in the horizontal width of the REPL window. As with the shortened summary described earlier, the user may additionally request that this output be paginated into chunks that exhausitively display every column. Each of these chunks is guaranteed to fit within the horizontal width of the REPL.

# Functions

For the end-user, this display strategy leads to four possible function calls:

* `show(adf)`: If the columns fit in the window, show the head and tail of the DataFrame with all columns. If the columns do not fit in the window, show a summary of the table in terms of its schema instead. *This function is the default used by the REPL.*
* `show(adf, true)`: Show the head and tail of the DataFrame with all columns, no matter what. If necessary, the output will be paginated so that each chunk of output fits within the horizontal width of the window.
* `showall(adf)`: Show all of the DataFrame's contents, including all rows and columns. The size of the REPL window is ignored.
* `showall(adf, true)`: Show all of the DataFrame's rows, but paginate the output so that each chunk fits in the horizontal width of the REPL window.

In addition to all the properties described above, the output is always formatted as a valid MultiMarkdown table that can be used anywhere that supports complex Markdown. This makes it especially easy to use DataFrames to organize data for reporting on GitHub.

# Usage Examples of Expected Output

```julia
julia> using DataFrames

julia> df = DataFrame(A = [repeat("a", 40) for i in 1:24],
                      B = [repeat("b", 40) for i in 1:24])
# 2x3 DataFrame
# | Col # | Name | Type        | Missing |
# |-------|------|-------------|---------|
# | 1     | A    | ASCIIString | 0       |
# | 2     | B    | ASCIIString | 0       |

julia> show(df)
# 2x3 DataFrame
# | Col # | Name | Type        | Missing |
# |-------|------|-------------|---------|
# | 1     | A    | ASCIIString | 0       |
# | 2     | B    | ASCIIString | 0       |

julia> show(df, true)
# 24x2 DataFrame
# | Row # | A                                        |
# |-------|------------------------------------------|
# | 1     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 2     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 3     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 4     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 5     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 6     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 7     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 8     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# ⋮
# | 16    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 17    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 18    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 19    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 20    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 21    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 22    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 23    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 24    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
#
# | Row # | B                                        |
# |-------|------------------------------------------|
# | 1     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 2     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 3     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 4     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 5     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 6     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 7     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 8     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# ⋮
# | 16    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 17    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 18    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 19    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 20    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 21    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 22    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 23    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 24    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |

julia> showall(df)
# 24x2 DataFrame
# | Row # | A                                        | B                                        |
# |-------|------------------------------------------|------------------------------------------|
# | 1     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 2     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 3     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 4     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 5     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 6     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 7     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 8     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 9     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 10    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 11    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 12    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 13    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 14    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 15    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 16    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 17    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 18    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 19    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 20    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 21    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 22    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 23    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 24    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |

julia> showall(df, true)
# 24x2 DataFrame
# | Row # | A                                        |
# |-------|------------------------------------------|
# | 1     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 2     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 3     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 4     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 5     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 6     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 7     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 8     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 9     | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 10    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 11    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 12    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 13    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 14    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 15    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 16    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 17    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 18    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 19    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 20    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 21    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 22    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 23    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
# | 24    | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
#
# | Row # | B                                        |
# |-------|------------------------------------------|
# | 1     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 2     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 3     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 4     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 5     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 6     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 7     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 8     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 9     | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 10    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 11    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 12    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 13    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 14    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 15    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 16    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 17    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 18    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 19    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 20    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 21    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 22    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 23    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
# | 24    | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
```
