
# Customizing Display Output

DataFrames.jl uses [PrettyTables.jl](https://github.com/ronisbr/PrettyTables.jl) to render
tables in both plain text (`show(df; ...)`) and HTML
(`show(stdout, MIME("text/html"), df; ...)` in notebook-like environments).

The `show` function exposes DataFrames-specific controls and also forwards `kwargs...` to
PrettyTables.jl, so you can customize formatting, styling, and highlights.

DataFrames-specific keywords accepted by `show` are:

- `allrows::Bool` (text): print all rows instead of only the rows that fit the display
   height.
- `allcols::Bool` (text): print all columns instead of only the columns that fit the display
   width.
- `rowlabel::Symbol` (text): set the label used for the row-number column (default: `:Row`).
- `summary::Bool` (text and HTML): show or hide the summary line above the table (for
   example, `3×3 DataFrame`).
- `eltypes::Bool` (text and HTML): show or hide the column element types under the column
   names.
- `truncate::Int` (text): maximum display width for each data column before truncation with
   `…`; `0` or negative disables truncation.
- `show_row_number::Bool` (text and HTML): show or hide row numbers.
- `max_column_width::AbstractString` (HTML): maximum column width as a CSS length (for
   example, `"120px"`); empty string means no width limit.

The remaining `kwargs...` are forwarded to PrettyTables.jl for backend-specific
customization.

For HTML output, DataFrames.jl reserves `rowid`, `title`, and `truncate` keywords. Use
`max_column_width` (instead of `truncate`) to control cell width in HTML rendering, and
`top_left_str` (instead of `title`) to set the table title in HTML rendering.

```jldoctest customizing_output
julia> using DataFrames

julia> df = DataFrame(
           a = [1, 2, 3],
           b = [3.14, -1.2, 42.0],
           c = ["short", "a very very very very very long string", "ok"]
       );

julia> # This is the default output.

julia> df
3×3 DataFrame
 Row │ a      b        c
     │ Int64  Float64  String
─────┼───────────────────────────────────────────────────
   1 │     1     3.14  short
   2 │     2    -1.2   a very very very very very long …
   3 │     3    42.0   ok

julia> # Using this option, no cell will be truncated if there is room to display it.

julia> show(df; truncate = 0)
3×3 DataFrame
 Row │ a      b        c
     │ Int64  Float64  String
─────┼────────────────────────────────────────────────────────
   1 │     1     3.14  short
   2 │     2    -1.2   a very very very very very long string
   3 │     3    42.0   ok

julia> # Hide row numbers.

julia> show(df; show_row_number = false)
3×3 DataFrame
 a      b        c
 Int64  Float64  String
───────────────────────────────────────────────────
     1     3.14  short
     2    -1.2   a very very very very very long …
     3    42.0   ok

julia> # Hide the column element types in text output.

julia> show(df; eltypes = false)
3×3 DataFrame
 Row │ a  b      c
─────┼─────────────────────────────────────────────
   1 │ 1   3.14  short
   2 │ 2  -1.2   a very very very very very long …
   3 │ 3  42.0   ok
```

!!! note

    The following examples assume that PrettyTables.jl v3.0 or later is installed. If you
    have an older version of PrettyTables.jl, you may need to update it to use the features
    shown in the examples below.

We can use formatters in PrettyTables.jl to change how cells are displayed. The following
example shows how to replace negative values with parentheses in text output. We define a
formatter function:

```julia
function parentheses_fmt(v, i, j)
    !(v isa Number) && return v
    v < 0 && return "($(-v))"
    return v
end
```

This function is called for each cell in the table. `v` is the current cell value, `i` and
`j` are the row and column indices of the cell. It must return the new object which will
be printed in the cell. In this case, we only want to change cells that are negative
numbers, so we return the original value for all other cells. This function must be
encapsulated in a `Vector` and passed to the `formatters` keyword as follows:

```jldoctest customizing_output
julia> using PrettyTables

julia> df = DataFrame(
           A = [ 0.73, -1.28,  1.91, -0.44,  0.12, -2.35,  1.08],
           B = [-0.55,  0.67, -1.49,  2.11, -0.03,  0.94, -2.20],
           C = [ 1.34, -0.88,  0.45, -1.76,  2.53, -0.61,  0.07],
           D = [-1.02,  2.40, -0.31,  0.58, -2.14,  1.77, -0.90],
           E = [ 0.26, -1.67,  2.22, -0.75,  1.05, -0.48, -2.93]
       );

julia> # This is the default output.

julia> df
7×5 DataFrame
 Row │ A        B        C        D        E
     │ Float64  Float64  Float64  Float64  Float64 
─────┼─────────────────────────────────────────────
   1 │    0.73    -0.55     1.34    -1.02     0.26
   2 │   -1.28     0.67    -0.88     2.4     -1.67
   3 │    1.91    -1.49     0.45    -0.31     2.22
   4 │   -0.44     2.11    -1.76     0.58    -0.75
   5 │    0.12    -0.03     2.53    -2.14     1.05
   6 │   -2.35     0.94    -0.61     1.77    -0.48
   7 │    1.08    -2.2      0.07    -0.9     -2.93

julia> function parentheses_fmt(v, i, j)
           !(v isa Number) && return v
           v < 0 && return "($(-v))"
           return v
       end;

julia> show(df; formatters = [parentheses_fmt])
7×5 DataFrame
 Row │ A        B        C        D        E
     │ Float64  Float64  Float64  Float64  Float64
─────┼─────────────────────────────────────────────
   1 │   0.73    (0.55)    1.34    (1.02)    0.26
   2 │  (1.28)    0.67    (0.88)    2.4     (1.67)
   3 │   1.91    (1.49)    0.45    (0.31)    2.22
   4 │  (0.44)    2.11    (1.76)    0.58    (0.75)
   5 │   0.12    (0.03)    2.53    (2.14)    1.05
   6 │  (2.35)    0.94    (0.61)    1.77    (0.48)
   7 │   1.08    (2.2)     0.07    (0.9)    (2.93)
```

The color of the cells can be changed using highlighters. The following example shows how to
highlight negative values in red in HTML output (e.g. in Jupyter).

```julia
# HTML output (e.g. in Jupyter): cap column width and highlight negatives in red.
julia> hl = HtmlHighlighter((data, i, j) -> data[i, j] < 0, ["color" => "red"]);

julia> show(
           stdout,
           MIME("text/html"),
           df;
           highlighters = [hl]
       )
```

You can also add summary rows at the bottom of a table using PrettyTables.jl keywords.  Pass
a vector of functions to the `summary_rows` parameter to compute metrics, and optionally use
`summary_row_labels` to set labels for those rows.

In the following example, a table displays quarterly profits for a fictional company. The
columns represent years (2020 through 2025), the rows represent quarters, and the summary
rows show the mean and standard deviation for each year, calculated across the four
quarterly values in that column.

```jldoctest customizing_output
julia> using Statistics, PrettyTables

julia> profit = DataFrame(
           "2020" => [ 94.6, -105.6, -104.9,  -88.0],
           "2021" => [-84.3,   -8.7, -109.6,   75.8],
           "2022" => [172.6,  -42.5,   95.5, -141.0],
           "2023" => [-71.2,   51.6,  114.3,   15.5],
           "2024" => [-35.4,  -44.9,  140.3,   30.8],
           "2025" => [ 24.1,  136.1,   34.8, -183.7]
       );

julia> show(
           profit;
           # We use this option to align the summary rows with the data rows at the decimal
           # point.
           apply_alignment_regex_to_summary_rows = true,
           summary_rows = [mean, std],
           summary_row_labels = ["Mean", "Std. Dev."]
       )
4×6 DataFrame
       Row │ 2020       2021       2022      2023      2024      2025
           │ Float64    Float64    Float64   Float64   Float64   Float64
───────────┼──────────────────────────────────────────────────────────────
         1 │   94.6      -84.3      172.6    -71.2     -35.4       24.1
         2 │ -105.6       -8.7      -42.5     51.6     -44.9      136.1
         3 │ -104.9     -109.6       95.5    114.3     140.3       34.8
         4 │  -88.0       75.8     -141.0     15.5      30.8     -183.7
───────────┼──────────────────────────────────────────────────────────────
      Mean │  -50.975    -31.7       21.15    27.55     22.7        2.825
 Std. Dev. │   97.3905    83.5073   140.011   77.4612   85.3244   134.2
```

For more customization options, check the
[PrettyTables.jl documentation](https://ronisbr.github.io/PrettyTables.jl/stable/).