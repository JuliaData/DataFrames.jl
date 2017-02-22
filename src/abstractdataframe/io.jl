##############################################################################
#
# Text output
#
##############################################################################

function escapedprint(io::IO, x::Any, escapes::AbstractString)
    print(io, x)
end

if VERSION < v"0.5.0-dev+4354"
    function escapedprint(io::IO, x::AbstractString, escapes::AbstractString)
        print_escaped(io, x, escapes)
    end
else
    function escapedprint(io::IO, x::AbstractString, escapes::AbstractString)
        escape_string(io, x, escapes)
    end
end

function printtable(io::IO,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA")
    n, p = size(df)
    etypes = eltypes(df)
    if header
        cnames = _names(df)
        for j in 1:p
            print(io, quotemark)
            print(io, cnames[j])
            print(io, quotemark)
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    quotestr = string(quotemark)
    for i in 1:n
        for j in 1:p
            if ! (isna(df[j],i))
                if ! (etypes[j] <: Real)
		    print(io, quotemark)
		    escapedprint(io, df[i, j], quotestr)
		    print(io, quotemark)
                else
		    print(io, df[i, j])
                end
            else
		print(io, nastring)
            end
            if j < p
		print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    return
end

function printtable(df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA")
    printtable(STDOUT,
               df,
               header = header,
               separator = separator,
               quotemark = quotemark,
               nastring = nastring)
    return
end

"""
Write data to a tabular-file format (CSV, TSV, ...)

```julia
writetable(filename, df, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be created
* `df::AbstractDataFrame` : the AbstractDataFrame to be written

### Keyword Arguments

* `separator::Char` -- The separator character that you would like to use. Defaults to the output of `getseparator(filename)`, which uses commas for files that end in `.csv`, tabs for files that end in `.tsv` and a single space for files that end in `.wsv`.
* `quotemark::Char` -- The character used to delimit string fields. Defaults to `'"'`.
* `header::Bool` -- Should the file contain a header that specifies the column names from `df`. Defaults to `true`.
* `nastring::AbstractString` -- What to write in place of missing data. Defaults to `"NA"`.

### Result

* `::DataFrame`

### Examples

```julia
df = DataFrame(A = 1:10)
writetable("output.csv", df)
writetable("output.dat", df, separator = ',', header = false)
writetable("output.dat", df, quotemark = '\', separator = ',')
writetable("output.dat", df, header = false)
```
"""
function writetable(filename::AbstractString,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA",
                    append::Bool = false)

    if endswith(filename, ".bz") || endswith(filename, ".bz2")
        throw(ArgumentError("BZip2 compression not yet implemented"))
    end

    if append && isfile(filename) && filesize(filename) > 0
        file_df = readtable(filename, header = false, nrows = 1)

        # Check if number of columns matches
        if size(file_df, 2) != size(df, 2)
            throw(DimensionMismatch("Number of columns differ between file and DataFrame"))
        end

        # When 'append'-ing to a nonempty file,
        # 'header' triggers a check for matching colnames
        if header
            if any(i -> @compat(Symbol(file_df[1, i])) != index(df)[i], 1:size(df, 2))
                throw(KeyError("Column names don't match names in file"))
            end

            header = false
        end
    end

    openfunc = endswith(filename, ".gz") ? gzopen : open

    openfunc(filename, append ? "a" : "w") do io
        printtable(io,
                   df,
                   header = header,
                   separator = separator,
                   quotemark = quotemark,
                   nastring = nastring)
    end

    return
end

##############################################################################
#
# HTML output
#
##############################################################################

function html_escape(cell::AbstractString)
    cell = replace(cell, "&", "&amp;")
    cell = replace(cell, "<", "&lt;")
    cell = replace(cell, ">", "&gt;")
    return cell
end

@compat function Base.show(io::IO, ::MIME"text/html", df::AbstractDataFrame)
    n = size(df, 1)
    cnames = _names(df)
    write(io, "<table class=\"data-frame\">")
    write(io, "<thead>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    write(io, "</thead>")
    write(io, "<tbody>")
    tty_rows, tty_cols = Base.displaysize(io)
    mxrow = min(n,tty_rows)
    for row in 1:mxrow
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in cnames
            cell = string(df[row, column_name])
            write(io, "<td>$(html_escape(cell))</td>")
        end
        write(io, "</tr>")
    end
    if n > mxrow
        write(io, "<tr>")
        write(io, "<th>&vellip;</th>")
        for column_name in cnames
            write(io, "<td>&vellip;</td>")
        end
        write(io, "</tr>")
    end
    write(io, "</tbody>")
    write(io, "</table>")
end

##############################################################################
#
# MIME
#
##############################################################################

@compat function Base.show(io::IO, ::MIME"text/csv", df::AbstractDataFrame)
    printtable(io, df, true, ',')
end

@compat function Base.show(io::IO, ::MIME"text/tab-separated-values", df::AbstractDataFrame)
    printtable(io, df, true, '\t')
end
