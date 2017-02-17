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
                    dt::AbstractDataTable;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NULL")
    n, p = size(dt)
    etypes = eltypes(dt)
    if header
        cnames = _names(dt)
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
            if !isnull(dt[j],i)
                if ! (etypes[j] <: Real)
		    print(io, quotemark)
		    escapedprint(io, get(dt[i, j]), quotestr)
		    print(io, quotemark)
                else
		    print(io, dt[i, j])
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

function printtable(dt::AbstractDataTable;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NULL")
    printtable(STDOUT,
               dt,
               header = header,
               separator = separator,
               quotemark = quotemark,
               nastring = nastring)
    return
end

"""
Write data to a tabular-file format (CSV, TSV, ...)

```julia
writetable(filename, dt, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be created
* `dt::AbstractDataTable` : the AbstractDataTable to be written

### Keyword Arguments

* `separator::Char` -- The separator character that you would like to use. Defaults to the output of `getseparator(filename)`, which uses commas for files that end in `.csv`, tabs for files that end in `.tsv` and a single space for files that end in `.wsv`.
* `quotemark::Char` -- The character used to delimit string fields. Defaults to `'"'`.
* `header::Bool` -- Should the file contain a header that specifies the column names from `dt`. Defaults to `true`.
* `nastring::AbstractString` -- What to write in place of missing data. Defaults to `"NULL"`.

### Result

* `::DataTable`

### Examples

```julia
dt = DataTable(A = 1:10)
writetable("output.csv", dt)
writetable("output.dat", dt, separator = ',', header = false)
writetable("output.dat", dt, quotemark = '\', separator = ',')
writetable("output.dat", dt, header = false)
```
"""
function writetable(filename::AbstractString,
                    dt::AbstractDataTable;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    nastring::AbstractString = "NULL",
                    append::Bool = false)

    if endswith(filename, ".bz") || endswith(filename, ".bz2")
        throw(ArgumentError("BZip2 compression not yet implemented"))
    end

    if append && isfile(filename) && filesize(filename) > 0
        file_dt = readtable(filename, header = false, nrows = 1)

        # Check if number of columns matches
        if size(file_dt, 2) != size(dt, 2)
            throw(DimensionMismatch("Number of columns differ between file and DataTable"))
        end

        # When 'append'-ing to a nonempty file,
        # 'header' triggers a check for matching colnames
        if header
            if any(i -> @compat(Symbol(file_dt[1, i])) != index(dt)[i], 1:size(dt, 2))
                throw(KeyError("Column names don't match names in file"))
            end

            header = false
        end
    end

    openfunc = endswith(filename, ".gz") ? gzopen : open

    openfunc(filename, append ? "a" : "w") do io
        printtable(io,
                   dt,
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

@compat function Base.show(io::IO, ::MIME"text/html", dt::AbstractDataTable)
    cnames = _names(dt)
    write(io, "<table class=\"data-frame\">")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    haslimit = get(io, :limit, true)
    n = size(dt, 1)
    if haslimit
        tty_rows, tty_cols = _displaysize(io)
        mxrow = min(n,tty_rows)
    else
        mxrow = n
    end
    for row in 1:mxrow
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in cnames
            cell = sprint(ourshowcompact, dt[row, column_name])
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
    write(io, "</table>")
end

##############################################################################
#
# LaTeX output
#
##############################################################################

function latex_char_escape(char::AbstractString)
    if char == "\\"
        return "\\textbackslash{}"
    elseif char == "~"
        return "\\textasciitilde{}"
    else
        return string("\\", char)
    end
end

function latex_escape(cell::AbstractString)
    cell = replace(cell, ['\\','~','#','$','%','&','_','^','{','}'], latex_char_escape)
    return cell
end

function Base.show(io::IO, ::MIME"text/latex", dt::AbstractDataTable)
    nrows = size(dt, 1)
    ncols = size(dt, 2)
    cnames = _names(dt)
    alignment = repeat("c", ncols)
    write(io, "\\begin{tabular}{r|")
    write(io, alignment)
    write(io, "}\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(c)), cnames), " & ")
    write(io, header)
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    for row in 1:nrows
        write(io, "\t")
        write(io, @sprintf("%d", row))
        for col in 1:ncols
            write(io, " & ")
            cell = dt[row,col]
            if !isnull(cell)
                content = get(cell)
                if mimewritable(MIME("text/latex"), content)
                    show(io, MIME("text/latex"), content)
                else
                    print(io, latex_escape(string(content)))
                end
            end
        end
        write(io, " \\\\\n")
    end
    write(io, "\\end{tabular}\n")
end

##############################################################################
#
# MIME
#
##############################################################################

@compat function Base.show(io::IO, ::MIME"text/csv", dt::AbstractDataTable)
    printtable(io, dt, true, ',')
end

@compat function Base.show(io::IO, ::MIME"text/tab-separated-values", dt::AbstractDataTable)
    printtable(io, dt, true, '\t')
end
