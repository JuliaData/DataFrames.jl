##############################################################################
#
# Text output
#
##############################################################################

function escapedprint(io::IO, x::Any, escapes::String)
    print(io, x)
end

function escapedprint(io::IO, x::String, escapes::String)
    print_escaped(io, x, escapes)
end

function printtable(io::IO,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::String = "NA")
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
    for i in 1:n
        for j in 1:p
            if ! (isna(df[j],i))
                if ! (etypes[j] <: Real)
		    print(io, quotemark)
		    escapedprint(io, df[i, j], "\"'")
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
                    nastring::String = "NA")
    printtable(STDOUT,
               df,
               header = header,
               separator = separator,
               quotemark = quotemark,
               nastring = nastring)
    return
end

# Infer configuration settings from filename
function writetable(filename::String,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    nastring::String = "NA",
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
            if any(i -> symbol(file_df[1, i]) != index(df)[i], 1:size(df, 2))
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

function html_escape(cell::String)
    cell = replace(cell, "&", "&amp;")
    cell = replace(cell, "<", "&lt;")
    cell = replace(cell, ">", "&gt;")
    return cell
end

function Base.writemime(io::IO,
                        ::MIME"text/html",
                        df::AbstractDataFrame)
    n = size(df, 1)
    cnames = _names(df)
    write(io, "<table class=\"data-frame\">")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    tty_rows, tty_cols = Base.tty_size()
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
    write(io, "</table>")
end

##############################################################################
#
# MIME
#
##############################################################################

function Base.writemime(io::IO,
                        ::MIME"text/csv",
                        df::AbstractDataFrame)
    printtable(io, df, true, ',')
end

function Base.writemime(io::IO,
                        ::MIME"text/tab-separated-values",
                        df::AbstractDataFrame)
    printtable(io, df, true, '\t')
end
