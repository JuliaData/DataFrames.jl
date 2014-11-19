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
                    quotemark::Char = '"')
    n, p = size(df)
    etypes = eltypes(df)
    if header
        cnames = names(df)
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
            if ! (etypes[j] <: Real)
                print(io, quotemark)
                escapedprint(io, df[i, j], "\"'")
                print(io, quotemark)
            else
                print(io, df[i, j])
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
                    quotemark::Char = '"')
    printtable(STDOUT,
               df,
               separator = separator,
               quotemark = quotemark,
               header = header)
    return
end

# Infer configuration settings from filename
function writetable(filename::String,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"')
    if endswith(filename, ".gz")
        io = gzopen(filename, "w")
    elseif endswith(filename, ".bz") || endswith(filename, ".bz2")
        error("BZip2 compression not yet implemented")
    else
        io = open(filename, "w")
    end
    printtable(io,
               df,
               separator = separator,
               quotemark = quotemark,
               header = header)
    close(io)
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
    cnames = names(df)
    write(io, "<table class=\"data-frame\">")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    tty_rows, tty_cols = Base.tty_size()
    for row in 1:min(n, tty_rows)
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in cnames
            cell = string(df[row, column_name])
            write(io, "<td>$(html_escape(cell))</td>")
        end
        write(io, "</tr>")
    end
    if n > 20
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
