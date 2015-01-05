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
                    header::Bool = ifelse(append,false,true),
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    append::Bool = false,
                    check_column_names::Bool = false
                    )
    
    
               
    if append==false
        # Case 0: Move to end of the if statement
        
    elseif !isfile(filename)
        # Case 1: There is no file to append to ==> Create a file
        append == false
    else
        # Case 2: There is a file. 
        
        # Checking number of columns and matching headers

        # Read first line
        read_io = open(filename, "r")
        file_headers = Base.split(readline(read_io),separator)  
        close(read_io)

        
        # Check if number of columns matches
        if length(file_headers) != length(names(df)) 
            error("Number of columns mismatch between file and DataFrame")
        end


        if check_column_names==true # Perform column name check
            
            # Convert to String
             file_headers =  [convert(String,i) for i in file_headers]

            # Parse headers            
            map!(k-> Base.strip(Base.strip(k),'\"'),file_headers) 

            # Change to symbol
            file_headers = map(m -> symbol(m),file_headers) 
            
            if any(file_headers.!= names(df))  # Check that all column names matches
                error("Column headers are not matching with first line of file")
            end  
        end 
    end

    mode_selected = ifelse(append, "a", "w")
    
    if endswith(filename, ".gz")
        io = gzopen(filename, mode_selected)
    elseif endswith(filename, ".bz") || endswith(filename, ".bz2")
        error("BZip2 compression not yet implemented")
    else
        io = open(filename, mode_selected)
        
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
    cnames = _names(df)
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
