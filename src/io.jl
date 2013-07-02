# NB: Linebreaks don't need to be stored, but they do aid debugging
# Implement peek(io, Uint8) to be used in dataframes io
import Base.peek
peek(io::IO, ::Type{Uint8}) = uint8(eof(io) ? -1 : peek(io))

# function atnewline(chr::Union(Uint8, Char), nextchr::Union(Uint8, Char))
#     return chr == '\n' ||                   # UNIX + Windows
#            (chr == '\r' && nextchr != '\n') # OS 9
# end
macro atnewline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == '\n' || $chr == '\r' && $nextchr != '\n'
    end
end

# function atblankline(chr::Union(Uint8, Char), nextchr::Union(Uint8, Char))
#     return (chr == '\n' && nextchr == '\n') || # UNIX
#            (chr == '\n' && nextchr == '\r') || # Windows
#            (chr == '\r' && nextchr == '\r')    # OS 9
# end
macro atblankline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        ($chr == '\n' && $nextchr == '\n') ||
        ($chr == '\n' && $nextchr == '\r') ||
        ($chr == '\r' && $nextchr == '\r')
    end
end

# function atescape(chr::Union(Uint8, Char),
#                   nextchr::Union(Uint8, Char),
#                   quotemark::Char)
#     return chr == '\\' ||                             # \" escaping
#            (chr == quotemark && nextchr == quotemark) # "" escaping
# end
macro atescape(chr, nextchr, quotemark)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quotemark = esc(quotemark)
    quote
        $chr == '\\' || ($chr == $quotemark && $nextchr == $quotemark)
    end
end

function safesetindex!(a::Array, val::Any, index::Real, a_size::Integer)
    if a_size < index
        a_size *= 2
        resize!(a, a_size)
    end
    a[index] = val
    return a_size
end

function getseparator(filename::String)
    if ismatch(r"csv$", filename)
        return ','
    elseif ismatch(r"tsv$", filename)
        return '\t'
    elseif ismatch(r"wsv$", filename)
        return ' '
    else
        error("Unable to determine separator used in $filename")
    end
end

# Read CSV file's rows into buffer while storing field boundary information
function readnrows!(io::IO,
                    buffer::Vector{Uint8},
                    linebreak_indices::Vector{Int},
                    right_boundary_indices::Vector{Int},
                    nrows::Int,
                    separator::Char,
                    allowquotes::Bool,
                    quotemark::Char,
                    skipblanks::Bool,
                    allowcomments::Bool,
                    commentmark::Char)
    bytes_read::Int = 0
    right_boundaries_read::Int = 0
    linebreaks_read::Int = 0

    in_quotes::Bool = false
    in_escape::Bool = false
    at_front::Bool = true

    chr::Uint8 = ' '
    nextchr::Uint8 = ' '

    buffer_size::Int = length(buffer)
    linebreak_indices_size::Int = length(linebreak_indices)
    right_boundary_indices_size::Int = length(right_boundary_indices)

    # Insert a dummy break at position 0
    right_boundaries_read += 1
    right_boundary_indices_size =
      safesetindex!(right_boundary_indices,
                    0,
                    right_boundaries_read,
                    right_boundary_indices_size)
    # BEGIN
    # Needed to satisfy strtod()
    bytes_read += 1
    buffer_size =
      safesetindex!(buffer,
                    '\n',
                    bytes_read,
                    buffer_size)
    # END
    linebreaks_read += 1
    linebreak_indices_size =
      safesetindex!(linebreak_indices,
                    0,
                    linebreaks_read,
                    linebreak_indices_size)

    # Loop over bytes from the input until we've read requested rows
    while !eof(io) && ((nrows == -1) || (linebreaks_read < nrows + 1))
        chr = read(io, Uint8)
        nextchr = peek(io, Uint8)
        # === Debugging ===
        # if in_quotes
        #     print_with_color(:red, string(char(chr)))
        # else
        #     print_with_color(:green, string(char(chr)))
        # end

        # Ignore text inside comments completely
        if allowcomments && !in_quotes && chr == commentmark
            while !eof(io) && !(@atnewline chr nextchr)
                chr = read(io, Uint8)
                nextchr = peek(io, Uint8)
            end
            # Skip the linebreak if the comment started at the front of a line
            if at_front
                continue
            end
        end

        # Skip blank lines
        if skipblanks && !in_quotes
            while !eof(io) && (@atblankline chr nextchr)
                chr = read(io, Uint8)
                nextchr = peek(io, Uint8)
                # Special handling for Windows
                if !eof(io) && chr == '\r' && nextchr == '\n'
                    chr = read(io, Uint8)
                    nextchr = peek(io, Uint8)
                end
            end
        end

        # No longer at the start of a line that might be a pure comment
        at_front = false

        # Processing is very different inside and outside of quotes
        if !in_quotes
            # Entering a quoted region
            if chr == quotemark && allowquotes
                in_quotes = true
            # Finished reading a field
            elseif chr == separator
                right_boundaries_read += 1
                right_boundary_indices_size =
                  safesetindex!(right_boundary_indices,
                                bytes_read,
                                right_boundaries_read,
                                right_boundary_indices_size)
                # BEGIN
                # Needed to satisfy strtod()
                bytes_read += 1
                buffer_size =
                  safesetindex!(buffer,
                                '\n',
                                bytes_read,
                                buffer_size)
                # END
            # Finished reading a row
            elseif @atnewline chr nextchr
                right_boundaries_read += 1
                right_boundary_indices_size =
                  safesetindex!(right_boundary_indices,
                                bytes_read,
                                right_boundaries_read,
                                right_boundary_indices_size)
                # BEGIN
                # Needed to satisfy strtod()
                bytes_read += 1
                buffer_size =
                  safesetindex!(buffer,
                                '\n',
                                bytes_read,
                                buffer_size)
                # END
                linebreaks_read +=1
                linebreak_indices_size =
                  safesetindex!(linebreak_indices,
                                bytes_read,
                                linebreaks_read,
                                linebreak_indices_size)
                at_front = true
            # Store character into buffer
            else
                bytes_read += 1
                buffer_size =
                  safesetindex!(buffer,
                                chr,
                                bytes_read,
                                buffer_size)
            end
        else
            # Escape a quotemark inside quoted regions
            if (@atescape chr nextchr quotemark) && !in_escape
                in_escape = true
            else
                # Exited a quoted region
                if chr == quotemark && allowquotes && !in_escape
                    in_quotes = false
                # Store character into buffer
                else
                    bytes_read += 1
                    buffer_size =
                      safesetindex!(buffer,
                                    chr,
                                    bytes_read,
                                    buffer_size)
                end

                # Escape mode only lasts for one byte
                in_escape = false
            end
        end
    end

    # Append a final EOL if it's missing in the raw input
    if eof(io) && !(@atnewline chr nextchr)
        right_boundaries_read += 1
        right_boundary_indices_size =
          safesetindex!(right_boundary_indices,
                        bytes_read,
                        right_boundaries_read,
                        right_boundary_indices_size)
        # BEGIN
        # Needed to satisfy strtod()
        bytes_read += 1
        buffer_size =
          safesetindex!(buffer,
                        '\n',
                        bytes_read,
                        buffer_size)
        # END
        linebreaks_read += 1
        linebreak_indices_size =
          safesetindex!(linebreak_indices,
                        bytes_read,
                        linebreaks_read,
                        linebreak_indices_size)
    end

    # Don't count the dummy boundaries in fields or rows
    return bytes_read, right_boundaries_read - 1, linebreaks_read - 1
end

function buffermatch{T <: ByteString}(buffer::Vector{Uint8},
                                      left::Int,
                                      right::Int,
                                      exemplars::Vector{T})
    l::Int = right - left + 1

    for index in 1:length(exemplars)
        exemplar = exemplars[index]
        if length(exemplar) == l
            isamatch = true
            for i in 0:(l - 1)
                isamatch &= buffer[left + i] == exemplar[1 + i]
            end
            if isamatch
                return true
            end
        end
    end

    return false
end

# TODO: Align more closely with parseint code
function bytestoint{T <: ByteString}(buffer::Vector{Uint8},
                                     left::Int,
                                     right::Int,
                                     nastrings::Vector{T})
    if left > right
        return 0, true, true
    end

    if buffermatch(buffer, left, right, nastrings)
        return 0, true, true
    end

    value::Int = 0
    power::Int = 1
    index::Int = right
    byte::Uint8 = buffer[index]

    while index > left
        if '0' <= byte <= '9'
            value += (byte - '0') * power
            power *= 10
        else
            return value, false, false
        end
        index -= 1
        byte = buffer[index]
    end

    if byte == '-'
        return -value, true, false
    elseif byte == '+'
        return value, true, false
    elseif '0' <= byte <= '9'
        value += (byte - '0') * power
        return value, true, false
    else
        return value, false, false
    end
end

let out::Vector{Float64} = Array(Float64, 1)
    global bytestofloat
    function bytestofloat{T <: ByteString}(buffer::Vector{Uint8},
                                           left::Int,
                                           right::Int,
                                           nastrings::Vector{T})
        if left > right
            return 0.0, true, true
        end

        if buffermatch(buffer, left, right, nastrings)
            return 0.0, true, true
        end

        wasparsed = ccall(:jl_substrtod,
                          Int32,
                          (Ptr{Uint8}, Int, Int, Ptr{Float64}),
                          buffer,
                          left - 1,
                          right - left + 1,
                          out) == 0

        return out[1], wasparsed, false
    end
end

function bytestobool{T <: ByteString}(buffer::Vector{Uint8},
                                      left::Int,
                                      right::Int,
                                      nastrings::Vector{T},
                                      truestrings::Vector{T},
                                      falsestrings::Vector{T})
    if left > right
        return false, true, true
    end

    if buffermatch(buffer, left, right, nastrings)
        return false, true, true
    end

    if buffermatch(buffer, left, right, truestrings)
        return true, true, false
    elseif buffermatch(buffer, left, right, falsestrings)
        return false, true, false
    else
        return false, false, false
    end
end

function bytestostring{T <: ByteString}(buffer::Vector{Uint8},
                                        left::Int,
                                        right::Int,
                                        nastrings::Vector{T},
                                        quotemark::Char)
    if left > right
        return "", true, false
    end

    if buffermatch(buffer, left, right, nastrings)
        return "", true, true
    end

    return bytestring(buffer[left:right]), true, false
end

function builddf{T <: ByteString}(rows::Int,
                                  cols::Int,
                                  bytes::Int,
                                  fields::Int,
                                  buffer::Vector{Uint8},
                                  linebreak_indices::Vector{Int},
                                  right_boundary_indices::Vector{Int},
                                  separator::Char,
                                  quotemark::Char,
                                  nastrings::Vector{T},
                                  truestrings::Vector{T},
                                  falsestrings::Vector{T},
                                  ignorepadding::Bool,
                                  makefactors::Bool,
                                  colnames::Vector)
    columns::Vector{Any} = Array(Any, cols)

    for j in 1:cols
        values = Array(Int, rows)
        missing::BitVector = falses(rows)
        is_int::Bool = true
        is_float::Bool = true
        is_bool::Bool = true
        is_string::Bool = true

        i::Int = 0
        while i < rows
            i += 1

            # Determine left and right boundaries of field
            left = right_boundary_indices[(i - 1) * cols + j] + 2
            right = right_boundary_indices[(i - 1) * cols + j + 1]

            # Ignore left-and-right whitespace padding
            # TODO: Debate moving this into readnrows()
            # TODO: Modify readnrows() so that '\r' and '\n' don't occur near edges
            if ignorepadding
                while left < right &&
                      (buffer[left] == ' ' ||
                       buffer[left] == '\t' ||
                       buffer[left] == '\r' ||
                       buffer[left] == '\n')
                    left += 1
                end
                while left <= right &&
                      (buffer[right] == ' ' ||
                       buffer[right] == '\t' ||
                       buffer[right] == '\r' ||
                       buffer[right] == '\n')
                    right -= 1
                end
            end

            # (1) Try to parse values as Int's
            if is_int
                values[i], wasparsed, missing[i] =
                  bytestoint(buffer, left, right, nastrings)
                if wasparsed
                    continue
                else
                    is_int = false
                    values = convert(Array{Float64}, values)
                end
            end

            # (2) Try to parse as Float64's
            if is_float
                values[i], wasparsed, missing[i] =
                  bytestofloat(buffer, left, right, nastrings)
                if wasparsed
                    continue
                else
                    is_float = false
                    values = Array(Bool, rows)
                    i = 1
                end
            end

            # (3) Try to parse as Bool's
            if is_bool
                values[i], wasparsed, missing[i] =
                  bytestobool(buffer, left, right,
                              nastrings,
                              truestrings, falsestrings)
                if wasparsed
                    continue
                else
                    is_bool = false
                    values = Array(UTF8String, rows)
                    i = 1
                end
            end

            # (4) Fallback to UTF8String
            # TODO: Make sure empty string is handled correctly
            values[i], wasparsed, missing[i] =
              bytestostring(buffer, left, right, nastrings, quotemark)
        end

        if makefactors && is_string
            columns[j] = PooledDataArray(values, missing)
        else
            columns[j] = DataArray(values, missing)
        end
    end

    if isempty(colnames)
        colnames = DataFrames.generate_column_names(cols)
    end

    return DataFrame(columns, colnames)
end

function parsecolnames(buffer::Vector{Uint8},
                       right_boundary_indices::Vector{Int},
                       fields::Int)
    if fields == 0
        error("Header line was empty")
    end

    colnames = Array(UTF8String, fields)

    for j in 1:fields
        left = right_boundary_indices[j] + 2
        right = right_boundary_indices[j + 1]
        if buffer[right] == '\r' || buffer[right] == '\n'
            colnames[j] = bytestring(buffer[left:(right - 1)])
        else
            colnames[j] = bytestring(buffer[left:right])
        end
    end

    return colnames
end

function readtable(io::IO;
                   header::Bool = true,
                   separator::Char = ',',
                   allowquotes::Bool = true,
                   quotemark::Char = '"',
                   decimal::Char = '.',
                   nastrings::Vector = ASCIIString["", "NA"],
                   truestrings::Vector = ASCIIString["T", "t", "TRUE", "true"],
                   falsestrings::Vector = ASCIIString["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Int = -1,
                   colnames::Vector = UTF8String[],
                   cleannames::Bool = false,
                   coltypes::Vector{Any} = Any[],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Int = 0,
                   skiprows::Vector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8,
                   buffersize::Int = 2^20)
    # Allocate buffers to conserve memory
    # TODO: Pass in these three buffers on every pass for DataStream's
    buffer::Vector{Uint8} = Array(Uint8, buffersize)
    linebreak_indices::Vector{Int} = Array(Int, 1)
    right_boundary_indices::Vector{Int} = Array(Int, 1)

    # Skip lines at the start
    skipped_lines::Int = 0
    if skipstart != 0
        chr::Uint8 = read(io, Uint8)
        nextchr::Uint8 = peek(io, Uint8)
        while skipped_lines < skipstart
            while !eof(io) && !(@atnewline chr nextchr)
                chr = read(io, Uint8)
                nextchr = peek(io, Uint8)
            end
            skipped_lines += 1
            if !eof(io) && skipped_lines < skipstart
                chr = read(io, Uint8)
                nextchr = peek(io, Uint8)
            end
        end
    end

    # Extract the header
    if header
        bytes, fields, rows =
          readnrows!(io,
                     buffer,
                     linebreak_indices,
                     right_boundary_indices,
                     1,
                     separator,
                     allowquotes,
                     quotemark,
                     skipblanks,
                     allowcomments,
                     commentmark)
    end

    # Insert column names from header if none present
    if header && isempty(colnames)
        colnames = parsecolnames(buffer,
                                 right_boundary_indices,
                                 fields)
    end

    # Separate text into fields
    bytes, fields, rows =
      readnrows!(io,
                 buffer,
                 linebreak_indices,
                 right_boundary_indices,
                 nrows,
                 separator,
                 allowquotes,
                 quotemark,
                 skipblanks,
                 allowcomments,
                 commentmark)

    # Throw an error if we didn't see any bytes
    if bytes == 0
        error("Failed to read any bytes.")
    end

    # Throw an error if we didn't see any rows
    if rows == 0
        error("Failed to read any rows.")
    end

    # Throw an error if we didn't see any fields
    if fields == 0
        error("Failed to read any fields.")
    end

    # Determine the number of columns
    cols = fld(fields, rows)

    # Confirm that the number of columns is consistent across rows
    if fields != rows * cols
        linesizes = Array(Int, rows)
        total_fields = 1
        n = length(right_boundary_indices)
        for i in 1:rows
            bound = linebreak_indices[i + 1]
            fields_in_row = 0
            while right_boundary_indices[total_fields] < bound
                fields_in_row += 1
                total_fields += 1
            end
            linesizes[i] = fields_in_row
        end
        msg = @sprintf "Saw %d rows, %d columns and %d fields\n" rows cols fields
        m = median(linesizes)
        broken_rows = find(linesizes .!= m)
        linenumber = broken_rows[1]
        msg = string(msg, @sprintf " * Line %d has %d columns\n" linenumber linesizes[linenumber] + 1)
        error(msg)
    end

    # Parse contents of a buffer into a DataFrame
    df = builddf(rows,
                 cols,
                 bytes,
                 fields,
                 buffer,
                 linebreak_indices,
                 right_boundary_indices,
                 separator,
                 quotemark,
                 nastrings,
                 truestrings,
                 falsestrings,
                 ignorepadding,
                 makefactors,
                 colnames)

    # Clean up column names if requested
    if cleannames
        clean_colnames!(df)
    end

    # Return the final DataFrame
    return df
end

function readtable(pathname::String;
                   header::Bool = true,
                   separator::Char = ',',
                   allowquotes::Bool = true,
                   quotemark::Char = '"',
                   decimal::Char = '.',
                   nastrings::Vector = ASCIIString["", "NA"],
                   truestrings::Vector = ASCIIString["T", "t", "TRUE", "true"],
                   falsestrings::Vector = ASCIIString["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Int = -1,
                   colnames::Vector = UTF8String[],
                   cleannames::Bool = false,
                   coltypes::Vector{Any} = Any[],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Int = 0,
                   skiprows::Vector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8)
    # Open an IO stream based on pathname
    # (1) Path is an HTTP or FTP URL
    if ismatch(r"^(http://)|(ftp://)", pathname)
        error("URL retrieval not yet implemented")
    # (2) Path is GZip file
    elseif ismatch(r"\.gz$", pathname)
        io = gzopen(pathname, "r")
        nbytes = 2 * filesize(pathname)
    # (3) Path is BZip2 file
    elseif ismatch(r"\.bz2?$", pathname)
        error("BZip2 decompression not yet implemented")
    # (4) Path is an uncompressed file
    else
        io = open(pathname, "r")
        nbytes = filesize(pathname)
    end

    # If user wants all rows, overestimate nrows
    if nrows == -1
        nrows = nbytes
    end

    # Use the IO stream method for readtable()
    df = readtable(io,
                   header = header,
                   separator = separator,
                   allowquotes = allowquotes,
                   quotemark = quotemark,
                   decimal = decimal,
                   nastrings = nastrings,
                   truestrings = truestrings,
                   falsestrings = falsestrings,
                   makefactors = makefactors,
                   nrows = nrows,
                   colnames = colnames,
                   cleannames = cleannames,
                   coltypes = coltypes,
                   allowcomments = allowcomments,
                   commentmark = commentmark,
                   ignorepadding = ignorepadding,
                   skipstart = skipstart,
                   skiprows = skiprows,
                   skipblanks = skipblanks,
                   encoding = encoding,
                   buffersize = nbytes)

    # Close the IO stream
    close(io)

    # Return the resulting DataFrame
    return df
end

##############################################################################
#
# Text output
#
##############################################################################

quoted(val::String, quotemark::Char) = string(quotemark, val, quotemark)
quoted(val::Real, quotemark::Char) = string(val)
quoted(val::Any, quotemark::Char) = string(quotemark, string(val), quotemark)

# TODO: Increase precision of string representation of Float64's
function printtable(io::IO,
                    df::DataFrame;
                    separator::Char = ',',
                    quotemark::Char = '"',
                    header::Bool = true)
    n, p = size(df)
    if header
        column_names = colnames(df)
        for j in 1:p
            if j < p
                print(io, quoted(column_names[j], quotemark))
                print(io, separator)
            else
                println(io, quoted(column_names[j], quotemark))
            end
        end
    end
    for i in 1:n
        for j in 1:p
            if j < p
                print(io, quoted(df[i, j], quotemark))
                print(io, separator)
            else
                println(io, quoted(df[i, j], quotemark))
            end
        end
    end
    return
end

function printtable(df::DataFrame;
                    separator::Char = ',',
                    quotemark::Char = '"',
                    header::Bool = true)
    printtable(STDOUT,
               df,
               separator = separator,
               quotemark = quotemark,
               header = header)
    return
end

# Infer configuration settings from filename
function writetable(filename::String,
                    df::DataFrame;
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    header::Bool = true)
    io = open(filename, "w")
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
# Binary serialization
#
##############################################################################

function save(filename::String, df::AbstractDataFrame)
    f = open(filename, "w")
    serialize(f, df)
    close(f)
    return
end

function load_df(filename::String)
    f = open(filename)
    dd = deserialize(f)
    close(f)
    return dd
end
