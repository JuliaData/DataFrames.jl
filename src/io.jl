import Base.peek
Base.peek(io::IO, ::Type{Uint8}) = uint8(eof(io) ? -1 : peek(io))

immutable ParsedCSV
    bytes::Vector{Uint8} # Raw bytes from CSV file
    bounds::Vector{Int}  # Right field boundary indices
    lines::Vector{Int}   # Line break indices
    quoted::BitVector    # Was field quoted in text
end

immutable ParseOptions{S <: ByteString, T <: ByteString}
    header::Bool
    separator::Char
    quotemark::Vector{Char}
    decimal::Char
    nastrings::Vector{S}
    truestrings::Vector{S}
    falsestrings::Vector{S}
    makefactors::Bool
    colnames::Vector{T}
    cleannames::Bool
    coltypes::Vector{Any}
    allowcomments::Bool
    commentmark::Char
    ignorepadding::Bool
    skipstart::Int
    skiprows::Vector{Int}
    skipblanks::Bool
    encoding::Symbol
end

macro atnewline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == '\n' || $chr == '\r'
    end
end

macro atblankline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        ($chr == '\n' || $chr == '\r') && ($nextchr == '\n' || $nextchr == '\r')
    end
end

macro atescape(chr, nextchr, quotemark)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quotemark = esc(quotemark)
    quote
        $chr == '\\' ||                                # \" escaping
        ($chr in $quotemark && $nextchr in $quotemark) # "" escaping
    end
end

macro isspace(byte)
    byte = esc(byte)
    quote
        0x09 <= $byte <= 0x0d || $byte == 0x20
    end
end

# This trick is ugly, but is ~33% faster than push!() for large arrays
macro push(count, a, val, l)
    count = esc(count) # Number of items in array
    a = esc(a)         # Array to update
    val = esc(val)     # Value to insert
    l = esc(l)         # Length of array
    quote
        $count += 1
        if $l < $count
            $l *= 2
            resize!($a, $l)
        end
        $a[$count] = $val
    end
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
# TODO: Experiment with mmaping input
function readnrows!(p::ParsedCSV, io::IO, nrows::Int, o::ParseOptions)
    # Information about parse results
    n_bytes = 0
    n_bounds = 0
    n_lines = 0
    n_fields = 1
    l_bytes = length(p.bytes)
    l_lines = length(p.lines)
    l_bounds = length(p.bounds)
    l_quoted = length(p.quoted)

    # Current state of the parser
    in_quotes = false
    in_escape = false
    at_start = true
    chr = 0xff
    nextchr = 0xff
    skip_white = true           # whitespace flag if separator=' '

    # 'in' does not work if passed Uint8 and Vector{Char}
    quotemark = convert(Vector{Uint8}, o.quotemark)

    # Insert a dummy field bound at position 0
    @push n_bounds p.bounds 0 l_bounds
    @push n_bytes p.bytes '\n' l_bytes
    @push n_lines p.lines 0 l_lines

    # Loop over bytes from the input until we've read requested rows
    while !eof(io) && ((nrows == -1) || (n_lines < nrows + 1))
        chr, nextchr = read(io, Uint8), peek(io, Uint8)
        # === Debugging ===
        # if in_quotes
        #     print_with_color(:red, string(char(chr)))
        # else
        #     print_with_color(:green, string(char(chr)))
        # end

        # Ignore text inside comments completely
        if o.allowcomments && !in_quotes && chr == o.commentmark
            while !eof(io) && !(@atnewline chr nextchr)
                chr, nextchr = read(io, Uint8), peek(io, Uint8)
            end
            # Skip the linebreak if the comment began at the start of a line
            if at_start
                continue
            end
        end

        # Skip blank lines
        if o.skipblanks && !in_quotes
            while !eof(io) && (@atblankline chr nextchr)
                chr, nextchr = read(io, Uint8), peek(io, Uint8)
                # Special handling for Windows
                if !eof(io) && chr == '\r' && nextchr == '\n'
                    chr, nextchr = read(io, Uint8), peek(io, Uint8)
                end
            end
        end

        # No longer at the start of a line that might be a pure comment
        at_start = false

        # Processing is very different inside and outside of quotes
        if !in_quotes
            # Entering a quoted region
            if chr in quotemark
                in_quotes = true
                p.quoted[n_fields] = true
                skip_white = false
            # Finished reading a field
            elseif o.separator == ' ' && (chr == ' ' || chr == '\t')
                if !in(nextchr, " \t\n\r") && !skip_white
                    @push n_bounds p.bounds n_bytes l_bounds
                    @push n_bytes p.bytes '\n' l_bytes
                    @push n_fields p.quoted false l_quoted
                    skip_white = false
                end
            elseif chr == o.separator
                @push n_bounds p.bounds n_bytes l_bounds
                @push n_bytes p.bytes '\n' l_bytes
                @push n_fields p.quoted false l_quoted
            # Finished reading a row
            elseif @atnewline chr nextchr
                at_start = true
                @push n_bounds p.bounds n_bytes l_bounds
                @push n_bytes p.bytes '\n' l_bytes
                @push n_lines p.lines n_bytes l_lines
                @push n_fields p.quoted false l_quoted
                skip_white = true
            # Store character in buffer
            else
                @push n_bytes p.bytes chr l_bytes
                skip_white = false
            end
        else
            # Escape a quotemark inside quoted field
            if (@atescape chr nextchr quotemark) && !in_escape
                in_escape = true
            else
                # Exit quoted field
                if chr in quotemark && !in_escape
                    in_quotes = false
                # Store character in buffer
                else
                    @push n_bytes p.bytes chr l_bytes
                end
                # Escape mode only lasts for one byte
                in_escape = false
            end
        end
    end

    # Append a final EOL if it's missing in the raw input
    if eof(io) && !(@atnewline chr nextchr)
        @push n_bounds p.bounds n_bytes l_bounds
        @push n_bytes p.bytes '\n' l_bytes
        @push n_lines p.lines n_bytes l_lines
    end

    # Don't count the dummy boundaries in fields or rows
    return n_bytes, n_bounds - 1, n_lines - 1
end

function bytematch{T <: ByteString}(bytes::Vector{Uint8},
                                    left::Int,
                                    right::Int,
                                    exemplars::Vector{T})
    l = right - left + 1
    for index in 1:length(exemplars)
        exemplar = exemplars[index]
        if length(exemplar) == l
            matched = true
            for i in 0:(l - 1)
                matched &= bytes[left + i] == exemplar[1 + i]
            end
            if matched
                return true
            end
        end
    end
    return false
end

# TODO: Align more closely with parseint code
function bytestoint{T <: ByteString}(bytes::Vector{Uint8},
                                     left::Int,
                                     right::Int,
                                     nastrings::Vector{T})
    if left > right
        return 0, true, true
    end

    if bytematch(bytes, left, right, nastrings)
        return 0, true, true
    end

    value = 0
    power = 1
    index = right
    byte = bytes[index]

    while index > left
        if '0' <= byte <= '9'
            value += (byte - '0') * power
            power *= 10
        else
            return value, false, false
        end
        index -= 1
        byte = bytes[index]
    end

    if byte == '-'
        return -value, left < right, false
    elseif byte == '+'
        return value, left < right, false
    elseif '0' <= byte <= '9'
        value += (byte - '0') * power
        return value, true, false
    else
        return value, false, false
    end
end

let out = Array(Float64, 1)
    global bytestofloat
    function bytestofloat{T <: ByteString}(bytes::Vector{Uint8},
                                           left::Int,
                                           right::Int,
                                           nastrings::Vector{T})
        if left > right
            return 0.0, true, true
        end

        if bytematch(bytes, left, right, nastrings)
            return 0.0, true, true
        end

        wasparsed = ccall(:jl_substrtod,
                          Int32,
                          (Ptr{Uint8}, Csize_t, Int, Ptr{Float64}),
                          bytes,
                          convert(Csize_t, left - 1),
                          right - left + 1,
                          out) == 0

        return out[1], wasparsed, false
    end
end

function bytestobool{T <: ByteString}(bytes::Vector{Uint8},
                                      left::Int,
                                      right::Int,
                                      nastrings::Vector{T},
                                      truestrings::Vector{T},
                                      falsestrings::Vector{T})
    if left > right
        return false, true, true
    end

    if bytematch(bytes, left, right, nastrings)
        return false, true, true
    end

    if bytematch(bytes, left, right, truestrings)
        return true, true, false
    elseif bytematch(bytes, left, right, falsestrings)
        return false, true, false
    else
        return false, false, false
    end
end

function bytestostring{T <: ByteString}(bytes::Vector{Uint8},
                                        left::Int,
                                        right::Int,
                                        wasquoted::Bool,
                                        nastrings::Vector{T})
    if left > right
        if wasquoted
            return "", true, false
        else
            return "", true, true
        end
    end

    if bytematch(bytes, left, right, nastrings)
        return "", true, true
    end

    return bytestring(bytes[left:right]), true, false
end

function builddf(rows::Int, cols::Int, bytes::Int, fields::Int,
                 p::ParsedCSV, o::ParseOptions)
    columns = Array(Any, cols)

    for j in 1:cols
        values = Array(Int, rows)
        missing = falses(rows)
        is_int = true
        is_float = true
        is_bool = true

        i = 0
        while i < rows
            i += 1

            # Determine left and right boundaries of field
            left = p.bounds[(i - 1) * cols + j] + 2
            right = p.bounds[(i - 1) * cols + j + 1]
            wasquoted = p.quoted[(i - 1) * cols + j]

            # Ignore left-and-right whitespace padding
            # TODO: Debate moving this into readnrows()
            # TODO: Modify readnrows() so that '\r' and '\n' don't occur near edges
            if o.ignorepadding && !wasquoted
                while left < right && (@isspace p.bytes[left])
                    left += 1
                end
                while left <= right && (@isspace p.bytes[right])
                    right -= 1
                end
            end

            # (1) Try to parse values as Int's
            if is_int
                values[i], wasparsed, missing[i] =
                  bytestoint(p.bytes, left, right,
                             o.nastrings)
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
                  bytestofloat(p.bytes, left, right,
                               o.nastrings)
                if wasparsed
                    continue
                else
                    is_float = false
                    values = Array(Bool, rows)
                    i = 0
                    continue
                end
            end

            # (3) Try to parse as Bool's
            if is_bool
                values[i], wasparsed, missing[i] =
                  bytestobool(p.bytes, left, right,
                              o.nastrings, o.truestrings, o.falsestrings)
                if wasparsed
                    continue
                else
                    is_bool = false
                    values = Array(UTF8String, rows)
                    i = 0
                    continue
                end
            end

            # (4) Fallback to UTF8String
            values[i], wasparsed, missing[i] =
              bytestostring(p.bytes, left, right,
                            wasquoted, o.nastrings)
        end

        if o.makefactors && !(is_int || is_float || is_bool)
            columns[j] = PooledDataArray(values, missing)
        else
            columns[j] = DataArray(values, missing)
        end
    end

    if isempty(o.colnames)
        return DataFrame(columns, DataFrames.generate_column_names(cols))
    else
        return DataFrame(columns, o.colnames)
    end
end

function parsecolnames!(colnames::Vector{UTF8String},
                        bytes::Vector{Uint8},
                        bounds::Vector{Int},
                        fields::Int)
    if fields == 0
        error("Header line was empty")
    end

    resize!(colnames, fields)

    for j in 1:fields
        left = bounds[j] + 2
        right = bounds[j + 1]
        if bytes[right] == '\r' || bytes[right] == '\n'
            colnames[j] = bytestring(bytes[left:(right - 1)])
        else
            colnames[j] = bytestring(bytes[left:right])
        end
    end

    return
end

function findcorruption(rows::Integer,
                        cols::Integer,
                        fields::Integer,
                        p::ParsedCSV)
    n = length(p.bounds)
    lengths = Array(Int, rows)
    t = 1
    for i in 1:rows
        bound = p.lines[i + 1]
        f = 0
        while p.bounds[t] < bound
            f += 1
            t += 1
        end
        lengths[i] = f
    end
    m = median(lengths)
    corruptrows = find(lengths .!= m)
    l = corruptrows[1]
    msg = @sprintf "Saw %d rows, %d columns and %d fields\n" rows cols fields
    msg = string(msg,
                 @sprintf " * Line %d has %d columns\n" l lengths[l] + 1)
    error(msg)
end

function readtable!(p::ParsedCSV,
                    io::IO,
                    nrows::Int,
                    o::ParseOptions)
    # Skip lines at the start
    chr, nextchr = 0xff, 0xff
    skipped_lines = 0
    if o.skipstart != 0
        chr, nextchr = read(io, Uint8), peek(io, Uint8)
        while skipped_lines < o.skipstart
            while !eof(io) && !(@atnewline chr nextchr)
                chr, nextchr = read(io, Uint8), peek(io, Uint8)
            end
            skipped_lines += 1
            if !eof(io) && skipped_lines < o.skipstart
                chr, nextchr = read(io, Uint8), peek(io, Uint8)
            end
        end
    end

    # Extract the header
    if o.header
        bytes, fields, rows = readnrows!(p, io, 1, o)
    end

    # Insert column names from header if none present
    if o.header && isempty(o.colnames)
        parsecolnames!(o.colnames, p.bytes, p.bounds, fields)
    end

    # Parse main data set
    bytes, fields, rows = readnrows!(p, io, nrows, o)

    # Sanity checks
    bytes != 0 || error("Failed to read any bytes.")
    rows != 0 || error("Failed to read any rows.")
    fields != 0 || error("Failed to read any fields.")

    # Determine the number of columns
    cols = fld(fields, rows)

    # Confirm that the number of columns is consistent across rows
    if fields != rows * cols
        findcorruption(rows, cols, fields, p)
    end

    # Parse contents of a buffer into a DataFrame
    df = builddf(rows, cols, bytes, fields, p, o)

    # Clean up column names if requested
    if o.cleannames
        clean_colnames!(df)
    end

    # Return the final DataFrame
    return df
end

function readtable(pathname::String;
                   header::Bool = true,
                   separator::Char = ',',
                   quotemark::Vector{Char} = ['"'],
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

    # Allocate buffers for storing metadata
    p = ParsedCSV(Array(Uint8, nbytes), Array(Int, 1),
                  Array(Int, 1), BitArray(1))

    # Set parsing options
    o = ParseOptions(header, separator, quotemark, decimal,
                     nastrings, truestrings, falsestrings,
                     makefactors, colnames, cleannames, coltypes,
                     allowcomments, commentmark, ignorepadding,
                     skipstart, skiprows, skipblanks, encoding)

    # Use the IO stream method for readtable()
    df = readtable!(p, io, nrows, o)

    # Close the IO stream
    close(io)

    # Return the resulting DataFrame
    return df
end

function filldf!(df::DataFrame,
                 rows::Int, cols::Int, bytes::Int, fields::Int,
                 p::ParsedCSV, o::ParseOptions)
    ctypes = coltypes(df)

    if rows != size(df, 1)
        for j in 1:cols
            resize!(df.columns[j].data, rows)
            resize!(df.columns[j].na, rows)
        end
    end

    for j in 1:cols
        c = df.columns[j]
        T = ctypes[j]

        i = 0
        while i < rows
            i += 1

            # Determine left and right boundaries of field
            left = p.bounds[(i - 1) * cols + j] + 2
            right = p.bounds[(i - 1) * cols + j + 1]
            wasquoted = p.quoted[(i - 1) * cols + j]

            # Ignore left-and-right whitespace padding
            # TODO: Debate moving this into readnrows()
            # TODO: Modify readnrows() so that '\r' and '\n' don't occur near edges
            if o.ignorepadding && !wasquoted
                while left < right && (@isspace p.bytes[left])
                    left += 1
                end
                while left <= right && (@isspace p.bytes[right])
                    right -= 1
                end
            end

            # NB: Assumes perfect type stability
            if T == Int
                c.data[i], wasparsed, c.na[i] =
                  bytestoint(p.bytes, left, right,
                             o.nastrings)
            elseif T == Float64
                c.data[i], wasparsed, c.na[i] =
                  bytestofloat(p.bytes, left, right,
                               o.nastrings)
            elseif T == Bool
                c.data[i], wasparsed, c.na[i] =
                  bytestobool(p.bytes, left, right,
                              o.nastrings, o.truestrings, o.falsestrings)
            elseif T == UTF8String
                c.data[i], wasparsed, c.na[i] =
                  bytestostring(p.bytes, left, right,
                                wasquoted, o.nastrings)
            else
                error("Invalid type encountered")
            end
        end
    end

    return
end

##############################################################################
#
# Text output
#
##############################################################################

function printtable(io::IO,
                    df::DataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"')
    n, p = size(df)
    ctypes = coltypes(df)
    if header
        column_names = colnames(df)
        for j in 1:p
            print(io, quotemark)
            print(io, column_names[j])
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
            if ! (ctypes[j] <: Real)
                print(io, quotemark)
                print(io, df[i, j])
                print(io, quotemark)
            else
                print(io, df[i, j]) #column_names[j])
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

function printtable(df::DataFrame;
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
                    df::DataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"')
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
                        df::DataFrame)
    n = size(df, 1)
    column_names = colnames(df)
    write(io, "<table>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in column_names
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    for row in 1:n
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in column_names
            cell = string(df[row, column_name])
            write(io, "<td>$(html_escape(cell))</td>")
        end
        write(io, "</tr>")
    end
    write(io, "</table>")
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

# MIME

function Base.writemime(io::IO,
                        ::MIME"text/csv",
                        df::DataFrame)
    printtable(io, df, true, ',')
end

function Base.writemime(io::IO,
                        ::MIME"text/tab-separated-values",
                        df::DataFrame)
    printtable(io, df, true, '\t')
end
