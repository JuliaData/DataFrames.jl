immutable ParsedCSV
    bytes::Vector{UInt8} # Raw bytes from CSV file
    bounds::Vector{Int}  # Right field boundary indices
    lines::Vector{Int}   # Line break indices
    quoted::BitVector    # Was field quoted in text
end

immutable ParseOptions{S <: String, T <: String}
    header::Bool
    separator::Char
    quotemarks::Vector{Char}
    decimal::Char
    nastrings::Vector{S}
    truestrings::Vector{T}
    falsestrings::Vector{T}
    makefactors::Bool
    names::Vector{Symbol}
    eltypes::Vector
    allowcomments::Bool
    commentmark::Char
    ignorepadding::Bool
    skipstart::Int
    skiprows::AbstractVector{Int}
    skipblanks::Bool
    encoding::Symbol
    allowescapes::Bool
    normalizenames::Bool
end

# Dispatch on values of ParseOptions to avoid running
#   unused checks for every byte read
immutable ParseType{ALLOWCOMMENTS, SKIPBLANKS, ALLOWESCAPES, SPC_SEP} end
ParseType(o::ParseOptions) = ParseType{o.allowcomments, o.skipblanks, o.allowescapes, o.separator == ' '}()

macro read_peek_eof(io, nextchr)
    io = esc(io)
    nextchr = esc(nextchr)
    quote
        nextnext = eof($io) ? 0xff : read($io, UInt8)
        $nextchr, nextnext, nextnext == 0xff
    end
end

macro skip_within_eol(io, chr, nextchr, endf)
    io = esc(io)
    chr = esc(chr)
    nextchr = esc(nextchr)
    endf = esc(endf)
    quote
        if $chr == UInt32('\r') && $nextchr == UInt32('\n')
            $chr, $nextchr, $endf = @read_peek_eof($io, $nextchr)
        end
    end
end

macro skip_to_eol(io, chr, nextchr, endf)
    io = esc(io)
    chr = esc(chr)
    nextchr = esc(nextchr)
    endf = esc(endf)
    quote
        while !$endf && !@atnewline($chr, $nextchr)
            $chr, $nextchr, $endf = @read_peek_eof($io, $nextchr)
        end
        @skip_within_eol($io, $chr, $nextchr, $endf)
    end
end

macro atnewline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == UInt32('\n') || $chr == UInt32('\r')
    end
end

macro atblankline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        ($chr == UInt32('\n') || $chr == UInt32('\r')) &&
        ($nextchr == UInt32('\n') || $nextchr == UInt32('\r'))
    end
end

macro atescape(chr, nextchr, quotemarks)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quotemarks = esc(quotemarks)
    quote
        (UInt32($chr) == UInt32('\\') &&
            (UInt32($nextchr) == UInt32('\\') ||
                UInt32($nextchr) in $quotemarks)) ||
                    (UInt32($chr) == UInt32($nextchr) &&
                        UInt32($chr) in $quotemarks)
    end
end

macro atcescape(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == UInt32('\\') &&
        ($nextchr == UInt32('n') ||
         $nextchr == UInt32('t') ||
         $nextchr == UInt32('r') ||
         $nextchr == UInt32('a') ||
         $nextchr == UInt32('b') ||
         $nextchr == UInt32('f') ||
         $nextchr == UInt32('v') ||
         $nextchr == UInt32('\\'))
    end
end

macro mergechr(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        if $chr == UInt32('\\')
            if $nextchr == UInt32('n')
                '\n'
            elseif $nextchr == UInt32('t')
                '\t'
            elseif $nextchr == UInt32('r')
                '\r'
            elseif $nextchr == UInt32('a')
                '\a'
            elseif $nextchr == UInt32('b')
                '\b'
            elseif $nextchr == UInt32('f')
                '\f'
            elseif $nextchr == UInt32('v')
                '\v'
            elseif $nextchr == UInt32('\\')
                '\\'
            else
                msg = @sprintf("Invalid escape character '%s%s' encountered",
                               $chr,
                               $nextchr)
                error(msg)
            end
        else
            msg = @sprintf("Invalid escape character '%s%s' encountered",
                           $chr,
                           $nextchr)
            error(msg)
        end
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

function getseparator(filename::AbstractString)
    m = match(r"\.(\w+)(\.(gz|bz|bz2))?$", filename)
    ext = isa(m, RegexMatch) ? m.captures[1] : ""
    if ext == "csv"
        return ','
    elseif ext == "tsv"
        return '\t'
    elseif ext == "wsv"
        return ' '
    else
        return ','
    end
end

tf = (true, false)
for allowcomments in tf, skipblanks in tf, allowescapes in tf, wsv in tf
    dtype = ParseType{allowcomments, skipblanks, allowescapes, wsv}
    @eval begin
        # Read CSV file's rows into buffer while storing field boundary information
        # TODO: Experiment with mmaping input
        function readnrows!(p::ParsedCSV,
                            io::IO,
                            nrows::Integer,
                            o::ParseOptions,
                            dispatcher::$(dtype),
                            firstchr::UInt8=0xff)
            # TODO: Use better variable names
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
            $(if allowcomments quote at_start = true end end)
            $(if wsv quote skip_white = true end end)
            chr = 0xff
            nextchr = (firstchr == 0xff && !eof(io)) ? read(io, UInt8) : firstchr
            endf = nextchr == 0xff

            # 'in' does not work if passed UInt8 and Vector{Char}
            quotemarks = convert(Vector{UInt8}, o.quotemarks)

            # Insert a dummy field bound at position 0
            @push(n_bounds, p.bounds, 0, l_bounds)
            @push(n_bytes, p.bytes, '\n', l_bytes)
            @push(n_lines, p.lines, 0, l_lines)

            # Loop over bytes from the input until we've read requested rows
            while !endf && ((nrows == -1) || (n_lines < nrows + 1))

                chr, nextchr, endf = @read_peek_eof(io, nextchr)

                # === Debugging ===
                # if in_quotes
                #     print_with_color(:red, string(char(chr)))
                # else
                #     print_with_color(:green, string(char(chr)))
                # end

                $(if allowcomments
                    quote
                        # Ignore text inside comments completely
                        if !in_quotes && chr == UInt32(o.commentmark)
                            @skip_to_eol(io, chr, nextchr, endf)

                            # Skip the linebreak if the comment began at the start of a line
                            if at_start
                                continue
                            end
                        end
                    end
                end)

                $(if skipblanks
                    quote
                        # Skip blank lines
                        if !in_quotes
                            while !endf && @atblankline(chr, nextchr)
                                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                                @skip_within_eol(io, chr, nextchr, endf)
                            end
                        end
                    end
                end)

                $(if allowescapes
                    quote
                        # Merge chr and nextchr here if they're a c-style escape
                        if @atcescape(chr, nextchr) && !in_escape
                            chr = @mergechr(chr, nextchr)
                            nextchr = eof(io) ? 0xff : read(io, UInt8)
                            endf = nextchr == 0xff
                            in_escape = true
                        end
                    end
                end)

                # No longer at the start of a line that might be a pure comment
                $(if allowcomments quote at_start = false end end)

                # Processing is very different inside and outside of quotes
                if !in_quotes
                    # Entering a quoted region
                    if chr in quotemarks
                        in_quotes = true
                        p.quoted[n_fields] = true
                        $(if wsv quote skip_white = false end end)
                    # Finished reading a field
                    elseif $(if wsv
                                quote chr == UInt32(' ') || chr == UInt32('\t') end
                            else
                                quote chr == UInt32(o.separator) end
                            end)
                        $(if wsv
                            quote
                                if !(nextchr in UInt32[' ', '\t', '\n', '\r']) && !skip_white
                                    @push(n_bounds, p.bounds, n_bytes, l_bounds)
                                    @push(n_bytes, p.bytes, '\n', l_bytes)
                                    @push(n_fields, p.quoted, false, l_quoted)
                                    skip_white = false
                                end
                            end
                        else
                            quote
                                @push(n_bounds, p.bounds, n_bytes, l_bounds)
                                @push(n_bytes, p.bytes, '\n', l_bytes)
                                @push(n_fields, p.quoted, false, l_quoted)
                            end
                        end)
                    # Finished reading a row
                    elseif @atnewline(chr, nextchr)
                        @skip_within_eol(io, chr, nextchr, endf)
                        $(if allowcomments quote at_start = true end end)
                        @push(n_bounds, p.bounds, n_bytes, l_bounds)
                        @push(n_bytes, p.bytes, '\n', l_bytes)
                        @push(n_lines, p.lines, n_bytes, l_lines)
                        @push(n_fields, p.quoted, false, l_quoted)
                        $(if wsv quote skip_white = true end end)
                    # Store character in buffer
                    else
                        @push(n_bytes, p.bytes, chr, l_bytes)
                        $(if wsv quote skip_white = false end end)
                    end
                else
                    # Escape a quotemark inside quoted field
                    if @atescape(chr, nextchr, quotemarks) && !in_escape
                        in_escape = true
                    else
                        # Exit quoted field
                        if UInt32(chr) in quotemarks && !in_escape
                            in_quotes = false
                        # Store character in buffer
                        else
                            @push(n_bytes, p.bytes, chr, l_bytes)
                        end
                        # Escape mode only lasts for one byte
                        in_escape = false
                    end
                end
            end

            # Append a final EOL if it's missing in the raw input
            if endf && !@atnewline(chr, nextchr)
                @push(n_bounds, p.bounds, n_bytes, l_bounds)
                @push(n_bytes, p.bytes, '\n', l_bytes)
                @push(n_lines, p.lines, n_bytes, l_lines)
            end

            # Don't count the dummy boundaries in fields or rows
            return n_bytes, n_bounds - 1, n_lines - 1, nextchr
        end
    end
end

function bytematch{T <: String}(bytes::Vector{UInt8},
                                    left::Integer,
                                    right::Integer,
                                    exemplars::Vector{T})
    l = right - left + 1
    for index in 1:length(exemplars)
        exemplar = exemplars[index]
        if length(exemplar) == l
            matched = true
            for i in 0:(l - 1)
                matched &= bytes[left + i] == UInt32(exemplar[1 + i])
            end
            if matched
                return true
            end
        end
    end
    return false
end

function bytestotype{N <: Integer,
                     T <: String,
                     P <: String}(::Type{N},
                                  bytes::Vector{UInt8},
                                  left::Integer,
                                  right::Integer,
                                  nastrings::Vector{T},
                                  wasquoted::Bool = false,
                                  truestrings::Vector{P} = P[],
                                  falsestrings::Vector{P} = P[])
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
        if UInt32('0') <= byte <= UInt32('9')
            value += (byte - UInt8('0')) * power
            power *= 10
        else
            return value, false, false
        end
        index -= 1
        byte = bytes[index]
    end

    if byte == UInt32('-')
        return -value, left < right, false
    elseif byte == UInt32('+')
        return value, left < right, false
    elseif UInt32('0') <= byte <= UInt32('9')
        value += (byte - UInt8('0')) * power
        return value, true, false
    else
        return value, false, false
    end
end

let out = Vector{Float64}(1)
    global bytestotype
    function bytestotype{N <: AbstractFloat,
                         T <: String,
                         P <: String}(::Type{N},
                                      bytes::Vector{UInt8},
                                      left::Integer,
                                      right::Integer,
                                      nastrings::Vector{T},
                                      wasquoted::Bool = false,
                                      truestrings::Vector{P} = P[],
                                      falsestrings::Vector{P} = P[])
        if left > right
            return 0.0, true, true
        end

        if bytematch(bytes, left, right, nastrings)
            return 0.0, true, true
        end

        wasparsed = ccall(:jl_substrtod,
                          Int32,
                          (Ptr{UInt8}, Csize_t, Int, Ptr{Float64}),
                          bytes,
                          convert(Csize_t, left - 1),
                          right - left + 1,
                          out) == 0

        return out[1], wasparsed, false
    end
end

function bytestotype{N <: Bool,
                     T <: String,
                     P <: String}(::Type{N},
                                  bytes::Vector{UInt8},
                                  left::Integer,
                                  right::Integer,
                                  nastrings::Vector{T},
                                  wasquoted::Bool = false,
                                  truestrings::Vector{P} = P[],
                                  falsestrings::Vector{P} = P[])
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

function bytestotype{N <: AbstractString,
                     T <: String,
                     P <: String}(::Type{N},
                                  bytes::Vector{UInt8},
                                  left::Integer,
                                  right::Integer,
                                  nastrings::Vector{T},
                                  wasquoted::Bool = false,
                                  truestrings::Vector{P} = P[],
                                  falsestrings::Vector{P} = P[])
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

    return String(bytes[left:right]), true, false
end

function builddf(rows::Integer,
                 cols::Integer,
                 bytes::Integer,
                 fields::Integer,
                 p::ParsedCSV,
                 o::ParseOptions)
    columns = Vector{Any}(cols)

    for j in 1:cols
        if isempty(o.eltypes)
            values = Vector{Int}(rows)
        else
            values = Vector{o.eltypes[j]}(rows)
        end

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
            # TODO: Modify readnrows() so that '\r' and '\n'
            #       don't occur near edges
            if o.ignorepadding && !wasquoted
                while left < right && @isspace(p.bytes[left])
                    left += 1
                end
                while left <= right && @isspace(p.bytes[right])
                    right -= 1
                end
            end

            # If eltypes has been defined, use it
            if !isempty(o.eltypes)
                values[i], wasparsed, missing[i] =
                    bytestotype(o.eltypes[j],
                                p.bytes,
                                left,
                                right,
                                o.nastrings,
                                wasquoted,
                                o.truestrings,
                                o.falsestrings)

                # Don't go to guess type zone
                if wasparsed
                    continue
                else
                    error(@sprintf("Failed to parse '%s' using type '%s'",
                                   String(p.bytes[left:right]),
                                   o.eltypes[j]))
                end
            end

            # (1) Try to parse values as Int's
            if is_int
                values[i], wasparsed, missing[i] =
                  bytestotype(Int64,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
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
                  bytestotype(Float64,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
                if wasparsed
                    continue
                else
                    is_float = false
                    values = Vector{Bool}(rows)
                    i = 0
                    continue
                end
            end

            # (3) Try to parse as Bool's
            if is_bool
                values[i], wasparsed, missing[i] =
                  bytestotype(Bool,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
                if wasparsed
                    continue
                else
                    is_bool = false
                    values = Vector{String}(rows)
                    i = 0
                    continue
                end
            end

            # (4) Fallback to String
            values[i], wasparsed, missing[i] =
              bytestotype(String,
                          p.bytes,
                          left,
                          right,
                          o.nastrings,
                          wasquoted,
                          o.truestrings,
                          o.falsestrings)
        end

        if o.makefactors && !(is_int || is_float || is_bool)
            columns[j] = PooledDataArray(values, missing)
        else
            columns[j] = DataArray(values, missing)
        end
    end

    if isempty(o.names)
        return DataFrame(columns, gennames(cols))
    else
        return DataFrame(columns, o.names)
    end
end

function parsenames!(names::Vector{Symbol},
                     ignorepadding::Bool,
                     bytes::Vector{UInt8},
                     bounds::Vector{Int},
                     quoted::BitVector,
                     fields::Int,
                     normalizenames::Bool)
    if fields == 0
        error("Header line was empty")
    end

    resize!(names, fields)

    for j in 1:fields
        left = bounds[j] + 2
        right = bounds[j + 1]

        if ignorepadding && !quoted[j]
            while left < right && @isspace(bytes[left])
                left += 1
            end
            while left <= right && @isspace(bytes[right])
                right -= 1
            end
        end

        name = String(bytes[left:right])
        if normalizenames
            name = identifier(name)
        end

        names[j] = name
    end

    return
end

function findcorruption(rows::Integer,
                        cols::Integer,
                        fields::Integer,
                        p::ParsedCSV)
    n = length(p.bounds)
    lengths = Vector{Int}(rows)
    t = 1
    for i in 1:rows
        bound = p.lines[i + 1]
        f = 0
        while t <= n && p.bounds[t] < bound
            f += 1
            t += 1
        end
        lengths[i] = f
    end
    m = median(lengths)
    corruptrows = find(lengths .!= m)
    l = corruptrows[1]
    error(@sprintf("Saw %d rows, %d columns and %d fields\n * Line %d has %d columns\n",
                   rows,
                   cols,
                   fields,
                   l,
                   lengths[l] + 1))
end

function readtable!(p::ParsedCSV,
                    io::IO,
                    nrows::Integer,
                    o::ParseOptions)

    chr, nextchr = 0xff, 0xff

    skipped_lines = 0

    # Skip lines at the start
    if o.skipstart != 0
        while skipped_lines < o.skipstart
            chr, nextchr, endf = @read_peek_eof(io, nextchr)
            @skip_to_eol(io, chr, nextchr, endf)
            skipped_lines += 1
        end
    else
        chr, nextchr, endf = @read_peek_eof(io, nextchr)
    end

    if o.allowcomments || o.skipblanks
        while true
            if o.allowcomments && nextchr == UInt32(o.commentmark)
                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                @skip_to_eol(io, chr, nextchr, endf)
            elseif o.skipblanks && @atnewline(nextchr, nextchr)
                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                @skip_within_eol(io, chr, nextchr, endf)
            else
                break
            end
            skipped_lines += 1
        end
    end

    # Use ParseOptions to pick the right method of readnrows!
    d = ParseType(o)

    # Extract the header
    if o.header
        bytes, fields, rows, nextchr = readnrows!(p, io, Int64(1), o, d, nextchr)

        # Insert column names from header if none present
        if isempty(o.names)
            parsenames!(o.names, o.ignorepadding, p.bytes, p.bounds, p.quoted, fields, o.normalizenames)
        end
    end

    # Parse main data set
    bytes, fields, rows, nextchr = readnrows!(p, io, Int64(nrows), o, d, nextchr)

    # Sanity checks
    bytes != 0 || error("Failed to read any bytes.")
    rows != 0 || error("Failed to read any rows.")
    fields != 0 || error("Failed to read any fields.")

    # Determine the number of columns
    cols = fld(fields, rows)

    # if the file is empty but has a header then fields, cols and rows will not be computed correctly
    if length(o.names) != cols && cols == 1 && rows == 1 && fields == 1 && bytes == 2
        fields = 0
        rows = 0
        cols = length(o.names)
    end

    # Confirm that the number of columns is consistent across rows
    if fields != rows * cols
        findcorruption(rows, cols, fields, p)
    end

    # Parse contents of a buffer into a DataFrame
    df = builddf(rows, cols, bytes, fields, p, o)

    # Return the final DataFrame
    return df
end

function readtable(io::IO,
                   nbytes::Integer = 1;
                   header::Bool = true,
                   separator::Char = ',',
                   quotemark::Vector{Char} = ['"'],
                   decimal::Char = '.',
                   nastrings::Vector = ["", "NA"],
                   truestrings::Vector = ["T", "t", "TRUE", "true"],
                   falsestrings::Vector = ["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Integer = -1,
                   names::Vector = Symbol[],
                   eltypes::Vector = [],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Integer = 0,
                   skiprows::AbstractVector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8,
                   allowescapes::Bool = false,
                   normalizenames::Bool = true)
    if encoding != :utf8
        throw(ArgumentError("Argument 'encoding' only supports ':utf8' currently."))
    elseif !isempty(skiprows)
        throw(ArgumentError("Argument 'skiprows' is not yet supported."))
    elseif decimal != '.'
        throw(ArgumentError("Argument 'decimal' is not yet supported."))
    end

    if !isempty(eltypes)
        for j in 1:length(eltypes)
            if !(eltypes[j] in [String, Bool, Float64, Int64])
                throw(ArgumentError("Invalid eltype $(eltypes[j]) encountered.\nValid eltypes: $(String), Bool, Float64 or Int64"))
            end
        end
    end

    # Allocate buffers for storing metadata
    p = ParsedCSV(Vector{UInt8}(nbytes),
                   Vector{Int}(1),
                   Vector{Int}(1),
                  BitArray(1))

    # Set parsing options
    o = ParseOptions(header, separator, quotemark, decimal,
                     nastrings, truestrings, falsestrings,
                     makefactors, names, eltypes,
                     allowcomments, commentmark, ignorepadding,
                     skipstart, skiprows, skipblanks, encoding,
                     allowescapes, normalizenames)

    # Use the IO stream method for readtable()
    df = readtable!(p, io, nrows, o)

    # Close the IO stream
    close(io)

    # Return the resulting DataFrame
    return df
end

"""
Read data from a tabular-file format (CSV, TSV, ...)

```julia
readtable(filename, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be read

### Keyword Arguments

*   `header::Bool` -- Use the information from the file's header line to determine column names. Defaults to `true`.
*   `separator::Char` -- Assume that fields are split by the `separator` character. If not specified, it will be guessed from the filename: `.csv` defaults to `','`, `.tsv` defaults to `'\t'`, `.wsv` defaults to `' '`.
*   `quotemark::Vector{Char}` -- Assume that fields contained inside of two `quotemark` characters are quoted, which disables processing of separators and linebreaks. Set to `Char[]` to disable this feature and slightly improve performance. Defaults to `['"']`.
*   `decimal::Char` -- Assume that the decimal place in numbers is written using the `decimal` character. Defaults to `'.'`.
*   `nastrings::Vector{String}` -- Translate any of the strings into this vector into an `NA`. Defaults to `["", "NA"]`.
*   `truestrings::Vector{String}` -- Translate any of the strings into this vector into a Boolean `true`. Defaults to `["T", "t", "TRUE", "true"]`.
*   `falsestrings::Vector{String}` -- Translate any of the strings into this vector into a Boolean `false`. Defaults to `["F", "f", "FALSE", "false"]`.
*   `makefactors::Bool` -- Convert string columns into `PooledDataVector`'s for use as factors. Defaults to `false`.
*   `nrows::Int` -- Read only `nrows` from the file. Defaults to `-1`, which indicates that the entire file should be read.
*   `names::Vector{Symbol}` -- Use the values in this array as the names for all columns instead of or in lieu of the names in the file's header. Defaults to `[]`, which indicates that the header should be used if present or that numeric names should be invented if there is no header.
*   `eltypes::Vector` -- Specify the types of all columns. Defaults to `[]`.
*   `allowcomments::Bool` -- Ignore all text inside comments. Defaults to `false`.
*   `commentmark::Char` -- Specify the character that starts comments. Defaults to `'#'`.
*   `ignorepadding::Bool` -- Ignore all whitespace on left and right sides of a field. Defaults to `true`.
*   `skipstart::Int` -- Specify the number of initial rows to skip. Defaults to `0`.
*   `skiprows::Vector{Int}` -- Specify the indices of lines in the input to ignore. Defaults to `[]`.
*   `skipblanks::Bool` -- Skip any blank lines in input. Defaults to `true`.
*   `encoding::Symbol` -- Specify the file's encoding as either `:utf8` or `:latin1`. Defaults to `:utf8`.
*   `normalizenames::Bool` -- Ensure that column names are valid Julia identifiers. For instance this renames a column named `"a b"` to `"a_b"` which can then be accessed with `:a_b` instead of `Symbol("a b")`. Defaults to `true`.

### Result

* `::DataFrame`

### Examples

```julia
df = readtable("data.csv")
df = readtable("data.tsv")
df = readtable("data.wsv")
df = readtable("data.txt", separator = '\t')
df = readtable("data.txt", header = false)
```
"""
function readtable(pathname::AbstractString;
                   header::Bool = true,
                   separator::Char = getseparator(pathname),
                   quotemark::Vector{Char} = ['"'],
                   decimal::Char = '.',
                   nastrings::Vector = String["", "NA"],
                   truestrings::Vector = String["T", "t", "TRUE", "true"],
                   falsestrings::Vector = String["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Integer = -1,
                   names::Vector = Symbol[],
                   eltypes::Vector = [],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Integer = 0,
                   skiprows::AbstractVector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8,
                   allowescapes::Bool = false,
                   normalizenames::Bool = true)
    # Open an IO stream based on pathname
    # (1) Path is an HTTP or FTP URL
    if startswith(pathname, "http://") || startswith(pathname, "ftp://")
        error("URL retrieval not yet implemented")
    # (2) Path is GZip file
    elseif endswith(pathname, ".gz")
        io = gzopen(pathname, "r")
        nbytes = 2 * filesize(pathname)
    # (3) Path is BZip2 file
    elseif endswith(pathname, ".bz") || endswith(pathname, ".bz2")
        error("BZip2 decompression not yet implemented")
    # (4) Path is an uncompressed file
    else
        io = open(pathname, "r")
        nbytes = filesize(pathname)
    end

    return readtable(io,
                     nbytes,
                     header = header,
                     separator = separator,
                     quotemark = quotemark,
                     decimal = decimal,
                     nastrings = nastrings,
                     truestrings = truestrings,
                     falsestrings = falsestrings,
                     makefactors = makefactors,
                     nrows = nrows,
                     names = names,
                     eltypes = eltypes,
                     allowcomments = allowcomments,
                     commentmark = commentmark,
                     ignorepadding = ignorepadding,
                     skipstart = skipstart,
                     skiprows = skiprows,
                     skipblanks = skipblanks,
                     encoding = encoding,
                     allowescapes = allowescapes,
                     normalizenames = normalizenames)
end

"""
    inlinetable(s[, flags]; args...)

A helper function to process strings as tabular data for non-standard string
literals. Parses the string `s` containing delimiter-separated tabular data
(by default, comma-separated values) using `readtable`. The optional `flags`
argument contains a list of flag characters, which, if present, are equivalent
to supplying named arguments to `readtable` as follows:

- `f`: `makefactors=true`, convert string columns to `PooledData` columns
- `c`: `allowcomments=true`, ignore lines beginning with `#`
- `H`: `header=false`, do not interpret the first line as column names
"""
inlinetable(s::AbstractString; args...) = readtable(IOBuffer(s); args...)
function inlinetable(s::AbstractString, flags::AbstractString; args...)
    flagbindings = Dict(
        'f' => (:makefactors, true),
        'c' => (:allowcomments, true),
        'H' => (:header, false) )
    for f in flags
        if haskey(flagbindings, f)
            push!(args, flagbindings[f])
        else
            throw(ArgumentError("Unknown inlinetable flag: $f"))
        end
    end
    readtable(IOBuffer(s); args...)
end

"""
    @csv_str(s[, flags])
    csv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing comma-
separated values (CSV) using `readtable`, just as if it were being loaded from
an external file. The suffix flags `f`, `c`, and `H` are optional. If present,
they are equivalent to supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `PooledDataArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = csv\"""
           name,  age, squidPerWeek
           Alice,  36,         3.14
           Bob,    24,         0
           Carol,  58,         2.71
           Eve,    49,         7.77
           \"""
4×3 DataFrames.DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro csv_str(s, flags...) inlinetable(s, flags...; separator=',') end

"""
    @csv2_str(s[, flags])
    csv2"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing
semicolon-separated values using `readtable`, with comma acting as the decimal
character, just as if it were being loaded from an external file. The suffix
flags `f`, `c`, and `H` are optional. If present, they are equivalent to
supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `PooledDataArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = csv2\"""
           name;  age; squidPerWeek
           Alice;  36;         3,14
           Bob;    24;         0
           Carol;  58;         2,71
           Eve;    49;         7,77
           \"""
4×3 DataFrames.DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro csv2_str(s, flags...)
    inlinetable(s, flags...; separator=';', decimal=',')
end

"""
    @wsv_str(s[, flags])
    wsv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing
whitespace-separated values (WSV) using `readtable`, just as if it were being
loaded from an external file. The suffix flags `f`, `c`, and `H` are optional.
If present, they are equivalent to supplying named arguments to `readtable` as
follows:

* `f`: `makefactors=true`, convert string columns to `PooledDataArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = wsv\"""
           name  age squidPerWeek
           Alice  36         3.14
           Bob    24         0
           Carol  58         2.71
           Eve    49         7.77
           \"""
4×3 DataFrames.DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro wsv_str(s, flags...) inlinetable(s, flags...; separator=' ') end

"""
    @tsv_str(s[, flags])
    tsv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing tab-
separated values (TSV) using `readtable`, just as if it were being loaded from
an external file. The suffix flags `f`, `c`, and `H` are optional. If present,
they are equivalent to supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `PooledDataArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = tsv\"""
           name\tage\tsquidPerWeek
           Alice\t36\t3.14
           Bob\t24\t0
           Carol\t58\t2.71
           Eve\t49\t7.77
           \"""
4×3 DataFrames.DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro tsv_str(s, flags...) inlinetable(s, flags...; separator='\t') end

function filldf!(df::DataFrame,
                 rows::Integer,
                 cols::Integer,
                 bytes::Integer,
                 fields::Integer,
                 p::ParsedCSV,
                 o::ParseOptions)
    etypes = eltypes(df)

    if rows != size(df, 1)
        for j in 1:cols
            resize!(df.columns[j].data, rows)
            resize!(df.columns[j].na, rows)
        end
    end

    for j in 1:cols
        c = df.columns[j]
        T = etypes[j]

        i = 0
        while i < rows
            i += 1

            # Determine left and right boundaries of field
            left = p.bounds[(i - 1) * cols + j] + 2
            right = p.bounds[(i - 1) * cols + j + 1]
            wasquoted = p.quoted[(i - 1) * cols + j]

            # Ignore left-and-right whitespace padding
            # TODO: Debate moving this into readnrows()
            # TODO: Modify readnrows() so that '\r' and '\n'
            #       don't occur near edges
            if o.ignorepadding && !wasquoted
                while left < right && @isspace(p.bytes[left])
                    left += 1
                end
                while left <= right && @isspace(p.bytes[right])
                    right -= 1
                end
            end

            # NB: Assumes perfect type stability
            # Use subtypes here
            if !(T in [Int, Float64, Bool, String])
                error("Invalid eltype encountered")
            end
            c.data[i], wasparsed, c.na[i] =
              bytestotype(T,
                          p.bytes,
                          left,
                          right,
                          o.nastrings,
                          wasquoted,
                          o.truestrings,
                          o.falsestrings)

            if !wasparsed
                error("Failed to parse entry")
            end
        end
    end

    return
end
