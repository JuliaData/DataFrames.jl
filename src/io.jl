function readnrows!(io::IO,
                    buffer::Vector{Uint8},
                    eol_indices::Vector{Int},
                    separator_indices::Vector{Int},
                    nrows::Int,
                    eol::Char,
                    separator::Char,
                    quotemark::Char)
    bytesread::Int = 0
    nrowsread::Int = 0
    eolsread::Int = 0
    separatorsread::Int = 0

    inquotes::Bool = false
    inescape::Bool = false

    chr::Uint8 = ' '

    buffer_size::Int = length(buffer)
    eol_size::Int = length(eol_indices)
    separator_size::Int = length(separator_indices)

    while !eof(io) && nrowsread < nrows
        bytesread += 1
        chr = read(io, Uint8)

        if buffer_size < bytesread
            buffer_size *= 2
            resize!(buffer, buffer_size)
        end
        buffer[bytesread] = chr

        if !inquotes
            if chr == eol
                nrowsread +=1
                eolsread +=1
                if eol_size < eolsread
                    eol_size *= 2
                    resize!(eol_indices, eol_size)
                end
                eol_indices[eolsread] = bytesread
            end

            if chr == separator
                separatorsread += 1
                if separator_size < separatorsread
                    separator_size *= 2
                    resize!(separator_indices, separator_size)
                end
                separator_indices[separatorsread] = bytesread
            end

            if chr == quotemark && !inescape
                inquotes = true
            end
        else
            if chr == quotemark && !inescape
                inquotes = false
            end
        end

        if chr == '\\'
            inescape = true
        else
            inescape = false
        end
    end

    return nrowsread, bytesread, eolsread, separatorsread
end

function buffermatch(buffer::Vector{Uint8},
                     left::Int,
                     right::Int,
                     exemplars::Vector{ASCIIString})
    l::Int = right - left + 1

    for index in 1:length(exemplars)
        exemplar::ASCIIString = exemplars[index]
        if length(exemplar) == l
            isamatch::Bool = true

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

# All of these functions return three items:
# Parsed value, Success indicator, Missing indicator

# TODO: Align more closely with parseint code
function bytestoint(buffer::Vector{Uint8},
                    left::Int,
                    right::Int,
                    missing_nonstrings::Vector{ASCIIString})
    if buffermatch(buffer, left, right, missing_nonstrings)
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
    function bytestofloat(buffer::Vector{Uint8},
                          left::Int,
                          right::Int,
                          missing_nonstrings::Vector{ASCIIString})
        if buffermatch(buffer, left, right, missing_nonstrings)
            return 0.0, true, true
        end

        success = ccall(:jl_substrtod,
                        Int32,
                        (Ptr{Uint8}, Int, Int, Ptr{Float64}),
                        buffer,
                        left - 1,
                        right - left + 1,
                        out) == 0

        return out[1], success, false
    end
end

function bytestobool(buffer::Vector{Uint8},
                     left::Int,
                     right::Int,
                     missing_nonstrings::Vector{ASCIIString},
                     true_strings::Vector{ASCIIString},
                     false_strings::Vector{ASCIIString})
    if buffermatch(buffer, left, right, missing_nonstrings)
        return false, true, true
    end

    if buffermatch(buffer, left, right, true_strings)
        return true, true, false
    elseif buffermatch(buffer, left, right, false_strings)
        return false, true, false
    else
        return false, false, false
    end
end

function bytestostring(buffer::Vector{Uint8},
                       left::Int,
                       right::Int,
                       missing_strings::Vector{ASCIIString})
    if buffermatch(buffer, left, right, missing_strings)
        return "", true, true
    end

    return bytestring(buffer[left:right]), true, false
end

function builddf(rows::Int,
                 cols::Int,
                 bytes::Int,
                 eols::Int,
                 separators::Int,
                 buffer::Vector{Uint8},
                 eol_indices::Vector{Int},
                 separator_indices::Vector{Int},
                 separator::Char,
                 eol::Char,
                 quotemark::Char,
                 missing_nonstrings::Vector{ASCIIString},
                 missing_strings::Vector{ASCIIString},
                 true_strings::Vector{ASCIIString},
                 false_strings::Vector{ASCIIString},
                 ignorespace::Bool,
                 makefactors::Bool)
    columns::Vector{Any} = Array(Any, cols)

    for j in 1:cols
        values = Array(Int, rows)
        missing::BitVector = falses(rows)
        isint::Bool = true
        isfloat::Bool = true
        isbool::Bool = true
        isstring::Bool = true

        i::Int = 0

        while i < rows
            i += 1

            # Determine left and right boundaries of field
            if j == 1
                if i == 1
                    left = 1
                else
                    left = eol_indices[i - 1] + 1
                end
            else
                left = separator_indices[(i - 1) * (cols - 1) + j - 1] + 1
            end

            if j == cols
                if i == rows
                    if buffer[bytes] == eol
                        right = bytes - 1
                    else
                        right = bytes
                    end
                else
                    right = eol_indices[i] - 1
                end
            else
                right = separator_indices[(i - 1) * (cols - 1) + j] - 1
            end

            # Ignore left-and-right whitespace padding
            if ignorespace
                while left < right && buffer[left] == ' '
                    left += 1
                end
                while left < right && buffer[right] == ' '
                    right -= 1
                end
            end

            # (1) Try to parse values as Int's
            if isint
                values[i], success, missing[i] =
                  bytestoint(buffer, left, right, missing_nonstrings)
                if success
                    continue
                else
                    isint = false
                    values = convert(Array{Float64}, values)
                end
            end

            # (2) Try to parse as Float64's
            if isfloat
                values[i], success, missing[i] =
                  bytestofloat(buffer, left, right, missing_nonstrings)
                if success
                    continue
                else
                    isfloat = false
                    values = Array(Bool, rows)
                    i = 1
                end
            end

            # If we go this far, we should ignore quote marks on the boundaries
            while left < right && buffer[left] == quotemark
                left += 1
            end
            while left < right && buffer[right] == quotemark
                right -= 1
            end

            # (3) Try to parse as Bool's
            if isbool
                values[i], success, missing[i] =
                  bytestobool(buffer, left, right,
                              missing_nonstrings,
                              true_strings, false_strings)
                if success
                    continue
                else
                    isbool = false
                    values = Array(UTF8String, rows)
                    i = 1
                end
            end

            # (4) Fallback to UTF8String
            if left == right && buffer[right] == quotemark
                # Empty string special method
                values[i], success, missing[i] = "", true, false
            else
                values[i], success, missing[i] =
                  bytestostring(buffer, left, right, missing_strings)
            end
        end

        if makefactors && isstring
            columns[j] = PooledDataArray(values, missing)
        else
            columns[j] = DataArray(values, missing)
        end
    end

    # Need to pass this in
    column_names = DataFrames.generate_column_names(cols)

    return DataFrame(columns, column_names)
end

function parseline(buffer::Vector{Uint8},
                   upper::Int,
                   eol::Char,
                   separator::Char,
                   quotemark::Char)
    column_names = Array(UTF8String, 0)
    if upper == 1
        return column_names
    end

    left::Int = 1
    right::Int = -1
    index::Int = -1
    atbound::Bool = false
    inquotes::Bool = false
    inescape::Bool = false
    chr::Uint8 = uint8(' ')

    while left < upper
        chr = buffer[left]
        while chr == ' '
            left += 1
            chr = buffer[left]
        end

        right = left - 1

        atbound = false
        while right < upper && !atbound
            right += 1
            chr = buffer[right]
            if !inquotes
                if chr == separator
                    atbound = true
                elseif chr == quotemark && !inescape
                    inquotes = true
                end
            else
                if chr == quotemark && !inescape
                    inquotes = false
                end
            end

            if chr == '\\'
                inescape = true
            else
                inescape = false
            end
        end

        inquotes = false
        inescape = false

        if buffer[left] == quotemark
            left += 1
        end

        index = right
        chr = buffer[index]
        while index > left &&
              (chr == ' ' || chr == eol ||
               chr == separator || chr == quotemark)
            index -= 1
            chr = buffer[index]
        end

        push!(column_names, bytestring(buffer[left:index]))

        left = right + 1
    end

    return column_names
end

# TODO: Respect coltypes
# TODO: Skip blanklines
# TODO: Skip comment lines
# TODO: Use file encoding information
function readtable(io::IO;
                   header::Bool = true,
                   separator::Char = ',',
                   eol::Char = '\n',
                   quotemark::Char = '"',
                   missing_nonstrings::Vector{ASCIIString} = ["", "NA"],
                   missing_strings::Vector{ASCIIString} = ["NA"],
                   true_strings::Vector{ASCIIString} = ["T", "t", "TRUE", "true"],
                   false_strings::Vector{ASCIIString} = ["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   ignorespace::Bool = true,
                   decimal::Char = '.',
                   colnames::Vector{UTF8String} = Array(UTF8String, 0),
                   coltypes::Vector{Any} = Array(Any, 0),
                   nrows::Int = -1,
                   skipstartlines::Int = 0,
                   cleancolnames::Bool = true,
                   skipblanklines::Bool = true,
                   comment::Char = '#',
                   encoding::Symbol = :utf8)

    # Allocate buffers to conserve memory
    buffer::Vector{Uint8} = Array(Uint8, 2^20)
    eol_indices::Vector{Int} = Array(Int, 1)
    separator_indices::Vector{Int} = Array(Int, 1)
    chr::Uint8 = uint8(' ')

    # Skip lines at the start
    skipped_lines::Int = 0
    while skipped_lines < skipstartlines
        while chr != eol
            chr = read(io, Uint8)
        end
        skipped_lines += 1
    end

    # Deal with header
    if header
        chr = uint8(' ')
        headerbytesread = 0
        headerbytes = Array(Uint8, 2^16)
        headerbytes_size = length(headerbytes)
        while chr != eol
            chr = read(io, Uint8)
            headerbytesread += 1
            if headerbytesread > headerbytes_size
                headerbytes_size *= 2
                resize!(headerbytes, headerbytes_size)
            end
            headerbytes[headerbytesread] = chr
        end
        column_names =
          parseline(headerbytes, headerbytesread, eol, separator, quotemark)
    end

    # Separate text into fields
    rows, bytes, eols, separators =
      readnrows!(io,
                 buffer,
                 eol_indices,
                 separator_indices,
                 nrows,
                 eol,
                 separator,
                 quotemark)

    # Determine the number of columns
    cols = fld(separators, rows) + 1

    # Confirm that the number of columns is consistent across rows
    if rem(separators, rows) != 0
        error(@sprintf "Every line must have %d columns" cols)
    end

    # Parse contents of a buffer into a DataFrame
    df = builddf(rows,
                 cols,
                 bytes,
                 eols,
                 separators,
                 buffer,
                 eol_indices,
                 separator_indices,
                 separator,
                 eol,
                 quotemark,
                 missing_nonstrings,
                 missing_strings,
                 true_strings,
                 false_strings,
                 ignorespace,
                 makefactors)

    # Set up column names based on user input and header
    if isempty(colnames)
        if header
            colnames!(df, column_names)
        end
    else
        colnames!(df, colnames)
    end

    # Clean up column names if requested
    if cleancolnames
        clean_colnames!(df)
    end

    # Return the final DataFrame
    return df
end

function readtable(filename::String;
                   header::Bool = true,
                   separator::Char = getseparator(filename),
                   eol::Char = '\n',
                   quotemark::Char = '"',
                   missing_nonstrings::Vector{ASCIIString} = ["", "NA"],
                   missing_strings::Vector{ASCIIString} = ["NA"],
                   true_strings::Vector{ASCIIString} = ["T", "t", "TRUE", "true"],
                   false_strings::Vector{ASCIIString} = ["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   ignorespace::Bool = true,
                   decimal::Char = '.',
                   colnames::Vector{UTF8String} = Array(UTF8String, 0),
                   coltypes::Vector{Any} = Array(Any, 0),
                   nrows::Int = -1,
                   skipstartlines::Int = 0,
                   cleancolnames::Bool = false,
                   skipblanklines::Bool = true,
                   comment::Char = '#',
                   encoding::Symbol = :utf8)

    # Open an IO stream
    io = open(filename, "r")

    # If user wants all rows, overestimate nrows
    if nrows == -1
        nrows = filesize(filename)
    end

    # Use the IO stream method for readtable()
    df = readtable(io,
                   header = header,
                   separator = separator,
                   eol = eol,
                   quotemark = quotemark,
                   missing_nonstrings = missing_nonstrings,
                   missing_strings = missing_strings,
                   true_strings = true_strings,
                   false_strings = false_strings,
                   makefactors = makefactors,
                   ignorespace = ignorespace,
                   decimal = decimal,
                   colnames = colnames,
                   coltypes = coltypes,
                   nrows = nrows,
                   skipstartlines = skipstartlines,
                   cleancolnames = cleancolnames,
                   skipblanklines = cleancolnames,
                   comment = comment,
                   encoding = encoding)

    # Close the IO stream
    close(io)

    # Return the resulting DataFrame
    return df
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
    printtable(OUTPUT_STREAM,
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
