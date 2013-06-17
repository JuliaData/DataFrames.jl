const DEFAULT_BOOLEAN_STRINGS =
  ["T", "F", "t", "f", "TRUE", "FALSE", "true", "false"]
const DEFAULT_TRUE_STRINGS =
  ["T", "t", "TRUE", "true"]
const DEFAULT_FALSE_STRINGS =
  ["F", "f", "FALSE", "false"]

const DEFAULT_QUOTATION_CHARACTER = '"'
const DEFAULT_SEPARATOR = ','

const DEFAULT_MISSINGNESS_INDICATORS =
  ["", "NA", "#NA", "N/A", "#N/A", "NULL", "."]

const MISSINGNESS_STRING = utf8("0")

function parsebool(x::String)
    if contains(DEFAULT_TRUE_STRINGS, x)
        return true
    elseif contains(DEFAULT_FALSE_STRINGS, x)
        return false
    else
        error("Could not parse bool")
    end
end

##############################################################################
#
# Low-level text parsing
#
##############################################################################

function make_extract_string()
    # extract_cache = memio(500, false)
    extract_cache = memio(500, false)
    # Do we really need a closure?
    # Why not just keep passing this argument in?
    function f(this, left::Int, right::Int, omitlist::Set = Set())
        extract_cache_size = right - left
        if extract_cache_size > length(extract_cache.ios)
            extract_cache = memio(extract_cache_size, false)
        end
        seek(extract_cache, 0) # necessary?
        if length(this) >= 1
            while isvalid(this, right) && right > left && this[right] == ' '
                right -= 1
            end
            i = left
            while i <= right
                lasti = i
                ch, i = next(this, i)
                if !contains(omitlist, lasti)
                    print(extract_cache, ch)
                end
            end
            return takebuf_string(extract_cache)
        else
            return ""
        end
    end
    return f
end
extract_string = make_extract_string()

const STATE_EXPECTING_VALUE = 0
const STATE_IN_BARE = 1
const STATE_IN_QUOTED = 2
const STATE_POSSIBLE_EOQUOTED = 3
const STATE_EXPECTING_SEP = 4

# Read a line of text into a Uint8 buffer from a given starting position
# which defaults to 1 if not specified. Mutates inputs and returns
# new value of upper. The length of the buffer may be increased
# if needed to fit text.

# TODO: generalize to arbitrary newline signifiers
# '\r'

# Assumes that !eof(io)
function readline!(io::IO,
                   buffer::Vector{Uint8},
                   upper::Int = 1,
                   l::Int = length(buffer))
    cur = read(io, Uint8)
    buffer[upper] = cur
    while cur != '\n' && !eof(io)
        if upper == l
            l *= 2
            resize!(buffer, l)
        end
        while cur != '\n' && !eof(io) && upper < l
            upper += 1
            cur = read(io, Uint8)
            buffer[upper] = cur
        end
    end
    return upper, l
end

# TODO: Mechanism for traversing a single line of text to find fields
# Use this to determine number of columns
function findfields(buffer::Vector{Uint8},
                    separator::Char,
                    quotation_character::Char,
                    lower::Int,
                    upper::Int)
    # Field starting indices
    # Field ending indices
    i = lower
    while i < upper

    end
end

# Read one line of delimited text
# This is complex because delimited text can contain EOL inside quoted fields
function readfields!(io::IO,
                     buffer::Vector{Uint8}, # Character buffer
                     fields::Vector{UTF8String}, # Fields buffer
                     separator::Char,
                     quotation_character::Char)
    # Indexes into the current line for the current item
    left = 0
    right = 0

    upper = 1
    l = length(buffer)

    # Don't bother trying to read after EOF
    # TODO: Don't allocate a new array
    #       Just edit something in place
    if eof(io)
        return 0 # Return number of fields
    end

    upper, l = readline!(io, buffer, upper, l)

    # Short-circuit on empty lines
    # TODO: Don't allocate a new array
    #       Just edit something in place
    if upper == 1
        return 0 # Return number of fields
    end

    # 5-state machine. See list of possible states above
    state = STATE_EXPECTING_VALUE

    # Index of characters to remove
    omitlist = Set()

    # Where are we
    i = start(buffer) # Why not just use 1?
    eol = false

    # Will eventually return a Vector of strings
    # TODO: Don't do this.
    # TODO: Never call anything ret. It's super confusing viz. x86 ASM.
    # TODO: Deal with Latin-1 and other shitty encodings
    num_elems = 0
    n_fields = length(fields)

    # off we go! use manual loops because this can grow
    while true
        # eol = done(buffer, i)
        eol = i == upper
        if !eol
            this_i = i
            this_char, i = buffer[i], i + 1
        end
        if state == STATE_EXPECTING_VALUE
            if eol
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = utf8("")
                break
            elseif this_char == ' '
                continue
            elseif this_char == separator
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = utf8("")
            elseif this_char == quotation_character
                left = this_i + 1
                state = STATE_IN_QUOTED
            else
                left = this_i
                state = STATE_IN_BARE
            end
        elseif state == STATE_IN_BARE
            if eol
                right = this_i
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = bytestring(buffer[left:right])
                break
            elseif this_char == separator
                right = this_i - 1
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = bytestring(buffer[left:right])
                state = STATE_EXPECTING_VALUE
            else
                continue
            end
        elseif state == STATE_IN_QUOTED
            if eol
                # We saw a newline inside a quoted field
                # So we read in another line
                upper, l = readline!(io, buffer, upper, l)
            elseif this_char == quotation_character
                state = STATE_POSSIBLE_EOQUOTED
            else
                continue
            end
        elseif state == STATE_POSSIBLE_EOQUOTED
            if eol
                right = this_i - 1
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = bytestring(buffer[left:right])
                # Pass omitlist?
                break
            elseif this_char == quotation_character
                add!(omitlist, this_i)
                state = STATE_IN_QUOTED
            elseif this_char == separator
                right = this_i - 2
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = bytestring(buffer[left:right])
                # Remove omitlist
                empty!(omitlist)
                state = STATE_EXPECTING_VALUE
            elseif this_char == ' '
                right = this_i - 2
                num_elems += 1
                if num_elems > n_fields
                    n_fields *= 2
                    resize!(fields, n_fields)
                end
                fields[num_elems] = bytestring(buffer[left:right])
                # Remove omitlist
                empty!(omitlist)
                state = STATE_EXPECTING_SEP
            else
                error("unexpected character after a quote")
            end
        elseif state == STATE_EXPECTING_SEP
            if eol
                break
            elseif this_char == ' '
                continue
            elseif this_char == separator
                state = STATE_EXPECTING_VALUE
            else
                error("expecting a separator but got something else")
            end
        end
    end

    return num_elems
end

# Read data line-by-line
function read_separated_text(io::IO,
                             nrows::Int,
                             separator::Char,
                             quotation_character::Char)
    # Read one line to determine the number of columns
    i = 1
    buffer = Array(Uint8, 2^16)
    fields = Array(UTF8String, 2^16)
    ncols = readfields!(io, buffer, fields, separator, quotation_character)

    # If the line is blank, return a 0x0 array to signify this
    if ncols == 0
        return Array(UTF8String, 0, 0)
    end

    # Otherwise, allocate an array to store all of the text we'll read
    text_data = Array(UTF8String, nrows, ncols)
    for j in 1:ncols
        text_data[i, j] = fields[j]
    end

    # Loop until we've read nrows of text or run out of text
    while i < nrows
        tmp = readfields!(io, buffer, fields, separator, quotation_character)
        if tmp == ncols
            i += 1
            for j in 1:ncols
                text_data[i, j] = fields[j]
            end
        else
            break
        end
    end

    # Return as much text as we read
    return text_data[1:i, :]
end

##############################################################################
#
# Inferential steps
#
##############################################################################

function determine_separator(filename::String)
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

function determine_nrows(filename::String, header::Bool)
    total_lines = countlines(filename)
    if header
        return total_lines - 1
    else
        return total_lines
    end
end

function determine_column_names(io::IO,
                                separator::Char,
                                quotation_character::Char,
                                header::Bool)
    seek(io, 0)
    buffer = Array(Uint8, 2^16)
    fields = Array(UTF8String, 2^16)
    ncols = readfields!(io, buffer, fields, separator, quotation_character)

    if ncols == 0
        error("Failed to determine column names from an empty data source")
    end

    if header
        column_names = fields[1:ncols]
    else
        column_names = generate_column_names(1:ncols)
    end

    seek(io, 0)

    return column_names
end

function convert_to_dataframe{R <: String,
                              S <: String,
                              T <: String}(text_data::Matrix{R},
                                           missingness_indicators::Vector{S},
                                           column_names::Vector{T})
    # Keep a record of number of rows and columns
    nrows = size(text_data, 1)
    ncols = length(column_names)

    # Short-circuit if the text data is empty
    if nrows == 0
        column_types = {Any for i in 1:ncols}
        return DataFrame(column_types, column_names, 0)
    end

    # Store the columns as a set of DataVector's inside an Array of Any's
    columns = Array(Any, ncols)

    # Convert each column of text into a typed DataVector
    for j in 1:ncols
        is_missing = BitVector(nrows)
        for i in 1:nrows
            if contains(missingness_indicators, text_data[i, j])
                text_data[i, j] = MISSINGNESS_STRING
                is_missing[i] = true
            else
                is_missing[i] = false
            end
        end

        values = Array(Int64, nrows)
        try
            for i in 1:nrows
                values[i] = parseint(text_data[i, j])
            end
        catch
            try
                values = Array(Float64, nrows)
                for i in 1:nrows
                    values[i] = parsefloat(text_data[i, j])
                end
            catch
                try
                    values = Array(Bool, nrows)
                    for i in 1:nrows
                        values[i] = parsebool(text_data[i, j])
                    end
                catch
                    values = text_data[:, j]
                end
            end
        end
        columns[j] = DataArray(values, is_missing)
    end

    # Prepare the DataFrame we'll return
    return DataFrame(columns, column_names)
end

##############################################################################
#
# Text input
#
##############################################################################

# Read at most N lines from an IO object
# Then return a minibatch of at most N rows as a DataFrame
# Add column_types, force_types option
function read_minibatch{R <: String,
                        S <: String}(io::IO,
                                     separator::Char,
                                     quotation_character::Char,
                                     missingness_indicators::Vector{R},
                                     column_names::Vector{S},
                                     minibatch_size::Int)
    # Represent data as an array of strings before type conversion
    text_data = read_separated_text(io,
                                    minibatch_size,
                                    separator,
                                    quotation_character)

    # Convert text data to a DataFrame
    return convert_to_dataframe(text_data,
                                missingness_indicators,
                                column_names)
end

# Read an entire data set into a DataFrame from an IO
# TODO: Do only IO-pass through the data
function read_table{R <: String,
                    S <: String}(io::IO,
                                 separator::Char,
                                 quotation_character::Char,
                                 missingness_indicators::Vector{R},
                                 header::Bool,
                                 column_names::Vector{S},
                                 nrows::Int)
    # Return to start of stream
    seek(io, 0)

    # Read first line to remove header in advance
    if header
        readline(io)
    end

    # Represent data as an array of strings before type conversion
    text_data = read_separated_text(io,
                                    nrows,
                                    separator,
                                    quotation_character)

    # Short-circuit if data set is empty except for a header line
    if size(text_data, 1) == 0
        column_types = {Any for i in 1:length(column_names)}
        return DataFrame(column_types, column_names, 0)
    else
        # Convert text data to a DataFrame
        df = convert_to_dataframe(text_data,
                                  missingness_indicators,
                                  column_names)
        return df
    end
end

function read_table{T <: String}(filename::T,
                                 separator = determine_separator(filename),
                                 quotation_character = DEFAULT_QUOTATION_CHARACTER,
                                 missingness_indicators = DEFAULT_MISSINGNESS_INDICATORS,
                                 header = true)
    nrows = determine_nrows(filename, header)
    io = open(filename, "r")
    column_names = determine_column_names(io,
                                          separator,
                                          quotation_character,
                                          header)
    df = read_table(io,
                    separator,
                    quotation_character,
                    missingness_indicators,
                    header,
                    column_names,
                    nrows)
    close(io)
    return df
end

##############################################################################
#
# Text output
#
##############################################################################

function in_quotes(val::String, quotation_character::Char)
    string(quotation_character, val, quotation_character)
end

function in_quotes(val::Real, quotation_character::Char)
    string(val)
end

function in_quotes(val::Any, quotation_character::Char)
    string(quotation_character, string(val), quotation_character)
end

# TODO: write_table should do more to react to the type of each column
# Need to increase precision of string representation of Float64's
function print_table(io::IO,
                     df::DataFrame,
                     separator::Char = DEFAULT_SEPARATOR,
                     quotation_character::Char = DEFAULT_QUOTATION_CHARACTER,
                     header::Bool = true)
    n, p = size(df)
    if header
        column_names = colnames(df)
        for j in 1:p
            if j < p
                print(io, in_quotes(column_names[j], quotation_character))
                print(io, separator)
            else
                print(io, in_quotes(column_names[j], quotation_character))
                println(io)
            end
        end
    end
    for i in 1:n
        for j in 1:p
            if j < p
                print(io, in_quotes(df[i, j], quotation_character))
                print(io, separator)
            else
                print(io, in_quotes(df[i, j], quotation_character))
                println(io)
            end
        end
    end
    return
end

function print_table(df::DataFrame,
                     separator::Char = DEFAULT_SEPARATOR,
                     quotation_character::Char = DEFAULT_QUOTATION_CHARACTER,
                     header::Bool = true)
    print_table(OUTPUT_STREAM,
                df,
                separator,
                quotation_character,
                header)
end

function write_table(filename::String,
                     df::DataFrame,
                     separator::Char = determine_separator(filename),
                     quotation_character::Char = DEFAULT_QUOTATION_CHARACTER,
                     header::Bool = true)
    io = open(filename, "w")
    print_table(io, df, separator, quotation_character, header)
    close(io)
end

##############################################################################
#
# Binary serialization
#
##############################################################################

function save(filename, d)
    f = open(filename, "w")
    serialize(f, d)
    close(f)
end

function load_df(filename)
    f = open(filename)
    dd = deserialize(f)
    close(f)
    return dd
end
