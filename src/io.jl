const DEFAULT_BOOLEAN_STRINGS = ["T", "F", "t", "f", "TRUE", "FALSE", "true", "false"]
const DEFAULT_TRUE_STRINGS = ["T", "t", "TRUE", "true"]
const DEFAULT_FALSE_STRINGS = ["F", "f", "FALSE", "false"]

const DEFAULT_QUOTATION_CHARACTER = '"'
const DEFAULT_SEPARATOR = ','

const DEFAULT_MISSINGNESS_INDICATORS = ["", "NA", "#NA", "N/A", "#N/A", "NULL", "."]

function parse_bool(x::String)
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

let extract_cache = memio(500, false)
    global extract_string
    function extract_string(this, left::Int, right::Int, omitlist::Set)
        if right - left > length(extract_cache.ios)
            extract_cache_size = right - left
            extract_cache = memio(extract_cache_size,false)
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
                if !has(omitlist, lasti)
                    print(extract_cache, ch)
                end
            end
            return takebuf_string(extract_cache)
        else
            return ""
        end
    end
end

const NULLSET = Set()
extract_string(this, left::Int, right::Int) = extract_string(this, left, right, NULLSET)

const STATE_EXPECTING_VALUE = 0
const STATE_IN_BARE = 1
const STATE_IN_QUOTED = 2
const STATE_POSSIBLE_EOQUOTED = 3
const STATE_EXPECTING_SEP = 4

# Read one line of delimited text
# This is complex because delimited text can contain EOL inside quoted fields
function read_separated_line(io,
                             separator::Char,
                             quotation_character::Char)
    # Indexes into the current line for the current item
    left = 0
    right = 0

    # Was using RopeString for efficient appends, but rare case and makes 
    # UTF-8 processing harder
    this = Base.chomp!(readline(io))

    # Short-circuit on the empty line
    if this == ""
      return Array(UTF8String, 0)
    end

    # 5-state machine. See list of possible states above
    state = STATE_EXPECTING_VALUE

    # Index of characters to remove
    omitlist = Set()

    # Where are we
    i = start(this)
    eol = false

    # Will eventually return a Vector of strings
    num_elems = 0
    ret = Array(ByteString, 0)

    # off we go! use manual loops because this can grow
    while true
        eol = done(this, i)
        if !eol
            this_i = i
            this_char, i = next(this, i)
        end
        if state == STATE_EXPECTING_VALUE
            if eol
                num_elems += 1
                push!(ret, "")
                break
            elseif this_char == ' '
                continue
            elseif this_char == separator
                num_elems += 1
                push!(ret, "")
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
                push!(ret, extract_string(this, left, right))
                break
            elseif this_char == separator
                right = this_i - 1
                num_elems += 1
                push!(ret, extract_string(this, left, right))
                state = STATE_EXPECTING_VALUE
            else
                continue
            end
        elseif state == STATE_IN_QUOTED
            if eol
                this = string(this, "\n", Base.chomp!(readline(io)))
            elseif this_char == quotation_character
                state = STATE_POSSIBLE_EOQUOTED
            else
                continue
            end
        elseif state == STATE_POSSIBLE_EOQUOTED
            if eol
                right = this_i - 1
                num_elems += 1
                push!(ret, extract_string(this, left, right, omitlist))
                break
            elseif this_char == quotation_character
                add!(omitlist, this_i)
                state = STATE_IN_QUOTED
            elseif this_char == separator
                right = this_i - 2
                num_elems += 1
                push!(ret, extract_string(this, left, right, omitlist))
                empty!(omitlist)
                state = STATE_EXPECTING_VALUE
            elseif this_char == ' '
                right = this_i - 2
                num_elems += 1
                push!(ret, extract_string(this, left, right, omitlist))
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
    ret
end

# Read data line-by-line
function read_separated_text(io::IO,
                             nrows::Int,
                             separator::Char,
                             quotation_character::Char)
    # Read one line to determine the number of columns
    i = 1
    sp = read_separated_line(io, separator, quotation_character)
    ncols = length(sp)

    # If the line is blank, return a 0x0 array to signify this
    if ncols == 0
        return Array(UTF8String, 0, 0)
    end

    # Otherwise, allocate an array to store all of the text we'll read
    text_data = Array(UTF8String, nrows, ncols)
    text_data[i, :] = sp

    # Loop until we've read nrows of text or run out of text
    while i < nrows
        sp = read_separated_line(io, separator, quotation_character)
        if length(sp) == ncols
            i += 1
            text_data[i, :] = sp 
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

function determine_separator{T <: String}(filename::T)
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

function determine_nrows{T <: String}(filename::T, header::Bool)
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
  fields = read_separated_line(io, separator, quotation_character)

  if length(fields) == 0
    error("Failed to determine column names from an empty data source")
  end

  column_names = header ? fields : generate_column_names(length(fields))
  seek(io, 0)
  return column_names
end

function convert_to_dataframe{R <: String,
                              S <: String,
                              T <: String}(text_data::Matrix{R},
                                           missingness_indicators::Vector{S},
                                           column_names::Vector{T})
  # Keep a record of number of rows and columns
  nrows, ncols = size(text_data, 1), length(column_names)

  # Short-circuit if the text data is empty
  if nrows == 0
    column_types = {Any for i in 1:ncols}
    return DataFrame(column_types, column_names, 0)
  end

  # Store the columns as a set of DataVector's inside an Array of Any's
  columns = Array(Any, ncols)

  # Convert each column of text into a DataVector of the
  # appropriate type
  dtime = 0.0
  for j in 1:ncols
    is_missing = BitVector(nrows)
    for i in 1:nrows
      value_missing = contains(missingness_indicators, text_data[i, j])
      if value_missing
        text_data[i, j] = utf8("0")
        is_missing[i] = true
      else
        is_missing[i] = false
      end
    end
    values = Array(Int64, nrows)
    try
      for i in 1:nrows
        values[i] = parse_int(text_data[i, j])
      end
    catch
      try
        values = Array(Float64, nrows)
        for i in 1:nrows
          values[i] = parse_float(text_data[i, j])
        end
      catch
        try
          values = Array(Bool, nrows)
          for i in 1:nrows
            values[i] = parse_bool(text_data[i, j])
          end
        catch
          values = text_data[:, j]
        end
      end
    end
    columns[j] = DataArray(values, is_missing)
  end

  # Prepare the DataFrame we'll return
  df = DataFrame(columns, column_names)
  return df
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
  text_data = read_separated_text(io, minibatch_size, separator, quotation_character)

  # Convert text data to a DataFrame
  return convert_to_dataframe(text_data, missingness_indicators, column_names)
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
  text_data = read_separated_text(io, nrows, separator, quotation_character)

  # Short-circuit if data set is empty except for a header line
  if size(text_data, 1) == 0
    column_types = {Any for i in 1:length(column_names)}
    return DataFrame(column_types, column_names, 0)
  else
    # Convert text data to a DataFrame
    df = convert_to_dataframe(text_data, missingness_indicators, column_names)
    return df
  end
end

function read_table{T <: String}(filename::T)
  # Do inference for missing configuration settings
  separator = determine_separator(filename)
  quotation_character = DEFAULT_QUOTATION_CHARACTER
  missingness_indicators = DEFAULT_MISSINGNESS_INDICATORS
  header = true
  nrows = determine_nrows(filename, header)
  io = open(filename, "r")
  column_names = determine_column_names(io, separator, quotation_character, header)
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

# Quotation rules
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
                     separator::Char,
                     quotation_character::Char,
                     header::Bool)
  n, p = nrow(df), ncol(df)
  if header
    column_names = colnames(df)
    for j in 1:p
      if j < p
        print(io, in_quotes(column_names[j], quotation_character))
        print(io, separator)
      else
        println(io, in_quotes(column_names[j], quotation_character))
      end
    end
  end
  for i in 1:n
    for j in 1:p
      if j < p
        print(io, in_quotes(df[i, j], quotation_character))
        print(io, separator)
      else
        println(io, in_quotes(df[i, j], quotation_character))
      end
    end
  end
end

function print_table(io::IO,
                     df::DataFrame,
                     separator::Char,
                     quotation_character::Char)
  print_table(io, df, separator, quotation_character, true)
end

function print_table(df::DataFrame, separator::Char, quotation_character::Char)
  print_table(OUTPUT_STREAM, df, separator, quotation_character, true)
end

function print_table(df::DataFrame)
    print_table(OUTPUT_STREAM,
                df,
                DEFAULT_SEPARATOR,
                DEFAULT_QUOTATION_CHARACTER,
                true)
end

function write_table(filename::String,
                     df::DataFrame,
                     separator::Char,
                     quotation_character::Char)
  io = open(filename, "w")
  print_table(io, df, separator, quotation_character)
  close(io)
end

# Infer configuration settings from filename
function write_table(filename::String, df::DataFrame)
  separator = determine_separator(filename)
  quotation_character = DEFAULT_QUOTATION_CHARACTER
  write_table(filename, df, separator, quotation_character)
end

##############################################################################
#
# Binary serialization
#
##############################################################################

# Wrappers for serialization
function save(filename, d)
    f = open(filename, "w")
    serialize(f, d)
    close(f)
end

function load_df(filename)
    f = open(filename)
    dd = deserialize(f)()
    close(f)
    return dd
end

# end
