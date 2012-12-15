##############################################################################
#
# Low-level text parsing
#
##############################################################################

# Implements a very simple two-state machine that splits *-separated
# lines on the single character `separator`, but ignores occurrences
# of `separator` when they occur inside a region bounded by
# `quotation_character`
#
# For now, we're going to restrict things to only handle incoming
# strings that use single Char encodings
function split_separated_line{T <: String}(line::T,
                                           separator::Char,
                                           quotation_character::Char)
  inside_quotes = false
  items = Array(UTF8String, strlen(line))
  current_item = Array(Uint8, strlen(line))
  total_items = 0
  i = 0
  for chr in line
    i += 1
    if inside_quotes
      if chr == quotation_character
        inside_quotes = false
        i -= 1
      else
        current_item[i] = chr
      end
    else
      if chr == quotation_character
        inside_quotes = true
        i -= 1
      else
        if chr == separator
          total_items += 1
          items[total_items] = bytestring(current_item[1:(i - 1)])
          i = 0
          current_item = Array(Uint8, strlen(line))
        else
          current_item[i] = chr
        end
      end
    end
  end
  total_items += 1
  items[total_items] = bytestring(current_item[1:i])
  return items[1:total_items]
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

function determine_ncols{T <: String}(filename::T,
                                      separator::Char,
                                      quotation_character::Char)
  io = open(filename, "r")
  line = chomp(readline(io))
  close(io)
  return length(split_separated_line(line, separator, quotation_character))
end

function determine_column_names(io::IOStream,
                                separator::Char,
                                quotation_character::Char,
                                header::Bool)
  seek(io, 0)
  line = chomp(readline(io))

  if length(line) == 0
    error("Failed to determine column names from an empty data source")
  end

  fields = split_separated_line(line, separator, quotation_character)

  if header
    seek(io, 0)
    return fields
  else
    seek(io, 0)
    column_names = generate_column_names(length(fields))
  end
end

# Read data line-by-line
# Line-by-line reading may be IO-bound
function read_separated_text(io::IOStream,
                             nrows::Int,
                             ncols::Int,
                             separator::Char,
                             quotation_character::Char)
  text_data = Array(UTF8String, nrows, ncols)

  i = 0
  while i < nrows
    line = chomp(readline(io))
    if length(line) == 0
      break
    end
    i += 1
    text_data[i, 1:ncols] = split_separated_line(line, separator, quotation_character)
  end

  if i == 0
    return Array(UTF8String, 0, 0)
  else
    return text_data[1:i, :]
  end
end

function infer_column_types{S <: String, T <: String}(text_data::Matrix{S},
                                         missingness_indicators::Vector{T})
  nrows, ncols = size(text_data)

  # Default to Int64 for all column types until we have to demote them
  # May want to shift to using numeric codes for types
  column_types = Array(Any, ncols)
  for i in 1:ncols
    column_types[i] = Int64
  end

  for j in 1:ncols
    for i in 1:nrows
      if column_types[j] <: String
        break
      end
      if !contains(missingness_indicators, text_data[i, j])
        column_types[j] = tightest_type(text_data[i, j], column_types[j])
      end
    end
  end

  return column_types
end

# TODO: Split this into determine_column_names and infer_column_types
# Short-circuit option allows one to just guess metadata for massive files
# Currently maxes out after 1,000 lines
function determine_metadata{T <: String}(filename::String,
                                         separator::Char,
                                         quotation_character::Char,
                                         missingness_indicators::Vector{T},
                                         header::Bool,
                                         short_circuit::Bool)

  nrows = determine_nrows(filename, header)
  maxlines = nrows
  if short_circuit
    maxlines = min(nrows, 1_000)
  end

  io = open(filename, "r")
  column_names = determine_column_names(io, separator, quotation_character, header)
  ncols = length(column_names)
  if header # Skip the header for type inference
    readline(io)
  end
  text_data = read_separated_text(io, maxlines, ncols, separator, quotation_character)
  close(io)

  column_types = infer_column_types(text_data, missingness_indicators)

  # Return the inferred column names and types
  return (column_names, column_types, nrows)
end

function determine_metadata{T <: String}(filename::String,
                                         header::Bool,
                                         short_circuit::Bool)
  separator = determine_separator(filename)
  quotation_character = '"'
  determine_metadata(filename, separator, quotation_character, missingness_indicators, header, short_circuit)
end

function convert_to_dataframe{R <: String,
                              S <: String,
                              T <: String}(text_data::Matrix{R},
                                           missingness_indicators::Vector{S},
                                           column_types::Vector,
                                           column_names::Vector{T})
  # Keep a record of number of rows and columns
  nrows, ncols = size(text_data)

  # Short-circuit if the text data is empty
  if nrows == 0
    return DataFrame(column_types, column_names, 0)
  end

  # Make sure that the user has specified coherent types and names
  if ncols != length(column_types) || ncols != length(column_names)
    error("Column types and names do not match the input data's size")
  end

  # Store the columns as a set of DataVec's inside an Array of Any's
  columns = Array(Any, ncols)

  # Convert each column of text into a DataVec of the
  # appropriate type
  for j in 1:ncols
    is_missing = BitVector(nrows)
    for i in 1:nrows
      if contains(missingness_indicators, text_data[i, j])
        text_data[i, j] = string(baseval(column_types[j]))
        is_missing[i] = true
      else
        is_missing[i] = false
      end
    end
    if column_types[j] == Int64
      values = int(text_data[1:nrows, j])
    elseif column_types[j] == Float64
      values = float(text_data[1:nrows, j])
    elseif column_types[j] == UTF8String
      values = convert(Array{UTF8String, 1}, text_data[1:nrows, j])
    elseif column_types[j] == ASCIIString
      values = convert(Array{ASCIIString, 1}, text_data[1:nrows, j])
    else
      error("Column cannot be converted to type: $(column_types[j])")
    end
    columns[j] = DataVec(values, is_missing)
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

# Read at most N lines from an IOStream
# Then return a minibatch of at most N rows as a DataFrame
function read_minibatch{R <: String,
                        S <: String,
                        T}(io::IOStream,
                           separator::Char,
                           quotation_character::Char,
                           missingness_indicators::Vector{R},
                           column_names::Vector{S},
                           column_types::Vector{T},
                           minibatch_size::Int64)
  # Keep a record of number of columns
  ncols = length(column_types)

  # Represent data as an array of strings before type conversion
  text_data = read_separated_text(io, minibatch_size, ncols, separator, quotation_character)

  # Convert text data to a DataFrame
  return convert_to_dataframe(text_data, missingness_indicators, column_types, column_names)
end

# Read an entire data set into a DataFrame from an IOStream
# TODO: Do only IO-pass through the data
function read_table{R <: String,
                    S <: String}(io::IOStream,
                                 separator::Char,
                                 quotation_character::Char,
                                 missingness_indicators::Vector{R},
                                 header::Bool,
                                 column_names::Vector{S},
                                 nrows::Int64)
  # Return to start of stream
  seek(io, 0)

  # Read first line to remove header in advance
  if header
    readline(io)
  end

  # Keep a record of number of columns
  ncols = length(column_names)

  # Represent data as an array of strings before type conversion
  text_data = read_separated_text(io, nrows, ncols, separator, quotation_character)

  # Short-circuit if data set is empty except for a header line
  if size(text_data, 1) == 0
    column_types = {Any for i in 1:ncols}
    return DataFrame(column_types, column_names, 0)
  end

  # Infer column types
  column_types = infer_column_types(text_data, missingness_indicators)

  # Convert text data to a DataFrame
  return convert_to_dataframe(text_data, missingness_indicators, column_types, column_names)
end

function read_table{T <: String}(filename::T)
  # Do inference for missing configuration settings
  separator = determine_separator(filename)
  quotation_character = '"'
  missingness_indicators = ["", "NA"]
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
# Quote all string fields
# Don't quote real-valued fields
# Quote non-string, non-real-valued fields
function in_quotes{T <: String}(val::T, quotation_character::Char)
  strcat(quotation_character, val, quotation_character)
end
function in_quotes{T <: Real}(val::T, quotation_character::Char)
  string(val)
end
function in_quotes{T <: Any}(val::T, quotation_character::Char)
  strcat(quotation_character, string(val), quotation_character)
end

# TODO: write_table should do more to react to the type of each column
# Need to increase precision of string representation of Float64's
function print_table(df::DataFrame,
                     io::IOStream,
                     separator::Char,
                     quotation_character::Char)
  n, p = nrow(df), ncol(df)
  column_names = colnames(df)
  for j in 1:p
    if j < p
      print(io, in_quotes(column_names[j], quotation_character))
      print(io, separator)
    else
      println(io, in_quotes(column_names[j], quotation_character))
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

function print_table(df::DataFrame, separator::Char, quotation_character::Char)
  print_table(df, OUTPUT_STREAM, separator, quotation_character)
end

print_table(df::DataFrame) = print_table(df, OUTPUT_STREAM, ',', '"')

function write_table{T <: String}(df::DataFrame,
                                  filename::T,
                                  separator::Char,
                                  quotation_character::Char)
  io = open(filename, "w")
  print_table(df, io, separator, quotation_character)
  close(io)
end

# Infer configuration settings from filename
function write_table{T <: String}(df::DataFrame, filename::T)
  separator = determine_separator(filename)
  quotation_character = '"'
  write_table(df, filename, separator, quotation_character)
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
