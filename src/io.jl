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
  items = Array(String, strlen(line))
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

# Reads 100% of data
#
# Should eventually supplement with method that only does a partial read
#
# TODO: Split this into determine_column_names and determine_column_types
function determine_metadata{T <: String}(filename::String,
                                         separator::Char,
                                         quotation_symbol::Char,
                                         missingness_indicators::Vector{T},
                                         header::Bool)

  # Read the first line of the file to determine the number of columns
  f = open(filename, "r")
  line = readline(f)
  if length(line) == 0
    error("Empty file: $filename")
  end
  fields = split_separated_line(chomp(line), separator, quotation_symbol)
  ncols = length(fields)

  # Default to Int64 for all column types until we have to demote them
  column_types = {Int64 for i in 1:ncols}

  # Deal with the header
  if header
    # Use fields as column names, then move on
    column_names = convert(Array{UTF8String}, fields)
    i = 1
  else
    # Set standard column names, then prepare to reread first line
    column_names = generate_column_names(ncols)
    seek(f, 0)
    i = 0
  end

  # Iterate over lines
  # If any entries violate type expectations, loosen them
  # Don't ever change type inference for missing fields
  for line in each_line(f)
    i += 1
    # Used to allow partial reads for speed
    # if i >= Inf
    #   break
    # end
    fields = split_separated_line(chomp(line), separator, quotation_symbol)
    for j in 1:ncols
      if column_types[j] <: String
        continue
      end
      if !contains(missingness_indicators, fields[j])
        column_types[j] = tightest_type(fields[j], column_types[j])
      end
    end
  end

  # Close the file after reading all of the entries
  close(f)

  # Return the inferred column names and types
  return (column_names, column_types, i)
end

# Read at most N lines, return minibatch of at most N rows as a DataFrame
function read_minibatch{R <: String, S <: String, T}(file::IOStream,
                        separator::Char,
                        quotation_character::Char,
                        missingness_indicators::Vector{R},
                        column_names::Vector{S},
                        column_types::Vector{T},
                        minibatch_size::Int64)
  # Keep a record of number of columns
  p = length(column_types)

  # Keep track of how many rows we've read and whether we hit EOF
  i, EOF = 0, false

  # Represent data as an array of strings before type conversion
  text_data = Array(UTF8String, minibatch_size, p)

  # Read data line-by-line
  # Line-by-line reading may be IO-bound
  # This will be a non-trivial issue to fix if true
  while i < minibatch_size && !EOF
    line = chomp(readline(file))
    if length(line) == 0
      EOF = true
      if EOF && i == 0
        return DataFrame(column_types, column_names, 0)
      end
    else
      i += 1
    end
    text_data[i, 1:p] = split_separated_line(line,
                                             separator,
                                             quotation_character)
  end

  # Store the columns as an Array of DataVec's
  columns = Array(Any, p)

  # Convert each column of text into a DataVec of the
  # appropriate type
  for j in 1:p
    is_missing = Array(Bool, i)
    for r in 1:i
      if contains(missingness_indicators, text_data[r, j])
        text_data[r, j] = string(baseval(column_types[j]))
        is_missing[r] = true
      else
        is_missing[r] = false
      end
    end
    if column_types[j] == Int64
      values = int(text_data[1:i, j])
    elseif column_types[j] == Float64
      values = float(text_data[1:i, j])
    elseif column_types[j] == UTF8String
      values = convert(Array{UTF8String}, text_data[1:i, j])
    else
      error("Unknown type conversion required: $(column_types[j])")
    end
    columns[j] = DataVec(values,
                         is_missing)
  end

  # Prepare the DataFrame we'll return
  df = DataFrame(columns)
  colnames!(df, column_names)
  return df
end

function read_table{R <: String, S <: String, T}(file::IOStream,
                    separator::Char,
                    quotation_character::Char,
                    missingness_indicators::Vector{R},
                    header::Bool,
                    column_names::Vector{S},
                    column_types::Vector{T},
                    nrows::Int64)
  # Core functionality here is based on read_minibatch()

  if header
    nrows -= 1
  end

  # Read one line to remove header in advance
  if header
    readline(file)
  end

  # Call read_minibatch with minibatch size of nrows
  df = read_minibatch(file,
                      separator,
                      quotation_character,
                      missingness_indicators,
                      column_names,
                      column_types,
                      nrows)

  return df
end

function read_table{T <: String}(filename::T)
  # Do inference for missing configuration settings
  separator = determine_separator(filename)
  quotation_character = '"'
  missingness_indicators = ["", "NA"]
  header = true
  column_names, column_types, nrows =
    determine_metadata(filename,
                       separator,
                       quotation_character,
                       missingness_indicators,
                       header)
  file = open(filename, "r")
  df = read_table(file,
                    separator,
                    quotation_character,
                    missingness_indicators,
                    header,
                    column_names,
                    column_types,
                    nrows)
  close(file)
  return df
end

#
# This file should be called io.jl and include read/write tools
#

# Quote all string fields
function in_quotes{T <: String}(s::T, quotation_character::Char)
  strcat(quotation_character, s, quotation_character)
end
# Don't quote non-string fields
function in_quotes{T <: Any}(s::T, quotation_character::Char)
  string(s)
end

# write_table should do more to react to the type of each column
# Increase precision of floats
function print_table(df::DataFrame, io::IOStream, separator::Char, quotation_character::Char)
  n, p = nrow(df), ncol(df)
  for j in 1:p
    if j < p
      print(io, in_quotes(colnames(df)[j], quotation_character))
      print(io, separator)
    else
      println(io, in_quotes(colnames(df)[j], quotation_character))
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
  file = open(filename, "w")
  print_table(df, file, separator, quotation_character)
  close(file)
end

# Infer configuration settings from filename
function write_table{T <: String}(df::DataFrame, filename::T)
  separator = DataFrames.determine_separator(filename)
  quotation_character = '"'
  write_table(df, filename, separator, quotation_character)
end

# Binary serialization

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
