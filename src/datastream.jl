#
# DataStream() should just convert things to appropriate
# type of DataStream, each of which implements the AbstractDataStream
# protocol. At present this can be done using a file on disk through
# the FileDataStream type or using an in-memory DataFrame through the
# the DataFrameDataStream type.
#

abstract AbstractDataStream

##############################################################################
#
# FileDataStream
#
##############################################################################

# TODO: Decide whether to keep column types. Reader no longer supports
# explicit a priori type information.
type FileDataStream <: AbstractDataStream
  filename::String
  stream::IO # Any
  separator::Char
  quotation_character::Char
  missingness_indicators::Vector
  header::Bool
  column_names::Vector
  column_types::Vector
  minibatch_size::Int
end

function FileDataStream{T <: String}(filename::T, minibatch_size::Int)
  stream = OUTPUT_STREAM
  separator = determine_separator(filename)
  quotation_character = DEFAULT_QUOTATION_CHARACTER
  missingness_indicators = DEFAULT_MISSINGNESS_INDICATORS
  header = true
  short_circuit = true
  io = open(filename, "r")
  column_names = determine_column_names(io,
                                        separator,
                                        quotation_character,
                                        header)
  close(io)
  column_types = {Any for i in 1:length(column_names)}
  FileDataStream(filename, stream, separator,
                 quotation_character, missingness_indicators,
                 header, column_names, column_types,
                 minibatch_size)
end

function FileDataStream{T <: String}(filename::T)
  FileDataStream(filename, 1)
end

function DataStream{T <: String}(filename::T, minibatch_size::Int)
  FileDataStream(filename, minibatch_size)
end

function DataStream{T <: String}(filename::T)
  FileDataStream(filename, 1)
end

function start(ds::FileDataStream)
  ds.stream = open(ds.filename, "r")

  # Read one line to remove header in advance
  if ds.header
    readline(ds.stream)
  end

  next_df = read_minibatch(ds.stream,
                           ds.separator,
                           ds.quotation_character,
                           ds.missingness_indicators,
                           colnames(ds),
                           ds.minibatch_size)

  return next_df
end

function next(ds::FileDataStream, df::DataFrame)
  next_df = read_minibatch(ds.stream,
                           ds.separator,
                           ds.quotation_character,
                           ds.missingness_indicators,
                           colnames(ds),
                           ds.minibatch_size)
  (df, next_df)
end

function done(ds::FileDataStream, df::DataFrame)
  if nrow(df) == 0
    close(ds.stream)
    return true
  else
    return false
  end
end

coltypes(ds::FileDataStream) = ds.column_types
colnames(ds::FileDataStream) = ds.column_names

##############################################################################
#
# IODataStream
#
##############################################################################

type IODataStream <: AbstractDataStream
  stream::IO
  separator::Char
  quotation_character::Char
  missingness_indicators::Vector
  header::Bool
  column_names::Vector
  minibatch_size::Int
end

function IODataStream(io::IO, minibatch_size::Int)
  stream = io
  separator = DEFAULT_SEPARATOR
  quotation_character = DEFAULT_QUOTATION_CHARACTER
  missingness_indicators = DEFAULT_MISSINGNESS_INDICATORS
  header = true
  column_names = ["FILLER"]
  short_circuit = true
  IODataStream(stream, separator,
               quotation_character, missingness_indicators,
               header, column_names, minibatch_size)
end

function IODataStream(io::IO)
  IODataStream(io, 1)
end

function DataStream(io::IO, minibatch_size::Int)
  IODataStream(io, minibatch_size)
end

function DataStream(io::IO)
  IODataStream(io, 1)
end

function start(ds::IODataStream)
  # Seek to the start of the stream
  # May be impossible for some streams like STDIN
  try
    seek(ds.stream, 0)
  catch
    warn("Could not seek to the start of the input stream")
  end

  # Read one line to remove header in advance
  if ds.header
    line = chomp(readline(ds.stream))
    if strlen(line) == 0
      error("Empty header line")
    end
    items = Array(UTF8String, strlen(line))
    current_field = Array(Char, strlen(line))
      ds.column_names = read_separated_line(ds.stream, ds.separator, ds.quotation_character)
  else
    error("Currently only IODataStream's with headers are supported")
  end

  df = read_minibatch(ds.stream,
                      ds.separator,
                      ds.quotation_character,
                      ds.missingness_indicators,
                      colnames(ds),
                      ds.minibatch_size)

  return df
end

function next(ds::IODataStream, df::DataFrame)
  next_df = read_minibatch(ds.stream,
                           ds.separator,
                           ds.quotation_character,
                           ds.missingness_indicators,
                           colnames(ds),
                           ds.minibatch_size)
  (df, next_df)
end

function done(ds::IODataStream, df::DataFrame)
  if nrow(df) == 0
    return true
  else
    return false
  end
end

coltypes(ds::IODataStream) = ds.column_types
colnames(ds::IODataStream) = ds.column_names

##############################################################################
#
# DataFrameDataStream
#
##############################################################################

type DataFrameDataStream <: AbstractDataStream
  df::DataFrame
  minibatch_size::Int
end

function start(ds::DataFrameDataStream)
  return 1
end

function next(ds::DataFrameDataStream, i::Int)
  (ds.df[i:(i + ds.minibatch_size - 1), :], i + ds.minibatch_size)
end

function done(ds::DataFrameDataStream, i::Int)
  return i > nrow(ds.df)
end

DataStream(df::DataFrame, minibatch_size::Int) = DataFrameDataStream(df, minibatch_size)

DataStream(df::DataFrame) = DataFrameDataStream(df, 1)

coltypes(ds::DataFrameDataStream) = coltypes(ds.df)
colnames(ds::DataFrameDataStream) = colnames(ds.df)

##############################################################################
#
# MatrixDataStream
#
##############################################################################

type MatrixDataStream{T} <: AbstractDataStream
  m::Matrix{T}
  minibatch_size::Int
end

function start(ds::MatrixDataStream)
  return 1
end

function next(ds::MatrixDataStream, i::Int)
  (DataFrame(ds.m[i:(i + ds.minibatch_size - 1), :]), i + ds.minibatch_size)
end

function done(ds::MatrixDataStream, i::Int)
  return i > size(ds.m, 1)
end

DataStream{T}(m::Matrix{T}, minibatch_size::Int) = MatrixDataStream{T}(m, minibatch_size)

DataStream{T}(m::Matrix{T}) = MatrixDataStream(m, 1)

coltypes{T}(ds::MatrixDataStream{T}) = {T for i in 1:size(ds.m, 2)}
colnames(ds::MatrixDataStream) = generate_column_names(size(ds.m, 2))

##############################################################################
#
# Streaming data functions
#
##############################################################################

function colsums(ds::AbstractDataStream)
  p = length(colnames(ds))
  sums = zeros(p)
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          sums[column_index] += minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  results = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      results[1, column_index] = sums[column_index]
    end
  end

  return results
end

function colprods(ds::AbstractDataStream)
  p = length(colnames(ds))
  prods = zeros(p)
  ns = ones(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          prods[column_index] *= minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  results = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      results[1, column_index] = prods[column_index]
    end
  end

  return results
end

function colmeans(ds::AbstractDataStream)
  p = length(colnames(ds))
  sums = zeros(p)
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          sums[column_index] += minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  results = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      results[1, column_index] = sums[column_index] / ns[column_index]
    end
  end

  return results
end

function colvars(ds::AbstractDataStream)
  p = length(colnames(ds))
  means = zeros(p)
  deltas = zeros(p)
  m2s = zeros(p)
  vars = zeros(p)
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          ns[column_index] += 1
          deltas[column_index] = minibatch[row_index, column_index] - means[column_index]
          means[column_index] += deltas[column_index] / ns[column_index]
          m2s[column_index] = m2s[column_index] + deltas[column_index] * (minibatch[row_index, column_index] - means[column_index])
          vars[column_index] = m2s[column_index] / (ns[column_index] - 1)
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  results = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      results[1, column_index] = vars[column_index]
    end
  end

  return results
end

function colstds(ds::AbstractDataStream)
  vars = colvars(ds)
  stds = deepcopy(vars)
  column_types = coltypes(vars)
  for j in 1:length(column_types)
    if column_types[j] <: Real
      stds[1, j] = sqrt(vars[1, j])
    end
  end
  return stds
end

function colmins(ds::AbstractDataStream)
  p = length(colnames(ds))
  mins = [Inf for i in 1:p]
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          if minibatch[row_index, column_index] < mins[column_index]
            mins[column_index] = minibatch[row_index, column_index]
            ns[column_index] += 1
          end
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  df = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      df[1, column_index] = mins[column_index]
    end
  end

  return df
end

function colmaxs(ds::AbstractDataStream)
  p = length(colnames(ds))
  maxs = [-Inf for i in 1:p]
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          if minibatch[row_index, column_index] > maxs[column_index]
            maxs[column_index] = minibatch[row_index, column_index]
            ns[column_index] += 1
          end
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  df = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      df[1, column_index] = maxs[column_index]
    end
  end

  return df
end

function colranges(ds::AbstractDataStream)
  p = length(colnames(ds))
  mins = [Inf for i in 1:p]
  maxs = [-Inf for i in 1:p]
  ns = zeros(Int, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
          ns[column_index] += 1
          if minibatch[row_index, column_index] < mins[column_index]
            mins[column_index] = minibatch[row_index, column_index]
          end
          if minibatch[row_index, column_index] > maxs[column_index]
            maxs[column_index] = minibatch[row_index, column_index]
          end
        end
      end
    end
  end

  result_types = {Float64 for i in 1:p}
  df_mins = DataFrame(result_types, colnames(ds), 1)
  df_maxs = DataFrame(result_types, colnames(ds), 1)

  for column_index in 1:p
    if ns[column_index] != 0
      df_mins[1, column_index] = mins[column_index]
      df_maxs[1, column_index] = maxs[column_index]
    end
  end

  return (df_mins, df_maxs)
end

# Two-pass algorithm for covariance and correlation
function cov(ds::AbstractDataStream)
  p = length(colnames(ds))

  # Make one pass to compute means
  means = colmeans(ds)

  # Now compute covariances during second pass
  ns = zeros(Int, p, p)
  covariances = datazeros(p, p)
 
  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        for alt_column_index in 1:p
          if coltypes(minibatch)[column_index] <: Real &&
                !isna(minibatch[row_index, column_index]) &&
                coltypes(minibatch)[alt_column_index] <: Real &&
                !isna(minibatch[row_index, alt_column_index])
            ns[column_index, alt_column_index] += 1
            n = ns[column_index, alt_column_index]
            a = minibatch[row_index, column_index] - means[1, column_index]
            b = minibatch[row_index, alt_column_index] - means[1, alt_column_index]
            covariances[column_index, alt_column_index] = ((n - 1) / n) * covariances[column_index, alt_column_index] + (a * b) / n
          end
        end
      end
    end
  end

  # Scale estimates by (n / (n - 1))
  for i in 1:p
    for j in 1:p
      if ns[i, j] <= 2
        covariances[i, j] = NA
      else
        n = ns[i, j]
        covariances[i, j] *= (n / (n - 1))
      end
    end
  end

  return covariances
end

function cor(ds::AbstractDataStream)
  covariances = cov(ds)
  correlations = deepcopy(covariances)
  p = nrow(correlations)
  for i in 1:p
    for j in 1:p
      correlations[i, j] = covariances[i, j] / sqrt(covariances[i, i] * covariances[j, j])
    end
  end
  return correlations
end

function getindex(ds::AbstractDataStream, i::Int)
  cur_i = 0
  for df in ds
    if cur_i + nrow(df) > i
      return df[i - cur_i, :]
    end
  end
  error("Did not find requested row")
end

# TODO: Stop returning empty DataFrame at the end of a stream
#       (NOTE: Probably not possible because we don't know nrows.)
# TODO: Implement
#        * colentropys
#        * colcardinalities
#        * colmedians
#        * colffts
#        * colnorms
