# DataStream() should just convert things to appropriate
# type of DataStream, each of which implements the AbstractDataStream
# protocol. For now, the only example is a FileDataStream
#
# TODO: Add MatrixDataStream, DataFrameDataStream
#

abstract AbstractDataStream

type FileDataStream <: AbstractDataStream
  filename::String
  stream::IOStream
  separator::Char
  quotation_character::Char
  missingness_indicators::Vector
  header::Bool
  column_names::Vector
  column_types::Vector
  minibatch_size::Int64
end

function FileDataStream{T <: String}(filename::T, minibatch_size::Int64)
  stream = open(filename, "r")
  separator = determine_separator(filename)
  quotation_character = '"'
  missingness_indicators = ["", "NA"]
  header = true
  # Will need to guess metadata in the future for huge data sets
  (column_names, column_types, nrows) =
    determine_metadata(filename,
                       separator,
                       quotation_character,
                       missingness_indicators,
                      header)
  FileDataStream(filename, stream, separator,
                 quotation_character, missingness_indicators,
                 header, column_names, column_types,
                 minibatch_size)
end

function FileDataStream{T <: String}(filename::T)
  FileDataStream(filename, 1)
end

function DataStream{T <: String}(filename::T, minibatch_size::Int64)
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

  return DataFrame(1, 1)
end

function next(ds::FileDataStream, df::DataFrame)
  df = read_minibatch(ds.stream,
                      ds.separator,
                      ds.quotation_character,
                      ds.missingness_indicators,
                      ds.column_names,
                      ds.column_types,
                      ds.minibatch_size)
  (df, df)
end

function done(ds::FileDataStream, df::DataFrame)
  if nrow(df) == 0
    close(ds.stream)
    return true
  else
    return false
  end
end

#
# Streaming data functions
#

function colsums(ds::AbstractDataStream)
  p = length(ds.column_types)
  sums = zeros(p)
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          sums[column_index] += minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  for j in 1:p
    if result_types[j] == Int64
      result_types[j] = Float64
    end
  end
  results = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      results[1, column_index] = sums[column_index]
    end
  end

  return results
end

function colprods(ds::AbstractDataStream)
  p = length(ds.column_types)
  prods = zeros(p)
  ns = ones(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          prods[column_index] *= minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  for j in 1:p
    if result_types[j] == Int64
      result_types[j] = Float64
    end
  end
  results = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      results[1, column_index] = prods[column_index]
    end
  end

  return results
end

function colmeans(ds::AbstractDataStream)
  p = length(ds.column_types)
  sums = zeros(p)
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          sums[column_index] += minibatch[row_index, column_index]
          ns[column_index] += 1
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  for j in 1:p
    if result_types[j] == Int64
      result_types[j] = Float64
    end
  end
  results = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      results[1, column_index] = sums[column_index] / ns[column_index]
    end
  end

  return results
end

function colvars(ds::AbstractDataStream)
  p = length(ds.column_types)
  means = zeros(p)
  deltas = zeros(p)
  m2s = zeros(p)
  vars = zeros(p)
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          ns[column_index] += 1
          deltas[column_index] = minibatch[row_index, column_index] - means[column_index]
          means[column_index] += deltas[column_index] / ns[column_index]
          m2s[column_index] = m2s[column_index] + deltas[column_index] * (minibatch[row_index, column_index] - means[column_index])
          vars[column_index] = m2s[column_index] / (ns[column_index] - 1)
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  for j in 1:p
    if result_types[j] == Int64
      result_types[j] = Float64
    end
  end
  results = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
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
  p = length(ds.column_types)
  mins = [Inf for i in 1:p]
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          if minibatch[row_index, column_index] < mins[column_index]
            mins[column_index] = minibatch[row_index, column_index]
            ns[column_index] += 1
          end
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  df = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      df[1, column_index] = mins[column_index]
    end
  end

  return df
end

function colmaxs(ds::AbstractDataStream)
  p = length(ds.column_types)
  maxs = [-Inf for i in 1:p]
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
          if minibatch[row_index, column_index] > maxs[column_index]
            maxs[column_index] = minibatch[row_index, column_index]
            ns[column_index] += 1
          end
        end
      end
    end
  end

  result_types = copy(ds.column_types)
  df = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      df[1, column_index] = maxs[column_index]
    end
  end

  return df
end

function colranges(ds::AbstractDataStream)
  p = length(ds.column_types)
  mins = [Inf for i in 1:p]
  maxs = [-Inf for i in 1:p]
  ns = zeros(Int64, p)

  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        if ds.column_types[column_index] <: Real && !isna(minibatch[row_index, column_index])
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

  result_types = copy(ds.column_types)
  for j in 1:p
    if result_types[j] == Int64
      result_types[j] = Float64
    end
  end
  df_mins = DataFrame(result_types, ds.column_names, 1)
  df_maxs = DataFrame(result_types, ds.column_names, 1)

  for column_index in 1:p
    if ds.column_types[column_index] <: Real && ns[column_index] != 0
      df_mins[1, column_index] = mins[column_index]
      df_maxs[1, column_index] = maxs[column_index]
    end
  end

  return (df_mins, df_maxs)
end

# Two-pass algorithm
function cov_pearson(ds::AbstractDataStream)
  p = length(ds.column_types)

  # Make one pass to compute means
  means = colmeans(ds)

  # Now compute covariances during second pass
  ns = zeros(Int64, p, p)
  covariances = dfzeros(p, p)
 
  for minibatch in ds
    for row_index in 1:nrow(minibatch)
      for column_index in 1:p
        for alt_column_index in 1:p
          if ds.column_types[column_index] <: Real &&
                !isna(minibatch[row_index, column_index]) &&
                ds.column_types[alt_column_index] <: Real &&
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
      if !(ds.column_types[i] <: Real) || !(ds.column_types[j] <: Real)
        covariances[i, j] = NA
      else
        n = ns[i, j]
        covariances[i, j] *= (n / (n - 1))
      end
    end
  end

  colnames!(covariances, ds.column_names)

  return covariances
end

function cor_pearson(ds::AbstractDataStream)
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

# TODO: Implement indexing into DataStream's
# TODO: Stop returning empty DataFrame at the end of a stream
# TODO: Implement
#        * colentropys
#        * colcardinalities
#        * colmedians
#        * colffts
#        * colnorms
