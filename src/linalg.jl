#
# Calculates the SVD of a data matrix containing missing entries.
#
# Uses the iterative algorithm of Hastie et al. 1999
#

# Calculate the rank-k SVD approximation to a matrix given the
# full SVD.
function approximate(u, d, v, k::Int64)
  u[:, 1:k] * diagm(d[1:k]) * v[1:k, :]
end

# Test code:
#
# srand(1)
# M = rand(3, 3)
# (u, d, v) = svd(M)
# norm(approximate(u, d, v, 1) - M)
# norm(approximate(u, d, v, 2) - M)
# norm(approximate(u, d, v, 3) - M)

# Impute a missing entries using current approximation.
function impute(m::Matrix{Float64}, missing_entries, u, d, v, k::Int64)
  approximate_m = approximate(u, d, v, k)

  for index_pair in missing_entries
    m[index_pair[1], index_pair[2]] = approximate_m[index_pair[1], index_pair[2]]
  end

  m
end

# Should be done with a proper N-dimensional Int64 array.
function ind_na{T}(df::DataMatrix{T})
  indices = {}
  for i = 1:nrow(df)
    for j = 1:ncol(df)
      if isna(df[i, j])
        push(indices, [i, j])
      end
    end
  end
  indices
end

# Kind of a nutty method without DataMatrix.
function mean{T}(df::DataMatrix{T})
  mu = 0.0
  n = 0
  for i = 1:nrow(df)
    for j = 1:ncol(df)
      if !isna(df[i, j])
        mu += df[i, j]
        n += 1
      end
    end
  end
  mu / n
end

# This will crash if a row is missing all entries.
function rowmeans{T}(df::DataMatrix{T})
  mus = zeros(nrow(df))
  for i = 1:nrow(df)
    mu = 0.0
    n = 0
    for j = 1:ncol(df)
      if !isna(df[i, j])
        mu += df[i, j]
        n += 1
      end
    end
    mus[i] = mu / n
  end
  mus
end

# Must select rank k of SVD to use.
# TODO: Default to failure in the face of NA's
function svd{T}(D::DataMatrix{T}, k::Int)
  df = deepcopy(D)

  print_trace = false

  tolerance = 10e-4

  # Cache the dimensions of the matrix.
  n = size(df, 1)
  p = size(df, 2)

  # Estimate missingness and print a message.
  missing_entries = ind_na(df)
  missingness = length(missing_entries) / (nrow(df) * ncol(df))
  if print_trace
    println("Matrix is missing $(missingness * 100)% of entries")
  end

  # Initial imputation uses row means.
  global_mu = mean(df)
  mu_i = rowmeans(df)

  for i = 1:n
    for j = 1:p
      if isna(df[i, j])
        if isna(mu_i[i])
          df[i, j] = global_mu
        else
          df[i, j] = mu_i[i]          
        end
      end
    end
  end

  # Make a matrix out of the dataframe.
  tmp = zeros(n, p)
  for i = 1:n
    for j = 1:p
      tmp[i, j] = df[i, j]
    end
  end
  df = tmp

  # Count iterations of proper imputation method.
  i = 0

  # Keep track of approximate matrices.
  previous_df = copy(df)
  current_df = copy(df)

  # Keep track of Frobenius norm of changes in imputed matrix.
  change = Inf

  # Iterate until imputation stops changing up to a tolerance of 10e-6.
  while change > tolerance
    if print_trace
      println("Iteration $i")
      println("Change $change")
    end

    # Impute missing entries using current SVD.
    previous_df = copy(current_df)
    u, d, v = svd(current_df)
    current_df = impute(current_df, missing_entries, u, d, v, k)
    
    # Compute the change in the matrix across iterations.
    change = norm(previous_df - current_df) / norm(df)

    # Increment the iteration counter.
    i = i + 1
  end

  # Tell the user how many iterations were required to impute matrix.
  if print_trace
    println("Tolerance achieved after $i iterations")
  end
  
  # Return both df and the SVD of df with all entries imputed.
  u, d, v = svd(current_df)

  # Only return the SVD entries, not the imputation
  return (u[:, 1:k], d[1:k], v[1:k, :])
end

svd{T}(D::DataMatrix{T}) = svd(D, min(nrow(D), ncol(D)))

function eig{T}(D::DataMatrix{T})
  u, d, v = svd(D)
  eig(u * diagm(d) * v)
end
