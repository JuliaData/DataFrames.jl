# Calculates the SVD of a data matrix containing missing entries.
#
# This should really be done with a DataMatrix{Int64} or a 
# DataMatrix{Float64}, but it's currently being done with generic
# DataFrame's that should be edited in advance to insure that the
# algorithm won't crash.

# Uses the iterative algorithm of Hastie et al. 1999

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
function ind_na(df::DataFrame)
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
function mean(df::DataFrame)
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
function row_means(df::DataFrame)
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
function missing_svd(D::DataFrame, k::Int)
  df = copy(D)

  tolerance = 10e-4

  # Cache the dimensions of the matrix.
  n = size(df, 1)
  p = size(df, 2)

  # Estimate missingness and print a message.
  missing_entries = ind_na(df)
  missingness = length(missing_entries) / (nrow(df) * ncol(df))
  println("Matrix is missing $(missingness * 100)% of entries")

  # Initial imputation uses row means.
  global_mu = mean(df)
  mu_i = row_means(df)

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
    println("Iteration $i")
    println("Change $change")

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
  println("Tolerance achieved after $i iterations")
  
  # Return both df and the SVD of df with all entries imputed.
  u, d, v = svd(current_df)
  (current_df, u[:, 1:k], d[1:k], v[1:k, :])
end
