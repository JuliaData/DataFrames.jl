#
# Calculates the SVD of a data matrix containing missing entries.
#
# Uses the iterative algorithm of Hastie et al. 1999
#

# Impute a missing entries using current approximation.
function impute!(X::Matrix, missing_entries::Vector,
                 U::Matrix, D::Vector, V::Matrix,
                 k::Integer)
    approximation = U[:, 1:k] * diagm(D[1:k]) * V[1:k, :]
    for indices in missing_entries
        X[indices[1], indices[2]] = approximation[indices[1], indices[2]]
    end
end

# Should be done with a proper N-dimensional Int64 array.
function findna(dm::DataMatrix)
    indices = {}
    n, p = size(dm)
    for i = 1:n
        for j = 1:p
            if isna(dm[i, j])
                push!(indices, [i, j])
            end
        end
    end
    indices
end

function global_mean(dm::DataMatrix)
    mu = 0.0
    n = 0
    n, p = size(dm)
    for i = 1:n
        for j = 1:p
            if !isna(dm[i, j])
                mu += dm[i, j]
                n += 1
            end
        end
    end
    return mu / n
end

function na_safe_rowmeans(dm::DataMatrix)
    n, p = size(dm)
    mus = DataArray(Float64, n)
    for i = 1:n
        mu = 0.0
        n = 0
        for j = 1:p
            if !isna(dm[i, j])
                mu += dm[i, j]
                n += 1
            end
        end
        if n != 0
            mus[i] = mu / n
        end
    end
    return mus
end

# TODO: Default to failure in the face of NA's
function svd(D::DataMatrix, k::Integer, opts::Options)
    @defaults opts tracing => false
    @defaults opts tolerance => 10e-4

    # Make a copy of the data that we can alter in place
    dm = copy(D)

    # Cache the dimensions of the matrix
    n, p = size(dm)

    # Estimate missingness and print a message.
    missing_entries = findna(dm)
    missingness = length(missing_entries) / (n * p)
    if tracing
        @printf "Matrix is missing %.2f%% of entries\n" missingness * 100
    end

    # Initial imputation uses global mean and row means
    global_mu = global_mean(dm)
    mu_i = na_safe_rowmeans(dm)
    for i = 1:n
        for j = 1:p
            if isna(dm[i, j])
                if isna(mu_i[i])
                    dm[i, j] = global_mu
                else
                    dm[i, j] = mu_i[i]
                end
            end
        end
    end

    # Convert dm to a Float array now that we've removed all NA's
    dm = float(dm)

    # Count iterations of proper imputation method
    itr = 0

    # Keep track of approximate matrices
    previous_dm = copy(dm)
    current_dm = copy(dm)

    # Keep track of Frobenius norm of changes in imputed matrix
    change = Inf

    # Iterate until imputation stops changing up to chosen tolerance
    while change > tolerance
        if tracing
            @printf "Iteration %d\nChange %f\n" itr change
        end

        # Impute missing entries using current SVD
        previous_dm = copy(current_dm)
        U, D, V = svd(current_dm, true)
        impute!(current_dm, missing_entries, U, D, V', k)

        # Compute the change in the matrix across iterations
        change = norm(previous_dm - current_dm) / norm(dm)

        # Increment the iteration counter
        itr = itr + 1
    end

    # Tell the user how many iterations were required to impute matrix
    if tracing
        @printf "Tolerance achieved after %d iterations" itr
    end

    # Return the rank-k SVD entries
    U, D, V = svd(current_dm, true)

    # Only return the SVD entries, not the imputation
    return (U[:, 1:k], D[1:k], V[:, 1:k])
end
svd(dm::DataMatrix, k::Integer) = svd(dm, k, Options())
svd(dm::DataMatrix) = svd(dm, min(size(dm)), Options())

function eig(dm::DataMatrix)
    U, D, V = svd(dm)
    return eig(U * diagm(D) * V')
end
