#' @@name AbstractDataFrame
#'
#' @@description
#'
#' An AbstractDataFrame is a Julia abstract type for which all concrete
#' types expose an database-like interface.
abstract AbstractDataFrame <: Associative{String, Any}

#' @@name colmissing
#'
#' @@description
#'
#' Count the number of missing values in every column of an AbstractDataFrame.
#'
#' @@arg adf::AbstractDataFrame An AbstractDataFrame.
#'
#' @@return missing::Vector{Int} The number of missing values in each column.
#'
#' @@examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' colmissing(df)
function colmissing(adf::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(adf)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countna(adf[j])
    end
    return missing
end
