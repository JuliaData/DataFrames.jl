# slow, but maintains order and seems to work:
function _setdiff(a::Vector, b::Vector)
    idx = Int[]
    for i in 1:length(a)
        if !(a[i] in b)
            push!(idx, i)
        end
    end
    a[idx]
end

function _uniqueofsorted(x::Vector)
    idx = fill(true, length(x))
    lastx = x[1]
    for i = 2:length(x)
        if lastx == x[i]
            idx[i] = false
        else
            lastx = x[i]
        end
    end
    x[idx]
end

function make_unique{S<:ByteString}(names::Vector{S})
    x = Index()
    names = copy(names)
    dups = Int[]
    for i in 1:length(names)
        if haskey(x, names[i])
            push!(dups, i)
        else
            push!(x, names[i])
        end
    end
    for i in dups
        nm = names[i]
        newnm = nm
        k = 1
        while true
            newnm = "$(nm)_$k"
            if !haskey(x, newnm)
                push!(x, newnm)
                break
            end
            k += 1
        end
        names[i] = newnm
    end
    names
end

#' @description
#'
#' Generate standardized names for columns of a DataFrame. The
#' first name will be "x1", the second "x2", etc.
#'
#' @field n::Integer The number of names to generate.
#'
#' @returns names::Vector{UTF8String} A vector of standardized column names.
#'
#' @examples
#'
#' DataFrames.gennames(10)
function gennames(n::Integer)
    res = Array(UTF8String, n)
    for i in 1:n
        res[i] = @sprintf "x%d" i
    end
    return res
end

#' @description
#'
#' Count the number of missing values in an Array.
#'
#' NOTE: This function always returns 0.
#'
#' @field a::Array The Array whose missing values are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna([1, 2, 3])
countna(a::Array) = 0

#' @description
#'
#' Count the number of missing values in a DataArray.
#'
#' @field da::DataArray The DataArray whose missing values are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna(@data([1, 2, 3]))
function countna(da::DataArray)
    n = length(da)
    res = 0
    for i in 1:n
        if da.na[i]
            res += 1
        end
    end
    return res
end

#' @description
#'
#' Count the number of missing values in a PooledDataArray.
#'
#' @field pda::PooledDataArray The PooledDataArray whose missing values
#'        are to be counted.
#'
#' @returns count::Int The number of missing values in `a`.
#'
#' @examples
#'
#' DataFrames.countna(@pdata([1, 2, 3]))
function countna(da::PooledDataArray)
    n = length(da)
    res = 0
    for i in 1:n
        if da.refs[i] == 0
            res += 1
        end
    end
    return res
end
