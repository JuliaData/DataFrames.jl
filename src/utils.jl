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

countna(a::Array) = 0

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
