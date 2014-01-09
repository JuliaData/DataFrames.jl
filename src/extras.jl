const letters = convert(Vector{ASCIIString}, split("abcdefghijklmnopqrstuvwxyz", ""))
const LETTERS = convert(Vector{ASCIIString}, split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", ""))

# Like string(s), but preserves Vector{String} and converts
# Vector{Any} to Vector{String}.
_vstring{T <: String}(s::T) = s
_vstring{T <: String}(s::AbstractVector{T}) = s
_vstring(s::AbstractVector) = String[_vstring(x) for x in s]
_vstring(s::Any) = string(s)
vcatstring(x) = vcat(_vstring(x))

function paste(s...)
    s = map(vcatstring, {s...})
    sa = {s...}
    N = maximum(length, sa)
    res = fill("", N)
    for i in 1:length(sa)
        Ni = length(sa[i])
        k = 1
        for j = 1:N
            res[j] = string(res[j], sa[i][k])
            if k == Ni   # This recycles array elements.
                k = 1
            else
                k += 1
            end
        end
    end
    res
end

function paste_columns(d::AbstractDataFrame, sep)
    res = fill("", nrow(d))
    for j in 1:ncol(d)
        for i in 1:nrow(d)
            res[i] *= string(d[i,j])
            if j != ncol(d)
                res[i] *= sep
            end
        end
    end
    res
end
paste_columns(d::AbstractDataFrame) = paste_columns(d, "_")

##############################################################################
##
## rep()
##
##############################################################################

function rep(x::AbstractVector, lengths::Vector{Int})
    if length(x) != length(lengths)
        error("vector lengths must match")
    end
    res = similar(x, sum(lengths))
    i = 1
    for idx in 1:length(x)
        tmp = x[idx]
        for kdx in 1:lengths[idx]
            res[i] = tmp
            i += 1
        end
    end
    res
end
rep(x::AbstractVector, times::AbstractVector{Int}, each::Int) = rep(x, times)

function rep(x::AbstractVector, times::Int = 1, each::Int = 1)
    res = similar(x, each * times * length(x))
    i = 1
    for jdx in 1:times
        for idx in 1:length(x)
            for kdx in 1:each
                res[i] = x[idx]
                i += 1
            end
        end
    end
    res
end

rep(x, times) = fill(x, times)

function rep(x; times = 1, each::Int = 1)
    rep(x, times, each)
end
