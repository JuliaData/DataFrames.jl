##############################################################################
##
## table()
##
##############################################################################

function table{T}(d::AbstractDataVector{T})
    counts = Dict{Union(T, NAtype), Int}(0)
    for i = 1:length(d)
        if has(counts, d[i])
            counts[d[i]] += 1
        else
            counts[d[i]] = 1
        end
    end
    return counts
end

##############################################################################
##
## paste()
##
##############################################################################

const letters = convert(Vector{ASCIIString}, split("abcdefghijklmnopqrstuvwxyz", ""))
const LETTERS = convert(Vector{ASCIIString}, split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", ""))

# Like string(s), but preserves Vector{String} and converts
# Vector{Any} to Vector{String}.
_vstring{T <: String}(s::T) = s
_vstring{T <: String}(s::Vector{T}) = s
_vstring(s::Vector) = map(_vstring, s)
_vstring(s::Any) = string(s)

function paste{T<:String}(s::Vector{T}...)
    sa = {s...}
    N = max(length, sa)
    res = fill("", N)
    for i in 1:length(sa)
        Ni = length(sa[i])
        k = 1
        for j = 1:N
            res[j] = strcat(res[j], sa[i][k])
            if k == Ni   # This recycles array elements.
                k = 1
            else
                k += 1
            end
        end
    end
    res
end
# The following converts all arguments to Vector{<:String} before
# calling paste.
function paste(s...)
    converted = map(vcat * _vstring, {s...})
    paste(converted...)
end

##############################################################################
##
## cut()
##
##############################################################################

function cut{S, T}(x::Vector{S}, breaks::Vector{T})
    if !issorted(breaks)
        sort!(breaks)
    end
    min_x, max_x = min(x), max(x)
    if breaks[1] > min_x
        unshift!(breaks, min_x)
    end
    if breaks[end] < max_x
        push!(breaks, max_x)
    end
    refs = fill(POOLED_DATA_VEC_REF_CONVERTER(0), length(x))
    for i in 1:length(x)
        if x[i] == min_x
            refs[i] = 1
        else
            refs[i] = search_sorted(breaks, x[i]) - 1
        end
    end
    n = length(breaks)
    from = map(x -> sprint(showcompact, x), breaks[1:(n - 1)])
    to = map(x -> sprint(showcompact, x), breaks[2:n])
    pool = Array(ASCIIString, n - 1)
    if breaks[1] == min_x
        pool[1] = strcat("[", from[1], ",", to[1], "]")
    else
        pool[1] = strcat("(", from[1], ",", to[1], "]")
    end
    for i in 2:(n - 1)
        pool[i] = strcat("(", from[i], ",", to[i], "]")
    end
    PooledDataArray(refs, pool)
end
cut(x::Vector, ngroups::Int) = cut(x, quantile(x, [1 : ngroups - 1] / ngroups))
