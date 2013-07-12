# This is multiplicative analog of diff
function reldiff{T}(v::Vector{T})
    n = length(v)
    res = Array(T, n - 1)
    for i in 2:n
        res[i - 1] = v[i] / v[i - 1]
    end
    return res
end

# Diff scaled by previous value
function percent_change{T}(v::Vector{T})
    n = length(v)
    res = Array(T, n - 1)
    for i in 2:n
        res[i - 1] = (v[i] - v[i - 1]) / v[i - 1]
    end
    return res
end

autocor{T}(dv::DataVector{T}, lag::Int) = cor(dv[1:(end - lag)], dv[(1 + lag):end])
autocor{T}(dv::DataVector{T}) = autocor(dv, 1)

# Generate levels - see the R documentation for gl
function gl(n::Integer, k::Integer, l::Integer)
    nk = n * k
    if l % nk != 0 error("length out must be a multiple of n * k") end
    aa = Array(Int, l)
    for j = 0:(l/nk - 1), i = 1:n
        aa[j * nk + (i - 1) * k + (1:k)] = i
    end
    compact(PooledDataArray(aa))
end

gl(n::Integer, k::Integer) = gl(n, k, n*k)

# A cross-tabulation type. Currently just a one-way table
type xtab{T}
    vals::Array{T}
    counts::Vector{Int}
end

function xtab{T}(x::AbstractArray{T})
    d = Dict{T, Int}()
    for el in x
        d[el] = get(d, el, 0) + 1
    end
    kk = sort(keys(d))
    cc = Array(Int, length(kk))
    for i in 1:length(kk)
        cc[i] = d[kk[i]]
    end
    return xtab(kk, cc)
end

# Another cross-tabulation function, this one leaves the result as a Dict
# Again, this is currently just for one-way tables.
function xtabs{T}(x::AbstractArray{T})
    d = Dict{T, Int}()
    for el in x
        d[el] = get(d, el, 0) + 1
    end
    return d
end
