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

autocor{T}(dv::DataVec{T}, lag::Int) = cor(dv[1:(end - lag)], dv[(1 + lag):end])
autocor{T}(dv::DataVec{T}) = autocor(dv, 1)
