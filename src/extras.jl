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
