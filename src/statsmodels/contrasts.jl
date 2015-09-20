## Specify contrasts for coding categorical data in model matrix. Contrast types
## are constructed with data and other options, creating a subtype of
## AbstractContrast. AbstractContrast provides the interface for creating modle
## matrix columns and coefficient names
##
## ModelFrame will hold a Dict{Symbol, T<:AbstractContrast} that maps column
## names to contrasts.
##
## ModelMatrix will check this dict when evaluating terms, falling back to a
## default for any categorical data without a specified contrast.

abstract AbstractContrast

termnames(term::Symbol, col::Any, contrast::AbstractContrast) =
    ["$term - $name" for name in contrast.termnames]

function cols(v::PooledDataVector, contrast::AbstractContrast)
    ## make sure the levels of the contrast matrix and the categorical data
    ## are the same by constructing a re-indexing vector. Indexing into
    ## reindex with v.refs will give the corresponding row number of the
    ## contrast matrix
    reindex = [findfirst(l .== contrast.levels) for l in levels(v)]

    ## TODO: add kwarg for full-rank contrasts (e.g., when intercept isn't
    ## specified in a model frame).

    return contrast.matrix[reindex[v.refs], :]
end

## Default contrast is treatment coding:
cols(v::PooledDataVector) = cols(v, TreatmentContrast(v))
termnames(term::Symbol, col::PooledDataArray) = termnames(term, col, TreatmentContrast(col))


## Constructing a contrast from a non-pooled data vector will first pool it
## 
## (NOT SURE THIS IS GOOD: constructing columns also depends on levels, so for
## _that_ to work would need to somehow hold onto pooled data...)
Base.call{T<: AbstractContrast}(C::Type{T}, v::DataVector, args...; kwargs...) =
    Base.call(C, pool(v), args...; kwargs...)

################################################################################
## Treatment (dummy-coded) contrast
################################################################################

## TODO: factor out some of this repetition between different contrast types
## using metaprogramming

type TreatmentContrast <: AbstractContrast
    base::Integer
    matrix::Array{Any,2}
    termnames::Vector{Any}
    levels::Vector{Any}
end

function TreatmentContrast(v::PooledDataVector; base::Integer=1)
    lvls = levels(v)

    n = length(lvls)
    if n < 2 error("not enought degrees of freedom to define contrasts") end

    not_base = [1:(base-1); (base+1):n]
    tnames = lvls[ not_base ]
    mat = eye(n)[:, not_base]

    return TreatmentContrast(base, mat, tnames, lvls)
end


################################################################################
## Sum-coded contrast
##
## -1 for base level and +1 for contrast level.
################################################################################

type SumContrast <: AbstractContrast
    base::Integer
    matrix::Array{Any,2}
    termnames::Vector{Any}
    levels::Vector{Any}
end

function SumContrast(v::PooledDataVector; base::Integer=1)
    lvls = levels(v)

    n = length(lvls)
    if n < 2 error("not enought degrees of freedom to define contrasts") end

    not_base = [1:(base-1); (base+1):n]
    tnames = lvls[ not_base ]
    mat = eye(n)[:, not_base]
    mat[base, :] = -1

    return SumContrast(base, mat, tnames, lvls)
end


################################################################################
## Helmert-coded contrast
##
## -1 for each of n levels below contrast, n for contrast level, and 0 above.
## Produces something like:
##
## [-1 -1 -1
##   1 -1 -1
##   0  2 -1
##   0  0  3]
##
## Interpretation is that the nth contrast column is the difference between
## level n+1 and the average of levels 1:n
##
## Has the nice property of each column in the resulting model matrix being
## orthogonal and with mean 0.
################################################################################

type HelmertContrast <: AbstractContrast
    base::Integer
    matrix::Array{Any,2}
    termnames::Vector{Any}
    levels::Vector{Any}
end

function HelmertContrast(v::PooledDataVector; base::Integer=1)
    lvls = levels(v)

    n = length(lvls)
    if n < 2 error("not enought degrees of freedom to define contrasts") end
    if !(1 <= base <= n) error("base = $(base) is not allowed for n = $n") end

    not_base = [1:(base-1); (base+1):n]
    tnames = lvls[ not_base ]

    mat = zeros(n, n-1)
    for i in 1:n-1
        mat[1:i, i] = -1
        mat[i+1, i] = i
    end

    ## re-shuffle the rows such that base is the all -1.0 row (currently first)
    mat = mat[[base; 1:(base-1); (base+1):end], :]

end
