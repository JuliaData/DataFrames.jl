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
    reindex = [findfirst(contrast.levels, l) for l in levels(v)]

    ## TODO: add kwarg for full-rank contrasts (e.g., when intercept isn't
    ## specified in a model frame).

    return contrast.matrix[reindex[v.refs], :]
end

## Constructing a contrast from a non-pooled data vector will first pool it
## 
## Also requires a cols method for non-PooledDataArray column...
## Base.call{T<: AbstractContrast}(C::Type{T}, v::DataVector, args...; kwargs...) =
##     Base.call(C, pool(v), args...; kwargs...)


## Making a contrast type T only requires that there be a method for
## contrast_matrix(T, v::PooledDataArray). The rest is boilerplate.
##
for contrastType in [:TreatmentContrast, :SumContrast, :HelmertContrast]
    @eval begin
        type $contrastType{T} <: AbstractContrast
            base::Integer
            matrix::Matrix{Float64}
            termnames::Vector{T}
            levels::Vector{T}
        end

        function $contrastType{T}(v::PooledDataVector{T}; base::Integer=1)
            lvls = levels(v)

            n = length(lvls)
            n > 1 || error("not enough degrees of freedom to define contrasts")
            (1 <= base <= n) || error("base = $(base) is not allowed for n = $n")

            not_base = [1:(base-1); (base+1):n]
            tnames = lvls[not_base]

            mat = contrast_matrix($contrastType, n, base)

            return $contrastType{T}(base, mat, tnames, lvls)
        end
    end
end

## Could write this as a macro, too, so that people can register their own
## contrast types easily, without having to write out this boilerplate...the
## downside of that would be that they'd be locked in to a particular set of
## fields...although they could always just write the boilerplate themselves...

################################################################################
## Dummy contrasts (full rank)
##
## Needed when a term is non-redundant with lower-order terms (e.g., in ~0+x vs.
## ~1+x, or in the interactions terms in ~1+x+x&y vs. ~1+x+y+x&y.  In the
## non-redundant cases, we can (probably) expand x into length(levels(x))
## columns without creating a non-identifiable model matrix (unless the user
## has done something dumb in specifying the model, which we can't do much about
## anyway).
################################################################################

## Dummy contrasts have no base level (since all levels produce a column)
type DummyContrast{T} <: AbstractContrast
    matrix::Matrix{Float64}
    termnames::Vector{T}
    levels::Vector{T}
end

function DummyContrast{T}(v::PooledDataVector{T})
    lvls = levels(v)
    mat = eye(Float64, length(lvls))

    DummyContrast(mat, lvls, lvls)
end

## Default for promoting contrasts to full rank is to convert to dummy contrasts
promote_contrast(C::AbstractContrast) = DummyContrast(eye(Float64, length(C.levels)), C.levels, C.levels)




################################################################################
## Treatment (rank-reduced dummy-coded) contrast
################################################################################

contrast_matrix(::Type{TreatmentContrast}, n, base) = eye(n)[:, [1:(base-1); (base+1):n]]


################################################################################
## Sum-coded contrast
##
## -1 for base level and +1 for contrast level.
################################################################################

function contrast_matrix(::Type{SumContrast}, n, base)
    not_base = [1:(base-1); (base+1):n]
    mat = eye(n)[:, not_base]
    mat[base, :] = -1
    return mat
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

function contrast_matrix(::Type{HelmertContrast}, n, base)
    mat = zeros(n, n-1)
    for i in 1:n-1
        mat[1:i, i] = -1
        mat[i+1, i] = i
    end

    ## re-shuffle the rows such that base is the all -1.0 row (currently first)
    mat = mat[[base; 1:(base-1); (base+1):end], :]
    return mat
end
    
