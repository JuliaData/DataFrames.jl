## Specify contrasts for coding categorical data in model matrix. Contrast types
## are a subtype of AbstractContrast. ContrastMatrix types hold a contrast
## matrix, levels, and term names and provide the interface for creating model
## matrix columns and coefficient names.
##
## Contrast types themselves can be instantiated to provide containers for
## contrast settings (currently, just the base level).
##
## ModelFrame will hold a Dict{Symbol, ContrastMatrix} that maps column
## names to contrasts.
##
## ModelMatrix will check this dict when evaluating terms, falling back to a
## default for any categorical data without a specified contrast.
##
## TODO: implement contrast types in Formula/Terms

abstract AbstractContrast

## Contrast + Data = ContrastMatrix
type ContrastMatrix{T}
    matrix::Matrix{Float64}
    termnames::Vector{T}
    levels::Vector{T}
    contrasts::AbstractContrast
end

function ContrastMatrix{T}(C::AbstractContrast, lvls::Vector{T})
    n = length(lvls)
    n > 1 || error("not enough degrees of freedom to define contrasts")
    (1 <= C.base <= n) || error("base = $(base) is not allowed for n = $n")

    not_base = [1:(C.base-1); (C.base+1):n]
    tnames = lvls[not_base]

    mat = contrast_matrix(C, n)

    ContrastMatrix(mat, tnames, lvls, C)
end    

ContrastMatrix(C::AbstractContrast, v::PooledDataArray) = ContrastMatrix(C, levels(v))


termnames(term::Symbol, col::Any, contrast::ContrastMatrix) =
    ["$term - $name" for name in contrast.termnames]

function cols(v::PooledDataVector, contrast::ContrastMatrix)
    ## make sure the levels of the contrast matrix and the categorical data
    ## are the same by constructing a re-indexing vector. Indexing into
    ## reindex with v.refs will give the corresponding row number of the
    ## contrast matrix
    reindex = [findfirst(contrast.levels, l) for l in levels(v)]
    return contrast.matrix[reindex[v.refs], :]
end

## Making a contrast type T only requires that there be a method for
## contrast_matrix(T, v::PooledDataArray). The rest is boilerplate.
##
for contrastType in [:TreatmentContrast, :SumContrast, :HelmertContrast]
    @eval begin
        type $contrastType <: AbstractContrast
            base::Integer
        end
        ## default base level is 1:
        $contrastType(; base::Integer=1) = $contrastType(base)
    end
end

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
type DummyContrast <: AbstractContrast
end

ContrastMatrix{T}(C::DummyContrast, lvls::Vector{T}) = ContrastMatrix(eye(Float64, length(lvls)), lvls, lvls, C)

## Default for promoting contrasts to full rank is to convert to dummy contrasts
## promote_contrast(C::AbstractContrast) = DummyContrast(eye(Float64, length(C.levels)), C.levels, C.levels)

promote_contrast(C::ContrastMatrix) = ContrastMatrix(DummyContrast(), C.levels)




################################################################################
## Treatment (rank-reduced dummy-coded) contrast
################################################################################

contrast_matrix(C::TreatmentContrast, n) = eye(n)[:, [1:(C.base-1); (C.base+1):n]]


################################################################################
## Sum-coded contrast
##
## -1 for base level and +1 for contrast level.
################################################################################

function contrast_matrix(C::SumContrast, n)
    not_base = [1:(C.base-1); (C.base+1):n]
    mat = eye(n)[:, not_base]
    mat[C.base, :] = -1
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

function contrast_matrix(C::HelmertContrast, n)
    mat = zeros(n, n-1)
    for i in 1:n-1
        mat[1:i, i] = -1
        mat[i+1, i] = i
    end

    ## re-shuffle the rows such that base is the all -1.0 row (currently first)
    mat = mat[[C.base; 1:(C.base-1); (C.base+1):end], :]
    return mat
end
    
