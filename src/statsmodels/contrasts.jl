## Specifying contrast coding for categorical data.  General strategy is to
## define types for each kind of contrast coding (treatment, sum, normalized sum,
## Helmert, etc.).  Constructing object with data will wrap the data and
## calculate the contrast matrix.  Objects will provide methods for generating
## contrast coded matrix for ModelMatrix construction, and term names.
##


## Okay what's the goal here?  Just to be able to call the `cols` and
## `termnames` functions on the column in the DataFrame and have it work
## automagically.
##
## The operations that need to happen are
## * contrast matrix given data (or number of contrasts)
## * make contrast-coded design matrix columns
## * make column names
##
## What about from the user's point of view?  How do you say that you want to
## use a particular kind of contrast?  Replace or assign the contrast wrapped
## data to the parent data frame.
##
## OKay, but having a container type is going to be a huge pain in the ass:
## you have to duplicate all the PooledDataArray functionality so that it can
## be used interchangeably.  I mean, maybe not, if it's enough to just write
## down all the methods that need to be delegated and shove them all through
## a macro (although I suspect it's not quite that simple).
##
## So instead the right place to specify this is in the ModelFrame.  Perhaps
## a Dict{Symbol,T<:AbstractContrast}, initialized to empty, and checked on
## construction of ModelMatrix.  Only change would be to change the cols() calls
## in the ModelMatrix constructor to take the contrasts into account, and the
## call to coefnames
##
## Other issue: what about specifying a contrast matrix manually?  Maybe have
## a type called ManualContrast that has fields for the matrix and the name?
## And if a matrix is passed to the ModelFrame then it'll first wrap it in the
## contrast type, then wrap the data, then call cols.  This seems needlessly
## complicated to say the least. Better migth just be to add a version of the
## `cols` function that takes a Contrast or a matrix and does the right thing,
## and can still benefit from the multiple dispatch.
## 
## 
##
## So I'll have two kinds of types:
## ContrastedDataArray - container for data, parametrized by kind of contrast
## ContrastTreatment <: AbstractContrast - For dispatching methods for making
##   specific contrast matrix, contrast columns, and names.
##
## AbstractContrast provides interface
##
## contrast_matrix(::AbstractContrast, v::PooledDataVector, ...) - construct
##   the contrast matrix for this contrast type and data.  Might optionally
##   provide other arguments (like the base level, whether to make it sparse,
##   etc.).  The columns of the contrast matrix correspond to the columsn that
##   will be put into the design matrix, and the rows correspond to the levels of
##   the input data.


abstract AbstractContrast

## contrast_matrix{T<:AbstractContrast}(C::Type{T}, v::PooledDataArray, args...; kwargs...) =
##     contrast_matrix(C(), length(levels(v)), args...; kwargs...)

## Allow specification of contrast Types (e.g. `SumContrast`) in addition to
## instantiated contrast objects (e.g. `SumContrast(base=1)`).
contrast_matrix{T<:AbstractContrast}(C::Type{T}, args...; kwargs...) =
    contrast_matrix(C(), args...; kwargs...)

## Generic generation of columns for design matrix based on a contrast matrix
function cols(v::PooledDataVector, contr_mat::Matrix)
    ## validate provided matrix
    n = length(levels(v))
    ## number of columns has to be < n-1
    dims = size(contr_mat)
    if dims[2] >= n
        error("Too many columns in contrast matrix: $(dims[2]) (only $n levels)")
    elseif dims[1] < n
        error("Not enough rows in contrast matrix: $(dims[1]) ($n levels)")
    end

    return contr_mat[v.refs, :]
end

## Default contrast is treatment:
cols(v::PooledDataVector) = cols(v, TreatmentContrast)
## Make contrast columns from contrast object:
cols(v::PooledDataVector, C::AbstractContrast) = cols(v, contrast_matrix(C,v))
## Make contrast columsn from contrast _type_:
cols{T<:AbstractContrast}(v::PooledDataVector, C::Type{T}) = cols(v, C())


## Default names for contrasts are just the level of the PDV used to construct.
termnames(term::Symbol, 
function contrast_names{T<:AbstractContrast}(C::Type{T}, v::PooledDataVector)
    levs = levels(v)
    return levs[2:end]
end



################################################################################
## Treatment (dummy-coded) contrast
################################################################################

type TreatmentContrast <: AbstractContrast
    sparse::Bool
    base::Integer
end

TreatmentContrast(; sparse::Bool=false, base::Integer=1) = TreatmentContrast(sparse, base)

function contrast_matrix(C::TreatmentContrast, v::PooledDataVector)
    n = length(levels(v))          # number of levels for contrast
    if n < 2 error("not enought degrees of freedom to define contrasts") end

    contr = C.sparse ? speye(n) : eye(n)

    ## Drop the base level column from the contrast matrix
    if !(1 <= C.base <= n) error("base = $(C.base) is not allowed for n = $n") end
    contr[:,vcat(1:(C.base-1),(C.base+1):end)]
end

################################################################################
## Sum-coded contrast
##
## -1 for base level and +1 for contrast level.
################################################################################

type SumContrast <: AbstractContrast 
    base::Integer
end

SumContrast(; base::Integer=1) = SumContrast(base)

function contrast_matrix(C::SumContrast, v::PooledDataVector)
    n = length(levels(v))
    if n < 2 error("not enought degrees of freedom to define contrasts") end
    if !(1 <= C.base <= n) error("base = $(C.base) is not allowed for n = $n") end
    contr = eye(n)[:, [1:(C.base-1), (C.base+1):end]]
    contr[C.base, :] = -1
    return contr
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
################################################################################

type HelmertContrast <: AbstractContrast 
    base::Integer
end

HelmertContrast(; base::Integer=1) = HelmertContrast(base)

function contrast_matrix(C::HelmertContrast, v::PooledDataVector)
    n = length(levels(v))
    if n < 2 error("not enought degrees of freedom to define contrasts") end
    if !(1 <= C.base <= n) error("base = $(C.base) is not allowed for n = $n") end

    contr = zeros(Integer, n, n-1)
    for i in 1:n-1
        contr[1:i, i] = -1
        contr[i+1, i] = i
    end

    return contr[[C.base, 1:(C.base-1), (C.base+1):end], :]
end


################################################################################

function contrast_matrix(::AbstractContrast, args...)
    error("Contrast not implemented")
end
