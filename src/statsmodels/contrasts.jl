## Specify contrasts for coding categorical data in model matrix. Contrasts types
## are a subtype of AbstractContrasts. ContrastsMatrix types hold a contrast
## matrix, levels, and term names and provide the interface for creating model
## matrix columns and coefficient names.
##
## Contrasts types themselves can be instantiated to provide containers for
## contrast settings (currently, just the base level).
##
## ModelFrame will hold a Dict{Symbol, ContrastsMatrix} that maps column
## names to contrasts.
##
## ModelMatrix will check this dict when evaluating terms, falling back to a
## default for any categorical data without a specified contrast.
##
## TODO: implement contrast types in Formula/Terms
##
## Dave Kleinschmidt 2015-2016


"""
    AbstractContrasts(; base::Any=NULL, levels::Vector=NULL)

Interface to describe contrast coding schemes for categorical variables.

Concrete subtypes of `AbstractContrasts` describe a particular way of converting a
categorical column in a `DataFrame` into numeric columns in a `ModelMatrix`. Each
instantiation optionally includes the levels to generate columns for and the base
level. If not specified these will be taken from the data when a `ContrastsMatrix` is
generated (during `ModelFrame` construction).

# Arguments

* `levels::Nullable{Vector}=NULL`: If specified, will be checked against data when
  generating a `ContrastsMatrix`. Levels that are specified here but missing in the
  data will result in an error, because this would lead to empty columns in the
  resulting ModelMatrix.
* `base::Nullable{Any}=NULL`: The base level of the contrast. The
  actual interpretation of this depends on the particular contrast type, but in
  general it can be thought of as a "reference" level.
"""
abstract AbstractContrasts

## Contrasts + Data = ContrastsMatrix
type ContrastsMatrix{T}
    matrix::Matrix{Float64}
    termnames::Vector{T}
    levels::Vector{T}
    contrasts::AbstractContrasts
end

"""
    ContrastsMatrix{T}(::AbstractContrasts, levels::Vector{T})

Instantiate contrasts matrix for given data (categorical levels)

If levels are specified in the `AbstractContrasts`, those will be used, and likewise
for the base level (which defaults to the first level).
"""
function ContrastsMatrix{T}(C::AbstractContrasts, lvls::Vector{T})

    ## if levels are defined on C, use those, validating that they line up.
    ## what does that mean?
    ##
    ## C.levels == lvls (best case)
    ## data levels missing from contrast: would generate empty/undefined rows. 
    ## better to filter data frame first
    ## contrast levels missing from data: would have empty columns, generate a
    ## rank-deficient model matrix.
    c_lvls = convert(typeof(lvls), get(C.levels, lvls))
    mismatched_lvls = symdiff(c_lvls, lvls)
    isempty(mismatched_lvls) || error("Contrasts levels not found in data or vice-versa: ", mismatched_lvls)

    n = length(c_lvls)
    n > 1 || error("not enough degrees of freedom to define contrasts")
    
    ## find index of base level. use C.base, then default (1).
    baseind = isnull(C.base) ? 1 : findfirst(c_lvls, convert(eltype(lvls), get(C.base)))
    baseind > 0 || error("Base level $(C.base) not found in levels")
    
    not_base = [1:(baseind-1); (baseind+1):n]
    tnames = c_lvls[not_base]

    mat = contrasts_matrix(C, baseind, n)

    ContrastsMatrix(mat, tnames, c_lvls, C)
end

ContrastsMatrix(C::AbstractContrasts, v::PooledDataArray) = ContrastsMatrix(C, levels(v))


nullify(x::Nullable) = x
nullify(x) = Nullable(x)

## Making a contrast type T only requires that there be a method for
## contrasts_matrix(T, v::PooledDataArray). The rest is boilerplate.
##
for contrastType in [:TreatmentContrasts, :SumContrasts, :HelmertContrasts]
    @eval begin
        type $contrastType <: AbstractContrasts
            base::Nullable{Any}
            levels::Nullable{Vector}
        end
        ## constructor with optional keyword arguments, defaulting to Nullables
        $contrastType(;
                      base=Nullable{Any}(),
                      levels=Nullable{Vector}()) = 
                          $contrastType(nullify(base),
                                        nullify(levels))
    end
end

"""
    DummyContrasts

One indicator (1 or 0) column for each level, __including__ the base level.

Needed internally when a term is non-redundant with lower-order terms (e.g., in
~0+x vs. ~1+x, or in the interactions terms in ~1+x+x&y vs. ~1+x+y+x&y. In the
non-redundant cases, we can (probably) expand x into length(levels(x)) columns
without creating a non-identifiable model matrix (unless the user has done
something dumb in specifying the model, which we can't do much about anyway).
"""
type DummyContrasts <: AbstractContrasts
## Dummy contrasts have no base level (since all levels produce a column)
end

ContrastsMatrix{T}(C::DummyContrasts, lvls::Vector{T}) = ContrastsMatrix(eye(Float64, length(lvls)), lvls, lvls, C)

## Default for promoting contrasts to full rank is to convert to dummy contrasts
## promote_contrast(C::AbstractContrasts) = DummyContrasts(eye(Float64, length(C.levels)), C.levels, C.levels)

"Promote a contrast to full rank"
promote_contrast(C::ContrastsMatrix) = ContrastsMatrix(DummyContrasts(), C.levels)




"""
    TreatmentContrasts

One indicator column (1 or 0) for each non-base level.

The default in R. Columns have non-zero mean and are collinear with an intercept
column (and lower-order columns for interactions) but are orthogonal to each other.
"""
TreatmentContrasts

contrasts_matrix(C::TreatmentContrasts, baseind, n) = eye(n)[:, [1:(baseind-1); (baseind+1):n]]


"""
    SumContrasts

Column for level `x` of column `col` is 1 where `col .== x` and -1 where
`col .== base`.

Produces mean-0 (centered) columns _only_ when all levels are equally frequent.
But with more than two levels, the generated columns are guaranteed to be non-
orthogonal (so beware of collinearity).
"""
SumContrasts

function contrasts_matrix(C::SumContrasts, baseind, n)
    not_base = [1:(baseind-1); (baseind+1):n]
    mat = eye(n)[:, not_base]
    mat[baseind, :] = -1
    return mat
end

"""
    HelmertContrasts

Produces contrast columns with -1 for each of n levels below contrast, n for
contrast level, and 0 above.  Produces something like:

```
[-1 -1 -1
  1 -1 -1
  0  2 -1
  0  0  3]
```

This is a good choice when you have more than two levels that are equally frequent.
Interpretation is that the nth contrast column is the difference between
level n+1 and the average of levels 1 to n.  When balanced, it has the
nice property of each column in the resulting model matrix being orthogonal and
with mean 0.
"""
HelmertContrasts

function contrasts_matrix(C::HelmertContrasts, baseind, n)
    mat = zeros(n, n-1)
    for i in 1:n-1
        mat[1:i, i] = -1
        mat[i+1, i] = i
    end

    ## re-shuffle the rows such that base is the all -1.0 row (currently first)
    mat = mat[[baseind; 1:(baseind-1); (baseind+1):end], :]
    return mat
end
    
