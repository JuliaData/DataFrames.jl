# Specify contrasts for coding categorical data in model matrix. Contrasts types
# are a subtype of AbstractContrasts. ContrastsMatrix types hold a contrast
# matrix, levels, and term names and provide the interface for creating model
# matrix columns and coefficient names.
#
# Contrasts types themselves can be instantiated to provide containers for
# contrast settings (currently, just the base level).
#
# ModelFrame will hold a Dict{Symbol, ContrastsMatrix} that maps column
# names to contrasts.
#
# ModelMatrix will check this dict when evaluating terms, falling back to a
# default for any categorical data without a specified contrast.
#
# TODO: implement contrast types in Formula/Terms


"""
Interface to describe contrast coding schemes for categorical variables.

Concrete subtypes of `AbstractContrasts` describe a particular way of converting a
categorical data vector into numeric columns in a `ModelMatrix`. Each
instantiation optionally includes the levels to generate columns for and the base
level. If not specified these will be taken from the data when a `ContrastsMatrix` is
generated (during `ModelFrame` construction).

# Constructors

For `C <: AbstractContrast`:

```julia
C()                                     # levels are inferred later 
C(levels = ::Vector{Any})               # levels checked against data later
C(base = ::Any)                         # specify base level
C(levels = ::Vector{Any}, base = ::Any) # specify levels and base
```

If specified, levels will be checked against data when generating a
`ContrastsMatrix`. Any mismatch will result in an error, because missing data
levels would lead to empty columns in the model matrix, and missing contrast
levels would lead to empty or undefined rows.

You can also specify the base level of the contrasts. The
actual interpretation of this depends on the particular contrast type, but in
general it can be thought of as a "reference" level.  It defaults to the first
level.

Both `levels` and `base` will be coerced to the type of the data when
constructing a `ContrastsMatrix`.

# Concrete types

* `TreatmentContrasts`
* `SumContrasts`
* `HelmertContrasts`

To implement your own concrete types, implement a constructor and a
`contrast_matrix` method for constructing the actual contrasts matrix
that maps from levels to `ModelMatrix` column values:

```julia
type MyContrasts <: AbstractContrasts
    ...
end

contrasts_matrix(C::MyContrasts, baseind, n) = ...
```

"""
abstract AbstractContrasts

# Contrasts + Levels (usually from data) = ContrastsMatrix
type ContrastsMatrix{S <: AbstractContrasts, T}
    matrix::Matrix{Float64}
    termnames::Vector{T}
    levels::Vector{T}
    contrasts::S
end

"""
    ContrastsMatrix{T}(C::AbstractContrasts, levels::Vector{T})

Compute contrasts matrix for a given set of categorical data levels.

If levels are specified in the `AbstractContrasts`, those will be used, and likewise
for the base level (which defaults to the first level).
"""
function ContrastsMatrix{C <: AbstractContrasts}(contrasts::C, levels::Vector)

    # if levels are defined on contrasts, use those, validating that they line up.
    # what does that mean? either:
    #
    # 1. contrasts.levels == levels (best case)
    # 2. data levels missing from contrast: would generate empty/undefined rows. 
    #    better to filter data frame first
    # 3. contrast levels missing from data: would have empty columns, generate a
    #    rank-deficient model matrix.
    c_levels = oftype(levels, get(contrasts.levels, levels))
    mismatched_levels = symdiff(c_levels, levels)
    isempty(mismatched_levels) || error("contrasts levels not found in data or vice-versa: $mismatched_levels.\nData levels: $levels.\nContrast levels: $c_levels")

    n = length(c_levels)
    n == 0 && error("empty set of levels found (need at least two to compute contrasts).")
    n == 1 && error("only one level found: $(c_levels[1]). need at least two to compute contrasts.")
    
    # find index of base level. use contrasts.base, then default (1).
    baseind = isnull(contrasts.base) ?
              1 :
              findfirst(c_levels, convert(eltype(levels), get(contrasts.base)))
    baseind > 0 || error("base level $(contrasts.base) not found in levels $c_levels.")

    tnames = termnames(contrasts, c_levels, baseind)

    mat = contrasts_matrix(contrasts, baseind, n)

    ContrastsMatrix(mat, tnames, c_levels, contrasts)
end

ContrastsMatrix(C::AbstractContrasts, v::PooledDataArray) = ContrastsMatrix(C, levels(v))
ContrastsMatrix{C <: AbstractContrasts}(c::Type{C}, col::PooledDataArray) = ContrastsMatrix(c(), col)
ContrastsMatrix(c::ContrastsMatrix, col::PooledDataArray) =
    isempty(symdiff(c.levels, levels(col))) ?
    c :
    error("mismatch between levels in ContrastsMatrix and data:\nData levels: $(levels(col))\nContrast levels $(c.levels)")

function termnames(C::AbstractContrasts, levels::Vector, baseind::Integer)
    not_base = [1:(baseind-1); (baseind+1):length(levels)]
    levels[not_base]
end

nullify(x::Nullable) = x
nullify(x) = Nullable(x)

# Making a contrast type T only requires that there be a method for
# contrasts_matrix(T, v::PooledDataArray). The rest is boilerplate.
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
`~0+x` vs. `~1+x`, or in the interactions terms in `~1+x+x&y` vs. `~1+x+y+x&y`. In the
non-redundant cases, we can expand x into `length(levels(x))` columns
without creating a non-identifiable model matrix (unless the user has done
something foolish in specifying the model, which we can't do much about anyway).
"""
type DummyContrasts <: AbstractContrasts
# Dummy contrasts have no base level (since all levels produce a column)
end

ContrastsMatrix{T}(C::DummyContrasts, lvls::Vector{T}) = ContrastsMatrix(eye(Float64, length(lvls)), lvls, lvls, C)

"Promote contrasts matrix to full rank version"
Base.convert(::Type{ContrastsMatrix{DummyContrasts}}, C::ContrastsMatrix) = ContrastsMatrix(DummyContrasts(), C.levels)

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

    # re-shuffle the rows such that base is the all -1.0 row (currently first)
    mat = mat[[baseind; 1:(baseind-1); (baseind+1):end], :]
    return mat
end
    
