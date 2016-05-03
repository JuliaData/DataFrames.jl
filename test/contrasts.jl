module TestContrasts

using Base.Test
using DataFrames


d = DataFrame(x = @pdata( [:a, :b, :c, :a, :a, :b] ))

mf = ModelFrame(Formula(Nothing(), :x), d)

@test ModelMatrix(mf).m == [1  0  0
                            1  1  0
                            1  0  1
                            1  0  0
                            1  0  0
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]

contrast!(mf, x = SumContrasts)
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1  0
                            1  0  1
                            1 -1 -1
                            1 -1 -1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]

## change base level of contrast
contrast!(mf, x = SumContrasts(base = :b))
@test ModelMatrix(mf).m == [1  1  0
                            1 -1 -1
                            1  0  1
                            1  1  0
                            1  1  0
                            1 -1 -1]
@test coefnames(mf) == ["(Intercept)"; "x: a"; "x: c"]

## change levels of contrast
contrast!(mf, x = SumContrasts(levels = [:c, :b, :a]))
@test ModelMatrix(mf).m == [1  0  1
                            1  1  0
                            1 -1 -1
                            1  0  1
                            1  0  1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: a"]


## change levels and base level of contrast
contrast!(mf, x = SumContrasts(levels = [:c, :b, :a], base = :a))
@test ModelMatrix(mf).m == [1 -1 -1
                            1  0  1
                            1  1  0
                            1 -1 -1
                            1 -1 -1
                            1  0  1]
@test coefnames(mf) == ["(Intercept)"; "x: c"; "x: b"]

## Helmert coded contrasts
contrast!(mf, x = HelmertContrasts)
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1 -1
                            1  0  2
                            1 -1 -1
                            1 -1 -1
                            1  1 -1]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]

## Types for contrast levels are coerced to data levels when constructing
## ContrastsMatrix
contrast!(mf, x = SumContrasts(levels = ["a", "b", "c"]))
@test mf.contrasts[:x].levels == levels(d[:x])

## Missing data is handled gracefully, dropping columns when a level is lost
d[3, :x] = NA
mf_missing = ModelFrame(Formula(Nothing(), :x), d, contrasts = Dict(:x => SumContrasts))
@test ModelMatrix(mf_missing).m == [1 -1
                                    1  1
                                    1 -1
                                    1 -1
                                    1  1]
@test coefnames(mf_missing) == ["(Intercept)"; "x: b"]

## Things that are bad to do:
## Applying contrasts that only have a subset of data levels:
@test_throws ErrorException contrast!(mf, x = SumContrasts(levels = [:a, :b]))
## Applying contrasts that expect levels not found in data:
@test_throws ErrorException contrast!(mf, x = SumContrasts(levels = [:a, :b, :c, :d]))



end
