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
@test coefnames(mf) == ["(Intercept)"; "x - b"; "x - c"]

contrast!(mf, x = SumContrast)
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1  0
                            1  0  1
                            1 -1 -1
                            1 -1 -1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x - b"; "x - c"]

## change base level of contrast
contrast!(mf, x = SumContrast(base = :b))
@test ModelMatrix(mf).m == [1  1  0
                            1 -1 -1
                            1  0  1
                            1  1  0
                            1  1  0
                            1 -1 -1]
@test coefnames(mf) == ["(Intercept)"; "x - a"; "x - c"]

## change levels of contrast
contrast!(mf, x = SumContrast(levels = [:c, :b, :a]))
@test ModelMatrix(mf).m == [1  0  1
                            1  1  0
                            1 -1 -1
                            1  0  1
                            1  0  1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x - b"; "x - a"]


## change levels and base level of contrast
contrast!(mf, x = SumContrast(levels = [:c, :b, :a], base = :a))
@test ModelMatrix(mf).m == [1 -1 -1
                            1  0  1
                            1  1  0
                            1 -1 -1
                            1 -1 -1
                            1  0  1]
@test coefnames(mf) == ["(Intercept)"; "x - c"; "x - b"]

## restricting to only a subset of levels
@test_throws ErrorException contrast!(mf, x = SumContrast(levels = [:a, :b]))

## asking for levels that are not in the data raises an error
@test_throws ErrorException contrast!(mf, x = SumContrast(levels = [:a, :b, :c, :d]))

## Helmert coded contrasts
contrast!(mf, x = HelmertContrast)
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1 -1
                            1  0  2
                            1 -1 -1
                            1 -1 -1
                            1  1 -1]
@test coefnames(mf) == ["(Intercept)"; "x - b"; "x - c"]

## test for missing data (and when it clobbers one of the levels)
d[3, :x] = NA
mf = ModelFrame(Formula(Nothing(), :x), d, contrasts = [:x => SumContrast])
@test ModelMatrix(mf).m == [1 -1
                            1  1
                            1 -1
                            1 -1
                            1  1]
@test coefnames(mf) == ["(Intercept)"; "x - b"]

end
