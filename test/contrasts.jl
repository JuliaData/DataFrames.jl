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
contrast!(mf, x = SumContrast(d[:x]; base = 2))
@test ModelMatrix(mf).m == [1  1  0
                            1 -1 -1
                            1  0  1
                            1  1  0
                            1  1  0
                            1 -1 -1]
@test coefnames(mf) == ["(Intercept)"; "x - a"; "x - c"]

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
