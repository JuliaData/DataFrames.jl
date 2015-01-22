## module TestContrasts

using Base.Test
using DataFrames

x = @pdata [1, 2, 3, 1]

@test contrast_matrix(TreatmentContrast, x) == [false false
                                                true false
                                                false true]

@test contrast_matrix(TreatmentContrast(base=2), x) == [true false
                                                        false false
                                                        false true]

@test contrast_matrix(SumContrast, x) == [-1 -1
                                           1  0
                                           0  1]

@test contrast_matrix(SumContrast(base=2), x) == [1  0
                                                  -1 -1
                                                  0  1]

@test contrast_matrix(HelmertContrast, x) == [-1 -1
                                              1  -1
                                              0  2]

@test contrast_matrix(HelmertContrast(base=2), x) == [1   -1
                                                      -1  -1
                                                      0   2]


## end
