@testset "Contrasts" begin

d = DataFrame(x = CategoricalVector([:a, :b, :c, :a, :a, :b]))

mf = ModelFrame(Formula(nothing, :x), d)

@testset "Dummy" begin
# Dummy coded contrasts by default:
@test ModelMatrix(mf).m == [1  0  0
                            1  1  0
                            1  0  1
                            1  0  0
                            1  0  0
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]

mmm = ModelMatrix(mf).m
setcontrasts!(mf, x = DummyCoding())
@test ModelMatrix(mf).m == mmm
end

@testset "Effects" begin
setcontrasts!(mf, x = EffectsCoding())
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1  0
                            1  0  1
                            1 -1 -1
                            1 -1 -1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]

# change base level of contrast
setcontrasts!(mf, x = EffectsCoding(base = :b))
@test ModelMatrix(mf).m == [1  1  0
                            1 -1 -1
                            1  0  1
                            1  1  0
                            1  1  0
                            1 -1 -1]
@test coefnames(mf) == ["(Intercept)"; "x: a"; "x: c"]

# change levels of contrast
setcontrasts!(mf, x = EffectsCoding(levels = [:c, :b, :a]))
@test ModelMatrix(mf).m == [1  0  1
                            1  1  0
                            1 -1 -1
                            1  0  1
                            1  0  1
                            1  1  0]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: a"]

# change levels and base level of contrast
setcontrasts!(mf, x = EffectsCoding(levels = [:c, :b, :a], base = :a))
@test ModelMatrix(mf).m == [1 -1 -1
                            1  0  1
                            1  1  0
                            1 -1 -1
                            1 -1 -1
                            1  0  1]
@test coefnames(mf) == ["(Intercept)"; "x: c"; "x: b"]

@testset "Helmert" begin
# Helmert coded contrasts
setcontrasts!(mf, x = HelmertCoding())
@test ModelMatrix(mf).m == [1 -1 -1
                            1  1 -1
                            1  0  2
                            1 -1 -1
                            1 -1 -1
                            1  1 -1]
@test coefnames(mf) == ["(Intercept)"; "x: b"; "x: c"]
end

@testset "Incorrect input data" begin
# Mismatching types of data and contrasts levels throws an error:
@test_throws ArgumentError setcontrasts!(mf, x = EffectsCoding(levels = ["a", "b", "c"]))

# Missing data is handled gracefully, dropping columns when a level is lost
d[3, :x] = Nullable()
mf_missing = ModelFrame(Formula(nothing, :x), d, contrasts = Dict(:x => EffectsCoding()))
@test ModelMatrix(mf_missing).m == [1 -1
                                    1  1
                                    1 -1
                                    1 -1
                                    1  1]
@test coefnames(mf_missing) == ["(Intercept)"; "x: b"]

# Things that are bad to do:
# Applying contrasts that only have a subset of data levels:
@test_throws ArgumentError setcontrasts!(mf, x = EffectsCoding(levels = [:a, :b]))
# Applying contrasts that expect levels not found in data:
@test_throws ArgumentError setcontrasts!(mf, x = EffectsCoding(levels = [:a, :b, :c, :d]))
# Asking for base level that's not found in data
@test_throws ArgumentError setcontrasts!(mf, x = EffectsCoding(base = :e))
end

# Manually specified contrasts
contrasts = [0  1
             -1 -.5
             1  -.5]
setcontrasts!(mf, x = ContrastsCoding(contrasts))
@test ModelMatrix(mf).m == [1  0  1
                            1 -1 -.5
                            1  1 -.5
                            1  0  1
                            1  0  1
                            1 -1 -.5]

# throw argument error if number of levels mismatches
@test_throws ArgumentError setcontrasts!(mf, x = ContrastsCoding(contrasts[1:2, :]))
@test_throws ArgumentError setcontrasts!(mf, x = ContrastsCoding(hcat(contrasts, contrasts)))

# contrasts types must be instaniated
@test_throws ArgumentError setcontrasts!(mf, x = DummyCoding)
end

end
