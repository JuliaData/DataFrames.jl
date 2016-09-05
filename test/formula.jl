module TestFormula
    using Base.Test
    using DataFrames
    using Compat

    # TODO:
    # - grouped variables in formulas with interactions
    # - is it fast?  Can expand() handle DataFrames?
    # - deal with intercepts
    # - implement ^2 for datavector's
    # - support more transformations with I()?

    ## Formula parsing
    import DataFrames.Terms

    ## totally empty
    t = Terms(Formula(nothing, 0))
    @test t.response == false
    @test t.intercept == false
    @test t.terms == []
    @test t.eterms == []

    ## empty RHS
    t = Terms(y ~ 0)
    @test t.intercept == false
    @test t.terms == []
    @test t.eterms == [:y]
    t = Terms(y ~ -1)
    @test t.intercept == false
    @test t.terms == []

    ## intercept-only
    t = Terms(y ~ 1)
    @test t.response == true
    @test t.intercept == true
    @test t.terms == []
    @test t.eterms == [:y]

    ## terms add
    t = Terms(y ~ 1 + x1 + x2)
    @test t.intercept == true
    @test t.terms == [:x1, :x2]
    @test t.eterms == [:y, :x1, :x2]

    ## implicit intercept behavior:
    t = Terms(y ~ x1 + x2)
    @test t.intercept == true
    @test t.terms == [:x1, :x2]
    @test t.eterms == [:y, :x1, :x2]

    ## no intercept
    t = Terms(y ~ 0 + x1 + x2)
    @test t.intercept == false
    @test t.terms == [:x1, :x2]

    t = Terms(y ~ -1 + x1 + x2)
    @test t.intercept == false
    @test t.terms == [:x1, :x2]

    t = Terms(y ~ x1 & x2)
    @test t.terms == [:(x1 & x2)]
    @test t.eterms == [:y, :x1, :x2]

    ## `*` expansion
    t = Terms(y ~ x1 * x2)
    @test t.terms == [:x1, :x2, :(x1 & x2)]
    @test t.eterms == [:y, :x1, :x2]

    ## associative rule:
    ## +
    t = Terms(y ~ x1 + x2 + x3)
    @test t.terms == [:x1, :x2, :x3]

    ## &
    t = Terms(y ~ x1 & x2 & x3)
    @test t.terms == [:((&)(x1, x2, x3))]
    @test t.eterms == [:y, :x1, :x2, :x3]

    ## distributive property of + and &
    t = Terms(y ~ x1 & (x2 + x3))
    @test t.terms == [:(x1&x2), :(x1&x3)]

    ## FAILS: ordering of expanded interaction terms is wrong
    ## (only has an observable effect when both terms are categorical and
    ## produce multiple model matrix columns that are multiplied together...)
    ##
    ## t = Terms(y ~ (x2 + x3) & x1)
    ## @test t.terms == [:(x2&x1), :(x3&x1)]

    ## three-way *
    t = Terms(y ~ x1 * x2 * x3)
    @test t.terms == [:x1, :x2, :x3,
                      :(x1&x2), :(x1&x3), :(x2&x3),
                      :((&)(x1, x2, x3))]
    @test t.eterms == [:y, :x1, :x2, :x3]

    ## Interactions with `1` reduce to main effect.  All fail at the moment.
    ## t = Terms(y ~ 1 & x1)
    ## @test t.terms == [:x1]              # == [:(1 & x1)]
    ## @test t.eterms == [:y, :x1]

    ## t = Terms(y ~ (1 + x1) & x2)
    ## @test t.terms == [:x2, :(x1&x2)]    # == [:(1 & x1)]
    ## @test t.eterms == [:y, :x1, :x2]



    ## Tests for constructing ModelFrame and ModelMatrix

    sparsetype = SparseMatrixCSC{Float64,Int}

    d = DataFrame()
    d[:y] = [1:4;]
    d[:x1] = [5:8;]
    d[:x2] = [9:12;]
    d[:x3] = [13:16;]
    d[:x4] = [17:20;]

    x1 = [5.:8;]
    x2 = [9.:12;]
    x3 = [13.:16;]
    x4 = [17.:20;]
    f = y ~ x1 + x2
    mf = ModelFrame(f, d)
    ## @test mm.response_colnames == ["y"] # nope: no response_colnames
    @test coefnames(mf) == ["(Intercept)","x1","x2"]
    ## @test model_response(mf) == transpose([1. 2 3 4]) # fails: Int64 vs. Float64
    mm = ModelMatrix(mf)
    smm = ModelMatrix{sparsetype}(mf)
    @test mm.m[:,1] == ones(4)
    @test mm.m[:,2:3] == [x1 x2]
    @test mm.m == smm.m

    @test isa(mm.m, Matrix{Float64})
    @test isa(smm.m, sparsetype)
    @test isa(ModelMatrix{DataMatrix{Float64}}(mf).m, DataMatrix{Float64})

    #test_group("expanding a PooledVec into a design matrix of indicators for each dummy variable")

    d[:x1p] = PooledDataArray(d[:x1])
    mf = ModelFrame(y ~ x1p, d)
    mm = ModelMatrix(mf)

    @test mm.m[:,2] == [0, 1., 0, 0]
    @test mm.m[:,3] == [0, 0, 1., 0]
    @test mm.m[:,4] == [0, 0, 0, 1.]
    @test coefnames(mf)[2:end] == ["x1p: 6", "x1p: 7", "x1p: 8"]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    #test_group("create a design matrix from interactions from two DataFrames")
    ## this was removed in commit dead4562506badd7e84a2367086f5753fa49bb6a

    ## b = DataFrame()
    ## b["x2"] = DataVector(x2)
    ## df = interaction_design_matrix(a,b)
    ## @test df[:,1] == DataVector([0, 10., 0, 0])
    ## @test df[:,2] == DataVector([0, 0, 11., 0])
    ## @test df[:,3] == DataVector([0, 0, 0, 12.])

    #test_group("expanding an singleton expression/symbol into a DataFrame")
    ## generalized expand was dropped, too
    ## df = deepcopy(d)
    ## r = expand(:x2, df)
    ## @test isa(r, DataFrame)
    ## @test r[:,1] == DataVector([9,10,11,12])  # TODO: test float vs int return

    ## df = deepcopy(d)
    ## ex = :(log(x2))
    ## r = expand(ex, df)
    ## @test isa(r, DataFrame)
    ## @test r[:,1] == DataVector(log([9,10,11,12]))

    # ex = :(x1 & x2)
    # r = expand(ex, df)
    # @test isa(r, DataFrame)
    # @test ncol(r) == 1
    # @test r[:,1] == DataArray([45, 60, 77, 96])

    ## r = expand(:(x1 + x2), df)
    ## @test isa(r, DataFrame)
    ## @test ncol(r) == 2
    ## @test r[:,1] == DataVector(df["x1"])
    ## @test r[:,2] == DataVector(df["x2"])

    ## df["x1"] = PooledDataArray(x1)
    ## r = expand(:x1, df)
    ## @test isa(r, DataFrame)
    ## @test ncol(r) == 3
    ## @test r == expand(PooledDataArray(x1), "x1", DataFrame())

    ## r = expand(:(x1 + x2), df)
    ## @test isa(r, DataFrame)
    ## @test ncol(r) == 4
    ## @test r[:,1:3] == expand(PooledDataArray(x1), "x1", DataFrame())
    ## @test r[:,4] == DataVector(df["x2"])

    ## df["x2"] = PooledDataArray(x2)
    ## r = expand(:(x1 + x2), df)
    ## @test isa(r, DataFrame)
    ## @test ncol(r) == 6
    ## @test r[:,1:3] == expand(PooledDataArray(x1), "x1", DataFrame())
    ## @test r[:,4:6] == expand(PooledDataArray(x2), "x2", DataFrame())

    #test_group("Creating a model matrix using full formulas: y ~ x1 + x2, etc")

    df = deepcopy(d)
    f = y ~ x1 & x2
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m == [ones(4) x1.*x2]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    f = y ~ x1 * x2
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m == [ones(4) x1 x2 x1.*x2]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    df[:x1] = PooledDataArray(x1)
    x1e = [[0, 1, 0, 0] [0, 0, 1, 0] [0, 0, 0, 1]]
    f = y ~ x1 * x2
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m == [ones(4) x1e x2 [0, 10, 0, 0] [0, 0, 11, 0] [0, 0, 0, 12]]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    #test_group("Basic transformations")

    ## the log(x2) appears to be broken (again, in The Great Purge, by the
    ## removal of the x -> with(d, x) in the ModelFrame constructor)
    ## df = deepcopy(d)
    ## f = y ~ x1 + log(x2)
    ## mf = ModelFrame(f, df)
    ## mm = ModelMatrix(mf)
    ## @test mm.m == [ones(4) x1 log(x2)]

    ## df = deepcopy(d)
    ## df["x1"] = PooledDataArray([5:8])
    ## f = Formula(:(y ~ x1 * (log(x2) + x3)))
    ## mf = ModelFrame(f, df)
    ## mm = ModelMatrix(mf)
    ## @test mm.model_colnames == [
    ##  "(Intercept)"
    ##  "x1:6"
    ##  "x1:7"
    ##  "x1:8"
    ##  "log(x2)"
    ##  "x3"
    ##  "x1:6&log(x2)"
    ##  "x1:6&x3"
    ##  "x1:7&log(x2)"
    ##  "x1:7&x3"
    ##  "x1:8&log(x2)"
    ##  "x1:8&x3" ]

    #test_group("Model frame response variables")

    ## also does not work, not sure what used to happen but now it seems that
    ## the LHS is assumed to be just a single symbol that indexes a DF column

    ## f = x1 + x2 ~ y + x3
    ## mf = ModelFrame(f, d)
    ## @test mf.y_indexes == [1, 2]
    ## @test isequal(mf.formula.lhs, [:(x1 + x2)])
    ## @test isequal(mf.formula.rhs, [:(y + x3)])

    # unique_symbol tests
    #@test unique_symbols(:(x1 + x2)) ==  {"x1"=>:x1, "x2"=>:x2}
    #unique_symbols(:(y ~ x1 + x2 + x3))

    # additional tests from Tom
    y = [1., 2, 3, 4]
    mf = ModelFrame(y ~ x2, d)
    mm = ModelMatrix(mf)
    @test mm.m == [ones(4) x2]
    @test mm.m == ModelMatrix{sparsetype}(mf).m
    ## @test model_response(mf) == y''     # fails: Int64 vs. Float64

    df = deepcopy(d)
    df[:x1] = PooledDataArray(df[:x1])

    f = y ~ x2 + x3 + x3*x2
    mm = ModelMatrix(ModelFrame(f, df))
    @test mm.m == [ones(4) x2 x3 x2.*x3]
    mm = ModelMatrix(ModelFrame(y ~ x3*x2 + x2 + x3, df))
    @test mm.m == [ones(4) x3 x2 x2.*x3]
    mm = ModelMatrix(ModelFrame(y ~ x1 + x2 + x3 + x4, df))
    @test mm.m[:,2] == [0, 1., 0, 0]
    @test mm.m[:,3] == [0, 0, 1., 0]
    @test mm.m[:,4] == [0, 0, 0, 1.]
    @test mm.m[:,5] == x2
    @test mm.m[:,6] == x3
    @test mm.m[:,7] == x4

    mm = ModelMatrix(ModelFrame(y ~ x2 + x3 + x4, df))
    @test mm.m == [ones(4) x2 x3 x4]
    mm = ModelMatrix(ModelFrame(y ~ x2 + x2, df))
    @test mm.m == [ones(4) x2]
    mm = ModelMatrix(ModelFrame(y ~ x2*x3 + x2&x3, df))
    @test mm.m == [ones(4) x2 x3 x2.*x3]
    mm = ModelMatrix(ModelFrame(y ~ x2*x3*x4, df))
    @test mm.m == [ones(4) x2 x3 x4 x2.*x3 x2.*x4 x3.*x4 x2.*x3.*x4]
    mm = ModelMatrix(ModelFrame(y ~ x2&x3 + x2*x3, df))
    @test mm.m == [ones(4) x2 x3 x2.*x3]

    f = y ~ x2 & x3 & x4
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m == [ones(4) x2.*x3.*x4]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    f = y ~ x1 & x2 & x3
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m[:, 2:end] == diagm(x2.*x3)
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    #test_group("Column groups in formulas")
    ## set_group was removed in The Great Purge (55e47cd)

    ## set_group(d, "odd_predictors", ["x1","x3"])
    ## @test expand(:odd_predictors, d) == d["odd_predictors"]
    ## mf = ModelFrame(Formula(:(y ~ odd_predictors)), d)
    ## @test mf.df[:,1] == d["y"]
    ## @test mf.df[:,2] == d["x1"]
    ## @test mf.df[:,3] == d["x3"]
    ## @test ncol(mf.df) == 3
    ## mf = ModelFrame(Formula(:(y ~ odd_predictors * x2)), d)
    ## mm = ModelMatrix(mf)
    ## @test mm.model == [ones(4) x1 x3 x2 x1.*x2 x3.*x2]

    ## Interactions between three PDA columns
    ##
    ## FAILS: behavior is wrong when no lower-order terms (1+x1+x2+x1&x2...)
    ##
    ## df = DataFrame(y=1:27,
    ##                x1 = PooledDataArray(vec([x for x in 1:3, y in 4:6, z in 7:9])),
    ##                x2 = PooledDataArray(vec([y for x in 1:3, y in 4:6, z in 7:9])),
    ##                x3 = PooledDataArray(vec([z for x in 1:3, y in 4:6, z in 7:9])))
    ## f = y ~ x1 & x2 & x3
    ## mf = ModelFrame(f, df)
    ## @test coefnames(mf)[2:end] ==
    ##     vec([string("x1: ", x, " & x2: ", y, " & x3: ", z) for
    ##          x in 2:3,
    ##          y in 5:6,
    ##          z in 8:9])

    ## mm = ModelMatrix(mf)
    ## @test mm.m[:,2] == 0. + (df[:x1] .== 2) .* (df[:x2] .== 5) .* (df[:x3].==8)
    ## @test mm.m[:,3] == 0. + (df[:x1] .== 3) .* (df[:x2] .== 5) .* (df[:x3].==8)
    ## @test mm.m[:,4] == 0. + (df[:x1] .== 2) .* (df[:x2] .== 6) .* (df[:x3].==8)
    ## @test mm.m[:,5] == 0. + (df[:x1] .== 3) .* (df[:x2] .== 6) .* (df[:x3].==8)
    ## @test mm.m[:,6] == 0. + (df[:x1] .== 2) .* (df[:x2] .== 5) .* (df[:x3].==9)
    ## @test mm.m[:,7] == 0. + (df[:x1] .== 3) .* (df[:x2] .== 5) .* (df[:x3].==9)
    ## @test mm.m[:,8] == 0. + (df[:x1] .== 2) .* (df[:x2] .== 6) .* (df[:x3].==9)
    ## @test mm.m[:,9] == 0. + (df[:x1] .== 3) .* (df[:x2] .== 6) .* (df[:x3].==9)

    ## Distributive property of :& over :+
    df = deepcopy(d)
    f = y ~ (x1+x2) & (x3+x4)
    mf = ModelFrame(f, df)
    mm = ModelMatrix(mf)
    @test mm.m == hcat(ones(4), x1.*x3, x1.*x4, x2.*x3, x2.*x4)
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    ## Condensing nested :+ calls
    f = y ~ x1 + (x2 + (x3 + x4))
    @test ModelMatrix(ModelFrame(f, df)).m == hcat(ones(4), x1, x2, x3, x4)


    ## Extra levels in categorical column
    mf_full = ModelFrame(y ~ x1p, d)
    mm_full = ModelMatrix(mf_full)
    @test size(mm_full) == (4,4)

    mf_sub = ModelFrame(y ~ x1p, d[2:4, :])
    mm_sub = ModelMatrix(mf_sub)
    ## should have only three rows, and only three columns (intercept plus two
    ## levels of factor)
    @test size(mm_sub) == (3,3)

    ## Missing data
    d[:x1m] = @data [5, 6, NA, 7]
    mf = ModelFrame(y ~ x1m, d)
    mm = ModelMatrix(mf)
    @test mm.m[:, 2] == d[complete_cases(d), :x1m]
    @test mm.m == ModelMatrix{sparsetype}(mf).m

    ## Same variable on left and right side
    mf = ModelFrame(x1 ~ x1, df)
    mm = ModelMatrix(mf)
    mm.m == float(model_response(mf))

## Promote non-redundant categorical terms to full rank

d = DataFrame(x = Compat.repeat([:a, :b], outer = 4),
              y = Compat.repeat([:c, :d], inner = 2, outer = 2),
              z = Compat.repeat([:e, :f], inner = 4))
[pool!(d, name) for name in names(d)]
cs = Dict([Pair(name, EffectsCoding()) for name in names(d)])
d[:n] = 1.:8


## No intercept
mf = ModelFrame(n ~ 0 + x, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == [1 0
               0 1
               1 0
               0 1
               1 0
               0 1
               1 0
               0 1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["x: a", "x: b"]

## No first-order term for interaction
mf = ModelFrame(n ~ 1 + x + x&y, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m[:, 2:end] == [-1 -1  0
                         1  0 -1
                         -1  1  0
                         1  0  1
                         -1 -1  0
                         1  0 -1
                         -1  1  0
                         1  0  1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["(Intercept)", "x: b", "x: a & y: d", "x: b & y: d"]

## When both terms of interaction are non-redundant:
mf = ModelFrame(n ~ 0 + x&y, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == [1 0 0 0
               0 1 0 0
               0 0 1 0
               0 0 0 1
               1 0 0 0
               0 1 0 0
               0 0 1 0
               0 0 0 1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["x: a & y: c", "x: b & y: c",
                        "x: a & y: d", "x: b & y: d"]

# only a three-way interaction: every term is promoted.
mf = ModelFrame(n ~ 0 + x&y&z, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == eye(8)
@test mm.m == ModelMatrix{sparsetype}(mf).m

# two two-way interactions, with no lower-order term. both are promoted in
# first (both x and y), but only the old term (x) in the second (because
# dropping x gives z which isn't found elsewhere, but dropping z gives x
# which is found (implicitly) in the promoted interaction x&y).
mf = ModelFrame(n ~ 0 + x&y + x&z, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == [1 0 0 0 -1  0
               0 1 0 0  0 -1
               0 0 1 0 -1  0
               0 0 0 1  0 -1
               1 0 0 0  1  0
               0 1 0 0  0  1
               0 0 1 0  1  0
               0 0 0 1  0  1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["x: a & y: c", "x: b & y: c",
                        "x: a & y: d", "x: b & y: d",
                        "x: a & z: f", "x: b & z: f"]

# ...and adding a three-way interaction, only the shared term (x) is promoted.
# this is because dropping x gives y&z which isn't present, but dropping y or z
# gives x&z or x&z respectively, which are both present.
mf = ModelFrame(n ~ 0 + x&y + x&z + x&y&z, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == [1 0 0 0 -1  0  1  0
               0 1 0 0  0 -1  0  1
               0 0 1 0 -1  0 -1  0
               0 0 0 1  0 -1  0 -1
               1 0 0 0  1  0 -1  0
               0 1 0 0  0  1  0 -1
               0 0 1 0  1  0  1  0
               0 0 0 1  0  1  0  1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["x: a & y: c", "x: b & y: c",
                        "x: a & y: d", "x: b & y: d",
                        "x: a & z: f", "x: b & z: f",
                        "x: a & y: d & z: f", "x: b & y: d & z: f"]

# two two-way interactions, with common lower-order term. the common term x is
# promoted in both (along with lower-order term), because in every case, when
# x is dropped, the remaining terms (1, y, and z) aren't present elsewhere.
mf = ModelFrame(n ~ 0 + x + x&y + x&z, d, contrasts=cs)
mm = ModelMatrix(mf)
@test mm.m == [1 0 -1  0 -1  0
               0 1  0 -1  0 -1
               1 0  1  0 -1  0
               0 1  0  1  0 -1
               1 0 -1  0  1  0
               0 1  0 -1  0  1
               1 0  1  0  1  0
               0 1  0  1  0  1]
@test mm.m == ModelMatrix{sparsetype}(mf).m
@test coefnames(mf) == ["x: a", "x: b",
                        "x: a & y: d", "x: b & y: d",
                        "x: a & z: f", "x: b & z: f"]


## FAILS: When both terms are non-redundant and intercept is PRESENT
## (not fully redundant). Ideally, would drop last column. Might make sense
## to warn about this, and suggest recoding x and y into a single variable.
# mf = ModelFrame(n ~ 1 + x&y, d[1:4, :], contrasts=cs)
# @test ModelMatrix(mf).m == [1 1 0 0
#                             1 0 1 0
#                             1 0 0 1
#                             1 0 0 0]
# @test coefnames(mf) == ["x: a & y: c", "x: b & y: c",
#                         "x: a & y: d", "x: b & y: d"]

## note that R also does not detect this automatically. it's left to glm et al.
## to detect numerically when the model matrix is rank deficient, which is hard
## to do correctly.
# > d = data.frame(x = factor(c(1, 2, 1, 2)), y = factor(c(3, 3, 4, 4)))
# > model.matrix(~ 1 + x:y, d)
#   (Intercept) x1:y3 x2:y3 x1:y4 x2:y4
# 1           1     1     0     0     0
# 2           1     0     1     0     0
# 3           1     0     0     1     0
# 4           1     0     0     0     1


# Ensure that random effects terms are dropped from coefnames
df = DataFrame(x = [1,2,3], y = [4,5,6])
mf = ModelFrame(y ~ 1 + (1 | x), df)
@test coefnames(mf) == ["(Intercept)"]

mf = ModelFrame(y ~ 0 + (1 | x), df)
@test_throws ErrorException ModelMatrix(mf)
@test coefnames(mf) == Vector{Compat.UTF8String}()


# Ensure X is not a view on df column
df = DataFrame(x = [1.0,2.0,3.0], y = [4.0,5.0,6.0])
mf = ModelFrame(y ~ 0 + x, df)
X = ModelMatrix(mf).m
X[1] = 0.0
@test mf.df[1, :x] == 1.0
end
