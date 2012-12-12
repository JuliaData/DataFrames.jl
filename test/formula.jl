require("extras/test.jl")

load("DataFrames")
using DataFrames

# TODO:
# - grouped variables in formulas with interactions
# - is it fast?  Can expand() handle DataFrames?
# - deal with intercepts
# - implement ^2 for datavecs
# - support more transformations with I()?

test_context("Formula")
test_group("Formula")

d = DataFrame()
d["y"] = [1:4]
d["x1"] = PooledDataVec([5:8])
d["x2"] = [9:12]
d["x3"] = [11:14]
d["x4"] = [12:15]
f = Formula(:(y ~ x1 * (log(x2) + x3)))
mf = model_frame(f, d)
mm = model_matrix(mf)
@test mm.model_colnames == [
 "(Intercept)"
 "x1:6"        
 "x1:7"        
 "x1:8"        
 "log(x2)"     
 "x3"          
 "x1:6&log(x2)"
 "x1:6&x3"     
 "x1:7&log(x2)"
 "x1:7&x3"     
 "x1:8&log(x2)"
 "x1:8&x3" ]

tmp = d["x2"]
 
test_group("Basic tests")

d = DataFrame()
d["y"] = [1:4]
d["x1"] = [5:8]
d["x2"] = [9:12]
d["x3"] = [13:16]
d["x4"] = [17:20]

x1 = [5.:8]
x2 = [9.:12]
x3 = [13.:16]
x4 = [17.:20]
f = Formula(:(y ~ x1 + x2))
mf = model_frame(f, d)
mm = model_matrix(mf)
@test mm.response_colnames == ["y"]
@test mm.model_colnames == ["(Intercept)","x1","x2"]
@test mm.response == transpose([1. 2 3 4])
@test mm.model[:,1] == ones(4)
@test mm.model[:,2:3] == [x1 x2]

test_group("expanding a PooledVec into a design matrix of indicators for each dummy variable")

a = expand(PooledDataVec(x1), "x1", DataFrame())
@test a[:,1] == DataVec([0, 1., 0, 0])
@test a[:,2] == DataVec([0, 0, 1., 0])
@test a[:,3] == DataVec([0, 0, 0, 1.])
@test colnames(a) == ["x1:6.0", "x1:7.0", "x1:8.0"]

test_group("create a design matrix from interactions from two DataFrames")

b = DataFrame()
b["x2"] = DataVec(x2)
df = interaction_design_matrix(a,b)
@test df[:,1] == DataVec([0, 10., 0, 0])
@test df[:,2] == DataVec([0, 0, 11., 0])
@test df[:,3] == DataVec([0, 0, 0, 12.])

test_group("expanding an singleton expression/symbol into a DataFrame")

df = deepcopy(d)
r = expand(:x2, df)
@test isa(r, DataFrame)
@test r[:,1] == DataVec([9,10,11,12])  # TODO: test float vs int return

df = deepcopy(d)
ex = :(log(x2))
r = expand(ex, df)
@test isa(r, DataFrame)
@test r[:,1] == DataVec(log([9,10,11,12]))

# ex = :(x1 & x2)
# r = expand(ex, df)
# @test isa(r, DataFrame)
# @test ncol(r) == 1
# @test r[:,1] == DataVec([45, 60, 77, 96])

r = expand(:(x1 + x2), df)
@test isa(r, DataFrame)
@test ncol(r) == 2
@test r[:,1] == DataVec(df["x1"])
@test r[:,2] == DataVec(df["x2"])

df["x1"] = PooledDataVec(x1)
r = expand(:x1, df)
@test isa(r, DataFrame)
@test ncol(r) == 3
@test r == expand(PooledDataVec(x1), "x1", DataFrame())

r = expand(:(x1 + x2), df)
@test isa(r, DataFrame)
@test ncol(r) == 4
@test r[:,1:3] == expand(PooledDataVec(x1), "x1", DataFrame())
@test r[:,4] == DataVec(df["x2"])

df["x2"] = PooledDataVec(x2)
r = expand(:(x1 + x2), df)
@test isa(r, DataFrame)
@test ncol(r) == 6
@test r[:,1:3] == expand(PooledDataVec(x1), "x1", DataFrame())
@test r[:,4:6] == expand(PooledDataVec(x2), "x2", DataFrame())

test_group("Creating a model matrix using full formulas: y ~ x1 + x2, etc")

df = deepcopy(d)
f = Formula(:(y ~ x1 & x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@test mm.model == [ones(4) x1.*x2]

f = Formula(:(y ~ x1 * x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@test mm.model == [ones(4) x1 x2 x1.*x2]

df["x1"] = PooledDataVec(x1)
x1e = [[0, 1, 0, 0] [0, 0, 1, 0] [0, 0, 0, 1]]
f = Formula(:(y ~ x1 * x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@test mm.model == [ones(4) x1e x2 [0, 10, 0, 0] [0, 0, 11, 0] [0, 0, 0, 12]]

test_group("Basic transformations")

df = deepcopy(d)
f = Formula(:(y ~ x1 + log(x2)))
mf = model_frame(f, df)
mm = model_matrix(mf)
@test mm.model == [ones(4) x1 log(x2)]

df = deepcopy(d)
df["x1"] = PooledDataVec([5:8])
f = Formula(:(y ~ x1 * (log(x2) + x3)))
mf = model_frame(f, df)
mm = model_matrix(mf)
@test mm.model_colnames == [
 "(Intercept)"
 "x1:6"        
 "x1:7"        
 "x1:8"        
 "log(x2)"     
 "x3"          
 "x1:6&log(x2)"
 "x1:6&x3"     
 "x1:7&log(x2)"
 "x1:7&x3"     
 "x1:8&log(x2)"
 "x1:8&x3" ]

test_group("Model frame response variables")

f = Formula(:(x1 + x2 ~ y + x3))
mf = model_frame(f, d)
@test mf.y_indexes == [1, 2]
@test isequal(mf.formula.lhs, [:(x1 + x2)])
@test isequal(mf.formula.rhs, [:(y + x3)])


f = Formula(:(x1 + x2 ~ y + x3))
mf = model_frame(f, d)
@test mf.y_indexes == [1, 2]
@test isequal(mf.formula.lhs, [:(x1 + x2)])
@test isequal(mf.formula.rhs, [:(y + x3)])

# unique_symbol tests
#@test unique_symbols(:(x1 + x2)) ==  {"x1"=>:x1, "x2"=>:x2}
#unique_symbols(:(y ~ x1 + x2 + x3))

# additional tests from Tom
y = [1., 2, 3, 4]
mm = model_matrix(model_frame(Formula(:(y ~ x2)), d))
@test mm.model == [ones(4) x2]
@test mm.response == y''

df = deepcopy(d)
df["x1"] = PooledDataVec(df["x1"])

mm = model_matrix(model_frame(Formula(:(y ~ x2 + x3 + x3*x2)), df))
@test mm.model == [ones(4) x2 x3 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x3*x2 + x2 + x3)), df))
@test mm.model == [ones(4) x3 x2 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x1 + x2 + x3 + x4)), df))
@test mm.model[:,2] == [0, 1., 0, 0]
@test mm.model[:,3] == [0, 0, 1., 0]
@test mm.model[:,4] == [0, 0, 0, 1.]
@test mm.model[:,5] == x2
@test mm.model[:,6] == x3
@test mm.model[:,7] == x4

mm = model_matrix(model_frame(Formula(:(y ~ x2 + x3 + x4)), df))
@test mm.model == [ones(4) x2 x3 x4]
mm = model_matrix(model_frame(Formula(:(y ~ x2 + x2)), df))
@test mm.model == [ones(4) x2]
mm = model_matrix(model_frame(Formula(:(y ~ x2*x3 + x2&x3)), df))
@test mm.model == [ones(4) x2 x3 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x2*x3*x4)), df))
@test mm.model == [ones(4) x2 x3 x4 x2.*x3 x2.*x4 x3.*x4 x2.*x3.*x4]
mm = model_matrix(model_frame(Formula(:(y ~ x2&x3 + x2*x3)), df))
@test mm.model == [ones(4) x2.*x3 x2 x3]  # TODO This disagrees with R
mm = model_matrix(model_frame(Formula(:(y ~ x2 & x3 & x4)), df))
@test mm.model == [ones(4) x2.*x3.*x4]

test_group("Column groups in formulas")
set_group(d, "odd_predictors", ["x1","x3"])
@test expand(:odd_predictors, d) == d["odd_predictors"]
mf = model_frame(Formula(:(y ~ odd_predictors)), d)
@test mf.df[:,1] == d["y"]
@test mf.df[:,2] == d["x1"]
@test mf.df[:,3] == d["x3"]
@test ncol(mf.df) == 3
mf = model_frame(Formula(:(y ~ odd_predictors * x2)), d)
mm = model_matrix(mf)
@test mm.model == [ones(4) x1 x3 x2 x1.*x2 x3.*x2]
