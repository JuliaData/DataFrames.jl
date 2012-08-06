# TODO:
# - grouped variables in formulas with interactions
# - is it fast?  Can expand() handle DataFrames?
# - deal with intercepts
# - implement ^2 for datavecs
# - support more transformations with I()?

# Load files
load("src/init.jl")

# test_group("Formula")

d = DataFrame()
d["y"] = [1:4]
d["x1"] = PooledDataVec([5:8])
d["x2"] = [9:12]
d["x3"] = [11:14]
d["x4"] = [12:15]
f = Formula(:(y ~ x1 * (log(x2) + x3)))
mf = model_frame(f, d)
mm = model_matrix(mf)
@assert mm.model_colnames == [
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
 
# # test_group("Basic tests")

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
@assert mm.response_colnames == ["y"]
@assert mm.model_colnames == ["(Intercept)","x1","x2"]
@assert mm.response == [1. 2 3 4]'
@assert mm.model[:,1] == ones(4)
@assert mm.model[:,2:3] == [x1 x2]

# test_group("expanding a PooledVec into a design matrix of indicators for each dummy variable")

a = expand(PooledDataVec(x1), "x1", DataFrame())
@assert a[:,1] == DataVec([0, 1., 0, 0])
@assert a[:,2] == DataVec([0, 0, 1., 0])
@assert a[:,3] == DataVec([0, 0, 0, 1.])
@assert colnames(a) == ["x1:6.0", "x1:7.0", "x1:8.0"]

# test_group("create a design matrix from interactions from two DataFrames")

b = DataFrame()
b["x2"] = DataVec(x2)
df = interaction_design_matrix(a,b)
@assert df[:,1] == DataVec([0, 10., 0, 0])
@assert df[:,2] == DataVec([0, 0, 11., 0])
@assert df[:,3] == DataVec([0, 0, 0, 12.])

# test_group("expanding an singleton expression/symbol into a DataFrame")

df = copy(d)
r = expand(:x2, df)
@assert isa(r, DataFrame)
@assert r[:,1] == DataVec([9,10,11,12])  # TODO: test float vs int return

df = copy(d)
ex = :(log(x2))
r = expand(ex, df)
@assert isa(r, DataFrame)
@assert r[:,1] == DataVec(log([9,10,11,12]))

ex = :(x1 & x2)
r = expand(ex, df)
@assert isa(r, DataFrame)
@assert ncol(r) == 1
@assert r[:,1] == DataVec([45, 60, 77, 96])

r = expand(:(x1 + x2), df)
@assert isa(r, DataFrame)
@assert ncol(r) == 2
@assert r[:,1] == DataVec(df["x1"])
@assert r[:,2] == DataVec(df["x2"])

df["x1"] = PooledDataVec(x1)
r = expand(:x1, df)
@assert isa(r, DataFrame)
@assert ncol(r) == 3
@assert r == expand(PooledDataVec(x1), "x1", DataFrame())

r = expand(:(x1 + x2), df)
@assert isa(r, DataFrame)
@assert ncol(r) == 4
@assert r[:,1:3] == expand(PooledDataVec(x1), "x1", DataFrame())
@assert r[:,4] == DataVec(df["x2"])

df["x2"] = PooledDataVec(x2)
r = expand(:(x1 + x2), df)
@assert isa(r, DataFrame)
@assert ncol(r) == 6
@assert r[:,1:3] == expand(PooledDataVec(x1), "x1", DataFrame())
@assert r[:,4:6] == expand(PooledDataVec(x2), "x2", DataFrame())

# test_group("Creating a model matrix using full formulas: y ~ x1 + x2, etc")

df = copy(d)
f = Formula(:(y ~ x1 & x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@assert mm.model == [ones(4) x1.*x2]

f = Formula(:(y ~ x1 * x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@assert mm.model == [ones(4) x1 x2 x1.*x2]

df["x1"] = PooledDataVec(x1)
x1e = [[0, 1, 0, 0] [0, 0, 1, 0] [0, 0, 0, 1]]
f = Formula(:(y ~ x1 * x2))
mf = model_frame(f, df)
mm = model_matrix(mf)
@assert mm.model == [ones(4) x1e x2 [0, 10, 0, 0] [0, 0, 11, 0] [0, 0, 0, 12]]

# test_group("Basic transformations")

df = copy(d)
f = Formula(:(y ~ x1 + log(x2)))
mf = model_frame(f, df)
mm = model_matrix(mf)
@assert mm.model == [ones(4) x1 log(x2)]

df = copy(d)
df["x1"] = PooledDataVec([5:8])
f = Formula(:(y ~ x1 * (log(x2) + x3)))
mf = model_frame(f, df)
mm = model_matrix(mf)
@assert mm.model_colnames == [
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

# test_group("Model frame response variables")

f = Formula(:(x1 + x2 ~ y + x3))
mf = model_frame(f, d)
@assert mf.y_indexes == [1, 2]
@assert isequal(mf.formula.lhs, [:(x1 + x2)])
@assert isequal(mf.formula.rhs, [:(y + x3)])


f = Formula(:(x1 + x2 ~ y + x3))
mf = model_frame(f, d)
@assert mf.y_indexes == [1, 2]
@assert isequal(mf.formula.lhs, [:(x1 + x2)])
@assert isequal(mf.formula.rhs, [:(y + x3)])

# unique_symbol tests
#@assert unique_symbols(:(x1 + x2)) ==  {"x1"=>:x1, "x2"=>:x2}
#unique_symbols(:(y ~ x1 + x2 + x3))

# additional tests from Tom
y = [1., 2, 3, 4]
mm = model_matrix(model_frame(Formula(:(y ~ x2)), d))
@assert mm.model == [ones(4) x2]
@assert mm.response == y''

df = copy(d)
df["x1"] = PooledDataVec(df["x1"])

mm = model_matrix(model_frame(Formula(:(y ~ x2 + x3 + x3*x2)), df))
@assert mm.model == [ones(4) x2 x3 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x3*x2 + x2 + x3)), df))
@assert mm.model == [ones(4) x3 x2 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x1 + x2 + x3 + x4)), df))
@assert mm.model[:,2] == [0, 1., 0, 0]
@assert mm.model[:,3] == [0, 0, 1., 0]
@assert mm.model[:,4] == [0, 0, 0, 1.]
@assert mm.model[:,5] == x2
@assert mm.model[:,6] == x3
@assert mm.model[:,7] == x4

mm = model_matrix(model_frame(Formula(:(y ~ x2 + x3 + x4)), df))
@assert mm.model == [ones(4) x2 x3 x4]
mm = model_matrix(model_frame(Formula(:(y ~ x2 + x2)), df))
@assert mm.model == [ones(4) x2]
mm = model_matrix(model_frame(Formula(:(y ~ x2*x3 + x2&x3)), df))
@assert mm.model == [ones(4) x2 x3 x2.*x3]
mm = model_matrix(model_frame(Formula(:(y ~ x2*x3*x4)), df))
@assert mm.model == [ones(4) x2 x3 x4 x2.*x3 x2.*x4 x3.*x4 x2.*x3.*x4]
mm = model_matrix(model_frame(Formula(:(y ~ x2&x3 + x2*x3)), df))
@assert mm.model == [ones(4) x2.*x3 x2 x3]  # TODO This disagrees with R
mm = model_matrix(model_frame(Formula(:(y ~ x2 & x3 & x4)), df))
@assert mm.model == [ones(4) x2.*x3.*x4]

# test_group("Column groups in formulas")
set_group(d, "odd_predictors", ["x1","x3"])
@assert expand(:odd_predictors, d) == d["odd_predictors"]
mf = model_frame(Formula(:(y ~ odd_predictors)), d)
@assert mf.df[:,1] == d["y"]
@assert mf.df[:,2] == d["x1"]
@assert mf.df[:,3] == d["x3"]
@assert ncol(mf.df) == 3
mf = model_frame(Formula(:(y ~ odd_predictors * x2)), d)
mm = model_matrix(mf)
@assert mm.model == [ones(4) x1 x3 x2 x1.*x2 x3.*x2]
