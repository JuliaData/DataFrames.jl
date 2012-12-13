require("extras/test.jl")

load("DataFrames")
using DataFrames

test_context("Data types and NA's")

test_group("NA's")
@assert length(NA) == 1
@assert size(NA) == ()
@assert isna(3 == NA)
@assert isna(NA == 3)
@assert isna(NA == NA)

test_group("DataVec creation")
dvint = DataVec[1, 2, NA, 4]
dvint2 = DataVec([5:8])
dvint3 = DataVec(5:8)
dvflt = DataVec[1.0, 2, NA, 4]
dvstr = DataVec["one", "two", NA, "four"]

@assert isa(dvint, DataVec{Int64})
@assert isa(dvint2, DataVec{Int64})
@assert isa(dvint3, DataVec{Int64})
@assert isa(dvflt, DataVec{Float64})
@assert isa(dvstr, DataVec{ASCIIString})
@test throws_exception(DataVec([5:8], falses(2)), Exception) 

@assert isequal(DataVec(dvint), dvint)

test_group("PooledDataVec creation")
pdvstr = PooledDataVec["one", "one", "two", "two", NA, "one", "one"]
@assert isa(pdvstr, PooledDataVec{ASCIIString})
@test throws_exception(PooledDataVec["one", "one", 9], Exception)
@assert isequal(PooledDataVec(pdvstr), pdvstr)

test_group("PooledDataVec creation with predetermined pool")
pdvpp = PooledDataVec([1, 2, 2, 3], [1, 2, 3, 4])
@assert isequal(pdvpp.pool, [1, 2, 3, 4])
@assert string(pdvpp) == "[1, 2, 2, 3]"
@test throws_exception(PooledDataVec([1, 2, 3], [1, 2]), Exception)
pdvpp = PooledDataVec([1, 2, 2, 3, 2, 1], [1, 2, 3, 4])
@assert isequal(pdvpp.pool, [1, 2, 3, 4])
@assert string(pdvpp) == "[1, 2, 2, 3, 2, 1]"
pdvpp = PooledDataVec(["one", "two", "two"], ["one", "two", "three"])
@assert isequal(values(pdvpp), DataVec["one", "two", "two"])
@assert all(indices(pdvpp) .== uint16([1, 3, 3]))
@assert isequal(levels(pdvpp), DataVec["one", "three", "two"])
@assert isequal(pdvpp.pool, ["one", "three", "two"])
@assert string(pdvpp) == "[one, two, two]"
@test throws_exception(PooledDataVec(["one", "two", "four"], ["one", "two", "three"]), Exception)

test_group("PooledDataVec utf8 support")
pdvpp = PooledDataVec([utf8("hello")], [false])
@assert isa(pdvpp[1], UTF8String)
pdvpp = PooledDataVec([utf8("hello")])
@assert isa(pdvpp[1], UTF8String)

test_group("DataVec access")
@assert dvint[1] == 1
@assert isna(dvint[3])
@assert isequal(dvflt[3:4], DataVec[NA, 4.0])
@assert isequal(dvint[[true, false, true, false]], DataVec[1, NA])
@assert isequal(dvstr[[1, 2, 1, 4]], DataVec["one", "two", "one", "four"])
@assert isequal(dvstr[[1, 2, 1, 3]], DataVec["one", "two", "one", NA])

test_group("PooledDataVec access")
@assert pdvstr[1] == "one"
@assert isna(pdvstr[5])
@assert isequal(pdvstr[1:3], DataVec["one", "one", "two"])
@assert isequal(pdvstr[[true, false, true, false, true, false, true]], PooledDataVec["one", "two", NA, "one"])
@assert isequal(pdvstr[[1, 3, 1, 2]], DataVec["one", "two", "one", "one"])

test_group("DataVec methods")
@assert size(dvint) == (4,)
@assert length(dvint) == 4
@assert sum(isna(dvint)) == 1
@assert eltype(dvint) == Int64

test_group("PooledDataVec methods")
@assert size(pdvstr) == (7,)
@assert length(pdvstr) == 7
@assert sum(isna(pdvstr)) == 1
@assert eltype(pdvstr) == ASCIIString

test_group("DataVec operations")
@assert isequal(dvint + 1, DataVec([2, 3, 4, 5], [false, false, true, false]))
@assert isequal(dvint .* 2, DataVec[2, 4, NA, 8])
@assert isequal(dvint .== 2, DataVec[false, true, NA, false])
@assert isequal(dvint .> 1, DataVec[false, true, NA, true])

test_group("PooledDataVec operations")
@assert isequal(pdvstr .== "two", PooledDataVec[false, false, true, true, NA, false, false])

test_group("DataVec to something else")
@assert all(removeNA(dvint) .== [1, 2, 4])
@assert all(replaceNA(dvint, 0) .== [1, 2, 0, 4])
@assert all(convert(Int, dvint2) .== [5:8])
@assert all([i + 1 for i in dvint2] .== [6:9])
@assert all([length(x)::Int for x in dvstr] == [3, 3, 1, 4])
@assert repr(dvint) == "[1,2,NA,4]"

test_group("PooledDataVec to something else")
@assert all(removeNA(pdvstr) .== ["one", "one", "two", "two", "one", "one"])
@assert all(replaceNA(pdvstr, "nine") .== ["one", "one", "two", "two", "nine", "one", "one"])
@assert all([length(i)::Int for i in pdvstr] .== [3, 3, 3, 3, 1, 3, 3])
@assert string(pdvstr[1:3]) == "[one, one, two]"

test_group("DataVec Filter and Replace")
@assert isequal(removeNA(dvint), [1, 2, 4])
@assert isequal(replaceNA(dvint, 7), [1, 2, 7, 4])
@assert sum(removeNA(dvint)) == 7
@assert sum(replaceNA(dvint, 7)) == 14

test_group("PooledDataVec Filter and Replace")
@assert reduce(strcat, "", removeNA(pdvstr)) == "oneonetwotwooneone"
@assert reduce(strcat, "", replaceNA(pdvstr,"!")) == "oneonetwotwo!oneone"

test_group("DataVec assignment")
assigntest = DataVec[1, 2, NA, 4]
assigntest[1] = 8
@assert isequal(assigntest, DataVec[8, 2, NA, 4])
assigntest[1:2] = 9
@assert isequal(assigntest, DataVec[9, 9, NA, 4])
assigntest[[1,3]] = 10
@assert isequal(assigntest, DataVec[10, 9, 10, 4])
assigntest[[true, false, true, true]] = 11
@assert isequal(assigntest, DataVec[11, 9, 11, 11])
assigntest[1:2] = [12, 13]
@assert isequal(assigntest, DataVec[12, 13, 11, 11])
assigntest[[1, 4]] = [14, 15]
@assert isequal(assigntest, DataVec[14, 13, 11, 15])
assigntest[[true, false, true, false]] = [16, 17]
@assert isequal(assigntest, DataVec[16, 13, 17, 15])
assigntest[1] = NA
@assert isequal(assigntest, DataVec[NA, 13, 17, 15])
assigntest[[1, 2]] = NA
@assert isequal(assigntest, DataVec[NA, NA, 17, 15])
assigntest[[true, false, true, false]] = NA
@assert isequal(assigntest, DataVec[NA, NA, NA, 15])
assigntest[1] = 1
assigntest[2:4] = NA
@assert isequal(assigntest, DataVec[1, NA, NA, NA])

test_group("PooledDataVec assignment")
ret = (pdvstr[2] = "three")
@assert ret == "three"
@assert pdvstr[2] == "three"
ret = pdvstr[[1,2]] = "two"
@assert ret == "two"
@assert pdvstr[2] == "two"
pdvstr2 = PooledDataVec["one", "one", "two", "two"]
ret = (pdvstr2[[true, false, true, false]] = "three")
@assert ret == "three"
@assert pdvstr2[1] == "three"
ret = (pdvstr2[[false, true, false, true]] = ["four", "five"]) 
@assert isequal(ret, ["four", "five"])
@assert isequal(pdvstr2[3:4], DataVec["three", "five"])
pdvstr2 = PooledDataVec["one", "one", "two", "two"]
ret = (pdvstr2[2:3] = "three")
@assert ret == "three"
@assert isequal(pdvstr2[3:4], DataVec["three", "two"])
ret = (pdvstr2[2:3] = ["four", "five"])
@assert ret == ["four", "five"]
@assert isequal(pdvstr2[1:2], DataVec["one", "four"])
pdvstr2 = PooledDataVec["one", "one", "two", "two", "three"]
@assert isna(begin pdvstr2[1] = NA end)
@assert all(isna(begin pdvstr2[[1, 2]] = NA end))
@assert all(isna(begin pdvstr2[[false, false, true, false, false]] = NA end))
@assert all(isna(begin pdvstr2[4:5] = NA end))
@assert all(isna(pdvstr2))

test_group("PooledDataVec replace!")
pdvstr2 = PooledDataVec["one", "one", "two", "two", "three"]
@assert replace!(pdvstr2, "two", "four") == "four"
@assert replace!(pdvstr2, "three", "four") == "four"
@assert isna(replace!(pdvstr2, "one", NA))
@assert replace!(pdvstr2, NA, "five") == "five"
@assert isequal(pdvstr2, DataVec["five", "five", "four", "four", "four"])

test_context("DataFrames")

test_group("constructors")
df1 = DataFrame({dvint, dvstr}, ["Ints", "Strs"])
df2 = DataFrame({dvint, dvstr}) 
df3 = DataFrame({dvint})
df4 = DataFrame([1:4 1:4])
df5 = DataFrame({DataVec[1,2,3,4], dvstr})
df6 = DataFrame({dvint, dvint, dvstr}, ["A", "B", "C"])

test_group("description functions")
@assert nrow(df6) == 4
@assert ncol(df6) == 3
@assert all(colnames(df6) .== ["A", "B", "C"])
@assert all(colnames(df2) .== ["x1", "x2"])

test_group("ref")
@assert df6[2, 3] == "two"
@assert isna(df6[3, 3])
@assert df6[2, "C"] == "two"
@assert isequal(df6["B"], dvint)
@assert ncol(df6[[2,3]]) == 2
@assert nrow(df6[2,:]) == 1
@assert size(df6[[1, 3], [1, 3]]) == (2, 2)
@assert size(df6[1:2, 1:2]) == (2, 2)
@assert size(head(df6,2)) == (2, 3)
# lots more to do

test_group("combining")

dfc = cbind(df3, df4)
@assert ncol(dfc) == 3
@assert all(colnames(dfc) .== ["x1", "x1_1", "x2"])
@assert isequal(dfc["x1"], df3["x1"])

@assert isequal(dfc, [df3 df4])

dfr = rbind(df4, df4)
@assert nrow(dfr) == 8
@assert all(colnames(df4) .== colnames(dfr))
@assert isequal(dfr, [df4, df4])

test_group("show")
@assert repr(df1) == "4x2 DataFrame:\n        Ints   Strs\n[1,]       1  \"one\"\n[2,]       2  \"two\"\n[3,]      NA     NA\n[4,]       4 \"four\"\n"

test_group("assign")
df6[3] = DataVec["un", "deux", "troix", "quatre"]
@assert df6[1, 3] == "un"
df6["B"] = [4, 3, 2, 1]
@assert df6[1,2] == 4
df6["D"] = [true, false, true, false]
@assert df6[1,4] == true
del!(df6, "D")
@assert all(colnames(df6) .== ["A", "B", "C"])
df6b = del(df6, 1)
@assert ncol(df6) == 3
@assert ncol(df6b) == 2

test_group("NA handling")
@assert nrow(df5[complete_cases(df5), :]) == 3

test_context("SubDataFrames")

test_group("constructors")
# single index is rows
sdf6a = sub(df6, 1)
sdf6b = sub(df6, 2:3)
sdf6c = sub(df6, [true, false, true, false])
@assert size(sdf6a) == (1,3)
sdf6d = sub(df6, [1,3], "B")
@assert size(sdf6d) == (2,1)

test_group("ref")
@assert sdf6a[1,2] == 4

test_context("Within")
test_group("Associative")

srand(1)
a1 = {:a => [1, 2], :b => [3, 4], :c => [5, 6]}
a2 = {"a" => [1, 2], "b" => [3, 4], "c" => [5, 6]}
a3 = {"a" => [1, 2], "b" => [3, 4], :c => [5, 6]}

@assert isequal(with(a1, :(c + 1)), a1[:c] + 1)
@assert isequal(with(a2, :(c + 1)), with(a1, :(c + 1)))
@assert isequal(with(a3, :(c + 1)), with(a1, :(c + 1)))
@assert isequal(with(a3, :(c + 1 + 0 * b)), with(a1, :(c + 1)))

a4 = within(a1, :( d = a + b ))
@assert isequal(a4[:d], a1[:a] + a1[:b])
@assert isequal(a4[:a], a1[:a])

a4 = within(a2, :( d = a + b ))
@assert isequal(a4["d"], a2["a"] + a2["b"])
@assert isequal(a4["a"], a2["a"])

a4 = within(a3, :( d = a + b ))
@assert isequal(a4[:d], a3["a"] + a3["b"])
@assert isequal(a4["a"], a3["a"])

a4 = based_on(a1, :( d = a + b ))
@assert isequal(a4[:d], a1[:a] + a1[:b])

a4 = based_on(a2, :( d = a + b ))
@assert isequal(a4["d"], a2["a"] + a2["b"])

a4 = based_on(a3, :( d = a + b ))
@assert isequal(a4[:d], a3["a"] + a3["b"])

test_group("DataFrame")

srand(1)
N = 20
d1 = PooledDataVec(randi(2, N))
d2 = PooledDataVec["A", "B", NA][randi(3, N)]
d3 = DataVec(randn(N))
d4 = DataVec(randn(N))
df7 = DataFrame({d1, d2, d3}, ["d1", "d2", "d3"])

@assert isequal(with(df7, :(d3 + d3)), df7["d3"] + df7["d3"])
@assert isequal(with(df7, :(d3 + $d4)), df7["d3"] + d4)
x = df7 | with(:( d3 + d3 ))
@assert isequal(x, df7["d3"] + df7["d3"])

df8 = within(df7, :(d4 = d3 + d3 + 1))
@assert isequal(df7, df8[1:3])
@assert isequal(df8["d4"], df7["d3"] + df7["d3"] + 1)
within!(df8, :( d4 = d1 ))
@assert isequal(df8["d1"], df8["d4"])

df8 = based_on(df7, :( d1 = d3 ))
@assert isequal(df8["d1"], df7["d3"])
df8 = df7 | based_on(:( d1 = d3 ))
@assert isequal(df8["d1"], df7["d3"])
df8 = based_on(df7, :( sum_d3 = sum(d3) ))
@assert isequal(df8[1,1], sum(df7["d3"]))

@assert all(df7[:( d2 .== "B" )]["d1"] .== PooledDataVec([1,2,1,1]))
@assert all(df7[:( d2 .== "B" ), "d1"] .== PooledDataVec([1,2,1,1]))

test_group("groupby")

gd = groupby(df7, "d1")
@assert length(gd) == 2
@assert isequal(gd[2]["d2"], PooledDataVec["A", "B", NA, "A", NA, NA, NA, NA])
@assert sum(gd[2]["d3"]) == sum(df7["d3"][removeNA(df7["d1"] .== 2)])

g1 = groupby(df7, ["d1", "d2"])
g2 = groupby(df7, ["d2", "d1"])
@assert sum(g1[1]["d3"]) == sum(g2[1]["d3"])

res = 0.0
for x in g1
    res += sum(x["d1"])
end
@assert res == sum(df7["d1"])

df8 = df7 | groupby(["d2"]) | :( d3sum = sum(d3); d3mean = mean(removeNA(d3)) )
@assert isequal(df8["d2"], PooledDataVec[NA, "A", "B"])

df9 = based_on(groupby(df7, "d2"),
               :( d3sum = sum(d3); d3mean = mean(removeNA(d3)) ))
@assert isequal(df9, df8)

df8 = within(groupby(df7, "d2"),
             :( d4 = d3 + 1; d1sum = sum(d1) ))
@assert all(df8[:( d2 .== "C" )]["d1sum"] .== 13)
 
@assert all(with(g1, :( sum(d1) )) .== map(x -> sum(x["d1"]), g1))

df8 = colwise(df7[[1, 3]], :sum)
@assert df8[1, "d1_sum"] == sum(df7["d1"])

df8 = colwise(groupby(df7, "d2"), [:sum, :length])
@assert nrow(df8) == 3
@assert ncol(df8) == 5
@assert df8[1, "d1_sum"] == 13
@assert df8[2, "d1_length"] == 8

df9 = df7 | groupby(["d2"]) | [:sum, :length]
@assert isequal(df9, df8)
df9 = by(df7, "d2", [:sum, :length])
@assert isequal(df9, df8)

test_group("reshape")

d1 = DataFrame(quote
    a = [1:3]
    b = [1:4]
    c = randn(12)
    d = randn(12)
end)

d1s = stack(d1, ["a", "b"])
d1s2 = stack(d1, ["c", "d"])
@assert isequal(d1s[1:12, "c"], d1["c"])
@assert isequal(d1s[13:24, "c"], d1["c"])
@assert all(colnames(d1s) .== ["key", "value", "c", "d"])

d1s["idx"] = [1:12, 1:12]
d1s2["idx"] = [1:12, 1:12]
d1us = unstack(d1s, "key", "value", "idx")
d1us2 = unstack(d1s2, "key", "value", "idx")
@assert isequal(d1us["a"], d1["a"])
@assert isequal(d1us2["d"], d1["d"])

test_group("merge")

srand(1)
df1 = DataFrame(quote
    a = shuffle([1:10])
    b = ["A","B"][randi(2,10)]
    v1 = randn(10)
end)

df2 = DataFrame(quote
    a = shuffle(reverse([1:5]))
    b2 = ["A","B","C"][randi(3,5)]
    v2 = randn(3)    # test unequal lengths in the constructor
end)

m1 = merge(df1, df2, "a")
@assert isequal(m1["b"], DataVec["B", "A", "B", "A", "B"])
m2 = merge(df1, df2, "a", "outer")
@assert isequal(m2["b2"], DataVec["B", "C", "A", "A", "A", NA, NA, NA, NA, NA])

test_group("extras")

srand(1)
x = randi(10,10) - 4.0
a1 = cut(x, 4)
a2 = cut(x, [-2, 3, 4.0])
@assert a2[1] == "[-3.0,-2.0]"
@assert a2[2] == "(-2.0,3.0]"
@assert a2[4] == "(4.0,6.0]"

test_group("New DataVec constructors")
dv = DataVec(Int64, 5)
@assert all(isna(dv))
dv = DataVec(Float64, 5)
@assert all(isna(dv))
dv = dvzeros(5)
@assert all(dv .== 0.0)
dv = dvones(5)
@assert all(dv .== 1.0)

# No more NA corruption
dv = dvones(10_000)
@assert !any(isna(dv))

PooledDataVec(falses(2), falses(2))
PooledDataVec(falses(2), trues(2))

# Test vectorized comparisons work for DataVec's and PooledDataVec's
DataVec[1, 2, NA] .== 1
PooledDataVec[1, 2, NA] .== 1
DataVec["1", "2", NA] .== "1"
PooledDataVec["1", "2", NA] .== "1"
