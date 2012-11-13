test_context("Data types and NAs")

test_group("NAs")
@test length(NA) == 1
@test size(NA) == ()
#@test (3 == NA) == NA Ironically not testable!
#@test (NA == 3) == NA
#@test (NA == NA) == NA

test_group("DataVec creation")
# why can't I put @test before these?
dvint = DataVec[1, 2, NA, 4]
dvint2 = DataVec([5:8])
dvflt = DataVec[1.0, 2, NA, 4]
dvstr = DataVec["one", "two", NA, "four"]

@test typeof(dvint) == DataVec{Int64}
@test typeof(dvint2) == DataVec{Int64}
@test typeof(dvflt) == DataVec{Float64}
@test typeof(dvstr) == DataVec{ASCIIString}
@test throws_exception(DataVec[[5:8], falses(2)], Exception) 

@test DataVec(dvint) == dvint 

test_group("PooledDataVec creation")
pdvstr = PooledDataVec["one", "one", "two", "two", NA, "one", "one"]
@test typeof(pdvstr) == PooledDataVec{ASCIIString}
@test throws_exception(PooledDataVec["one", "one", 9], Exception)
@test PooledDataVec(pdvstr) == pdvstr

test_group("PooledDataVec creation with predetermined pool")
pdvpp = PooledDataVec([1,2,2,3], [1,2,3,4])
@test pdvpp.pool == [1,2,3,4]
@test string(pdvpp) == "[1,2,2,3]"
@test throws_exception(PooledDataVec([1,2,3], [1,2]), Exception)
pdvpp = PooledDataVec([1,2,2,3,2,1], [1,2,3,4])
@test pdvpp.pool == [1,2,3,4]
@test string(pdvpp) == "[1,2,2,3,2,1]"
pdvpp = PooledDataVec(["one","two","two"], ["one","two","three"])
@test all(values(pdvpp) .== ["one","two","two"])
@test all(indices(pdvpp) .== uint16([1,3,3]))
@test all(levels(pdvpp) .== ["one","three","two"])
@test pdvpp.pool == ["one","three","two"]
@test string(pdvpp) == "[one,two,two]"
@test throws_exception(PooledDataVec(["one","two","four"], ["one","two","three"]), Exception)

test_group("PooledDataVec utf8 support")
pdvpp = PooledDataVec([utf8("hello")],[false],KEEP,utf8(""))
@test typeof(pdvpp[1]) == UTF8String
pdvpp = PooledDataVec([utf8("hello")])
@test typeof(pdvpp[1]) == UTF8String

test_group("DataVec access")
@test dvint[1] == 1
@test isna(dvint[3])
@test dvflt[3:4] == DataVec[NA,4.0]
@test dvint[[true, false, true, false]] == DataVec[1,NA]
@test dvstr[[1,2,1,4]] == DataVec["one", "two", "one", "four"]
@test dvstr[[1,2,1,3]] == DataVec["one", "two", "one", NA] 

test_group("PooledDataVec access")
@test pdvstr[1] == "one"
@test isna(pdvstr[5])
@test pdvstr[1:3] == DataVec["one", "one", "two"]
@test pdvstr[[true, false, true, false, true, false, true]] == DataVec["one", "two", NA, "one"]
@test pdvstr[[1,3,1,2]] == DataVec["one", "two", "one", "one"]

test_group("DataVec methods")
@test size(dvint) == (4,)
@test length(dvint) == 4
@test sum(isna(dvint)) == 1
@test eltype(dvint) == Int64

test_group("PooledDataVec methods")
@test size(pdvstr) == (7,)
@test length(pdvstr) == 7
@test sum(isna(pdvstr)) == 1
@test eltype(pdvstr) == ASCIIString

test_group("DataVec operations")
@test dvint+1 == DataVec([2,3,4,5], [false, false, true, false])
@test dvint.*2 == DataVec[2,4,NA,8]
@test (dvint .== 2) == DataVec[false, true, NA, false]
@test (dvint .> 1) == DataVec[false, true, NA, true]

test_group("PooledDataVec operations")
@test (pdvstr .== "two") == PooledDataVec[false, false, true, true, NA, false, false]

test_group("DataVec to something else")
@test all(nafilter(dvint) == [1,2,4]) # TODO: test.jl should grok all(a == b)
@test all(nareplace(dvint,0) == [1,2,0,4])
@test all(convert(Int, dvint2) == [5:8])
@test all([i+1 for i=dvint2] == [6:9]) # iterator test
@test all([length(x)::Int for x=dvstr] == [3,3,1,4])
@test repr(dvint) == "[1,2,NA,4]"

test_group("PooledDataVec to something else")
#@test all(nafilter(pdvstr) == ["one", "one", "two", "two", "one", "one"])
#@test all(nareplace(pdvstr, "nine") == ["one", "one", "two", "two", "nine", "one", "one"])
@test all([length(i)::Int for i in pdvstr] == [3, 3, 3, 3, 1, 3, 3])
@test string(pdvstr[1:3]) == "[one,one,two]"

test_group("DataVec Filter and Replace")
@test naFilter(dvint) == dvint
@test naReplace(dvint,7) == dvint
@test sum(naFilter(dvint)) == 7
@test sum(naReplace(dvint,7)) == 14

test_group("PooledDataVec Filter and Replace")
@test reduce(strcat, "", naFilter(pdvstr)) == "oneonetwotwooneone"
@test reduce(strcat, "", naReplace(pdvstr,"!")) == "oneonetwotwo!oneone"

test_group("DataVec assignment")
assigntest = DataVec[1, 2, NA, 4]
assigntest[1] = 8
@test assigntest == DataVec[8, 2, NA, 4]
assigntest[1:2] = 9
@test assigntest == DataVec[9, 9, NA, 4]
assigntest[[1,3]] = 10
@test assigntest == DataVec[10, 9, 10, 4]
assigntest[[true, false, true, true]] = 11
@test assigntest == DataVec[11, 9, 11, 11]
assigntest[1:2] = [12,13]
@test assigntest == DataVec[12, 13, 11, 11]
assigntest[[1,4]] = [14,15]
@test assigntest == DataVec[14, 13, 11, 15]
assigntest[[true,false,true,false]] = [16,17]
@test assigntest == DataVec[16, 13, 17, 15]
assigntest[1] = NA
@test assigntest == DataVec[NA, 13, 17, 15]
assigntest[[1,2]] = NA
@test assigntest == DataVec[NA, NA, 17, 15]
assigntest[[true,false,true,false]] = NA
@test assigntest == DataVec[NA, NA, NA, 15]
assigntest[1] = 1
assigntest[2:4] = NA
@test assigntest == DataVec[1, NA, NA, NA]

test_group("PooledDataVec assignment")
@test (pdvstr[2] = "three") == "three" 
@test pdvstr[2] == "three"
@test (pdvstr[[1,2]] = "two") == "two"
@test pdvstr[2] == "two"
pdvstr2 = PooledDataVec["one", "one", "two", "two"]
@test (pdvstr2[[true, false, true, false]] = "three") == "three"
@test pdvstr2[1] == "three"
@test (pdvstr2[[false, true, false, true]] = ["four", "five"]) == ["four", "five"]
@test pdvstr2[3:4] == DataVec["three", "five"]
pdvstr2 = PooledDataVec["one", "one", "two", "two"]
@test (pdvstr2[2:3] = "three") == "three"
@test pdvstr2[3:4] == DataVec["three", "two"]
@test (pdvstr2[2:3] = ["four", "five"]) == ["four", "five"]
@test pdvstr2[1:2] == DataVec["one", "four"]
pdvstr2 = PooledDataVec["one", "one", "two", "two", "three"]
@test isna(begin pdvstr2[1] = NA end)
@test all(isna(begin pdvstr2[[1,2]] = NA end))
@test all(isna(begin pdvstr2[[false, false, true, false, false]] = NA end))
@test all(isna(begin pdvstr2[4:5] = NA end))
@test all(isna(pdvstr2))

test_group("PooledDataVec replace!")
pdvstr2 = PooledDataVec["one", "one", "two", "two", "three"]
@test replace!(pdvstr2, "two", "four") == "four"
@test replace!(pdvstr2, "three", "four") == "four"
@test isna(replace!(pdvstr2, "one", NA))
@test replace!(pdvstr2, NA, "five") == "five"
@test pdvstr2 == DataVec["five", "five", "four", "four", "four"]

test_context("DataFrames")

test_group("constructors")
df1 = DataFrame({dvint, dvstr}, ["Ints", "Strs"])
df2 = DataFrame({dvint, dvstr}) 
df3 = DataFrame({dvint})
df4 = DataFrame([1:4 1:4])
df5 = DataFrame({DataVec[1,2,3,4], dvstr})
df6 = DataFrame({dvint, dvint, dvstr}, ["A", "B", "C"])

test_group("description functions")
@test nrow(df6) == 4
@test ncol(df6) == 3
@test all(names(df6) == ["A", "B", "C"])
@test all(names(df2) == ["x1", "x2"])

test_group("ref")
@test df6[2,3] == "two"
@test isna(df6[3,3])
@test df6[2, "C"] == "two"
@test df6["B"] == dvint
@test ncol(df6[[2,3]]) == 2
@test nrow(df6[2,:]) == 1
@test size(df6[[1,3], [1,3]]) == (2,2)
@test size(df6[1:2, 1:2]) == (2,2)
@test size(head(df6,2)) == (2,3)
# lots more to do


test_group("combining")

dfc = cbind(df3, df4)
@assert ncol(dfc) == 3
@assert colnames(dfc) == ["x1", "x1_1", "x2"] 
@assert dfc["x1"] == df3["x1"] 

@assert dfc == [df3 df4]

dfr = rbind(df4, df4)
@assert nrow(dfr) == 8
@assert colnames(df4) == colnames(dfr)
@assert dfr == [df4, df4]


test_group("show")
@test repr(df1) == "DataFrame  (4,2)\n        Ints   Strs\n[1,]       1  \"one\"\n[2,]       2  \"two\"\n[3,]      NA     NA\n[4,]       4 \"four\"\n"

test_group("assign")
df6[3] = DataVec["un", "deux", "troix", "quatre"]
@test df6[1,3] == "un"
df6["B"] = [4,3,2,1]
@test df6[1,2] == 4
df6["D"] = [true, false, true, false]
@test df6[1,4] == true
del!(df6, "D")
@test colnames(df6) == ["A", "B", "C"]
df6b = del(df6, 1)
@test ncol(df6) == 3
@test ncol(df6b) == 2

test_group("NA handling")
@test nrow(df5[complete_cases(df5),:]) == 3

test_context("SubDataFrames")

test_group("constructors")
# single index is rows
sdf6a = sub(df6, 1)
sdf6b = sub(df6, 2:3)
sdf6c = sub(df6, [true, false, true, false])
@test size(sdf6a) == (1,3)
sdf6d = sub(df6, [1,3], "B")
@test size(sdf6d) == (2,1)

test_group("ref")
@test sdf6a[1,2] == 4

test_context("Within")
test_group("Associative")

srand(1)
a1 = {:a => [1,2], :b => [3,4], :c => [5,6]}
a2 = {"a" => [1,2], "b" => [3,4], "c" => [5,6]}
a3 = {"a" => [1,2], "b" => [3,4], :c => [5,6]}

@assert with(a1, :( c + 1 )) == a1[:c] + 1
@assert with(a2, :( c + 1 )) == with(a1, :( c + 1 ))
@assert with(a3, :( c + 1 )) == with(a1, :( c + 1 ))
@assert with(a3, :( c + 1 + 0 * b)) == with(a1, :( c + 1 ))

a4 = within(a1, :( d = a + b ))
@assert a4[:d] == a1[:a] + a1[:b]
@assert a4[:a] == a1[:a]

a4 = within(a2, :( d = a + b ))
@assert a4["d"] == a2["a"] + a2["b"]
@assert a4["a"] == a2["a"]

a4 = within(a3, :( d = a + b ))
@assert a4[:d] == a3["a"] + a3["b"]
@assert a4["a"] == a3["a"]

a4 = based_on(a1, :( d = a + b ))
@assert a4[:d] == a1[:a] + a1[:b]

a4 = based_on(a2, :( d = a + b ))
@assert a4["d"] == a2["a"] + a2["b"]

a4 = based_on(a3, :( d = a + b ))
@assert a4[:d] == a3["a"] + a3["b"]


test_group("DataFrame")

srand(1)
N = 20
d1 = PooledDataVec(randi(2,N))
d2 = PooledDataVec["A", "B", NA][randi(3,N)]
d3 = DataVec(randn(N))
d4 = DataVec(randn(N))
df7 = DataFrame({d1,d2,d3}, ["d1","d2","d3"])


@assert with(df7, :(d3 + d3)) == df7["d3"] + df7["d3"]
@assert with(df7, :(d3 + $d4)) == df7["d3"] + d4
x = df7 | with(:( d3 + d3 ))
@assert x == df7["d3"] + df7["d3"]

df8 = within(df7, :(d4 = d3 + d3 + 1))
@assert df7 == df8[1:3]
@assert df8["d4"] == df7["d3"] + df7["d3"] + 1
within!(df8, :( d4 = d1 ))
@assert df8["d1"] == df8["d4"]

df8 = based_on(df7, :( d1 = d3 ))
@assert df8["d1"] == df7["d3"]
df8 = df7 | based_on(:( d1 = d3 ))
@assert df8["d1"] == df7["d3"]
df8 = based_on(df7, :( sum_d3 = sum(d3) ))
@assert df8[1,1] == sum(df7["d3"])


@assert df7[:( d2 .== "B" )]["d1"] == PooledDataVec([1,2,1,1]) 
@assert df7[:( d2 .== "B" ), "d1"] == PooledDataVec([1,2,1,1]) 


test_group("groupby")

gd = groupby(df7, "d1")
@assert length(gd) == 2
@assert gd[2]["d2"] == PooledDataVec["A","B",NA,"A",NA,NA,NA,NA]
@assert sum(gd[2]["d3"]) == sum(df7["d3"][nafilter(df7["d1"] .== 2)])

g1 = groupby(df7, ["d1","d2"])
g2 = groupby(df7, ["d2","d1"])
@assert sum(g1[1]["d3"]) == sum(g2[1]["d3"])

res = 0.0
for x in g1
    res += sum(x["d1"])
end
@assert res == sum(df7["d1"])

df8 = df7 | groupby(["d2"]) | :( d3sum = sum(d3); d3mean = mean(nafilter(d3)) )
@assert df8["d2"] == PooledDataVec[NA, "A", "B"] # may change if these end up getting sorted
df9 = based_on(groupby(df7, "d2"),
               :( d3sum = sum(d3); d3mean = mean(nafilter(d3)) ))
@assert df9 == df8

df8 = within(groupby(df7, "d2"),
             :( d4 = d3 + 1; d1sum = sum(d1) ))
@assert all(df8[:( d2 .== "C" )]["d1sum"] .== 13)
             
@assert with(g1, :( sum(d1) )) == map(x -> sum(x["d1"]), g1)

df8 = colwise(df7[[1,3]], :sum)
@assert df8[1,"d1_sum"] == sum(df7["d1"])

df8 = colwise(groupby(df7, "d2"), [:sum, :length])
@assert nrow(df8) == 3
@assert ncol(df8) == 5
@assert df8[1,"d1_sum"] == 13
@assert df8[2,"d1_length"] == 8

df9 = df7 | groupby(["d2"]) | [:sum, :length]
@assert df9 == df8
df9 = by(df7, "d2", [:sum, :length])
@assert df9 == df8

test_group("reshape")

d1 = DataFrame(quote
    a = [1:3]
    b = [1:4]
    c = randn(12)
    d = randn(12)
end)

d1s = stack(d1, ["a", "b"])
d1s2 = stack(d1, ["c", "d"])
@assert d1s[1:12,"c"] == d1["c"]
@assert d1s[13:24,"c"] == d1["c"]
@assert colnames(d1s) == ["key", "value", "c", "d"]

d1s["idx"] = [1:12, 1:12]
d1s2["idx"] = [1:12, 1:12]
d1us = unstack(d1s, "key", "value", "idx")
d1us2 = unstack(d1s2, "key", "value", "idx")
@assert d1us["a"] == d1["a"]
@assert d1us2["d"] == d1["d"]

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
@assert m1["b"] == DataVec["B", "A", "B", "A", "B"]
m2 = merge(df1, df2, "a", "outer")
@assert m2["b2"] == DataVec["B", "C", "A", "A", "A", NA, NA, NA, NA, NA]

test_group("extras")

srand(1)
x = randi(10,10) - 4.0
a1 = cut(x, 4)
a2 = cut(x, [-2, 3, 4.0])
@assert a2[1] == "[-3.0,-2.0]"
@assert a2[2] == "(-2.0,3.0]"
@assert a2[4] == "(4.0,6.0]"

