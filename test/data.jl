using Base.Test
using DataFrames

#let
    #test_context("Data types and NA's")

    #test_group("NA's")
    @assert length(NA) == 1
    @assert size(NA) == ()
    @assert isna(3 == NA)
    @assert isna(NA == 3)
    @assert isna(NA == NA)

    #test_group("DataVector creation")
    dvint = DataVector[1, 2, NA, 4]
    dvint2 = DataArray([5:8])
    dvint3 = DataArray(5:8)
    dvflt = DataVector[1.0, 2, NA, 4]
    dvstr = DataVector["one", "two", NA, "four"]
    dvdict = DataArray(Dict,4)    # for issue #199

    @assert isa(dvint, DataVector{Int})
    @assert isa(dvint2, DataVector{Int})
    @assert isa(dvint3, DataVector{Int})
    @assert isa(dvflt, DataVector{Float64})
    @assert isa(dvstr, DataVector{ASCIIString})
    # @test throws_exception(DataArray([5:8], falses(2)), Exception) 

    @assert isequal(DataArray(dvint), dvint)

    #test_group("PooledDataVector creation")
    pdvstr = PooledDataVector["one", "one", "two", "two", NA, "one", "one"]
    @assert isa(pdvstr, PooledDataVector{ASCIIString})
    # @test throws_exception(PooledDataVector["one", "one", 9], Exception)
    @assert isequal(PooledDataArray(pdvstr), pdvstr)

    #test_group("PooledDataVector creation with predetermined pool")
    pdvpp = PooledDataArray([1, 2, 2, 3], [1, 2, 3, 4])
    @assert isequal(pdvpp.pool, [1, 2, 3, 4])
    @assert string(pdvpp) == "[1, 2, 2, 3]"
    pdvpp = PooledDataArray([1, 2, 2, 3, 2, 1], [1, 2, 3, 4])
    @assert isequal(pdvpp.pool, [1, 2, 3, 4])
    @assert string(pdvpp) == "[1, 2, 2, 3, 2, 1]"
    pdvpp = PooledDataArray(["one", "two", "two"], ["one", "two", "three"])
    @assert isequal(values(pdvpp), DataVector["one", "two", "two"])
    @assert all(get_indices(pdvpp) .== uint16([1, 2, 2]))
    @assert isequal(levels(pdvpp), DataVector["one", "two", "three"])
    @assert isequal(pdvpp.pool, ["one", "two", "three"])
    @assert string(pdvpp) == "[one, two, two]"
    @assert string(PooledDataArray(["one", "two", "four"], ["one", "two", "three"])) == "[one, two, NA]"

    #test_group("PooledDataVector utf8 support")
    pdvpp = PooledDataArray([utf8("hello")], [false])
    @assert isa(pdvpp[1], UTF8String)
    pdvpp = PooledDataArray([utf8("hello")])
    @assert isa(pdvpp[1], UTF8String)

    #test_group("DataVector access")
    @assert dvint[1] == 1
    @assert isna(dvint[3])
    @assert isequal(dvflt[3:4], DataVector[NA, 4.0])
    @assert isequal(dvint[[true, false, true, false]], DataVector[1, NA])
    @assert isequal(dvstr[[1, 2, 1, 4]], DataVector["one", "two", "one", "four"])
    @assert isequal(dvstr[[1, 2, 1, 3]], DataVector["one", "two", "one", NA])

    #test_group("PooledDataVector access")
    @assert pdvstr[1] == "one"
    @assert isna(pdvstr[5])
    @assert isequal(pdvstr[1:3], DataVector["one", "one", "two"])
    @assert isequal(pdvstr[[true, false, true, false, true, false, true]], PooledDataVector["one", "two", NA, "one"])
    @assert isequal(pdvstr[[1, 3, 1, 2]], DataVector["one", "two", "one", "one"])

    #test_group("DataVector methods")
    @assert size(dvint) == (4,)
    @assert length(dvint) == 4
    @assert sum(isna(dvint)) == 1
    @assert eltype(dvint) == Int

    #test_group("PooledDataVector methods")
    @assert size(pdvstr) == (7,)
    @assert length(pdvstr) == 7
    @assert sum(isna(pdvstr)) == 1
    @assert eltype(pdvstr) == ASCIIString

    #test_group("DataVector operations")
    @assert isequal(dvint + 1, DataArray([2, 3, 4, 5], [false, false, true, false]))
    @assert isequal(dvint .* 2, DataVector[2, 4, NA, 8])
    @assert isequal(dvint .== 2, DataVector[false, true, NA, false])
    @assert isequal(dvint .> 1, DataVector[false, true, NA, true])

    #test_group("PooledDataVector operations")
    # @assert isequal(pdvstr .== "two", PooledDataVector[false, false, true, true, NA, false, false])

    #test_group("DataVector to something else")
    @assert all(removeNA(dvint) .== [1, 2, 4])
    @assert all(replaceNA(dvint, 0) .== [1, 2, 0, 4])
    @assert all(convert(Vector{Int}, dvint2) .== [5:8])
    @assert all([i + 1 for i in dvint2] .== [6:9])
    @assert all([length(x)::Int for x in dvstr] == [3, 3, 1, 4])
    @assert repr(dvint) == "[1,2,NA,4]"

    #test_group("PooledDataVector to something else")
    @assert all(removeNA(pdvstr) .== ["one", "one", "two", "two", "one", "one"])
    @assert all(replaceNA(pdvstr, "nine") .== ["one", "one", "two", "two", "nine", "one", "one"])
    @assert all([length(i)::Int for i in pdvstr] .== [3, 3, 3, 3, 1, 3, 3])
    @assert string(pdvstr[1:3]) == "[one, one, two]"

    #test_group("DataVector Filter and Replace")
    @assert isequal(removeNA(dvint), [1, 2, 4])
    @assert isequal(replaceNA(dvint, 7), [1, 2, 7, 4])
    @assert sum(removeNA(dvint)) == 7
    @assert sum(replaceNA(dvint, 7)) == 14

    #test_group("PooledDataVector Filter and Replace")
    @assert reduce(string, "", removeNA(pdvstr)) == "oneonetwotwooneone"
    @assert reduce(string, "", replaceNA(pdvstr,"!")) == "oneonetwotwo!oneone"

    #test_group("DataVector assignment")
    assigntest = DataVector[1, 2, NA, 4]
    assigntest[1] = 8
    @assert isequal(assigntest, DataVector[8, 2, NA, 4])
    assigntest[1:2] = 9
    @assert isequal(assigntest, DataVector[9, 9, NA, 4])
    assigntest[[1,3]] = 10
    @assert isequal(assigntest, DataVector[10, 9, 10, 4])
    assigntest[[true, false, true, true]] = 11
    @assert isequal(assigntest, DataVector[11, 9, 11, 11])
    assigntest[1:2] = [12, 13]
    @assert isequal(assigntest, DataVector[12, 13, 11, 11])
    assigntest[[1, 4]] = [14, 15]
    @assert isequal(assigntest, DataVector[14, 13, 11, 15])
    assigntest[[true, false, true, false]] = [16, 17]
    @assert isequal(assigntest, DataVector[16, 13, 17, 15])
    assigntest[1] = NA
    @assert isequal(assigntest, DataVector[NA, 13, 17, 15])
    assigntest[[1, 2]] = NA
    @assert isequal(assigntest, DataVector[NA, NA, 17, 15])
    assigntest[[true, false, true, false]] = NA
    @assert isequal(assigntest, DataVector[NA, NA, NA, 15])
    assigntest[1] = 1
    assigntest[2:4] = NA
    @assert isequal(assigntest, DataVector[1, NA, NA, NA])

    #test_group("PooledDataVector assignment")
    ret = (pdvstr[2] = "three")
    @assert ret == "three"
    @assert pdvstr[2] == "three"
    ret = pdvstr[[1,2]] = "two"
    @assert ret == "two"
    @assert pdvstr[2] == "two"
    pdvstr2 = PooledDataVector["one", "one", "two", "two"]
    ret = (pdvstr2[[true, false, true, false]] = "three")
    @assert ret == "three"
    @assert pdvstr2[1] == "three"
    ret = (pdvstr2[[false, true, false, true]] = ["four", "five"]) 
    @assert isequal(ret, ["four", "five"])
    @assert isequal(pdvstr2[3:4], DataVector["three", "five"])
    pdvstr2 = PooledDataVector["one", "one", "two", "two"]
    ret = (pdvstr2[2:3] = "three")
    @assert ret == "three"
    @assert isequal(pdvstr2[3:4], DataVector["three", "two"])
    ret = (pdvstr2[2:3] = ["four", "five"])
    @assert ret == ["four", "five"]
    @assert isequal(pdvstr2[1:2], DataVector["one", "four"])
    pdvstr2 = PooledDataVector["one", "one", "two", "two", "three"]
    @assert isna(begin pdvstr2[1] = NA end)
    @assert all(isna(begin pdvstr2[[1, 2]] = NA end))
    @assert all(isna(begin pdvstr2[[false, false, true, false, false]] = NA end))
    @assert all(isna(begin pdvstr2[4:5] = NA end))
    @assert all(isna(pdvstr2))

    #test_group("PooledDataVector replace!")
    pdvstr2 = PooledDataVector["one", "one", "two", "two", "three"]
    @assert replace!(pdvstr2, "two", "four") == "four"
    @assert replace!(pdvstr2, "three", "four") == "four"
    @assert isna(replace!(pdvstr2, "one", NA))
    @assert replace!(pdvstr2, NA, "five") == "five"
    @assert isequal(pdvstr2, DataVector["five", "five", "four", "four", "four"])

    #test_context("DataFrames")

    #test_group("constructors")
    df1 = DataFrame({dvint, dvstr}, ["Ints", "Strs"])
    df2 = DataFrame({dvint, dvstr}) 
    df3 = DataFrame({dvint})
    df4 = DataFrame([1:4 1:4])
    df5 = DataFrame({DataVector[1,2,3,4], dvstr})
    df6 = DataFrame({dvint, dvint, dvstr}, ["A", "B", "C"])
    df7 = DataFrame(x = dvint, y = dvstr)
    @assert size(df7) == (4, 2)
    @assert isequal(df7["x"], dvint)

    #test_group("description functions")
    @assert nrow(df6) == 4
    @assert ncol(df6) == 3
    @assert all(colnames(df6) .== ["A", "B", "C"])
    @assert all(colnames(df2) .== ["x1", "x2"])
    @assert all(colnames(df7) .== ["x", "y"])

    #test_group("ref")
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

    #test_group("combining")

    dfc = cbind(df3, df4)
    @assert ncol(dfc) == 3
    @assert all(colnames(dfc) .== ["x1", "x1_1", "x2"])
    @assert isequal(dfc["x1"], df3["x1"])

    @assert isequal(dfc, [df3 df4])

    dfr = rbind(df4, df4)
    @assert nrow(dfr) == 8
    @assert all(colnames(df4) .== colnames(dfr))
    @assert isequal(dfr, [df4, df4])

    dfr = rbind(df2, df3)
    @assert size(dfr) == (8,2)
    @assert all(colnames(df2) .== colnames(dfr))
    @assert isna(dfr[8,"x2"])

    #test_group("show")
    @assert repr(df1) == "4x2 DataFrame:\n        Ints   Strs\n[1,]       1  \"one\"\n[2,]       2  \"two\"\n[3,]      NA     NA\n[4,]       4 \"four\"\n"

    #test_group("assign")
    df6[3] = DataVector["un", "deux", "troix", "quatre"]
    @assert df6[1, 3] == "un"
    df6["B"] = [4, 3, 2, 1]
    @assert df6[1,2] == 4
    df6["D"] = [true, false, true, false]
    @assert df6[1,4] == true
    delete!(df6, "D")
    @assert all(colnames(df6) .== ["A", "B", "C"])
    @assert ncol(df6) == 3

    #test_group("NA handling")
    @assert nrow(df5[complete_cases(df5), :]) == 3

    #test_context("SubDataFrames")

    #test_group("constructors")
    # single index is rows
    sdf6a = sub(df6, 1)
    sdf6b = sub(df6, 2:3)
    sdf6c = sub(df6, [true, false, true, false])
    @assert size(sdf6a) == (1,3)
    sdf6d = sub(df6, [1,3], "B")
    @assert size(sdf6d) == (2,1)

    #test_group("ref")
    @assert sdf6a[1,2] == 4

    #test_context("Within")
    #test_group("Associative")

    srand(1)
    a1 = [:a => [1, 2], :b => [3, 4], :c => [5, 6]]
    a2 = ["a" => [1, 2], "b" => [3, 4], "c" => [5, 6]]
    a3 = {"a" => [1, 2], "b" => [3, 4], :c => [5, 6]}

    @assert isequal(with(a1, :(c + 1)), a1[:c] + 1)
    @assert isequal(with(a2, :(c + 1)), with(a1, :(c + 1)))
    @assert isequal(with(a3, :(c + 1)), with(a1, :(c + 1)))
    @assert isequal(with(a3, :(c + 1 + 0 * b)), with(a1, :(c + 1)))

    a4 = within(a1, :( d = a + b ))
    @assert isequal(a4[:d], a1[:a] + a1[:b])
    @assert isequal(a4[:a], a1[:a])

    a4c = @transform(copy(a1), d => a + b )
    @assert isequal(a4[:d], a4c[:d])

    a4 = within(a2, :( d = a + b ))
    @assert isequal(a4["d"], a2["a"] + a2["b"])
    @assert isequal(a4["a"], a2["a"])

    # a4c = @transform(copy(a2), d => a + b )
    # @assert isequal(a4c["d"], a4["d"])

    a4 = within(a3, :( d = a + b ))
    @assert isequal(a4[:d], a3["a"] + a3["b"])
    @assert isequal(a4["a"], a3["a"])

    # Note: The following won't work. 
    #       @transform will only find the keys that are symbols.
    ## a4c = @transform(copy(a3), d => a + b )
    ## @assert isequal(a4c[:d], a4[:d])

    a4 = based_on(a1, :( d = a + b ))
    @assert isequal(a4[:d], a1[:a] + a1[:b])

    a4 = based_on(a2, :( d = a + b ))
    @assert isequal(a4["d"], a2["a"] + a2["b"])

    a4 = based_on(a3, :( d = a + b ))
    @assert isequal(a4[:d], a3["a"] + a3["b"])

    #test_group("DataFrame")

    srand(1)
    N = 20
    d1 = PooledDataArray(rand(1:2, N))
    d2 = PooledDataVector["A", "B", NA][rand(1:3, N)]
    d3 = DataArray(randn(N))
    d4 = DataArray(randn(N))
    df7 = DataFrame({d1, d2, d3}, ["d1", "d2", "d3"])

    @assert isequal(with(df7, :(d3 + d3)), df7["d3"] + df7["d3"])
    @assert isequal(with(df7, :(d3 + $d4)), df7["d3"] + d4)
    x = df7 |> with(:( d3 + d3 ))
    @assert isequal(x, df7["d3"] + df7["d3"])

    df8 = within(df7, :(d4 = d3 + d3 + 1))
    @assert isequal(df7, df8[1:3])
    @assert isequal(df8["d4"], df7["d3"] + df7["d3"] + 1)
    within!(df8, :( d4 = d1 ))
    @assert isequal(df8["d1"], df8["d4"])

    df8 = @transform(copy(df7), d4 => d3 + 1)
    @assert isequal(df7, df8[1:3])

    df8 = based_on(df7, :( d1 = d3 ))
    @assert isequal(df8["d1"], df7["d3"])
    df8 = df7 |> based_on(:( d1 = d3 ))
    @assert isequal(df8["d1"], df7["d3"])
    df8 = based_on(df7, :( sum_d3 = sum(d3) ))
    @assert isequal(df8[1,1], sum(df7["d3"]))

    #@assert all(df7[:( d2 .== "B" )]["d1"] .== PooledDataArray([1,2,1,1]))
    # TODO: Remove all tests that depend upon srand(), which was just changed.
    # TODO: Restore this test
    # @assert all(df7[:( d2 .== "B" ), "d1"] .== PooledDataArray([2,1,1,1,1,2,2,1,2]))

    #test_group("groupby")

    gd = groupby(df7, "d1")
    @assert length(gd) == 2
    # @assert isequal(gd[2]["d2"], PooledDataVector["A", "B", NA, "A", NA, NA, NA, NA])
    @assert sum(gd[2]["d3"]) == sum(df7["d3"][removeNA(df7["d1"] .== 2)])

    g1 = groupby(df7, ["d1", "d2"])
    g2 = groupby(df7, ["d2", "d1"])
    @assert sum(g1[1]["d3"]) == sum(g2[1]["d3"])

    res = 0.0
    for x in g1
        res += sum(x["d1"])
    end
    @assert res == sum(df7["d1"])

    df8 = df7 |> groupby(["d2"]) |> :( d3sum = sum(d3); d3mean = mean(removeNA(d3)) )
    @assert isequal(df8["d2"], PooledDataVector[NA, "A", "B"])

    df9 = based_on(groupby(df7, "d2"),
                   :( d3sum = sum(d3); d3mean = mean(removeNA(d3)) ))
    @assert isequal(df9, df8)

    df8 = within(groupby(df7, "d2"),
                 :( d4 = d3 + 1; d1sum = sum(d1) ))
    @assert all(df8[:( d2 .== "C" ), "d1sum"] .== 13)
     
    ## @assert isequal(with(g1, :( sum(d1) )), map(x -> sum(x["d1"]), g1))

    df8 = colwise(df7[[1, 3]], :sum)
    @assert df8[1, "d1_sum"] == sum(df7["d1"])

    df8 = colwise(groupby(df7, "d2"), [:sum, :length])
    @assert nrow(df8) == 3
    @assert ncol(df8) == 5
    #@assert df8[1, "d1_sum"] == 13
    # @assert df8[2, "d1_length"] == 7
    @assert df8[2, "d1_length"] == 8

    df9 = df7 |> groupby(["d2"]) |> [:sum, :length]
    @assert isequal(df9, df8)
    df9 = by(df7, "d2", [:sum, :length])
    @assert isequal(df9, df8)

    #test_group("reshape")

    d1 = DataFrame(quote
        a = [1:3]
        b = [1:4]
        c = randn(12)
        d = randn(12)
    end)

    d1c = @DataFrame(a => [1:3],
                     b => [1:4],
                     c => randn(12),
                     d => randn(12))
                     
    @assert isequal(d1[1:2], d1c[1:2])

    d1s = stack(d1, ["a", "b"])
    d1s2 = stack(d1, ["c", "d"])
    d1s3 = melt(d1, ["c", "d"])
    @assert isequal(d1s[1:12, "c"], d1["c"])
    @assert isequal(d1s[13:24, "c"], d1["c"])
    @assert all(colnames(d1s) .== ["variable", "value", "c", "d"])
    @assert isequal(d1s, d1s3)
    d1s_df = stack_df(d1, ["a", "b"])
    @assert isequal(d1s["variable"], d1s_df["variable"][:])
    @assert isequal(d1s["value"], d1s_df["value"][:])
    @assert isequal(d1s["c"], d1s_df["c"][:])
    @assert isequal(d1s[1,:], d1s_df[1,:])

    d1s["idx"] = [1:12, 1:12]
    d1s2["idx"] = [1:12, 1:12]
    d1us = unstack(d1s, "variable", "idx", "value")
    d1us2 = unstack(d1s2, "variable", "idx", "value")
    @assert isequal(d1us["a"], d1["a"])
    @assert isequal(d1us2["d"], d1["d"])

    d = DataFrame(quote
        a = letters[5:8]
        b = LETTERS[10:11]
        c = LETTERS[13 + [1, 1, 2, 2, 2, 1, 1, 2, 1, 2, 2, 1, 1, 2]]
        d = pi * [1:14]
    end)

    dpv = pivot_table(d, ["a", "b"], "c", "d")
    @assert( dpv[1,"O"] == d[5,"d"])
    @assert( nrow(dpv) == 4 )

    dpv2 = pivot_table(d, ["a"], ["c", "b"], "d")
    @assert( dpv2[1,"O_J"] == d[5,"d"])

    dpv3 = pivot_table(d, ["a"], ["c", "b"], "d", length)
    @assert( dpv3[1,"O_J"] == 1.0)

    #test_group("merge")

    srand(1)
    df1 = DataFrame(quote
        a = shuffle!([1:10])
        b = ["A","B"][rand(1:2, 10)]
        v1 = randn(10)
    end)

    df2 = DataFrame(quote
        a = shuffle!(reverse([1:5]))
        b2 = ["A","B","C"][rand(1:3, 5)]
        v2 = randn(3)    # test unequal lengths in the constructor
    end)

    m1 = join(df1, df2, on = "a")
    @assert isequal(m1["a"], DataVector[1, 2, 3, 4, 5])
    m2 = join(df1, df2, on = "a", kind = :outer)
    # @assert isequal(m2["b2"], DataVector["A", "B", "B", "B", "B", NA, NA, NA, NA, NA])
    # @assert isequal(m2["b2"], DataVector["B", "B", "B", "C", "B", NA, NA, NA, NA, NA])

    df1 = DataFrame({"a" => [1, 2, 3],
                     "b" => ["America", "Europe", "Africa"]})
    df2 = DataFrame({"a" => [1, 2, 4],
                     "c" => ["New World", "Old World", "New World"]})
    m1 = join(df1, df2, on = "a", kind = :inner)
    @assert isequal(m1["a"], DataVector[1, 2])
    m2 = join(df1, df2, on = "a", kind = :left)
    @assert isequal(m2["a"], DataVector[1, 2, 3])
    m3 = join(df1, df2, on = "a", kind = :right)
    @assert isequal(m3["a"], DataVector[1, 2, 4])
    m4 = join(df1, df2, on = "a", kind = :outer)
    @assert isequal(m4["a"], DataVector[1, 2, 3, 4])

    # test with NAs (issue #185)
    df1 = DataFrame()
    df1["A"] = DataVector["a", "b", "a", NA]
    df1["B"] = DataVector[1, 2, 1, 3]

    df2 = DataFrame()
    df2["A"] = DataVector["a", NA, "c"]
    df2["C"] = DataVector[1, 2, 4]

    m1 = join(df1, df2, on = "A")
    @assert size(m1) == (3,3) 
    @assert isequal(m1["A"], DataVector[NA,"a","a"])
    m2 = join(df1, df2, on = "A", kind = :outer)
    @assert size(m2) == (5,3) 
    @assert isequal(m2["A"], DataVector[NA,"a","a","b","c"])

    srand(1)
    df1 = DataFrame(quote
        a = ["x","y"][rand(1:2, 10)]
        b = ["A","B"][rand(1:2, 10)]
        v1 = randn(10)
    end)

    df2 = DataFrame(quote
        a = ["x","y"][[1,2,1,1,2]]
        b = ["A","B","C"][[1,1,1,2,3]]
        v2 = randn(5)    
    end)
    df2[1,"a"] = NA

    # TODO: Restore this functionality
    m1 = join(df1, df2, on = ["a","b"])
    @assert isequal(m1["a"], DataArray(["x", "x", "y", "y", fill("x", 5)]))
    m2 = join(df1, df2, on = ["a","b"], kind = :outer)
    @assert isequal(m2[10,"v2"], NA)
    @assert isequal(m2["a"], DataVector["x", "x", "y", "y", "x", "x", "x", "x", "x", "y", NA, "y"])

    m1a = join(within(df1, :(key = PooledDataArray(_DF[["a","b"]]))),
               based_on(df2, :(key = PooledDataArray(_DF[["a","b"]]); v2 = v2)),
               on = "key")
    m2a = join(within(df1, :(key = PooledDataArray(_DF[["a","b"]]))),
               based_on(df2, :(key = PooledDataArray(_DF[["a","b"]]); v2 = v2)),
               on = "key",
               kind = :outer)
    @assert isequal(sort(m1["b"]), sort(m1a["b"]))

    srand(1)
    function spltdf(d)
        d["x1"] = map(x -> x[1], d["a"])
        d["x2"] = map(x -> x[2], d["a"])
        d["x3"] = map(x -> x[3], d["a"])
        d
    end
    df1 = DataFrame(quote
        a = ["abc","abx", "axz", "def", "dfr"]
        v1 = randn(5)
    end)
    df1 = spltdf(df1)
    df2 = DataFrame(quote
        a = ["def", "abc","abx", "axz", "xyz"]
        v2 = randn(5)    
    end)
    df2 = spltdf(df2)

    m1 = join(df1, df2, on = "a")
    m2 = join(df1, df2, on = ["x1", "x2", "x3"])
    @assert isequal(sort(m1["a"]), sort(m2["a"]))

    #test_group("New DataVector constructors")
    dv = DataArray(Int, 5)
    @assert all(isna(dv))
    dv = DataArray(Float64, 5)
    @assert all(isna(dv))
    dv = datazeros(5)
    @assert all(dv .== 0.0)
    dv = dataones(5)
    @assert all(dv .== 1.0)

    # No more NA corruption
    dv = dataones(10_000)
    @assert !any(isna(dv))

    PooledDataArray(falses(2), falses(2))
    PooledDataArray(falses(2), trues(2))

    # Test vectorized comparisons work for DataVector's and PooledDataVector's
    DataVector[1, 2, NA] .== 1
    PooledDataVector[1, 2, NA] .== 1
    DataVector["1", "2", NA] .== "1"
    PooledDataVector["1", "2", NA] .== "1"

    # Test unique()
    #test_group("unique()")
    dv = DataArray(1:4)
    dv[4] = NA
    @assert (1 in unique(dv))
    @assert (2 in unique(dv))
    @assert (3 in unique(dv))
    @assert (NA in unique(dv))

    #test_group("find()")
    dv = DataArray([true, false, true])
    @assert isequal(find(dv), [1, 3])

    pdv = PooledDataArray([true, false, true])
    @assert isequal(find(pdv), [1, 3])

    dv[1] = NA
    @assert isequal(find(dv), [3])

    pdv[1] = NA
    @assert isequal(find(pdv), [3])
#end
