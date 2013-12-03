using Base.Test
using DataFrames

#let
    #test_group("DataVector creation")
    dvint = @data([1, 2, NA, 4])
    dvint2 = DataArray([5:8])
    dvint3 = DataArray(5:8)
    dvflt = @data([1.0, 2, NA, 4])
    dvstr = @data(["one", "two", NA, "four"])
    dvdict = DataArray(Dict,4)    # for issue #199

    #test_group("constructors")
    df1 = DataFrame({dvint, dvstr}, ["Ints", "Strs"])
    df2 = DataFrame({dvint, dvstr}) 
    df3 = DataFrame({dvint})
    df4 = DataFrame([1:4 1:4])
    df5 = DataFrame({@data([1,2,3,4]), dvstr})
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
    # @assert repr(df1) == "4x2 DataFrame:\n        Ints   Strs\n[1,]       1  \"one\"\n[2,]       2  \"two\"\n[3,]      NA     NA\n[4,]       4 \"four\"\n"

    #test_group("assign")
    df6[3] = @data(["un", "deux", "troix", "quatre"])
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
    d2 = @pdata(["A", "B", NA])[rand(1:3, N)]
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
    @assert isequal(df8["d2"], @pdata([NA, "A", "B"]))

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
    # TODO: Fix this
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
    @assert isequal(m1["a"], @data([1, 2, 3, 4, 5]))
    # TODO: Re-enable
    # m2 = join(df1, df2, on = "a", kind = :outer)
    # @assert isequal(m2["b2"], DataVector["A", "B", "B", "B", "B", NA, NA, NA, NA, NA])
    # @assert isequal(m2["b2"], DataVector["B", "B", "B", "C", "B", NA, NA, NA, NA, NA])

    df1 = DataFrame({"a" => [1, 2, 3],
                     "b" => ["America", "Europe", "Africa"]})
    df2 = DataFrame({"a" => [1, 2, 4],
                     "c" => ["New World", "Old World", "New World"]})
    m1 = join(df1, df2, on = "a", kind = :inner)
    @assert isequal(m1["a"], @data([1, 2]))
    m2 = join(df1, df2, on = "a", kind = :left)
    @assert isequal(m2["a"], @data([1, 2, 3]))
    m3 = join(df1, df2, on = "a", kind = :right)
    @assert isequal(m3["a"], @data([1, 2, 4]))
    # TODO: Re-enable
    # m4 = join(df1, df2, on = "a", kind = :outer)
    # @assert isequal(m4["a"], DataVector[1, 2, 3, 4])

    # # test with NAs (issue #185)
    # df1 = DataFrame()
    # df1["A"] = DataVector["a", "b", "a", NA]
    # df1["B"] = DataVector[1, 2, 1, 3]

    # df2 = DataFrame()
    # df2["A"] = DataVector["a", NA, "c"]
    # df2["C"] = DataVector[1, 2, 4]

    # m1 = join(df1, df2, on = "A")
    # @assert size(m1) == (3,3) 
    # @assert isequal(m1["A"], DataVector[NA,"a","a"])
    # m2 = join(df1, df2, on = "A", kind = :outer)
    # @assert size(m2) == (5,3) 
    # @assert isequal(m2["A"], DataVector[NA,"a","a","b","c"])

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

    # # TODO: Restore this functionality
    # m1 = join(df1, df2, on = ["a","b"])
    # @assert isequal(m1["a"], DataArray(["x", "x", "y", "y", fill("x", 5)]))
    # m2 = join(df1, df2, on = ["a","b"], kind = :outer)
    # @assert isequal(m2[10,"v2"], NA)
    # @assert isequal(m2["a"], DataVector["x", "x", "y", "y", "x", "x", "x", "x", "x", "y", NA, "y"])

    # m1a = join(within(df1, :(key = PooledDataArray(_DF[["a","b"]]))),
    #            based_on(df2, :(key = PooledDataArray(_DF[["a","b"]]); v2 = v2)),
    #            on = "key")
    # m2a = join(within(df1, :(key = PooledDataArray(_DF[["a","b"]]))),
    #            based_on(df2, :(key = PooledDataArray(_DF[["a","b"]]); v2 = v2)),
    #            on = "key",
    #            kind = :outer)
    # @assert isequal(sort(m1["b"]), sort(m1a["b"]))

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

    # m1 = join(df1, df2, on = "a")
    # m2 = join(df1, df2, on = ["x1", "x2", "x3"])
    # @assert isequal(sort(m1["a"]), sort(m2["a"]))

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
    @data([1, 2, NA]) .== 1
    @pdata([1, 2, NA]) .== 1
    @data(["1", "2", NA]) .== "1"
    @pdata(["1", "2", NA]) .== "1"

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
