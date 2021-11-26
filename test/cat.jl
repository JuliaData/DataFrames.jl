module TestCat

using Test, Random, DataFrames, CategoricalArrays
const ≅ = isequal

@testset "hcat" begin
    nvint = [1, 2, missing, 4]
    nvstr = ["one", "two", missing, "four"]

    df2 = DataFrame([nvint, nvstr], :auto)
    df3 = DataFrame([nvint], :auto)
    df4 = DataFrame([1:4 1:4], [:x1, :x2])
    df5 = DataFrame([Union{Int, Missing}[1, 2, 3, 4], nvstr], :auto)

    ref_df = copy(df3)
    dfh = hcat(df3, df4, makeunique=true)
    @test ref_df ≅ df3 # make sure that df3 is not mutated by hcat
    @test size(dfh, 2) == 3
    @test names(dfh) ≅ ["x1", "x1_1", "x2"]
    @test dfh[!, :x1] ≅ df3[!, :x1]
    @test dfh ≅ DataFrames.hcat!(DataFrame(), df3, df4, makeunique=true)

    dfa = DataFrame(a=[1, 2])
    dfb = DataFrame(b=[3, missing])
    @test hcat(dfa, dfb) ≅ [dfa dfb]

    dfh3 = hcat(df3, df4, df5, makeunique=true)
    @test names(dfh3) == ["x1", "x1_1", "x2", "x1_2", "x2_1"]
    @test dfh3 ≅ hcat(dfh, df5, makeunique=true)
    @test dfh3 ≅ DataFrames.hcat!(DataFrame(), df3, df4, df5, makeunique=true)

    @test df2 ≅ DataFrames.hcat!(df2, makeunique=true)
end

@testset "hcat: copying" begin
    df = DataFrame(x=1:3)
    @test hcat(df)[!, 1] == df[!, 1]
    @test hcat(df)[!, 1] !== df[!, 1]
    hdf = hcat(df, df, makeunique=true)
    @test hdf[!, 1] == df[!, 1]
    @test hdf[!, 1] !== df[!, 1]
    @test hdf[!, 2] == df[!, 1]
    @test hdf[!, 2] !== df[!, 1]
    @test hdf[!, 1] == hdf[!, 2]
    @test hdf[!, 1] !== hdf[!, 2]
    hdf = hcat(df, df, df, makeunique=true)
    @test hdf[!, 1] == df[!, 1]
    @test hdf[!, 1] !== df[!, 1]
    @test hdf[!, 2] == df[!, 1]
    @test hdf[!, 2] !== df[!, 1]
    @test hdf[!, 3] == df[!, 1]
    @test hdf[!, 3] !== df[!, 1]
    @test hdf[!, 1] == hdf[!, 2]
    @test hdf[!, 1] !== hdf[!, 2]
    @test hdf[!, 1] == hdf[!, 3]
    @test hdf[!, 1] !== hdf[!, 3]
    @test hdf[!, 2] == hdf[!, 3]
    @test hdf[!, 2] !== hdf[!, 3]
end

@testset "hcat ::AbstractDataFrame" begin
    df = DataFrame(A=repeat('A':'C', inner=4), B=1:12)
    gd = groupby(df, :A)
    answer = DataFrame(A=fill('A', 4), B=1:4, A_1='B', B_1=5:8, A_2='C', B_2=9:12)
    @test hcat(gd..., makeunique=true) == answer
    answer = answer[:, 1:4]
    @test hcat(gd[1], gd[2], makeunique=true) == answer

    @test_throws MethodError hcat("a", df, makeunique=true)
    @test_throws MethodError hcat(df, "a", makeunique=true)
end

@testset "hcat: copycols" begin
    df1 = DataFrame(a=1:3)
    df2 = DataFrame(b=1:3)
    dfv = view(df2, :, :)
    x = [1, 2, 3]

    df3 = hcat(df1)
    @test df3 == df1
    @test df3.a !== df1.a
    df3 = hcat(df1, copycols=true)
    @test df3 == df1
    @test df3.a !== df1.a
    df3 = hcat(df1, copycols=false)
    @test df3 == df1
    @test df3.a === df1.a

    df3 = hcat(df1, df2)
    @test propertynames(df3) == [:a, :b]
    @test df3.a == df1.a
    @test df3.b == df2.b
    @test df3.a !== df1.a
    @test df3.b !== df2.b
    df3 = hcat(df1, df2, copycols=true)
    @test propertynames(df3) == [:a, :b]
    @test df3.a == df1.a
    @test df3.b == df2.b
    @test df3.a !== df1.a
    @test df3.b !== df2.b
    df3 = hcat(df1, df2, copycols=false)
    @test propertynames(df3) == [:a, :b]
    @test df3.a === df1.a
    @test df3.b === df2.b

    df3 = hcat(df1, dfv)
    @test propertynames(df3) == [:a, :b]
    @test df3.a == df1.a
    @test df3.b == df2.b
    @test df3.a !== df1.a
    @test df3.b !== df2.b
    df3 = hcat(df1, dfv, copycols=true)
    @test propertynames(df3) == [:a, :b]
    @test df3.a == df1.a
    @test df3.b == df2.b
    @test df3.a !== df1.a
    @test df3.b !== df2.b
    df3 = hcat(df1, dfv, copycols=false)
    @test propertynames(df3) == [:a, :b]
    @test df3.a === df1.a
    @test df3.b === dfv.b
end

@testset "vcat" begin
    missing_df = DataFrame()
    df = DataFrame([3.0  2.0  2.0
                    2.0  2.0  2.0
                    3.0  1.0  2.0
                    3.0  3.0  3.0], :auto)
    df[!, 3] = Int.(df[!, 3])

    @test vcat(missing_df) == DataFrame()
    @test vcat(missing_df, missing_df) == DataFrame()
    @test vcat(missing_df) == DataFrame()
    @test vcat(missing_df, missing_df) == DataFrame()
    @test vcat(missing_df, df) == df
    @test vcat(df, missing_df) == df
    @test eltype.(eachcol(vcat(df, df))) == Type[Float64, Float64, Int]
    @test size(vcat(df, df)) == (size(df, 1) * 2, size(df, 2))
    res = vcat(df, df)
    @test res[1:size(df, 1), :] == df
    @test res[(1+size(df, 1)):end, :] == df
    res = vcat(df, df, df)
    @test eltype.(eachcol(res)) == Type[Float64, Float64, Int]
    @test size(res) == (size(df, 1) * 3, size(df, 2))

    s = size(df, 1)
    for i in 1:3
        @test res[1+(i-1)*s:i*s, :] == df
    end

    alt_df = deepcopy(df)
    @test vcat(df, alt_df) == DataFrame([[3.0, 2.0, 3.0, 3.0, 3.0, 2.0, 3.0, 3.0],
                                         [2.0, 2.0, 1.0, 3.0, 2.0, 2.0, 1.0, 3.0],
                                         [2, 2, 2, 3, 2, 2, 2, 3]], :auto)

    # Don't fail on non-matching types
    df[!, 1] = zeros(Int, nrow(df))
    @test vcat(df, alt_df) == DataFrame([[0.0, 0.0, 0.0, 0.0, 3.0, 2.0, 3.0, 3.0],
                                         [2.0, 2.0, 1.0, 3.0, 2.0, 2.0, 1.0, 3.0],
                                         [2, 2, 2, 3, 2, 2, 2, 3]], :auto)

    df1 = DataFrame(A=Int[], B=Float64[])
    df2 = DataFrame(B=1.0, A=1)
    @test vcat(df2, df1, df2, df1) == vcat(df2, df2)

    df = DataFrame(A=1:5, B=11:15, C=21:25)
    @test vcat(view(df, 1:2, :), view(df, 3:5, [3, 2, 1])) == df
    @test_throws ArgumentError view(df, 1:2, [1, 2, 3, 1, 2, 3])
    @test_throws ArgumentError view(df, 3:5, [3, 2, 1, 1, 2, 3])
end

@testset "vcat copy" begin
    df = DataFrame(x=1:3)
    @test vcat(df)[!, 1] == df[!, 1]
    @test vcat(df)[!, 1] !== df[!, 1]
end

@testset "vcat >2 args" begin
    empty_dfs = [DataFrame(), DataFrame(), DataFrame()]
    @test vcat(empty_dfs...) == reduce(vcat, empty_dfs) == DataFrame()
    @test reduce(vcat, Tuple(empty_dfs)) == DataFrame()

    df = DataFrame(x=trues(1), y=falses(1))
    dfs = [df, df, df]
    @test vcat(dfs...) == reduce(vcat, dfs) == DataFrame(x=trues(3), y=falses(3))
    @test reduce(vcat, Tuple(dfs)) == DataFrame(x=trues(3), y=falses(3))
end

@testset "vcat mixed coltypes" begin
    df = vcat(DataFrame([[1]], [:x]), DataFrame([[1.0]], [:x]))
    @test df == DataFrame([[1.0, 1.0]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Float64}]
    df = vcat(DataFrame([[1]], [:x]), DataFrame([["1"]], [:x]))
    @test df == DataFrame([[1, "1"]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Any}]
    df = vcat(DataFrame([Union{Missing, Int}[1]], [:x]), DataFrame([[1]], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Union{Missing, Int}}]
    df = vcat(DataFrame([CategoricalArray([1])], [:x]), DataFrame([[1]], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test df.x isa Vector{Int}
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([Union{Missing, Int}[1]], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test df.x isa Vector{Union{Int, Missing}}
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([CategoricalArray{Union{Int, Missing}}([1])], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test df.x isa CategoricalVector{Union{Int, Missing}}
    df = vcat(DataFrame([Union{Int, Missing}[1]], [:x]),
              DataFrame([["1"]], [:x]))
    @test df == DataFrame([[1, "1"]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Any}]
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([CategoricalArray([1.0])], [:x]))
    @test df == DataFrame([[1.0, 1.0]], [:x])
    @test df.x isa CategoricalVector{Float64}
    @test levels(df.x) == [1.0]
    df = vcat(DataFrame([trues(1)], [:x]), DataFrame([[false]], [:x]))
    @test df == DataFrame([[true, false]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Bool}]
end

@testset "vcat out of order" begin
    df1 = DataFrame(A=1:3, B=4:6, C=7:9)
    df2 = DataFrame([2x for x in eachcol(df1)], reverse(names(df1)))
    @test vcat(df1, df2) == DataFrame(A=[1, 2, 3, 14, 16, 18],
                                      B=[4, 5, 6, 8, 10, 12],
                                      C=[7, 8, 9, 2, 4, 6])
    @test vcat(df1, df1, df2) == DataFrame(A=[1, 2, 3, 1, 2, 3, 14, 16, 18],
                                           B=[4, 5, 6, 4, 5, 6, 8, 10, 12],
                                           C=[7, 8, 9, 7, 8, 9, 2, 4, 6])
    @test vcat(df1, df2, df2) == DataFrame(A=[1, 2, 3, 14, 16, 18, 14, 16, 18],
                                           B=[4, 5, 6, 8, 10, 12, 8, 10, 12],
                                           C=[7, 8, 9, 2, 4, 6, 2, 4, 6])
    @test vcat(df2, df1, df2) == DataFrame(C=[2, 4, 6, 7, 8, 9, 2, 4, 6],
                                           B=[8, 10, 12, 4, 5, 6, 8, 10, 12],
                                           A=[14, 16, 18, 1, 2, 3, 14, 16, 18])
    @test size(vcat(df1, df1, df1, df2, df2, df2)) == (18, 3)
    df3 = df1[:, [1, 3, 2]]
    res = vcat(df1, df1, df1, df2, df2, df2, df3, df3, df3, df3)
    @test res == reduce(vcat, [df1, df1, df1, df2, df2, df2, df3, df3, df3, df3])
    @test res == reduce(vcat, (df1, df1, df1, df2, df2, df2, df3, df3, df3, df3))
    @test size(res) == (30, 3)
    @test res[1:3, :] == df1
    @test res[4:6, :] == df1
    @test res[7:9, :] == df1
    @test res[10:12, :] == df2[:, names(res)]
    @test res[13:15, :] == df2[:, names(res)]
    @test res[16:18, :] == df2[:, names(res)]
    @test res[19:21, :] == df3[:, names(res)]
    @test res[22:24, :] == df3[:, names(res)]
    @test res[25:27, :] == df3[:, names(res)]

    df1 = DataFrame(A=1, B=2)
    df2 = DataFrame(B=12, A=11)
    df3 = DataFrame(A=[1, 11], B=[2, 12])
    @test [df1; df2] == df3 == reduce(vcat, [df1, df2])
    @test df3 == reduce(vcat, (df1, df2))
    @test_throws ArgumentError vcat(df1, df2, cols=:orderequal)
    @test_throws ArgumentError reduce(vcat, [df1, df2], cols=:orderequal)
end

@testset "vcat with cols=:union" begin
    df1 = DataFrame(A=1:3, B=4:6)
    df2 = DataFrame(A=7:9)
    df3 = DataFrame(B=4:6, A=1:3)

    @test vcat(df1, df2; cols = :union) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9],
                  B=[4, 5, 6, missing, missing, missing])
    @test vcat(df1, df2; cols = :union) ≅ reduce(vcat, [df1, df2]; cols = :union)
    @test vcat(df1, df2; cols = :union) ≅ reduce(vcat, (df1, df2); cols = :union)
    @test vcat(df1, df2, df3; cols = :union) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9, 1, 2, 3],
                  B=[4, 5, 6, missing, missing, missing, 4, 5, 6])
    @test vcat(df1, df2, df3; cols = :union) ≅ reduce(vcat, [df1, df2, df3]; cols = :union)
    @test vcat(df1, df2, df3; cols = :union) ≅ reduce(vcat, (df1, df2, df3); cols = :union)
end

@testset "vcat with cols=:intersect" begin
    df1 = DataFrame(A=1:3, B=4:6)
    df2 = DataFrame(A=7:9)
    df3 = DataFrame(A=10:12, C=13:15)

    @test vcat(df1, df2; cols = :intersect) ≅ DataFrame(A=[1, 2, 3, 7, 8, 9])
    @test vcat(df1, df2; cols = :intersect) ≅ reduce(vcat, [df1, df2]; cols = :intersect)
    @test vcat(df1, df2; cols = :intersect) ≅ reduce(vcat, (df1, df2); cols = :intersect)
    @test vcat(df1, df2, df3; cols = :intersect) ≅ DataFrame(A=[1, 2, 3, 7, 8, 9,
                                                                10, 11, 12])
    @test vcat(df1, df2, df3; cols = :intersect) ≅ reduce(vcat, [df1, df2, df3]; cols = :intersect)
    @test vcat(df1, df2, df3; cols = :intersect) ≅ reduce(vcat, (df1, df2, df3); cols = :intersect)
end

@testset "vcat with cols::Vector" begin
    df1 = DataFrame(A=1:3, B=4:6)
    df2 = DataFrame(A=7:9)
    df3 = DataFrame(A=10:12, C=13:15)

    @test vcat(df1, df2; cols = [:A, :B, :C]) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9],
                  B=[4, 5, 6, missing, missing, missing],
                  C=[missing, missing, missing, missing, missing, missing])
    @test vcat(df1, df2; cols = [:A, :B, :C]) ≅
          reduce(vcat, [df1, df2]; cols = [:A, :B, :C])
    @test vcat(df1, df2; cols = [:A, :B, :C]) ≅
          reduce(vcat, (df1, df2); cols = [:A, :B, :C])

    @test vcat(df1, df2, df3; cols = [:A, :B, :C]) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9, 10, 11, 12],
                  B=[4, 5, 6, missing, missing, missing, missing, missing, missing],
                  C=[missing, missing, missing, missing, missing, missing, 13, 14, 15])
    @test vcat(df1, df2, df3; cols = [:A, :B, :C]) ≅
          reduce(vcat, [df1, df2, df3]; cols = [:A, :B, :C])
    @test vcat(df1, df2, df3; cols = [:A, :B, :C]) ≅
          reduce(vcat, (df1, df2, df3); cols = [:A, :B, :C])

    df1 = DataFrame(A=Int[], B=Float64[])
    df2 = DataFrame(B=1.0, A=1)
    @test vcat(df1, df2, df1, cols=[:A, :C, :B]) ≅ DataFrame(A=1, C=missing, B=1.0)
    @test vcat(df1, df2, df1, cols=[:A, :C, :B]) ≅
          reduce(vcat, [df1, df2, df1], cols=[:A, :C, :B])
    @test vcat(df1, df2, df1, cols=[:A, :C, :B]) ≅
          reduce(vcat, (df1, df2, df1), cols=[:A, :C, :B])
    @test vcat(df1, df2, df2, cols=[:C]) ≅ DataFrame(C=[missing, missing])
    @test vcat(df1, df2, df2, cols=[:C]) ≅ reduce(vcat, [df1, df2, df2], cols=[:C])
    @test vcat(df1, df2, df2, cols=[:C]) ≅ reduce(vcat, (df1, df2, df2), cols=[:C])
    @test_throws ArgumentError vcat(df1, df2, df2, cols=[:C, :C])
    @test_throws ArgumentError reduce(vcat, [df1, df2, df2], cols=[:C, :C])
    @test_throws ArgumentError reduce(vcat, (df1, df2, df2), cols=[:C, :C])

    df1 = DataFrame(A=1:3, B=4:6)
    df2 = DataFrame(A=7:9)
    df3 = DataFrame(A=10:12, C=13:15)

    @test vcat(df1, df2; cols = ["A", "B", "C"]) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9],
                  B=[4, 5, 6, missing, missing, missing],
                  C=[missing, missing, missing, missing, missing, missing])
    @test vcat(df1, df2; cols = ["A", "B", "C"]) ≅
          reduce(vcat, [df1, df2]; cols = ["A", "B", "C"])
    @test vcat(df1, df2; cols = ["A", "B", "C"]) ≅
          reduce(vcat, (df1, df2); cols = ["A", "B", "C"])

    @test vcat(df1, df2, df3; cols = ["A", "B", "C"]) ≅
        DataFrame(A=[1, 2, 3, 7, 8, 9, 10, 11, 12],
                  B=[4, 5, 6, missing, missing, missing, missing, missing, missing],
                  C=[missing, missing, missing, missing, missing, missing, 13, 14, 15])
    @test vcat(df1, df2, df3; cols = ["A", "B", "C"]) ≅
          reduce(vcat, [df1, df2, df3]; cols = ["A", "B", "C"])
    @test vcat(df1, df2, df3; cols = ["A", "B", "C"]) ≅
          reduce(vcat, (df1, df2, df3); cols = ["A", "B", "C"])

    df1 = DataFrame(A=Int[], B=Float64[])
    df2 = DataFrame(B=1.0, A=1)
    @test vcat(df1, df2, df1, cols=["A", "C", "B"]) ≅ DataFrame(A=1, C=missing, B=1.0)
    @test vcat(df1, df2, df1, cols=["A", "C", "B"]) ≅
          reduce(vcat, [df1, df2, df1], cols=["A", "C", "B"])
    @test vcat(df1, df2, df1, cols=["A", "C", "B"]) ≅
          reduce(vcat, (df1, df2, df1), cols=["A", "C", "B"])
    @test vcat(df1, df2, df2, cols=["C"]) ≅ DataFrame(C=[missing, missing])
    @test vcat(df1, df2, df2, cols=["C"]) ≅ reduce(vcat, [df1, df2, df2], cols=["C"])
    @test vcat(df1, df2, df2, cols=["C"]) ≅ reduce(vcat, (df1, df2, df2), cols=["C"])
    @test_throws ArgumentError vcat(df1, df2, df2, cols=[:C, :C])
    @test_throws ArgumentError reduce(vcat, [df1, df2, df2], cols=["C", "C"])
    @test_throws ArgumentError reduce(vcat, (df1, df2, df2), cols=["C", "C"])
end

@testset "vcat thrown exceptions" begin
    df1 = DataFrame(A=1:3, B=1:3)
    df2 = DataFrame(A=1:3)

    # wrong cols argument
    @test_throws ArgumentError vcat(df1, df1, cols=:unions)
    # right missing 1 column
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) B are missing from argument(s) 2"
    # left missing 1 column
    err = @test_throws ArgumentError vcat(df2, df1)
    @test err.value.msg == "column(s) B are missing from argument(s) 1"
    # multiple missing 1 column
    err1 = @test_throws ArgumentError vcat(df1, df2, df2, df2, df2, df2)
    err2 = @test_throws ArgumentError reduce(vcat, [df1, df2, df2, df2, df2, df2])
    @test_throws ArgumentError reduce(vcat, (df1, df2, df2, df2, df2, df2))
    @test err1.value.msg == err2.value.msg ==
          "column(s) B are missing from argument(s) 2, 3, 4, 5 and 6"
    # argument missing >1 columns
    df1 = DataFrame(A=1:3, B=1:3, C=1:3, D=1:3, E=1:3)
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2"
    # >1 arguments missing >1 columns
    err = @test_throws ArgumentError vcat(df1, df2, df2, df2, df2)
    @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2, 3, 4 and 5"
    # missing columns throws error
    df1 = DataFrame(A=1, B=1)
    df2 = DataFrame(A=1)
    df3 = DataFrame(B=1, A=1)
    err = @test_throws ArgumentError vcat(df1, df2, df3)
    @test err.value.msg == "column(s) B are missing from argument(s) 2"
    # unique columns for both sides
    df1 = DataFrame(A=1, B=1, C=1, D=1)
    df2 = DataFrame(A=1, C=1, D=1, E=1, F=1)
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, " *
                           "and column(s) B are missing from argument(s) 2"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, " *
                           "and column(s) B are missing from argument(s) 3 and 4"
    df3 = DataFrame(A=1, B=1, C=1, D=1, E=1)
    err = @test_throws ArgumentError vcat(df1, df2, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, " *
                           "column(s) B are missing from argument(s) 2, " *
                           "and column(s) F are missing from argument(s) 3"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2, df3, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, " *
                           "column(s) B are missing from argument(s) 3 and 4, " *
                           "and column(s) F are missing from argument(s) 5 and 6"
    err = @test_throws ArgumentError vcat(df1, df1, df1, df2, df2, df2, df3, df3, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, " *
                           "column(s) B are missing from argument(s) 4, 5 and 6, " *
                           "and column(s) F are missing from argument(s) 7, 8 and 9"
    # df4 is a superset of names found in all other DataFrames and won't be shown in error
    df4 = DataFrame(A=1, B=1, C=1, D=1, E=1, F=1)
    err = @test_throws ArgumentError vcat(df1, df2, df3, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, " *
                           "column(s) B are missing from argument(s) 2, " *
                           "and column(s) F are missing from argument(s) 3"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2, df3, df3, df4, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, " *
                           "column(s) B are missing from argument(s) 3 and 4, " *
                           "and column(s) F are missing from argument(s) 5 and 6"
    err = @test_throws ArgumentError vcat(df1, df1, df1, df2, df2, df2,
                                          df3, df3, df3, df4, df4, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 " *
                           "and 3, column(s) B are missing from argument(s) 4, 5 " *
                           "and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
    err = @test_throws ArgumentError vcat(df1, df2, df3, df4, df1, df2,
                                          df3, df4, df1, df2, df3, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 5 and 9, " *
                           "column(s) B are missing from argument(s) 2, 6 and 10, " *
                           "and column(s) F are missing from argument(s) 3, 7 and 11"
end

@testset "vcat with view" begin
    x = view(DataFrame(A=Vector{Union{Missing, Int}}(1:3)), 2:2, :)
    y = DataFrame(A=4:5)
    @test vcat(x, y) == DataFrame(A=[2, 4, 5]) ==
          reduce(vcat, [x, y]) == reduce(vcat, (x, y))
end

@testset "reduce vcat corner cases" begin
    df = DataFrame(a=1, b=2)
    df1 = @view df[:, :]
    df2 = @view df[:, 1:2]
    @test typeof(df1) != typeof(df2)

    @test_throws Union{MethodError, ArgumentError} reduce(vcat, ())
    @test reduce(vcat, DataFrame[]) == DataFrame()
    @test reduce(vcat, SubDataFrame[]) == DataFrame()
    @test reduce(vcat, AbstractDataFrame[]) == DataFrame()

    @test reduce(vcat, [df]) == df
    @test reduce(vcat, [df1]) == df
    @test reduce(vcat, [df2]) == df
    @test reduce(vcat, [df, df]) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, [df, df1]) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, [df, df2]) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, [df1, df2]) == DataFrame(a=[1, 1], b=[2, 2])

    @test reduce(vcat, (df,)) == df
    @test reduce(vcat, (df1,)) == df
    @test reduce(vcat, (df2,)) == df
    @test reduce(vcat, (df, df)) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, (df, df1)) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, (df, df2)) == DataFrame(a=[1, 1], b=[2, 2])
    @test reduce(vcat, (df1, df2)) == DataFrame(a=[1, 1], b=[2, 2])
end

end # module
