module TestCat

using Test, Random, DataFrames
const ≅ = isequal

#
# hcat
#
@testset "hcat" begin
    nvint = [1, 2, missing, 4]
    nvstr = ["one", "two", missing, "four"]

    df2 = DataFrame([nvint, nvstr])
    df3 = DataFrame([nvint])
    df4 = convert(DataFrame, [1:4 1:4])
    df5 = DataFrame([Union{Int, Missing}[1,2,3,4], nvstr])

    ref_df = copy(df3)
    dfh = hcat(df3, df4, makeunique=true)
    @test ref_df ≅ df3 # make sure that df3 is not mutated by hcat
    @test size(dfh, 2) == 3
    @test names(dfh) ≅ [:x1, :x1_1, :x2]
    @test dfh[:x1] ≅ df3[:x1]
    @test dfh ≅ DataFrames.hcat!(DataFrame(), df3, df4, makeunique=true)

    dfa = DataFrame(a=[1,2])
    dfb = DataFrame(b=[3,missing])
    @test hcat(dfa, dfb) ≅ [dfa dfb]

    dfh3 = hcat(df3, df4, df5, makeunique=true)
    @test names(dfh3) == [:x1, :x1_1, :x2, :x1_2, :x2_1]
    @test dfh3 ≅ hcat(dfh, df5, makeunique=true)
    @test dfh3 ≅ DataFrames.hcat!(DataFrame(), df3, df4, df5, makeunique=true)

    @test df2 ≅ DataFrames.hcat!(df2, makeunique=true)
end

@testset "hcat ::AbstractDataFrame" begin
    df = DataFrame(A = repeat('A':'C', inner=4), B = 1:12)
    gd = groupby(df, :A)
    answer = DataFrame(A = fill('A', 4), B = 1:4, A_1 = 'B', B_1 = 5:8, A_2 = 'C', B_2 = 9:12)
    @test hcat(gd..., makeunique=true) == answer
    answer = answer[1:4]
    @test hcat(gd[1], gd[2], makeunique=true) == answer
end

@testset "hcat ::AbstractDataFrame" begin
    df = DataFrame(A = repeat('A':'C', inner=4), B = 1:12)
    gd = groupby(df, :A)
    answer = DataFrame(A = fill('A', 4), B = 1:4, A_1 = 'B', B_1 = 5:8, A_2 = 'C', B_2 = 9:12)
    @test hcat(gd..., makeunique=true) == answer
    answer = answer[1:4]
    @test hcat(gd[1], gd[2], makeunique=true) == answer
end

@testset "hcat ::AbstractVectors" begin
    df = DataFrame()
    DataFrames.hcat!(df, CategoricalVector{Union{Int,Missing}}(1:10), makeunique=true)
    @test df[1] == CategoricalVector(1:10)
    DataFrames.hcat!(df, 1:10, makeunique=true)
    @test df[2] == collect(1:10)
    DataFrames.hcat!(df, collect(1:10), makeunique=true)
    @test df[3] == collect(1:10)

    df = DataFrame()
    df2 = hcat(CategoricalVector{Union{Int,Missing}}(1:10), df, makeunique=true)
    @test isempty(df)
    @test df2[1] == collect(1:10)
    @test names(df2) == [:x1]
    ref_df = copy(df2)
    df3 = hcat(11:20, df2, makeunique=true)
    @test df2 == ref_df
    @test df3[1] == collect(11:20)
    @test names(df3) == [:x1, :x1_1]

    @test_throws ArgumentError hcat("a", df, makeunique=true)
    @test_throws ArgumentError hcat(df, "a", makeunique=true)
end
#
# vcat
#

@testset "vcat" begin
    missing_df = DataFrame(Int, 0, 0)
    df = DataFrame(Int, 4, 3)

    # Assignment of rows
    # TODO: re-enable when we fix setindex! to handle DataFrameRow on RHS
    # df[1, :] = df[1, :]
    df[1, :] = df[1:1, :]
    df[1:2, :] = df[1:2, :]
    df[[true,false,false,true], :] = df[2:3, :]

    # Scalar broadcasting assignment of rows
    df[1, :] = 1
    df[1:2, :] = 1
    df[[true,false,false,true], :] = 3

    # Vector broadcasting assignment of rows
    df[1:2, :] = [2,3]
    df[[true,false,false,true], :] = [2,3]

    # Assignment of columns
    df[1] = zeros(4)
    df[:, 2] = ones(4)

    # Broadcasting assignment of columns
    df[:, 1] = 1
    df[1] = 3
    df[:x3] = 2

    # assignment of subtables
    # TODO: re-enable when we fix setindex! to handle DataFrameRow on RHS
    # df[1, 1:2] = df[2, 2:3]
    df[1, 1:2] = df[2:2, 2:3]
    df[1:2, 1:2] = df[2:3, 2:3]
    df[[true,false,false,true], 2:3] = df[1:2,1:2]

    # scalar broadcasting assignment of subtables
    df[1, 1:2] = 3
    df[1:2, 1:2] = 3
    df[[true,false,false,true], 2:3] = 3

    # vector broadcasting assignment of subtables
    df[1:2, 1:2] = [3,2]
    df[[true,false,false,true], 2:3] = [2,3]

    @test vcat(missing_df) == DataFrame()
    @test vcat(missing_df, missing_df) == DataFrame()
    @test_throws ArgumentError vcat(missing_df, df)
    @test_throws ArgumentError vcat(df, missing_df)
    @test eltypes(vcat(df, df)) == Type[Float64, Float64, Int]
    @test size(vcat(df, df)) == (size(df, 1) * 2, size(df, 2))
    res = vcat(df, df)
    @test res[1:size(df, 1), :] == df
    @test res[1+size(df, 1):end, :] == df
    @test eltypes(vcat(df, df, df)) == Type[Float64, Float64, Int]
    @test size(vcat(df, df, df)) == (size(df, 1) * 3, size(df, 2))
    res = vcat(df, df, df)
    s = size(df, 1)
    for i in 1:3
        @test res[1+(i-1)*s:i*s, :] == df
    end

    alt_df = deepcopy(df)
    @test vcat(df, alt_df) == DataFrame([[3.0,2.0,3.0,3.0,3.0,2.0,3.0,3.0],
                                         [2.0,2.0,1.0,3.0,2.0,2.0,1.0,3.0],
                                         [2,2,2,3,2,2,2,3]])

    # Don't fail on non-matching types
    df[1] = zeros(Int, nrow(df))
    @test vcat(df, alt_df) == DataFrame([[0.0,0.0,0.0,0.0,3.0,2.0,3.0,3.0],
                                         [2.0,2.0,1.0,3.0,2.0,2.0,1.0,3.0],
                                         [2,2,2,3,2,2,2,3]])
end

@testset "vcat >2 args" begin
    empty_dfs = [DataFrame(), DataFrame(), DataFrame()]
    @test vcat(empty_dfs...) == reduce(vcat, empty_dfs) == DataFrame()

    df = DataFrame(x = trues(1), y = falses(1))
    dfs = [df, df, df]
    @test vcat(dfs...) ==reduce(vcat, dfs) == DataFrame(x = trues(3), y = falses(3))
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
    @test df[:x] isa Vector{Int}
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([Union{Missing, Int}[1]], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test df[:x] isa Vector{Union{Int, Missing}}
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([CategoricalArray{Union{Int, Missing}}([1])], [:x]))
    @test df == DataFrame([[1, 1]], [:x])
    @test df[:x] isa CategoricalVector{Union{Int, Missing}}
    df = vcat(DataFrame([Union{Int, Missing}[1]], [:x]),
              DataFrame([["1"]], [:x]))
    @test df == DataFrame([[1, "1"]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Any}]
    df = vcat(DataFrame([CategoricalArray([1])], [:x]),
              DataFrame([CategoricalArray(["1"])], [:x]))
    @test df == DataFrame([[1, "1"]], [:x])
    @test df[:x] isa CategoricalVector{Any}
    df = vcat(DataFrame([trues(1)], [:x]), DataFrame([[false]], [:x]))
    @test df == DataFrame([[true, false]], [:x])
    @test typeof.(eachcol(df)) == [Vector{Bool}]
end

@testset "vcat out of order" begin
    df1 = DataFrame(A = 1:3, B = 4:6, C = 7:9)
    df2 = DataFrame(colwise(x->2x, df1), reverse(names(df1)))
    @test vcat(df1, df2) == DataFrame([[1, 2, 3, 14, 16, 18],
                                       [4, 5, 6, 8, 10, 12],
                                       [7, 8, 9, 2, 4, 6]], [:A, :B, :C])
    @test vcat(df1, df1, df2) == DataFrame([[1, 2, 3, 1, 2, 3, 14, 16, 18],
                                            [4, 5, 6, 4, 5, 6, 8, 10, 12],
                                            [7, 8, 9, 7, 8, 9, 2, 4, 6]], [:A, :B, :C])
    @test vcat(df1, df2, df2) == DataFrame([[1, 2, 3, 14, 16, 18, 14, 16, 18],
                                            [4, 5, 6, 8, 10, 12, 8, 10, 12],
                                            [7, 8, 9, 2, 4, 6, 2, 4, 6]], [:A, :B, :C])
    @test vcat(df2, df1, df2) == DataFrame([[2, 4, 6, 7, 8, 9, 2, 4, 6],
                                            [8, 10, 12, 4, 5, 6, 8, 10, 12],
                                            [14, 16, 18, 1, 2, 3, 14, 16, 18]] ,[:C, :B, :A])

    @test size(vcat(df1, df1, df1, df2, df2, df2)) == (18, 3)
    df3 = df1[[1, 3, 2]]
    res = vcat(df1, df1, df1, df2, df2, df2, df3, df3, df3, df3)
    @test res == reduce(vcat, [df1, df1, df1, df2, df2, df2, df3, df3, df3, df3])

    @test size(res) == (30, 3)
    @test res[1:3,:] == df1
    @test res[4:6,:] == df1
    @test res[7:9,:] == df1
    @test res[10:12,:] == df2[names(res)]
    @test res[13:15,:] == df2[names(res)]
    @test res[16:18,:] == df2[names(res)]
    @test res[19:21,:] == df3[names(res)]
    @test res[22:24,:] == df3[names(res)]
    @test res[25:27,:] == df3[names(res)]
    df1 = DataFrame(A = 1, B = 2)
    df2 = DataFrame(B = 12, A = 11)
    df3 = DataFrame(A = [1, 11], B = [2, 12])
    @test [df1; df2] == df3 == reduce(vcat, [df1, df2])
end

@testset "vcat errors" begin
    err = @test_throws ArgumentError vcat(DataFrame(), DataFrame(), DataFrame(x=[]))
    @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
    err = @test_throws ArgumentError vcat(DataFrame(), DataFrame(), DataFrame(x=[1]))
    @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
    df1 = DataFrame(A = 1:3, B = 1:3)
    df2 = DataFrame(A = 1:3)
    # right missing 1 column
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) B are missing from argument(s) 2"
    # left missing 1 column
    err = @test_throws ArgumentError vcat(df2, df1)
    @test err.value.msg == "column(s) B are missing from argument(s) 1"
    # multiple missing 1 column
    err = @test_throws ArgumentError vcat(df1, df2, df2, df2, df2, df2)
    err2 = @test_throws ArgumentError reduce(vcat, [df1, df2, df2, df2, df2, df2])
    @test err == err2
    @test err.value.msg == "column(s) B are missing from argument(s) 2, 3, 4, 5 and 6"
    # argument missing >1 columns
    df1 = DataFrame(A = 1:3, B = 1:3, C = 1:3, D = 1:3, E = 1:3)
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2"
    # >1 arguments missing >1 columns
    err = @test_throws ArgumentError vcat(df1, df2, df2, df2, df2)
    @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2, 3, 4 and 5"
    # missing columns throws error
    df1 = DataFrame(A = 1, B = 1)
    df2 = DataFrame(A = 1)
    df3 = DataFrame(B = 1, A = 1)
    err = @test_throws ArgumentError vcat(df1, df2, df3)
    @test err.value.msg == "column(s) B are missing from argument(s) 2"
    # unique columns for both sides
    df1 = DataFrame(A = 1, B = 1, C = 1, D = 1)
    df2 = DataFrame(A = 1, C = 1, D = 1, E = 1, F = 1)
    err = @test_throws ArgumentError vcat(df1, df2)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, and column(s) B are missing from argument(s) 2"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, and column(s) B are missing from argument(s) 3 and 4"
    df3 = DataFrame(A = 1, B = 1, C = 1, D = 1, E = 1)
    err = @test_throws ArgumentError vcat(df1, df2, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2, df3, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
    err = @test_throws ArgumentError vcat(df1, df1, df1, df2, df2, df2, df3, df3, df3)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
    # df4 is a superset of names found in all other DataFrames and won't be shown in error
    df4 = DataFrame(A = 1, B = 1, C = 1, D = 1, E = 1, F = 1)
    err = @test_throws ArgumentError vcat(df1, df2, df3, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
    err = @test_throws ArgumentError vcat(df1, df1, df2, df2, df3, df3, df4, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
    err = @test_throws ArgumentError vcat(df1, df1, df1, df2, df2, df2, df3, df3, df3, df4, df4, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
    err = @test_throws ArgumentError vcat(df1, df2, df3, df4, df1, df2, df3, df4, df1, df2, df3, df4)
    @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 5 and 9, column(s) B are missing from argument(s) 2, 6 and 10, and column(s) F are missing from argument(s) 3, 7 and 11"
end
x = view(DataFrame(A = Vector{Union{Missing, Int}}(1:3)), 2:2, :)
y = DataFrame(A = 4:5)
@test vcat(x, y) == DataFrame(A = [2, 4, 5]) == reduce(vcat, [x, y])

end # module
