module TestCat
    using Base.Test
    using DataFrames

    #
    # hcat
    #

    dvint = @data([1, 2, NA, 4])
    dvstr = @data(["one", "two", NA, "four"])

    df2 = DataFrame(Any[dvint, dvstr])
    df3 = DataFrame(Any[dvint])
    df4 = convert(DataFrame, [1:4 1:4])
    df5 = DataFrame(Any[@data([1,2,3,4]), dvstr])

    dfh = hcat(df3, df4)
    @test size(dfh, 2) == 3
    @test names(dfh) == [:x1, :x1_1, :x2]
    @test isequal(dfh[:x1], df3[:x1])
    @test isequal(dfh, [df3 df4])

    dfh3 = hcat(df3, df4, df5)
    @test names(dfh3) == [:x1, :x1_1, :x2, :x1_2, :x2_1]
    @test isequal(dfh3, hcat(dfh, df5))

    #
    # vcat
    #

    null_df = DataFrame(Int, 0, 0)
    df = DataFrame(Int, 4, 3)

    # Assignment of rows
    df[1, :] = df[1, :]
    df[1:2, :] = df[1:2, :]

    # Broadcasting assignment of rows
    df[1, :] = 1

    # Assignment of columns
    df[1] = zeros(4)

    # Broadcasting assignment of columns
    df[:, 1] = 1
    df[1] = 3
    df[:x3] = 2

    vcat(null_df)
    vcat(null_df, null_df)
    vcat(null_df, df)
    vcat(df, null_df)
    vcat(df, df)
    vcat(df, df, df)

    alt_df = deepcopy(df)
    vcat(df, alt_df)
    df[1] = zeros(Int, nrow(df))
    # Fail on non-matching types
    vcat(df, alt_df)

    alt_df = deepcopy(df)
    names!(alt_df, [:A, :B, :C])
    # Fail on non-matching names
    vcat(df, alt_df)

    dfr = vcat(df4, df4)
    @test size(dfr, 1) == 8
    @test names(df4) == names(dfr)
    @test isequal(dfr, [df4, df4])

    dfr = vcat(df2, df3)
    @test size(dfr) == (8,2)
    @test names(df2) == names(dfr)
    @test isna(dfr[8,:x2])

    @test eltypes(vcat(DataFrame(a = [1]), DataFrame(a = [2.1]))) == [Float64]

    dfa = DataFrame(a = @pdata([1, 2, 2]))
    dfb = DataFrame(a = @pdata([2, 3, 4]))
    # dfc = DataFrame(a = @data([2, 3, 4]))
    # dfd = DataFrame(Any[2:4], [:a])
    @test vcat(dfa, dfb)[:a] == @pdata([1, 2, 2, 2, 3, 4])
    # @test vcat(dfa, dfc)[:a] == @data([1, 2, 2, 2, 3, 4])
    # @test vcat(dfc, dfd) == vcat(dfd, dfc)
    # ^^ if/when container promotion happens in Base/DataArrays

end
