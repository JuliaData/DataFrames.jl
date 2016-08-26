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
    @test isequal(dfh, DataFrames.hcat!(DataFrame(), df3, df4))

    dfh3 = hcat(df3, df4, df5)
    @test names(dfh3) == [:x1, :x1_1, :x2, :x1_2, :x2_1]
    @test isequal(dfh3, hcat(dfh, df5))
    @test isequal(dfh3, DataFrames.hcat!(DataFrame(), df3, df4, df5))

    @test isequal(df2, DataFrames.hcat!(df2))

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

    vcat([])
    vcat(null_df)
    vcat(null_df, null_df)
    vcat(null_df, df)
    vcat(df, null_df)
    vcat(df, df)
    vcat(df, df, df)
    @test vcat(DataFrame[]) == DataFrame()

    alt_df = deepcopy(df)
    vcat(df, alt_df)

    # Don't fail on non-matching types
    df[1] = zeros(Int, nrow(df))
    vcat(df, alt_df)

    # Don't fail on non-matching names
    names!(alt_df, [:A, :B, :C])
    vcat(df, alt_df)

    dfr = vcat(df4, df4)
    @test size(dfr, 1) == 8
    @test names(df4) == names(dfr)
    @test isequal(dfr, [df4; df4])

    dfr = vcat(df2, df3)
    @test size(dfr) == (8,2)
    @test names(df2) == names(dfr)
    @test isna(dfr[8,:x2])

    # Eltype promotion
    @test eltypes(vcat(DataFrame(a = [1]), DataFrame(a = [2.1]))) == [Float64]
    @test eltypes(vcat(DataFrame(a = [NA]), DataFrame(a = [2.1]))) == [Float64]

    # Minimal container type promotion
    dfa = DataFrame(a = @pdata([1, 2, 2]))
    dfb = DataFrame(a = @pdata([2, 3, 4]))
    dfc = DataFrame(a = @data([2, 3, 4]))
    dfd = DataFrame(Any[2:4], [:a])
    @test vcat(dfa, dfb)[:a] == @pdata([1, 2, 2, 2, 3, 4])
    @test vcat(dfa, dfc)[:a] == @pdata([1, 2, 2, 2, 3, 4])
    # ^^ container may flip if container promotion happens in Base/DataArrays
    dc = vcat(dfd, dfc)
    @test vcat(dfc, dfd) == dc

    # Zero-row DataFrames
    dfc0 = similar(dfc, 0)
    @test vcat(dfd, dfc0, dfc) == dc
    @test eltypes(vcat(dfd, dfc0)) == eltypes(dc)

    # Missing columns
    rename!(dfd, :a, :b)
    dfda = DataFrame(b = @data([2, 3, 4, NA, NA, NA]),
                     a = @pdata([NA, NA, NA, 1, 2, 2]))
    @test isequal(vcat(dfd, dfa), dfda)

    # Alignment
    @test isequal(vcat(dfda, dfd, dfa), vcat(dfda, dfda))

    # vcat should be able to concatenate different implementations of AbstractDataFrame (PR #944)
    @test vcat(sub(DataFrame(A=1:3),2),DataFrame(A=4:5)) == DataFrame(A=[2,4,5])
end
