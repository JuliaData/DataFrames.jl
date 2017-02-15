module TestCat
    using Base.Test
    using DataFrames

    #
    # hcat
    #

    nvint = NullableArray(Nullable{Int}[1, 2, Nullable(), 4])
    nvstr = NullableArray(Nullable{String}["one", "two", Nullable(), "four"])

    df2 = DataFrame(Any[nvint, nvstr])
    df3 = DataFrame(Any[nvint])
    df4 = convert(DataFrame, [1:4 1:4])
    df5 = DataFrame(Any[NullableArray([1,2,3,4]), nvstr])

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

    # assignment of sub tables
    df[1, 1:2] = df[2, 2:3]
    df[1:2, 1:2] = df[2:3, 2:3]
    df[[true,false,false,true], 2:3] = df[1:2,1:2]

    # scalar broadcasting assignment of sub tables
    df[1, 1:2] = 3
    df[1:2, 1:2] = 3
    df[[true,false,false,true], 2:3] = 3

    # vector broadcasting assignment of sub tables
    df[1:2, 1:2] = [3,2]
    df[[true,false,false,true], 2:3] = [2,3]

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
    @test isnull(dfr[8,:x2])

    # Eltype promotion
    # Fails on Julia 0.4 since promote_type(Nullable{Int}, Nullable{Float64}) gives Nullable{T}
    if VERSION >= v"0.5.0-dev"
        @test eltypes(vcat(DataFrame(a = [1]), DataFrame(a = [2.1]))) == [Nullable{Float64}]
        @test eltypes(vcat(DataFrame(a = NullableArray(Int, 1)), DataFrame(a = [2.1]))) == [Nullable{Float64}]
    else
        @test eltypes(vcat(DataFrame(a = [1]), DataFrame(a = [2.1]))) == [Nullable{Any}]
        @test eltypes(vcat(DataFrame(a = NullableArray(Int, 1)), DataFrame(a = [2.1]))) == [Nullable{Any}]
    end

    # Minimal container type promotion
    dfa = DataFrame(a = CategoricalArray([1, 2, 2]))
    dfb = DataFrame(a = CategoricalArray([2, 3, 4]))
    dfc = DataFrame(a = NullableArray([2, 3, 4]))
    dfd = DataFrame(Any[2:4], [:a])
    dfab = vcat(dfa, dfb)
    dfac = vcat(dfa, dfc)
    @test isequal(dfab[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isequal(dfac[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isa(dfab[:a], NullableCategoricalVector{Int})
    # Fails on Julia 0.4 since promote_type(Nullable{Int}, Nullable{Float64}) gives Nullable{T}
    if VERSION >= v"0.5.0-dev"
        @test isa(dfac[:a], NullableCategoricalVector{Int})
    else
        @test isa(dfac[:a], NullableCategoricalVector{Any})
    end
    # ^^ container may flip if container promotion happens in Base/DataArrays
    dc = vcat(dfd, dfc)
    @test isequal(vcat(dfc, dfd), dc)

    # Zero-row DataFrames
    dfc0 = similar(dfc, 0)
    @test isequal(vcat(dfd, dfc0, dfc), dc)
    @test eltypes(vcat(dfd, dfc0)) == eltypes(dc)

    # Missing columns
    rename!(dfd, :a, :b)
    dfda = DataFrame(b = NullableArray(Nullable{Int}[2, 3, 4, Nullable(), Nullable(), Nullable()]),
                     a = NullableCategoricalVector(Nullable{Int}[Nullable(), Nullable(), Nullable(), 1, 2, 2]))
    @test isequal(vcat(dfd, dfa), dfda)

    # Alignment
    @test isequal(vcat(dfda, dfd, dfa), vcat(dfda, dfda))

    # vcat should be able to concatenate different implementations of AbstractDataFrame (PR #944)
    @test isequal(vcat(view(DataFrame(A=1:3),2),DataFrame(A=4:5)), DataFrame(A=[2,4,5]))
end
