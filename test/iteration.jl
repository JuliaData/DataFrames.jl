module TestIteration

import Compat
using Test, DataFrames

@testset "eachrow and eachcol" begin
    df = DataFrame(A = Vector{Union{Int, Missing}}(1:2), B = Vector{Union{Int, Missing}}(2:3))

    @test size(eachrow(df)) == (size(df, 1),)
    @test parent(eachrow(df)) === df
    @test names(eachrow(df)) == names(df)
    @test IndexStyle(eachrow(df)) == IndexLinear()
    @test sprint(summary, eachrow(df)) == "2-element DataFrameRows"
    @test Base.IndexStyle(eachrow(df)) == IndexLinear()
    @test eachrow(df)[1] == DataFrameRow(df, 1, :)
    @test collect(eachrow(df)) isa Vector{<:DataFrameRow}
    @test eltype(eachrow(df)) <: DataFrameRow
    for row in eachrow(df)
        @test isa(row, DataFrameRow)
        @test (row[:B] - row[:A]) == 1
        # issue #683 (https://github.com/JuliaData/DataFrames.jl/pull/683)
        @test collect(pairs(row)) isa Vector{Pair{Symbol, Int}}
    end

    @test size(eachcol(df)) == (size(df, 2),)
    @test parent(eachcol(df)) === df
    @test names(eachcol(df)) == names(df)
    @test IndexStyle(eachcol(df)) == IndexLinear()
    @test Base.IndexStyle(eachcol(df)) == IndexLinear()
    @test length(eachcol(df)) == size(df, 2)
    @test eachcol(df)[1] == df[:, 1]
    @test collect(eachcol(df)) isa Vector{AbstractVector}
    @test collect(eachcol(df)) == [[1, 2], [2, 3]]
    @test eltype(eachcol(df)) == AbstractVector
    for col in eachcol(df)
        @test isa(col, AbstractVector)
    end

    @test map(x -> minimum(convert(Vector, x)), eachrow(df)) == [1,2]
    @test map(Vector, eachrow(df)) == [[1, 2], [2, 3]]
    @test mapcols(minimum, df) == DataFrame(A = [1], B = [2])
    @test map(minimum, eachcol(df)) == [1, 2]
    @test eltype.(eachcol(mapcols(Vector{Float64}, df))) == [Float64, Float64]
    @test eltype(map(Vector{Float64}, eachcol(df))) == Vector{Float64}

    @test_throws ErrorException for x in df; end
end

@testset "mapcols" begin
    df_mapcols = DataFrame(a=1:10, b=11:20)
    @test mapcols(sum, df_mapcols) == DataFrame(a=55, b=155)
    @test mapcols(x -> 1, df_mapcols) == DataFrame(a=1, b=1)
    @test_throws ArgumentError mapcols(x -> x[1] == 1 ? 0 : [0], df_mapcols)
    @test_throws DimensionMismatch mapcols(x -> x[1] == 1 ? [1] : [1,2], df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] == 1 ? x : 0, df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] != 1 ? x : 0, df_mapcols)
    df_mapcols2 = mapcols(x -> x, df_mapcols)
    @test df_mapcols2 == df_mapcols
    @test df_mapcols2.a !== df_mapcols.a
    @test df_mapcols2.b !== df_mapcols.b
end

@testset "mapcols!" begin
    df_mapcols = DataFrame(a=1:10, b=11:20)
    mapcols!(sum, df_mapcols)
    @test df_mapcols == DataFrame(a=55, b=155)

    df_mapcols = DataFrame(a=1:10, b=11:20)
    mapcols!(x -> 1, df_mapcols)
    @test df_mapcols == DataFrame(a=1, b=1)

    df_mapcols = DataFrame(a=1:10, b=11:20)
    @test_throws ArgumentError mapcols!(x -> x[1] == 1 ? 0 : [0], df_mapcols)
    @test_throws DimensionMismatch mapcols!(x -> x[1] == 1 ? [1] : [1,2], df_mapcols)
    @test_throws ArgumentError mapcols!(x -> x[1] == 1 ? x : 0, df_mapcols)
    @test_throws ArgumentError mapcols!(x -> x[1] != 1 ? x : 0, df_mapcols)
    @test df_mapcols == DataFrame(a=1:10, b=11:20)

    a = df_mapcols.a
    b = df_mapcols.b
    mapcols!(x -> x, df_mapcols)
    @test a === df_mapcols.a
    @test b === df_mapcols.b
end

@testset "SubDataFrame" begin
    df = DataFrame([11:16 21:26 31:36 41:46])
    sdf = view(df, [3,1,4], [3,1,4])
    @test sdf == df[[3,1,4], [3,1,4]]
    @test eachrow(sdf) == eachrow(df[[3,1,4], [3,1,4]])
    @test size(eachrow(sdf)) == (3,)
    @test eachcol(sdf) == eachcol(df[[3,1,4], [3,1,4]])
    @test size(eachcol(sdf)) == (3,)
end

@testset "parent mutation" begin
    df = DataFrame([11:16 21:26 31:36 41:46])
    sdf = view(df, [3,1,4], [3,1,4])
    erd = eachrow(df)
    erv = eachrow(sdf)
    rename!(df, Symbol.(string.("y", 1:4)))
    df[!, 1] = 51:56
    @test df[1, :] == erd[1]
    @test copy(erv[1]) == (y3=33, y1=53, y4=43)
    df[!, :z] .= 1
    @test length(erd[1]) == 5 # the added column is reflected
    select!(df, Not([4,5]))
    @test copy(erd[1]) == (y1 = 51, y2 = 21, y3 = 31) # the removed columns are reflected
end

@testset "getproperty and propertynames" begin
    df_base = DataFrame([11:16 21:26 31:36 41:46])
    for df in (df_base, view(df_base, 1:3, 1:3))
        for x in (eachcol(df), eachrow(df))
            @test propertynames(x) == propertynames(df)
            for n in names(df)
                @test getproperty(x, n) === getproperty(df, n)
            end
            @test_throws ArgumentError x.a
        end
    end
end

@testset "overload Compat functions" begin
    @testset "DataFrames.$f === Compat.$f" for f in intersect(names(DataFrames), names(Compat))
        @test getproperty(DataFrames, f) === getproperty(Compat, f)
    end
end

@testset "keys and pairs for eachcol" begin
    df = DataFrame([11:16 21:26 31:36 41:46])

    cols = eachcol(df)

    @test keys(cols) == propertynames(df)
    for (a, b, c) in zip(keys(cols), cols, pairs(cols))
        @test (a => b) == c
    end

    for (i, n) in enumerate(keys(cols))
        @test cols[i] === cols[n]
    end
    @test_throws ArgumentError cols[:non_existent]
end

end # module
