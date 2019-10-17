module TestIteration

using Test, DataFrames

df = DataFrame(A = Vector{Union{Int, Missing}}(1:2), B = Vector{Union{Int, Missing}}(2:3))

@test size(eachrow(df)) == (size(df, 1),)
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
@test IndexStyle(eachcol(df)) == IndexLinear()
@test Base.IndexStyle(eachcol(df)) == IndexLinear()
@test size(eachcol(df, true)) == (size(df, 2),)
@test IndexStyle(eachcol(df, true)) == IndexLinear()
@test size(eachcol(df, false)) == (size(df, 2),)
@test IndexStyle(eachcol(df, false)) == IndexLinear()
@test length(eachcol(df)) == size(df, 2)
@test length(eachcol(df, true)) == size(df, 2)
@test length(eachcol(df, false)) == size(df, 2)
@test eachcol(df)[1] == df[:, 1]
@test eachcol(df, true)[1] == (:A => df[:, 1])
@test eachcol(df, false)[1] == df[:, 1]
@test collect(eachcol(df, true)) isa Vector{Pair{Symbol, AbstractVector}}
@test collect(eachcol(df, true)) == [:A => [1, 2], :B => [2, 3]]
@test collect(eachcol(df)) isa Vector{AbstractVector}
@test collect(eachcol(df)) == [[1, 2], [2, 3]]
@test collect(eachcol(df, false)) isa Vector{AbstractVector}
@test collect(eachcol(df, false)) == [[1, 2], [2, 3]]
@test eltype(eachcol(df, true)) == Pair{Symbol, AbstractVector}
@test eltype(eachcol(df, false)) == AbstractVector
@test eltype(eachcol(df)) == AbstractVector
for col in eachcol(df, true)
    @test typeof(col) <: Pair{Symbol, <:AbstractVector}
end
for col in eachcol(df)
    @test isa(col, AbstractVector)
end
for col in eachcol(df, false)
    @test isa(col, AbstractVector)
end

@test map(x -> minimum(convert(Vector, x)), eachrow(df)) == [1,2]
@test map(Vector, eachrow(df)) == [[1, 2], [2, 3]]
@test mapcols(minimum, df) == DataFrame(A = [1], B = [2])
@test map(minimum, eachcol(df, false)) == [1, 2]
@test map(minimum, eachcol(df)) == [1, 2]
@test eltype.(eachcol(mapcols(Vector{Float64}, df))) == [Float64, Float64]
@test eltype(map(Vector{Float64}, eachcol(df, false))) == Vector{Float64}
@test eltype(map(Vector{Float64}, eachcol(df))) == Vector{Float64}

row = DataFrameRow(df, 1, :)

row[:A] = 100
@test df[1, :A] == 100

row[1] = 101
@test df[1, :A] == 101

df = DataFrame(A = Vector{Union{Int, Missing}}(1:4), B = Union{String, Missing}["M", "F", "F", "M"])

s1 = view(df, 1:3, :)
s1[2,:A] = 4
@test df[2, :A] == 4
@test view(s1, 1:2, :) == view(df, 1:2, :)

s2 = view(df, 1:2:3, :)
s2[2, :B] = "M"
@test df[3, :B] == "M"
@test view(s2, 1:1:2, :) == view(df, [1,3], :)

@test_throws MethodError for x in df; end

@testset "mapcols" begin
    df_mapcols = DataFrame(a=1:10, b=11:20)
    @test mapcols(sum, df_mapcols) == DataFrame(a=55, b=155)
    @test mapcols(x -> 1, df_mapcols) == DataFrame(a=1, b=1)
    @test_throws ArgumentError mapcols(x -> x[1] == 1 ? 0 : [0], df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] == 1 ? x : 0, df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] != 1 ? x : 0, df_mapcols)
    df_mapcols2 = mapcols(x -> x, df_mapcols)
    @test df_mapcols2 == df_mapcols
    @test df_mapcols2.a !== df_mapcols.a
    @test df_mapcols2.b !== df_mapcols.b
end

@testset "SubDataFrame" begin
    df = DataFrame([11:16 21:26 31:36 41:46])
    sdf = view(df, [3,1,4], [3,1,4])
    @test sdf == df[[3,1,4], [3,1,4]]
    @test eachrow(sdf) == eachrow(df[[3,1,4], [3,1,4]])
    @test eachcol(sdf, true) == eachcol(df[[3,1,4], [3,1,4]], true)
    @test eachcol(sdf, false) == eachcol(df[[3,1,4], [3,1,4]], false)
    @test size(eachrow(sdf)) == (3,)
    @test size(eachcol(sdf, true)) == (3,)
    @test size(eachcol(sdf, false)) == (3,)
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
        for x in (eachcol(df), eachcol(df, true), eachrow(df))
            @test propertynames(x) == propertynames(df)
            for n in names(df)
                @test getproperty(x, n) === getproperty(df, n)
            end
            @test_throws ArgumentError x.a
        end
    end
end

end # module
