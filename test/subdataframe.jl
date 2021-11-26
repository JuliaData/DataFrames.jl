module TestSubDataFrame

using Test, DataFrames

@testset "copy - SubDataFrame" begin
    df = DataFrame(x=1:10, y=1.0:10.0)
    sdf = view(df, 1:2, 1:1)
    @test sdf isa SubDataFrame
    @test copy(sdf) isa DataFrame
    @test sdf == copy(sdf)
    @test view(sdf, :, :) === sdf
    @test view(sdf, :, r"") == sdf
end

@testset "view -- DataFrame" begin
    df = DataFrame(x=1:10, y=1.0:10.0)
    @test view(df, 1, :) == DataFrameRow(df, 1, :)
    @test view(df, UInt(1), :) == DataFrameRow(df, 1, :)
    @test view(df, BigInt(1), :) == DataFrameRow(df, 1, :)
    @test view(df, UInt(1):UInt(1), :) == SubDataFrame(df, 1:1, :)
    @test view(df, BigInt(1):BigInt(1), :) == SubDataFrame(df, 1:1, :)
    @test view(df, 1:2, :) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), :) == first(df, 2)
    @test view(df, [1, 2], :) == first(df, 2)

    @test view(df, 1, r"") == DataFrameRow(df, 1, :)
    @test view(df, UInt(1), r"") == DataFrameRow(df, 1, :)
    @test view(df, BigInt(1), r"") == DataFrameRow(df, 1, :)
    @test view(df, UInt(1):UInt(1), r"") == SubDataFrame(df, 1:1, :)
    @test view(df, BigInt(1):BigInt(1), r"") == SubDataFrame(df, 1:1, :)
    @test view(df, 1:2, r"") == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), r"") == first(df, 2)
    @test view(df, [1, 2], r"") == first(df, 2)

    @test view(df, 1, r"") == DataFrameRow(df, 1, r"")
    @test view(df, UInt(1), r"") == DataFrameRow(df, 1, r"")
    @test view(df, BigInt(1), r"") == DataFrameRow(df, 1, r"")
    @test view(df, UInt(1):UInt(1), r"") == SubDataFrame(df, 1:1, r"")
    @test view(df, BigInt(1):BigInt(1), r"") == SubDataFrame(df, 1:1, r"")

    @test view(df, 1, :x) == view(df[!, :x], 1)
    @test view(df, 1, :x) isa SubArray
    @test size(view(df, 1, :x)) == ()
    @test view(df, 1:2, :x) == df[!, :x][1:2]
    @test view(df, 1:2, :x) isa SubArray
    @test view(df, vcat(trues(2), falses(8)), :x) == view(df[!, :x], vcat(trues(2), falses(8)))
    @test view(df, [1, 2], :x) == view(df[!, :x], [1, 2])
    @test view(df, 1, 1) == view(df[!, 1], 1)
    @test view(df, 1, 1) isa SubArray
    @test size(view(df, 1, 1)) == ()
    @test view(df, 1:2, 1) == df[!, 1][1:2]
    @test view(df, 1:2, 1) isa SubArray
    @test view(df, vcat(trues(2), falses(8)), 1) == view(df[!, 1], vcat(trues(2), falses(8)))
    @test view(df, [1, 2], 1) == view(df[!, 1], [1, 2])
    @test view(df, 1:2, 1) == df[!, 1][1:2]
    @test view(df, 1:2, 1) isa SubArray

    @test view(df, 1, [:x, :y]) == DataFrameRow(df[:, [:x, :y]], 1, :)
    @test view(df, 1, [:x, :y]) == DataFrameRow(df, 1, [:x, :y])
    @test view(df, 1:2, [:x, :y]) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), [:x, :y]) == first(df, 2)
    @test view(df, [1, 2], [:x, :y]) == first(df, 2)

    @test view(df, 1, r"[xy]") == DataFrameRow(df[:, [:x, :y]], 1, :)
    @test view(df, 1, r"[xy]") == DataFrameRow(df, 1, [:x, :y])
    @test view(df, 1:2, r"[xy]") == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), r"[xy]") == first(df, 2)
    @test view(df, [1, 2], r"[xy]") == first(df, 2)

    @test view(df, 1, [1, 2]) == DataFrameRow(df[:, 1:2], 1, :)
    @test view(df, 1, [1, 2]) == DataFrameRow(df, 1, 1:2)
    @test view(df, 1:2, [1, 2]) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), [1, 2]) == first(df, 2)
    @test view(df, [1, 2], [1, 2]) == first(df, 2)

    @test view(df, 1, trues(2)) == DataFrameRow(df[:, trues(2)], 1, :)
    @test view(df, 1, trues(2)) == DataFrameRow(df, 1, trues(2))
    @test view(df, 1:2, trues(2)) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), trues(2)) == first(df, 2)
    @test view(df, [1, 2], trues(2)) == first(df, 2)

    @test view(df, Integer[1, 2], :) == first(df, 2)
    @test view(df, UInt[1, 2], :) == first(df, 2)
    @test view(df, BigInt[1, 2], :) == first(df, 2)
    @test view(df, Union{Int, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{Integer, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{UInt, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{BigInt, Missing}[1, 2], :) == first(df, 2)

    @test view(df, :, :) == df
    @test view(df, 1, :) == DataFrameRow(df, 1, :)
    @test view(df, :, 1) == df[:, 1]
    @test view(df, :, 1) isa SubArray

    @test view(df, 1, r"") == DataFrameRow(df, 1, :)
    @test view(df, :, 1) == df[:, 1]
    @test view(df, :, 1) isa SubArray

    @test size(view(df, :, r"a")) == (0, 0)
    @test size(view(df, 1:2, r"a")) == (0, 0)
    @test size(view(df, 2, r"a")) == (0,)

    @test_throws ArgumentError view(df, :, [missing, 1])
    @test_throws ArgumentError view(df, [missing, 1], :)
    @test_throws ArgumentError view(df, [missing, 1], r"")
end

@testset "view -- SubDataFrame" begin
    df = view(DataFrame(x=1:10, y=1.0:10.0), 1:10, :)

    @test view(df, 1, :) == DataFrameRow(df, 1, :)
    @test view(df, UInt(1), :) == DataFrameRow(df, 1, :)
    @test view(df, BigInt(1), :) == DataFrameRow(df, 1, :)
    @test view(df, 1:2, :) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), :) == first(df, 2)
    @test view(df, [1, 2], :) == first(df, 2)

    @test view(df, 1, r"") == DataFrameRow(df, 1, :)
    @test view(df, UInt(1), r"") == DataFrameRow(df, 1, :)
    @test view(df, BigInt(1), r"") == DataFrameRow(df, 1, :)
    @test view(df, 1:2, r"") == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), r"") == first(df, 2)
    @test view(df, [1, 2], r"") == first(df, 2)

    @test view(df, 1, r"") == DataFrameRow(df, 1, r"")
    @test view(df, UInt(1), r"") == DataFrameRow(df, 1, r"")
    @test view(df, BigInt(1), r"") == DataFrameRow(df, 1, r"")

    @test view(df, 1, :x) == view(df[!, :x], 1)
    @test view(df, 1, 1) isa SubArray
    @test size(view(df, 1, 1)) == ()
    @test view(df, 1:2, :x) == view(df[!, :x], 1:2)
    @test view(df, vcat(trues(2), falses(8)), :x) == view(df[!, :x], vcat(trues(2), falses(8)))
    @test view(df, [1, 2], :x) == view(df[!, :x], [1, 2])

    @test view(df, 1, 1) == view(df[!, 1], 1)
    @test view(df, 1, 1) isa SubArray
    @test size(view(df, 1, 1)) == ()
    @test view(df, 1:2, 1) == view(df[!, :x], 1:2)
    @test view(df, vcat(trues(2), falses(8)), 1) == view(df[!, :x], vcat(trues(2), falses(8)))
    @test view(df, [1, 2], 1) == view(df[!, :x], [1, 2])

    @test view(df, 1, [:x, :y]) == DataFrameRow(df[:, [:x, :y]], 1, :)
    @test view(df, 1, [:x, :y]) == DataFrameRow(df, 1, [:x, :y])
    @test view(df, 1:2, [:x, :y]) == first(df, 2)

    @test view(df, 1, r"[xy]") == DataFrameRow(df[:, [:x, :y]], 1, :)
    @test view(df, 1, r"[xy]") == DataFrameRow(df, 1, [:x, :y])
    @test view(df, 1:2, r"[xy]") == first(df, 2)

    @test view(df, vcat(trues(2), falses(8)), [:x, :y]) == first(df, 2)
    @test view(df, [1, 2], [:x, :y]) == first(df, 2)
    @test view(df, 1, [1, 2]) == DataFrameRow(df[:, 1:2], 1, :)
    @test view(df, 1, [1, 2]) == DataFrameRow(df, 1, 1:2)
    @test view(df, 1:2, [1, 2]) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), [1, 2]) == first(df, 2)
    @test view(df, [1, 2], [1, 2]) == first(df, 2)
    @test view(df, 1, trues(2)) == DataFrameRow(df[:, trues(2)], 1, :)
    @test view(df, 1, trues(2)) == DataFrameRow(df, 1, trues(2))
    @test view(df, 1:2, trues(2)) == first(df, 2)
    @test view(df, vcat(trues(2), falses(8)), trues(2)) == first(df, 2)
    @test view(df, [1, 2], trues(2)) == first(df, 2)
    @test view(df, Integer[1, 2], :) == first(df, 2)
    @test view(df, UInt[1, 2], :) == first(df, 2)
    @test view(df, BigInt[1, 2], :) == first(df, 2)
    @test view(df, Union{Int, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{Integer, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{UInt, Missing}[1, 2], :) == first(df, 2)
    @test view(df, Union{BigInt, Missing}[1, 2], :) == first(df, 2)

    @test view(df, :, :) == df
    @test view(df, 1, :) == DataFrameRow(df, 1, :)
    @test view(df, :, 1) == df[:, 1]
    @test view(df, :, 1) isa SubArray

    @test view(df, :, r"") == df
    @test view(df, 1, r"") == DataFrameRow(df, 1, :)

    @test_throws ArgumentError view(df, :, [missing, 1])
    @test_throws ArgumentError view(df, [missing, 1], :)
    @test_throws ArgumentError view(df, [missing, 1], r"")
    @test_throws ArgumentError view(df, :, true)
end

@testset "getproperty, setproperty! and propertynames" begin
    x = collect(1:10)
    y = collect(1.0:10.0)
    df = view(DataFrame(:x=>x, :y=>y, copycols=false), 2:6, :)

    @test propertynames(df) == Symbol.(names(df))

    @test df.x == 2:6
    @test df.y == 2:6
    @test_throws ArgumentError df.z

    df[:, :x] = 1:5
    @test df.x == 1:5
    @test x == [1; 1:5; 7:10]
    df[:, :y] .= 1
    @test df.y == [1, 1, 1, 1, 1]
    @test y == [1; 1; 1; 1; 1; 1; 7:10]
end

@testset "index" begin
    y = 1.0:10.0
    df = view(DataFrame(y=y), 2:6, :)
    df2 = view(DataFrame(x=y, y=y), 2:6, 2:2)
    @test DataFrames.index(df) == DataFrames.index(df2)
    @test haskey(DataFrames.index(df2), :y)
    @test !haskey(DataFrames.index(df2), :x)
    @test haskey(DataFrames.index(df2), 1)
    @test !haskey(DataFrames.index(df2), 2)
    @test !haskey(DataFrames.index(df2), 0)
    @test_throws ArgumentError haskey(DataFrames.index(df2), true)
    @test names(DataFrames.index(df2)) == ["y"]
    @test DataFrames._names(DataFrames.index(df2)) == [:y]

    x = DataFrame(ones(5, 4), :auto)
    df = view(x, 2:3, 2:3)
    @test names(df) == names(x)[2:3]
    df = view(x, 2:3, [4, 2])
    @test names(df) == names(x)[[4, 2]]
end

@testset "deleteat!" begin
    y = 1.0:10.0
    df = view(DataFrame(y=y), 2:6, :)
    @test_throws ArgumentError deleteat!(df, 1)
end

@testset "parent" begin
    df = DataFrame(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing])
    @test parent(view(df, [4, 2], :)) === df
    @test parent(view(df, [4, 2], r"")) === df
    @test parentindices(view(df, [4, 2], :)) == ([4, 2], Base.OneTo(3))
    @test parentindices(view(df, [4, 2], r"")) == ([4, 2], [1, 2, 3])
    @test parent(view(df, [4, 2], 1:3)) === df
    @test parentindices(view(df, [4, 2], 1:3)) == ([4, 2], Base.OneTo(3))
end

@testset "duplicate column" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)
    @test_throws ArgumentError view(df, [3, 1, 4], [3, 3, 3])
end

@testset "conversion to DataFrame" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)
    sdf = view(df, [3, 1, 4], [3, 2, 1])
    df2 = DataFrame(sdf)
    @test df2 isa DataFrame
    @test df2 == df[[3, 1, 4], [3, 2, 1]]
    @test all(x -> x isa Vector{Int}, eachcol(df2))
    df2 = convert(DataFrame, sdf)
    @test df2 isa DataFrame
    @test df2 == df[[3, 1, 4], [3, 2, 1]]
    @test all(x -> x isa Vector{Int}, eachcol(df2))

    df = DataFrame(x=1:4, y=11:14, z=21:24)
    sdf = @view df[2:3, [2]]
    df2 = DataFrame(sdf)
    @test size(df2) == (2, 1)
    @test df2.y isa Vector{Int}
    @test df2.y == [12, 13]
    df2 = DataFrame(sdf, copycols=false)
    @test size(df2) == (2, 1)
    @test df2.y isa SubArray{Int, 1, Vector{Int}, Tuple{UnitRange{Int}}, true}
    @test df2.y == [12, 13]
end

@testset "setindex! in view" begin
    df = DataFrame(A=Vector{Union{Int, Missing}}(1:4), B=Union{String, Missing}["M", "F", "F", "M"])

    s1 = view(df, 1:3, :)
    s1[2, :A] = 4
    @test df[2, :A] == 4
    @test view(s1, 1:2, :) == view(df, 1:2, :)

    s2 = view(df, 1:2:3, :)
    s2[2, :B] = "M"
    @test df[3, :B] == "M"
    @test view(s2, 1:1:2, :) == view(df, [1, 3], :)
end

@testset "SubDataFrame corner cases" begin
    df = DataFrame(a=[1, 2], b=[3, 4])
    @test_throws ArgumentError SubDataFrame(df, Integer[true], 1)
    @test_throws ArgumentError SubDataFrame(df, [true], 1)

    sdf = @view df[:, :]
    @test_throws ArgumentError SubDataFrame(sdf, true, 1)
    @test_throws ArgumentError SubDataFrame(sdf, true, :)
    @test_throws ArgumentError SubDataFrame(sdf, Integer[true], 1)

    if VERSION >= v"1.7.0-DEV"
        @test_throws ArgumentError SubDataFrame(sdf, Integer[true, true, true], :)
    else
        @test SubDataFrame(sdf, Integer[true, true, true], :) ==
              SubDataFrame(sdf, [1, 1, 1], :)
    end
end

end # module
