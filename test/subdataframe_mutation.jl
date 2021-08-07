module TestIndexing

using Test, DataFrames, CategoricalArrays

const ≅ = isequal

@testset "mutating SubDataFrame with assignment to [!, col]" begin
    df = DataFrame()
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(a=[])

    df = DataFrame()
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = Int[]
    @test isempty(df)

    df = DataFrame()
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(a=[])

    df = DataFrame()
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = Int[]
    @test isempty(df)

    df = DataFrame(x=Int[])
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(x=Int[], a=[])
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = Int[]
    @test df == DataFrame(x=Int[])
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(x=Int[], a=[])
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = Int[]
    @test df == DataFrame(x=Int[])
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = Int[]
    @test df ≅ DataFrame(x=1:5)
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=1:5)

    df = DataFrame(x=1:5)
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[!, :a] = [1]
    sdf[!, :a] = 11:15
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=11:15)
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = fill(nothing, 5)
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=nothing, a=11:15)

    df = DataFrame(x=1:5)
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :a] = 11:15
    @test df ≅ DataFrame(x=1:5)
    @test_throws DimensionMismatch sdf[!, :x] = ["a"]
    sdf[!, :x] = fill(nothing, 5)
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=fill(nothing, 5))

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], :]
    sdf[!, :d] = [101, 103]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[101, missing, 103, missing, missing])
    sdf[!, :a] = [-1.0, -3.0]
    @test eltype(df.a) === Float64
    @test df ≅ DataFrame(a=[-1.0, 2, -3.0, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 103, missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], 1:end]
    @test_throws ArgumentError sdf[!, :d] = [101, 103]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[!, :a] = [-1.0, -3.0]
    @test eltype(df.a) === Float64
    @test df ≅ DataFrame(a=[-1.0, 2, -3.0, 4, 5],
                         b=11:15, c=21:25)

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], :]
    sdf[!, :d] = [103, 102]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing])
    sdf[!, "e"] = [1003, 1002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws ArgumentError sdf[!, 0] = [10003, 10002]
    @test_throws ArgumentError sdf[!, 6] = [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[!, 1] = ["10003", "10002"]
    @test eltype(df.a) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[!, :b] = [-13.0, -12.0]
    @test eltype(df.b) === Float64
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws ArgumentError sdf[!, :x] = 1
    @test_throws ArgumentError sdf[!, :x] = [1]
    @test_throws ArgumentError sdf[!, :a] = 1
    @test_throws DimensionMismatch sdf[!, :a] = [1]
    sdf[!, :f] = categorical(["3", "2"])
    @test df.f isa CategoricalArray
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    tmpc = df.c
    sdf[!, 3] = [33, 22]
    @test tmpc == 21:25
    @test tmpc != df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    sdf[!, 3] = categorical(["33", "22"])
    @test eltype(df.c) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, "22", "33", 24, 25],
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    @test df.c[2] isa CategoricalValue
    @test df.c[3] isa CategoricalValue

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], 1:3]
    @test_throws ArgumentError sdf[!, :d] = [103, 102]
    @test_throws ArgumentError sdf[!, "e"] = [1003, 1002]
    @test_throws ArgumentError sdf[!, 0] = [10003, 10002]
    @test_throws ArgumentError sdf[!, 6] = [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[!, 1] = ["10003", "10002"]
    @test eltype(df.a) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=11:15, c=21:25)
    sdf[!, :b] = [-13.0, -12.0]
    @test eltype(df.b) === Float64
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=21:25)
    @test_throws ArgumentError sdf[!, :x] = 1
    @test_throws ArgumentError sdf[!, :x] = [1]
    @test_throws ArgumentError sdf[!, :a] = 1
    @test_throws DimensionMismatch sdf[!, :a] = [1]
    @test_throws ArgumentError sdf[!, :f] = categorical(["3", "2"])
    tmpc = df.c
    sdf[!, 3] = [33, 22]
    @test tmpc == 21:25
    @test tmpc != df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, 22, 33, 24, 25])
    sdf[!, 3] = categorical(["33", "22"])
    @test eltype(df.c) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, "22", "33", 24, 25])
    @test df.c[2] isa CategoricalValue
    @test df.c[3] isa CategoricalValue

    sdf = @view df[[3, 2], 1:2]
    @test_throws ArgumentError sdf[!, :c] = 1:2
end

@testset "mutating SubDataFrame with broadcasting assignment to [!, col]" begin
    df = DataFrame()
    sdf = @view df[:, :]
    sdf[!, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[!, :b] .= 1
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)
    @test_throws DimensionMismatch sdf[!, :c] .= 1:2
    @test_throws DimensionMismatch sdf[!, :a] .= 1:2
    sdf[!, :a] .= [1.0]
    @test df.a isa Vector{Union{Missing, Float64}}
    @test isempty(df.a)
    sdf[!, :b] .= 1.0
    @test df.b isa Vector{Union{Missing, Float64}}
    @test isempty(df.b)

    df = DataFrame()
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[!, :a] .= [1]
    @test_throws ArgumentError sdf[!, :b] .= 1
    @test isempty(df)

    df = DataFrame()
    sdf = @view df[1:0, :]
    sdf[!, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[!, :b] .= 1
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)
    @test_throws DimensionMismatch sdf[!, :c] .= 1:2
    @test_throws DimensionMismatch sdf[!, :a] .= 1:2
    sdf[!, :a] .= [1.0]
    @test df.a isa Vector{Union{Missing, Float64}}
    @test isempty(df.a)
    sdf[!, :b] .= 1.0
    @test df.b isa Vector{Union{Missing, Float64}}
    @test isempty(df.b)

    df = DataFrame()
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[!, :a] .= [1]
    @test_throws ArgumentError sdf[!, :b] .= 1
    @test isempty(df)

    df = DataFrame(x=Int[])
    sdf = @view df[:, :]
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[:, 1:end]
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, :]
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, 1:end]
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, :]
    sdf[!, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)
    sdf[!, :x] .= Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[!, :a] .= [1]
    @test df == DataFrame(x=1:5)
    sdf[!, :x] .= Nothing[]
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=1:5)

    df = DataFrame(x=1:5)
    sdf = @view df[:, :]
    sdf[!, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=1)
    sdf[!, :b] .= 2
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=1, b=2)
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=fill(nothing, 5), a=1, b=2)

    df = DataFrame(x=1:5)
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[!, :a] .= [1]
    @test_throws ArgumentError sdf[!, :b] .= 2
    @test df == DataFrame(x=1:5)
    sdf[!, :x] .= nothing
    @test df.x isa Vector{Union{Nothing, Int}}
    @test df ≅ DataFrame(x=fill(nothing, 5))

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], :]
    sdf[!, :d] .= 101
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    sdf[!, :a] .= -1.0
    @test eltype(df.a) === Float64
    @test df ≅ DataFrame(a=[-1.0, 2, -1.0, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    @test_throws DimensionMismatch sdf[!, :a] .= [-1.0, -2.0, -3.0]
    sdf[!, :a] .= [-1.0, -2.0]
    @test df ≅ DataFrame(a=[-1.0, 2, -2.0, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    sdf[!, :e] .= 1:2
    @test df ≅ DataFrame(a=[-1.0, 2, -2.0, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing],
                         e=[1, missing, 2, missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], 1:end]
    @test_throws ArgumentError sdf[!, :d] .= 101
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[!, :a] .= -1.0
    @test eltype(df.a) === Float64
    @test df ≅ DataFrame(a=[-1.0, 2, -1.0, 4, 5],
                         b=11:15, c=21:25)
    @test_throws DimensionMismatch sdf[!, :a] .= [-1.0, -2.0, -3.0]
    sdf[!, :a] .= [-1.0, -2.0]
    @test df ≅ DataFrame(a=[-1.0, 2, -2.0, 4, 5],
                         b=11:15, c=21:25)
    @test_throws ArgumentError sdf[!, :e] .= 1:2
    @test df ≅ DataFrame(a=[-1.0, 2, -2.0, 4, 5],
                         b=11:15, c=21:25)

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], :]
    sdf[!, :d] .= 102
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing])
    sdf[!, "e"] .= [1003, 1002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws ArgumentError sdf[!, 0] .= [10003, 10002]
    @test_throws ArgumentError sdf[!, 6] .= 10002
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[!, 1] .= "10002"
    @test eltype(df.a) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10002", 4, 5],
                         b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[!, :b] .= [-13.0, -12.0]
    @test eltype(df.b) === Float64
    @test df ≅ DataFrame(a=[1, "10002", "10002", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws DimensionMismatch sdf[!, :x] .= 1:3
    @test_throws DimensionMismatch sdf[!, :a] .= 1:3
    sdf[!, :f] .= categorical(["3", "2"])
    @test df.f isa CategoricalArray
    @test df ≅ DataFrame(a=[1, "10002", "10002", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    tmpc = df.c
    sdf[!, 3] .= [33, 22]
    @test tmpc == 21:25
    @test tmpc != df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, "10002", "10002", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    sdf[!, 3] .= categorical(["33", "22"])[2]
    @test eltype(df.c) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10002", 4, 5],
                         b=[11, -12.0, -13.0, 14, 15],
                         c=[21, "22", "22", 24, 25],
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    @test df.c[2] isa CategoricalValue
    @test df.c[3] isa CategoricalValue

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], 1:3]
    @test_throws ArgumentError sdf[!, :d] .= [103, 102]
    @test_throws ArgumentError sdf[!, "e"] .= [1003, 1002]
    @test_throws ArgumentError sdf[!, 0] .= [10003, 10002]
    @test_throws ArgumentError sdf[!, 6] .= [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[!, 1] .= ["10003", "10002"]
    @test eltype(df.a) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=11:15, c=21:25)
    sdf[!, :b] .= -12.0
    @test eltype(df.b) === Float64
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -12.0, 14, 15],
                         c=21:25)
    @test_throws ArgumentError sdf[!, :x] .= 1
    @test_throws ArgumentError sdf[!, :x] .= [1]
    @test_throws ArgumentError sdf[!, :f] .= categorical(["3", "2"])
    tmpc = df.c
    sdf[!, 3] .= [33, 22]
    @test tmpc == 21:25
    @test tmpc != df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -12.0, 14, 15],
                         c=[21, 22, 33, 24, 25])
    sdf[!, 3] .= categorical(["33", "22"])[2]
    @test eltype(df.c) === Any
    @test df ≅ DataFrame(a=[1, "10002", "10003", 4, 5],
                         b=[11, -12.0, -12.0, 14, 15],
                         c=[21, "22", "22", 24, 25])
    @test df.c[2] isa CategoricalValue
    @test df.c[3] isa CategoricalValue

    sdf = @view df[[3, 2], 1:2]
    @test_throws ArgumentError sdf[!, :c] .= 1:2
end

@testset "mutating SubDataFrame with assignment to [!, cols]" begin
    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, [:c, :b, :a]] = DataFrame(c=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=[21, "d", "c", 24, 25])
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64
        @test eltype(df.c) == Any

        @test_throws ArgumentError sdf[!, [:c, :b, :a]] = DataFrame(d=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test_throws ArgumentError sdf[!, [:c, :b, :a]] = DataFrame(a=["c", "d"], b=[1.0, 2.0], c=[13, 12])
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64

        @test_throws ArgumentError sdf[!, cols] = DataFrame(b=[1.0, 2.0], a=[13, 12])
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[!, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64
        @test_throws ArgumentError sdf[!, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0], c=1)
    end

    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, [:c, :b, :a]] = ["b" "d" "f"; "a" "c" "e"]
        @test df == DataFrame(a=[1, "e", "f", 4, 5],
                            b=[11.0, "c", "d", 14.0, 15.0],
                            c=[21, "a", "b", 24, 25])
        @test tmpa !== df.a
        @test eltype(df.a) == Any
        @test eltype(df.b) == Any
        @test eltype(df.c) == Any

        @test_throws DimensionMismatch sdf[!, [:c, :b, :a]] = ones(2, 2)
        @test_throws DimensionMismatch sdf[!, [:c, :b, :a]] = ones(1, 3)
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, cols] = [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2.0, 1.0, 4, 5],
                            b=[11.0, 4.0, 3.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Float64
        @test eltype(df.b) == Float64

        @test_throws DimensionMismatch sdf[!, cols] = ones(1, 3)
        @test_throws DimensionMismatch sdf[!, cols] = ones(3, 1)
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[!, cols] = [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2.0, 1.0, 4, 5],
                            b=[11.0, 4.0, 3.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Float64
        @test eltype(df.b) == Float64
        @test_throws DimensionMismatch sdf[!, cols] = ones(1, 3)
        @test_throws DimensionMismatch sdf[!, cols] = ones(3, 1)
    end
end

@testset "mutating SubDataFrame with broadcasting assignment to [!, cols]" begin
    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, [:c, :b, :a]] .= DataFrame(c=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=[21, "d", "c", 24, 25])
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64
        @test eltype(df.c) == Any

        sdf[!, [:c, :b, :a]] .= [100, 200]
        @test df == DataFrame(a=[1, 200, 100, 4, 5],
                            b=[11.0, 200.0, 100.0, 14.0, 15.0],
                            c=[21, 200, 100, 24, 25])

        @test_throws ArgumentError sdf[!, [:c, :b, :a]] .= DataFrame(d=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test_throws ArgumentError sdf[!, [:c, :b, :a]] .= DataFrame(a=["c", "d"], b=[1.0, 2.0], c=[13, 12])
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64

        sdf[!, cols] .= [100 200]
        @test df == DataFrame(a=[1, 100, 100, 4, 5],
                            b=[11.0, 200.0, 200.0, 14.0, 15.0],
                            c=21:25)

        @test_throws ArgumentError sdf[!, cols] .= DataFrame(b=[1.0, 2.0], a=[13, 12])
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[!, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Float64

        sdf[!, cols] .= 100
        @test df == DataFrame(a=[1, 100, 100, 4, 5],
                            b=[11.0, 100.0, 100.0, 14.0, 15.0],
                            c=21:25)

        @test_throws DimensionMismatch sdf[!, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0], c=1)
    end

    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, [:c, :b, :a]] .= ["b" "d" "f"; "a" "c" "e"]
        @test df == DataFrame(a=[1, "e", "f", 4, 5],
                            b=[11.0, "c", "d", 14.0, 15.0],
                            c=[21, "a", "b", 24, 25])
        @test tmpa !== df.a
        @test eltype(df.a) == Any
        @test eltype(df.b) == Any
        @test eltype(df.c) == Any

        @test_throws DimensionMismatch sdf[!, [:c, :b, :a]] .= ones(2, 2)
        @test_throws DimensionMismatch sdf[!, [:c, :b, :a]] .= ones(4, 3)
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[!, cols] .= [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2.0, 1.0, 4, 5],
                            b=[11.0, 4.0, 3.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Float64
        @test eltype(df.b) == Float64

        @test_throws DimensionMismatch sdf[!, cols] .= ones(4, 3)
        @test_throws DimensionMismatch sdf[!, cols] .= ones(3, 4)
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[!, cols] .= [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2.0, 1.0, 4, 5],
                            b=[11.0, 4.0, 3.0, 14.0, 15.0],
                            c=21:25)
        @test tmpa !== df.a
        @test eltype(df.a) == Float64
        @test eltype(df.b) == Float64
        @test_throws DimensionMismatch sdf[!, cols] .= ones(4, 3)
        @test_throws DimensionMismatch sdf[!, cols] .= ones(3, 4)
    end
end

@testset "mutating SubDataFrame with assignment to [:, col]" begin
    df = DataFrame()
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(a=[])

    df = DataFrame()
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = Int[]
    @test isempty(df)

    df = DataFrame()
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(a=[])

    df = DataFrame()
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = Int[]
    @test isempty(df)

    df = DataFrame(x=Int[])
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(x=Int[], a=[])
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = Int[]
    @test df == DataFrame(x=Int[])
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df == DataFrame(x=Int[], a=[])
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = Int[]
    @test df == DataFrame(x=Int[])
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = Int[]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}
    @test df ≅ DataFrame(x=1:5, a=missing)

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = Int[]
    @test df ≅ DataFrame(x=1:5)
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    sdf[:, :x] = Nothing[]
    @test df.x isa Vector{Int}
    @test df ≅ DataFrame(x=1:5)

    df = DataFrame(x=1:5)
    sdf = @view df[:, :]
    @test_throws ArgumentError sdf[:, :a] = [1]
    sdf[:, :a] = 11:15
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=11:15)
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    @test_throws MethodError sdf[:, :x] = fill(nothing, 5)
    @test df ≅ DataFrame(x=1:5, a=11:15)

    df = DataFrame(x=1:5)
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :a] = 11:15
    @test df ≅ DataFrame(x=1:5)
    @test_throws DimensionMismatch sdf[:, :x] = ["a"]
    @test_throws MethodError sdf[:, :x] = fill(nothing, 5)

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], :]
    sdf[:, :d] = [101, 103]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[101, missing, 103, missing, missing])
    sdf[:, :a] = [-1.0, -3.0]
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[-1, 2, -3, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 103, missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], 1:end]
    @test_throws ArgumentError sdf[:, :d] = [101, 103]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[:, :a] = [-1.0, -3.0]
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[-1, 2, -3, 4, 5],
                         b=11:15, c=21:25)

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], :]
    sdf[:, :d] = [103, 102]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing])
    sdf[:, "e"] = [1003, 1002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws BoundsError sdf[:, 0] = [10003, 10002]
    @test_throws BoundsError sdf[:, 6] = [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws MethodError sdf[:, 1] = ["10003", "10002"]
    sdf[:, 1] = [10003, 10002]
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=11:15, c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[:, :b] = [-13.0, -12.0]
    @test eltype(df.b) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws ArgumentError sdf[:, :x] = 1
    @test_throws ArgumentError sdf[:, :x] = [1]
    @test_throws MethodError sdf[:, :a] = 1
    @test_throws DimensionMismatch sdf[:, :a] = [1]
    sdf[:, :f] = categorical(["3", "2"])
    @test df.f isa CategoricalArray
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=21:25,
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    tmpc = df.c
    sdf[:, 3] = [33, 22]
    @test tmpc === df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    @test_throws MethodError sdf[:, 3] = categorical(["33", "22"])
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 103, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], 1:3]
    @test_throws ArgumentError sdf[:, :d] = [103, 102]
    @test_throws ArgumentError sdf[:, "e"] = [1003, 1002]
    @test_throws BoundsError sdf[:, 0] = [10003, 10002]
    @test_throws BoundsError sdf[:, 6] = [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    @test_throws MethodError sdf[:, 1] = ["10003", "10002"]
    sdf[:, 1] = [10003, 10002]
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=11:15, c=21:25)
    sdf[:, :b] = [-13.0, -12.0]
    @test eltype(df.b) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=21:25)
    @test_throws ArgumentError sdf[:, :x] = 1
    @test_throws ArgumentError sdf[:, :x] = [1]
    @test_throws MethodError sdf[:, :a] = 1
    @test_throws DimensionMismatch sdf[:, :a] = [1]
    @test_throws ArgumentError sdf[:, :f] = categorical(["3", "2"])
    tmpc = df.c
    sdf[:, 3] = [33, 22]
    @test tmpc === df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25])
    @test_throws MethodError sdf[:, 3] = categorical(["33", "22"])
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25])

    sdf = @view df[[3, 2], 1:2]
    @test_throws ArgumentError df[!, :c] = 1:2
end

@testset "mutating SubDataFrame with broadcasting assignment to [:, col]" begin
    df = DataFrame()
    sdf = @view df[:, :]
    sdf[:, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[:, :b] .= 1
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)
    @test_throws DimensionMismatch sdf[:, :c] .= 1:2
    @test_throws DimensionMismatch sdf[:, :a] .= 1:2
    sdf[:, :a] .= [1.0]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[:, :b] .= 1.0
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)

    df = DataFrame()
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[:, :a] .= [1]
    @test_throws ArgumentError sdf[:, :b] .= 1
    @test isempty(df)

    df = DataFrame()
    sdf = @view df[1:0, :]
    sdf[:, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[:, :b] .= 1
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)
    @test_throws DimensionMismatch sdf[:, :c] .= 1:2
    @test_throws DimensionMismatch sdf[:, :a] .= 1:2
    sdf[:, :a] .= [1.0]
    @test df.a isa Vector{Union{Missing, Int}}
    @test isempty(df.a)
    sdf[:, :b] .= 1.0
    @test df.b isa Vector{Union{Missing, Int}}
    @test isempty(df.b)

    df = DataFrame()
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[:, :a] .= [1]
    @test_throws ArgumentError sdf[:, :b] .= 1
    @test isempty(df)

    df = DataFrame(x=Int[])
    sdf = @view df[:, :]
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[:, 1:end]
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, :]
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}

    df = DataFrame(x=Int[])
    sdf = @view df[1:0, 1:end]
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, :]
    sdf[:, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=missing)
    sdf[:, :x] .= Nothing[]
    @test df.x isa Vector{Int}
    @test df ≅ DataFrame(x=1:5, a=missing)

    df = DataFrame(x=1:5)
    sdf = @view df[1:0, 1:end]
    @test_throws ArgumentError sdf[:, :a] .= [1]
    @test df == DataFrame(x=1:5)
    sdf[:, :x] .= Nothing[]
    @test df.x isa Vector{Int}
    @test df ≅ DataFrame(x=1:5)

    df = DataFrame(x=1:5)
    sdf = @view df[:, :]
    sdf[:, :a] .= [1]
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=1)
    sdf[:, :b] .= 2
    @test df.a isa Vector{Union{Missing, Int}}
    @test df ≅ DataFrame(x=1:5, a=1, b=2)
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}
    sdf[:, :x] .= 1
    @test df ≅ DataFrame(x=fill(1, 5), a=1, b=2)

    df = DataFrame(x=1:5)
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError sdf[:, :a] .= [1]
    @test_throws ArgumentError sdf[:, :b] .= 2
    @test df == DataFrame(x=1:5)
    @test_throws MethodError sdf[:, :x] .= nothing
    @test df.x isa Vector{Int}
    sdf[:, :x] .= 1
    @test df ≅ DataFrame(x=fill(1, 5))

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], :]
    sdf[:, :d] .= 101
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    sdf[:, :a] .= -1.0
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[-1, 2, -1, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    @test_throws DimensionMismatch sdf[:, :a] .= [-1.0, -2.0, -3.0]
    sdf[:, :a] .= [-1.0, -2.0]
    @test df ≅ DataFrame(a=[-1, 2, -2, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing])
    sdf[:, :e] .= 1:2
    @test df ≅ DataFrame(a=[-1, 2, -2, 4, 5],
                         b=11:15, c=21:25,
                         d=[101, missing, 101, missing, missing],
                         e=[1, missing, 2, missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[1, 3], 1:end]
    @test_throws ArgumentError sdf[:, :d] .= 101
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    sdf[:, :a] .= -1.0
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[-1, 2, -1, 4, 5],
                         b=11:15, c=21:25)
    @test_throws DimensionMismatch sdf[:, :a] .= [-1.0, -2.0, -3.0]
    sdf[:, :a] .= [-1.0, -2.0]
    @test df ≅ DataFrame(a=[-1, 2, -2, 4, 5],
                         b=11:15, c=21:25)
    @test_throws ArgumentError sdf[:, :e] .= 1:2
    @test df ≅ DataFrame(a=[-1, 2, -2, 4, 5],
                         b=11:15, c=21:25)

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], :]
    sdf[:, :d] .= 102
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing])
    sdf[:, "e"] .= [1003, 1002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws ArgumentError sdf[:, 0] .= [10003, 10002]
    @test_throws ArgumentError sdf[:, 6] .= 10002
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws MethodError sdf[:, 1] .= "10002"
    sdf[:, 1] .= 10002
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10002, 4, 5],
                         b=11:15, c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    sdf[:, :b] .= [-13.0, -12.0]
    @test eltype(df.b) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10002, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing])
    @test_throws DimensionMismatch sdf[:, :x] .= 1:3
    @test_throws DimensionMismatch sdf[:, :a] .= 1:3
    sdf[:, :f] .= categorical(["3", "2"])
    @test df.f isa CategoricalArray
    @test df ≅ DataFrame(a=[1, 10002, 10002, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=21:25,
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    tmpc = df.c
    sdf[:, 3] .= [33, 22]
    @test tmpc === df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10002, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])
    @test_throws MethodError sdf[:, 3] .= categorical(["33", "22"])[2]
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10002, 4, 5],
                         b=[11, -12, -13, 14, 15],
                         c=[21, 22, 33, 24, 25],
                         d=[missing, 102, 102, missing, missing],
                         e=[missing, 1002, 1003, missing, missing],
                         f=[missing, "2", "3", missing, missing])

    df = DataFrame(a=1:5, b=11:15, c=21:25)
    sdf = @view df[[3, 2], 1:3]
    @test_throws ArgumentError sdf[:, :d] .= [103, 102]
    @test_throws ArgumentError sdf[:, "e"] .= [1003, 1002]
    @test_throws ArgumentError sdf[:, 0] .= [10003, 10002]
    @test_throws ArgumentError sdf[:, 6] .= [10003, 10002]
    @test df ≅ DataFrame(a=1:5, b=11:15, c=21:25)
    @test_throws MethodError sdf[:, 1] .= ["10003", "10002"]
    sdf[:, 1] .= [10003, 10002]
    @test eltype(df.a) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=11:15, c=21:25)
    sdf[:, :b] .= -12.0
    @test eltype(df.b) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -12, 14, 15],
                         c=21:25)
    @test_throws ArgumentError sdf[:, :x] .= 1
    @test_throws ArgumentError sdf[:, :x] .= [1]
    @test_throws ArgumentError sdf[:, :f] .= categorical(["3", "2"])
    tmpc = df.c
    sdf[:, 3] .= [33, 22]
    @test tmpc === df.c
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -12, 14, 15],
                         c=[21, 22, 33, 24, 25])
    @test_throws MethodError sdf[:, 3] .= categorical(["33", "22"])[2]
    @test eltype(df.c) === Int
    @test df ≅ DataFrame(a=[1, 10002, 10003, 4, 5],
                         b=[11, -12, -12, 14, 15],
                         c=[21, 22, 33, 24, 25])

    sdf = @view df[[3, 2], 1:2]
    @test_throws ArgumentError sdf[:, :c] .= 1:2
end

@testset "mutating SubDataFrame with assignment to [:, cols]" begin
    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, [:c, :b, :a]] = DataFrame(c=[5, 6], b=[1.0, 2.0], a=[13, 12])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11.0, 2.0, 1.0, 14.0, 15.0],
                            c=[21, 6, 5, 24, 25])
        @test tmpa === df.a

        @test_throws ArgumentError sdf[:, [:c, :b, :a]] = DataFrame(d=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test_throws ArgumentError sdf[:, [:c, :b, :a]] = DataFrame(a=["c", "d"], b=[1.0, 2.0], c=[13, 12])
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11, 2, 1, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int

        @test_throws ArgumentError sdf[:, cols] = DataFrame(b=[1.0, 2.0], a=[13, 12])
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[:, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11, 2, 1, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test_throws ArgumentError sdf[:, cols] = DataFrame(a=[13, 12], b=[1.0, 2.0], c=1)
    end

    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, [:c, :b, :a]] = [100 101 102; 103 104 105]
        @test df == DataFrame(a=[1, 105, 102, 4, 5],
                            b=[11.0, 104, 101, 14.0, 15.0],
                            c=[21, 103, 100, 24, 25])
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test eltype(df.c) == Int

        @test_throws DimensionMismatch sdf[:, [:c, :b, :a]] = ones(2, 2)
        @test_throws DimensionMismatch sdf[:, [:c, :b, :a]] = ones(1, 3)
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, cols] = [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2, 1, 4, 5],
                            b=[11, 4, 3, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int

        @test_throws DimensionMismatch sdf[:, cols] = ones(1, 3)
        @test_throws DimensionMismatch sdf[:, cols] = ones(3, 1)
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[:, cols] = [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2, 1, 4, 5],
                            b=[11, 4, 3, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test_throws DimensionMismatch sdf[:, cols] = ones(1, 3)
        @test_throws DimensionMismatch sdf[:, cols] = ones(3, 1)
    end
end

@testset "mutating SubDataFrame with broadcasting assignment to [:, cols]" begin
    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, [:c, :b, :a]] .= DataFrame(c=[100, 101], b=[1.0, 2.0], a=[13, 12])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11, 2, 1, 14, 15],
                            c=[21, 101, 100, 24, 25])
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test eltype(df.c) == Int

        sdf[:, [:c, :b, :a]] .= [100, 200]
        @test df == DataFrame(a=[1, 200, 100, 4, 5],
                            b=[11, 200, 100, 14, 15],
                            c=[21, 200, 100, 24, 25])

        @test_throws ArgumentError sdf[:, [:c, :b, :a]] .= DataFrame(d=["c", "d"], b=[1.0, 2.0], a=[13, 12])
        @test_throws ArgumentError sdf[:, [:c, :b, :a]] .= DataFrame(a=["c", "d"], b=[1.0, 2.0], c=[13, 12])
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11, 2, 1, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int

        sdf[:, cols] .= [100 200]
        @test df == DataFrame(a=[1, 100, 100, 4, 5],
                            b=[11, 200, 200, 14, 15],
                            c=21:25)

        @test_throws ArgumentError sdf[:, cols] .= DataFrame(b=[1.0, 2.0], a=[13, 12])
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[:, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0])
        @test df == DataFrame(a=[1, 12, 13, 4, 5],
                            b=[11, 2, 1, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int

        sdf[:, cols] .= 100
        @test df == DataFrame(a=[1, 100, 100, 4, 5],
                            b=[11, 100, 100, 14, 15],
                            c=21:25)

        @test_throws DimensionMismatch sdf[:, cols] .= DataFrame(a=[13, 12], b=[1.0, 2.0], c=1)
    end

    for sel in (:, 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, [:c, :b, :a]] .= [100 101 102; 103 104 105]
        @test df == DataFrame(a=[1, 105, 102, 4, 5],
                            b=[11.0, 104, 101, 14.0, 15.0],
                            c=[21, 103, 100, 24, 25])
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test eltype(df.c) == Int

        @test_throws DimensionMismatch sdf[:, [:c, :b, :a]] .= ones(2, 2)
        @test_throws DimensionMismatch sdf[:, [:c, :b, :a]] .= ones(4, 3)
    end

    for sel in (:, 1:3), cols in (Between(:a, :b), Not(:c), r"[ab]", [true, true, false])
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], sel]
        tmpa = df.a
        sdf[:, cols] .= [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2, 1, 4, 5],
                            b=[11, 4, 3, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int

        @test_throws DimensionMismatch sdf[:, cols] .= ones(4, 3)
        @test_throws DimensionMismatch sdf[:, cols] .= ones(3, 4)
    end

    for cols in (All(), :, Cols(:a, :b))
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        sdf = @view df[[3, 2], 1:2]
        tmpa = df.a
        sdf[:, cols] .= [1.0 3.0; 2.0 4.0]
        @test df == DataFrame(a=[1, 2, 1, 4, 5],
                            b=[11, 4, 3, 14, 15],
                            c=21:25)
        @test tmpa === df.a
        @test eltype(df.a) == Int
        @test eltype(df.b) == Int
        @test_throws DimensionMismatch sdf[:, cols] .= ones(4, 3)
        @test_throws DimensionMismatch sdf[:, cols] .= ones(3, 4)
    end
end

@testset "mutating SubDataFrame with assignment to sdf.col" begin
    df = DataFrame(a=1:3)
    sdf = @view df[[3, 2], :]
    sdf.c = [5, 6]
    @test df ≅ DataFrame(a=1:3, c=[missing, 6, 5])
    sdf.a = [13.0, 12.0]
    @test eltype(sdf.a) === Float64
    @test df ≅ DataFrame(a=[1.0, 12.0, 13.0], c=[missing, 6, 5])

    df = DataFrame(a=1:3)
    sdf = @view df[[3, 2], 1:1]
    @test_throws ArgumentError sdf.c = [5, 6]
    sdf.a = [13.0, 12.0]
    @test eltype(sdf.a) === Float64
    @test df ≅ DataFrame(a=[1.0, 12.0, 13.0])
end

@testset "mutating SubDataFrame with broadcasting assignment to sdf.col" begin
    df = DataFrame(a=1:3)
    sdf = @view df[[3, 2], :]
    sdf.a .= 12.0
    @test eltype(sdf.a) === Int
    @test df ≅ DataFrame(a=[1, 12, 12])

    if VERSION >= v"1.7"
        sdf.c .= 100
        @test df ≅ DataFrame(a=[1, 12, 12], c=[missing, 100, 100])
    else
        @test_throws ArgumentError sdf.c .= 100
    end

    df = DataFrame(a=1:3)
    sdf = @view df[[3, 2], 1:1]
    @test_throws ArgumentError sdf.c = [5, 6]
    sdf.a .= 12.0
    @test eltype(sdf.a) === Int
    @test df ≅ DataFrame(a=[1, 12, 12])
end

@testset "insertcols! for SubDataFrame" begin
    df = DataFrame(a=1:5, b=11:15)
    sdf = @view df[:, 1:end]
    @test_throws ArgumentError insertcols!(sdf, :c => 1)
    @test df == DataFrame(a=1:5, b=11:15)

    df = DataFrame(a=1:5, b=11:15)
    sdf = @view df[:, :]
    insertcols!(sdf, :c => 1)
    @test df == DataFrame(a=1:5, b=11:15, c=1)
    @test eltype(df.c) === Union{Int, Missing}
    @test_throws DimensionMismatch insertcols!(sdf, :d => [1])
    insertcols!(sdf, :d => 101:105)
    @test df == DataFrame(a=1:5, b=11:15, c=1, d=101:105)
    @test eltype(df.d) === Union{Int, Missing}

    df = DataFrame(a=1:5, b=11:15)
    sdf = @view df[[3, 2], :]
    insertcols!(sdf, :c => 1)
    @test df ≅ DataFrame(a=1:5, b=11:15, c=[missing, 1, 1, missing, missing])
    @test eltype(df.c) === Union{Int, Missing}
    @test_throws DimensionMismatch insertcols!(sdf, :d => [1])
    insertcols!(sdf, :d => [103, 102])
    @test df ≅ DataFrame(a=1:5, b=11:15, c=[missing, 1, 1, missing, missing],
                         d=[missing, 102, 103, missing, missing])
    @test eltype(df.d) === Union{Int, Missing}

    df = DataFrame(a=1:5, b=11:15)
    sdf = @view df[3:2, :]
    insertcols!(sdf, :c => 1)
    @test df ≅ DataFrame(a=1:5, b=11:15, c=[missing, missing, missing, missing, missing])
    @test eltype(df.c) === Union{Int, Missing}
    @test_throws DimensionMismatch insertcols!(sdf, :d => [1])
    insertcols!(sdf, :d => Int[])
    @test df ≅ DataFrame(a=1:5, b=11:15, c=[missing, missing, missing, missing, missing],
                         d=[missing, missing, missing, missing, missing])
    @test eltype(df.d) === Union{Int, Missing}
end

end # module