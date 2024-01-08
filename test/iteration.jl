module TestIteration

import Compat
using Test, DataFrames

@testset "eachrow and eachcol" begin
    df = DataFrame(A=Vector{Union{Int, Missing}}(1:2), B=Vector{Union{Int, Missing}}(2:3))

    @test nrow(eachrow(df)) == nrow(df)
    @test ncol(eachrow(df)) == ncol(df)
    @test size(eachrow(df)) == (size(df, 1),)
    @test parent(eachrow(df)) === df
    @test names(eachrow(df)) == names(df)
    @test IndexStyle(eachrow(df)) == IndexLinear()
    @test sprint(summary, eachrow(df)) == "2-element DataFrameRows"
    @test Base.IndexStyle(eachrow(df)) == IndexLinear()
    @test eachrow(df)[1] == DataFrameRow(df, 1, :)
    @test eachrow(df)[CartesianIndex(1)] == DataFrameRow(df, 1, :)
    @test_throws MethodError eachrow(df)[CartesianIndex(1, 1)]
    @test collect(eachrow(df)) isa Vector{<:DataFrameRow}
    @test eltype(eachrow(df)) <: DataFrameRow
    for row in eachrow(df)
        @test isa(row, DataFrameRow)
        @test (row[:B] - row[:A]) == 1
        # issue #683 (https://github.com/JuliaData/DataFrames.jl/pull/683)
        @test collect(pairs(row)) isa Vector{Pair{Symbol, Int}}
    end

    @test nrow(eachcol(df)) == nrow(df)
    @test ncol(eachcol(df)) == ncol(df)
    @test Base.IteratorSize(eachcol(df)) == Base.HasShape{1}()
    @test parent(eachcol(df)) === df
    @test names(eachcol(df)) == names(df)
    @test length(eachcol(df)) == size(df, 2)
    @test size(eachcol(df)) == (size(df, 2),)
    @test size(eachcol(df), 1) == size(df, 2)
    @test_throws ArgumentError size(eachcol(df), 2)
    @test_throws ArgumentError size(eachcol(df), 0)
    @test eachcol(df)[1] == df[:, 1]
    @test eachcol(df)[CartesianIndex(1)] == df[:, 1]
    @test_throws MethodError eachcol(df)[CartesianIndex(1, 1)]
    @test eachcol(df)[:A] === df[!, :A]
    @test eachcol(df)[All()] == eachcol(df)
    @test eachcol(df)[Cols(:)] == eachcol(df)
    @test isequal(eachcol(df)[[1]], eachcol(df[!, [1]]))
    @test eachcol(df).A === df[!, :A]
    @test eachcol(df)["A"] === df[!, "A"]
    @test eachcol(df)."A" === df[!, "A"]
    @test collect(eachcol(df)) isa Vector{AbstractVector}
    @test collect(eachcol(df)) == [[1, 2], [2, 3]]
    @test eltype(eachcol(df)) == AbstractVector
    @test_throws ArgumentError eachcol(df)[[1, 1]]
    @test eachcol(df)[[1]][1] === df.A
    for col in eachcol(df)
        @test isa(col, AbstractVector)
    end

    @test map(x -> minimum(Vector(x)), eachrow(df)) == [1, 2]
    @test map(Vector, eachrow(df)) == [[1, 2], [2, 3]]
    @test mapcols(minimum, df) == DataFrame(A=[1], B=[2])
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
    @test_throws DimensionMismatch mapcols(x -> x[1] == 1 ? [1] : [1, 2], df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] == 1 ? x : 0, df_mapcols)
    @test_throws ArgumentError mapcols(x -> x[1] != 1 ? x : 0, df_mapcols)
    df_mapcols2 = mapcols(x -> x, df_mapcols)
    @test df_mapcols2 == df_mapcols
    @test df_mapcols2.a !== df_mapcols.a
    @test df_mapcols2.b !== df_mapcols.b

    df = DataFrame(a=1)
    df = mapcols(x -> 2:2, df)
    @test df == DataFrame(a=2)
    @test df.a isa Vector{Int}

    df = DataFrame(a1=[1, 2], a2=[2, 3], b=[3, 4])
    @test mapcols(x -> 2x, df, cols=r"a") == DataFrame(a1=[2, 4], a2=[4, 6], b=[3, 4])
    @test mapcols(x -> 2x, df, cols="b") == DataFrame(a1=[1, 2], a2=[2, 3], b=[6, 8])
    @test mapcols(x -> 2x, df, cols=Not(r"a")) == DataFrame(a1=[1, 2], a2=[2, 3], b=[6, 8])
    @test mapcols(x -> 2x, df, cols=Int) == DataFrame(a1=[2, 4], a2=[4, 6], b=[6, 8])
    @test mapcols(x -> 2x, df, cols=Not(All())) == DataFrame(a1=[1, 2], a2=[2, 3], b=[3, 4])
    @test mapcols(x -> 2x, df, cols=:) == DataFrame(a1=[2, 4], a2=[4, 6], b=[6, 8])

    df2 = mapcols(x -> 2x, df, cols="b")
    @test df2.a1 == df.a1 && df2.a1 !== df.a1
    @test df2.a2 == df.a2 && df2.a2 !== df.a2
    @test df2.b == 2*df.b
end

@testset "mapcols!" begin
    df_empty = DataFrame()
    @test mapcols!(sum, df_empty) === df_empty

    df_mapcols = DataFrame(a=1:10, b=11:20)
    @test mapcols!(sum, df_mapcols) === df_mapcols
    @test df_mapcols == DataFrame(a=55, b=155)

    df_mapcols = DataFrame(a=1:10, b=11:20)
    mapcols!(x -> 1, df_mapcols)
    @test df_mapcols == DataFrame(a=1, b=1)

    df_mapcols = DataFrame(a=1:10, b=11:20)
    @test_throws ArgumentError mapcols!(x -> x[1] == 1 ? 0 : [0], df_mapcols)
    @test_throws DimensionMismatch mapcols!(x -> x[1] == 1 ? [1] : [1, 2], df_mapcols)
    @test_throws ArgumentError mapcols!(x -> x[1] == 1 ? x : 0, df_mapcols)
    @test_throws ArgumentError mapcols!(x -> x[1] != 1 ? x : 0, df_mapcols)
    @test df_mapcols == DataFrame(a=1:10, b=11:20)

    a = df_mapcols.a
    b = df_mapcols.b
    mapcols!(x -> x, df_mapcols)
    @test a === df_mapcols.a
    @test b === df_mapcols.b

    df = DataFrame(a=1)
    mapcols!(x -> 2:2, df)
    @test df == DataFrame(a=2)
    @test df.a isa Vector{Int}

    df = DataFrame(a1=[1, 2], a2=[2, 3], b=[3, 4])
    @test mapcols!(x -> 2x, copy(df), cols=r"a") == DataFrame(a1=[2, 4], a2=[4, 6], b=[3, 4])
    @test mapcols!(x -> 2x, copy(df), cols="b") == DataFrame(a1=[1, 2], a2=[2, 3], b=[6, 8])
    @test mapcols!(x -> 2x, copy(df), cols=Not(r"a")) == DataFrame(a1=[1, 2], a2=[2, 3], b=[6, 8])
    @test mapcols!(x -> 2x, copy(df), cols=Int) == DataFrame(a1=[2, 4], a2=[4, 6], b=[6, 8])
    @test mapcols!(x -> 2x, copy(df), cols=Not(All())) == DataFrame(a1=[1, 2], a2=[2, 3], b=[3, 4])
    @test mapcols!(x -> 2x, copy(df), cols=:) == DataFrame(a1=[2, 4], a2=[4, 6], b=[6, 8])
    a1, a2, b = eachcol(df)
    mapcols!(x -> 2x, df, cols=Not(All()))
    @test df == DataFrame(a1=[1, 2], a2=[2, 3], b=[3, 4])
    @test df.a1 === a1 && df.a2 === a2 && df.b === b
end

@testset "SubDataFrame" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)
    sdf = view(df, [3, 1, 4], [3, 1, 4])
    @test sdf == df[[3, 1, 4], [3, 1, 4]]
    @test eachrow(sdf) == eachrow(df[[3, 1, 4], [3, 1, 4]])
    @test size(eachrow(sdf)) == (3,)
    @test eachcol(sdf) == eachcol(df[[3, 1, 4], [3, 1, 4]])
    @test length(eachcol(sdf)) == 3
end

@testset "parent mutation" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)
    sdf = view(df, [3, 1, 4], [3, 1, 4])
    erd = eachrow(df)
    erv = eachrow(sdf)
    rename!(df, Symbol.(string.("y", 1:4)))
    df[!, 1] = 51:56
    @test df[1, :] == erd[1]
    @test copy(erv[1]) == (y3=33, y1=53, y4=43)
    df[!, :z] .= 1
    @test length(erd[1]) == 5 # the added column is reflected
    select!(df, Not([4, 5]))
    @test copy(erd[1]) == (y1 = 51, y2 = 21, y3 = 31) # the removed columns are reflected
end

@testset "getproperty and propertynames" begin
    df_base = DataFrame([11:16 21:26 31:36 41:46], :auto)
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

@testset "keys, values and pairs for eachcol" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)

    cols = eachcol(df)

    @test keys(cols) == propertynames(df)
    for (a, b, c) in zip(keys(cols), cols, pairs(cols))
        @test (a => b) == c
    end

    for (i, n) in enumerate(keys(cols))
        @test cols[i] === cols[n]
    end
    @test_throws ArgumentError cols[:non_existent]

    @test values(cols) == collect(cols)
end

@testset "findfirst, findnext, findlast, findprev, findall" begin
    df = DataFrame(a=[1, 2, 1, 2], b=["1", "2", "1", "2"],
                   c=[1, 2, 1, 2], d=["1", "2", "1", "2"])

    rows = eachrow(df)
    @test findfirst(row -> row.a == 1, rows) == 1
    @test findnext(row -> row.a == 1, rows, 2) == 3
    @test findlast(row -> row.a == 1, rows) == 3
    @test findprev(row -> row.a == 1, rows, 2) == 1
    @test findall(row -> row.a == 1, rows) == [1, 3]

    cols = eachcol(df)
    @test findfirst(col -> eltype(col) <: Int, cols) == 1
    @test findnext(col -> eltype(col) <: Int, cols, 2) == 3
    @test findnext(col -> eltype(col) <: Int, cols, 10) === nothing
    @test_throws BoundsError findnext(col -> eltype(col) <: Int, cols, -1)
    @test_throws ArgumentError findnext(col -> eltype(col) <: Int, cols, :x1)
    @test_throws ArgumentError findnext(col -> eltype(col) <: Int, cols, "x1")
    @test findnext(col -> eltype(col) <: Int, cols, :b) == 3
    @test findnext(col -> eltype(col) <: Int, cols, "b") == 3
    @test findlast(col -> eltype(col) <: Int, cols) == 3
    @test findprev(col -> eltype(col) <: Int, cols, 2) == 1
    @test findprev(col -> eltype(col) <: Int, cols, :b) == 1
    @test findprev(col -> eltype(col) <: Int, cols, "b") == 1
    @test findprev(col -> eltype(col) <: Int, cols, -1) === nothing
    @test_throws BoundsError findprev(col -> eltype(col) <: Int, cols, 10)
    @test_throws ArgumentError findprev(col -> eltype(col) <: Int, cols, :x1)
    @test_throws ArgumentError findprev(col -> eltype(col) <: Int, cols, "x1")
    @test findall(col -> eltype(col) <: Int, cols) == [1, 3]
end

@testset "multirow indexing of DataFrameRows" begin
    df = DataFrame(a=1:4, b=11:14)
    er = eachrow(df)
    for sel in (1:2, Not(3:4), [true, true, false, false])
        er2 = er[sel]
        @test er2 isa DataFrames.DataFrameRows
        @test parent(er2) isa SubDataFrame
        @test parent(parent(er2)) === df
        @test collect(er2) == [er[1], er[2]]
    end
    er2 = er[:]
    @test er2 == er
    @test er2 isa DataFrames.DataFrameRows
    er2 = er[2:1]
    @test length(er2) == 0
    @test isempty(parent(er2))

    # this is still allowed
    er2 = er[1:2, 1]
    @test er2 isa Vector{DataFrameRow}
    @test er2 == er[1:2]
end

@testset "test unaliasing of index" begin
    for idx in ([2, 3], [0x2, 0x3], 2:3, Not([1, 4]),
                [false, true, true, false], big.([2, 3]), big.(2:3), :)
        df = DataFrame(a=1:4)
        er = eachrow(df)
        er2 = er[idx]
        len = length(er2)
        @test len == (idx === Colon() ? 4 : 2)
        p = parent(er2)
        @test p isa SubDataFrame
        @test parentindices(p)[1] == (1:4)[idx]
        if !(idx isa UnitRange)
            @test parentindices(p)[1] !== idx
        end
        if idx isa Vector
            empty!(idx)
        end
        @test length(er2) == len

        er3 = filter(x -> x.a <= 2, er)
        @test length(er3) == 2
        @test parent(er3) isa SubDataFrame
        @test parentindices(parent(er3))[1] == 1:2
    end

end

@testset "haskey and get for DataFrameColumns" begin
    df_ref = DataFrame(a=1:3, b=2:4, c=3:5)
    for df in (df_ref, @view df_ref[1:3, 1:2])
        dfc = eachcol(df)
        @test !haskey(dfc, 0)
        @test haskey(dfc, 1)
        @test haskey(dfc, 2)
        @test !haskey(dfc, 4)
        @test !haskey(dfc, 0x0)
        @test haskey(dfc, 0x1)
        @test !haskey(dfc, :x)
        @test !haskey(dfc, "x")
        @test !haskey(dfc, Test.GenericString("x"))
        @test haskey(dfc, :a)
        @test haskey(dfc, "a")
        @test haskey(dfc, Test.GenericString("a"))

        @test get(dfc, 0, "error") == "error"
        @test get(dfc, 1, "error") == 1:3
        @test get(dfc, 4, "error") == "error"
        @test get(dfc, 0x0, "error") == "error"
        @test get(dfc, 0x1, "error") == 1:3
        @test get(dfc, :x, "error") == "error"
        @test get(dfc, "x", "error") == "error"
        @test get(dfc, Test.GenericString("x"), "error") == "error"
        @test get(dfc, :a, "error") == 1:3
        @test get(dfc, "a", "error") == 1:3
        @test get(dfc, Test.GenericString("a"), "error") == 1:3
        @test_throws MethodError get(dfc, "a")
    end
end

end # module
