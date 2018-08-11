module TestTables
using Test
using DataFrames
using Tables

@testset "Tables" begin
    df = DataFrame(a=[1, 2, 3], b=[:a, :b, :c])

    @testset "basics" begin
        @test Tables.schema(df) === NamedTuple{(:a, :b), Tuple{Int64, Symbol}}
        @test Tables.AccessStyle(df) === Tables.ColumnAccess()

        rowstate = iterate(Tables.rows(df))[1]
        @test all(propertynames(rowstate) .== (:a, :b))
    end

    @testset "Row-style" begin
        bare_rows = rowtable(df)
        for (actual, expected) in zip(bare_rows, eachrow(df))
            @test actual.a == expected.a
            @test actual.b == expected.b
        end

        and_back = DataFrame(rowtable(df))
        @test and_back isa DataFrame
        @test all(DataFrames.names(and_back) .== (:a, :b))
        @test all(and_back.a .== df.a)
        @test all(and_back.b .== df.b)
    end

    @testset "Column-style" begin
        @test columntable(df).a  ==  df.a
        @test columntable(df).b  ==  df.b

        and_back = DataFrame(columntable(df))
        @test and_back isa DataFrame
        @test all(DataFrames.names(and_back) .== (:a, :b))
        @test all(and_back.a .== df.a)
        @test all(and_back.b .== df.b)
    end
end
end
