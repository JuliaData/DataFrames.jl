module TestTables
using Test, Tables, DataFrames

@testset "Tables" begin
    df = DataFrame(a=[1, 2, 3], b=[:a, :b, :c])

    @testset "basics" begin
        @test Tables.schema(df) === NamedTuple{(:a, :b), Tuple{Int64, Symbol}}
        @test Tables.AccessStyle(df) === Tables.ColumnAccess()

        rowstate = iterate(Tables.rows(df))[1]
        @test propertynames(rowstate) == (:a, :b)
    end

    @testset "Row-style" begin
        bare_rows = Tables.rowtable(df)
        for (actual, expected) in zip(bare_rows, eachrow(df))
            @test actual.a == expected.a
            @test actual.b == expected.b
        end

        and_back = DataFrame(bare_rows)
        @test and_back isa DataFrame
        @test names(and_back) == (:a, :b)
        @test and_back.a == df.a
        @test and_back.b == df.b
    end

    @testset "Column-style" begin
        cols = Tables.columntable(df)
        @test cols.b  ==  df.b
        @test cols.a  ==  df.a

        and_back = DataFrame(cols)
        @test and_back isa DataFrame
        @test names(and_back) == (:a, :b)
        @test and_back.a == df.a
        @test and_back.b == df.b
    end

    @testset "Extras" begin
        df = DataFrame(a=[1, missing, 3], b=[missing, 'a', "hey"])
        @test isequal(df, DataFrame(Tables.rowtable(df)))
        @test isequal(df, DataFrame(Tables.columntable(df)))
    end
end
end
