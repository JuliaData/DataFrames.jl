module TestOffsetIndexing

using Test, DataFrames, OffsetArrays

@testset "Check allowing only 1-based indexing of vectors" begin
    ov1 = OffsetArray(1:5, 0:4)
    ov2 = OffsetArray(1:5, 1:5)

    @test_throws ArgumentError DataFrame(a=ov1)
    @test DataFrame(a=ov2) == DataFrame(a=1:5)

    @test_throws ArgumentError mapcols!(x -> ov1, DataFrame(a=1))
    @test mapcols!(x -> ov2, DataFrame(a=1)) == DataFrame(a=ov2)

    df = DataFrame(a=1)
    DataFrames._columns(df)[1] = ov1
    @test_throws ArgumentError DataFrames._check_consistency(df)
    DataFrames._columns(df)[1] = ov2
    DataFrames._check_consistency(df)

    @test_throws ArgumentError df.b = ov1
    @test_throws ArgumentError insertcols!(df, :b => ov1)
    @test_throws DimensionMismatch df[!, :b] .= ov1

    # this is consequence of the fact that OffsetArrays wrap AbstractRange in this case
    @test_throws ErrorException df[:, :a] = ov1

    # this inconsistency is the consequence how setindex! for vector is defined in Base
    df = DataFrame(a=5:-1:1)
    @test_throws ArgumentError df[:, :b] = ov1
    df[:, :a] = ov1
    @test df == DataFrame(a=1:5)
end

end # module
