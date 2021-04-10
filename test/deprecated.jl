module TestDeprecated

using Test, DataFrames

const ≅ = isequal

@testset "by and aggregate" begin
    @test_throws ArgumentError by()
    @test_throws ArgumentError aggregate()
end

@testset "deprecated broadcasting assignment" begin
    df = DataFrame(a=1:4, b=1, c=2)
    df.a .= 'a':'d'
    @test df == DataFrame(a=97:100, b=1, c=2)
    dfv = view(df, 2:3, 2:3)
    dfv.b .= 0
    @test df.b == [1, 0, 0, 1]
end

@testset "indicator in joins" begin
    name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

    @test outerjoin(name, job, on = :ID, indicator=:source) ≅
          outerjoin(name, job, on = :ID, source=:source)
    @test leftjoin(name, job, on = :ID, indicator=:source) ≅
          leftjoin(name, job, on = :ID, source=:source)
    @test rightjoin(name, job, on = :ID, indicator=:source) ≅
          rightjoin(name, job, on = :ID, source=:source)

    @test_throws ArgumentError outerjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
    @test_throws ArgumentError leftjoin(name, job, on = :ID,
                                       indicator=:source, source=:source)
    @test_throws ArgumentError rightjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
end

end # module
