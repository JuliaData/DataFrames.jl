module TestDeprecated

using Test, DataFrames, CategoricalArrays

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
    name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])

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

@testset "hcat with vector" begin
      df = DataFrame(x=1:3)
      x = [4, 5, 6]
      hdf = hcat(df, x)
      @test hdf[!, 1] == df[!, 1]
      @test hdf[!, 1] !== df[!, 1]
      @test hdf[!, 2] !== x
      hdf = hcat(x, df)
      @test hdf[!, 2] == df[!, 1]
      @test hdf[!, 2] !== df[!, 1]
      @test hdf[!, 1] !== x

      df = DataFrame()
      DataFrames.hcat!(df, CategoricalVector{Union{Int, Missing}}(1:10), makeunique=true)
      @test df[!, 1] == CategoricalVector(1:10)
      DataFrames.hcat!(df, 1:10, makeunique=true)
      @test df[!, 2] == collect(1:10)
      DataFrames.hcat!(df, collect(1:10), makeunique=true)
      @test df[!, 3] == collect(1:10)

      df = DataFrame()
      df2 = hcat(CategoricalVector{Union{Int, Missing}}(1:10), df, makeunique=true)
      @test isempty(df)
      @test df2[!, 1] == collect(1:10)
      @test names(df2) == ["x1"]
      ref_df = copy(df2)
      df3 = hcat(11:20, df2, makeunique=true)
      @test df2 == ref_df
      @test df3[!, 1] == collect(11:20)
      @test names(df3) == ["x1", "x1_1"]

      df1 = DataFrame(a=1:3)
      df2 = DataFrame(b=1:3)
      dfv = view(df2, :, :)
      x = [1, 2, 3]

      df3 = hcat(df1, x)
      @test propertynames(df3) == [:a, :x1]
      @test df3.a == df1.a
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.x1 !== x
      df3 = hcat(df1, x, copycols=true)
      @test propertynames(df3) == [:a, :x1]
      @test df3.a == df1.a
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.x1 !== x
      df3 = hcat(df1, x, copycols=false)
      @test propertynames(df3) == [:a, :x1]
      @test df3.a === df1.a
      @test df3.x1 === x

      df3 = hcat(x, df1)
      @test propertynames(df3) == [:x1, :a]
      @test df3.a == df1.a
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.x1 !== x
      df3 = hcat(x, df1, copycols=true)
      @test propertynames(df3) == [:x1, :a]
      @test df3.a == df1.a
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.x1 !== x
      df3 = hcat(x, df1, copycols=false)
      @test propertynames(df3) == [:x1, :a]
      @test df3.a === df1.a
      @test df3.x1 === x

      df3 = hcat(dfv, x, df1)
      @test propertynames(df3) == [:b, :x1, :a]
      @test df3.a == df1.a
      @test df3.b == dfv.b
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.b !== dfv.b
      @test df3.x1 !== x
      df3 = hcat(dfv, x, df1, copycols=true)
      @test propertynames(df3) == [:b, :x1, :a]
      @test df3.a == df1.a
      @test df3.b == dfv.b
      @test df3.x1 == x
      @test df3.a !== df1.a
      @test df3.b !== dfv.b
      @test df3.x1 !== x
      df3 = hcat(dfv, x, df1, copycols=false)
      @test propertynames(df3) == [:b, :x1, :a]
      @test df3.a === df1.a
      @test df3.b === dfv.b
      @test df3.x1 === x
end

@testset "delete!" begin
    df = DataFrame(a=1:4, b=1, c=2)
    @test delete!(copy(df), 1) == deleteat!(copy(df), 1)
    @test delete!(copy(df), [1, 3]) == deleteat!(copy(df), [1, 3])
    @test delete!(copy(df), [true, false, false, true]) == deleteat!(copy(df), [true, false, false, true])
    @test delete!(copy(df), Not(1)) == deleteat!(copy(df), Not(1))
    delete!(df, 2)
    @test df == DataFrame(a=[1, 3, 4], b=1, c=2)
end

@testset "deprecated sort" begin
    df = DataFrame(x=1, y=4:-1:1)
    @test sort(df, []) == DataFrame(x=1, y=1:4)
    @test !issorted(df, [])
    @test sortperm(df, []) == 4:-1:1
    sort!(df, [])
    @test df == DataFrame(x=1, y=1:4)
end

end # module
