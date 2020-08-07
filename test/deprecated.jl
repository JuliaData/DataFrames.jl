module TestDeprecated

using Test, DataFrames

@testset "DataFrame!" begin
    x = [1,2,3]
    y = [4,5,6]
    @test DataFrame!(x=x, y=y, copycols=true) == DataFrame(x=x,y=y)
    df1 = DataFrame(x=x, y=y)
    df2 = DataFrame!(df1)
    @test df1 == df2
    @test df1.x === df2.x
    @test df1.y === df2.y

    a=[1,2,3]
    df = DataFrame!(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!((x, y))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame!((x, y), (:x1, :x2))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame!((x, y), ("x1", "x2"))
    @test names(df) == ["x1", "x2"]
    @test df."x1" === x
    @test df."x2" === y

    @test_throws MethodError DataFrame!([1 2; 3 4], copycols=false)
    @test_throws MethodError DataFrame!([1 2; 3 4])
    @test_throws MethodError DataFrame!([Union{Int, Missing}, Union{Float64, Missing}],
                                        [:x1, :x2], 2)
end

end # module
