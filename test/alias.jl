module TestAliasAssignment

using Test, DataFrames
const ≅ = isequal

@testset "Aliasing asignment" begin
    @testset "$init" for init in [[], Matrix([4 5 6]'), [4 5;6 7;8 9]]
        dfr = DataFrame(init, :auto)
        @testset "$v" for v in [
                [1,2,3],
                1:3,
                ]
            @testset "df.x2 = v" begin
                df = copy(dfr)
                a = @alias df.x2 = v
                @test a === v
                @test df.x2 === df[!, :x2] === v
                df.x2 = v
                @test df.x2 !== v
                @test df.x2 ≅ v
                a = @alias df.x2 = v
                @test a === v
                @test df.x2 === df[!, :x2] === v
            end

            @testset "df[!, :x2] = v" begin
                df = copy(dfr)
                a = @alias df[!, :x2] = v
                @test a === v
                @test df.x2 === df[!, :x2] === v
                df[!, :x2] = v
                @test df[!, :x2] !== v
                @test df[!, :x2] ≅ v
                a = @alias df[!, :x2] = v
                @test a === v
                @test df.x2 === df[!, :x2] === v
            end
        end

        @testset "Invalid use of alias macro" begin
            ex = try
                eval(:(@alias 1))
            catch ex
                ex
            end
            @test ex.error isa ArgumentError

            df = copy(dfr)
            ex = try
                eval(:(@alias df.x .= 1))
            catch ex
                ex
            end
            @test ex.error isa ArgumentError

            struct S; x; end
            s = S(1)
            @test_throws MethodError @alias s.x = 1
            @test_throws MethodError @alias s.x2 = 1

            @test_throws MethodError @alias df.x2 = 1
            @test_throws MethodError @alias df[!, :x2] = 1
            @test_throws MethodError @alias df[:, :x2] = 1
        end
    end
end

end # module
