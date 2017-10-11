module DepWarns
    using Base.Test, DataFrames, CategoricalArrays

    @testset "categorical! on a single non-String columns" begin
        df = DataFrame(A = [1, 1, 1, 2, 2, 2], B = ["X", "X", "X", "Y", "Y", "Y"])
        @test_warn "CategoricalVectors are designed to work with String values and their use on other column types is deprecated" categorical!(deepcopy(df), :A)
        @test eltypes(categorical!(deepcopy(df), :A)) == [Int64, String]
    end

    @testset "categorical! with some non-String columns" begin
        df = DataFrame(A = [1, 1, 1, 2, 2, 2], B = ["X", "X", "X", "Y", "Y", "Y"])
        @test_warn "CategoricalVectors are designed to work with String values and their use on other column types is deprecated" categorical!(deepcopy(df), [:A, :B])
        @test eltypes(categorical!(deepcopy(df), [:A, :B])) ==
            [Int64,
             CategoricalArrays.CategoricalValue{String,CategoricalArrays.DefaultRefType}]
    end
end
