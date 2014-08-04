module TestConversions
    using Base.Test
    using DataFrames

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Matrix, df), Matrix{Any})
    @test isa(convert(Matrix{Any}, df), Matrix{Any})

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = 1.0:5.0
    @test isa(convert(Matrix, df), Matrix{Real})
    @test isa(convert(Matrix{Float64}, df), Matrix{Float64})

    df = DataFrame()
    df[:A] = 1.0:5.0
    df[:B] = 1.0:5.0
    @test isa(convert(Matrix, df), Matrix{Float64})
    @test isa(convert(Matrix{Float64}, df), Matrix{Float64})
    @test isa(convert(Matrix{Int}, df), Matrix{Int})
end
