module TestIteration
    using Test, DataFrames

    dv = [1, 2, missing]
    dm = Union{Int, Missing}[1 2; 3 4]
    df = Array{Union{Int, Missing}}(zeros(2, 2, 2))

    df = DataFrame(A = Vector{Union{Int, Missing}}(1:2), B = Vector{Union{Int, Missing}}(2:3))

    @test size(eachrow(df)) == (size(df, 1),)
    @test eachrow(df)[1] == DataFrameRow(df, 1)
    @test collect(eachrow(df)) isa Vector{DataFrameRow{DataFrame}}
    @test eltype(eachrow(df)) == DataFrameRow{DataFrame}
    for row in eachrow(df)
        @test isa(row, DataFrameRow)
        @test (row[:B] - row[:A]) == 1
        # issue #683 (https://github.com/JuliaData/DataFrames.jl/pull/683)
        @test collect(row) isa Vector{Pair{Symbol, Int}}
    end

    @test size(eachcol(df)) == (size(df, 2),)
    @test length(eachcol(df)) == size(df, 2)
    @test eachcol(df)[1] == df[:, 1]
    @test collect(eachcol(df)) isa Vector{Tuple{Symbol, Any}}
    @test eltype(eachcol(df)) == Tuple{Symbol, Any}
    for col in eachcol(df)
        @test isa(col, Tuple{Symbol, AbstractVector})
    end

    @test map(x -> minimum(convert(Array, x)), eachrow(df)) == Any[1,2]
    @test map(minimum, eachcol(df)) == DataFrame(A = [1], B = [2])

    row = DataFrameRow(df, 1)

    row[:A] = 100
    @test df[1, :A] == 100

    row[1] = 101
    @test df[1, :A] == 101

    df = DataFrame(A = Vector{Union{Int, Missing}}(1:4), B = Union{String, Missing}["M", "F", "F", "M"])

    s1 = view(df, 1:3)
    s1[2,:A] = 4
    @test df[2, :A] == 4
    @test view(s1, 1:2) == view(df, 1:2)

    s2 = view(df, 1:2:3)
    s2[2, :B] = "M"
    @test df[3, :B] == "M"
    @test view(s2, 1:1:2) == view(df, [1,3])

    # @test_fail for x in df; end # Raises an error
end
