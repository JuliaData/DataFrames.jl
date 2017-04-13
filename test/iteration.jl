module TestIteration
    using Base.Test, DataFrames, Compat

    dv = NullableArray(Nullable{Int}[1, 2, Nullable()])
    dm = NullableArray([1 2; 3 4])
    dt = NullableArray(zeros(2, 2, 2))

    df = DataFrame(A = NullableArray(1:2), B = NullableArray(2:3))

    for row in eachrow(df)
        @test isa(row, DataFrameRow)
        @test isequal(row[:B]-row[:A], Nullable(1))

        # issue #683 (https://github.com/JuliaStats/DataFrames.jl/pull/683)
        @test typeof(collect(row)) == @compat Array{Tuple{Symbol, Any}, 1}
    end

    for col in eachcol(df)
        @test isa(col, @compat Tuple{Symbol, NullableVector})
    end

    @test isequal(map(x -> minimum(convert(Array, x)), eachrow(df)), Any[1,2])
    @test isequal(map(minimum, eachcol(df)), DataFrame(A = Nullable{Int}[1], B = Nullable{Int}[2]))

    row = DataFrameRow(df, 1)

    row[:A] = 100
    @test isequal(df[1, :A], Nullable(100))

    row[1] = 101
    @test isequal(df[1, :A], Nullable(101))

    df = DataFrame(A = NullableArray(1:4), B = NullableArray(["M", "F", "F", "M"]))

    s1 = view(df, 1:3)
    s1[2,:A] = 4
    @test isequal(df[2, :A], Nullable(4))
    @test isequal(view(s1, 1:2), view(df, 1:2))

    s2 = view(df, 1:2:3)
    s2[2, :B] = "M"
    @test isequal(df[3, :B], Nullable("M"))
    @test isequal(view(s2, 1:1:2), view(df, [1,3]))

    # @test_fail for x in df; end # Raises an error
end
