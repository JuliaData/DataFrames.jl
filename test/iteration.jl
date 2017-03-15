module TestIteration
    using Base.Test, DataFrames, Compat

    dv = @data([1, 2, NA])
    dm = DataArray([1 2; 3 4])
    dt = DataArray(zeros(2, 2, 2))

    df = DataFrame(A = 1:2, B = 2:3)

    for el in dv
        @test ndims(el) == 0
    end

    for el in dm
        @test ndims(el) == 0
    end

    for el in dt
        @test ndims(el) == 0
    end

    for row in eachrow(df)
        @test isa(row, DataFrameRow)
        @test row[:B]-row[:A] == 1

        # issue #683 (https://github.com/JuliaStats/DataFrames.jl/pull/683)
        @test typeof(collect(row)) == Array{Tuple{Symbol, Any}, 1}
    end

    for col in eachcol(df)
        @test isa(col, Tuple{Symbol, AbstractDataVector})
    end

    @test isequal(map(x -> minimum(convert(Array, x)), eachrow(df)), Any[1,2])
    @test isequal(map(minimum, eachcol(df)), DataFrame(A = [1], B = [2]))

    row = DataFrameRow(df, 1)

    row[:A] = 100
    @test df[1, :A] == 100

    row[1] = 101
    @test df[1, :A] == 101

    df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])

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
