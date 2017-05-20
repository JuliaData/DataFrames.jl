module TestIteration
    using Base.Test, DataTables

    dv = [1, 2, null]
    dm = Union{Int, Null}[1 2; 3 4]
    dt = Array{Union{Int, Null}}(zeros(2, 2, 2))

    dt = DataTable(A = Vector{Union{Int, Null}}(1:2), B = Vector{Union{Int, Null}}(2:3))

    for row in eachrow(dt)
        @test isa(row, DataTableRow)
        @test (row[:B] - row[:A]) == 1

        # issue #683 (https://github.com/JuliaStats/DataFrames.jl/pull/683)
        @test typeof(collect(row)) == Array{Tuple{Symbol, Any}, 1}
    end

    for col in eachcol(dt)
        @test isa(col, Tuple{Symbol, AbstractVector})
    end

    @test map(x -> minimum(convert(Array, x)), eachrow(dt)) == Any[1,2]
    @test map(minimum, eachcol(dt)) == DataTable(A = [1], B = [2])

    row = DataTableRow(dt, 1)

    row[:A] = 100
    @test dt[1, :A] == 100

    row[1] = 101
    @test dt[1, :A] == 101

    dt = DataTable(A = Vector{Union{Int, Null}}(1:4), B = Union{String, Null}["M", "F", "F", "M"])

    s1 = view(dt, 1:3)
    s1[2,:A] = 4
    @test dt[2, :A] == 4
    @test view(s1, 1:2) == view(dt, 1:2)

    s2 = view(dt, 1:2:3)
    s2[2, :B] = "M"
    @test dt[3, :B] == "M"
    @test view(s2, 1:1:2) == view(dt, [1,3])

    # @test_fail for x in dt; end # Raises an error
end
