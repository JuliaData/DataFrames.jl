module TestShow
    using Base.Test, DataFrames

    # In the future newline characte \n should be added to this test case
    df = DataFrame(A = 1:4, B = ["x\"", "∀ε⫺0: x+ε⫺x", "z\$", "ABC"],
                   C = Float32[1.0, 2.0, 3.0, 4.0])
    srand(1)
    df_big = DataFrame(rand(50,50))

    refstr = """
    4×3 DataFrames.DataFrame
    │ Row │ A │ B             │ C   │
    ├─────┼───┼───────────────┼─────┤
    │ 1   │ 1 │ x\"            │ 1.0 │
    │ 2   │ 2 │ ∀ε⫺0: x+ε⫺x │ 2.0 │
    │ 3   │ 3 │ z\$            │ 3.0 │
    │ 4   │ 4 │ ABC           │ 4.0 │"""

    for f in [show, showall], allcols in [true, false]
        io = IOBuffer()
        f(io, df, allcols)
        str = String(take!(io))
        @test str == refstr
    end

    refstr = """
    4×3 DataFrames.DataFrame
    
    │ Col # │ Name │ Eltype  │ Missing │
    ├───────┼──────┼─────────┼─────────┤
    │ 1     │ A    │ Int64   │ 0       │
    │ 2     │ B    │ String  │ 0       │
    │ 3     │ C    │ Float32 │ 0       │"""
    for a in [true, false]
        io = IOBuffer()
        showcols(io, df, a, false)
        str = String(take!(io))
        @test str == refstr
    end

    refstr = """
    4×3 DataFrames.DataFrame
    
    │ Col # │ Name │ Eltype  │ Missing │ Values          │
    ├───────┼──────┼─────────┼─────────┼─────────────────┤
    │ 1     │ A    │ Int64   │ 0       │ 1  …  4         │
    │ 2     │ B    │ String  │ 0       │ \"x\\\"\"  …  \"ABC\" │
    │ 3     │ C    │ Float32 │ 0       │ 1.0  …  4.0     │"""
    for a in [true, false]
        io = IOBuffer()
        showcols(io, df, a, true)
        str = String(take!(io))
        @test str == refstr
    end

    io = IOBuffer()
    show(io, df_big)
    show(io, df_big, true)
    showall(io, df_big)
    showall(io, df_big, false)
    showcols(io, df_big, false, false)
    showcols(io, df_big, true, false)
    showcols(io, df_big, false, true)
    showcols(io, df_big, true, true)

    io = IOBuffer()
    df_small = DataFrame([1.0:5.0;])
    showcols(io, df_small)
    str = String(take!(io))
    @test str == """
    1×5 DataFrames.DataFrame
    
    │ Col # │ Name │ Eltype  │ Missing │ Values │
    ├───────┼──────┼─────────┼─────────┼────────┤
    │ 1     │ x1   │ Float64 │ 0       │ 1.0    │
    │ 2     │ x2   │ Float64 │ 0       │ 2.0    │
    │ 3     │ x3   │ Float64 │ 0       │ 3.0    │
    │ 4     │ x4   │ Float64 │ 0       │ 4.0    │
    │ 5     │ x5   │ Float64 │ 0       │ 5.0    │"""

    io = IOBuffer()
    df_min = DataFrame(rand(0,5))
    showcols(io, df_min)
    str = String(take!(io))
    @test str == """
    0×5 DataFrames.DataFrame
    
    │ Col # │ Name │ Eltype  │ Missing │
    ├───────┼──────┼─────────┼─────────┤
    │ 1     │ x1   │ Float64 │ 0       │
    │ 2     │ x2   │ Float64 │ 0       │
    │ 3     │ x3   │ Float64 │ 0       │
    │ 4     │ x4   │ Float64 │ 0       │
    │ 5     │ x5   │ Float64 │ 0       │"""

    subdf = view(df, [2, 3]) # df[df[:A] .> 1.0, :]
    show(io, subdf)
    show(io, subdf, true)
    showall(io, subdf)
    showall(io, subdf, false)

    dfvec = DataFrame[df for _=1:3]
    show(io, dfvec)
    showall(io, dfvec)

    gd = groupby(df, :A)
    show(io, gd)
    showall(io, gd)

    dfr = DataFrameRow(df, 1)
    show(io, dfr)

    df = DataFrame(A = Vector{String}(3))

    A = DataFrames.StackedVector(Any[[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    show(io, A)
    A = DataFrames.RepeatedVector([1, 2, 3], 5, 1)
    show(io, A)
    A = DataFrames.RepeatedVector([1, 2, 3], 1, 5)
    show(io, A)

    #Test show output for REPL and similar
    df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, null])
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
    2×2 DataFrames.DataFrame
    │ Row │ Fish │ Mass │
    ├─────┼──────┼──────┤
    │ 1   │ Suzy │ 1.5  │
    │ 2   │ Amir │ null │"""

    # Test computing width for Array{String} columns
    df = DataFrame(Any[["a"]], [:x])
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
    1×1 DataFrames.DataFrame
    │ Row │ x │
    ├─────┼───┤
    │ 1   │ a │"""
end
