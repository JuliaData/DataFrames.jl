module TestShow
    using Base.Test, DataFrames

    df = DataFrame(A = 1:4, B = ["x\"", "∀ε⫺0: x+ε⫺x", "z\$", "AB\nC"], C = Float32[1.0, 2.0, 3.0, 4.0])
    srand(1)
    df_big = DataFrame(rand(50,50))

    io = IOBuffer()
    show(io, df)
    show(io, df, true)
    showall(io, df)
    showall(io, df, false)
    showcols(io, df, false, false)
    showcols(io, df, true, false)
    showcols(io, df, false, true)
    showcols(io, df, true, true)

    show(io, df_big)
    show(io, df_big, true)
    showall(io, df_big)
    showall(io, df_big, false)
    showcols(io, df_big, false, false)
    showcols(io, df_big, true, false)
    showcols(io, df_big, false, true)
    showcols(io, df_big, true, true)

    df_small = DataFrame(rand(1,5))
    showcols(df_small)

    df_min = DataFrame(rand(0,5))
    showcols(df_min)

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
