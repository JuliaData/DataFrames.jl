module TestShow
    using DataFrames
    using Compat
    using Base.Test
    import Compat.String
    df = DataFrame(A = 1:3, B = ["x", "y", "z"])

    io = IOBuffer()
    show(io, df)
    show(io, df, true)
    showall(io, df)
    showall(io, df, true)

    subdf = view(df, [2, 3]) # df[df[:A] .> 1.0, :]
    show(io, subdf)
    show(io, subdf, true)
    showall(io, subdf)
    showall(io, subdf, true)

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
    df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, Nullable()])
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
    2×2 DataFrames.DataFrame
    │ Row │ Fish │ Mass  │
    ├─────┼──────┼───────┤
    │ 1   │ Suzy │ 1.5   │
    │ 2   │ Amir │ #NULL │"""

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
