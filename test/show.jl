module TestShow
    using Base.Test, DataFrames

    dt = DataFrame(A = 1:3, B = ["x", "y", "z"])

    io = IOBuffer()
    show(io, dt)
    show(io, dt, true)
    showall(io, dt)
    showall(io, dt, true)

    subdt = view(dt, [2, 3]) # dt[dt[:A] .> 1.0, :]
    show(io, subdt)
    show(io, subdt, true)
    showall(io, subdt)
    showall(io, subdt, true)

    dtvec = DataFrame[dt for _=1:3]
    show(io, dtvec)
    showall(io, dtvec)

    gd = groupby(dt, :A)
    show(io, gd)
    showall(io, gd)

    dtr = DataFrameRow(dt, 1)
    show(io, dtr)

    dt = DataFrame(A = Vector{String}(3))

    A = DataFrames.StackedVector(Any[[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    show(io, A)
    A = DataFrames.RepeatedVector([1, 2, 3], 5, 1)
    show(io, A)
    A = DataFrames.RepeatedVector([1, 2, 3], 1, 5)
    show(io, A)

    #Test show output for REPL and similar
    dt = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, null])
    io = IOBuffer()
    show(io, dt)
    str = String(take!(io))
    @test str == """
    2×2 DataFrames.DataFrame
    │ Row │ Fish │ Mass │
    ├─────┼──────┼──────┤
    │ 1   │ Suzy │ 1.5  │
    │ 2   │ Amir │ null │"""

    # Test computing width for Array{String} columns
    dt = DataFrame(Any[["a"]], [:x])
    io = IOBuffer()
    show(io, dt)
    str = String(take!(io))
    @test str == """
    1×1 DataFrames.DataFrame
    │ Row │ x │
    ├─────┼───┤
    │ 1   │ a │"""
end
