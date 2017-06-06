module TestShow
    using DataTables
    using Compat
    using Base.Test
    import Compat.String
    dt = DataTable(A = 1:3, B = ["x", "y", "z"])

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

    dtvec = DataTable[dt for _=1:3]
    show(io, dtvec)
    showall(io, dtvec)

    gd = groupby(dt, :A)
    show(io, gd)
    showall(io, gd)

    dtr = DataTableRow(dt, 1)
    show(io, dtr)

    dt = DataTable(A = Vector{String}(3))

    A = DataTables.StackedVector(Any[[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    show(io, A)
    A = DataTables.RepeatedVector([1, 2, 3], 5, 1)
    show(io, A)
    A = DataTables.RepeatedVector([1, 2, 3], 1, 5)
    show(io, A)

    #Test show output for REPL and similar
    dt = DataTable(Fish = ["Suzy", "Amir"], Mass = [1.5, Nullable()],
                   E = NullableCategoricalArray(["a", Nullable()]))
    io = IOBuffer()
    show(io, dt)
    str = String(take!(io))
    @test str == """
    2×3 DataTables.DataTable
    │ Row │ Fish │ Mass  │ E     │
    ├─────┼──────┼───────┼───────┤
    │ 1   │ Suzy │ 1.5   │ a     │
    │ 2   │ Amir │ #NULL │ #NULL │"""

    # Test computing width for Array{String} columns
    dt = DataTable(Any[["a"]], [:x])
    io = IOBuffer()
    show(io, dt)
    str = String(take!(io))
    @test str == """
    1×1 DataTables.DataTable
    │ Row │ x │
    ├─────┼───┤
    │ 1   │ a │"""
end
