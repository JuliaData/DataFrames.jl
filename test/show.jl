module TestShow
    using DataFrames
    df = DataFrame(A = 1:3, B = ["x", "y", "z"])

    io = IOBuffer()
    show(io, df)
    show(io, df, true)
    showall(io, df)
    showall(io, df, true)

    subdf = df[df[:A] .> 1.0, :]
    show(io, subdf)
    show(io, subdf, true)
    showall(io, subdf)
    showall(io, subdf, true)

    dfr = DataFrameRow(df, 1)
    show(io, dfr)

    df = DataFrame(A = Array(UTF8String, 3))

    A = DataFrames.StackedVector({[1, 2, 3], [4, 5, 6], [7, 8, 9]})
    show(io, A)
    A = DataFrames.RepeatedVector([1, 2, 3], 5)
    show(io, A)
    A = DataFrames.EachRepeatedVector([1, 2, 3], 5)
    show(io, A)
end
