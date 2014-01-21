module TestShow
    using DataArrays
    using DataFrames
    using RDatasets

    iris = data("datasets", "iris")
    iris = iris[1:50, :]

    io = IOBuffer()
    show(io, iris)
    show(io, iris, true)
    showall(io, iris)
    showall(io, iris, true)

    subdf = select(:(SepalLength .> 4.0), iris)
    show(io, subdf)
    show(io, subdf, true)
    showall(io, subdf)
    showall(io, subdf, true)

    df = DataFrame(A = Array(UTF8String, 3))
end
