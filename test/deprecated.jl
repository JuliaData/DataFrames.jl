module TestDeprecated
    using Compat, Compat.Test, DataFrames
    using DataFrames: Index

    i = Index()

    push!(i, :A)
    push!(i, :B)

    inds = Any[1.0, true, false,
               Any[1, missing], [1, missing],
               [true, missing], Any[true, missing],
               [:A, missing], Any[:A, missing],
               1.0:1.0, [1.0], Any[1.0]]
    for ind in inds
        if ind == :A || ndims(ind) == 0
            @test i[ind] == 1
        else
            @test (i[ind] == [1])
        end
    end

    # deprecation warning for automatically generating dup column name from indexing
    x = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    v = DataFrame(a = [5, 6, 7], b = [8, 9, 10])
    z = vcat(v, x)
    z2 = z[:, [1, 1, 2]]
    @test names(z2) == [:a, :a_1, :b]

    # TODO: uncomment the line below after deprecation, and move to dataframe.jl
    # @test_throws ArgumentError z[:, [1, 1, 2]]
end
