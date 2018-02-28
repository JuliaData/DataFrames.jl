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
end
