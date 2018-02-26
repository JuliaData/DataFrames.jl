module TestIndex
using Compat, Compat.Test, DataFrames, DataFrames.Index

i = Index()
push!(i, :A)
push!(i, :B)

inds = Any[1,
           big(1),
           :A,
           [true, false],
           [1],
           [big(1)],
           big(1):big(1),
           [:A],
           Union{Bool, Missing}[true, false],
           Union{Int, Missing}[1],
           Union{BigInt, Missing}[big(1)],
           Union{Symbol, Missing}[:A],
           Any[1],
           Any[:A]]

for ind in inds
    if ind == :A || ndims(ind) == 0
        @test i[ind] == 1
    else
        @test (i[ind] == [1])
    end
end

# TODO: add tests that above indices throw error after deprecation period
# [1.0,
# 1.0:1.0,
# [true],
# [false],
# true,
# false,
# Any[1, missing],
# Any[true, missing],
# Any[:A, missing],
# [true, missing]]

@test i[1:1] == 1:1

@test_throws BoundsError i[[true]]
@test_throws BoundsError i[[true, false, true]]

@test_throws ArgumentError i[["a"]]
@test_throws ArgumentError i[Any["a"]]

@test i[[]] == Int[]
@test i[Int[]] == Int[]
@test i[Symbol[]] == Int[]

@test names(i) == [:A,:B]
@test names!(i, [:a,:a], makeunique=true) == Index([:a,:a_1])
@test_throws ArgumentError names!(i, [:a,:a])
@test names!(i, [:a,:b]) == Index([:a,:b])
@test rename(i, Dict(:a=>:A, :b=>:B)) == Index([:A,:B])
@test rename(i, :a => :A) == Index([:A,:b])
@test rename(i, :a => :a) == Index([:a,:b])
@test rename(i, [:a => :A]) == Index([:A,:b])
@test rename(i, [:a => :a]) == Index([:a,:b])
@test rename(x->Symbol(uppercase(string(x))), i) == Index([:A,:B])
@test rename(x->Symbol(lowercase(string(x))), i) == Index([:a,:b])

@test delete!(i, :a) == Index([:b])
push!(i, :C)
@test delete!(i, 1) == Index([:C])

i = Index([:A, :B, :C, :D, :E])
i2 = copy(i)
names!(i2, reverse(names(i2)))
names!(i2, reverse(names(i2)))
@test names(i2) == names(i)
for name in names(i)
  i2[name] # Issue #715
end

#= Aliasing & Mutation =#

# columns should not alias if scalar broadcasted
df = DataFrame(A=[0],B=[0])
df[1:end] = 0.0
df[1,:A] = 1.0
@test df[1,:B] === 0

end
