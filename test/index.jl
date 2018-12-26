module TestIndex
using Test, DataFrames
using DataFrames: Index, SubIndex

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

@test_throws MethodError i[1.0]
@test_throws ArgumentError i[true]
@test_throws ArgumentError i[false]
@test_throws ArgumentError i[Union{Bool, Missing}[true, false]]
@test_throws ArgumentError i[Any[1, missing]]
@test_throws ArgumentError i[[1, missing]]
@test_throws ArgumentError i[[true, missing]]
@test_throws ArgumentError i[Any[true, missing]]
@test_throws MethodError i[[:A, missing]]
@test_throws MethodError i[Any[:A, missing]]
@test_throws ArgumentError i[1.0:1.0]
@test_throws ArgumentError i[[1.0]]
@test_throws ArgumentError i[Any[1.0]]

@test i[1:1] == 1:1

@test_throws BoundsError i[[true]]
@test_throws BoundsError i[true:true]
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

i = Index([:A, :B, :C, :D, :E])
si1 = SubIndex(i, :)
si2 = SubIndex(i, 3:5)
si3 = SubIndex(i, [3,4,5])
si4 = SubIndex(i, [false, false, true, true, true])
si5 = SubIndex(i, [:C, :D, :E])

@test_throws ArgumentError SubIndex(i, 1)
@test_throws ArgumentError SubIndex(i, :A)
@test_throws ArgumentError SubIndex(i, true)
@test si1 isa Index
@test si2.cols == 3:5
@test si2.remap == -1:3
@test si3.cols == 3:5
@test si3.remap == Int[]
@test !haskey(si3, :A)
@test si3.remap == [0, 0, 1, 2, 3]
@test si4.cols == 3:5
@test si4.remap == Int[]
@test !haskey(si4, :A)
@test si4.remap == [0, 0, 1, 2, 3]
@test si5.cols == 3:5
@test si5.remap == Int[]
@test !haskey(si5, :A)
@test si5.remap == [0, 0, 1, 2, 3]

@test length(si1) == 5
@test length(si2) == 3
@test length(si3) == 3
@test length(si4) == 3
@test length(si5) == 3

@test names(si1) == keys(si1) == [:A, :B, :C, :D, :E]
@test names(si2) == keys(si2) == [:C, :D, :E]
@test names(si3) == keys(si3) == [:C, :D, :E]
@test names(si4) == keys(si4) == [:C, :D, :E]
@test names(si5) == keys(si5) == [:C, :D, :E]

@test_throws ArgumentError haskey(si3, true)
@test haskey(si3, 1)
@test !haskey(si3, 0)
@test !haskey(si3, 4)
@test haskey(si3, :D)
@test !haskey(si3, :A)
@test si3[:C] == 1
@test si3[names(i)] == [0, 0, 1, 2, 3]

end
