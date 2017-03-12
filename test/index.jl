module TestIndex
using Base.Test
using DataFrames, DataFrames.Index, Compat

i = Index()
push!(i, :A)
push!(i, :B)

inds = Any[1,
           1.0,
           :A,
           [true],
           trues(1),
           [1],
           [1.0],
           1:1,
           1.0:1.0,
           [:A],
           @data([true]),
           @data([1]),
           @data([1.0]),
           @data([:A]),
           DataArray([:A]),
           PooledDataArray([true]),
           @pdata([1]),
           @pdata([1.0]),
           @pdata([:A]),
           PooledDataArray([:A])]

for ind in inds
    if isequal(ind, :A) || ndims(ind) == 0
        @test isequal(i[ind], 1)
    else
        @test (i[ind] == [1])
    end
end

@test names(i) == [:A,:B]
@test names!(i, [:a,:a], allow_duplicates=true) == Index([:a,:a_1])
@test_throws ArgumentError names!(i, [:a,:a]) 
@test names!(i, [:a,:b]) == Index([:a,:b])
@test rename(i, Dict(:a=>:A, :b=>:B)) == Index([:A,:B])
@test rename(i, :a, :A) == Index([:A,:b])
@test rename(i, [:a], [:A]) == Index([:A,:b])
# @test rename(i, uppercase) == Index([:A,:B])
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
