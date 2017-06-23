module TestIndex
using Base.Test
using DataTables, DataTables.Index, Compat

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
           NullableArray([true]),
           NullableArray([1]),
           NullableArray([1.0]),
           NullableArray([:A]),
           NullableArray([:A])]

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
@test rename(i, @compat(Dict(:a=>:A, :b=>:B))) == Index([:A,:B])
@test rename(i, :a, :A) == Index([:A,:b])
@test rename(i, :a, :a) == Index([:a,:b])
@test rename(i, [:a], [:A]) == Index([:A,:b])
@test rename(i, [:a], [:a]) == Index([:a,:b])
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
dt = DataTable(A=[0],B=[0])
dt[1:end] = 0.0
dt[1,:A] = 1.0
@test dt[1,:B] === 0

end
