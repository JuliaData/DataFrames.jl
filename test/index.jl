module TestIndex
using Base.Test
using DataArrays
using DataFrames

i = Index()
push!(i, "A")
push!(i, "B")

inds = {1,
        1.0,
        "A",
        :A,
        [true],
        trues(1),
        [1],
        [1.0],
        1:1,
        1.0:1.0,
        ["A"],
        [:A],
        @data([true]),
        @data([1]),
        @data([1.0]),
        @data(["A"]),
        DataArray([:A]),
        PooledDataArray([true]),
        @pdata([1]),
        @pdata([1.0]),
        @pdata(["A"]),
        PooledDataArray([:A])}

for ind in inds
    if isequal(ind, "A") || isequal(ind, :A) || ndims(ind) == 0
        @test isequal(i[ind], 1)
    else
        @test (i[ind] == [1])
    end
end

@test names(i) == ["A","B"]
@test names!(i, ["a","b"]) == Index(["a","b"])
@test rename(i, ["a"=>"A", "b"=>"B"]) == Index(["A","B"])
@test rename(i, "a", "A") == Index(["A","b"])
@test rename(i, ["a"], ["A"]) == Index(["A","b"])
@test rename(i, uppercase) == Index(["A","B"])
@test delete!(i, "a") == Index(["b"])
push!(i, "C")
@test delete!(i, 1) == Index(["C"])

end
