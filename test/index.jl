using Base.Test
using DataFrames

let
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
			DataVector[true],
			DataVector[1],
			DataVector[1.0],
			DataVector["A"],
			DataVector[:A],
			PooledDataVector[true],
			PooledDataVector[1],
			PooledDataVector[1.0],
			PooledDataVector["A"],
			PooledDataVector[:A]}

	for ind in inds
		if isequal(ind, "A") || isequal(ind, :A) || ndims(ind) == 0
			@assert isequal(i[ind], 1)
		else
			@assert (i[ind] == [1])
		end
	end
end
