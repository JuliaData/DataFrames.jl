i = Index()
push(i, "A")
push(i, "B")

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
		DataVec[true],
		DataVec[1],
		DataVec[1.0],
		DataVec["A"],
		DataVec[:A],
		PooledDataVec[true],
		PooledDataVec[1],
		PooledDataVec[1.0],
		PooledDataVec["A"],
		PooledDataVec[:A]}

for ind in inds
	if isequal(ind, "A") || isequal(ind, :A) || ndims(ind) == 0
		@assert isequal(i[ind], 1)
	else
		@assert isequal(i[ind], [1])
	end
end
