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
			@pdata([true]),
			@pdata([1]),
			@pdata([1.0]),
			@pdata(["A"]),
			PooledDataArray([:A])}

	for ind in inds
		if isequal(ind, "A") || isequal(ind, :A) || ndims(ind) == 0
			@assert isequal(i[ind], 1)
		else
			@assert (i[ind] == [1])
		end
	end
end
