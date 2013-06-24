require("test.jl")
using DataFrames

let
	dv1 = DataVector[9,1,8,NA,3,3,7,NA]
	dv2 = 1.0 * dv1
	dv3 = DataArray([1:8])
	pdv1 = PooledDataArray(dv1)
	idv1 = IndexedVector(dv1)

	@assert sortperm(dv1) == sortperm(dv2)
	@assert sortperm(dv1) == sortperm(pdv1)
	@assert sortperm(dv1) == sortperm(idv1)
	@assert isequal(sort(dv1), DataArray(sort(dv1)))
	@assert isequal(sort(dv1), DataArray(sort(pdv1)))
	@assert isequal(sort(dv1), DataArray(sort(idv1)))

	d = @DataFrame(dv1 => dv1, dv2 => dv2, dv3 => dv3, pdv1 => pdv1, idv1 => idv1)

	@assert sortperm(d) == sortperm(dv1)
	@assert sortperm(d[["dv3","dv1"]]) == sortperm(dv3)
	@assert sortby(d, "dv1")["dv3"] == sortperm(dv1)
	@assert sortby(d, "dv2")["dv3"] == sortperm(dv1)
	@assert sortby(d, "pdv1")["dv3"] == sortperm(dv1)
	@assert sortby(d, "idv1")["dv3"] == sortperm(dv1)
	@assert sortby(d, ["dv1","pdv1"])["dv3"] == sortperm(dv1)
	@assert sortby(d, ["dv1","dv3"])["dv3"] == sortperm(dv1)
end
