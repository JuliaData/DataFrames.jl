using Base.Test
using DataFrames

let
	dv = dataones(3)
	@assert isequal(dv, padNA(dv, 0, 0))
	@assert length(padNA(dv, 2, 0)) == length(dv) + 2
	@assert length(padNA(dv, 0, 2)) == length(dv) + 2
	@assert length(padNA(dv, 2, 2)) == length(dv) + 4
end
