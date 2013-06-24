require("test.jl")
using DataFrames

let
	d = dataeye(3, 3)
	d[1, 1] = NA

	svd(d)
end
