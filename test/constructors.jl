using Base.Test
using DataFrames

let
	#
	# DataFrame
	#

	df = DataFrame()
	@assert isequal(df.columns, {})
	# TODO: Get this to work
	#@assert isequal(df.colindex, Index())

	df = DataFrame({DataArray(zeros(3)), DataArray(ones(3))}, Index(["x1", "x2"]))
	@assert nrow(df) == 3
	@assert ncol(df) == 2

	# TODO: Make isequal fail if colnames don't match
	@assert isequal(df, DataFrame({DataArray(zeros(3)), DataArray(ones(3))}))
	@assert isequal(df, DataFrame(quote x1 = [0.0, 0.0, 0.0]; x2 = [1.0, 1.0, 1.0] end))

	@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0], ["x1", "x2"]))
	@assert isequal(df, DataFrame([0.0 1.0; 0.0 1.0; 0.0 1.0]))
	@assert isequal(df, DataFrame(DataArray(zeros(3)), DataArray(ones(3))))

	# TODO: Fill these in
	# From (Associative): ???
	# From (Vector, Vector, Groupings): ???

	@assert isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0],
		                           "x2" => [1.0, 1.0, 1.0]}))
	@assert isequal(df, DataFrame({"x1" => [0.0, 0.0, 0.0],
		                           "x2" => [1.0, 1.0, 1.0],
		                           "x3" => [2.0, 2.0, 2.0]},
		                          ["x1", "x2"]))

	df = DataFrame(Int, 2, 2)
	@assert size(df) == (2, 2)
	@assert all(coltypes(df) .== {Int, Int})
	@assert all(isna(df))

	df = DataFrame(2, 2)
	@assert size(df) == (2, 2)
	@assert all(coltypes(df) .== {Float64, Float64})
	@assert all(isna(df))

	df = DataFrame({Int, Float64}, ["x1", "x2"], 2)
	@assert size(df) == (2, 2)
	@assert all(coltypes(df) .== {Int, Float64})
	@assert all(isna(df))

	@assert isequal(df, DataFrame({Int, Float64}, 2))
end
