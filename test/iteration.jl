using Base.Test
using DataFrames

let
	dv = DataVector[1, 2, NA]
	dm = DataArray([1 2; 3 4])
	dt = datazeros(2, 2, 2)

	df = DataFrame(quote
	                 A = 1:2
	                 B = 2:3
	               end)

	for el in dv
	    @assert ndims(el) == 0
	end

	for el in dm
	    @assert ndims(el) == 0
	end

	for el in dt
	    @assert ndims(el) == 0
	end

	for row in EachRow(df)
	    @assert isa(row, DataFrame)
	end

	for col in EachCol(df)
	    @assert isa(col, AbstractDataVector)
	end

	@assert isequal(map(x -> minimum(matrix(x)), EachRow(df)), {1,2})
	@assert isequal(map(minimum, EachCol(df)), DataFrame(quote A = 1; B = 2 end))

	# @test_fail for x in df; end # Raises an error
end
