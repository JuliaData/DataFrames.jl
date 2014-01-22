module TestIteration
	using Base.Test
	using DataArrays
	using DataFrames

	dv = @data([1, 2, NA])
	dm = DataArray([1 2; 3 4])
	dt = DataArray(zeros(2, 2, 2))

	df = DataFrame(A = 1:2, B = 2:3)

	for el in dv
	    @assert ndims(el) == 0
	end

	for el in dm
	    @assert ndims(el) == 0
	end

	for el in dt
	    @assert ndims(el) == 0
	end

	for row in eachrow(df)
	    @assert isa(row, DataFrameRow)
	    @assert row["B"]-row["A"] == 1
	end

	for col in eachcol(df)
	    @assert isa(col, AbstractDataVector)
	end

	@assert isequal(map(x -> minimum(array(x)), eachrow(df)), {1,2})
	@assert isequal(map(minimum, eachcol(df)), DataFrame(A = [1], B = [2]))

	row = DataFrameRow(df, 1)

	row["A"] = 100
	@assert df[1, "A"] == 100

	row[1] = 101
	@assert df[1, "A"] == 101

        df = DataFrame(A = 1:4, B = ["M", "F", "F", "M"])

        s1 = sub(df, 1:3)
        s1[2,"A"] = 4
        @assert df[2, "A"] == 4
        @assert sub(s1, 1:2) == sub(df, 1:2)

        s2 = sub(df, 1:2:3)
        s2[2, "B"] = "M"
        @assert df[3, "B"] == "M"
        @assert sub(s2, 1:1:2) == sub(df, [1,3])

	# @test_fail for x in df; end # Raises an error
end
