module TestFWF
	using Base.Test
	using DataFrames

	original = DataFrame(A = [1.0, 2.0],
		                 B = ["foo", "b"],
		                 C = [1, 3])

	path = joinpath("test", "data", "fwf.bin")

	writefwfbin(path, original)

	reproduction = readfwfbin(path,
		                      [Float64, UTF8String, Int64],
		                      [8, 4, 8])

	# original == reproduction # TODO: Make this pass

	@test all(original .== reproduction)

	rm(path)
end
