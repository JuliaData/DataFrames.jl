N = 10
filenames = readdir(joinpath("test", "data"))
for filename in filenames
	full_filename = joinpath(julia_pkgdir(),
		                     "DataFrames",
	                         "test",
	                         "data",
	                         filename)
	df = benchmark(() -> read_table(full_filename),
	                                "DataFrame I/O",
	                                filename,
	                                N)
	# TODO: Keep permanent record
	print_table(stdout_stream, df, ',', '"', false)
end
