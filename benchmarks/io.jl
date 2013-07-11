N = 10
filenames = readdir(joinpath("test", "data"))
for filename in filenames
	full_filename = Pkg.dir("DataFrames", "test", "data", filename)
	df = benchmark(() -> read_table(full_filename),
	                                "DataFrame I/O",
	                                filename,
	                                N)
	# TODO: Keep permanent record
	printtable(STDOUT, df, ',', '"', false)
end
