N = 10
filenames = readdir(joinpath("test", "data"))
for filename in filenames
	full_filename = Pkg.dir("DataFrames", "test", "data", filename)
	df = benchmark(() -> readtable(full_filename),
	                                "DataFrame I/O",
	                                filename,
	                                N)
	# TODO: Keep permanent record
	printtable(df, header=false)
end
