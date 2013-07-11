filename = Pkg.dir("DataFrames", "test", "data", "big_data.csv")

minibatch_sizes = [1, 5, 25, 100, 1_000, 10_000]

for f in (colmeans, colvars, cor)
	for minibatch_size in minibatch_sizes
		ds = DataStream(filename, minibatch_size)
		N = 3
		df = benchmark(() -> apply(f, (ds,)),
			           "DataStream Functions",
			           join({
			           	      string(f),
			           	      "w/ minibatches of",
			           	      minibatch_size,
			           	      "rows"
			           	    }, " "),
			           N)
		# TODO: Keep permanent record
		printtable(STDOUT, df, ',', '"', false)
	end
end
