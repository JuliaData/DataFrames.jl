filename = joinpath(julia_pkgdir(),
                    "DataFrames",
                    "test",
                    "data",
                    "big_data.csv")

minibatch_sizes = [1, 5, 25, 100, 1_000, 10_000]

for f in (colmeans, colvars, cor)
	for minibatch_size in minibatch_sizes
		ds = DataStream(filename, minibatch_size)
		N = 10
		df = benchmark(f,
			           "DataStream Functions",
			           join({
			           	      string(f),
			           	      "w/ minibatches of",
			           	      minibatch_size,
			           	      "rows"
			           	    }, " "),
			           N)
		# TODO: Keep permanent record
		print_table(stdout_stream, df, false, ',', '"', false)
	end
end
