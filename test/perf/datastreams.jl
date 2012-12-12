load("DataFrames")
using DataFrames

filename = file_path(julia_pkgdir(),"DataFrames/test/data/big_data.csv")

minibatch_sizes = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000]

for (f) in (:colmeans, :colmins, :colmaxs, :colsums, :colprods, :colvars, :colstds, :cov, :cor)
	@eval begin
		for minibatch_size in minibatch_sizes
			ds = DataStream(filename, minibatch_size)
			N = 10
			res = ($f)(ds)
			total_time = @elapsed for iter in 1:N
			    res = ($f)(ds)
			end
			println(join({"$(string($f)) from a FileDataStream", N, total_time / N, minibatch_size}, "\t"))
		end
	end
end
