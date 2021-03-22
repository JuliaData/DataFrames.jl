using BenchmarkTools
using DataFrames
using PooledArrays
using Random

@show Threads.nthreads()

Random.seed!(1234)
ref_dfi = DataFrame(rand(1:10^4, 10^7, 4), :auto)
ref_dfs = string.(ref_dfi)
ref_dfp = mapcols(PooledArray, ref_dfs)

res = DataFrame(rows=Int[],cols=Int[], type=String[], op=String[], time=Float64[])

for x in (10, 10^6-1, 10^6, 10^7), y in 1:4
    dfi = ref_dfi[1:x, 1:y]
    dfs = ref_dfs[1:x, 1:y]
    dfp = ref_dfp[1:x, 1:y]

    @show (x, y) # ping that the process is alive
    push!(res, (x, y, "integer", "copy", @belapsed DataFrame($dfi)))
    push!(res, (x, y, "string", "copy", @belapsed DataFrame($dfs)))
    push!(res, (x, y, "pooled", "copy", @belapsed DataFrame($dfp)))
    push!(res, (x, y, "integer", ":", @belapsed $dfi[:, :]))
    push!(res, (x, y, "string", ":", @belapsed $dfs[:, :]))
    push!(res, (x, y, "pooled", ":", @belapsed $dfp[:, :]))
    push!(res, (x, y, "integer", "1:end-5", @belapsed $dfi[1:end-5, :]))
    push!(res, (x, y, "string", "1:end-5", @belapsed $dfs[1:end-5, :]))
    push!(res, (x, y, "pooled", "1:end-5", @belapsed $dfp[1:end-5, :]))
    push!(res, (x, y, "integer", "1:5", @belapsed $dfi[1:5, :]))
    push!(res, (x, y, "string", "1:5", @belapsed $dfs[1:1:5, :]))
    push!(res, (x, y, "pooled", "1:5", @belapsed $dfp[1:1:5, :]))
end

res.time *= 1_000

@show Threads.nthreads()
@show unstack(res, [:cols, :type, :op], :rows, :time)
