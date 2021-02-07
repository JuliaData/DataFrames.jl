using CategoricalArrays
using CSV
using DataFrames
using Dates
using PooledArrays
using Random

function run_innerjoin_tests(warmup::Bool = false)
    warmup || run_innerjoin_tests(true)
    Random.seed!(1234);
    @info warmup ? "warmup" : "testing performance"
    df = DataFrame(llen=[], rlen=[], type=[], time=[], alloc=[], gc=[])
    # change line below to match
    # your preferred range of values tested
    # and memory availability in your system
    for llen in [10^3, 10^6], rlen in [2*10^7]
        if warmup
            llen, rlen = 1000, 1000
        else
            println("\nSize:")
            @show llen, rlen
        end
        println()
        warmup || @info "sorted string unique"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(string.(1:llen)), copycols=false)
        df2 = DataFrame(id = sort!(string.(1:rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted string unique", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted string unique", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled string unique"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = shuffle!(string.(1:llen)), copycols=false)
        df2 = DataFrame(id = shuffle!(string.(1:rlen)), copycols=false)
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled string unique", x.time, x.bytes, x.gctime])
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled string unique", x.time, x.bytes, x.gctime])
        warmup || @info "sorted string duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(rand(string.(1:llen), llen)), copycols=false)
        df2 = DataFrame(id = sort!(rand(string.(1:rlen), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted string duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted string duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "sorted string duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(rand(string.(1:llen ÷ 100), llen)), copycols=false)
        df2 = DataFrame(id = sort!(rand(string.(1:rlen ÷ 100), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted string duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted string duplicates many", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled string duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = rand(string.(1:llen), llen), copycols=false)
        df2 = DataFrame(id = rand(string.(1:rlen), rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled string duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled string duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled string duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = rand(string.(1:llen ÷ 100), llen), copycols=false)
        df2 = DataFrame(id = rand(string.(1:rlen ÷ 100), rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled string duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled string duplicates many", x.time, x.bytes, x.gctime])

        warmup || @info "sorted int unique"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(1:llen), copycols=false)
        df2 = DataFrame(id = sort!(1:rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted int unique", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted int unique", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled int unique"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = shuffle(1:llen), copycols=false)
        df2 = DataFrame(id = shuffle(1:rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled int unique", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled int unique", x.time, x.bytes, x.gctime])
        warmup || @info "sorted int duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(rand(1:llen, llen)), copycols=false)
        df2 = DataFrame(id = sort!(rand(1:rlen, rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted int duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted int duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "sorted int duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = sort!(rand(1:llen ÷ 100, llen)), copycols=false)
        df2 = DataFrame(id = sort!(rand(1:rlen ÷ 100, rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted int duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted int duplicates many", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled int duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = rand(1:llen, llen), copycols=false)
        df2 = DataFrame(id = rand(1:rlen, rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled int duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled int duplicates", x.time, x.bytes, x.gctime])

        warmup || @info "shuffled int duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = rand(1:llen ÷ 100, llen), copycols=false)
        df2 = DataFrame(id = rand(1:rlen ÷ 100, rlen), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled int duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled int duplicates many", x.time, x.bytes, x.gctime])

        warmup || @info "sorted PooledArray duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = PooledArray(rand(string.(1:llen), llen)), copycols=false)
        df2 = DataFrame(id = PooledArray(repeat(string.(1:rlen ÷ 10), inner=10)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted PooledArray duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted PooledArray duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "sorted PooledArray duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = PooledArray(rand(string.(1:llen ÷ 100), llen)), copycols=false)
        df2 = DataFrame(id = PooledArray(repeat(string.(1:rlen ÷ 100), inner=10)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted PooledArray duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted PooledArray duplicates many", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled PooledArray duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = PooledArray(rand(string.(1:llen), llen)), copycols=false)
        df2 = DataFrame(id = PooledArray(rand(string.(1:rlen ÷ 10), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled PooledArray duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled PooledArray duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled PooledArray duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = PooledArray(rand(string.(1:llen ÷ 100), llen)), copycols=false)
        df2 = DataFrame(id = PooledArray(rand(string.(1:rlen ÷ 100), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled PooledArray duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled PooledArray duplicates many", x.time, x.bytes, x.gctime])

        warmup || @info "sorted CategoricalArray duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = categorical(rand(string.(1:llen), llen)), copycols=false)
        df2 = DataFrame(id = categorical(repeat(string.(1:rlen ÷ 10), inner=10)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted CategoricalArray duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted CategoricalArray duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "sorted CategoricalArray duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = categorical(rand(string.(1:llen ÷ 100), llen)), copycols=false)
        df2 = DataFrame(id = categorical(repeat(string.(1:rlen ÷ 100), inner=10)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "sorted CategoricalArray duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "sorted CategoricalArray duplicates many", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled CategoricalArray duplicates"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = categorical(rand(string.(1:llen), llen)), copycols=false)
        df2 = DataFrame(id = categorical(rand(string.(1:rlen ÷ 10), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled CategoricalArray duplicates", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled CategoricalArray duplicates", x.time, x.bytes, x.gctime])
        warmup || @info "shuffled CategoricalArray duplicates many"
        df1, df2 = nothing, nothing
        df1 = DataFrame(id = categorical(rand(string.(1:llen ÷ 100), llen)), copycols=false)
        df2 = DataFrame(id = categorical(rand(string.(1:rlen ÷ 100), rlen)), copycols=false)
        GC.gc()
        x = @timed innerjoin(df1, df2, on=:id)
        push!(df, [llen, rlen, "shuffled CategoricalArray duplicates many", x.time, x.bytes, x.gctime])
        GC.gc()
        x = @timed innerjoin(df2, df1, on=:id)
        push!(df, [llen, rlen, "shuffled CategoricalArray duplicates many", x.time, x.bytes, x.gctime])

        warmup && break
    end
    return df
end

res = run_innerjoin_tests()
CSV.write("results_$(now()).csv", res)
