load("DataFrames")
using DataFrames

srand(1)
N = 10_000_000
v = randn(N)
dv = DataVec(v)
dvna = copy(dv)
dvna[randi(N, 10000)] = NA
idxv = shuffle([1:N])
idxdv = DataVec(idxv)

f1(v) = sum(v)
f2(v) = sum(removeNA(v))
f3(v) = sum(removeNA(v)) # Make this an iterator
f4(v) = mean(v)
f5(v) = mean(removeNA(v))
f6(v) = mean(removeNA(v)) # Make this an iterator
f7(v1,v2) = v1 + v2
f8(v1,v2) = v1 .> v2
f9(v,i) = v[i]

perf_test = Dict()

perf_test["sum(v): Vector with no NA's"] = () -> f1(v)
perf_test["sum(dv): DataVec with no NA's"] = () -> f1(dv)
perf_test["sum(removeNA(dv)): DataVec with no NA's"] = () -> f2(dv)
perf_test["sum(*removeNA(dv)): DataVec with no NA's"] = () -> f3(dv)

perf_test["sum(dvna): DataVec with NA's"] = () -> f4(dv)
perf_test["sum(removeNA(dvna)): DataVec with NA's"] = () -> f5(dv)
perf_test["sum(*removeNA(dvna)): DataVec with NA's"] = () -> f6(dv)

perf_test["mean(v): Vector with no NA's"] = () -> f4(v)
perf_test["mean(dv): DataVec with no NA's"] = () -> f4(dv)
perf_test["mean(removeNA(dv)): DataVec with no NA's"] = () -> f5(dv)
perf_test["mean(*removeNA(dv)): DataVec with no NA's"] = () -> f6(dv)

perf_test["mean(dvna): DataVec with NA's"] = () -> f4(dv)
perf_test["mean(removeNA(dvna)): DataVec with NA's"] = () -> f5(dv)
perf_test["mean(*removeNA(dvna)): DataVec with NA's"] = () -> f6(dv)

perf_test["v + 1.0 : Vector"] = () -> f7(v, 1.0)
perf_test["dv + 1.0 : DataVec with no NA's"] = () -> f7(dv, 1.0)
perf_test["dvna + 1.0 : DataVec with NA's"] = () -> f7(dvna, 1.0)

perf_test["v .> 1.0 : Vector"] = () -> f8(v, 1.0)
perf_test["dv .> 1.0 : DataVec with no NA's"] = () -> f8(dv, 1.0)
perf_test["dvna .> 1.0 : DataVec with NA's"] = () -> f8(dvna, 1.0)

perf_test["v[idxv] : Vector"] = () -> f9(v, idxv)
perf_test["dv[idxv] : DataVec and Vector indexing"] = () -> f9(dv, idxv)
perf_test["dv[idxdv] : DataVec and DataVec indexing"] = () -> f9(dv, idxdv)

tm = strftime("%Y-%m-%d %H:%M:%S", int(time()))

for (name, f) in perf_test
    for i in 1:3     # do each three times
        res = try
            @elapsed f()
        catch
            NA
        end
        println(name, ", ", res, ", ", tm) 
    end
end

# We could write what's above to a file periodically. Then, we could
# write a DataStream to iterate over the files. It would make a good
# test case.
