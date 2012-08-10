srand(1)
N = 10000000
v = randn(N)
dv = DataVec(v)
dvna = copy(dv)
dvna[randi(N, 10000)] = NA

f1(v) = sum(v)
f2(v) = sum(nafilter(v))
f3(v) = sum(naFilter(v))
f4(v) = mean(v)
f5(v) = mean(nafilter(v))
f6(v) = mean(naFilter(v))

perf_test = Dict()

perf_test["sum(v): Vector with no NA's"] = () -> f1(v)
perf_test["sum(dv): DataVec with no NA's"] = () -> f1(dv)
perf_test["sum(nafilter(dv)): DataVec with no NA's"] = () -> f2(dv)
perf_test["sum(naFilter(dv)): DataVec with no NA's"] = () -> f3(dv)

perf_test["sum(dvna): DataVec with NA's"] = () -> f4(dv)
perf_test["sum(nafilter(dvna)): DataVec with NA's"] = () -> f5(dv)
perf_test["sum(naFilter(dvna)): DataVec with NA's"] = () -> f6(dv)

perf_test["mean(v): Vector with no NA's"] = () -> f4(v)
perf_test["mean(dv): DataVec with no NA's"] = () -> f4(dv)
perf_test["mean(nafilter(dv)): DataVec with no NA's"] = () -> f5(dv)
perf_test["mean(naFilter(dv)): DataVec with no NA's"] = () -> f6(dv)

perf_test["mean(dvna): DataVec with NA's"] = () -> f4(dv)
perf_test["mean(nafilter(dvna)): DataVec with NA's"] = () -> f5(dv)
perf_test["mean(naFilter(dvna)): DataVec with NA's"] = () -> f6(dv)


for (name, f) in perf_test
    for i in 1:3     # do each three times
        res = try
            @elapsed f()
        catch
            NA
        end
        println(name, ", ", res, ", ", strftime("%Y-%m-%d %H:%M:%S",int(time())))
    end
end


# We could write what's above to a file periodically. Then, we could
# write a DataStream to iterate over the files. It would make a good
# test case.





