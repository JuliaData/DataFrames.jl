module TestMetaData
    using Compat, Compat.Test, DataFrames, StatsBase, Compat.Random
    using Suppressor
    using Compat: @warn

df1 = DataFrame(a = [1, 2], b = [3, 4])
df2 = DataFrame(c = [3, 4], d = [5, 6])

# Just used to add metadata easily for testing. 
metadata!(df, :a, :label, "A label for variable a")

testdata = DataFrame(variable = names(df1), label = 
    ["A label for variable a",
    nothing])

@test showmeta(df1) == testdata

mergeddata = merge!(df1, df2)
testmergeddata = DataFrame(variable = names(mergeddata,
    label = 
    ["A label for variable a",
    nothing, 
    nothing,
    nothing,
    nothing]))

@test showmeta(mergeddata) == testmergeddata

end # module TestMetaData
