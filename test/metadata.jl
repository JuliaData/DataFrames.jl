module TestMetaData
    using Compat, Compat.Test, DataFrames, StatsBase, Compat.Random
    using Suppressor
    using Compat: @warn

io = IOBuffer()

df1 = DataFrame(a = [1, 2], b = [3, 4])
df2 = DataFrame(c = [3, 4], d = [5, 6])

# Just used to add metadata easily for testing. 
function addlabels!(df::DataFrame)
    for name in names(df)
        addlabel!(df, name, "Variable label for variable $name")
    end
end


addlabels!(df1)
addlabels!(df2)

str = @capture_out showlabels(df1)
@test str ==
"Variable label for a:
\tVariable label for variable a
Variable label for b:
\tVariable label for variable b\n"

merge!(df1, df2)
str = @capture_out showlabels(df1)
@test str == 
"Variable label for a:
\tVariable label for variable a
Variable label for b:
\tVariable label for variable b
Variable label for c:
\tVariable label for variable c
Variable label for d:
\tVariable label for variable d\n" 



end # module TestMetaData
