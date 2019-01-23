module TestSort

using DataFrames, Random, Test

dv1 = [9, 1, 8, missing, 3, 3, 7, missing]
dv2 = [9, 1, 8, missing, 3, 3, 7, missing]
dv3 = Vector{Union{Int, Missing}}(1:8)
cv1 = CategoricalArray(dv1, ordered=true)

d = DataFrame(dv1 = dv1, dv2 = dv2, dv3 = dv3, cv1 = cv1)

@test sortperm(d) == sortperm(dv1)
@test sortperm(d[[:dv3, :dv1]]) == sortperm(dv3)
@test sort(d, :dv1)[:dv3] == sortperm(dv1)
@test sort(d, :dv2)[:dv3] == sortperm(dv1)
@test sort(d, :cv1)[:dv3] == sortperm(dv1)
@test sort(d, [:dv1, :cv1])[:dv3] == sortperm(dv1)
@test sort(d, [:dv1, :dv3])[:dv3] == sortperm(dv1)
@test sort(d, (:dv1, :cv1))[:dv3] == sortperm(dv1)
@test sort(d, (:dv1, :dv3))[:dv3] == sortperm(dv1)

df = DataFrame(rank=rand(1:12, 1000),
               chrom=rand(1:24, 1000),
               pos=rand(1:100000, 1000))

@test issorted(sort(df))
@test issorted(sort(df, rev=true), rev=true)
@test issorted(sort(df, [:chrom,:pos])[[:chrom,:pos]])

ds = sort(df, (order(:rank, rev=true),:chrom,:pos))
@test issorted(ds, (order(:rank, rev=true),:chrom,:pos))
@test issorted(ds, rev=(true, false, false))

ds2 = sort(df, (:rank, :chrom, :pos), rev=(true, false, false))
@test issorted(ds2, (order(:rank, rev=true), :chrom, :pos))
@test issorted(ds2, rev=(true, false, false))

@test ds2 == ds

sort!(df, (:rank, :chrom, :pos), rev=(true, false, false))
@test issorted(df, (order(:rank, rev=true), :chrom, :pos))
@test issorted(df, rev=(true, false, false))

@test df == ds

df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
@test !issorted(df, :x)
@test issorted(sort(df, :x), :x)

x = DataFrame(a=1:3,b=3:-1:1,c=3:-1:1)
@test issorted(x)
@test !issorted(x, [:b,:c])
@test !issorted(x[2:3], [:b,:c])
@test issorted(sort(x,[2,3]), [:b,:c])
@test issorted(sort(x[2:3]), [:b,:c])

# Check that columns that shares the same underlying array are only permuted once PR#1072
df = DataFrame(a=[2,1])
df[:b] = df[:a]
sort!(df, :a)
@test df == DataFrame(a=[1,2],b=[1,2])

x = DataFrame(x=[1,2,3,4], y=[1,3,2,4])
sort!(x, :y)
@test x[:y] == [1,2,3,4]
@test x[:x] == [1,3,2,4]

@test_throws ArgumentError sort(x, by=:x)

Random.seed!(1)
# here there will be probably no ties
df_rand1 = DataFrame(rand(100, 4))
# but here we know we will have ties
df_rand2 = df_rand1[:]
df_rand2[:x1] = shuffle([fill(1, 50); fill(2, 50)])
df_rand2[:x4] = shuffle([fill(1, 50); fill(2, 50)])

# test sorting by 1 column
for df_rand in [df_rand1, df_rand2]
    # testing sort
    for n1 in names(df_rand)
        # passing column name
        @test sort(df_rand, n1) == df_rand[sortperm(df_rand[n1]),:]
        # passing vector with one column name
        @test sort(df_rand, [n1]) == df_rand[sortperm(df_rand[n1]),:]
        # passing vector with two column names
        for n2 in setdiff(names(df_rand), [n1])
            @test sort(df_rand, [n1,n2]) == df_rand[sortperm(collect(zip(df_rand[n1],
                                                                         df_rand[n2]))),:]
        end
    end
    # testing if sort! is consistent with issorted and sort
    ref_df = df_rand[:]
    for n1 in names(df_rand)
        @test sort!(df_rand, n1) == sort(ref_df, n1)
        @test issorted(df_rand, n1)
        @test sort!(df_rand, [n1]) == sort(ref_df, [n1])
        @test issorted(df_rand, [n1])
        for n2 in setdiff(names(df_rand), [n1])
            @test sort!(df_rand, [n1, n2]) == sort(ref_df, [n1, n2])
            @test issorted(df_rand, n1)
            @test issorted(df_rand, [n1, n2])
        end
    end
end

end # module
