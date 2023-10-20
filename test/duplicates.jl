module TestDuplicates

using Test, DataFrames, CategoricalArrays, Random, PooledArrays
const ≅ = isequal

@testset "nonunique" begin
    df = DataFrame(a=[1, 2, 3, 3, 4])
    udf = DataFrame(a=[1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    unique!(df)
    @test df == udf
    @test_throws ArgumentError unique(df, true)

    pdf = DataFrame(a=CategoricalArray(["a", "a", missing, missing, "b", missing, "a", missing]),
                    b=CategoricalArray(["a", "b", missing, missing, "b", "a", "a", "a"]))
    updf = DataFrame(a=CategoricalArray(["a", "a", missing, "b", missing]),
                    b=CategoricalArray(["a", "b", missing, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf ≅ unique(pdf)
    unique!(pdf)
    @test pdf ≅ updf
    @test_throws ArgumentError unique(pdf, true)

    df = view(DataFrame(a=[1, 2, 3, 3, 4]), :, :)
    udf = DataFrame(a=[1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    @test_throws ArgumentError unique!(df)
    @test_throws ArgumentError unique(df, true)

    pdf = view(DataFrame(a=CategoricalArray(["a", "a", missing, missing, "b", missing, "a", missing]),
                         b=CategoricalArray(["a", "b", missing, missing, "b", "a", "a", "a"])), :,  :)
    updf = DataFrame(a=CategoricalArray(["a", "a", missing, "b", missing]),
                     b=CategoricalArray(["a", "b", missing, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf ≅ unique(pdf)
    @test_throws ArgumentError unique!(pdf)
    @test_throws ArgumentError unique(pdf, true)

    @test isempty(nonunique(DataFrame(a=PooledArray(Int[]))))
    @test typeof(nonunique(DataFrame(a=PooledArray(Int[])))) === Vector{Bool}
end

@testset "nonunique, nonunique, unique! with extra argument" begin
    df1 = DataFrame(a=Union{String, Missing}["a", "b", "a", "b", "a", "b"],
                    b=Vector{Union{Int, Missing}}(1:6),
                    c=Union{Int, Missing}[1:3;1:3])
    df = vcat(df1, df1)
    @test findall(nonunique(df)) == collect(7:12)
    @test findall(nonunique(df, :)) == collect(7:12)
    @test findall(nonunique(df, Colon())) == collect(7:12)
    @test findall(nonunique(df, :a)) == collect(3:12)
    @test findall(nonunique(df, "a")) == collect(3:12)
    @test findall(nonunique(df, [:a, :c])) == collect(7:12)
    @test findall(nonunique(df, ["a", "c"])) == collect(7:12)
    @test findall(nonunique(df, r"[ac]")) == collect(7:12)
    @test findall(nonunique(df, Not(2))) == collect(7:12)
    @test findall(nonunique(df, Not([2]))) == collect(7:12)
    @test findall(nonunique(df, Not(:b))) == collect(7:12)
    @test findall(nonunique(df, Not([:b]))) == collect(7:12)
    @test findall(nonunique(df, Not([false, true, false]))) == collect(7:12)
    @test findall(nonunique(df, [1, 3])) == collect(7:12)
    @test findall(nonunique(df, 1)) == collect(3:12)
    @test findall(nonunique(df, :a => x -> 1)) == 2:12

    @test unique(df) == df1
    @test unique(df, :) == df1
    @test unique(df, Colon()) == df1
    @test unique(df, 2:3) == df1
    @test unique(df, 3) == df1[1:3, :]
    @test unique(df, [1, 3]) == df1
    @test unique(df, [:a, :c]) == df1
    @test unique(df, ["a", "c"]) == df1
    @test unique(df, r"[ac]") == df1
    @test unique(df, Not(2)) == df1
    @test unique(df, Not([2])) == df1
    @test unique(df, Not(:b)) == df1
    @test unique(df, Not([:b])) == df1
    @test unique(df, Not([false, true, false])) == df1
    @test unique(df, :a) == df1[1:2, :]
    @test unique(df, "a") == df1[1:2, :]
    @test unique(df, :a => x -> 1) == df[1:1, :]
    @test unique(DataFrame()) == DataFrame()
    @test isempty(nonunique(DataFrame())) && nonunique(DataFrame()) isa Vector{Bool}
    @test_throws ArgumentError nonunique(DataFrame(a=1:3), [])
    @test_throws ArgumentError unique(DataFrame(a=1:3), [])

    @test unique(copy(df1), "a") == unique(copy(df1), :a) == unique(copy(df1), 1) ==
          df1[1:2, :]

    unique!(df, [1, 3])
    @test df == df1
    for cols in (r"[ac]", Not(:b), Not(2), Not([:b]), Not([2]), Not([false, true, false]))
        df = vcat(df1, df1)
        unique!(df, cols)
        @test df == df1
    end
end

@testset "keep argument to nonunique/unique/unique!" begin
    df = DataFrame(a=[1, 2, 3, 1, 2, 1],
                   b=["a", "b", "c", "a", "b", "a"],
                   c=categorical(["a", "b", "c", "a", "b", "a"]))
    for cols in (1, 2, 3, [1, 2], [1, 3], [2, 3], [1, 2, 3])
        @test nonunique(df, cols, keep=:first) ==
              [false, false, false, true, true, true]
        @test nonunique(df, cols, keep=:last) ==
              [true, true, false, true, false, false]
        @test nonunique(df, cols, keep=:noduplicates) ==
              [true, true, false, true, true, true]
        @test nonunique(select(df, cols), keep=:first) ==
              [false, false, false, true, true, true]
        @test nonunique(select(df, cols), keep=:last) ==
              [true, true, false, true, false, false]
        @test nonunique(select(df, cols), keep=:noduplicates) ==
              [true, true, false, true, true, true]

        @test unique(df, cols, keep=:first) ==
              df[.![false, false, false, true, true, true], :]
        @test unique(df, cols, keep=:last) ==
              df[.![true, true, false, true, false, false], :]
        @test unique(df, cols, keep=:noduplicates) ==
              df[.![true, true, false, true, true, true], :]
        @test unique(select(df, cols), keep=:first) ==
              df[.![false, false, false, true, true, true], Cols(cols)]
        @test unique(select(df, cols), keep=:last) ==
              df[.![true, true, false, true, false, false], Cols(cols)]
        @test unique(select(df, cols), keep=:noduplicates) ==
              df[.![true, true, false, true, true, true], Cols(cols)]

        @test unique!(copy(df), cols, keep=:first) ==
              df[.![false, false, false, true, true, true], :]
        @test unique!(copy(df), cols, keep=:last) ==
              df[.![true, true, false, true, false, false], :]
        @test unique!(copy(df), cols, keep=:noduplicates) ==
              df[.![true, true, false, true, true, true], :]
        @test unique!(select(df, cols), keep=:first) ==
              df[.![false, false, false, true, true, true], Cols(cols)]
        @test unique!(select(df, cols), keep=:last) ==
              df[.![true, true, false, true, false, false], Cols(cols)]
        @test unique!(select(df, cols), keep=:noduplicates) ==
              df[.![true, true, false, true, true, true], Cols(cols)]
    end

    # some larger randomized test
    Random.seed!(1234)
    df = DataFrame(a=rand(1:10^5, 10^5))
    df.b = string.(df.a)
    df.c = categorical(df.b)
    df.id = 1:10^5

    for cols in (1, 2, 3, [1, 2], [1, 3], [2, 3], [1, 2, 3])
        @test select(unique(df, cols, keep=:first), cols, Not(cols)) ==
              combine(groupby(df, cols, sort=false), first)
        @test select(unique(df, cols, keep=:last), cols, Not(cols)) ==
              sort(combine(groupby(df, cols, sort=false), last), :id)
        @test select(unique(df, cols, keep=:noduplicates), cols, Not(cols)) ==
              sort(combine(groupby(df, cols, sort=false),
                           sdf -> nrow(sdf) == 1 ? sdf : NamedTuple()), :id)
    end

    @test isempty(nonunique(DataFrame(), keep=:first))
    @test unique(DataFrame(a=[]), keep=:last) == DataFrame(a=[])
    @test unique!(DataFrame(), keep=:noduplicates) == DataFrame()
    @test_throws ArgumentError nonunique(DataFrame(), keep=:a)
    @test_throws ArgumentError unique(DataFrame(), keep=:b)
    @test_throws ArgumentError unique!(DataFrame(), keep=:c)
end

@testset "case when groups are not compressed in row_group_slots!" begin
   df = DataFrame(x=repeat([1:1000; -1], 2));
   @test getindex.(keys(groupby(df, :x, sort=true)), 1) == [-1; 1:1000]
   @test nonunique(df, :x) == [falses(1001); trues(1001)]
   @test nonunique(df, :x, keep=:last) == [trues(1001); falses(1001)]
   @test all(nonunique(df, :x, keep=:noduplicates))
end

end # module
