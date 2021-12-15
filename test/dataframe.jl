module TestDataFrame

using Dates, DataFrames, Statistics, Random, Test, Logging, DataStructures,
      CategoricalArrays
using DataFrames: _columns, index
const ≅ = isequal
const ≇ = !isequal

# randomized test from https://github.com/JuliaData/DataFrames.jl/pull/1974
@testset "randomized tests for rename!" begin
    n = Symbol.('a':'z')
    Random.seed!(1234)
    for k in 1:20
        sn = shuffle(n)
        df = DataFrame(zeros(1, 26), n)
        p = Dict(Pair.(n, sn))
        cyclelength = Int[]
        for x in n
            i = 0
            y = x
            while true
                y = p[y]
                i += 1
                x == y && break
            end
            push!(cyclelength, i)
        end
        i = lcm(cyclelength)
        while true
            rename!(df, p)
            @test sort(propertynames(df)) == n
            @test sort(names(df)) == string.(n)
            @test sort(collect(keys(index(df).lookup))) == n
            @test sort(collect(values(index(df).lookup))) == 1:26
            @test all(index(df).lookup[x] == i for (i, x) in enumerate(propertynames(df)))
            i -= 1
            propertynames(df) == n && break
        end
        @test i == 0
    end
end

# additional randomized tests of renaming only part of the columns
# they cover both cases leading to duplicate names and not leading to them
# but possibly allowing for cyclical renaming and non-cyclical renaming that
# would lead to duplicates if we did the renaming sequentially as before
@testset "additional rename! tests" begin
    Random.seed!(123)
    for i in 1:1000
        oldnames = Symbol.(rand('a':'z', 8))
        while !allunique(oldnames)
            oldnames .= Symbol.(rand('a':'z', 8))
        end
        newnames = [Symbol.(rand('a':'z', 4)); oldnames[5:end]]
        df = DataFrame([[] for i in 1:8], oldnames)
        if allunique(newnames)
            @test names(rename(df, Pair.(oldnames[1:4], newnames[1:4])...)) == string.(newnames)
            @test propertynames(df) == oldnames
            rename!(df, Pair.(oldnames[1:4], newnames[1:4])...)
            @test propertynames(df) == newnames
        else
            @test_throws ArgumentError rename(df, Pair.(oldnames[1:4], newnames[1:4])...)
            @test propertynames(df) == oldnames
            @test_throws ArgumentError rename!(df, Pair.(oldnames[1:4], newnames[1:4])...)
            @test propertynames(df) == oldnames
        end

        newnames = [oldnames[1:2]; reverse(oldnames[3:6]); oldnames[7:end]]
        df = DataFrame([[] for i in 1:8], oldnames)
        @test names(rename(df, Pair.(oldnames[3:6], newnames[3:6])...)) == string.(newnames)
        @test propertynames(df) == oldnames
        rename!(df, Pair.(oldnames[3:6], newnames[3:6])...)
        @test propertynames(df) == newnames
    end
end

@testset "rename with integer source" begin
    df = DataFrame(a=1, b=2)
    @test rename(df, 1=>:c) == DataFrame(c=1, b=2)
    @test rename(df, big(1)=>:c) == DataFrame(c=1, b=2)
    @test rename(df, 0x1=>:c) == DataFrame(c=1, b=2)
    @test rename(df, 1=>:a) == DataFrame(a=1, b=2)
    @test rename(df, 1=>"c") == DataFrame(c=1, b=2)
    @test rename(df, [1=>:b, 2=>:a]) == DataFrame(b=1, a=2)
    @test rename(df, [1=>"b", 2=>"a"]) == DataFrame(b=1, a=2)
    @test rename(df, Dict(1=>:b, 2=>:a)) == DataFrame(b=1, a=2)
    @test rename(df, Dict(1=>"b", 2=>"a")) == DataFrame(b=1, a=2)
    @test_throws ArgumentError rename(df, 1=>:b)
    @test_throws ArgumentError rename(df, true=>:b)
    @test_throws BoundsError rename(df, 0=>:b)
    @test_throws BoundsError rename(df, 3=>:b)
    @test_throws DimensionMismatch rename(df, [:p, :q, :r])

    @test rename!(copy(df), 1=>:c) == DataFrame(c=1, b=2)
    @test rename!(copy(df), big(1)=>:c) == DataFrame(c=1, b=2)
    @test rename!(copy(df), 0x1=>:c) == DataFrame(c=1, b=2)
    @test rename!(copy(df), 1=>:a) == DataFrame(a=1, b=2)
    @test rename!(copy(df), 1=>"c") == DataFrame(c=1, b=2)
    @test rename!(copy(df), [1=>:b, 2=>:a]) == DataFrame(b=1, a=2)
    @test rename!(copy(df), [1=>"b", 2=>"a"]) == DataFrame(b=1, a=2)
    @test rename!(copy(df), Dict(1=>:b, 2=>:a)) == DataFrame(b=1, a=2)
    @test rename!(copy(df), Dict(1=>"b", 2=>"a")) == DataFrame(b=1, a=2)
    @test_throws ArgumentError rename!(df, 1=>:b)
    @test_throws ArgumentError rename!(df, true=>:b)
    @test_throws BoundsError rename!(df, 0=>:b)
    @test_throws BoundsError rename!(df, 3=>:b)
    @test_throws DimensionMismatch rename!(df, [:p, :q, :r])
    @test df == DataFrame(a=1, b=2)
end

@testset "equality" begin
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) == DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2], b=[4, 5]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], c=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(b=[4, 5, 6], a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 2], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 3, missing], b=[4, 5, 6]) != DataFrame(a=[1, 2, missing], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, missing], b=[4, 5, 6]) ≅ DataFrame(a=[1, 2, missing], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, missing], b=[4, 5, 6]) ≇ DataFrame(a=[1, 2, 3], b=[4, 5, 6])
end

@testset "copying" begin
    df = DataFrame(a=Union{Int, Missing}[2, 3],
                   b=Union{DataFrame, Missing}[DataFrame(c=1), DataFrame(d=2)])
    dfc = copy(df)
    dfcc = copy(df, copycols=false)
    dfdc = deepcopy(df)

    @test dfc == df
    @test dfc.a !== df.a
    @test dfc.b !== df.b
    @test DataFrames._columns(dfc) == DataFrames._columns(df)
    @test DataFrames._columns(dfc) !== DataFrames._columns(df)
    @test dfcc == df
    @test dfcc.a === df.a
    @test dfcc.b === df.b
    @test DataFrames._columns(dfcc) == DataFrames._columns(df)
    @test DataFrames._columns(dfcc) !== DataFrames._columns(df)
    @test dfdc == df
    @test dfdc.a !== df.a
    @test dfdc.b !== df.b
    @test DataFrames._columns(dfdc) == DataFrames._columns(df)
    @test DataFrames._columns(dfdc) !== DataFrames._columns(df)

    df[1, :a] = 4
    df[1, :b][!, :e] .= 5

    @test names(rename(df, [:f, :g])) == ["f", "g"]
    @test names(rename(df, [:f, :f], makeunique=true)) == ["f", "f_1"]
    @test names(df) == ["a", "b"]

    rename!(df, [:f, :g])

    @test names(dfc) == ["a", "b"]
    @test names(dfdc) == ["a", "b"]

    @test dfc[1, :a] === 2
    @test dfdc[1, :a] === 2

    @test names(dfc[1, :b]) == ["c", "e"]
    @test names(dfdc[1, :b]) == ["c"]
end

@testset "similar / missings" begin
    df = DataFrame(a=Union{Int, Missing}[1],
                   b=Union{String, Missing}["b"],
                   c=CategoricalArray{Union{Float64, Missing}}([3.3]))
    missingdf = DataFrame(a=missings(Int, 2),
                          b=missings(String, 2),
                          c=CategoricalArray{Union{Float64, Missing}}(undef, 2))
    # https://github.com/JuliaData/Missings.jl/issues/66
    # @test missingdf ≅ similar(df, 2)
    @test typeof.(eachcol(similar(df, 2))) == typeof.(eachcol(missingdf))
    @test size(similar(df, 2)) == size(missingdf)
end

@testset "hasproperty" begin
    df = DataFrame(a=[1, 2])
    @test hasproperty(df, :a)
    @test !hasproperty(df, :c)
    @test_throws MethodError hasproperty(df, 1)
    @test_throws MethodError hasproperty(df, 1.5)
    @test_throws MethodError hasproperty(df, true)
end

@testset "insertcols!" begin
    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test_throws ArgumentError insertcols!(df, 5, :newcol => ["a", "b"])
    @test_throws ArgumentError insertcols!(df, 0, :newcol => ["a", "b"])
    @test_throws ArgumentError insertcols!(df, :z, :newcol => ["a", "b"])
    @test_throws ArgumentError insertcols!(df, "z", :newcol => ["a", "b"])
    @test_throws MethodError insertcols!(df, true, :newcol => ["a", "b"])
    @test_throws DimensionMismatch insertcols!(df, 1, :newcol => ["a"])
    @test_throws DimensionMismatch insertcols!(df, :a, :newcol => ["a"])
    @test_throws DimensionMismatch insertcols!(df, "a", :newcol => ["a"])
    ref1 = insertcols!(copy(df), :a, :newcol => ["a", "b"])
    ref2 = insertcols!(copy(df), "a", :newcol => ["a", "b"])
    @test insertcols!(df, 1, :newcol => ["a", "b"]) == df
    @test ref1 == ref2 == df
    @test names(df) == ["newcol", "a", "b"]
    @test df.a == [1, 2]
    @test df.b == [3.0, 4.0]
    @test df.newcol == ["a", "b"]

    @test_throws ArgumentError insertcols!(df, 1, :newcol => ["a1", "b1"])
    @test insertcols!(df, 1, :newcol => ["a1", "b1"], makeunique=true) == df
    @test propertynames(df) == [:newcol_1, :newcol, :a, :b]
    @test df.a == [1, 2]
    @test df.b == [3.0, 4.0]
    @test df.newcol == ["a", "b"]
    @test df.newcol_1 == ["a1", "b1"]

    @test insertcols!(df, 1, :c1 => 1:2) === df
    @test df.c1 isa Vector{Int}
    x = [1, 2]
    @test insertcols!(df, 1, :c2 => x, copycols=true) === df
    @test df.c2 == x
    @test df.c2 !== x
    @test insertcols!(df, 1, :c3 => x, copycols=false) === df
    @test df.c3 === x

    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test_throws ArgumentError insertcols!(df, 5, "newcol" => ["a", "b"])
    @test_throws ArgumentError insertcols!(df, 0, "newcol" => ["a", "b"])
    @test_throws DimensionMismatch insertcols!(df, 1, "newcol" => ["a"])
    @test insertcols!(df, 1, "newcol" => ["a", "b"]) == df
    @test names(df) == ["newcol", "a", "b"]
    @test df.a == [1, 2]
    @test df.b == [3.0, 4.0]
    @test df.newcol == ["a", "b"]

    @test_throws ArgumentError insertcols!(df, 1, "newcol" => ["a1", "b1"])
    @test insertcols!(df, 1, "newcol" => ["a1", "b1"], makeunique=true) == df
    @test propertynames(df) == [:newcol_1, :newcol, :a, :b]
    @test df.a == [1, 2]
    @test df.b == [3.0, 4.0]
    @test df.newcol == ["a", "b"]
    @test df.newcol_1 == ["a1", "b1"]

    @test insertcols!(df, 1, "c1" => 1:2) === df
    @test df.c1 isa Vector{Int}
    x = [1, 2]
    @test insertcols!(df, 1, "c2" => x, copycols=true) === df
    @test df.c2 == x
    @test df.c2 !== x
    @test insertcols!(df, 1, "c3" => x, copycols=false) === df
    @test df.c3 === x

    df = DataFrame(a=[1, 2], a_1=[3, 4])
    @test_throws ArgumentError insertcols!(df, 1, :a => [11, 12])
    @test df == DataFrame(a=[1, 2], a_1=[3, 4])
    insertcols!(df, 1, :a => [11, 12], makeunique=true)
    @test propertynames(df) == [:a_2, :a, :a_1]
    insertcols!(df, 4, :a => [11, 12], makeunique=true)
    @test propertynames(df) == [:a_2, :a, :a_1, :a_3]
    @test_throws ArgumentError insertcols!(df, 10, :a => [11, 12], makeunique=true)

    dfc = copy(df)
    @test insertcols!(df, 2) == dfc
    @test_throws ArgumentError insertcols!(df, 10)
    @test_throws ArgumentError insertcols!(df, 2, a=1, b=2)

    df = DataFrame()
    @test insertcols!(df, 1, :x=>[1]) == DataFrame(x=[1])
    df = DataFrame()
    @test_throws ArgumentError insertcols!(df, 2, :x=>[1])
    df = DataFrame()
    @test insertcols!(df, 1, :x=>1:2) == DataFrame(x=1:2)
    @test df.x isa Vector{Int}
    x = [1, 2]
    df = DataFrame()
    @test insertcols!(df, 1, :x=>x, copycols=true) == DataFrame(x=1:2)
    @test df.x !== x
    df = DataFrame()
    @test insertcols!(df, 1, :x=>x, copycols=false) == DataFrame(x=1:2)
    @test df.x === x

    df = DataFrame()
    v1 = 1:2
    v2 = [3, 4]
    v3 = [5, 6]
    @test insertcols!(df, 1, :a=>v1, :b=>v2, :c=>v3, copycols=false) == DataFrame(a=v1, b=v2, c=v3)
    @test df.a isa Vector{Int}
    @test df.b === v2
    @test df.c === v3

    df = DataFrame()
    @test insertcols!(df, 1, :a=>v1, :b=>v2, :c=>v3, copycols=true) ==
          DataFrame(a=v1, b=v2, c=v3)
    @test df.a isa Vector{Int}
    @test df.b !== v2
    @test df.c !== v3

    df = DataFrame()
    @test insertcols!(df, 1, :a=>v1, :a=>v2, :a=>v3, makeunique=true, copycols=false) ==
          DataFrame(a=v1, a_1=v2, a_2=v3)
    @test df.a isa Vector{Int}
    @test df.a_1 === v2
    @test df.a_2 === v3

    df = DataFrame(p='a':'b', q='r':'s')
    @test insertcols!(df, 2, :a=>v1, :b=>v2, :c=>v3) ==
          DataFrame(p='a':'b', a=v1, b=v2, c=v3, q='r':'s')

    df = DataFrame(p='a':'b', q='r':'s')
    @test insertcols!(df, 2, "a"=>v1, "b"=>v2, "c"=>v3) ==
          DataFrame(p='a':'b', a=v1, b=v2, c=v3, q='r':'s')

    df = DataFrame(p='a':'b', q='r':'s')
    @test_throws ArgumentError insertcols!(df, 2, :p=>v1, :q=>v2, :p=>v3)
    @test insertcols!(df, 2, :p=>v1, :q=>v2, :p=>v3, makeunique=true, copycols=true) ==
          DataFrame(p='a':'b', p_1=v1, q_1=v2, p_2=v3, q='r':'s')
    @test df.p_1 isa Vector{Int}
    @test df.q_1 !== v2
    @test df.p_2 !== v3

    df = DataFrame(a=1:3, b=4:6)
    @test insertcols!(copy(df), :c=>7:9) == insertcols!(copy(df), 3, :c=>7:9)
    df = DataFrame()
    @test insertcols!(df, :a=>1:3) == DataFrame(a=1:3)

    df = DataFrame(a=[1, 2], a_1=[3, 4])
    insertcols!(df, 1, :a => 11, makeunique=true)
    @test propertynames(df) == [:a_2, :a, :a_1]
    @test df[!, 1] == [11, 11]
    insertcols!(df, 4, :a => 12, makeunique=true)
    @test propertynames(df) == [:a_2, :a, :a_1, :a_3]
    @test df[!, 4] == [12, 12]
    df = DataFrame()
    @test insertcols!(df, :a => "a", :b => 1:2) == DataFrame(a=["a", "a"], b=1:2)

    df = DataFrame()
    insertcols!(df, :a => Ref(1), :b => fill(1))
    @test df == DataFrame(a=[1], b=[1])
    df = DataFrame()
    insertcols!(df, :a => Ref(1), :b => fill(1), :c => 1:3)
    @test df == DataFrame(a=[1, 1, 1], b=[1, 1, 1], c=1:3)
    df = DataFrame(c=1:3)
    insertcols!(df, 1, :a => Ref(1), :b => fill(1))
    @test df == DataFrame(a=[1, 1, 1], b=[1, 1, 1], c=1:3)

    df = DataFrame(a=1)
    @test insertcols!(df, "a" => 2, makeunique=true) == DataFrame(a=1, a_1=2)
end

@testset "unsupported insertcols!" begin
    df = DataFrame(x=1:2)
    @test_throws ArgumentError insertcols!(df, 2, y=2:3)
end

@testset "insertcols! after" begin
    df = DataFrame(a=1:3)
    insertcols!(df, :a, "b" => 2:4, after=true)
    @test df == DataFrame(a=1:3, b=2:4)
    insertcols!(df, :b, "b" => 2:4, after=true, makeunique=true)
    @test df == DataFrame(a=1:3, b=2:4, b_1=2:4)
    insertcols!(df, 0, :e => 1:3, after=true)
    @test df == DataFrame(e=1:3, a=1:3, b=2:4, b_1=2:4)

    @test_throws ArgumentError insertcols!(df, :a, "b" => 2:4, after=true)
    @test_throws DimensionMismatch insertcols!(df, :a, :c => 2:5, after=true)
    @test_throws ArgumentError insertcols!(df, :c, :b => 2:4, after=true, makeunique=true)
    @test_throws ArgumentError insertcols!(df, ncol(df)+1, :d => 1:3, after=true)
    @test_throws ArgumentError insertcols!(df, -1, :d => 1:3, after=true)

    df = DataFrame(a=1:3, b=2:4)
    insertcols!(df, 1, :c => 7:9, after=true)
    @test df == DataFrame(a=1:3, c=7:9, b=2:4)

    df = DataFrame(a=1:3)
    insertcols!(df, 1, :b => 2:4, :c => 7:9, after=true)
    @test df == DataFrame(a=1:3, b=2:4, c=7:9)
    insertcols!(df, "b", :b => 2:4, :c => 7:9, after=true, makeunique=true)
    @test df == DataFrame(a=1:3, b=2:4, b_1=2:4, c_1=7:9, c=7:9)
end

@testset "DataFrame constructors" begin
    @test DataFrame([Union{Int, Missing}[1, 2, 3], Union{Float64, Missing}[2.5, 4.5, 6.5]],
                    [:A, :B]) ==
        DataFrame(A=Union{Int, Missing}[1, 2, 3], B=Union{Float64, Missing}[2.5, 4.5, 6.5])

    # This assignment was missing before
    df = DataFrame(Column=[:A])
    df[1, :Column] = :Testing

    # zero-row DataFrame and subDataFrame test
    df = DataFrame(x=[], y=[])
    @test nrow(df) == 0
    df = DataFrame(x=[1:3;], y=[3:5;])
    sdf = view(df, df[!, :x] .== 4, :)
    @test size(sdf, 1) == 0

    # Test that vector type is correctly determined from scalar type
    df = DataFrame(x=categorical(["a"])[1])
    @test df.x isa CategoricalVector{String}

    @test hash(DataFrame([1 2; 3 4], :auto)) == hash(DataFrame([1 2; 3 4], :auto))
    @test hash(DataFrame([1 2; 3 4], :auto)) != hash(DataFrame([1 3; 2 4], :auto))
    @test hash(DataFrame([1 2; 3 4], :auto)) == hash(DataFrame([1 2; 3 4], :auto), zero(UInt))
end

@testset "push!(df, row)" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfc = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, Any[3, "pear"])
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, (3, "pear"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws InexactError push!(dfb, (33.33, "pear"))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    @test_throws DimensionMismatch push!(dfb, (1, "2", 3))
    @test dfc == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, ("coconut", 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, (11, 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, Dict(:first=>3, :second=>"pear"))
    @test df == dfb

    df = DataFrame(first=[1, 2, 3], second=["apple", "orange", "banana"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, Dict(:first=>3, :second=>"banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, (first=3, second="banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, (second=3, first=3))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    push!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>true, :second=>false))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>"stuff"))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df = DataFrame(x=1)
    push!(df, Dict(:x=>2), Dict(:x=>3))
    @test df[!, :x] == [1, 2, 3]

    df = DataFrame(x=1, y=2)
    push!(df, [3, 4], [5, 6])
    @test df[!, :x] == [1, 3, 5] && df[!, :y] == [2, 4, 6]

    df = DataFrame(x=1, y=2)
    with_logger(sl) do
        @test_throws KeyError push!(df, Dict(:x=>1, "y"=>2))
    end
    @test df == DataFrame(x=1, y=2)
    @test occursin("Error adding value to column :y", String(take!(buf)))

    df = DataFrame()
    @test push!(df, (a=1, b=true)) === df
    @test df == DataFrame(a=1, b=true)

    df = DataFrame()
    df.a = [1, 2, 3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError push!(df, [1, 2])
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1, b=2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, Dict(:a=>1, :b=>2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    @test_throws AssertionError push!(df, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError push!(df, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame()
    df.a = [1, 2, 3, 4]
    df.b = df.a
    df.c = [1, 2, 3, 4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError push!(df, [1, 2, 3])
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1, b=2, c=3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, Dict(:a=>1, :b=>2, :c=>3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    @test_throws AssertionError push!(df, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError push!(df, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame(a=1, b=2)
    push!(df, [1 2])
    @test df == DataFrame(a=[1, 1], b=[2, 2])
    push!(df, (1, 2))
    @test df == DataFrame(a=[1, 1, 1], b=[2, 2, 2])

    @test_throws ArgumentError push!(df, "ab")
end

@testset "deleteat!" begin
    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    @test_throws BoundsError deleteat!(df, [true, true, true])
    @test deleteat!(df, 1) === df
    @test df == DataFrame(a=[2], b=[4.0])

    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    @test deleteat!(df, 2) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test deleteat!(df, 1) === df
    @test df == DataFrame(a=[2], b=[4.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test deleteat!(df, 2) === df
    @test df == DataFrame(a=[1], b=[3.0])

    for v in (2:3, [2, 3])
        df = DataFrame(a=Union{Int, Missing}[1, 2, 3], b=Union{Float64, Missing}[3.0, 4.0, 5.0])
        @test deleteat!(df, v) === df
        @test df == DataFrame(a=[1], b=[3.0])

        df = DataFrame(a=[1, 2, 3], b=[3.0, 4.0, 5.0])
        @test deleteat!(df, v) === df
        @test df == DataFrame(a=[1], b=[3.0])
    end

    df = DataFrame()
    @test_throws BoundsError deleteat!(df, 10)
    @test_throws BoundsError deleteat!(df, [10])

    df = DataFrame(a=[])
    @test_throws BoundsError deleteat!(df, 10)

    if VERSION >= v"1.1"
        @test_throws BoundsError deleteat!(df, [10])
    else
        @test_throws InexactError deleteat!(df, [10])
    end

    df = DataFrame(a=[1, 2, 3], b=[3, 2, 1])
    @test_throws ArgumentError deleteat!(df, [3, 2])
    @test_throws ArgumentError deleteat!(df, [2, 2])
    @test deleteat!(df, [false, true, false]) === df
    @test df == DataFrame(a=[1, 3], b=[3, 1])

    for v in (1, [1], 1:1, [true, false, false])
        x = [1, 2, 3]
        df = DataFrame(x=x)
        @test deleteat!(df, v) == DataFrame(x=[2, 3])
        @test x == [1, 2, 3]
    end

    for v in (1, [1], 1:1, [true, false, false], Not(2, 3), Not([false, true, true]))
        x = [1, 2, 3]
        df = DataFrame(x=x, copycols=false)
        @test deleteat!(df, v) == DataFrame(x=[2, 3])
        @test x == [2, 3]
    end

    for inds in (1, [1], [true, false])
        df = DataFrame(x1=[1, 2])
        df.x2 = df.x1
        @test deleteat!(df, inds) === df
        @test df == DataFrame(x1=[2], x2=[2])
    end

    df = DataFrame(a=1, b=2)
    push!(df.b, 3)
    @test_throws AssertionError deleteat!(df, 1)
end

@testset "describe" begin
    # Construct the test dataframe
    df = DataFrame(number=[1, 2, 3, 4],
                   number_missing=[1, 2, 3, missing],
                   string=["a", "b", "c", "d"],
                   string_missing=["a", "b", "c", missing],
                   dates=Date.([2000, 2001, 2003, 2004]),
                   catarray=CategoricalArray([1, 2, 1, 2]))

    describe_output = DataFrame(variable=[:number, :number_missing, :string,
                                          :string_missing, :dates, :catarray],
                                mean=[2.5, 2.0, nothing, nothing, nothing, nothing],
                                std=[std(df[!, :number]), 1.0, nothing,
                                       nothing, nothing, nothing],
                                min=[1.0, 1.0, "a", "a", Date(2000), 1],
                                q25=[1.75, 1.5, nothing, nothing, nothing, nothing],
                                median=[2.5, 2.0, nothing, nothing, VERSION >= v"1.7.0-beta1.2" ? Date(2002) : nothing, nothing],
                                q75=[3.25, 2.5, nothing, nothing, nothing, nothing],
                                max=[4.0, 3.0, "d", "c", Date(2004), 2],
                                nunique=[nothing, nothing, 4, 3, 4, 2],
                                nmissing=[0, 1, 0, 1, 0, 0],
                                first=[1, 1, "a", "a", Date(2000), 1],
                                last=[4, missing, "d", missing, Date(2004), 2],
                                eltype=[Int, Union{Missing, Int}, String,
                                        Union{Missing, String}, Date, CategoricalValue{Int, UInt32}])

    default_fields = [:mean, :min, :median, :max, :nmissing, :eltype]

    # Test that it works as a whole, without keyword arguments
    @test describe_output[:, [:variable; default_fields]] == describe(df)

    # Test that it works with one stats argument
    @test describe_output[:, [:variable, :mean]] == describe(df, :mean)

    # Test that it works with :all
    @test describe_output ≅ describe(df, :all)

    # Test that it works with :detailed
    @test describe_output[:, [:variable, :mean, :std, :min, :q25, :median, :q75,
                              :max, :nunique, :nmissing, :eltype]] ≅
        describe(df, :detailed)

    # Test that it works on a custom function
    describe_output.test_std = describe_output.std
    # Test that describe works with a Pair and a symbol
    @test describe_output[:, [:variable, :mean, :test_std]] ≅
          describe(df, :mean, std => :test_std)
    @test describe_output[:, [:variable, :mean, :test_std]] ≅
          describe(df, :mean, std => "test_std")

    @test describe(df, cols=[:number => identity => :number, :number => ByRow(string) => :string]) ==
          DataFrame(variable=[:number, :string], mean=[2.5, nothing], min=[1, "1"],
                    median=[2.5, nothing], max=[4, "4"], nmissing=[0, 0], eltype=[Int, String])

    # Test that describe works with a dataframe with no observations
    df = DataFrame(a=Int[], b=String[], c=[])
    @test describe(df, :mean) ≅ DataFrame(variable=[:a, :b, :c],
                                          mean=[NaN, nothing, nothing])

    @test describe(df, :all, cols=Not(1)) ≅ describe(select(df, Not(1)), :all)
    @test describe(df, cols=Not(1)) ≅ describe(select(df, Not(1)))
    @test describe(df, cols=Not("a")) ≅ describe(select(df, Not(1)))

    @test describe(DataFrame(a=[1, 2]), cols=:a, :min, minimum => :min2, maximum => "max2", :max) ==
          DataFrame(variable=:a, min=1, min2=1, max2=2, max=2)

    @test_throws ArgumentError describe(df, :mean, :all)
    @test_throws MethodError describe(DataFrame(a=[1, 2]), cols=:a, "max2" => maximum)
    @test_throws ArgumentError describe(df, :min, :min)
    @test_throws ArgumentError describe(df, :minimum)
end

@testset "append!" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)
    df = DataFrame(A=1:2, B=1:2)
    df2 = DataFrame(A=1:4, B=1:4)
    @test append!(df, DataFrame(A=3:4, B=[3.0, 4.0])) == df2
    with_logger(sl) do
        @test_throws InexactError append!(df, DataFrame(A=3:4, B=[3.5, 4.5]))
    end
    @test df == df2
    @test occursin("Error adding value to column :B", String(take!(buf)))
    with_logger(sl) do
        @test_throws MethodError append!(df, DataFrame(A=3:4, B=["a", "b"]))
    end
    @test df == df2
    @test occursin("Error adding value to column :B", String(take!(buf)))
    @test_throws ArgumentError append!(df, DataFrame(A=1:4, C=1:4))
    @test df == df2

    dfx = DataFrame()
    df3 = append!(dfx, df)
    @test dfx === df3
    @test df3 == df
    @test df3[!, 1] !== df[!, 1]
    @test df3[!, 2] !== df[!, 2]

    df4 = append!(df3, DataFrame())
    @test df4 === df3
    @test df4 == df

    df = DataFrame()
    df.a = [1, 2, 3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError append!(df, dfc)
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))

    df = DataFrame()
    df.a = [1, 2, 3, 4]
    df.b = df.a
    df.c = [1, 2, 3, 4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError append!(df, dfc)
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))

    rename!(df, [:a, :b, :z])
    @test_throws ArgumentError append!(df, dfc)

    df = DataFrame(A=1:2, B=1:2)
    df2 = DataFrame(A=1:4, B=1:4)
    @test append!(copy(df), DataFrame(A=3:4, B=[3.0, 4.0])) == df2
    @test append!(copy(df), DataFrame(A=3:4, B=[3.0, 4.0]), cols=:setequal) == df2
    @test append!(copy(df), DataFrame(B=3:4, A=[3.0, 4.0])) == df2
    @test append!(copy(df), DataFrame(B=3:4, A=[3.0, 4.0]), cols=:setequal) == df2
    @test append!(copy(df), Dict(:A => 3:4, :B => [3.0, 4.0])) == df2
    @test append!(copy(df), Dict(:A => 3:4, :B => [3.0, 4.0]), cols=:setequal) == df2
    @test append!(copy(df), DataFrame(A=3:4, B=[3.0, 4.0]), cols=:orderequal) == df2
    @test append!(copy(df), OrderedDict(:A => 3:4, :B => [3.0, 4.0]), cols=:orderequal) == df2
    @test_throws ArgumentError append!(df, Dict(:A => 3:4, :B => [3.0, 4.0]), cols=:orderequal)
    @test_throws ArgumentError append!(df, DataFrame(B=3:4, A=[3.0, 4.0]), cols=:orderequal)
    @test_throws ArgumentError append!(df, OrderedDict(:B => 3:4, :A => [3.0, 4.0]), cols=:orderequal)
    @test_throws ArgumentError append!(df, DataFrame(B=3:4, A=[3.0, 4.0]), cols=:xxx)
    @test df == DataFrame(A=1:2, B=1:2)
end

@testset "append! default options" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df1 = DataFrame(x=1:3, y=1:3)
    df2 = DataFrame(y=4:6, x=1:3)
    append!(df1, df2)
    @test df1 == DataFrame(x=[1:3;1:3], y=1:6)

    df2 = DataFrame(y=4:6, x=1:3, z=1)
    @test_throws ArgumentError append!(df1, df2)
    @test df1 == DataFrame(x=[1:3;1:3], y=1:6)

    df2 = DataFrame(y=4:6, x=[missing, missing, missing])
    with_logger(sl) do
        @test_throws MethodError append!(df1, df2)
    end
    @test df1 == DataFrame(x=[1:3;1:3], y=1:6)

    df2 = DataFrame(x=[missing, missing, missing], y=4:6)
    for cols in (:orderequal, :intersect)
        with_logger(sl) do
            @test_throws MethodError append!(df1, df2, cols=cols)
        end
        @test df1 == DataFrame(x=[1:3;1:3], y=1:6)
    end

    for cols in (:subset, :union)
        df1 = DataFrame(x=1:3, y=1:3)
        df2 = DataFrame(y=4:6)
        append!(df1, df2, cols=cols)
        @test df1 ≅ DataFrame(x=[1:3;missing; missing; missing], y=1:6)
    end
end

@testset "append! advanced options" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    for cols in (:orderequal, :setequal, :intersect, :subset, :union)
        for promote in (true, false)
            df1 = DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(x=1:3, y=4:6)
            append!(df1, df2, cols=cols, promote=promote)
            @test df1 == DataFrame(x=[1:3;1:3], y=1:6)
            @test eltype(df1.x) == Int
            @test eltype(df1.y) == Int

            df1 = DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(y=4:6, x=1:3)
            if cols == :orderequal
                @test_throws ArgumentError append!(df1, df2, cols=cols, promote=promote)
                @test df1 == DataFrame(x=1:3, y=1:3)
            else
                append!(df1, df2, cols=cols, promote=promote)
                @test df1 == DataFrame(x=[1:3;1:3], y=1:6)
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Int
            end

            df1 = DataFrame()
            df1.x = 1:3
            df1.y = df1.x
            df2 = DataFrame(x=1:3, y=4:6)
            with_logger(sl) do
                @test_throws AssertionError append!(df1, df2, cols=cols, promote=promote)
            end
            @test df1 == DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(y=4:6, x=1:3)
            with_logger(sl) do
                @test_throws (cols == :orderequal ? ArgumentError :
                              AssertionError) append!(df1, df2, cols=cols, promote=promote)
            end
            @test df1 == DataFrame(x=1:3, y=1:3)

            df1 = DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(x=1:3, y=4:6, z=11:13)
            if cols in [:orderequal, :setequal]
                @test_throws ArgumentError append!(df1, df2, cols=cols, promote=promote)
                @test df1 == DataFrame(x=1:3, y=1:3)
            elseif cols == :union
                append!(df1, df2, cols=cols, promote=promote)
                @test df1 ≅ DataFrame(x=[1:3;1:3], y=1:6,
                                      z=[missing; missing; missing; 11:13])
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Int
                @test eltype(df1.z) == Union{Missing, Int}
            else
                append!(df1, df2, cols=cols, promote=promote)
                @test df1 == DataFrame(x=[1:3;1:3], y=1:6)
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Int
            end

            df1 = DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(x=1:3, y=[missing, missing, missing])
            if promote
                append!(df1, df2, cols=cols, promote=true)
                @test df1 ≅ DataFrame(x=[1:3;1:3], y=[1:3; missing; missing; missing])
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Union{Missing, Int}
            else
                with_logger(sl) do
                    @test_throws MethodError append!(df1, df2, cols=cols, promote=promote)
                end
                @test df1 == DataFrame(x=1:3, y=1:3)
            end

            df1 = DataFrame(x=1:3, y=1:3)
            df2 = DataFrame(x=1:3, z=11:13)
            if !promote || cols in [:orderequal, :setequal, :intersect]
                with_logger(sl) do
                    @test_throws ArgumentError append!(df1, df2, cols=cols, promote=promote)
                end
                @test df1 == DataFrame(x=1:3, y=1:3)
            elseif cols == :union
                append!(df1, df2, cols=cols, promote=true)
                @test df1 ≅ DataFrame(x=[1:3;1:3], y=[1:3; missing; missing; missing],
                                      z=[missing; missing; missing; 11:13])
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Union{Missing, Int}
                @test eltype(df1.z) == Union{Missing, Int}
            else
                append!(df1, df2, cols=cols, promote=true)
                @test df1 ≅ DataFrame(x=[1:3;1:3], y=[1:3; missing; missing; missing])
                @test eltype(df1.x) == Int
                @test eltype(df1.y) == Union{Missing, Int}
            end
        end
    end
end

@testset "new append! and push! tests" begin
    for df in [DataFrame(a=Any[1]), DataFrame(a=1)]
        @test append!(df, DataFrame(b=1), cols=:union) ≅
              DataFrame(a=[1, missing], b=[missing, 1])
        @test append!(df, DataFrame(b=1), cols=:union) ≅
              DataFrame(a=[1, missing, missing], b=[missing, 1, 1])
        df.x = 1:3
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws ArgumentError append!(df, DataFrame(b=1), cols=:union,
                                               promote=false)
        end
        @test df  ≅ DataFrame(a=[1, missing, missing], b=[missing, 1, 1], x=1:3)
        allowmissing!(df, :x)
        @test append!(df, DataFrame(b=1), cols=:union, promote=false) ≅
              DataFrame(a=[1, missing, missing, missing], b=[missing, 1, 1, 1],
                        x=[1:3; missing])
    end

    for df in [DataFrame(a=Any[1]), DataFrame(a=1)]
        @test push!(df, (b=1,), cols=:union) ≅
              DataFrame(a=[1, missing], b=[missing, 1])
        @test push!(df, (b=1,), cols=:union) ≅
              DataFrame(a=[1, missing, missing], b=[missing, 1, 1])
        df.x = 1:3
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError push!(df, (b=1,), cols=:union, promote=false)
        end
        @test df  ≅ DataFrame(a=[1, missing, missing], b=[missing, 1, 1], x=1:3)
        allowmissing!(df, :x)
        @test push!(df, (b=1,), cols=:union, promote=false) ≅
              DataFrame(a=[1, missing, missing, missing], b=[missing, 1, 1, 1],
                        x=[1:3; missing])
    end

    for df in [DataFrame(a=Any[1]), DataFrame(a=1)]
        @test push!(df, DataFrame(b=1)[1, :], cols=:union) ≅
              DataFrame(a=[1, missing], b=[missing, 1])
        @test push!(df, DataFrame(b=1)[1, :], cols=:union) ≅
              DataFrame(a=[1, missing, missing], b=[missing, 1, 1])
        df.x = 1:3
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError push!(df, DataFrame(b=1)[1, :], cols=:union,
                                           promote=false)
        end
        @test df  ≅ DataFrame(a=[1, missing, missing], b=[missing, 1, 1], x=1:3)
        allowmissing!(df, :x)
        @test push!(df, DataFrame(b=1)[1, :], cols=:union, promote=false) ≅
              DataFrame(a=[1, missing, missing, missing], b=[missing, 1, 1, 1],
                        x=[1:3; missing])
    end

    @test_throws ArgumentError push!(DataFrame(), (a=1, b=2), cols=:unions)
    @test_throws ArgumentError push!(DataFrame(), Dict('a'=>1, 'b'=>2), cols=:union)
end

@testset "rename" begin
    for asview in (false, true)
        df = DataFrame(A=1:3, B='A':'C')
        asview && (df=view(df, :, :))
        @test names(rename(df, :A => :A_1)) == ["A_1", "B"]
        @test names(df) == ["A", "B"]
        @test names(rename(df, :A => :A_1, :B => :B_1)) == ["A_1", "B_1"]
        @test names(df) == ["A", "B"]
        @test names(rename(df, [:A => :A_1, :B => :B_1])) == ["A_1", "B_1"]
        @test names(df) == ["A", "B"]
        @test names(rename(df, Dict(:A => :A_1, :B => :B_1))) == ["A_1", "B_1"]
        @test names(df) == ["A", "B"]
        @test names(rename(lowercase, df)) == ["a", "b"]
        @test names(df) == ["A", "B"]

        @test rename!(df, :A => :A_1) === df
        @test propertynames(df) == [:A_1, :B]
        @test rename!(df, :A_1 => :A_2, :B => :B_2) === df
        @test propertynames(df) == [:A_2, :B_2]
        @test rename!(df, [:A_2 => :A_3, :B_2 => :B_3]) === df
        @test propertynames(df) == [:A_3, :B_3]
        @test rename!(df, Dict(:A_3 => :A_4, :B_3 => :B_4)) === df
        @test propertynames(df) == [:A_4, :B_4]
        @test rename!(lowercase, df) === df
        @test propertynames(df) == [:a_4, :b_4]

        df = DataFrame(A=1:3, B='A':'C', C=[:x, :y, :z])
        asview && (df = view(df, :, :))
        @test rename!(df, :A => :B, :B => :A) === df
        @test propertynames(df) == [:B, :A, :C]
        @test rename!(df, :A => :B, :B => :A, :C => :D) === df
        @test propertynames(df) == [:A, :B, :D]
        @test rename!(df, :A => :B, :B => :C, :D => :A) === df
        @test propertynames(df) == [:B, :C, :A]
        @test rename!(df, :A => :C, :B => :A, :C => :B) === df
        @test propertynames(df) == [:A, :B, :C]
        @test rename!(df, :A => :A, :B => :B, :C => :C) === df
        @test propertynames(df) == [:A, :B, :C]

        cdf = copy(df)
        @test_throws ArgumentError rename!(df, :X => :Y)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :X, :X => :Y)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :B)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :X, :A => :X)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :X, :A => :Y)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :X, :B => :X)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :B, :B => :A, :C => :B)
        @test df == cdf
        @test_throws ArgumentError rename!(df, :A => :B, :B => :A, :A => :X)
        @test df == cdf

        df = DataFrame(A=1)
        asview && (df=view(df, :, :))
        @test rename(x -> 1, df) == DataFrame(Symbol("1") => 1)
    end

    sdf = view(DataFrame(ones(2, 3), :auto), 1:2, 1:3)
    @test_throws ArgumentError rename!(uppercase, sdf)
    @test_throws ArgumentError rename!(sdf, :x1 => :y1)
    @test_throws ArgumentError rename!(sdf, [:a, :b, :c])
end

@testset "flexible rename arguments" begin
    df = DataFrame(x=1, y=2, z=3)
    for ren in ([:a, :b, :c], ["a", "b", "c"],
                ["x"=>:a, "y"=>:b, "z"=>:c],
                [:x=>"a", :y=>"b", :z=>"c"],
                ["x"=>"a", "y"=>"b", "z"=>"c"],
                [:x=>:a, :y=>:b, :z=>:c],
                Dict(["x"=>:a, "y"=>:b, "z"=>:c]),
                Dict([:x=>"a", :y=>"b", :z=>"c"]),
                Dict(["x"=>"a", "y"=>"b", "z"=>"c"]),
                Dict([:x=>:a, :y=>:b, :z=>:c]))
        @test rename(df, ren) == DataFrame(a=1, b=2, c=3)
        df == DataFrame(x=1, y=2, z=3)
        if eltype(ren) isa Pair
            @test rename(df, ren...) == DataFrame(a=1, b=2, c=3)
            @test df == DataFrame(x=1, y=2, z=3)
        end
        df2 = copy(df)
        @test rename!(df2, ren) == DataFrame(a=1, b=2, c=3)
        @test df2 == DataFrame(a=1, b=2, c=3)
        df2 = copy(df)
        if eltype(ren) isa Pair
            @test rename!(df2, ren...) == DataFrame(a=1, b=2, c=3)
            @test df2 == DataFrame(a=1, b=2, c=3)
        end
    end
end

@testset "size" begin
    df = DataFrame(A=1:3, B='A':'C')
    @test_throws ArgumentError size(df, 3)
    @test ndims(df) == 2
    @test ndims(typeof(df)) == 2
    @test (nrow(df), ncol(df)) == (3, 2)
    @test size(df) == (3, 2)
    @inferred nrow(df)
    @inferred ncol(df)
end

@testset "first, last and only" begin
    df = DataFrame(A=1:10)

    @test first(df) == df[1, :]
    @test last(df) == df[end, :]
    @test_throws BoundsError first(DataFrame(x=[]))
    @test_throws BoundsError last(DataFrame(x=[]))

    @test first(df, 6) == DataFrame(A=1:6)
    @test first(df, 1) == DataFrame(A=1)
    @test last(df, 6) == DataFrame(A=5:10)
    @test last(df, 1) == DataFrame(A=10)

    @inferred first(df, 6)
    @inferred last(df, 6)
    @inferred first(df)
    @inferred last(df)

    @test first(df, 6, view=true) == DataFrame(A=1:6)
    @test last(df, 6, view=true) == DataFrame(A=5:10)

    @test first(df, 6, view=true) isa SubDataFrame
    @test first(df, 6, view=false) isa DataFrame
    @test last(df, 6, view=true) isa SubDataFrame
    @test last(df, 6, view=false) isa DataFrame

    @test_throws ArgumentError only(df)
    @test_throws ArgumentError only(DataFrame())
    df = DataFrame(a=1, b=2)
    @test only(df) === df[1, :]
end

@testset "column conversions" begin
    df = DataFrame([collect(1:10), collect(1:10)], :auto)
    @test !isa(df[!, 1], Vector{Union{Int, Missing}})
    @test allowmissing!(df, 1) === df
    @test isa(df[!, 1], Vector{Union{Int, Missing}})
    @test !isa(df[!, 2], Vector{Union{Int, Missing}})
    df[1, 1] = missing
    @test_throws ArgumentError disallowmissing!(df, 1)
    tmpcol = df[!, 1]
    disallowmissing!(df, 1, error=false)
    @test df[!, 1] === tmpcol
    df[1, 1] = 1
    @test disallowmissing!(df, 1) === df
    @test isa(df[!, 1], Vector{Int})

    df = DataFrame([collect(1:10), collect(1:10)], :auto)
    @test !isa(df[!, 1], Vector{Union{Int, Missing}})
    @test allowmissing!(df, Not(Not(1))) === df
    @test isa(df[!, 1], Vector{Union{Int, Missing}})
    @test !isa(df[!, 2], Vector{Union{Int, Missing}})
    df[1, 1] = missing
    @test_throws ArgumentError disallowmissing!(df, Not(Not(1)))
    tmpcol = df[!, 1]
    disallowmissing!(df, Not(Not(1)), error=false)
    @test df[!, 1] === tmpcol
    df[1, 1] = 1
    @test disallowmissing!(df, Not(Not(1))) === df
    @test isa(df[!, 1], Vector{Int})

    for em in [true, false]
        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df, [1, 2]) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, [1, 2], error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df, Not(Not([1, 2]))) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, Not(Not([1, 2])), error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test_throws BoundsError allowmissing!(df, [true])
        @test allowmissing!(df, [true, true]) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test_throws BoundsError disallowmissing!(df, [true], error=em)
        @test disallowmissing!(df, [true, true], error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df, :) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, :, error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df, r"") === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, r"", error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)], :auto)
        @test allowmissing!(df, Not(1:0)) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, Not(1:0), error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})
    end

    df = DataFrame([CategoricalArray(1:10),
                    CategoricalArray(string.('a':'j'))], :auto)
    @test allowmissing!(df) === df
    @test all(x->x <: CategoricalVector, typeof.(eachcol(df)))
    @test eltype(df[!, 1]) <: Union{CategoricalValue{Int}, Missing}
    @test eltype(df[!, 2]) <: Union{CategoricalValue{String}, Missing}
    df[1, 2] = missing
    @test_throws ArgumentError disallowmissing!(df)
    tmpcol =df[!, 2]
    disallowmissing!(df, error=false)
    @test df[!, 2] === tmpcol
    df[1, 2] = "a"
    @test disallowmissing!(df) === df
    @test all(x->x <: CategoricalVector, typeof.(eachcol(df)))
    @test eltype(df[!, 1]) <: CategoricalValue{Int}
    @test eltype(df[!, 2]) <: CategoricalValue{String}

    for em in [true, false]
        df = DataFrame(b=[1, 2], c=[1, 2], d=[1, 2])
        @test allowmissing!(df, [:b, :c]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Union{Int, Missing}
        @test eltype(df.d) == Int
        @test disallowmissing!(df, :c, error=em) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Int
        @test allowmissing!(df, [:d]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Union{Int, Missing}
        @test disallowmissing!(df, [:c, :d], error=em) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Int
        @test allowmissing!(df, [false, false, true]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Union{Int, Missing}
        @test disallowmissing!(df, [true, false, false], error=em) === df
        @test eltype(df.b) == Int
        @test eltype(df.c) == Int
        @test eltype(df.d) == Union{Int, Missing}
    end

    for em in [true, false]
        df = DataFrame(b=[1, 2], c=[1, 2], d=[1, 2])
        @test allowmissing!(df, ["b", "c"]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Union{Int, Missing}
        @test eltype(df.d) == Int
        @test disallowmissing!(df, "c", error=em) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Int
        @test allowmissing!(df, ["d"]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Union{Int, Missing}
        @test disallowmissing!(df, ["c", "d"], error=em) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Int
        @test eltype(df.d) == Int
    end

    df = DataFrame(x=[1], y=Union{Int, Missing}[1], z=[missing])
    disallowmissing!(df, error=false)
    @test eltype(df.x) == Int
    @test eltype(df.y) == Int
    @test eltype(df.z) == Missing

    df = DataFrame(x=[1], y=Union{Int, Missing}[1], z=[missing])
    disallowmissing!(df, 2:3, error=false)
    @test eltype(df.x) == Int
    @test eltype(df.y) == Int
    @test eltype(df.z) == Missing
end

@testset "test disallowmissing" begin
    df = DataFrame(x=Union{Int, Missing}[1, 2, 3],
                   y=Union{Int, Missing}[1, 2, 3],
                   z=[1, 2, 3])
    for x in [df, view(df, :, :)], em in [true, false]
        y = disallowmissing(x, error=em)
        @test y isa DataFrame
        @test x == y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test eltype.(eachcol(y)) == [Int, Int, Int]

        for colsel in [:, names(x), [1, 2, 3], [true, true, true], r"", Not(r"a")]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Int, Int, Int]
        end

        for colsel in [:x, "x", 1, [:x], ["x"], [1], [true, false, false], r"x", Not(2:3)]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Int, Union{Missing, Int}, Int]
        end

        for colsel in [:z, "z", 3, [:z], ["z"], [3], [false, false, true], r"z", Not(1:2)]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Int, Missing}, Union{Int, Missing}, Int]
        end

        for colsel in [Int[], Symbol[], [false, false, false], r"a", Not(:)]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Int, Missing}, Union{Int, Missing}, Int]
        end
    end

    @test_throws ArgumentError disallowmissing(DataFrame(x=[missing]))
    @test disallowmissing(DataFrame(x=[missing]), error=false) ≅ DataFrame(x=[missing])
    @test_throws ArgumentError disallowmissing(DataFrame(x=[1, missing]))
    @test disallowmissing(DataFrame(x=[1, missing]), error=false) ≅ DataFrame(x=[1, missing])

    df = DataFrame(x=[1], y=Union{Int, Missing}[1], z=[missing])
    df2 = disallowmissing(df, error=false)
    @test eltype(df2.x) == Int
    @test eltype(df2.y) == Int
    @test eltype(df2.z) == Missing

    df2 = disallowmissing(df, 2:3, error=false)
    @test eltype(df2.x) == Int
    @test eltype(df2.y) == Int
    @test eltype(df2.z) == Missing
end

@testset "test allowmissing" begin
    df = DataFrame(x=Union{Int, Missing}[1, 2, 3],
                   y=[1, 2, 3],
                   z=[1, 2, 3])
    for x in [df, view(df, :, :)]
        y = allowmissing(x)
        @test y isa DataFrame
        @test x == y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test eltype.(eachcol(y)) == fill(Union{Missing, Int}, 3)

        for colsel in [:, names(x), [1, 2, 3], [true, true, true], r"", Not(r"a")]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == fill(Union{Missing, Int}, 3)
        end

        for colsel in [:x, "x", 1, [:x], ["x"], [1], [true, false, false], r"x", Not(2:3)]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Missing, Int}, Int, Int]
        end

        for colsel in [:z, "z", 3, [:z], ["z"], [3], [false, false, true], r"z", Not(1:2)]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Int, Missing}, Int, Union{Missing, Int}]
        end

        for colsel in [Int[], Symbol[], [false, false, false], r"a", Not(:)]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Int, Missing}, Int, Int]
        end
    end
end

@testset "similar" begin
    df = DataFrame(a=["foo"],
                   b=CategoricalArray(["foo"]),
                   c=[0.0],
                   d=CategoricalArray([0.0]))
    @test eltype.(eachcol(similar(df))) == eltype.(eachcol(df))
    @test size(similar(df)) == size(df)

    rows = size(df, 1) + 5
    @test size(similar(df, rows)) == (rows, size(df, 2))
    @test eltype.(eachcol(similar(df, rows))) == eltype.(eachcol(df))

    @test size(similar(df, 0)) == (0, size(df, 2))
    @test eltype.(eachcol(similar(df, 0))) == eltype.(eachcol(df))

    e = @test_throws ArgumentError similar(df, -1)
    @test e.value.msg == "the number of rows must be non-negative"
end

@testset "passing range to a DataFrame" begin
    df = DataFrame(a=1:3, b='a':'c')
    df[!, :c] = 1:3
    df[!, :d] = 'a':'c'
    @test all(typeof(df[!, i]) <: Vector for i in 1:ncol(df))
end

@testset "test getindex using ! syntax" begin
    x = [1]
    y = [1]
    df = DataFrame(x=x, y=y, copycols=false)
    @test df.x === x
    @test df[!, :y] === y
    @test df[!, 1] === x
    @test df[:, 1:1][!, 1] == x
    @test df[:, r"x"][!, 1] == x
    @test df[:, 1:1][!, 1] !== x
    @test df[:, r"x"][!, 1] !== x
    @test df[:, 1:2][!, :y] == y
    @test df[:, 1:2][!, :y] !== y
    @test df[:, r""][!, :y] == y
    @test df[:, r""][!, :y] !== y
    @test df[:, :][!, :x] == x
    @test df[:, :][!, :x] !== x
    @test df[:, [:y, :x]][!, :x] == x
    @test df[:, [:y, :x]][!, :x] !== x
end

@testset "test corner case of getindex" begin
    df = DataFrame(x=[1], y=[1])
    @test_throws ArgumentError df[true, 1:2]
    @test_throws ArgumentError df[true, r""]
end

@testset "empty data frame getindex" begin
    @test_throws BoundsError DataFrame(x=[])[1, :]
    @test_throws BoundsError DataFrame()[1, :]
    @test_throws BoundsError DataFrame()[1:2, :]
    @test_throws BoundsError DataFrame()[1, Bool[]]
    @test_throws BoundsError DataFrame()[1:2, Bool[]]
    @test_throws BoundsError DataFrame(x=[1])[1:2, [false]]
    @test_throws BoundsError DataFrame(x=[1])[2, [false]]
    #but this is OK:
    @test DataFrame(x=[1])[1:1, [false]] == DataFrame()
    @test DataFrame(x=[1])[1:1, r"xx"] == DataFrame()
end

@testset "handling of end in indexing" begin
    z = DataFrame(rand(4, 5), :auto)
    x = z
    y = deepcopy(x)
    @test x[:, end] == x[:, 5]
    @test x[:, end:end] == x[:, 5:5]
    @test x[end, :] == x[4, :]
    @test x[end:end, :] == x[4:4, :]
    @test x[end, end] == x[4, 5]
    @test x[2:end, 2:end] == x[2:4, 2:5]
    x[!, end] = 1:4
    y[!, 5] = 1:4
    @test x == y
    x[:, 4:end] .= DataFrame([11:14, 21:24], [:x4, :x5])
    y[!, 4] = [11:14;]
    y[!, 5] = [21:24;]
    @test x == y
    x[end, :] .= 111
    y[4, :] .= 111
    @test x == y
    x[end, end] = 1000
    y[4, 5] = 1000
    @test x == y
    x[2:end, 2:end] .= 0
    y[2:4, 2:5] .= 0
    @test x == y

    x = view(z, 1:4, :)
    y = deepcopy(x)
    @test x[:, end] == x[:, 5]
    @test x[:, end:end] == x[:, 5:5]
    @test x[end, :] == x[4, :]
    @test x[end:end, :] == x[4:4, :]
    @test x[end, end] == x[4, 5]
    @test x[2:end, 2:end] == x[2:4, 2:5]
    x[:, end] = 1:4
    y[:, 5] = 1:4
    @test x == y
    x[:, 4:end] .= DataFrame([11:14, 21:24], [:x4, :x5])
    y[:, 4] = [11:14;]
    y[:, 5] = [21:24;]
    @test x == y
    x[end, :] .= 111
    y[4, :] .= 111
    @test x == y
    x[end, end] = 1000
    y[4, 5] = 1000
    @test x == y
    x[2:end, 2:end] .= 0
    y[2:4, 2:5] .= 0
    @test x == y
end

@testset "aliasing in indexing" begin
    # columns should not alias if scalar broadcasted
    df = DataFrame(A=[0], B=[0])
    df[:, 1:end] .= 0.0
    df[1, :A] = 1.0
    @test df[1, :B] === 0

    df = DataFrame(A=[0], B=[0])
    df[:, 1:end] .= 0.0
    df[1, :A] = 1.0
    @test df[1, :B] === 0

    df = DataFrame(A=[0], B=[0])
    x = [0.0]
    df[:, 1:end] .= x
    x[1] = 1.0
    @test df[1, :A] === 0
    @test df[1, :B] === 0
    df[1, :A] = 1.0
    @test df[1, :B] === 0
end

@testset "getproperty, setproperty! and propertynames" begin
    x = collect(1:10)
    y = collect(1.0:10.0)
    z = collect(10:-1:1)
    df = DataFrame(x=x, y=y, copycols=false)

    @test propertynames(df) == Symbol.(names(df))

    @test df.x === x
    @test df.y === y
    @test_throws ArgumentError df.z

    df.x = 2:11
    @test df.x == 2:11
    @test x == 1:10
    df.y .= 1
    @test df.y == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    @test df.y === y
    df.z = z
    @test df.z === z
    df[!, :zz] .= 1
    @test df.zz == df.y
end

@testset "duplicate column names" begin
    x = DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    v = DataFrame(a=[5, 6, 7], b=[8, 9, 10])
    z = vcat(v, x)
    @test_throws ArgumentError z[:, [1, 1, 2]]
end

@testset "parent, size and axes" begin
    x = DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test parent(x) === x
    @test parentindices(x) === (Base.OneTo(3), Base.OneTo(2))
    @test size(x) == (3, 2)
    @test size(x, 1) == 3
    @test size(x, 2) == 2
    @test_throws ArgumentError size(x, 3)
    @test axes(x) === (Base.OneTo(3), Base.OneTo(2))
    @test axes(x, 1) === Base.OneTo(3)
    @test axes(x, 2) === Base.OneTo(2)
    @test_throws ArgumentError axes(x, 3)
    @test size(DataFrame()) == (0, 0)
end

@testset "0-row DataFrame corner cases" begin
    df = DataFrame(a=1:0)
    @test df.a isa Vector{Int}
    v = Int[]
    df = DataFrame(a=v, b=v)
    @test df.a !== df.b
    df = DataFrame(a=v, b=v, copycols=true)
    @test df.a !== df.b
    df = DataFrame(a=v, b=v, copycols=false)
    @test df.a === df.b
end

@testset "All and Between tests" begin
    # we check dispatch here only
    df = DataFrame(a=1, b=2, c=3)
    completecases(df, All())
    completecases(df, Cols(:))
    completecases(df, Between(1, 2))
    dropmissing(df, All())
    dropmissing(df, Cols(:))
    dropmissing(df, Between(1, 2))
    dropmissing!(df, All())
    dropmissing!(df, Cols(:))
    dropmissing!(df, Between(1, 2))
    disallowmissing(df, All())
    disallowmissing(df, Cols(:))
    disallowmissing(df, Between(1, 2))
    allowmissing(df, All())
    allowmissing(df, Cols(:))
    allowmissing(df, Between(1, 2))

    df[1, All()]
    df[1, Cols(:)]
    df[1, Between(1, 2)]
    df[1:1, All()]
    df[1:1, Cols(:)]
    df[1:1, Between(1, 2)]
    df[Not(1), All()]
    df[Not(1), Cols(:)]
    df[Not(1), Between(1, 2)]
    df[:, All()]
    df[:, Cols(:)]
    df[:, Between(1, 2)]
    df[!, All()]
    df[!, Cols(:)]
    df[!, Between(1, 2)]

    @view df[1, All()]
    @view df[1, Cols(:)]
    @view df[1, Between(1, 2)]
    @view df[1:1, All()]
    @view df[1:1, Cols(:)]
    @view df[1:1, Between(1, 2)]
    @view df[Not(1), All()]
    @view df[Not(1), Cols(:)]
    @view df[Not(1), Between(1, 2)]
    @view df[:, All()]
    @view df[:, Cols(:)]
    @view df[:, Between(1, 2)]
    @view df[!, All()]
    @view df[!, Cols(:)]
    @view df[!, Between(1, 2)]

    df[1, All()] = (a=1, b=2, c=3)
    df[1, Cols(:)] = (a=1, b=2, c=3)
    df[1, Between(1, 2)] = (a=1, b=2)
    df[1:1, All()] = df
    df[1:1, Cols(:)] = df
    df[1:1, Between(1, 2)] = df[!, 1:2]
    df[:, All()] = df
    df[:, Cols(:)] = df
    df[:, Between(1, 2)] = df[!, 1:2]
    df[1:1, All()] = Matrix(df)
    df[1:1, Cols(:)] = Matrix(df)
    df[1:1, Between(1, 2)] = Matrix(df[!, 1:2])
    df[:, All()] = Matrix(df)
    df[:, Cols(:)] = Matrix(df)
    df[:, Between(1, 2)] = Matrix(df[!, 1:2])

    df2 = vcat(df, df)
    df2[Not(1), All()] = df
    df2[Not(1), Cols(:)] = df
    df2[Not(1), Between(1, 2)] = df[!, 1:2]
    df2[Not(1), All()] = Matrix(df)
    df2[Not(1), Cols(:)] = Matrix(df)
    df2[Not(1), Between(1, 2)] = Matrix(df[!, 1:2])

    allowmissing!(df2, All())
    allowmissing!(df2, Cols(:))
    allowmissing!(df2, Between(1, 2))
    disallowmissing!(df2, All())
    disallowmissing!(df2, Cols(:))
    disallowmissing!(df2, Between(1, 2))

    dfr = df[1, :]
    dfr[All()]
    dfr[Cols(:)]
    dfr[Between(1, 2)]
    dfr[All()] = (a=1, b=2, c=3)
    dfr[Cols(:)] = (a=1, b=2, c=3)
    dfr[Between(1, 2)] = (a=1, b=2)
    @view dfr[All()]
    @view dfr[Cols(:)]
    @view dfr[Between(1, 2)]

    dfv = view(df, :, :)

    dfv[1, All()]
    dfv[1, Cols(:)]
    dfv[1, Between(1, 2)]
    dfv[1:1, All()]
    dfv[1:1, Cols(:)]
    dfv[1:1, Between(1, 2)]
    dfv[Not(1), All()]
    dfv[Not(1), Cols(:)]
    dfv[Not(1), Between(1, 2)]
    dfv[:, All()]
    dfv[:, Cols(:)]
    dfv[:, Between(1, 2)]
    dfv[!, All()]
    dfv[!, Cols(:)]
    dfv[!, Between(1, 2)]

    @view dfv[1, All()]
    @view dfv[1, Cols(:)]
    @view dfv[1, Between(1, 2)]
    @view dfv[1:1, All()]
    @view dfv[1:1, Cols(:)]
    @view dfv[1:1, Between(1, 2)]
    @view dfv[Not(1), All()]
    @view dfv[Not(1), Cols(:)]
    @view dfv[Not(1), Between(1, 2)]
    @view dfv[:, All()]
    @view dfv[:, Cols(:)]
    @view dfv[:, Between(1, 2)]
    @view dfv[!, All()]
    @view dfv[!, Cols(:)]
    @view dfv[!, Between(1, 2)]

    dfv[1, All()] = (a=1, b=2, c=3)
    dfv[1, Cols(:)] = (a=1, b=2, c=3)
    dfv[1, Between(1, 2)] = (a=1, b=2)
    dfv[1:1, All()] = df
    dfv[1:1, Cols(:)] = df
    dfv[1:1, Between(1, 2)] = df[!, 1:2]
    dfv[:, All()] = df
    dfv[:, Cols(:)] = df
    dfv[:, Between(1, 2)] = df[!, 1:2]
    dfv[1:1, All()] = Matrix(df)
    dfv[1:1, Cols(:)] = Matrix(df)
    dfv[1:1, Between(1, 2)] = Matrix(df[!, 1:2])
    dfv[:, All()] = Matrix(df)
    dfv[:, Cols(:)] = Matrix(df)
    dfv[:, Between(1, 2)] = Matrix(df[!, 1:2])

    df2v = view(vcat(df, df), :, :)
    df2v[Not(1), All()] = df
    df2v[Not(1), Cols(:)] = df
    df2v[Not(1), Between(1, 2)] = df[!, 1:2]
    df2v[Not(1), All()] = Matrix(df)
    df2v[Not(1), Cols(:)] = Matrix(df)
    df2v[Not(1), Between(1, 2)] = Matrix(df[!, 1:2])

    @test_throws ArgumentError df[1, All(1)]
end

@testset "vcat and push! with :orderequal" begin
    for v in ((a=10, b=20, c=30),
              DataFrame(a=10, b=20, c=30)[1, :],
              OrderedDict(:a=>10, :b=>20, :c=>30))
        df = DataFrame(a=1, b=2, c=3)
        push!(df, v, cols=:orderequal)
        @test df == DataFrame(a=[1, 10], b=[2, 20], c=[3, 30])
    end

    for v in ((a=10, b=20, d=30), (a=10, c=20, b=30),
              DataFrame(a=10, c=20, b=30)[1, :],
              (a=10, b=20, c=30, d=0),
              DataFrame(a=10, b=20, c=30, d=0)[1, :],
              Dict(:a=>10, :b=>20, :c=>30),
              OrderedDict(:c=>10, :b=>20, :a=>30))
        df = DataFrame(a=1, b=2, c=3)
        @test_throws ArgumentError push!(df, v, cols=:orderequal)
    end

    @test vcat(DataFrame(a=1, b=2, c=3), DataFrame(a=10, b=20, c=30),
               cols=:orderequal) == DataFrame(a=[1, 10], b=[2, 20], c=[3, 30])
    @test_throws ArgumentError vcat(DataFrame(a=1, b=2, c=3), DataFrame(a=10, b=20, c=30),
                                    cols=:equal)
    @test_throws ArgumentError vcat(DataFrame(a=1, b=2, c=3), DataFrame(a=10, c=20, b=30),
                                    cols=:orderequal)
    @test_throws ArgumentError vcat(DataFrame(a=1, b=2, c=3), DataFrame(a=10, b=20, d=30),
                                    cols=:orderequal)
    @test_throws ArgumentError vcat(DataFrame(a=1, b=2, c=3), DataFrame(a=10, b=20, c=30, d=0),
                                    cols=:orderequal)
end

@testset "vcat with source" begin
    df1 = DataFrame(A=1:3, B=1:3)
    df2 = DataFrame(A=4:6, B=4:6)
    df3 = DataFrame(A=7:9, C=7:9)
    df4 = DataFrame()

    for col in [:source, "source"]
        @test vcat(df1, df2, df3, df4, cols=:union, source=col) ≅
              vcat(df1, df2, df3, df4, cols=:union, source=col => [1, 2, 3, 4]) ≅
              DataFrame(A=1:9, B=[1:6; fill(missing, 3)],
                        C=[fill(missing, 6); 7:9],
                        source=[1, 1, 1, 2, 2, 2, 3, 3, 3])
        res = vcat(df1, df2, df3, df4, cols=:union, source=col => categorical(-4:-1))
        @test res ≅ DataFrame(A=1:9, B=[1:6; fill(missing, 3)],
                              C=[fill(missing, 6); 7:9],
                              source=[-4, -4, -4, -3, -3, -3, -2, -2, -2])
        @test res.source isa CategoricalVector
    end

    @test_throws TypeError vcat(df1, df2, df3, df4, cols=:union, source=1)
    @test_throws TypeError vcat(df1, df2, df3, df4, cols=:union, source=:a => 1)
    @test_throws ArgumentError vcat(df1, df2, df3, df4, cols=:union, source=:C)
    @test_throws ArgumentError vcat(df1, df2, df3, df4, cols=:union, source=:a => [1])
end

@testset "push! with :subset" begin
    for v in (Dict(:a=>10, :b=>20, :d=>30), (a=10, b=20, d=30),
              DataFrame(a=10, b=20, d=30)[1, :])
        df = DataFrame(a=1, b=2, c=3)
        old_logger = global_logger(NullLogger())
        @test_throws MethodError push!(df, v, cols=:subset, promote=false)
        global_logger(old_logger)
        @test df == DataFrame(a=1, b=2, c=3)
    end
    for v in (Dict(:a=>10, :b=>20, :d=>30), (a=10, b=20, d=30),
              DataFrame(a=10, b=20, d=30)[1, :])
        df = DataFrame(a=1, b=2, c=3)
        allowmissing!(df, :c)
        push!(df, v, cols=:subset, promote=false)
        @test df ≅ DataFrame(a=[1, 10], b=[2, 20], c=[3, missing])
        old_logger = global_logger(NullLogger())
        @test_throws MethodError push!(df, Dict(), cols=:subset, promote=false)
        global_logger(old_logger)
        @test df ≅ DataFrame(a=[1, 10], b=[2, 20], c=[3, missing])
        allowmissing!(df, [:a, :b])
        push!(df, Dict(), cols=:subset)
        @test df ≅ DataFrame(a=[1, 10, missing], b=[2, 20, missing], c=[3, missing, missing])
    end
end

@testset "push! with :intersect" begin
    for row in ((y=4, x=3), Dict(:y=>4, :x=>3), (z=1, y=4, x=3), Dict(:y=>4, :x=>3, :z=>1))
        df = DataFrame(x=1, y=2)
        push!(df, row, cols=:intersect)
        @test df == DataFrame(x=[1, 3], y=[2, 4])
    end

    old_logger = global_logger(NullLogger())
    for row in ((z=4, x=3), (z=1, p=4, x=3))
        df = DataFrame(x=1, y=2)
        @test_throws ErrorException push!(df, row, cols=:intersect)
        @test df == DataFrame(x=1, y=2)
    end

    for row in (Dict(:z=>4, :x=>3), Dict(:p=>4, :x=>3, :z=>1))
        df = DataFrame(x=1, y=2)
        @test_throws KeyError push!(df, row, cols=:intersect)
        @test df == DataFrame(x=1, y=2)
    end
    global_logger(old_logger)
end

@testset "push!(df, row) with :union" begin
    df = DataFrame()
    push!(df, (a=1, b=2))
    a = df.a
    push!(df, (a=1, c=2), cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[2, missing], c=[missing, 2])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    push!(df, (a=1, c=2), cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    push!(df, (c=2,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    push!(df, (c=missing,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    push!(df, (c="a", d=1), cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    push!(df, (a="b",), cols=:union)
    @test df ≅ DataFrame(a=[missing, missing, "b"],
                         c=[missing, "a", missing],
                         d=[missing, 1, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1]
    df = DataFrame(a=a, copycols=false)
    push!(df, (a=1,), cols=:union)
    @test df.a === a
    push!(df, (a=1.0,), cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError push!(df, (a=1, b=2, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    # note that this is correct although we have a problem with aliasing
    # as we eventually reallocate column :b to a correct length
    # and aliasing does not affect rows that already existed in df
    push!(df, (a=1, b=2.0, c=3), cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[1.0, 2.0], c=[missing, 3], copycols=false)
    @test df.a === x
    @test eltype(df.b) === Float64

    df = DataFrame()
    push!(df, DataFrame(a=1, b=2)[1, :])
    a = df.a
    push!(df, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[2, missing], c=[missing, 2])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    push!(df, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    push!(df, DataFrame(c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    push!(df, DataFrame(c=missing)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    push!(df, DataFrame(c="a", d=1)[1, :], cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    push!(df, DataFrame(a="b")[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing, missing, "b"],
                         c=[missing, "a", missing],
                         d=[missing, 1, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1]
    df = DataFrame(a=a, copycols=false)
    push!(df, DataFrame(a=1)[1, :], cols=:union)
    @test df.a === a
    push!(df, DataFrame(a=1.0)[1, :], cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError push!(df, DataFrame(a=1, b=2, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    # note that this is correct although we have a problem with aliasing
    # as we eventually reallocate column :b to a correct length
    # and aliasing does not affect rows that already existed in df
    push!(df, DataFrame(a=1, b=2.0, c=3)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[1.0, 2.0], c=[missing, 3], copycols=false)
    @test df.a === x
    @test eltype(df.b) === Float64
end

@testset "push!(df, row) with promote options" begin
    df = DataFrame(a=1)
    with_logger(SimpleLogger(IOBuffer())) do
        @test_throws MethodError push!(df, ["a"])
    end
    @test push!(df, ["a"], promote=true) == DataFrame(a=[1, "a"])

    for v in [(a="a",), DataFrame(a="a")[1, :]]
        for cols in [:orderequal, :setequal, :intersect]
            df = DataFrame(a=1)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError push!(df, v, cols=cols)
            end
            @test push!(df, v, cols=cols, promote=true) == DataFrame(a=[1, "a"])
        end
        for cols in [:subset, :union]
        df = DataFrame(a=1, b=1)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError push!(df, v, cols=cols, promote=false)
            end
            @test push!(df, v, cols=cols) ≅ DataFrame(a=[1, "a"], b=[1, missing])
        end
    end
end

@testset "push! with :setequal and wrong number of entries" begin
    df = DataFrame(a=1:3)
    @test_throws ArgumentError push!(df, (a=10, b=20))
    @test_throws ArgumentError push!(df, "a")
end

@testset "names for Type, predicate + standard tests of cols" begin
    df_long = DataFrame(a1=1:3, a2=[1, missing, 3],
                        b1=1.0:3.0, b2=[1.0, missing, 3.0],
                        c1='1':'3', c2=['1', missing, '3'], x=1:3)
    for x in (df_long[:, Not(end)], @view(df_long[:, Not(end)]),
              groupby(df_long[:, Not(end)], :a1), groupby(@view(df_long[:, Not(end)]), :a1),
              eachrow(df_long[:, Not(end)]), eachrow(@view(df_long[:, Not(end)])),
              eachcol(df_long[:, Not(end)]), eachcol(@view(df_long[:, Not(end)])),
              df_long[1, Not(end)])
        @test names(x, 1) == ["a1"]
        @test names(x, "a1") == ["a1"]
        @test names(x, :a1) == ["a1"]
        @test names(x, [2, 1]) == ["a2", "a1"]
        @test names(x, ["a2", "a1"]) == ["a2", "a1"]
        @test names(x, [:a2, :a1]) == ["a2", "a1"]
        @test names(x, Int) == ["a1"]
        @test names(x, Union{Missing, Int}) == ["a1", "a2"]
        @test names(x, Real) == ["a1", "b1"]
        @test names(x, Union{Missing, Real}) == ["a1", "a2", "b1", "b2"]
        @test names(x, Any) == names(x)
        @test isempty(names(x, BigInt))
        @test names(x, Union{Char, Float64, Missing}) == ["b1", "b2", "c1", "c2"]
        @test names(x, startswith("a")) == ["a1", "a2"]
        @test names(x, :) == names(x)
        @test names(x, <("a2")) == ["a1"]

        # before Julia 1.8 it is TypeError; the change is caused by the redesign of ifelse
        @test_throws Union{MethodError, TypeError} names(x, x -> 1)
    end
end

@testset "reverse DataFrame" begin
    df = DataFrame(a=1:5, b=5:-1:1)
    @test reverse(df) == DataFrame(a=5:-1:1, b=1:5)
    @test reverse(DataFrame(a=1, b=1)) == DataFrame(a=1, b=1)
    @test typeof(reverse(df)) == DataFrame
end

@testset "reverse SubDataFrame" begin
    df = DataFrame(a=1:10, b=10:-1:1, c=11:20)
    @test reverse(view(df, 1:3, 1:2)) == DataFrame(a=[3, 2, 1], b=[8, 9, 10])
    @test reverse(view(df, 1:5, 1:3)) == DataFrame(a=5:-1:1, b=6:10, c=15:-1:11)
    @test reverse(view(df, :, 1:2)) == DataFrame(a=10:-1:1, b=1:10)
    @test typeof(reverse(view(df, :, 1:2))) == DataFrame
end

end # module
