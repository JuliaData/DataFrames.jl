module TestDataFrame

using Dates, DataFrames, Statistics, Random, Test, Logging
using DataFrames: _columns, index
const ≅ = isequal
const ≇ = !isequal

# randomized test from https://github.com/JuliaData/DataFrames.jl/pull/1974
@testset "randomized tests for rename!" begin
    n = Symbol.('a':'z')
    Random.seed!(1234)
    for k in 1:20
        sn = shuffle(n)
        df = DataFrame(zeros(1,26), n)
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
            @test sort(names(df)) == n
            @test sort(collect(keys(index(df).lookup))) == n
            @test sort(collect(values(index(df).lookup))) == 1:26
            @test all(index(df).lookup[x] == i for (i,x) in enumerate(names(df)))
            i -= 1
            names(df) == n && break
        end
        @test i == 0
    end
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
                   b=Union{DataFrame, Missing}[DataFrame(c = 1), DataFrame(d = 2)])
    dfc = copy(df)
    dfdc = deepcopy(df)

    df[1, :a] = 4
    df[1, :b][!, :e] .= 5
    names!(df, [:f, :g])

    @test names(dfc) == [:a, :b]
    @test names(dfdc) == [:a, :b]

    @test dfc[1, :a] === 2
    @test dfdc[1, :a] === 2

    @test names(dfc[1, :b]) == [:c, :e]
    @test names(dfdc[1, :b]) == [:c]
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
    @test_throws BoundsError insertcols!(df, 5, :newcol => ["a", "b"], )
    @test_throws DimensionMismatch insertcols!(df, 1, :newcol => ["a"])
    @test insertcols!(df, 1, :newcol => ["a", "b"]) == df
    @test names(df) == [:newcol, :a, :b]
    @test df[!,:a] == [1, 2]
    @test df[!, :b] == [3.0, 4.0]
    @test df[!, :newcol] == ["a", "b"]

    @test insertcols!(df, 1, :newcol => ["a1", "b1"], makeunique=true) == df
    @test names(df) == [:newcol_1, :newcol, :a, :b]
    @test df[!,:a] == [1, 2]
    @test df[!, :b] == [3.0, 4.0]
    @test df[!, :newcol] == ["a", "b"]
    @test df[!, :newcol_1] == ["a1", "b1"]

    df = DataFrame(a=[1,2], a_1=[3,4])
    @test_throws ArgumentError insertcols!(df, 1, :a => [11,12])
    df = DataFrame(a=[1,2], a_1=[3,4])
    insertcols!(df, 1, :a => [11,12], makeunique=true)
    @test names(df) == [:a_2, :a, :a_1]
    insertcols!(df, 4, :a => [11,12], makeunique=true)
    @test names(df) == [:a_2, :a, :a_1, :a_3]
    @test_throws BoundsError insertcols!(df, 10, :a => [11,12], makeunique=true)
    df = DataFrame(a=[1,2], a_1=[3,4])
    insertcols!(df, 1, :a => 11, makeunique=true)
    @test names(df) == [:a_2, :a, :a_1]
    insertcols!(df, 4, :a => 11, makeunique=true)
    @test names(df) == [:a_2, :a, :a_1, :a_3]
    @test_throws BoundsError insertcols!(df, 10, :a => 11, makeunique=true)

    df = DataFrame(x = 1:2)
    @test insertcols!(df, 2, y=2:3) == DataFrame(x=1:2, y=2:3)
    @test_throws ArgumentError insertcols!(df, 2)
    @test_throws ArgumentError insertcols!(df, 2, a=1, b=2)

    df = DataFrame()
    @test insertcols!(df, 1, x=[1]) == DataFrame(x = [1])
end

@testset "DataFrame constructors" begin
    df = convert(DataFrame, zeros(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[!, 1]) == Vector{Float64}
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, ones(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[!, 1]) == Vector{Float64}
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, Matrix{Float64}(undef, 10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[!, 1]) == Vector{Float64}
    @test typeof(df[:, 1]) == Vector{Float64}

    @test DataFrame([Union{Int, Missing}[1, 2, 3], Union{Float64, Missing}[2.5, 4.5, 6.5]],
                    [:A, :B]) ==
        DataFrame(A = Union{Int, Missing}[1, 2, 3], B = Union{Float64, Missing}[2.5, 4.5, 6.5])

    # This assignment was missing before
    df = DataFrame(Column = [:A])
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

    @test hash(convert(DataFrame, [1 2; 3 4])) == hash(convert(DataFrame, [1 2; 3 4]))
    @test hash(convert(DataFrame, [1 2; 3 4])) != hash(convert(DataFrame, [1 3; 2 4]))
    @test hash(convert(DataFrame, [1 2; 3 4])) == hash(convert(DataFrame, [1 2; 3 4]), zero(UInt))
end

@testset "push!(df, row)" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df = DataFrame(first=[1,2,3], second=["apple","orange","pear"])

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    dfc = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, Any[3,"pear"])
    @test df == dfb

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, (3,"pear"))
    @test df == dfb

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws InexactError push!(dfb, (33.33,"pear"))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    @test_throws ArgumentError push!(dfb, (1,"2",3))
    @test dfc == dfb

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, ("coconut",22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, (11,22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, Dict(:first=>3, :second=>"pear"))
    @test df == dfb

    df = DataFrame(first=[1,2,3], second=["apple","orange","banana"])
    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, Dict(:first=>3, :second=>"banana"))
    @test df == dfb

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, (first=3, second="banana"))
    @test df == dfb

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1,2], second=["apple","orange"])
    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, (second=3, first=3))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    push!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1,2], second=["apple","orange"])
    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>true, :second=>false))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df0 = DataFrame(first=[1,2], second=["apple","orange"])
    dfb = DataFrame(first=[1,2], second=["apple","orange"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>"stuff"))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=[1,2,3], second=["apple","orange","pear"])
    dfb = DataFrame(first=[1,2,3], second=["apple","orange","pear"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=["1","2","3"], second=["apple","orange","pear"])
    dfb = DataFrame(first=["1","2","3"], second=["apple","orange","pear"])
    with_logger(sl) do
        @test_throws MethodError push!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df = DataFrame(x=1)
    push!(df, Dict(:x=>2), Dict(:x=>3))
    @test df[!, :x] == [1,2,3]

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
    df.a = [1,2,3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError push!(df, [1,2])
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1,b=2))
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
    @test_throws AssertionError push!(df, dfc[1, :])
    @test df == dfc

    df = DataFrame()
    df.a = [1,2,3,4]
    df.b = df.a
    df.c = [1,2,3,4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError push!(df, [1,2,3])
    end
    @test df == dfc
    @test occursin("Error adding value to column :a", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1,b=2,c=3))
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
    @test_throws AssertionError push!(df, dfc[1, :])
    @test df == dfc

    df = DataFrame(a=1, b=2)
    push!(df, [1 2])
    @test df == DataFrame(a=[1, 1], b=[2, 2])
    push!(df, (1, 2))
    @test df == DataFrame(a=[1, 1, 1], b=[2, 2, 2])

    @test_logs (:warn, r"In the future push! will not allow passing collections of type") push!(df, "ab")
end

@testset "select! Not" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws BoundsError select!(df, Not(0))
    @test_throws BoundsError select!(df, Not(6))
    @test_throws ArgumentError select!(df, Not([1, 1]))
    @test_throws ArgumentError select!(df, Not(:f))
    @test_throws BoundsError select!(df, Not([true, false]))

    d = copy(df)
    select!(d, Not([:a, :e, :c]))
    @test names(d) == [:b, :d]
    select!(d, Not(:b))
    @test d == DataFrame(d=4)

    d = copy(df)
    select!(d, Not(r"[aec]"))
    @test names(d) == [:b, :d]
    select!(d, Not(r"b"))
    @test d == DataFrame(d=4)

    d = copy(df)
    select!(d, Not([2, 5, 3]))
    @test names(d) == [:a, :d]
    select!(d, Not(2))
    @test d == DataFrame(a=1)

    d = copy(df)
    select!(d, Not(2:3))
    @test d == DataFrame(a=1, d=4, e=5)

    d = copy(df)
    select!(d, Not([false, true, true, false, false]))
    @test d == DataFrame(a=1, d=4, e=5)
end

@testset "select Not" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws BoundsError select(df, Not(0))
    @test_throws BoundsError select(df, Not(6))
    @test_throws ArgumentError select(df, Not([1, 1]))
    @test_throws ArgumentError select(df, Not(:f))
    @test_throws BoundsError select(df, Not([true, false]))

    df2 = copy(df)
    d = select(df, Not([:a, :e, :c]))
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    df2 = copy(df)
    d = select(df, Not(r"[aec]"))
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]))
    @test names(d) == [:a, :d]
    @test d.a !== df.a
    @test d.d !== df.d
    @test d == df[:, [:a, :d]]
    @test df == df2

    d = select(df, Not(2:3))
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a !== df.a
    @test d.d !== df.d
    @test d.e !== df.e
    @test df == df2

    d = select(df, Not([false, true, true, false, false]))
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a !== df.a
    @test d.d !== df.d
    @test d.e !== df.e
    @test df == df2

    d = select(df, Not(1))
    @test d == DataFrame(b=2,c=3,d=4,e=5)
    @test d.b !== df.b
    @test d.b == df.b
    @test df == df2

    d = select(df, Not([:a, :e, :c]), copycols=false)
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not(r"[aec]"), copycols=false)
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]), copycols=false)
    @test names(d) == [:a, :d]
    @test d.a === df.a
    @test d.d === df.d
    @test d == df[:, [:a, :d]]
    @test df == df2

    d = select(df, Not(2:3), copycols=false)
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a === df.a
    @test d.d === df.d
    @test d.e === df.e
    @test df == df2

    d = select(df, Not([false, true, true, false, false]), copycols=false)
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a === df.a
    @test d.d === df.d
    @test d.e === df.e
    @test df == df2

    d = select(df, Not(1), copycols=false)
    @test d == DataFrame(b=2,c=3,d=4,e=5)
    @test d.b === df.b
    @test df == df2
end

@testset "select Not view" begin
    df = view(DataFrame(a=1, b=2, c=3, d=4, e=5), :, :)
    @test_throws BoundsError select(df, Not(0))
    @test_throws BoundsError select(df, Not(6))
    @test_throws ArgumentError select(df, Not([1, 1]))
    @test_throws ArgumentError select(df, Not(:f))
    @test_throws BoundsError select(df, Not([true, false]))

    df2 = copy(df)
    d = select(df, Not([:a, :e, :c]))
    @test d isa DataFrame
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    df2 = copy(df)
    d = select(df, Not(r"[aec]"))
    @test d isa DataFrame
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]))
    @test d isa DataFrame
    @test names(d) == [:a, :d]
    @test d.a !== df.a
    @test d.d !== df.d
    @test d == df[:, [:a, :d]]
    @test df == df2

    d = select(df, Not(2:3))
    @test d isa DataFrame
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a !== df.a
    @test d.d !== df.d
    @test d.e !== df.e
    @test df == df2

    d = select(df, Not([false, true, true, false, false]))
    @test d isa DataFrame
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a !== df.a
    @test d.d !== df.d
    @test d.e !== df.e
    @test df == df2

    d = select(df, Not(1))
    @test d isa DataFrame
    @test d == DataFrame(b=2,c=3,d=4,e=5)
    @test d.b !== df.b
    @test d.b == df.b
    @test df == df2

    d = select(df, Not([:a, :e, :c]), copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not(r"[aec]"), copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:b, :d]
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]), copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:a, :d]
    @test d.a === df.a
    @test d.d === df.d
    @test d == df[:, [:a, :d]]
    @test df == df2

    d = select(df, Not(2:3), copycols=false)
    @test d isa SubDataFrame
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a === df.a
    @test d.d === df.d
    @test d.e === df.e
    @test df == df2

    d = select(df, Not([false, true, true, false, false]), copycols=false)
    @test d isa SubDataFrame
    @test d == DataFrame(a=1, d=4, e=5)
    @test d.a === df.a
    @test d.d === df.d
    @test d.e === df.e
    @test df == df2

    d = select(df, Not(1), copycols=false)
    @test d isa SubDataFrame
    @test d == DataFrame(b=2,c=3,d=4,e=5)
    @test d.b === df.b
    @test df == df2
end

@testset "select!" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws ArgumentError select!(df, 0)
    @test_throws ArgumentError select!(df, 6)
    @test_throws ArgumentError select!(df, [1, 1])
    @test_throws ArgumentError select!(df, :f)
    @test_throws BoundsError select!(df, [true, false])

    @test_throws MethodError select!(view(df, :, :), 1:2)

    d = copy(df, copycols=false)
    @test select!(d, 1:0) == DataFrame()
    @test select!(d, Not(r"")) == DataFrame()

    d = copy(df, copycols=false)
    select!(d, [:a, :e, :c])
    @test names(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, r"[aec]")
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, [true, false, true, false, true])
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = copy(df, copycols=false)
    select!(d, [:d, :e, :a, :c, :b])
    @test names(d) == [:d, :e, :a, :c, :b]
    for i in [:d, :e, :a, :c, :b]
        @test d[!, i] === df[!, i]
    end

    d = copy(df, copycols=false)
    select!(d, [2, 5, 3])
    @test names(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, 2:3)
    @test names(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, 2)
    @test names(d) == [:b]
    @test d.b === df.b
end

@testset "select" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws BoundsError select(df, 0)
    @test_throws BoundsError select(df, 6)
    @test_throws ArgumentError select(df, [1, 1])
    @test_throws ArgumentError select(df, :f)
    @test_throws BoundsError select!(df, [true, false])

    @test select(df, 1:0) == DataFrame()
    @test select(df, Not(r"")) == DataFrame()
    @test select(df, 1:0, copycols=false) == DataFrame()
    @test select(df, Not(r""), copycols=false) == DataFrame()

    d = select(df, [:a, :e, :c])
    @test names(d) == [:a, :e, :c]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, r"[aec]")
    @test names(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, [true, false, true, false, true])
    @test names(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.c !== df.c
    @test d.e !== df.e
    @test d.a == df.a
    @test d.c == df.c
    @test d.e == df.e

    d = select(df, [2, 5, 3])
    @test names(d) == [:b, :e, :c]
    @test d.b !== df.b
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.b == df.b
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, 2:3)
    @test names(d) == [:b, :c]
    @test d.b !== df.b
    @test d.c !== df.c
    @test d.b == df.b
    @test d.c == df.c

    d = select(df, 2)
    @test names(d) == [:b]
    @test d.b !== df.b
    @test d.b == df.b

    d = select(df, [:a, :e, :c], copycols=false)
    @test names(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, r"[aec]", copycols=false)
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, [true, false, true, false, true], copycols=false)
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = select(df, [2, 5, 3], copycols=false)
    @test names(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, 2:3, copycols=false)
    @test names(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = select(df, 2, copycols=false)
    @test names(d) == [:b]
    @test d.b === df.b
end

@testset "select view" begin
    df = view(DataFrame(a=1, b=2, c=3, d=4, e=5), :, :)
    @test_throws BoundsError select(df, 0)
    @test_throws BoundsError select(df, 6)
    @test_throws ArgumentError select(df, [1, 1])
    @test_throws ArgumentError select(df, :f)
    @test_throws MethodError select!(df, [true, false])

    @test select(df, 1:0) == DataFrame()
    @test select(df, Not(r"")) == DataFrame()
    @test select(df, 1:0, copycols=false) == DataFrame()
    @test select(df, Not(r""), copycols=false) == DataFrame()

    d = select(df, [:a, :e, :c])
    @test d isa DataFrame
    @test names(d) == [:a, :e, :c]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, r"[aec]")
    @test d isa DataFrame
    @test names(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, [true, false, true, false, true])
    @test d isa DataFrame
    @test names(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.c !== df.c
    @test d.e !== df.e
    @test d.a == df.a
    @test d.c == df.c
    @test d.e == df.e

    d = select(df, [2, 5, 3])
    @test d isa DataFrame
    @test names(d) == [:b, :e, :c]
    @test d.b !== df.b
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.b == df.b
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, 2:3)
    @test d isa DataFrame
    @test names(d) == [:b, :c]
    @test d.b !== df.b
    @test d.c !== df.c
    @test d.b == df.b
    @test d.c == df.c

    d = select(df, 2)
    @test d isa DataFrame
    @test names(d) == [:b]
    @test d.b !== df.b
    @test d.b == df.b

    d = select(df, [:a, :e, :c], copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, r"[aec]", copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, [true, false, true, false, true], copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = select(df, [2, 5, 3], copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, 2:3, copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = select(df, 2, copycols=false)
    @test d isa SubDataFrame
    @test names(d) == [:b]
    @test d.b === df.b
end

@testset "deleterows!" begin
    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    @test deleterows!(df, 1) === df
    @test df == DataFrame(a=[2], b=[4.0])

    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    @test deleterows!(df, 2) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=[1, 2, 3], b=[3.0, 4.0, 5.0])
    @test deleterows!(df, 2:3) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=[1, 2, 3], b=[3.0, 4.0, 5.0])
    @test deleterows!(df, [2, 3]) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test deleterows!(df, 1) === df
    @test df == DataFrame(a=[2], b=[4.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2], b=Union{Float64, Missing}[3.0, 4.0])
    @test deleterows!(df, 2) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2, 3], b=Union{Float64, Missing}[3.0, 4.0, 5.0])
    @test deleterows!(df, 2:3) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Missing}[1, 2, 3], b=Union{Float64, Missing}[3.0, 4.0, 5.0])
    @test deleterows!(df, [2, 3]) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame()
    @test_throws BoundsError deleterows!(df, 10)
    @test_throws BoundsError deleterows!(df, [10])

    df = DataFrame(a=[])
    @test_throws BoundsError deleterows!(df, 10)
    # the exception type changed between Julia 1.0.2 and Julia 1.1
    # so we use their supertype below
    @test_throws Exception deleterows!(df, [10])

    df = DataFrame(a=[1, 2, 3], b=[3, 2, 1])
    @test_throws ArgumentError deleterows!(df, [3,2])
    @test_throws ArgumentError deleterows!(df, [2,2])
    @test deleterows!(df, [false, true, false]) === df
    @test df == DataFrame(a=[1, 3], b=[3, 1])

    x = [1, 2, 3]
    df = DataFrame(x=x)
    @test deleterows!(df, 1) == DataFrame(x=[2, 3])
    @test x == [1, 2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x)
    @test deleterows!(df, [1]) == DataFrame(x=[2, 3])
    @test x == [1, 2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x)
    @test deleterows!(df, 1:1) == DataFrame(x=[2, 3])
    @test x == [1, 2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x)
    @test deleterows!(df, [true, false, false]) == DataFrame(x=[2, 3])
    @test x == [1, 2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test deleterows!(df, 1) == DataFrame(x=[2, 3])
    @test x == [2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test deleterows!(df, [1]) == DataFrame(x=[2, 3])
    @test x == [2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test deleterows!(df, 1:1) == DataFrame(x=[2, 3])
    @test x == [2, 3]

    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test deleterows!(df, [true, false, false]) == DataFrame(x=[2, 3])
    @test x == [2, 3]
end

@testset "describe" begin
    # Construct the test dataframe
    df = DataFrame(number = [1, 2, 3, 4],
                   number_missing = [1,2, 3, missing],
                   string = ["a", "b", "c", "d"],
                   string_missing = ["a", "b", "c", missing],
                   dates  = Date.([2000, 2001, 2003, 2004]),
                   catarray = CategoricalArray([1,2,1,2]))

    describe_output = DataFrame(variable = [:number, :number_missing, :string,
                                            :string_missing, :dates, :catarray],
                                mean = [2.5, 2.0, nothing, nothing, nothing, nothing],
                                std = [std(df[!, :number]), 1.0, nothing,
                                       nothing, nothing, nothing],
                                min = [1.0, 1.0, "a", "a", Date(2000), 1],
                                q25 = [1.75, 1.5, nothing, nothing, nothing, nothing],
                                median = [2.5, 2.0, nothing, nothing, nothing, nothing],
                                q75 = [3.25, 2.5, nothing, nothing, nothing, nothing],
                                max = [4.0, 3.0, "d", "c", Date(2004), 2],
                                nunique = [nothing, nothing, 4, 3, 4, 2],
                                nmissing = [nothing, 1, nothing, 1, nothing, nothing],
                                first = [1, 1, "a", "a", Date(2000), 1],
                                last = [4, missing, "d", missing, Date(2004), 2],
                                eltype = [Int, Union{Missing, Int}, String,
                                          Union{Missing, String}, Date, CategoricalValue{Int, UInt32}])

    default_fields = [:mean, :min, :median, :max, :nunique, :nmissing, :eltype]

    # Test that it works as a whole, without keyword arguments
    @test describe_output[:, [:variable; default_fields]] == describe(df)

    # Test that it works with one stats argument
    @test describe_output[:, [:variable, :mean]] == describe(df, :mean)

    # Test that it works with all keyword arguments
    @test describe_output ≅ describe(df, :all)

    # Test that it works on a custom function
    describe_output.test_std = describe_output.std
    # Test that describe works with a Pair and a symbol
    @test describe_output[:, [:variable, :mean, :test_std]] ≅ describe(df, :mean, :test_std => std)

    # Test that describe works with a dataframe with no observations
    df = DataFrame(a = Int[], b = String[], c = [])
    @test describe(df, :mean) ≅ DataFrame(variable = [:a, :b, :c],
                                          mean = [NaN, nothing, nothing])

    @test_throws ArgumentError describe(df, :mean, :all)
end

@testset "append!" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)
    df = DataFrame(A = 1:2, B = 1:2)
    df2 = DataFrame(A = 1:4, B = 1:4)
    @test append!(df, DataFrame(A = 3:4, B = [3.0, 4.0])) == df2
    with_logger(sl) do
        @test_throws InexactError append!(df, DataFrame(A = 3:4, B = [3.5, 4.5]))
    end
    @test df == df2
    @test occursin("Error adding value to column B", String(take!(buf)))
    with_logger(sl) do
        @test_throws MethodError append!(df, DataFrame(A = 3:4, B = ["a", "b"]))
    end
    @test df == df2
    @test occursin("Error adding value to column B", String(take!(buf)))
    @test_throws ArgumentError append!(df, DataFrame(A = 1:4, C = 1:4))
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
    df.a = [1,2,3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError append!(df, dfc)
    end
    @test df == dfc
    @test occursin("Error adding value to column a", String(take!(buf)))

    df = DataFrame()
    df.a = [1,2,3,4]
    df.b = df.a
    df.c = [1,2,3,4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError append!(df, dfc)
    end
    @test df == dfc
    @test occursin("Error adding value to column a", String(take!(buf)))

    names!(df, [:a, :b, :z])
    @test_throws ArgumentError append!(df, dfc)
end

@testset "test categorical!" begin
    df = DataFrame(A = Vector{Union{Int, Missing}}(1:3), B = Vector{Union{Int, Missing}}(4:6))
    DRT = CategoricalArrays.DefaultRefType
    @test all(c -> isa(c, Vector{Union{Int, Missing}}), eachcol(categorical!(deepcopy(df))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), [1,2])))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), [:A,:B])))
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), [:A]))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), :A))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), [1]))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), 1))) == 1

    @test all(c -> isa(c, Vector{Union{Int, Missing}}), eachcol(categorical!(deepcopy(df))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), Not(Not([1,2])))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), Not(Not([:A,:B])))))
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not([:A]))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not(:A))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not([1]))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not(1))))) == 1
end

@testset "categorical!" begin
    df = DataFrame([["a", "b"], ['a', 'b'], [true, false], 1:2, ["x", "y"]])
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df)))),
                  [CategoricalArrays.CategoricalString{UInt32},
                   Char, Bool, Int,
                   CategoricalArrays.CategoricalString{UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), :))),
                  [CategoricalArrays.CategoricalString{UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalString{UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), compress=true))),
                  [CategoricalArrays.CategoricalString{UInt8},
                   Char, Bool, Int,
                   CategoricalArrays.CategoricalString{UInt8}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), names(df)))),
                  [CategoricalArrays.CategoricalString{UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalString{UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), names(df), compress=true))),
                  [CategoricalArrays.CategoricalString{UInt8},
                   CategoricalArrays.CategoricalValue{Char,UInt8},
                   CategoricalArrays.CategoricalValue{Bool,UInt8},
                   CategoricalArrays.CategoricalValue{Int,UInt8},
                   CategoricalArrays.CategoricalString{UInt8}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Not(1:0)))),
                  [CategoricalArrays.CategoricalString{UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalString{UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Not(1:0), compress=true))),
                  [CategoricalArrays.CategoricalString{UInt8},
                   CategoricalArrays.CategoricalValue{Char,UInt8},
                   CategoricalArrays.CategoricalValue{Bool,UInt8},
                   CategoricalArrays.CategoricalValue{Int,UInt8},
                   CategoricalArrays.CategoricalString{UInt8}]))

    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Integer))),
                  [String, Char,
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   String]))

    df = DataFrame([["a", missing]])
    categorical!(df)
    @test df.x1 isa CategoricalVector{Union{Missing, String}}

    df = DataFrame(x1=[1, 2])
    categorical!(df)
    @test df.x1 isa Vector{Int}
    categorical!(df, :)
    @test df.x1 isa CategoricalVector{Int}
end

@testset "rename" begin
    df = DataFrame(A = 1:3, B = 'A':'C')
    @test names(rename(df, :A => :A_1)) == [:A_1, :B]
    @test names(df) == [:A, :B]
    @test names(rename(df, :A => :A_1, :B => :B_1)) == [:A_1, :B_1]
    @test names(df) == [:A, :B]
    @test names(rename(df, [:A => :A_1, :B => :B_1])) == [:A_1, :B_1]
    @test names(df) == [:A, :B]
    @test names(rename(df, Dict(:A => :A_1, :B => :B_1))) == [:A_1, :B_1]
    @test names(df) == [:A, :B]
    @test names(rename(x->Symbol(lowercase(string(x))), df)) == [:a, :b]
    @test names(df) == [:A, :B]

    @test rename!(df, :A => :A_1) === df
    @test names(df) == [:A_1, :B]
    @test rename!(df, :A_1 => :A_2, :B => :B_2) === df
    @test names(df) == [:A_2, :B_2]
    @test rename!(df, [:A_2 => :A_3, :B_2 => :B_3]) === df
    @test names(df) == [:A_3, :B_3]
    @test rename!(df, Dict(:A_3 => :A_4, :B_3 => :B_4)) === df
    @test names(df) == [:A_4, :B_4]
    @test rename!(x->Symbol(lowercase(string(x))), df) === df
    @test names(df) == [:a_4, :b_4]

    df = DataFrame(A = 1:3, B = 'A':'C', C = [:x, :y, :z])
    @test rename!(df, :A => :B, :B => :A) === df
    @test names(df) == [:B, :A, :C]
    @test rename!(df, :A => :B, :B => :A, :C => :D) === df
    @test names(df) == [:A, :B, :D]
    @test rename!(df, :A => :B, :B => :C, :D => :A) === df
    @test names(df) == [:B, :C, :A]
    @test rename!(df, :A => :C, :B => :A, :C => :B) === df
    @test names(df) == [:A, :B, :C]
    @test rename!(df, :A => :A, :B => :B, :C => :C) === df
    @test names(df) == [:A, :B, :C]

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
end

@testset "size" begin
    df = DataFrame(A = 1:3, B = 'A':'C')
    @test_throws ArgumentError size(df, 3)
    @test ndims(df) == 2
    @test ndims(typeof(df)) == 2
    @test (nrow(df), ncol(df)) == (3, 2)
    @test size(df) == (3, 2)
    @inferred nrow(df)
    @inferred ncol(df)
end

@testset "description" begin
    df = DataFrame(A = 1:10)

    @test first(df) == df[1, :]
    @test last(df) == df[end, :]
    @test_throws BoundsError first(DataFrame(x=[]))
    @test_throws BoundsError last(DataFrame(x=[]))

    @test first(df, 6) == DataFrame(A = 1:6)
    @test first(df, 1) == DataFrame(A = 1)
    @test last(df, 6) == DataFrame(A = 5:10)
    @test last(df, 1) == DataFrame(A = 10)
end

@testset "column conversions" begin
    df = DataFrame([collect(1:10), collect(1:10)])
    @test !isa(df[!, 1], Vector{Union{Int, Missing}})
    @test allowmissing!(df, 1) === df
    @test isa(df[!, 1], Vector{Union{Int, Missing}})
    @test !isa(df[!, 2], Vector{Union{Int, Missing}})
    df[1,1] = missing
    @test_throws MethodError disallowmissing!(df, 1)
    tmpcol = df[!, 1]
    disallowmissing!(df, 1, error=false)
    @test df[!, 1] === tmpcol
    df[1,1] = 1
    @test disallowmissing!(df, 1) === df
    @test isa(df[!, 1], Vector{Int})

    df = DataFrame([collect(1:10), collect(1:10)])
    @test !isa(df[!, 1], Vector{Union{Int, Missing}})
    @test allowmissing!(df, Not(Not(1))) === df
    @test isa(df[!, 1], Vector{Union{Int, Missing}})
    @test !isa(df[!, 2], Vector{Union{Int, Missing}})
    df[1,1] = missing
    @test_throws MethodError disallowmissing!(df, Not(Not(1)))
    tmpcol = df[!, 1]
    disallowmissing!(df, Not(Not(1)), error=false)
    @test df[!, 1] === tmpcol
    df[1,1] = 1
    @test disallowmissing!(df, Not(Not(1))) === df
    @test isa(df[!, 1], Vector{Int})

    for em in [true, false]
        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df, [1,2]) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, [1,2], error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df, Not(Not([1,2]))) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, Not(Not([1,2])), error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test_throws BoundsError allowmissing!(df, [true])
        @test allowmissing!(df, [true, true]) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test_throws BoundsError disallowmissing!(df, [true], error=em)
        @test disallowmissing!(df, [true,true], error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df, :) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, :, error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df, r"") === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, r"", error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})

        df = DataFrame([collect(1:10), collect(1:10)])
        @test allowmissing!(df, Not(1:0)) === df
        @test isa(df[!, 1], Vector{Union{Int, Missing}}) && isa(df[!, 2], Vector{Union{Int, Missing}})
        @test disallowmissing!(df, Not(1:0), error=em) === df
        @test isa(df[!, 1], Vector{Int}) && isa(df[!, 2], Vector{Int})
    end

    df = DataFrame([CategoricalArray(1:10),
                    CategoricalArray(string.('a':'j'))])
    @test allowmissing!(df) === df
    @test all(x->x <: CategoricalVector, typeof.(eachcol(df)))
    @test eltype(df[!, 1]) <: Union{CategoricalValue{Int}, Missing}
    @test eltype(df[!, 2]) <: Union{CategoricalString, Missing}
    df[1,2] = missing
    @test_throws MissingException disallowmissing!(df)
    tmpcol =df[!, 2]
    disallowmissing!(df, error=false)
    @test df[!, 2] === tmpcol
    df[1,2] = "a"
    @test disallowmissing!(df) === df
    @test all(x->x <: CategoricalVector, typeof.(eachcol(df)))
    @test eltype(df[!, 1]) <: CategoricalValue{Int}
    @test eltype(df[!, 2]) <: CategoricalString

    for em in [true, false]
        df = DataFrame(b=[1,2], c=[1,2], d=[1,2])
        @test allowmissing!(df, [:b, :c]) === df
        @test eltype(df.b) == Union{Int, Missing}
        @test eltype(df.c) == Union{Int, Missing}
        @test eltype(df.d) == Int
        @test disallowmissing!(df, :c, error=em) === df
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

    df = DataFrame(x=[1], y = Union{Int,Missing}[1], z=[missing])
    disallowmissing!(df, error=false)
    @test eltype(df.x) == Int
    @test eltype(df.y) == Int
    @test eltype(df.z) == Missing

    df = DataFrame(x=[1], y = Union{Int,Missing}[1], z=[missing])
    disallowmissing!(df, 2:3, error=false)
    @test eltype(df.x) == Int
    @test eltype(df.y) == Int
    @test eltype(df.z) == Missing
end

@testset "test disallowmissing" begin
    df = DataFrame(x=Union{Int,Missing}[1,2,3],
                   y=Union{Int,Missing}[1,2,3],
                   z=[1,2,3])
    for x in [df, view(df, :, :)], em in [true, false]
        y = disallowmissing(x, error=em)
        @test y isa DataFrame
        @test x == y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test eltype.(eachcol(y)) == [Int, Int, Int]

        for colsel in [:, names(x), [1,2,3], [true,true,true], r"", Not(r"a")]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Int, Int, Int]
        end

        for colsel in [:x, 1, [:x], [1], [true, false, false], r"x", Not(2:3)]
            y = disallowmissing(x, colsel, error=em)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Int, Union{Missing, Int}, Int]
        end

        for colsel in [:z, 3, [:z], [3], [false, false, true], r"z", Not(1:2)]
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

    @test_throws MethodError disallowmissing(DataFrame(x=[missing]))
    @test disallowmissing(DataFrame(x=[missing]), error=false) ≅ DataFrame(x=[missing])
    @test_throws MethodError disallowmissing(DataFrame(x=[1, missing]))
    @test disallowmissing(DataFrame(x=[1, missing]), error=false) ≅ DataFrame(x=[1, missing])

    df = DataFrame(x=[1], y = Union{Int,Missing}[1], z=[missing])
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
    df = DataFrame(x=Union{Int,Missing}[1,2,3],
                   y=[1,2,3],
                   z=[1,2,3])
    for x in [df, view(df, :, :)]
        y = allowmissing(x)
        @test y isa DataFrame
        @test x == y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test eltype.(eachcol(y)) == fill(Union{Missing, Int}, 3)

        for colsel in [:, names(x), [1,2,3], [true,true,true], r"", Not(r"a")]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == fill(Union{Missing, Int}, 3)
        end

        for colsel in [:x, 1, [:x], [1], [true, false, false], r"x", Not(2:3)]
            y = allowmissing(x, colsel)
            @test y isa DataFrame
            @test x == y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test eltype.(eachcol(y)) == [Union{Missing, Int}, Int, Int]
        end

        for colsel in [:z, 3, [:z], [3], [false, false, true], r"z", Not(1:2)]
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

@testset "test categorical" begin
    df = DataFrame(x=["a", "b", "c"],
                   y=["a", "b", missing],
                   z=[1,2,3])
    for x in [df, view(df, :, :)]
        y = categorical(x)
        @test y isa DataFrame
        @test x ≅ y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test y.x isa CategoricalVector{String}
        @test y.y isa CategoricalVector{Union{Missing, String}}
        @test y.z isa Vector{Int}

        y = categorical(x, Int)
        @test y isa DataFrame
        @test x ≅ y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test y.x isa Vector{String}
        @test y.y isa Vector{Union{Missing, String}}
        @test y.z isa CategoricalVector{Int}

        for colsel in [:, names(x), [1,2,3], [true,true,true], r"", Not(r"a")]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa CategoricalVector{String}
            @test y.y isa CategoricalVector{Union{Missing, String}}
            @test y.z isa CategoricalVector{Int}
        end

        for colsel in [:x, 1, [:x], [1], [true, false, false], r"x", Not(2:3)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa CategoricalVector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa Vector{Int}
        end

        for colsel in [:z, 3, [:z], [3], [false, false, true], r"z", Not(1:2)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa Vector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa CategoricalVector{Int}
        end

        for colsel in [Int[], Symbol[], [false, false, false], r"a", Not(:)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa Vector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa Vector{Int}
        end
    end
end

@testset "similar" begin
    df = DataFrame(a = ["foo"],
                   b = CategoricalArray(["foo"]),
                   c = [0.0],
                   d = CategoricalArray([0.0]))
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
    @test df[:, [:y,:x]][!, :x] == x
    @test df[:, [:y,:x]][!, :x] !== x
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
    z = DataFrame(rand(4,5))
    x = z
    y = deepcopy(x)
    @test x[:, end] == x[:, 5]
    @test x[:, end:end] == x[:, 5:5]
    @test x[end, :] == x[4, :]
    @test x[end:end, :] == x[4:4, :]
    @test x[end, end] == x[4,5]
    @test x[2:end, 2:end] == x[2:4,2:5]
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
    x[end,end] = 1000
    y[4,5] = 1000
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
    @test x[end, end] == x[4,5]
    @test x[2:end, 2:end] == x[2:4,2:5]
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
    x[end,end] = 1000
    y[4,5] = 1000
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

@testset "select! on all columns" begin
    a, b, c = 1:5, 2:6, 3:7
    original = DataFrame(a=a, b=b, c=c)

    df = deepcopy(original)
    expected = deepcopy(original)
    @test select!(df, [:a, :b, :c]) === df
    @test df == expected
    @test select!(df, 1:3) === df
    @test df == expected

    df = deepcopy(original)
    expected = DataFrame(b=b, c=c, a=a)
    select!(df, [:b, :c, :a]) === df
    @test df == expected
    df = deepcopy(original)
    select!(df, [2, 3, 1]) === df
    @test df == expected

    df = deepcopy(original)
    expected = DataFrame(c=c, a=a, b=b)
    select!(df, [:c, :a, :b]) === df
    @test df == expected
    df = deepcopy(original)
    select!(df, [3, 1, 2]) === df
    @test df == expected

    df = deepcopy(original)
    expected = DataFrame(a=a, c=c, b=b)
    select!(df, [:a, :c, :b]) === df
    @test df == expected
    df = deepcopy(original)
    select!(df, [1, 3, 2]) === df
    @test df == expected

    df = deepcopy(original)
    expected = DataFrame(b=b, a=a, c=c)
    select!(df, [:b, :a, :c]) === df
    @test df == expected
    df = deepcopy(original)
    select!(df, [2, 1, 3]) === df
    @test df == expected

    df = deepcopy(original)
    expected = DataFrame(c=c, b=b, a=a)
    select!(df, [:c, :b, :a]) === df
    @test df == expected
    df = deepcopy(original)
    select!(df, [3, 2, 1]) === df
    @test df == expected

    df = DataFrame(a=a, b=b, c=c)
    @test_throws ArgumentError select!(df, 1:4)
    @test_throws ArgumentError select!(df, [:a, :b, :c, :d])
    @test_throws ArgumentError select!(df, [1, 2, 3, 1])
    @test_throws ArgumentError select!(df, [:a, :b, :c, :a])

    # but this works
    @test select!(copy(df), [:a, :c]) == df[:, [:a, :c]]
    @test select!(copy(df), [:a, :b]) == df[:, [:a, :b]]
    @test select!(copy(df), [1, 3]) == df[:, [1, 3]]
end

@testset "getproperty, setproperty! and propertynames" begin
    x = collect(1:10)
    y = collect(1.0:10.0)
    z = collect(10:-1:1)
    df = DataFrame(x=x, y=y, copycols=false)

    @test Base.propertynames(df) == names(df)

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
    x = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    v = DataFrame(a = [5, 6, 7], b = [8, 9, 10])
    z = vcat(v, x)
    @test_throws ArgumentError z[:, [1, 1, 2]]
end

@testset "parent, size and axes" begin
    x = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    @test parent(x) === x
    @test parentindices(x) === (Base.OneTo(3), Base.OneTo(2))
    @test size(x) == (3,2)
    @test size(x, 1) == 3
    @test size(x, 2) == 2
    @test_throws ArgumentError size(x, 3)
    @test axes(x) === (Base.OneTo(3), Base.OneTo(2))
    @test axes(x, 1) === Base.OneTo(3)
    @test axes(x, 2) === Base.OneTo(2)
    @test_throws ArgumentError axes(x, 3)
    @test size(DataFrame()) == (0,0)
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

@testset "And and Between tests" begin
    # we check dispatch here only
    df = DataFrame(a=1, b=2, c=3)
    completecases(df, All())
    completecases(df, Between(1, 2))
    dropmissing(df, All())
    dropmissing(df, Between(1, 2))
    dropmissing!(df, All())
    dropmissing!(df, Between(1, 2))
    disallowmissing(df, All())
    disallowmissing(df, Between(1, 2))
    allowmissing(df, All())
    allowmissing(df, Between(1, 2))
    categorical(df, All())
    categorical(df, Between(1, 2))

    df[1, All()]
    df[1, Between(1,2)]
    df[1:1, All()]
    df[1:1, Between(1,2)]
    df[Not(1), All()]
    df[Not(1), Between(1,2)]
    df[:, All()]
    df[:, Between(1,2)]
    df[!, All()]
    df[!, Between(1,2)]

    @view df[1, All()]
    @view df[1, Between(1,2)]
    @view df[1:1, All()]
    @view df[1:1, Between(1,2)]
    @view df[Not(1), All()]
    @view df[Not(1), Between(1,2)]
    @view df[:, All()]
    @view df[:, Between(1,2)]
    @view df[!, All()]
    @view df[!, Between(1,2)]

    df[1, All()] = (a=1, b=2, c=3)
    df[1, Between(1,2)] = (a=1, b=2)
    df[1:1, All()] = df
    df[1:1, Between(1,2)] = df[!, 1:2]
    df[:, All()] = df
    df[:, Between(1,2)] = df[!, 1:2]
    df[1:1, All()] = Matrix(df)
    df[1:1, Between(1,2)] = Matrix(df[!, 1:2])
    df[:, All()] = Matrix(df)
    df[:, Between(1,2)] = Matrix(df[!, 1:2])

    df2 = vcat(df, df)
    df2[Not(1), All()] = df
    df2[Not(1), Between(1,2)] = df[!, 1:2]
    df2[Not(1), All()] = Matrix(df)
    df2[Not(1), Between(1,2)] = Matrix(df[!,1:2])

    allowmissing!(df2, All())
    allowmissing!(df2, Between(1,2))
    disallowmissing!(df2, All())
    disallowmissing!(df2, Between(1,2))
    categorical!(df2, All())
    categorical!(df2, Between(1,2))

    dfr = df[1, :]
    dfr[All()]
    dfr[Between(1,2)]
    dfr[All()] = (a=1, b=2, c=3)
    dfr[Between(1,2)] = (a=1, b=2)
    @view dfr[All()]
    @view dfr[Between(1,2)]

    dfv = view(df, :, :)

    dfv[1, All()]
    dfv[1, Between(1,2)]
    dfv[1:1, All()]
    dfv[1:1, Between(1,2)]
    dfv[Not(1), All()]
    dfv[Not(1), Between(1,2)]
    dfv[:, All()]
    dfv[:, Between(1,2)]
    dfv[!, All()]
    dfv[!, Between(1,2)]

    @view dfv[1, All()]
    @view dfv[1, Between(1,2)]
    @view dfv[1:1, All()]
    @view dfv[1:1, Between(1,2)]
    @view dfv[Not(1), All()]
    @view dfv[Not(1), Between(1,2)]
    @view dfv[:, All()]
    @view dfv[:, Between(1,2)]
    @view dfv[!, All()]
    @view dfv[!, Between(1,2)]

# TODO: enable after setindex! rules update
#    dfv[1, All()] = (a=1, b=2, c=3)
#    dfv[1, Between(1,2)] = (a=1, b=2)
    dfv[1:1, All()] = df
    dfv[1:1, Between(1,2)] = df[!, 1:2]
    dfv[:, All()] = df
    dfv[:, Between(1,2)] = df[!, 1:2]
    dfv[1:1, All()] = Matrix(df)
    dfv[1:1, Between(1,2)] = Matrix(df[!, 1:2])
    dfv[:, All()] = Matrix(df)
    dfv[:, Between(1,2)] = Matrix(df[!, 1:2])

    df2v = view(vcat(df, df), :, :)
    df2v[Not(1), All()] = df
    df2v[Not(1), Between(1,2)] = df[!, 1:2]
    df2v[Not(1), All()] = Matrix(df)
    df2v[Not(1), Between(1,2)] = Matrix(df[!, 1:2])
end

@testset "select and select! with multiple columns passed" begin
    df = DataFrame(rand(10, 4))
    @test select(df, :x2, :x4, All()) == select(df, :x2, :x4, :x1, :x3)
    @test select(df, :x4, Between(:x2, :x4), All()) == select(df, :x4, :x2, :x3, :x1)

    dfv = view(df, :, :)
    @test select(dfv, :x2, :x4, All()) == select(dfv, :x2, :x4, :x1, :x3)
    @test select(dfv, :x4, Between(:x2, :x4), All()) == select(dfv, :x4, :x2, :x3, :x1)

    dfc = copy(df)
    @test select!(dfc, :x2, :x4, All()) == dfc
    @test dfc == select(df, :x2, :x4, :x1, :x3)
    dfc = copy(df)
    @test select!(dfc, :x4, Between(:x2, :x4), All()) == dfc
    @test dfc == select(df, :x4, :x2, :x3, :x1)

    @test select(df, Not([:x2, :x3]), All()) == select(df, :x1, :x4, :x2, :x3)
end

end # module
