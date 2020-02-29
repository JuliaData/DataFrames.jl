module TestSelect

using DataFrames, Test, Random

Random.seed!(1234)

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
    DataFrames._check_consistency(d)

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

@testset "select and select! renaming" begin
    df = DataFrame(rand(10, 4))
    @test select(df, :x1 => :x2, :x2 => :x1) == rename(df[:, 1:2], [:x2, :x1])
    @test select(df, :x2 => :x1, :x1 => :x2) == DataFrame(x1=df.x2, x2=df.x1)
    @test_throws MethodError select(df, [:x1, :x2] => :x3)
    @test_throws MethodError select!(df, [:x1, :x2] => :x3)

    df2 = select(df, :x1 => :x2, :x2 => :x1)
    @test df2.x1 == df.x2
    @test df2.x1 !== df.x2
    df2 = select(df, :x1 => :x2, :x2 => :x1, copycols=false)
    @test df2.x1 === df.x2

    df2 = select(df, :x1, :x1 => :x2)
    @test df2.x1 == df2.x2
    @test df2.x1 !== df2.x2

    df2 = select(df, :x1, :x1 => :x2, copycols=false)
    @test df2.x1 === df2.x2

    x1 = df.x1
    x2 = df.x2
    select!(df, :x1 => :x2, :x2 => :x1)
    @test x1 === df.x2
    @test x2 === df.x1
    @test names(df) == [:x2, :x1]

    df = DataFrame(rand(10, 4))
    select!(df, :x1, :x1 => :x2)
    @test df2.x1 === df2.x2

    df = DataFrame(rand(10, 4))
    df2 = select(df, :, :x1 => :x3)
    @test df2 == DataFrame(eachcol(df)[[1,2,1,4]])
    @test df2.x1 !== df2.x3
    df2 = select(df, :, :x1 => :x3, copycols=false)
    @test df2 == DataFrame(eachcol(df)[[1,2,1,4]])
    @test df2.x1 === df2.x3
    @test select(df, :x1 => :x3, :) == DataFrame(eachcol(df)[[1,1,2,4]],
                                                 [:x3, :x1, :x2, :x4])
    select!(df, :, :x1 => :x3)
    @test df2 == df
    @test all(i -> df2[!, i] === df[!, i], ncol(df2))
end

@testset "select and select! empty selection" begin
    df = DataFrame(rand(10, 4))
    @test select(df, r"z") == DataFrame()
    @test_throws ArgumentError select(df, r"z" => ByRow(rand))
end

@testset "select and select! duplicates" begin
    df = DataFrame(rand(10, 4))
    df_ref = copy(df)

    @test_throws ArgumentError select(df, :x1, :x1)
    @test_throws ArgumentError select(df, :x1, :x5)
    @test select(df, :x2, r"x", :x1, :) == df[:, [:x2, :x1, :x3, :x4]]

    @test_throws ArgumentError select(df, :x1, :x2 => :x1)

    @test_throws ArgumentError select!(df, :x1, :x1)
    @test_throws ArgumentError select!(df, :x1, :x5)
    @test df == df_ref

    select!(df, :x2, r"x", :x1, :)
    @test df == df_ref[:, [:x2, :x1, :x3, :x4]]
end

end # module
