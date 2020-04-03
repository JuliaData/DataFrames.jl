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
    @test d == DataFrame(b=2, d=4)
    DataFrames._check_consistency(d)
    select!(d, Not(:b))
    @test d == DataFrame(d=4)
    DataFrames._check_consistency(d)

    d = copy(df)
    select!(d, Not(r"[aec]"))
    @test d == DataFrame(b=2, d=4)
    DataFrames._check_consistency(d)
    select!(d, Not(r"b"))
    @test d == DataFrame(d=4)
    DataFrames._check_consistency(d)

    d = copy(df)
    select!(d, Not([2, 5, 3]))
    @test d == DataFrame(a=1, d=4)
    DataFrames._check_consistency(d)
    select!(d, Not(2))
    @test d == DataFrame(a=1)
    DataFrames._check_consistency(d)

    d = copy(df)
    select!(d, Not(2:3))
    @test d == DataFrame(a=1, d=4, e=5)
    DataFrames._check_consistency(d)

    d = copy(df)
    select!(d, Not([false, true, true, false, false]))
    @test d == DataFrame(a=1, d=4, e=5)
    DataFrames._check_consistency(d)
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
    @test d == df[:, [:b, :d]]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    df2 = copy(df)
    d = select(df, Not(r"[aec]"))
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]))
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
    @test d == df[:, [:b, :d]]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not(r"[aec]"), copycols=false)
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]), copycols=false)
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

@testset "select Not on SubDataFrame" begin
    df = view(DataFrame(a=1, b=2, c=3, d=4, e=5), :, :)
    @test_throws BoundsError select(df, Not(0))
    @test_throws BoundsError select(df, Not(6))
    @test_throws ArgumentError select(df, Not([1, 1]))
    @test_throws ArgumentError select(df, Not(:f))
    @test_throws BoundsError select(df, Not([true, false]))

    df2 = copy(df)
    d = select(df, Not([:a, :e, :c]))
    @test d isa DataFrame
    @test d == df[:, [:b, :d]]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    df2 = copy(df)
    d = select(df, Not(r"[aec]"))
    @test d isa DataFrame
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b !== df.b
    @test d.d !== df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]))
    @test d isa DataFrame
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
    @test d == df[:, [:b, :d]]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not(r"[aec]"), copycols=false)
    @test d isa SubDataFrame
    @test d == df[:, [:b, :d]]
    @test d == df[:, r"[bd]"]
    @test d.b === df.b
    @test d.d === df.d
    @test df == df2

    d = select(df, Not([2, 5, 3]), copycols=false)
    @test d isa SubDataFrame
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

@testset "select on SubDataFrame" begin
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

@testset "select and select! with multiple selectors passed" begin
    df = DataFrame(rand(10, 4))
    @test select(df, :x2, :x4, All()) == select(df, :x2, :x4, :x1, :x3)
    @test select(df, :x4, Between(:x2, :x4), All()) == select(df, :x4, :x2, :x3, :x1)

    dfv = view(df, :, :)
    @test select(dfv, :x2, :x4, All()) == select(df, :x2, :x4, :x1, :x3)
    @test select(dfv, :x4, Between(:x2, :x4), All()) == select(df, :x4, :x2, :x3, :x1)
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
    @test_throws ArgumentError select(df, [:x1, :x2] => :x3)
    @test_throws ArgumentError select!(df, [:x1, :x2] => :x3)
    @test_throws BoundsError select(df, 0 => :x3)
    @test_throws BoundsError select!(df, 0 => :x3)

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

@testset "select and select! many columns naming" begin
    df = DataFrame(rand(10, 4))
    for fun in (+, ByRow(+)), copycols in [true, false]
        @test select(df, 1 => fun, copycols=copycols) ==
              DataFrame(Symbol("x1_+") => df.x1)
        @test select(df, 1:2 => fun, copycols=copycols) ==
              DataFrame(Symbol("x1_x2_+") => df.x1 + df.x2)
        @test select(df, 1:3 => fun, copycols=copycols) ==
              DataFrame(Symbol("x1_x2_x3_+") => df.x1 + df.x2 + df.x3)
        @test select(df, 1:4 => fun, copycols=copycols) ==
              DataFrame(Symbol("x1_x2_etc_+") => sum.(eachrow(df)))
    end
    for fun in (+, ByRow(+))
        dfc = copy(df)
        select!(dfc, 1 => fun)
        @test dfc == DataFrame(Symbol("x1_+") => df.x1)
        dfc = copy(df)
        select!(dfc, 1:2 => fun)
        @test dfc == DataFrame(Symbol("x1_x2_+") => df.x1 + df.x2)
        dfc = copy(df)
        select!(dfc, 1:3 => fun)
        @test dfc == DataFrame(Symbol("x1_x2_x3_+") => df.x1 + df.x2 + df.x3)
        dfc = copy(df)
        select!(dfc, 1:4 => fun)
        @test dfc == DataFrame(Symbol("x1_x2_etc_+") => sum.(eachrow(df)))
    end
end

@testset "select and select! many different transforms" begin
    df = DataFrame(rand(10, 4))

    df2 = select(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
                 [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)
    @test names(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test df.x2 == df2.x2
    @test df.x2 !== df2.x2
    @test df.x1 == df2.x4
    @test df.x4 !== df2.x1
    @test df2.r1 == df.x1 .^ 2
    @test df2.r1 == df2.r2
    @test df2.x1 == df.x1 + df.x2
    @test df2.x3 == df.x1 ./ df.x2

    @test select(df, [:x1, :x1] => +) == DataFrame(Symbol("x1_x1_+") => 2*df.x1)
    @test select(df, [1, 1] => +) == DataFrame(Symbol("x1_x1_+") => 2*df.x1)

    df2 = select(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
                 [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4, copycols=false)
    @test names(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test df.x2 === df2.x2
    @test df.x1 === df2.x4
    @test df2.r1 == df.x1 .^ 2
    @test df2.r1 == df2.r2
    @test df2.x1 == df.x1 + df.x2
    @test df2.x3 == df.x1 ./ df.x2

    x1, x2, x3, x4 = df.x1, df.x2, df.x3, df.x4
    select!(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
            [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)
    @test names(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test x2 === df.x2
    @test x1 === df.x4
    @test df.r1 == x1 .^ 2
    @test df.r1 == df.r2
    @test df.x1 == x1 + x2
    @test df.x3 == x1 ./ x2
end

@testset "nrow in select" begin
    df_ref = DataFrame(ones(3,4))
    for df in [df_ref, view(df_ref, 1:2, 1:2),
               df_ref[1:2, []], view(df_ref, 1:2, []),
               df_ref[[], 1:2], view(df_ref, [], 1:2)]
        @test select(df, nrow => :z, nrow, [nrow => :z2]) ==
              DataFrame(z=nrow(df), nrow=nrow(df), z2=nrow(df))
        @test_throws ArgumentError select(df, nrow, nrow)
        @test_throws ArgumentError select(df, [nrow])
    end
end

@testset "select and select! reserved return values" begin
    df = DataFrame(x=1)
    df2 = copy(df)
    for retval in [df2, (a=1, b=2), df2[1, :], ones(2,2)]
        @test_throws ArgumentError select(df, :x => x -> retval)
        @test_throws ArgumentError select(df, :x => x -> retval, copycols=false)
        @test_throws ArgumentError select!(df, :x => x -> retval)
        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function = [retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function = [retval])
    end

    for retval in [(1, 2), ones(2,2,2)]
        @test select(df, :x => x -> retval) == DataFrame(x_function = [retval])
        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function = [retval])
        cdf = copy(df)
        select!(cdf, :x => x -> retval)
        @test cdf == DataFrame(x_function = [retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function = [retval])
    end
end

@testset "select and select! empty selection" begin
    df = DataFrame(rand(10, 4))
    x = [1,2,3]

    @test select(df, r"z") == DataFrame()
    @test select(df, r"z" => () -> x) == DataFrame(:function => x)
    @test select(df, r"z" => () -> x)[!, 1] === x # no copy even for copycols=true
    @test_throws MethodError select(df, r"z" => x -> 1)
    @test_throws ArgumentError select(df, r"z" => ByRow(rand))

    @test select(df, r"z", copycols=false) == DataFrame()
    @test select(df, r"z" => () -> x, copycols=false) == DataFrame(:function => x)
    @test select(df, r"z" => () -> x, copycols=false)[!, 1] === x
    @test_throws MethodError select(df, r"z" => x -> 1, copycols=false)
    @test_throws ArgumentError select(df, r"z" => ByRow(rand), copycols=false)

    @test_throws MethodError select!(df, r"z" => x -> 1)
    @test_throws ArgumentError select!(df, r"z" => ByRow(rand))

    if VERSION >= v"1.4"
        @test_throws MethodError select!(df, r"z" => () -> x, copycols=false)
    else
        @test_throws ErrorException select!(df, r"z" => () -> x, copycols=false)
    end

    select!(df, r"z" => () -> x)
    @test df == DataFrame(:function => x)
end

@testset "wrong selection patterns" begin
    df = DataFrame(rand(10, 4))

    @test_throws ArgumentError select(df, "z")
    @test_throws ArgumentError select(df, "z" => :x1)
    @test_throws ArgumentError select(df, "z" => identity)
    @test_throws ArgumentError select(df, "z" => identity => :x1)
end

@testset "select and select! duplicates" begin
    df = DataFrame(rand(10, 4))
    df_ref = copy(df)

    @test_throws ArgumentError select(df, :x1, :x1)
    @test_throws ArgumentError select(df, :x1, :x5)
    @test select(df, :x2, r"x", :x1, :) == df[:, [:x2, :x1, :x3, :x4]]

    @test_throws ArgumentError select(df, :x1, :x2 => :x1)
    @test_throws ArgumentError select(df, :x3 => :x1, :x2 => :x1)
    @test_throws ArgumentError select(df, :x1, :x2 => identity => :x1)
    @test_throws ArgumentError select(df, :x1 => :x1, :x2 => identity => :x1)
    @test_throws ArgumentError select(df, :x3 => identity => :x1, :x2 => identity => :x1)
    @test select(df, [:x1], :x2 => :x1) == DataFrame(x1 = df.x2)

    @test_throws ArgumentError select!(df, :x1, :x1)
    @test_throws ArgumentError select!(df, :x1, :x5)
    @test df == df_ref

    select!(df, :x2, r"x", :x1, :)
    @test df == df_ref[:, [:x2, :x1, :x3, :x4]]

    df = DataFrame(rand(10, 2))
    @test select(df, [:x1, :x1] => -) == DataFrame(Symbol("x1_x1_-") => zeros(10))
    select!(df, [:x1, :x1] => -)
    @test df == DataFrame(Symbol("x1_x1_-") => zeros(10))
end

@testset "SubDataFrame selection" begin
    df = DataFrame(rand(12, 5))
    sdf = view(df, 1:10, 1:4)
    df_ref = copy(sdf)

    @test select(sdf, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
                 [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4) ==
          select(df_ref, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
                 [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)

    for fun in (+, ByRow(+))
        @test select(sdf, 1 => fun) ==
              DataFrame(Symbol("x1_+") => sdf.x1)
        @test select(sdf, 1:2 => fun) ==
              DataFrame(Symbol("x1_x2_+") => sdf.x1 + sdf.x2)
        @test select(sdf, 1:3 => fun) ==
              DataFrame(Symbol("x1_x2_x3_+") => sdf.x1 + sdf.x2 + sdf.x3)
        @test select(sdf, 1:4 => fun) ==
              DataFrame(Symbol("x1_x2_etc_+") => sum.(eachrow(sdf)))
    end

    @test_throws ArgumentError select(sdf, :x1, :x1)
    @test_throws ArgumentError select(sdf, :x1, :x1, copycols=false)
    @test select(sdf, :x1, [:x1]) == sdf[:, [:x1]]
    @test select(sdf, :x1, [:x1]) isa DataFrame
    @test select(sdf, :x1, [:x1], copycols=false) == sdf[:, [:x1]]
    @test select(sdf, :x1, [:x1], copycols=false) isa SubDataFrame
    @test_throws ArgumentError select(sdf, :x1 => :r1, copycols=false)
    @test_throws ArgumentError select(sdf, :x1 => identity => :r1, copycols=false)
end

@testset "pseudo-broadcasting" begin
    df = DataFrame([1 2 3
                    4 5 6])
    df2 = DataFrame([1 2 3])
    df3 = DataFrame(x1=Int[], x2=Int[], x3=Int[])
    for v in [9, Ref(9), view([9], 1)]
        @test select(df, [] => (() -> v) => :a, :, (:) => (+) => :d) ==
              DataFrame([9 1 2 3 6
                         9 4 5 6 15], [:a, :x1, :x2, :x3, :d])
        @test select(df, (:) => (+) => :d, :, r"z" => (() -> v)  => :a) ==
              DataFrame([6  1 2 3 9
                         15 4 5 6 9], [:d, :x1, :x2, :x3, :a])
        @test select(df, [] => (() -> v) => :a, :x1 => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6
                         9 4 15], [:a, :b, :d])
        @test select(df, (:) => (+) => :d, :x1 => :b, [] => (() -> v) => :a) ==
              DataFrame([6  1 9
                         15 4 9], [:d, :b, :a])
        @test select(df, [] => (() -> v) => :a, :x1 => (x -> x) => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6
                         9 4 15], [:a, :b, :d])
        @test select(df, (:) => (+) => :d, :x1 => (x -> x) => :b, [] => (() -> v) => :a) ==
              DataFrame([6  1 9
                         15 4 9], [:d, :b, :a])
        @test select(df2, [] => (() -> v) => :a, :, (:) => (+) => :d) ==
              DataFrame([9 1 2 3 6], [:a, :x1, :x2, :x3, :d])
        @test select(df2, (:) => (+) => :d, :, r"z" => (() -> v)  => :a) ==
              DataFrame([6  1 2 3 9], [:d, :x1, :x2, :x3, :a])
        @test select(df2, [] => (() -> v) => :a, :x1 => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6], [:a, :b, :d])
        @test select(df2, (:) => (+) => :d, :x1 => :b, [] => (() -> v) => :a) ==
              DataFrame([6  1 9], [:d, :b, :a])
        @test select(df2, [] => (() -> v) => :a, :x1 => (x -> x) => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6], [:a, :b, :d])
        @test select(df2, (:) => (+) => :d, :x1 => (x -> x) => :b, [] => (() -> v) => :a) ==
              DataFrame([6  1 9], [:d, :b, :a])
        @test_throws ArgumentError select(df3, [] => (() -> v) => :a, :x1 => x -> [])
        @test_throws ArgumentError select(df3, :x1 => x -> [], [] => (() -> v) => :a)
    end
    @test_throws ArgumentError select(df, [] => (() -> [9]) => :a, :)
    @test_throws ArgumentError select(df, :, [] => (() -> [9]) => :a)
end

@testset "copycols special cases" begin
    df = DataFrame(a=1:3, b=4:6)
    c = [7, 8]
    df2 = select(df, :a => (x -> c) => :c1, :b => (x -> c) => :c2)
    @test df2.c1 === df2.c2
    df2 = select(df, :a => identity => :c1, :a => :c2)
    @test df2.c1 !== df2.c2
    df2 = select(df, :a => identity => :c1)
    @test df2.c1 !== df.a
    df2 = select(df, :a => (x -> df.b) => :c1)
    @test df2.c1 === df.b
    df2 = select(view(df, 1:2, :), :a => parent => :c1)
    @test df2.c1 !== df.a
    df2 = select(view(df, 1:2, :), :a => (x -> view(x, 1:1)) => :c1)
    @test df2.c1 isa Vector
    df2 = select(df, :a, :a => :b, :a => identity => :c, copycols=false)
    @test df2.b === df2.c === df.a
    a = df.a
    select!(df, :a, :a => :b, :a => identity => :c)
    @test df.b === df.c === a
end

@testset "empty select" begin
    df_ref = DataFrame(rand(10, 4))

    for df in (df_ref, view(df_ref, 1:9, 1:3))
        @test ncol(select(df)) == 0
        @test ncol(select(df, copycols=false)) == 0
    end
    select!(df_ref)
    @test ncol(df_ref) == 0
end

@testset "transform and transform!" begin
    df = DataFrame(rand(10,4))

    for dfx in (df, view(df, :, :))
        df2 = transform(dfx, [:x1, :x2] => +, :x2 => :x3)
        @test df2 == select(dfx, :, [:x1, :x2] => +, :x2 => :x3)
        @test df2.x2 == df2.x3
        @test df2.x2 !== df2.x3
        @test dfx.x2 == df2.x3
        @test dfx.x2 !== df2.x3
        @test dfx.x2 !== df2.x2
    end

    df2 = transform(df, [:x1, :x2] => +, :x2 => :x3, copycols=false)
    @test df2 == select(df, :, [:x1, :x2] => +, :x2 => :x3)
    @test df.x2 === df2.x2 === df2.x3
    @test_throws ArgumentError transform(view(df, :, :), [:x1, :x2] => +, :x2 => :x3, copycols=false)

    x2 = df.x2
    transform!(df, [:x1, :x2] => +, :x2 => :x3)
    @test df == df2
    @test x2 === df.x2 === df.x3

    @test transform(df) == df
    df2 = transform(df, copycols=false)
    @test df2 == df
    for (a, b) in zip(eachcol(df), eachcol(df2))
        @test a === b
    end
    cols = collect(eachcol(df))
    transform!(df)
    @test df2 == df
    for (a, b) in zip(eachcol(df), cols)
        @test a === b
    end
end

@testset "vectors of pairs" begin
    df_ref = DataFrame(a=1:3, b=4:6)
    for df in [df_ref, view(df_ref, :, :)]
        @test select(df, [] .=> sum) == DataFrame()
        @test select(df, names(df) .=> sum) == DataFrame(a_sum=6, b_sum=15)
        @test transform(df, names(df) .=> ByRow(-)) ==
              DataFrame(:a => 1:3, :b => 4:6,
                        Symbol("a_-") => -1:-1:-3,
                        Symbol("b_-") => -4:-1:-6)
        @test select(df, :a, [] .=> sum, :b => :x, [:b, :a] .=> identity) ==
              DataFrame(a=1:3, x=4:6, b_identity=4:6, a_identity=1:3)
        @test select(df, names(df) .=> sum .=> [:A, :B]) == DataFrame(A=6, B=15)
        @test Base.broadcastable(ByRow(+)) isa Base.RefValue{ByRow{typeof(+)}}
        @test identity.(ByRow(+)) == ByRow(+)
    end
end

end # module
