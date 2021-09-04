module TestSelect

using DataFrames, Test, Random, Statistics, CategoricalArrays, PooledArrays

const ≅ = isequal

"""Check if passed data frames are `isequal` and have the same types of columns"""
isequal_coltyped(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && typeof.(eachcol(df1)) == typeof.(eachcol(df2))

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
    @test d == DataFrame(b=2, c=3, d=4, e=5)
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
    @test d == DataFrame(b=2, c=3, d=4, e=5)
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
    @test d == DataFrame(b=2, c=3, d=4, e=5)
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
    @test d == DataFrame(b=2, c=3, d=4, e=5)
    @test d.b === df.b
    @test df == df2
end

@testset "select!" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws BoundsError select!(df, 0)
    @test_throws BoundsError select!(df, 6)
    @test_throws ArgumentError select!(df, [1, 1])
    @test_throws ArgumentError select!(df, :f)
    @test_throws BoundsError select!(df, [true, false])

    @test_throws MethodError select!(view(df, :, :), 1:2)

    d = copy(df, copycols=false)
    @test select!(d, 1:0) == DataFrame()
    @test select!(d, Not(r"")) == DataFrame()

    d = copy(df, copycols=false)
    select!(d, [:a, :e, :c])
    @test propertynames(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, r"[aec]")
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, [true, false, true, false, true])
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = copy(df, copycols=false)
    select!(d, [:d, :e, :a, :c, :b])
    @test propertynames(d) == [:d, :e, :a, :c, :b]
    for i in [:d, :e, :a, :c, :b]
        @test d[!, i] === df[!, i]
    end

    d = copy(df, copycols=false)
    select!(d, [2, 5, 3])
    @test propertynames(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, 2:3)
    @test propertynames(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = copy(df, copycols=false)
    select!(d, 2)
    @test propertynames(d) == [:b]
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
    @test propertynames(d) == [:a, :e, :c]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, r"[aec]")
    @test propertynames(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, [true, false, true, false, true])
    @test propertynames(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.c !== df.c
    @test d.e !== df.e
    @test d.a == df.a
    @test d.c == df.c
    @test d.e == df.e

    d = select(df, [2, 5, 3])
    @test propertynames(d) == [:b, :e, :c]
    @test d.b !== df.b
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.b == df.b
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, 2:3)
    @test propertynames(d) == [:b, :c]
    @test d.b !== df.b
    @test d.c !== df.c
    @test d.b == df.b
    @test d.c == df.c

    d = select(df, 2)
    @test propertynames(d) == [:b]
    @test d.b !== df.b
    @test d.b == df.b

    d = select(df, [:a, :e, :c], copycols=false)
    @test propertynames(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, r"[aec]", copycols=false)
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, [true, false, true, false, true], copycols=false)
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = select(df, [2, 5, 3], copycols=false)
    @test propertynames(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, 2:3, copycols=false)
    @test propertynames(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = select(df, 2, copycols=false)
    @test propertynames(d) == [:b]
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
    @test propertynames(d) == [:a, :e, :c]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, r"[aec]")
    @test d isa DataFrame
    @test propertynames(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.a == df.a
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, [true, false, true, false, true])
    @test d isa DataFrame
    @test propertynames(d) == [:a, :c, :e]
    @test d.a !== df.a
    @test d.c !== df.c
    @test d.e !== df.e
    @test d.a == df.a
    @test d.c == df.c
    @test d.e == df.e

    d = select(df, [2, 5, 3])
    @test d isa DataFrame
    @test propertynames(d) == [:b, :e, :c]
    @test d.b !== df.b
    @test d.e !== df.e
    @test d.c !== df.c
    @test d.b == df.b
    @test d.e == df.e
    @test d.c == df.c

    d = select(df, 2:3)
    @test d isa DataFrame
    @test propertynames(d) == [:b, :c]
    @test d.b !== df.b
    @test d.c !== df.c
    @test d.b == df.b
    @test d.c == df.c

    d = select(df, 2)
    @test d isa DataFrame
    @test propertynames(d) == [:b]
    @test d.b !== df.b
    @test d.b == df.b

    d = select(df, [:a, :e, :c], copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:a, :e, :c]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, r"[aec]", copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, [true, false, true, false, true], copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:a, :c, :e]
    @test d.a === df.a
    @test d.c === df.c
    @test d.e === df.e

    d = select(df, [2, 5, 3], copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:b, :e, :c]
    @test d.b === df.b
    @test d.e === df.e
    @test d.c === df.c

    d = select(df, 2:3, copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:b, :c]
    @test d.b === df.b
    @test d.c === df.c

    d = select(df, 2, copycols=false)
    @test d isa SubDataFrame
    @test propertynames(d) == [:b]
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
    @test_throws BoundsError select!(df, 1:4)
    @test_throws ArgumentError select!(df, [:a, :b, :c, :d])
    @test_throws ArgumentError select!(df, [1, 2, 3, 1])
    @test_throws ArgumentError select!(df, [:a, :b, :c, :a])

    # but this works
    @test select!(copy(df), [:a, :c]) == df[:, [:a, :c]]
    @test select!(copy(df), [:a, :b]) == df[:, [:a, :b]]
    @test select!(copy(df), [1, 3]) == df[:, [1, 3]]
end

@testset "select and select! with multiple selectors passed" begin
    df = DataFrame(rand(10, 4), :auto)
    @test select(df, :x2, :x4, All()) == select(df, :x2, :x4, :x1, :x3)
    @test select(df, :x2, :x4, Cols(:)) == select(df, :x2, :x4, :x1, :x3)
    @test select(df, :x2, :x4, Cols()) == select(df, :x2, :x4)
    @test select(df, :x4, Between(:x2, :x4), All()) == select(df, :x4, :x2, :x3, :x1)

    dfv = view(df, :, :)
    @test select(dfv, :x2, :x4, All()) == select(df, :x2, :x4, :x1, :x3)
    @test select(dfv, :x2, :x4, Cols(:)) == select(df, :x2, :x4, :x1, :x3)
    @test select(dfv, :x2, :x4, Cols()) == select(df, :x2, :x4)
    @test select(dfv, :x4, Between(:x2, :x4), All()) == select(df, :x4, :x2, :x3, :x1)
    @test select(dfv, :x4, Between(:x2, :x4), Cols(:)) == select(df, :x4, :x2, :x3, :x1)
    @test select(dfv, :x4, Between(:x2, :x4), Cols()) == select(df, :x4, :x2, :x3)
    @test select(dfv, :x2, :x4, All()) == select(dfv, :x2, :x4, :x1, :x3)
    @test select(dfv, :x4, Between(:x2, :x4), All()) == select(dfv, :x4, :x2, :x3, :x1)
    @test select(dfv, :x4, Between(:x2, :x4), Cols(:)) == select(dfv, :x4, :x2, :x3, :x1)
    @test select(dfv, :x4, Between(:x2, :x4), Cols()) == select(dfv, :x4, :x2, :x3)

    dfc = copy(df)
    @test select!(dfc, :x2, :x4, All()) == dfc
    @test select!(dfc, :x2, :x4, Cols(:)) == dfc
    @test dfc == select(df, :x2, :x4, :x1, :x3)
    dfc = copy(df)
    @test select!(dfc, :x4, Between(:x2, :x4), All()) == dfc
    @test select!(dfc, :x4, Between(:x2, :x4), Cols(:)) == dfc
    @test dfc == select(df, :x4, :x2, :x3, :x1)

    @test select(df, Not([:x2, :x3]), All()) == select(df, :x1, :x4, :x2, :x3)
    @test select(df, Not([:x2, :x3]), Cols(:)) == select(df, :x1, :x4, :x2, :x3)
end

@testset "select and select! renaming" begin
    df = DataFrame(rand(10, 4), :auto)
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
    @test names(df) == ["x2", "x1"]

    df = DataFrame(rand(10, 4), :auto)
    select!(df, :x1, :x1 => :x2)
    @test df2.x1 === df2.x2

    df = DataFrame(rand(10, 4), :auto)
    df2 = select(df, :, :x1 => :x3)
    @test df2 == DataFrame(collect(eachcol(df))[[1, 2, 1, 4]], :auto)
    @test df2.x1 !== df2.x3
    df2 = select(df, :, :x1 => :x3, copycols=false)
    @test df2 == DataFrame(collect(eachcol(df))[[1, 2, 1, 4]], :auto)
    @test df2.x1 === df2.x3
    @test select(df, :x1 => :x3, :) == DataFrame(collect(eachcol(df))[[1, 1, 2, 4]],
                                                 [:x3, :x1, :x2, :x4])
    select!(df, :, :x1 => :x3)
    @test df2 == df
    @test all(i -> df2[!, i] === df[!, i], ncol(df2))
end

@testset "select and select! many columns naming" begin
    df = DataFrame(rand(10, 4), :auto)
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
    df = DataFrame(rand(10, 4), :auto)

    df2 = select(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
                 [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)
    @test propertynames(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
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
    @test propertynames(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test df.x2 === df2.x2
    @test df.x1 === df2.x4
    @test df2.r1 == df.x1 .^ 2
    @test df2.r1 == df2.r2
    @test df2.x1 == df.x1 + df.x2
    @test df2.x3 == df.x1 ./ df.x2

    x1, x2, x3, x4 = df.x1, df.x2, df.x3, df.x4
    select!(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
            [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)
    @test propertynames(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test x2 === df.x2
    @test x1 === df.x4
    @test df.r1 == x1 .^ 2
    @test df.r1 == df.r2
    @test df.x1 == x1 + x2
    @test df.x3 == x1 ./ x2
end

@testset "nrow in select" begin
    df_ref = DataFrame(ones(3, 4), :auto)
    for df in [df_ref, view(df_ref, 1:2, 1:2),
               df_ref[1:2, []], view(df_ref, 1:2, []),
               df_ref[[], 1:2], view(df_ref, [], 1:2)]
        @test select(df, nrow => :z, nrow, [nrow => :z2]) ==
              repeat(DataFrame(z=nrow(df), nrow=nrow(df), z2=nrow(df)), nrow(df))
        @test combine(df, nrow => :z, nrow, [nrow => :z2]) ==
              DataFrame(z=nrow(df), nrow=nrow(df), z2=nrow(df))
        @test_throws ArgumentError select(df, nrow, nrow)
        @test_throws ArgumentError select(df, [nrow])
    end
end

@testset "select and select! reserved return values" begin
    df = DataFrame(x=1)
    df2 = copy(df)
    for retval in [df2, (a=1, b=2), df2[1, :], ones(2, 2)]
        @test_throws ArgumentError select(df, :x => x -> retval)
        @test_throws ArgumentError select(df, :x => x -> retval, copycols=false)
        @test_throws ArgumentError select!(df, :x => x -> retval)

        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function = [retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function = [retval])

        if retval isa Union{NamedTuple, DataFrameRow}
            @test select(df, :x => ByRow(x -> retval) => AsTable) == DataFrame(;retval...)
        elseif retval isa DataFrame
            @test_throws MethodError select(df, :x => ByRow(x -> retval) => AsTable)
        else # Matrix: wrong type of keys
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => AsTable)
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => [:a, :b, :c, :d])
        end
    end

    for retval in [(1, 2), ones(2, 2, 2)]
        @test select(df, :x => x -> retval) == DataFrame(x_function = [retval])
        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function = [retval])
        if retval isa Tuple
            @test select(df, :x => ByRow(x -> retval) => AsTable) == DataFrame(x1=1, x2=2)
        else
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => AsTable)
        end
        cdf = copy(df)
        select!(cdf, :x => x -> retval)
        @test cdf == DataFrame(x_function = [retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function = [retval])
    end
end

@testset "select and select! empty selection" begin
    df = DataFrame(rand(10, 4), :auto)
    x = [1:10;]
    y = [1, 2, 3]

    @test select(df, r"z") == DataFrame()
    @test select(df, r"z" => () -> x) == DataFrame(:function => x)
    @test_throws ArgumentError select(df, r"z" => () -> y)
    @test combine(df, r"z" => () -> y) == DataFrame(:function => y)
    @test select(df, r"z" => () -> x)[!, 1] === x # no copy even for copycols=true
    @test_throws MethodError select(df, r"z" => x -> 1)
    @test select(df, r"z" => ByRow(() -> 1)) == DataFrame(:function => fill(1, 10))

    @test select(df, r"z", copycols=false) == DataFrame()
    @test select(df, r"z" => () -> x, copycols=false) == DataFrame(:function => x)
    @test select(df, r"z" => () -> x, copycols=false)[!, 1] === x
    @test_throws MethodError select(df, r"z" => x -> 1, copycols=false)
    @test select(df, r"z" => ByRow(() -> 1)) == DataFrame(:function => fill(1, 10), copycols=false)

    @test_throws MethodError select!(df, r"z" => x -> 1)
    @test select!(df, r"z" => ByRow(() -> 1)) == DataFrame(:function => fill(1, 10))
    @test_throws MethodError select!(df, r"z" => () -> x, copycols=false)

    select!(df, r"z" => () -> x)
    @test df == DataFrame(:function => x)
end

@testset "wrong selection patterns" begin
    df = DataFrame(rand(10, 4), :auto)

    @test_throws ArgumentError select(df, "z")
    @test_throws ArgumentError select(df, "z" => :x1)
    @test_throws ArgumentError select(df, "z" => identity)
    @test_throws ArgumentError select(df, "z" => identity => :x1)
end

@testset "select and select! duplicates" begin
    df = DataFrame(rand(10, 4), :auto)
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

    df = DataFrame(rand(10, 2), [:x1, :x2])
    @test select(df, [:x1, :x1] => -) == DataFrame(Symbol("x1_x1_-") => zeros(10))
    select!(df, [:x1, :x1] => -)
    @test df == DataFrame(Symbol("x1_x1_-") => zeros(10))
end

@testset "SubDataFrame selection" begin
    df = DataFrame(rand(12, 5), :auto)
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
                    4 5 6], :auto)
    df2 = DataFrame([1 2 3], :auto)
    df3 = DataFrame(x1=Char[], x2=Int[], x3=Int[])
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
              DataFrame([6 1 2 3 9], [:d, :x1, :x2, :x3, :a])
        @test select(df2, [] => (() -> v) => :a, :x1 => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6], [:a, :b, :d])
        @test select(df2, (:) => (+) => :d, :x1 => :b, [] => (() -> v) => :a) ==
              DataFrame([6 1 9], [:d, :b, :a])
        @test select(df2, [] => (() -> v) => :a, :x1 => (x -> x) => :b, (:) => (+) => :d) ==
              DataFrame([9 1 6], [:a, :b, :d])
        @test select(df2, (:) => (+) => :d, :x1 => (x -> x) => :b, [] => (() -> v) => :a) ==
              DataFrame([6 1 9], [:d, :b, :a])

        @test isequal_coltyped(select(df3, [] => (() -> v) => :a, :x1 => x -> []),
                               DataFrame(a=Int[], x1_function=Any[]))
        @test isequal_coltyped(select(df3, :x1 => x -> [], [] => (() -> v) => :a),
                               DataFrame(x1_function=Any[], a=Int[]))
        @test isequal_coltyped(select(df3, [] => (() -> v) => :a, :x1),
                               DataFrame(a=Int[], x1=Char[]))
        @test isequal_coltyped(select(df3, :x1, [] => (() -> v) => :a),
                               DataFrame(x1=Char[], a=Int[]))
    end
    @test_throws ArgumentError select(df, [] => (() -> [9]) => :a, :)
    @test_throws ArgumentError select(df, :, [] => (() -> [9]) => :a)
    @test transform(df, names(df) .=> (x -> 9) .=> names(df)) ==
          repeat(DataFrame([9 9 9], :auto), nrow(df))
    @test combine(df, names(df) .=> (x -> 9) .=> names(df)) ==
          DataFrame([9 9 9], :auto)
    @test transform(df, names(df) .=> (x -> 9) .=> names(df), :x1 => :x4) ==
          DataFrame([9 9 9 1; 9 9 9 4], :auto)
    @test transform(df3, names(df3) .=> (x -> 9) .=> names(df3)) ==
          repeat(DataFrame([9 9 9], :auto), nrow(df3))
    @test combine(df3, names(df3) .=> (x -> 9) .=> names(df3)) ==
          DataFrame([9 9 9], :auto)
    @test transform(df3, names(df3) .=> (x -> 9) .=> names(df3), :x1 => :x4) ==
          DataFrame(ones(0, 4), :auto)

    df = DataFrame(x1=1:2, x2=categorical(1:2),
                   x3=[missing, 2], x4=categorical([missing, 2]))

    df2 = select(df, names(df) .=> first)
    @test df2 ≅ repeat(DataFrame(x1_first=1, x2_first=1, x3_first=missing,
                                 x4_first=missing), nrow(df2))
    @test df2.x1_first isa Vector{Int}
    @test df2.x2_first isa CategoricalVector{Int}
    @test df2.x3_first isa Vector{Missing}
    @test df2.x4_first isa Vector{Missing}

    df2 = combine(df, names(df) .=> first)
    @test df2 ≅ DataFrame(x1_first=1, x2_first=1, x3_first=missing,
                          x4_first=missing)
    @test df2.x1_first isa Vector{Int}
    @test df2.x2_first isa CategoricalVector{Int}
    @test df2.x3_first isa Vector{Missing}
    @test df2.x4_first isa Vector{Missing}

    df2 = select(df, names(df) .=> last)
    @test df2 ≅ repeat(DataFrame(x1_last=2, x2_last=2, x3_last=2,
                                 x4_last=2), nrow(df2))
    @test df2.x1_last isa Vector{Int}
    @test df2.x2_last isa CategoricalVector{Int}
    @test df2.x3_last isa Vector{Int}
    @test df2.x4_last isa CategoricalVector{Int}

    df2 = combine(df, names(df) .=> last)
    @test df2 ≅ DataFrame(x1_last=2, x2_last=2, x3_last=2,
                          x4_last=2)
    @test df2.x1_last isa Vector{Int}
    @test df2.x2_last isa CategoricalVector{Int}
    @test df2.x3_last isa Vector{Int}
    @test df2.x4_last isa CategoricalVector{Int}

    for v in [:x1, :x1 => (x -> x) => :x1]
        df2 = select(df, names(df) .=> first, v)
        @test df2 ≅ DataFrame(x1_first=1, x2_first=1, x3_first=missing,
                              x4_first=missing, x1=[1, 2])
        @test df2.x1_first isa Vector{Int}
        @test df2.x2_first isa CategoricalVector{Int}
        @test df2.x3_first isa Vector{Missing}
        @test df2.x4_first isa Vector{Missing}


        df2 = select(df, names(df) .=> last, v)
        @test df2 ≅ DataFrame(x1_last=2, x2_last=2, x3_last=2,
                              x4_last=2, x1=[1, 2])
        @test df2.x1_last isa Vector{Int}
        @test df2.x2_last isa CategoricalVector{Int}
        @test df2.x3_last isa Vector{Int}
        @test df2.x4_last isa CategoricalVector{Int}


        df2 = select(df, v, names(df) .=> first)
        @test df2 ≅ DataFrame(x1=[1, 2], x1_first=1, x2_first=1, x3_first=missing,
                              x4_first=missing)
        @test df2.x1_first isa Vector{Int}
        @test df2.x2_first isa CategoricalVector{Int}
        @test df2.x3_first isa Vector{Missing}
        @test df2.x4_first isa Vector{Missing}


        df2 = select(df, v, names(df) .=> last)
        @test df2 ≅ DataFrame(x1=[1, 2], x1_last=2, x2_last=2, x3_last=2,
                              x4_last=2)
        @test df2.x1_last isa Vector{Int}
        @test df2.x2_last isa CategoricalVector{Int}
        @test df2.x3_last isa Vector{Int}
        @test df2.x4_last isa CategoricalVector{Int}
    end

    @test_throws ArgumentError select(df, names(df) .=> first, [] => (() -> Int[]) => :x1)
    df2 = combine(df, names(df) .=> first, [] => (() -> Int[]) => :x1)
    @test size(df2) == (0, 5)
    @test df2.x1_first isa Vector{Int}
    @test df2.x2_first isa CategoricalVector{Int}
    @test df2.x3_first isa Vector{Missing}
    @test df2.x4_first isa Vector{Missing}

    @test_throws ArgumentError select(df, names(df) .=> last, [] => (() -> Int[]) => :x1)
    df2 = combine(df, names(df) .=> last, [] => (() -> Int[]) => :x1)
    @test size(df2) == (0, 5)
    @test df2.x1_last isa Vector{Int}
    @test df2.x2_last isa CategoricalVector{Int}
    @test df2.x3_last isa Vector{Int}
    @test df2.x4_last isa CategoricalVector{Int}

    @test_throws ArgumentError select(df, [] => (() -> Int[]) => :x1, names(df) .=> first)
    df2 = combine(df, [] => (() -> Int[]) => :x1, names(df) .=> first)
    @test size(df2) == (0, 5)
    @test df2.x1_first isa Vector{Int}
    @test df2.x2_first isa CategoricalVector{Int}
    @test df2.x3_first isa Vector{Missing}
    @test df2.x4_first isa Vector{Missing}

    @test_throws ArgumentError select(df, [] => (() -> Int[]) => :x1, names(df) .=> last)
    df2 = combine(df, [] => (() -> Int[]) => :x1, names(df) .=> last)
    @test size(df2) == (0, 5)
    @test df2.x1_last isa Vector{Int}
    @test df2.x2_last isa CategoricalVector{Int}
    @test df2.x3_last isa Vector{Int}
    @test df2.x4_last isa CategoricalVector{Int}
end

@testset "copycols special cases" begin
    df = DataFrame(a=1:3, b=4:6)
    c = [7, 8]
    @test_throws ArgumentError select(df, :a => (x -> c) => :c1, :b => (x -> c) => :c2)
    df2 = combine(df, :a => (x -> c) => :c1, :b => (x -> c) => :c2)
    @test df2.c1 === df2.c2
    df2 = select(df, :a => identity => :c1, :a => :c2)
    @test df2.c1 !== df2.c2
    df2 = select(df, :a => identity => :c1)
    @test df2.c1 !== df.a
    df2 = select(df, :a => (x -> df.b) => :c1)
    @test df2.c1 === df.b
    @test_throws ArgumentError select(view(df, 1:2, :), :a => parent => :c1)
    df2 = combine(view(df, 1:2, :), :a => parent => :c1)
    @test df2.c1 !== df.a
    @test_throws ArgumentError select(view(df, 1:2, :), :a => (x -> view(x, 1:1)) => :c1)
    df2 = combine(view(df, 1:2, :), :a => (x -> view(x, 1:1)) => :c1)
    @test df2.c1 isa Vector
    df2 = select(df, :a, :a => :b, :a => identity => :c, copycols=false)
    @test df2.b === df2.c === df.a
    a = df.a
    select!(df, :a, :a => :b, :a => identity => :c)
    @test df.b === df.c === a
end

@testset "empty select" begin
    df_ref = DataFrame(rand(10, 4), :auto)

    for df in (df_ref, view(df_ref, 1:9, 1:3))
        @test ncol(select(df)) == 0
        @test ncol(select(df, copycols=false)) == 0
    end
    select!(df_ref)
    @test ncol(df_ref) == 0
end

@testset "transform and transform!" begin
    df = DataFrame(rand(10, 4), :auto)

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
    @test df.x2 == df2.x2 == df2.x3
    @test df.x2 === df2.x2
    @test df.x2 !== df2.x3
    @test_throws ArgumentError transform(view(df, :, :), [:x1, :x2] => +, :x2 => :x3, copycols=false)

    x2 = df.x2
    transform!(df, [:x1, :x2] => +, :x2 => :x3)
    @test df == df2
    @test x2 == df.x2 == df.x3
    @test x2 === df.x2
    @test x2 !== df.x3

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
        @test select(df, names(df) .=> sum) == repeat(DataFrame(a_sum=6, b_sum=15), nrow(df))
        @test combine(df, names(df) .=> sum) == DataFrame(a_sum=6, b_sum=15)
        @test transform(df, names(df) .=> ByRow(-)) ==
              DataFrame(:a => 1:3, :b => 4:6,
                        Symbol("a_-") => -1:-1:-3,
                        Symbol("b_-") => -4:-1:-6)
        @test select(df, :a, [] .=> sum, :b => :x, [:b, :a] .=> identity) ==
              DataFrame(a=1:3, x=4:6, b_identity=4:6, a_identity=1:3)
        @test select(df, names(df) .=> sum .=> [:A, :B]) == repeat(DataFrame(A=6, B=15), nrow(df))
        @test combine(df, names(df) .=> sum .=> [:A, :B]) == DataFrame(A=6, B=15)
        @test Base.broadcastable(ByRow(+)) isa Base.RefValue{ByRow{typeof(+)}}
        @test identity.(ByRow(+)) == ByRow(+)
    end
end

@testset "AsTable tests" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    @test select(df, AsTable(:) => sum) ==
          DataFrame(a_b_c_sum=map(sum, eachrow(df)))
    @test transform(df, AsTable(:) => sum) ==
          DataFrame(a=1:3, b=4:6, c=7:9, a_b_c_sum=map(sum, eachrow(df)))

    @test select(df, AsTable(:) => sum ∘ sum) ==
          repeat(DataFrame(a_b_c_sum_sum=45), nrow(df))
    @test combine(df, AsTable(:) => sum ∘ sum) ==
          DataFrame(a_b_c_sum_sum=45)
    @test transform(df, AsTable(:) => sum ∘ sum) ==
          DataFrame(a=1:3, b=4:6, c=7:9, a_b_c_sum_sum=45)

    @test select(df, AsTable(:) => ByRow(x -> [x])) ==
          DataFrame(a_b_c_function=[[(a = 1, b = 4, c = 7)],
                                    [(a = 2, b = 5, c = 8)],
                                    [(a = 3, b = 6, c = 9)]])
    @test transform(df, AsTable(:) => ByRow(x -> [x])) ==
          hcat(df, DataFrame(a_b_c_function=[[(a = 1, b = 4, c = 7)],
                                             [(a = 2, b = 5, c = 8)],
                                             [(a = 3, b = 6, c = 9)]]))
    @test select(df, AsTable(:) => ByRow(identity)) ==
          DataFrame(a_b_c_identity=[(a = 1, b = 4, c = 7), (a = 2, b = 5, c = 8), (a = 3, b = 6, c = 9)])
    @test select(df, AsTable(:) => ByRow(identity) => AsTable) == df
    @test select(df, AsTable(:) => ByRow(x -> df[1, :])) ==
          DataFrame(a_b_c_function=fill(df[1, :], 3))
    @test select(df, AsTable(:) => ByRow(x -> df[1, :]) => AsTable) ==
          DataFrame(a=[1, 1, 1], b=4, c=7)
    @test transform(df, AsTable(Not(:)) =>
          ByRow(identity)) == [df DataFrame(:identity => fill(NamedTuple(), nrow(df)))]

    @test select(df, AsTable(Not(:)) => Ref) == repeat(DataFrame(Ref = NamedTuple()), nrow(df))
    @test combine(df, AsTable(Not(:)) => Ref) == DataFrame(Ref = NamedTuple())
    @test transform(df, AsTable(Not(:)) => Ref) ==
          DataFrame(a=1:3, b=4:6, c=7:9, Ref=NamedTuple())

    if VERSION >= v"1.4.0"
        df = DataFrame(x=[1, 2, missing], y=[1, missing, missing])
        @test transform(df, AsTable(:) .=>
                            ByRow.([sum∘skipmissing,
                                    x -> count(!ismissing, x),
                                    mean∘skipmissing]) .=>
                            [:sum, :n, :mean]) ≅
              [df DataFrame(sum=[2, 2, 0], n=[2, 1, 0], mean=[1, 2, NaN])]
    else
        df = DataFrame(x=[1, 2], y=[1, missing])
        @test transform(df, AsTable(:) .=>
                            ByRow.([sum∘skipmissing,
                                    x -> count(!ismissing, x),
                                    mean∘skipmissing]) .=>
                            [:sum, :n, :mean]) ≅
              [df DataFrame(sum=[2, 2], n=[2, 1], mean=[1, 2])]
    end
end

@testset "make sure select! is safe on error" begin
    a = [1]
    df = DataFrame()
    df.a = a
    @test_throws DomainError select!(df, :a => x -> sqrt(-1))
    @test df.a === a
    @test propertynames(df) == [:a, ]
end

@testset "combine AbstractDataFrame" begin
    df = DataFrame(x=1:3, y=4:6)

    @test combine(x -> Matrix(x), df) == rename(df, [:x1, :x2])
    @test combine(x -> Ref(1:3), df) == DataFrame(x1=[1:3])
    @test combine(df, x -> Ref(1:3)) == DataFrame(x1=[1:3])

    @test_throws ArgumentError combine(df, AsTable(:) => identity)
    @test combine(df, AsTable(:) => identity => AsTable) == df
    @test combine(df, (:) => cor) == DataFrame(x_y_cor = 1.0)
    @test combine(df, :x => x -> Ref(1:3)) == DataFrame(x_function=[1:3])
    @test_throws ArgumentError combine(df, :x => x -> ones(1, 1))
    @test combine(df, :x => (x -> ones(1, 1)) => AsTable) == DataFrame(x1=1.0)

    df2 = combine(df, :x => identity)
    @test df2[:, 1] == df.x
    @test df2[:, 1] !== df.x

    @test combine(df, :x => sum, :y => collect ∘ extrema) ==
          DataFrame(x_sum=[6, 6], y_collect_extrema = [4, 6])
    @test combine(df, :y => collect ∘ extrema, :x => sum) ==
          DataFrame(y_collect_extrema = [4, 6], x_sum=[6, 6])
    @test combine(df, :x => sum, :y => x -> []) ==
          DataFrame(x_sum=[], y_function = [])
    @test combine(df, :y => x -> [], :x => sum) ==
          DataFrame(y_function = [], x_sum=[])

    dfv = view(df, [2, 1], [2, 1])

    @test combine(x -> Matrix(x), dfv) == rename(dfv, [:x1, :x2])

    @test_throws ArgumentError combine(dfv, AsTable(:) => identity)
    @test combine(dfv, AsTable(:) => identity => AsTable) == dfv
    @test combine(dfv, (:) => cor) == DataFrame(y_x_cor = 1.0)

    df2 = combine(dfv, :x => identity)
    @test df2[:, 1] == dfv.x
    @test df2[:, 1] !== dfv.x

    @test combine(dfv, :x => sum, :y => collect ∘ extrema) ==
          DataFrame(x_sum=[3, 3], y_collect_extrema = [4, 5])
    @test combine(dfv, :y => collect ∘ extrema, :x => sum) ==
          DataFrame(y_collect_extrema = [4, 5], x_sum=[3, 3])
end

@testset "select and transform AbstractDataFrame" begin
    df = DataFrame(x=1:3, y=4:6)

    @test select(df) == DataFrame()

    @test select(df, :x => first) == DataFrame(x_first=fill(1, 3))
    df2 = select(df, :x, :x => first, copycols=true)
    @test df2 == DataFrame(x=df.x, x_first=fill(1, 3))
    @test df2.x !== df.x
    df2 = select(df, :x, :x => first, copycols=false)
    @test df2 == DataFrame(x=df.x, x_first=fill(1, 3))
    @test df2.x === df.x
    @test_throws ArgumentError select(df, :x => x -> [first(x)], copycols=true)
    @test_throws ArgumentError select(df, :x => x -> [first(x)], copycols=false)

    df2 = transform(df, :x => first, copycols=true)
    @test df2 == [df DataFrame(x_first=fill(1, 3))]
    @test df2.x !== df.x
    @test df2.y !== df.y
    df2 = transform(df, :x => first, copycols=false)
    @test df2 == [df DataFrame(x_first=fill(1, 3))]
    @test df2.x === df.x
    @test df2.y === df.y
    @test transform(df, names(df) .=> first .=> names(df)) ==
          DataFrame(x=fill(1, 3), y=fill(4, 3))
    @test_throws ArgumentError transform(df, :x => x -> [first(x)], copycols=true)
    @test_throws ArgumentError transform(df, :x => x -> [first(x)], copycols=false)

    dfv = view(df, [2, 1], [2, 1])
    @test select(dfv, :x => first) == DataFrame(x_first=fill(2, 2))
    df2 = select(dfv, :x, :x => first, copycols=true)
    @test df2 == DataFrame(x=dfv.x, x_first=fill(2, 2))
    @test df2.x !== dfv.x
    @test_throws ArgumentError select(dfv, :x, :x => first, copycols=false)
    @test_throws ArgumentError select(dfv, :x => x -> [first(x)], copycols=true)
    @test_throws ArgumentError select(dfv, :x => x -> [first(x)], copycols=false)

    df2 = transform(dfv, :x => first, copycols=true)
    @test df2 == [dfv DataFrame(x_first=fill(2, 2))]
    @test df2.x !== dfv.x
    @test df2.y !== dfv.y
    @test_throws ArgumentError transform(dfv, :x => first, copycols=false)
    @test transform(dfv, names(dfv) .=> first .=> names(dfv)) ==
          DataFrame(y=fill(5, 2), x=fill(2, 2))
    @test_throws ArgumentError transform(df, :x => x -> [first(x)], copycols=true)
    @test_throws ArgumentError transform(df, :x => x -> [first(x)], copycols=false)
end

@testset "select! and transform! AbstractDataFrame" begin
    df = DataFrame(x=1:3, y=4:6)
    select!(df, :x => first)
    @test df == DataFrame(x_first = fill(1, 3))

    # if we select! we do copycols=false, so we can get aliases
    df = DataFrame(x=1:3, y=4:6)
    x = df.x
    select!(df, :x => (x->x), :x)
    @test x === df.x_function === df.x

    df = DataFrame(x=1:3, y=4:6)
    @test_throws ArgumentError select!(df, :x => x -> [1])
    @test df == DataFrame(x=1:3, y=4:6)

    df = DataFrame(x=1:3, y=4:6)
    x = df.x
    y = df.y
    transform!(df, :x => first)
    @test df == DataFrame(x=x, y=y, x_first=fill(1, 3))
    @test df.x == x
    @test df.y == y

    df = DataFrame(x=1:3, y=4:6)
    transform!(df, names(df) .=> first .=> names(df))
    @test df == DataFrame(x=fill(1, 3), y=fill(4, 3))

    df = DataFrame(x=1:3, y=4:6)
    @test_throws ArgumentError transform!(df, :x => x -> [1])
    @test df == DataFrame(x=1:3, y=4:6)

    dfv = view(df, [2, 1], [2, 1])
    @test_throws MethodError select!(dfv, 1)
    @test_throws MethodError transform!(dfv, 1)
end

@testset "renamecols=false tests" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    @test select(df, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test select(df, :a => +, [:a, :b] => +, Cols(:) => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test_throws ArgumentError select(df, [] => () -> 10, renamecols=false)
    @test transform(df, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, b=4:6, c=7:9, d=10:12, a_b=5:2:9, a_b_etc=22:4:30)
    @test combine(df, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test combine(df, [:a, :b] => +, renamecols=false) == DataFrame(a_b=5:2:9)
    @test combine(identity, df, renamecols=false) == df

    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    @test select!(df, :a => +, [:a, :b] => +, All() => +, renamecols=false) == df
    @test df == DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)

    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    @test transform!(df, :a => +, [:a, :b] => +, All() => +, renamecols=false) == df
    @test df == DataFrame(a=1:3, b=4:6, c=7:9, d=10:12, a_b=5:2:9, a_b_etc=22:4:30)

    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    @test transform!(df, :a => +, [:a, :b] => +, Cols(:) => +, renamecols=false) == df
    @test df == DataFrame(a=1:3, b=4:6, c=7:9, d=10:12, a_b=5:2:9, a_b_etc=22:4:30)
end

@testset "transformation function with a function as first argument" begin
    for df in (DataFrame(a=1:2, b=3:4, c=5:6), view(DataFrame(a=1:3, b=3:5, c=5:7, d=11:13), 1:2, 1:3))
        @test select(sdf -> sdf.b, df) == DataFrame(x1=3:4)
        @test select(sdf -> (b = 2sdf.b,), df) == DataFrame(b=[6, 8])
        @test select(sdf -> (b = 1,), df) == DataFrame(b=[1, 1])
        @test_throws ArgumentError select(sdf -> (b = [1],), df)
        @test select(sdf -> (b = [1, 5],), df) == DataFrame(b=[1, 5])
        @test select(sdf -> 1, df) == DataFrame(x1=[1, 1])
        @test select(sdf -> fill([1]), df) == DataFrame(x1=[[1], [1]])
        @test select(sdf -> Ref([1]), df) == DataFrame(x1=[[1], [1]])
        @test select(sdf -> "x", df) == DataFrame(x1=["x", "x"])
        @test select(sdf -> [[1, 2], [3, 4]], df) == DataFrame(x1=[[1, 2], [3, 4]])
        for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0])
            @test select(sdf -> ret, df) == DataFrame()
        end
        @test_throws ArgumentError select(sdf -> DataFrame(a=10), df)
        @test_throws ArgumentError select(sdf -> zeros(1, 2), df)
        @test select(sdf -> DataFrame(a=[10, 11]), df) == DataFrame(a=[10, 11])
        @test select(sdf -> [10 11; 12 13], df) == DataFrame(x1=[10, 12], x2=[11, 13])
        @test select(sdf -> DataFrame(a=10)[1, :], df) == DataFrame(a=[10, 10])

        @test transform(sdf -> sdf.b, df) == [df DataFrame(x1=3:4)]
        @test transform(sdf -> (b = 2sdf.b,), df) == DataFrame(a=1:2, b=[6, 8], c=5:6)
        @test transform(sdf -> (b = 1,), df) == DataFrame(a=[1, 2], b=[1, 1], c=[5, 6])
        @test_throws ArgumentError transform(sdf -> (b = [1],), df)
        @test transform(sdf -> (b = [1, 5],), df) == DataFrame(a=[1, 2], b=[1, 5], c=[5, 6])
        @test transform(sdf -> 1, df) == DataFrame(a=1:2, b=3:4, c=5:6, x1=1)
        @test transform(sdf -> fill([1]), df) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1], [1]])
        @test transform(sdf -> Ref([1]), df) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1], [1]])
        @test transform(sdf -> "x", df) == DataFrame(a=1:2, b=3:4, c=5:6, x1="x")
        @test transform(sdf -> [[1, 2], [3, 4]], df) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1, 2], [3, 4]])
        for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0])
            @test transform(sdf -> ret, df) == df
        end
        @test_throws ArgumentError transform(sdf -> DataFrame(a=10), df)
        @test_throws ArgumentError transform(sdf -> zeros(1, 2), df)
        @test transform(sdf -> DataFrame(a=[10, 11]), df) == DataFrame(a=[10, 11], b=3:4, c=5:6)
        @test transform(sdf -> [10 11; 12 13], df) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[10, 12], x2=[11, 13])
        @test transform(sdf -> DataFrame(a=10)[1, :], df) == DataFrame(a=[10, 10], b=3:4, c=5:6)

        @test combine(sdf -> sdf.b, df) == DataFrame(x1=3:4)
        @test combine(sdf -> (b = 2sdf.b,), df) == DataFrame(b=[6, 8])
        @test combine(sdf -> (b = 1,), df) == DataFrame(b=[1])
        @test combine(sdf -> (b = [1],), df) == DataFrame(b=[1])
        @test combine(sdf -> (b = [1, 5],), df) == DataFrame(b=[1, 5])
        @test combine(sdf -> 1, df) == DataFrame(x1=[1])
        @test combine(sdf -> fill([1]), df) == DataFrame(x1=[[1]])
        @test combine(sdf -> Ref([1]), df) == DataFrame(x1=[[1]])
        @test combine(sdf -> "x", df) == DataFrame(x1=["x"])
        @test combine(sdf -> [[1, 2], [3, 4]], df) == DataFrame(x1=[[1, 2], [3, 4]])
        for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0])
            @test combine(sdf -> ret, df) == DataFrame()
        end
        @test combine(sdf -> DataFrame(a=10), df) == DataFrame(a=10)
        @test combine(sdf -> zeros(1, 2), df) == DataFrame(x1=0, x2=0)
        @test combine(sdf -> DataFrame(a=[10, 11]), df) == DataFrame(a=[10, 11])
        @test combine(sdf -> [10 11; 12 13], df) == DataFrame(x1=[10, 12], x2=[11, 13])
        @test combine(sdf -> DataFrame(a=10)[1, :], df) == DataFrame(a=[10])
    end

    df = DataFrame(a=1:2, b=3:4, c=5:6)
    @test select!(sdf -> sdf.b, copy(df)) == DataFrame(x1=3:4)
    @test select!(sdf -> (b = 2sdf.b,), copy(df)) == DataFrame(b=[6, 8])
    @test select!(sdf -> (b = 1,), copy(df)) == DataFrame(b=[1, 1])
    @test_throws ArgumentError select!(sdf -> (b = [1],), copy(df))
    @test select!(sdf -> (b = [1, 5],), copy(df)) == DataFrame(b=[1, 5])
    @test select!(sdf -> 1, copy(df)) == DataFrame(x1=[1, 1])
    @test select!(sdf -> fill([1]), copy(df)) == DataFrame(x1=[[1], [1]])
    @test select!(sdf -> Ref([1]), copy(df)) == DataFrame(x1=[[1], [1]])
    @test select!(sdf -> "x", copy(df)) == DataFrame(x1=["x", "x"])
    @test select!(sdf -> [[1, 2], [3, 4]], copy(df)) == DataFrame(x1=[[1, 2], [3, 4]])
    for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0])
        @test select!(sdf -> ret, copy(df)) == DataFrame()
    end
    @test_throws ArgumentError select!(sdf -> DataFrame(a=10), copy(df))
    @test_throws ArgumentError select!(sdf -> zeros(1, 2), copy(df))
    @test select!(sdf -> DataFrame(a=[10, 11]), copy(df)) == DataFrame(a=[10, 11])
    @test select!(sdf -> [10 11; 12 13], copy(df)) == DataFrame(x1=[10, 12], x2=[11, 13])
    @test select!(sdf -> DataFrame(a=10)[1, :], copy(df)) == DataFrame(a=[10, 10])

    @test transform!(sdf -> sdf.b, copy(df)) == [df DataFrame(x1=3:4)]
    @test transform!(sdf -> (b = 2sdf.b,), copy(df)) == DataFrame(a=1:2, b=[6, 8], c=5:6)
    @test transform!(sdf -> (b = 1,), copy(df)) == DataFrame(a=[1, 2], b=[1, 1], c=[5, 6])
    @test_throws ArgumentError transform!(sdf -> (b = [1],), copy(df))
    @test transform!(sdf -> (b = [1, 5],), copy(df)) == DataFrame(a=[1, 2], b=[1, 5], c=[5, 6])
    @test transform!(sdf -> 1, copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1=1)
    @test transform!(sdf -> fill([1]), copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1], [1]])
    @test transform!(sdf -> Ref([1]), copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1], [1]])
    @test transform!(sdf -> "x", copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1="x")
    @test transform!(sdf -> [[1, 2], [3, 4]], copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[[1, 2], [3, 4]])
    for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0])
        @test transform!(sdf -> ret, copy(df)) == df
    end
    @test_throws ArgumentError transform!(sdf -> DataFrame(a=10), copy(df))
    @test_throws ArgumentError transform!(sdf -> zeros(1, 2), copy(df))
    @test transform!(sdf -> DataFrame(a=[10, 11]), copy(df)) == DataFrame(a=[10, 11], b=3:4, c=5:6)
    @test transform!(sdf -> [10 11; 12 13], copy(df)) == DataFrame(a=1:2, b=3:4, c=5:6, x1=[10, 12], x2=[11, 13])
    @test transform!(sdf -> DataFrame(a=10)[1, :], copy(df)) == DataFrame(a=[10, 10], b=3:4, c=5:6)

    @test_throws ArgumentError combine(:x => identity, DataFrame(x=[1, 2, 3]))
end

@testset "transformation function with multiple columns as destination" begin
    for df in (DataFrame(a=1:2, b=3:4, c=5:6), view(DataFrame(a=1:3, b=3:5, c=5:7, d=11:13), 1:2, 1:3))
        for fun in (select, combine, transform),
            res in (DataFrame(), DataFrame(a=1, b=2)[1, :], ones(1, 1),
                    (a=1, b=2), (a=[1], b=[2]), (a=1, b=[2]))
            @test_throws ArgumentError fun(df, :a => x -> res)
            @test_throws ArgumentError fun(df, :a => (x -> res) => :z)
        end
        for res in (DataFrame(x1=1, x2=2)[1, :], (x1=1, x2=2))
            @test select(df, :a => (x -> res) => AsTable) == DataFrame(x1=[1, 1], x2=[2, 2])
            @test transform(df, :a => (x -> res) => AsTable) == [df DataFrame(x1=[1, 1], x2=[2, 2])]
            @test combine(df, :a => (x -> res) => AsTable) == DataFrame(x1=[1], x2=[2])
            @test select(df, :a => (x -> res) => [:p, :q]) == DataFrame(p=[1, 1], q=[2, 2])
            @test transform(df, :a => (x -> res) => [:p, :q]) == [df DataFrame(p=[1, 1], q=[2, 2])]
            @test combine(df, :a => (x -> res) => [:p, :q]) == DataFrame(p=[1], q=[2])
            @test_throws ArgumentError select(df, :a => (x -> res) => [:p, :q, :r])
            @test_throws ArgumentError select(df, :a => (x -> res) => [:p])
        end
        for res in (DataFrame(x1=1, x2=2), [1 2], Tables.table([1 2], header=[:x1, :x2]),
                    (x1=[1], x2=[2]))
            @test combine(df, :a => (x -> res) => AsTable) == DataFrame(x1=1, x2=2)
            @test combine(df, :a => (x -> res) => [:p, :q]) == DataFrame(p=1, q=2)
            @test_throws ArgumentError combine(df, :a => (x -> res) => [:p])
            @test_throws ArgumentError select(df, :a => (x -> res) => AsTable)
            @test_throws ArgumentError transform(df, :a => (x -> res) => AsTable)
        end
        @test combine(df, :a => ByRow(x -> [x, x+1]),
                      :a => ByRow(x -> [x, x+1]) => AsTable,
                      :a => ByRow(x -> [x, x+1]) => [:p, :q],
                      :a => ByRow(x -> (s=x, t=x+1)) => AsTable,
                      :a => (x -> (k=x, l=x.+1)) => AsTable,
                      :a => ByRow(x -> (s=x, t=x+1)) => :z) ==
              DataFrame(a_function=[[1, 2], [2, 3]], x1=[1, 2], x2=[2, 3],
                        p=[1, 2], q=[2, 3], s=[1, 2], t=[2, 3], k=[1, 2], l=[2, 3],
                        z=[(s=1, t=2), (s=2, t=3)])
        @test select(df, :a => ByRow(x -> [x, x+1]),
                     :a => ByRow(x -> [x, x+1]) => AsTable,
                     :a => ByRow(x -> [x, x+1]) => [:p, :q],
                     :a => ByRow(x -> (s=x, t=x+1)) => AsTable,
                     :a => (x -> (k=x, l=x.+1)) => AsTable,
                     :a => ByRow(x -> (s=x, t=x+1)) => :z) ==
              DataFrame(a_function=[[1, 2], [2, 3]], x1=[1, 2], x2=[2, 3],
                        p=[1, 2], q=[2, 3], s=[1, 2], t=[2, 3], k=[1, 2], l=[2, 3],
                        z=[(s=1, t=2), (s=2, t=3)])
        @test transform(df, :a => ByRow(x -> [x, x+1]),
                        :a => ByRow(x -> [x, x+1]) => AsTable,
                        :a => ByRow(x -> [x, x+1]) => [:p, :q],
                        :a => ByRow(x -> (s=x, t=x+1)) => AsTable,
                        :a => (x -> (k=x, l=x.+1)) => AsTable,
                        :a => ByRow(x -> (s=x, t=x+1)) => :z) ==
              [df DataFrame(a_function=[[1, 2], [2, 3]], x1=[1, 2], x2=[2, 3],
                            p=[1, 2], q=[2, 3], s=[1, 2], t=[2, 3], k=[1, 2], l=[2, 3],
                            z=[(s=1, t=2), (s=2, t=3)])]
        @test_throws ArgumentError select(df, :a => (x -> [(a=1, b=2), (a=1, b=2, c=3)]) => AsTable)
        @test_throws ArgumentError select(df, :a => (x -> [(a=1, b=2), (a=1, c=3)]) => AsTable)
        @test_throws ArgumentError combine(df, :a => (x -> (a=1, b=2)) => :x)
    end
end

@testset "check correctness of duplicate column names" begin
    for df in (DataFrame(a=1:2, b=3:4, c=5:6), view(DataFrame(a=1:3, b=3:5, c=5:7, d=11:13), 1:2, 1:3))
        @test select(df, :b, :) == DataFrame(b=3:4, a=1:2, c=5:6)
        @test select(df, :b => :c, :) == DataFrame(c=3:4, a=1:2, b=3:4)
        @test_throws ArgumentError select(df, :b => [:c, :d], :)
        @test_throws ArgumentError select(df, :a, :a => x -> (a=[1, 2], b=[3, 4]))
        @test_throws ArgumentError select(df, :a, :a => (x -> (a=[1, 2], b=[3, 4])) => AsTable)
        @test select(df, [:b, :a], :a => (x -> (a=[11, 12], b=[13, 14])) => AsTable, :) ==
              DataFrame(b=[13, 14], a=[11, 12], c=[5, 6])
        @test select(df, [:b, :a], :a => (x -> (a=[11, 12], b=[13, 14])) => [:b, :a], :) ==
              DataFrame(b=[11, 12], a=[13, 14], c=[5, 6])
    end
end

@testset "empty ByRow" begin
    df = DataFrame(a=1:3)

    @test select(df, [] => ByRow(() -> 1)) == DataFrame("function" => [1, 1, 1])
    @test combine(df, [] => ByRow(() -> 1)) == DataFrame("function" => [1, 1, 1])
    @test transform(df, [] => ByRow(() -> 1)) == DataFrame("a" => 1:3, "function" => [1, 1, 1])

    for df in (DataFrame(), DataFrame(a=[]))
        @test select(df, [] => ByRow(() -> 1)) == DataFrame("function" => [])
        @test combine(df, [] => ByRow(() -> 1)) == DataFrame("function" => [])
        if ncol(df) == 0
            @test transform(df, [] => ByRow(() -> 1)) == DataFrame("function" => [])
        else
            @test transform(df, [] => ByRow(() -> 1)) == DataFrame("a" => [], "function" => [])
        end
        @test eltype(select(df, [] => ByRow(() -> 1)).function) == Int
        @test eltype(combine(df, [] => ByRow(() -> 1)).function) == Int
        @test eltype(transform(df, [] => ByRow(() -> 1)).function) == Int

        @test isequal_coltyped(select(df, [] => ByRow(() -> (a=1, b="1")) => AsTable),
                               DataFrame(a=Int[], b=String[]))
        @test isequal_coltyped(select(df, [] => ByRow(() -> (a=1, b="1")) => [:p, :q]),
                               DataFrame(p=Int[], q=String[]))

        # here this follows Tables.jl behavior
        @test select(df, [] => ByRow(() -> [1, "1"]) => AsTable) == DataFrame()
        @test_throws ArgumentError select(df, [] => ByRow(() -> [1, "1"]) => [:p, :q])
        @test isequal_coltyped(select(df, [] => ByRow(() -> (1, "1")) => AsTable),
                               DataFrame(Column1=Int[], Column2=String[]))
        @test isequal_coltyped(select(df, [] => ByRow(() -> (1, "1")) => [:p, :q]),
                               DataFrame(p=Int[], q=String[]))
    end

    @test select(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("function" => [1, 1, 1])
    @test combine(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("function" => [1, 1, 1])
    @test transform(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("a" => 1:3, "function" => [1, 1, 1])

    for df in (DataFrame(), DataFrame(a=[]))
        @test select(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("function" => [])
        @test combine(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("function" => [])
        if ncol(df) == 0
            @test transform(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("function" => [])
        else
            @test transform(df, AsTable([]) => ByRow(x -> 1)) == DataFrame("a" => [], "function" => [])
        end
        @test eltype(select(df, AsTable([]) => ByRow(x -> 1)).function) == Int
        @test eltype(combine(df, AsTable([]) => ByRow(x -> 1)).function) == Int
        @test eltype(transform(df, AsTable([]) => ByRow(x -> 1)).function) == Int

        @test isequal_coltyped(select(df, AsTable([]) => ByRow(x -> (a=1, b="1")) => AsTable),
                               DataFrame(a=Int[], b=String[]))
        @test isequal_coltyped(select(df, AsTable([]) => ByRow(x -> (a=1, b="1")) => [:p, :q]),
                               DataFrame(p=Int[], q=String[]))

        # here this follows Tables.jl behavior
        @test select(df, [] => ByRow(() -> [1, "1"]) => AsTable) == DataFrame()
        @test_throws ArgumentError select(df, [] => ByRow(() -> [1, "1"]) => [:p, :q])
        @test isequal_coltyped(select(df, [] => ByRow(() -> (1, "1")) => AsTable),
                               DataFrame(Column1=Int[], Column2=String[]))
        @test isequal_coltyped(select(df, [] => ByRow(() -> (1, "1")) => [:p, :q]),
                               DataFrame(p=Int[], q=String[]))
    end
end

@testset "selection special cases" begin
    df = DataFrame(a=1)
    @test_throws ArgumentError select(df, '1' => sum => "b")
    @test_throws ArgumentError select(df, '1' => sum)
    @test select(df, ["a", "a"] => (+) => "b") == DataFrame(b=2)
    @test_throws ArgumentError combine(df, :a => (x -> (a=1, b=[2])) => AsTable)
    @test_throws ArgumentError combine(:, df)
    @test_throws ArgumentError select(:, df)
    @test_throws ArgumentError select!(:, df)
    @test_throws ArgumentError transform(:, df)
    @test_throws ArgumentError transform!(:, df)
    @test combine(df, :a => (x -> 1) => :x1, :a => (x -> [1, 2]) => :x2) ==
          DataFrame(x1=1, x2=[1, 2])
    @test isequal_coltyped(combine(df, :a => (x -> 1) => :x1, :a => (x -> []) => :x2),
                           DataFrame(x1=Int[], x2=[]))
end

@testset "test resizing via a vector of columns after scalars" begin
    df = DataFrame(a=1:2)
    @test combine(df, :a => (x -> 1) => :a1, :a => (x -> 2) => :a2, [:a]) ==
          DataFrame(a1=1, a2=2, a=1:2)
    @test select(df, :a => (x -> 1) => :a1, :a => (x -> 2) => :a2, [:a]) ==
          DataFrame(a1=1, a2=2, a=1:2)
    @test_throws ArgumentError combine(df, :a => (x -> 1) => :a1, :a => (x -> [2]) => :a2, [:a])
    @test_throws ArgumentError select(df, :a => (x -> 1) => :a1, :a => (x -> [2]) => :a2, [:a])
end

@testset "normalize_selection" begin
    @test DataFrames.normalize_selection(DataFrames.Index(Dict(:a => 1, :b => 2), [:a, :b]),
                                         [:a] => sum, false) == (1 => (sum => :a))

    @test DataFrames.normalize_selection(DataFrames.Index(Dict(:a => 1, :b => 2), [:a, :b]),
                                         [:a] => sum => [:new],  false) == (1 => (sum => [:new]))

    # Test that target col strings are converted to Symbols
    @test DataFrames.normalize_selection(DataFrames.Index(Dict(:a => 1, :b => 2), [:a, :b]),
                                         [:a] => sum => ["new"],  false) == (1 => (sum => [:new]))
end

@testset "copying in transform when renaming" begin
    for oldcol in (:a, "a", 1), newcol in (:b, "b")
        df = DataFrame(a=1)
        df2 = transform(df, oldcol => newcol)
        @test df2.b == df2.a == df.a
        @test df2.b !== df2.a
        @test df2.b !== df.a
        @test df2.a !== df.a

        df2 = transform(df, oldcol => newcol, copycols=false)
        @test df2.b == df2.a == df.a
        @test df2.b !== df2.a
        @test df2.b !== df.a
        @test df2.a === df.a

        a = df.a
        transform!(df, oldcol => newcol)
        @test df.b == df.a == a
        @test df.b !== df.a
        @test df.b !== a
        @test df.a === a
    end
end

@testset ":col => AsTable and :col => cols" begin
    df = DataFrame(id=1:2, c1=[(a=1, b=2), (a=3, b=4)], c2=[(11, 12), (13, 14)])
    @test select(df, :c1 => AsTable) == DataFrame(a=[1, 3], b=[2, 4])
    @test select(df, :c1 => [:p, :q]) == DataFrame(p=[1, 3], q=[2, 4])
    @test select(df, :c2 => AsTable) == DataFrame(x1=[11, 13], x2=[12, 14])
    @test select(df, :c2 => [:p, :q]) == DataFrame(p=[11, 13], q=[12, 14])
    @test_throws ArgumentError select(df, [:c1, :c2] => AsTable)
    @test_throws ArgumentError select(df, [:c1, :c2] => AsTable)
    gdf = groupby(df, :id)
    @test select(gdf, :c1 => AsTable) == DataFrame(id=1:2, a=[1, 3], b=[2, 4])
    @test select(gdf, :c1 => [:p, :q]) == DataFrame(id=1:2, p=[1, 3], q=[2, 4])
    @test select(gdf, :c2 => AsTable) == DataFrame(id=1:2, x1=[11, 13], x2=[12, 14])
    @test select(gdf, :c2 => [:p, :q]) == DataFrame(id=1:2, p=[11, 13], q=[12, 14])
    @test_throws ArgumentError select(gdf, [:c1, :c2] => AsTable)
    @test_throws ArgumentError select(gdf, [:c1, :c2] => AsTable)
end

@testset "ByRow on PooledArray calls function on each entry" begin
    id = 0
    df = DataFrame(a=PooledArray([1, 1, 1]))
    function f(x)
        id += 1
        return id
    end
    @test select(df, :a => ByRow(f) => :a) == DataFrame(a=1:3)
end

end # module
