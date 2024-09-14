module TestSelect

using DataFrames, Test, Random, SparseArrays, Statistics
using CategoricalArrays, PooledArrays
using ShiftedArrays: lag, ShiftedVector

const ≅ = isequal

isequal_coltyped(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && typeof.(eachcol(df1)) == typeof.(eachcol(df2))

isequal_coltyped(v1::AbstractVector, v2::AbstractVector) =
    isequal(v1, v2) && typeof(v1) == typeof(v2)

const ≃ = isequal_coltyped

Random.seed!(1234)

@testset "select! Not" begin
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws BoundsError select!(df, Not(0))
    @test_throws BoundsError select!(df, Not(6))
    @test_throws ArgumentError select!(df, Not(:f))
    @test_throws BoundsError select!(df, Not([true, false]))

    @test select!(copy(df), Not([1, 1])) == df[!, 2:end]

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
    @test_throws ArgumentError select(df, Not(:f))
    @test_throws BoundsError select(df, Not([true, false]))

    @test select(df, Not([1, 1])) == df[!, 2:end]

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
    @test_throws ArgumentError select(df, Not(:f))
    @test_throws BoundsError select(df, Not([true, false]))

    @test select(df, Not([1, 1])) == df[!, 2:end]

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

    @test select!(view(df, :, :), 1:2) == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
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
    @test_throws ArgumentError select(df, 1.0)
    @test_throws ArgumentError select(df, true)
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
    @test_throws BoundsError select!(df, [true, false])

    df = view(DataFrame(a=1, b=2, c=3, d=4, e=5), :, :)
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
    @test df2.x1 == df2.x2
    @test df2.x1 !== df2.x2

    x1 = df.x1
    x2 = df.x2
    select!(df, :x1 => :x2, :x2 => :x1)
    @test x1 === df.x2
    @test x2 === df.x1
    @test names(df) == ["x2", "x1"]

    df = DataFrame(rand(10, 4), :auto)
    select!(df, :x1, :x1 => :x2)
    @test df2.x1 !== df2.x2
    @test df2.x1 == df2.x2

    df = DataFrame(rand(10, 4), :auto)
    df2 = select(df, :, :x1 => :x3)
    @test df2 == DataFrame(collect(eachcol(df))[[1, 2, 1, 4]], :auto)
    @test df2.x1 !== df2.x3
    df2 = select(df, :, :x1 => :x3, copycols=false)
    @test df2 == DataFrame(collect(eachcol(df))[[1, 2, 1, 4]], :auto)
    @test df2.x1 == df2.x3
    @test df2.x1 !== df2.x3
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
    @test df.x1 == df2.x4
    @test df.x1 == df2.x4
    # a copy is made as we earlier used ":" selector, although later :x1 gets overwritten
    @test df.x1 !== df2.x4
    @test df2.r1 == df.x1 .^ 2
    @test df2.r1 == df2.r2
    @test df2.x1 == df.x1 + df.x2
    @test df2.x3 == df.x1 ./ df.x2

    x1, x2, x3, x4 = df.x1, df.x2, df.x3, df.x4
    select!(df, :x2, :, :x1 => ByRow(x -> x^2) => :r1, :x1 => (x -> x .^ 2) => :r2,
            [:x1, :x2] => (+) => :x1, 1:2 => ByRow(/) => :x3, :x1 => :x4)
    @test propertynames(df2) == [:x2, :x1, :x3, :x4, :r1, :r2]
    @test x2 === df.x2
    # a copy is made as we earlier used ":" selector, although later :x1 gets overwritten
    @test x1 !== df.x4
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
    for retval in [df2, (a=1, b=2), df2[1, :], ones(2, 2), Tables.Row(df2[1, :]), Tables.Row((a=1, b=2))]
        @test_throws ArgumentError select(df, :x => x -> retval)
        @test_throws ArgumentError select(df, :x => x -> retval, copycols=false)
        @test_throws ArgumentError select!(df, :x => x -> retval)

        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function=[retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function=[retval])

        if retval isa Union{NamedTuple, DataFrameRow}
            @test select(df, :x => ByRow(x -> retval) => AsTable) == DataFrame(; retval...)
        elseif retval isa Tables.AbstractRow
            @test select(df, :x => ByRow(x -> retval) => AsTable) ==
                  DataFrame([col => retval[col] for col in Tables.columnnames(retval)]...)
        elseif retval isa DataFrame
            @test_throws MethodError select(df, :x => ByRow(x -> retval) => AsTable)
        else # Matrix: wrong type of keys
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => AsTable)
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => [:a, :b, :c, :d])
        end
    end

    for retval in [(1, 2), ones(2, 2, 2)]
        @test select(df, :x => x -> retval) == DataFrame(x_function=[retval])
        @test select(df, :x => ByRow(x -> retval)) == DataFrame(x_function=[retval])
        if retval isa Tuple
            @test select(df, :x => ByRow(x -> retval) => AsTable) == DataFrame(x1=1, x2=2)
        else
            @test_throws ArgumentError select(df, :x => ByRow(x -> retval) => AsTable)
        end
        cdf = copy(df)
        select!(cdf, :x => x -> retval)
        @test cdf == DataFrame(x_function=[retval])
        cdf = copy(df)
        select!(cdf, :x => ByRow(x -> retval))
        @test cdf == DataFrame(x_function=[retval])
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
    @test select(df, [:x1], :x2 => :x1) == DataFrame(x1=df.x2)

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
    @test_throws ArgumentError select(sdf, identity, copycols=false)
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

        @test select(df3, [] => (() -> v) => :a, :x1 => x -> []) ≃
              DataFrame(a=Int[], x1_function=Any[])
        @test select(df3, :x1 => x -> [], [] => (() -> v) => :a) ≃
              DataFrame(x1_function=Any[], a=Int[])
        @test select(df3, [] => (() -> v) => :a, :x1) ≃
              DataFrame(a=Int[], x1=Char[])
        @test select(df3, :x1, [] => (() -> v) => :a) ≃
              DataFrame(x1=Char[], a=Int[])
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
    @test df2.b == df2.c == df.a
    @test df2.b !== df2.c
    @test df2.b !== df2.a
    @test df2.c !== df2.a
    @test df.a === df2.a
    a = df.a
    select!(df, :a, :a => :b, :a => identity => :c)
    @test df2.b == df2.c == df.a
    @test df2.b !== df2.c
    @test df2.b !== df2.a
    @test df2.c !== df2.a
    @test a === df2.a
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
          DataFrame(a_b_c_function=[[(a=1, b=4, c=7)],
                                    [(a=2, b=5, c=8)],
                                    [(a=3, b=6, c=9)]])
    @test transform(df, AsTable(:) => ByRow(x -> [x])) ==
          hcat(df, DataFrame(a_b_c_function=[[(a=1, b=4, c=7)],
                                             [(a=2, b=5, c=8)],
                                             [(a=3, b=6, c=9)]]))
    @test select(df, AsTable(:) => ByRow(identity)) ==
          DataFrame(a_b_c_identity=[(a=1, b=4, c=7), (a=2, b=5, c=8), (a=3, b=6, c=9)])
    @test select(df, AsTable(:) => ByRow(identity) => AsTable) == df
    @test select(df, AsTable(:) => ByRow(x -> df[1, :])) ==
          DataFrame(a_b_c_function=fill(df[1, :], 3))
    @test select(df, AsTable(:) => ByRow(x -> df[1, :]) => AsTable) ==
          DataFrame(a=[1, 1, 1], b=4, c=7)
    @test select(df, AsTable(:) => ByRow(x -> Tables.Row(df[1, :]))) ==
          DataFrame(a_b_c_function=fill(Tables.Row(df[1, :]), 3))
    @test select(df, AsTable(:) => ByRow(x -> Tables.Row(df[1, :])) => AsTable) ==
          DataFrame(a=[1, 1, 1], b=4, c=7)
    @test transform(df, AsTable(Not(:)) =>
          ByRow(identity)) == [df DataFrame(:identity => fill(NamedTuple(), nrow(df)))]

    @test select(df, AsTable(Not(:)) => Ref) == repeat(DataFrame(Ref=NamedTuple()), nrow(df))
    @test combine(df, AsTable(Not(:)) => Ref) == DataFrame(Ref=NamedTuple())
    @test transform(df, AsTable(Not(:)) => Ref) ==
          DataFrame(a=1:3, b=4:6, c=7:9, Ref=NamedTuple())

    df = DataFrame(x=[1, 2, missing], y=[1, missing, missing])
    @test transform(df, AsTable(:) .=>
                        ByRow.([sum∘skipmissing,
                                x -> count(!ismissing, x),
                                mean∘skipmissing]) .=>
                        [:sum, :n, :mean]) ≅
            [df DataFrame(sum=[2, 2, 0], n=[2, 1, 0], mean=[1, 2, NaN])]
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
    @test combine(df, (:) => cor) == DataFrame(x_y_cor=1.0)
    @test combine(df, :x => x -> Ref(1:3)) == DataFrame(x_function=[1:3])
    @test_throws ArgumentError combine(df, :x => x -> ones(1, 1))
    @test combine(df, :x => (x -> ones(1, 1)) => AsTable) == DataFrame(x1=1.0)

    df2 = combine(df, :x => identity)
    @test df2[:, 1] == df.x
    @test df2[:, 1] !== df.x

    @test combine(df, :x => sum, :y => collect ∘ extrema) ==
          DataFrame(x_sum=[6, 6], y_collect_extrema=[4, 6])
    @test combine(df, :y => collect ∘ extrema, :x => sum) ==
          DataFrame(y_collect_extrema=[4, 6], x_sum=[6, 6])
    @test combine(df, :x => sum, :y => x -> []) ==
          DataFrame(x_sum=[], y_function=[])
    @test combine(df, :y => x -> [], :x => sum) ==
          DataFrame(y_function=[], x_sum=[])

    dfv = view(df, [2, 1], [2, 1])

    @test combine(x -> Matrix(x), dfv) == rename(dfv, [:x1, :x2])

    @test_throws ArgumentError combine(dfv, AsTable(:) => identity)
    @test combine(dfv, AsTable(:) => identity => AsTable) == dfv
    @test combine(dfv, (:) => cor) == DataFrame(y_x_cor=1.0)

    df2 = combine(dfv, :x => identity)
    @test df2[:, 1] == dfv.x
    @test df2[:, 1] !== dfv.x

    @test combine(dfv, :x => sum, :y => collect ∘ extrema) ==
          DataFrame(x_sum=[3, 3], y_collect_extrema=[4, 5])
    @test combine(dfv, :y => collect ∘ extrema, :x => sum) ==
          DataFrame(y_collect_extrema=[4, 5], x_sum=[3, 3])
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
    @test df == DataFrame(x_first=fill(1, 3))


    # if we select! we do copycols=false, so we do not get aliases
    df = DataFrame(x=1:3, y=4:6)
    x = df.x
    select!(df, :x => (x->x), :x)
    @test x === df.x_function
    @test x == df.x
    @test x !== df.x

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
    @test_throws ArgumentError select!(dfv, 1)
    @test transform!(dfv, 1) == dfv
    @test df == DataFrame(x=1:3, y=4:6)
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
        for ret in (DataFrame(), NamedTuple(), zeros(0, 0), DataFrame(t=1)[1, 1:0], Tables.Row(DataFrame(t=1)[1, 1:0]))
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

        @test select(df, [] => ByRow(() -> (a=1, b="1")) => AsTable) ≃
              DataFrame(a=Int[], b=String[])
        @test select(df, [] => ByRow(() -> (a=1, b="1")) => [:p, :q]) ≃
              DataFrame(p=Int[], q=String[])

        # here this follows Tables.jl behavior
        @test select(df, [] => ByRow(() -> [1, "1"]) => AsTable) == DataFrame()
        @test_throws ArgumentError select(df, [] => ByRow(() -> [1, "1"]) => [:p, :q])
        @test select(df, [] => ByRow(() -> (1, "1")) => AsTable) ≃
              DataFrame(Column1=Int[], Column2=String[])
        @test select(df, [] => ByRow(() -> (1, "1")) => [:p, :q]) ≃
              DataFrame(p=Int[], q=String[])
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

        @test select(df, AsTable([]) => ByRow(x -> (a=1, b="1")) => AsTable) ≃
              DataFrame(a=Int[], b=String[])
        @test select(df, AsTable([]) => ByRow(x -> (a=1, b="1")) => [:p, :q]) ≃
              DataFrame(p=Int[], q=String[])

        # here this follows Tables.jl behavior
        @test select(df, [] => ByRow(() -> [1, "1"]) => AsTable) == DataFrame()
        @test_throws ArgumentError select(df, [] => ByRow(() -> [1, "1"]) => [:p, :q])
        @test select(df, [] => ByRow(() -> (1, "1")) => AsTable) ≃
              DataFrame(Column1=Int[], Column2=String[])
        @test select(df, [] => ByRow(() -> (1, "1")) => [:p, :q]) ≃
              DataFrame(p=Int[], q=String[])
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
    @test combine(df, :a => (x -> 1) => :x1, :a => (x -> []) => :x2) ≃
          DataFrame(x1=Int[], x2=[])
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

@testset "wide reductions" begin
    Random.seed!(1234)
    df = DataFrame(rand(Int64(1):Int64(10^6), 2, 20_000), :auto)
    df2 = Int32.(df)
    df.x100 = [1, 2]
    df.x1000 = Union{Int, Missing}[1, 2]
    df.x10000 = Union{Float64, Missing}[1, missing]

    @test @elapsed(select(df, All() => (+) => :res)) < 10.0
    @test @elapsed(select(df, Between(:x2,:x19999) => ByRow(+) => :res)) < 10.0
    @test @elapsed(select(df, AsTable(All()) => sum => :res)) < 10.0
    @test @elapsed(select(df, AsTable(Not(:x99)) => ByRow(sum) => :res)) < 10.0

    for sel in (All(), Between(:x2,:x19999), Not(:x99))
        @test select(df, sel => (+) => :res) ≅
              select(df, sel => ByRow(+) => :res) ≅
              select(df, AsTable(sel) => sum => :res) ≅
              select(df, AsTable(sel) => ByRow(sum) => :res) ≅
              DataFrame(res=sum(collect(eachcol(select(df, sel)))))
        @test select(df, AsTable(sel) => mean => :res) ≅
              select(df, AsTable(sel) => ByRow(mean) => :res) ≅
              DataFrame(res=mean(collect(eachcol(select(df, sel)))))
    end

    df2.x100 = Int32[1, 2]
    df2.x1000 = Union{Int32, Missing}[1, 2]
    df2.x10000 = Union{Float64, Missing}[1, missing]

    @test @elapsed(select(df2, All() => (+) => :res)) < 10.0
    @test @elapsed(select(df2, Between(:x2,:x19999) => ByRow(+) => :res)) < 10.0
    @test @elapsed(select(df2, AsTable(All()) => sum => :res)) < 10.0
    @test @elapsed(select(df2, AsTable(Not(:x99)) => ByRow(sum) => :res)) < 10.0

    for sel in (All(), Between(:x2,:x19999), Not(:x99))
        @test select(df2, sel => (+) => :res) ≅
              select(df2, sel => ByRow(+) => :res) ≅
              select(df2, AsTable(sel) => sum => :res) ≅
              DataFrame(res=sum(collect(eachcol(select(df2, sel)))))

        # Note that this is exact as opposed to other options for Int32 type
        @test select(df2, AsTable(sel) => ByRow(sum) => :res) ≅
              select(df, AsTable(sel) => ByRow(sum) => :res)

        @test select(df2, AsTable(sel) => mean => :res) ≅
              DataFrame(res=mean(collect(eachcol(select(df2, sel)))))

        # Note that this is exact as opposed to other options for
        # Int32 type on Julia 1.0
        @test select(df2, AsTable(sel) => ByRow(mean) => :res) ≅
              select(df, AsTable(sel) => ByRow(mean) => :res)
    end
end

@testset "vectors of pairs with non-specific type are accepted" begin
    df = DataFrame(x=[1,2,3])
    @test combine(df, [1 => length => :a, 1 => length => "b"]) == DataFrame(a=3, b=3)
    @test combine(df, [:x => length => :a, 1 => :b]) == DataFrame(a=3, b=1:3)
    gdf = groupby(df, :x)
    @test combine(gdf, [1 => length => :a, 1 => length => "b"]) == DataFrame(x=1:3, a=1, b=1)
    @test combine(gdf, [:x => length => :a, 1 => :b]) == DataFrame(x=1:3, a=1, b=1:3)
    sdf = view(df, :, :)
    @test select(sdf, [1 => length => :a, 1 => length => "b"]) == DataFrame(a=[3, 3, 3], b=[3, 3, 3])
    @test select(sdf, [:x => length => :a, 1 => :b]) == DataFrame(a=3, b=1:3)
    @test_throws ArgumentError select(sdf, [1 => length => :a, 1 => length => "b"], copycols=false)
    @test_throws ArgumentError select(sdf, [:x => length => :a, 1 => :b], copycols=false)
end

@testset "fast reductions positional: cols => ..." begin
    Random.seed!(1234)
    m = rand(10, 10000)
    df = DataFrame(m, :auto)
    @test combine(df, All() => (+) => :sum).sum ≃
          combine(df, All() => ByRow(+) => :sum).sum ≃
          reduce(+, collect(eachcol(df)))
    @test combine(df, All() => ByRow(min) => :min).min ≃ minimum.(eachrow(m))
    @test combine(df, All() => ByRow(max) => :max).max ≃ maximum.(eachrow(m))

    df = DataFrame(ones(UInt8, 10, 256), :auto)
    @test combine(df, All() => (+) => :sum).sum ≃
          combine(df, All() => ByRow(+) => :sum).sum ≃
          zeros(UInt8, 10)

    m = rand([big(1),big(2)], 10, 10000)
    df = DataFrame(m, :auto)
    df.x1000 = fill(1.5, 10)
    @test combine(df, All() => (+) => :sum).sum ≃
          combine(df, All() => ByRow(+) => :sum).sum ≃
          reduce(+, collect(eachcol(df)))
    @test combine(df, All() => ByRow(min) => :min).min == minimum.(eachrow(m))
    @test combine(df, All() => ByRow(max) => :max).max == maximum.(eachrow(m))
    @test combine(df, All() => (+) => :sum).sum isa Vector{BigFloat}
    @test combine(df, All() => ByRow(+) => :sum).sum isa Vector{BigFloat}
    @test combine(df, All() => ByRow(min) => :min).min isa Vector{BigFloat}
    @test combine(df, All() => ByRow(max) => :max).max isa Vector{BigFloat}

    df.x2000 = fill('a', 10)
    @test_throws MethodError combine(df, All() => (+) => :sum)
    @test_throws MethodError combine(df, All() => ByRow(+) => :sum)
    @test_throws MethodError combine(df, All() => ByRow(min) => :min)
    @test_throws MethodError combine(df, All() => ByRow(max) => :max)

    m = rand([1, missing], 10, 10000)
    df = DataFrame(m, :auto)
    @test combine(df, All() => (+) => :sum).sum ≃ fill(missing, 10)
    @test combine(df, All() => ByRow(+) => :sum).sum ≃ fill(missing, 10)
    @test combine(df, All() => ByRow(min) => :min).min ≃ missings(Int, 10)
    @test combine(df, All() => ByRow(max) => :max).max ≃ missings(Int, 10)
end

@testset "fast reductions: AsTable(cols)=>sum, mean, minimum, maximum variants" begin
    Random.seed!(1234)
    m = rand(10, 10000)
    df = DataFrame(m, :auto)

    # note that the sums below are not the same due to how Julia Base works
    @test combine(df, AsTable(:) => sum => :sum).sum ≃
          sum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃
          combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum ≃
          sum.(eachrow(df))

    @test combine(df, AsTable(:) => mean => :mean).mean ≃
          mean(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃
          combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean ≃
          mean.(eachrow(df))

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃
          minimum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃
          combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum ≃
          minimum.(eachrow(df))

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃
          maximum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃
          combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum ≃
          maximum.(eachrow(df))

    m = fill(UInt8(1), 10, 10000)
    df = DataFrame(m, :auto)

    # note that the sums below are not the same due to how Julia Base works
    @test combine(df, AsTable(:) => sum => :sum).sum ≃ fill(0x10, 10)
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃
          combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum ≃
          fill(UInt(10000), 10)

    @test combine(df, AsTable(:) => mean => :mean).mean ≃ fill(1.0, 10)

    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃
          combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean ≃
          fill(1.0, 10)

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃
          combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃
          combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum ≃
          fill(0x1, 10)

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃
          combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃
          combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum ≃
          fill(0x1, 10)

    m = rand([1, missing], 10, 10000)
    df = DataFrame(m, :auto)
    @test combine(df, AsTable(:) => sum => :sum).sum ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum ≃ count.(!ismissing, eachrow(m))

    @test combine(df, AsTable(:) => mean => :mean).mean ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃ missings(Float64, 10)
    @test combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean ≃ fill(1.0, 10)

    m = rand([1, 2, missing], 10, 10000)
    df = DataFrame(m, :auto)

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃
        minimum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum ≃ fill(1, 10)

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃
        maximum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum ≃ fill(2, 10)

    m = fill(missing, 10, 100)
    df = DataFrame(m, :auto)
    @test combine(df, AsTable(:) => sum => :sum).sum ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃ fill(missing, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum
    @test combine(df, AsTable(:) => mean => :mean).mean ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃ fill(missing, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃ fill(missing, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃ fill(missing, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum

    m = missings(Int, 10, 10000)
    df = DataFrame(m, :auto)
    @test combine(df, AsTable(:) => sum => :sum).sum ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum ≃ fill(0, 10)

    @test combine(df, AsTable(:) => mean => :mean).mean ≃ fill(missing, 10)
    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃ missings(Float64, 10)
    @test combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean ≃ fill(NaN, 10)

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃ missings(Int, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃ missings(Int, 10)
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃ missings(Int, 10)
    @test_throws ArgumentError combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum

    m = rand([big(1),big(2)], 10, 100)
    df = DataFrame(m, :auto)
    df.x10 = fill(1.5, 10)
    @test combine(df, AsTable(:) => sum => :sum).sum ≃
          combine(df, AsTable(:) => ByRow(sum) => :sum).sum ≃
          combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum ≃
          sum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => sum => :sum).sum isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(sum) => :sum).sum isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum).sum isa Vector{BigFloat}

    @test combine(df, AsTable(:) => mean => :mean).mean ≃
          combine(df, AsTable(:) => ByRow(mean) => :mean).mean ≃
          combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean ≃
          mean(collect(eachcol(df)))
    @test combine(df, AsTable(:) => mean => :mean).mean isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(mean) => :mean).mean isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean).mean isa Vector{BigFloat}

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃
          minimum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum ≃
          combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum ≃
          fill(big(1.0), 10)
    @test combine(df, AsTable(:) => minimum => :minimum).minimum isa Vector{BigInt}
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum).minimum isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum).minimum isa Vector{BigFloat}

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃
          maximum(collect(eachcol(df)))
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum ≃
          combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum ≃
          fill(big(2.0), 10)
    @test combine(df, AsTable(:) => maximum => :maximum).maximum isa Vector{BigInt}
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum).maximum isa Vector{BigFloat}
    @test combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum).maximum isa Vector{BigFloat}

    df.x20 = fill('a', 10)
    @test_throws MethodError combine(df, AsTable(:) => sum => :sum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(sum) => :sum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum)

    @test_throws MethodError combine(df, AsTable(:) => mean => :mean)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(mean) => :mean)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean)

    @test_throws MethodError combine(df, AsTable(:) => minimum => :minimum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(minimum) => :minimum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum)

    @test_throws MethodError combine(df, AsTable(:) => minimum => :minimum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(minimum) => :minimum)
    @test_throws MethodError combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum)

    m = rand(Any[1], 10, 100)
    df = DataFrame(m, :auto)
    @test combine(df, AsTable(:) => sum => :sum) ≃
          combine(df, AsTable(:) => ByRow(sum) => :sum) ≃
          combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :sum) ≃
          DataFrame(sum=fill(100, 10))

    @test combine(df, AsTable(:) => mean => :mean) ≃
          combine(df, AsTable(:) => ByRow(mean) => :mean) ≃
          combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :mean) ≃
          DataFrame(mean=fill(1.0, 10))

    m = rand(Any[1, 2], 10, 100)
    df = DataFrame(m, :auto)

    @test combine(df, AsTable(:) => minimum => :minimum).minimum ≃
          minimum(eachcol(df))
    @test combine(df, AsTable(:) => ByRow(minimum) => :minimum) ≃
          combine(df, AsTable(:) => ByRow(minimum∘skipmissing) => :minimum) ≃
          DataFrame(minimum=Any[1 for i in 1:10])

    @test combine(df, AsTable(:) => maximum => :maximum).maximum ≃
          maximum(eachcol(df))
    @test combine(df, AsTable(:) => ByRow(maximum) => :maximum) ≃
          combine(df, AsTable(:) => ByRow(maximum∘skipmissing) => :maximum) ≃
          DataFrame(maximum=Any[2 for i in 1:10])
end

@testset "fast reductions: AsTable(cols)=>length variants" begin
    Random.seed!(1234)
    m = rand([1, missing], 10, 10000)
    df = DataFrame(m, :auto)
    @test combine(df, AsTable(:) => length => :len) ≃ DataFrame(len=10000)
    @test combine(df, AsTable(:) => ByRow(length) => :len) ≃
          DataFrame(len=fill(10000, 10))
    @test combine(df, AsTable(:) => ByRow(length∘skipmissing) => :len) ≃
          DataFrame(len=count.(!ismissing, eachrow(m)))
    @test select(df, AsTable(:) => length => :len) ≃
          select(df, AsTable(:) => ByRow(length) => :len) ≃
          DataFrame(len=fill(10000, 10))
    @test select(df, AsTable(:) => ByRow(length∘skipmissing) => :len) ≃
          DataFrame(len=count.(!ismissing, eachrow(m)))
end

@testset "pathological cases that get custom eltype promotion" begin
    # in this tests we make sure that result columns with concrete eltype are created
    df = DataFrame(a=Any[1, 1.0], b=Any[1, 1.0])
    @test combine(df, AsTable(:) => ByRow(sum) => :res).res ≃  [2.0, 2.0]
    @test combine(df, AsTable(:) => ByRow(sum∘skipmissing) => :res).res ≃ [2.0, 2.0]
    df = DataFrame(a=Any[big(1), 1//1], b=Any[big(1), 1//1])
    @test combine(df, AsTable(:) => ByRow(mean) => :res).res ≃ BigFloat[1, 1]
    @test combine(df, AsTable(:) => ByRow(mean∘skipmissing) => :res).res ≃ BigFloat[1, 1]
end

@testset "tests of reductions over empty selections" begin
    df = DataFrame(zeros(3, 5), :auto)
    for fun in (+, min, max, ByRow(+), ByRow(min), ByRow(max))
        @test_throws MethodError combine(df, Cols() => fun)
        @test_throws MethodError combine(df, AsTable(Cols()) => fun)
    end
    for fun in (sum, ByRow(sum), ByRow(sum∘skipmissing),
                mean, ByRow(mean), ByRow(mean∘skipmissing),
                minimum, ByRow(minimum), ByRow(minimum∘skipmissing),
                maximum, ByRow(maximum), ByRow(maximum∘skipmissing))
        @test_throws MethodError combine(df, Cols() => fun)
        # need union as the error type is not stable across Julia versions
        @test_throws Union{MethodError,ArgumentError} combine(df, AsTable(Cols()) => fun)
    end
    @test_throws MethodError combine(df, Cols() => length)
    @test_throws MethodError combine(df, Cols() => ByRow(length))
    @test_throws MethodError combine(df, Cols() => ByRow(length∘skipmissing))
    @test combine(df, AsTable(Cols()) => length) == DataFrame(length=0)
    @test combine(df, AsTable(Cols()) => ByRow(length)) ==
          DataFrame(length=[0, 0, 0])
    @test combine(df, AsTable(Cols()) => ByRow(length∘skipmissing)) ==
          DataFrame(length_skipmissing=[0, 0, 0])
end

@testset "test AsTable(...) => fun∘collect and ByRow(fun∘collect)" begin
    Random.seed!(1234)
    df1 = DataFrame(rand(10, 100), :auto)
    df1.id = repeat(1:2, 5)
    df2 = copy(df1)
    df2.x50 = 1:10
    df3 = copy(df1)
    df3.x60 = [1.0:9.0; missing]
    df4 = copy(df3)
    df4.x50 = 1:10

    for df in (df1, df2, df3, df4)
        dfv = view(df, 2:10, 2:101)
        for x in (df, dfv), fun in (sum, prod, first, x -> first(x) - sum(x))
            @test combine(x, AsTable(r"x") => ByRow(fun∘collect) => :res) ≃
                  combine(x, AsTable(r"x") => ByRow(x -> (fun∘collect)(x)) => :res)
            @test combine(x, AsTable(r"x") => ByRow(fun∘skipmissing∘collect) => :res) ≃
                  combine(x, AsTable(r"x") => ByRow(x -> (fun∘skipmissing∘collect)(x)) => :res)
        end
    end

    for df in (df1, df2, df3, df4)
        dfv = view(df, 2:10, 2:101)
        for x in (df, dfv), fun in (mean, std, var)
            @test combine(x, AsTable(r"x") => ByRow(fun∘skipmissing∘collect) => :res) ≃
                  combine(x, AsTable(r"x") => ByRow(x -> (fun∘skipmissing∘collect)(x)) => :res)
        end
    end

    for df in (df1, df2, df3, df4)
        dfv = view(df, 2:10, 2:101)
        for x in (df, dfv), fun in (std, var, median)
            if df === df2 || df === df4
                # mixing Int and Float64 invokes a different implementation of these functions
                @test combine(x, AsTable(r"x") => ByRow(fun∘skipmissing) => :res) ≈
                    combine(x, AsTable(r"x") => ByRow(x -> (fun∘skipmissing)(x)) => :res)
            else
                @test combine(x, AsTable(r"x") => ByRow(fun∘skipmissing) => :res) ≃
                    combine(x, AsTable(r"x") => ByRow(x -> (fun∘skipmissing)(x)) => :res)
            end
        end
    end

    df3 .= coalesce.(df3, 0.0)
    df4 .= coalesce.(df4, 0.0)

    for df in (df1, df2, df3, df4)
        dfv = view(df, 2:10, 2:101)
        for x in (df, dfv), fun in (mean, std, var)
            if df === df2 || df === df4
                # mixing Int and Float64 invokes a different implementation of these functions
                @test combine(x, AsTable(r"x") => ByRow(fun∘collect) => :res) ≈
                    combine(x, AsTable(r"x") => ByRow(x -> (fun∘collect)(x)) => :res)
            else
                @test combine(x, AsTable(r"x") => ByRow(fun∘collect) => :res) ≃
                    combine(x, AsTable(r"x") => ByRow(x -> (fun∘collect)(x)) => :res)
            end
        end
    end

    for df in (df1, df2, df3, df4)
        dfv = view(df, 2:10, 2:101)
        for x in (df, dfv), fun in (std, var, median)
            if fun !== median
                # mixing Int and Float64 invokes a different implementation of these functions
                @test combine(x, AsTable(r"x") => ByRow(fun) => :res) ≈
                    combine(x, AsTable(r"x") => ByRow(x -> fun(x)) => :res)
            else
                @test combine(x, AsTable(r"x") => ByRow(fun) => :res) ≃
                    combine(x, AsTable(r"x") => ByRow(x -> fun(x)) => :res)
            end
        end
    end

    # make sure we are not mutating not reusing worker vector if we are not in reduction mode
    push0!(x) = push!(x, 0.0)
    for df in (DataFrame(rand(10, 1000), :auto), DataFrame(rand(1000, 100), :auto))
        @test combine(df, AsTable(All()) => ByRow(pop!∘collect) => :res) ≃
            combine(df, AsTable(All()) => ByRow(x -> x |> collect |> pop!) => :res)
        @test combine(df, AsTable(All()) => ByRow(push0!∘collect) => :res) ≃
            combine(df, AsTable(All()) => ByRow(x -> x |> collect |> push0!) => :res)
        @test combine(df, AsTable(All()) => ByRow(identity∘collect) => :res) ≃
            combine(df, AsTable(All()) => ByRow(x -> x |> collect |> identity) => :res)
    end

    # test of fast reductions
    for df in (DataFrame(rand(5, 10), :auto),
               DataFrame(rand([1:10; missing], 5, 10), :auto)),
        fun in (sum, sum∘skipmissing, length,
                mean, mean∘skipmissing, var, var∘skipmissing,
                std, std∘skipmissing, median, median∘skipmissing,
                minimum, minimum∘skipmissing, maximum, maximum∘skipmissing,
                prod, prod∘skipmissing, first, first∘skipmissing, last)
        @test combine(df, AsTable(All()) => ByRow(fun∘collect) => :res) ≃
              combine(df, AsTable(All()) => ByRow(x -> x |> collect |> fun) => :res)
    end

    # test promotion for two or more distinct types of columns
    df = DataFrame(x=[true, false], y=1:2, z=1.0:2.0)
    df2 = DataFrame(rand(1:10, 10, 10_000), :auto)
    df2.y = 1.0:10.0
    df2.z = repeat([true, false], 5)
    df3 = DataFrame(rand(1:10, 10, 10_000), :auto)
    df3.y = 1.0:10.0
    fun(v::AbstractVector{<:Real}) = sum(v)
    for x in (df, df2, df3)
        @test combine(x, AsTable(All()) => ByRow(fun∘collect) => :res) ≃
            combine(x, AsTable(All()) => ByRow(sum) => :res)
        @test combine(df, AsTable(All()) => ByRow(eltype∘collect) => :res) ≃
              combine(df, AsTable(All()) => ByRow(x -> (eltype∘collect)(x)) => :res) ≃
              DataFrame(res=[Real, Real])
    end

    df4 = DataFrame(rand(1:10, 2, 1000), :auto)
    df4.y = fill(missing, 2)
    df5 = copy(df4)
    df5.z = fill(0x0, 2)

    @test combine(df4, AsTable(All()) => ByRow(eltype∘collect) => :res) ≃
            combine(df4, AsTable(All()) => ByRow(x -> (eltype∘collect)(x)) => :res) ≃
            DataFrame(res=[Union{Int, Missing}, Union{Int, Missing}])
    # T is Any under Julia 1.0, but Union{Missing, Integer} currently
    T = Base.promote_typejoin(Missing, Base.promote_typejoin(Int, UInt8))
    @test combine(df5, AsTable(All()) => ByRow(eltype∘collect) => :res) ≃
            combine(df5, AsTable(All()) => ByRow(x -> (eltype∘collect)(x)) => :res) ≃
            DataFrame(res=[T, T])

    df = DataFrame(x=1:2, y=PooledArray(1:2), z=view([1, 2], 1:2), copycols=false)
    df2 = DataFrame(rand(1:10, 10, 10_000), :auto)
    df2.y = PooledArray(1:10)
    df2.z = view([1:10;], 1:10)
    df3 = DataFrame(rand(1:10, 10, 10_000), :auto)
    df3.y = PooledArray(1:10)
    fun2(v::Vector{Int}) = sum(v)
    for x in (df, df2)
        @test combine(x, AsTable(All()) => ByRow(fun∘collect) => :res) ≃
            combine(x, AsTable(All()) => ByRow(sum) => :res)
        @test combine(df, AsTable(All()) => ByRow(eltype∘collect) => :res) ≃
              combine(df, AsTable(All()) => ByRow(x -> (eltype∘collect)(x)) => :res) ≃
              DataFrame(res=[Int, Int])
    end

    # test without ByRow
    df = DataFrame(rand(10, 1000), :auto)
    @test combine(df, AsTable(All()) => sum∘collect => :res) ≃
          combine(df, AsTable(All()) => sum => :res)
    @test combine(df, AsTable(All()) => first∘collect => :res) ≃
          combine(df, :x1 => :res)
    @test combine(df, AsTable(All()) => last∘collect => :res) ≃
          combine(df, :x1000 => :res)
end

@testset "function as target column names specifier" begin
    df_ref = DataFrame(x=[[1, 2], [3, 4]], id=1:2)
    for v in (df_ref, groupby(df_ref, :id))
        @test select(v, :id, :x => ByRow(first) => identity) == DataFrame(id=1:2, x=[1, 3])
        @test select(v, :id, "x" => ByRow(first) => identity) == DataFrame(id=1:2, x=[1, 3])
        @test select(v, :id, 1 => ByRow(first) => identity) == DataFrame(id=1:2, x=[1, 3])
        @test select(v, :id, 1 => ByRow(first) => uppercase) == DataFrame(id=1:2, X=[1, 3])
        @test select(v, :id, 1 => ByRow(first) => string) == DataFrame(id=1:2, x=[1, 3])
        @test select(v, :id, 1 => ByRow(first) => x -> Symbol(x)) == DataFrame(id=1:2, x=[1, 3])
        @test select(v, :id, 1 => identity => x -> ["p", "q"]) ==
              DataFrame(id=1:2, p=[1, 3], q=[2, 4])
        @test select(v, :id, 1 => identity => x -> [:p, :q]) ==
              DataFrame(id=1:2, p=[1, 3], q=[2, 4])
        @test_throws ArgumentError select(v, :id, 1 => identity => x -> [:p, "q"])
        @test_throws ArgumentError select(v, :id, 1 => identity => x -> AsTable)
        @test select(v, :id, AsTable(1) => first => string) ==
              DataFrame("id" => 1:2, "[\"x\"]" => [[1, 2], [3, 4]])
        @test select(v, :id, ["x", "x"] => ByRow((p,q) -> first(p)) => string) ==
            DataFrame("id" => 1:2, "[\"x\", \"x\"]" => [1, 3])
        @test select(v, :id, 1:2 => ((p, q) -> q) => x -> join(x, "_")) ==
              DataFrame(id=1:2, x_id=1:2)
        @test select(v, :id, AsTable(1:2) => last => x -> join(x, "_")) ==
              DataFrame(id=1:2, x_id=1:2)
        # we could make this work, but I skip it to keep the code simpler
        # The problem is that Symbol and String are not Function
        @test_throws ArgumentError select(v, :id, 1 => ByRow(first) => Symbol)
        @test_throws ArgumentError select(v, :id, 1 => ByRow(first) => String)
    end
end

@testset "broadcasting column selectors: All, Cols, Between, Not" begin
    df = DataFrame(reshape(1:200, 20, 10), :auto)
    insertcols!(df, 1, :id => repeat(["a", "b"], 10))
    dfv = @view df[1:end-1, 1:end-1]

    for sel in (All(), Cols(2:8), Between(:x3, :x5), Not(:x1),
                Cols(), Between(:x5, :x3), Not(:)),
                tab in (df, dfv),
                op in (select, select!, transform, transform!, combine)

        @test op(copy(tab), sel .=> first) == op(copy(tab), names(tab, sel) .=> first)
        @test op(copy(tab), sel .=> [first]) == op(copy(tab), names(tab, sel) .=> [first])
        @test op(copy(tab), sel .=> [first length]) ==
              op(copy(tab), names(tab, sel) .=> [first length])
        @test op(copy(tab), sel .=> sel) ==
              op(copy(tab), names(tab, sel) .=> sel) ==
              op(copy(tab), sel .=> names(tab, sel)) ==
              op(copy(tab), names(tab, sel) .=> names(tab, sel))
        @test op(copy(tab), sel .=> first .=> sel) ==
              op(copy(tab), names(tab, sel) .=> first .=> sel) ==
              op(copy(tab), sel .=> first .=> names(tab, sel)) ==
              op(copy(tab), names(tab, sel) .=> first .=> names(tab, sel))
        @test op(copy(tab), sel .=> [first] .=> sel) ==
              op(copy(tab), names(tab, sel) .=> [first] .=> sel) ==
              op(copy(tab), sel .=> [first] .=> names(tab, sel)) ==
              op(copy(tab), names(tab, sel) .=> [first] .=> names(tab, sel))

        @test op(groupby(copy(tab), :id), sel .=> first) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> first)
        @test op(groupby(copy(tab), :id), sel .=> [first length]) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> [first length])
        @test op(groupby(copy(tab), :id), sel .=> sel) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> sel) ==
              op(groupby(copy(tab), :id), sel .=> names(tab, sel)) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> names(tab, sel))
        @test op(groupby(copy(tab), :id), sel .=> first .=> sel) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> first .=> sel) ==
              op(groupby(copy(tab), :id), sel .=> first .=> names(tab, sel)) ==
              op(groupby(copy(tab), :id), names(tab, sel) .=> first .=> names(tab, sel))

        res = names(tab, sel) .=> sum
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> sum) == res

        res = names(tab, sel) .=> [sum]
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> [sum]) == res

        res = names(tab, sel) .=> [sum length]
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> [sum length]) ==
              res

        res = names(tab, sel) .=> names(tab, sel)
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> sel) ==
              DataFrames.broadcast_pair(tab, names(tab, sel) .=> sel) ==
              DataFrames.broadcast_pair(tab, sel .=> names(tab, sel)) ==
              res

        res = names(tab, sel) .=> sum .=> names(tab, sel)
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> sum .=> sel) ==
              DataFrames.broadcast_pair(tab, names(tab, sel) .=> sum .=> sel) ==
              DataFrames.broadcast_pair(tab, sel .=> sum .=> names(tab, sel)) ==
              res

        res = names(tab, sel) .=> [sum] .=> names(tab, sel)
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> [sum] .=> sel) ==
              DataFrames.broadcast_pair(tab, names(tab, sel) .=> [sum] .=> sel) ==
              DataFrames.broadcast_pair(tab, sel .=> [sum] .=> names(tab, sel)) ==
              res

        # this is an invalid transformation, but we can check if a correct result
        # is produced in the preprocessing step
        res = names(tab, sel) .=> [sum length] .=> names(tab, sel)
        res = isempty(res) ? [] : res
        @test DataFrames.broadcast_pair(tab, sel .=> [sum length] .=> sel) ==
              DataFrames.broadcast_pair(tab, names(tab, sel) .=> [sum length] .=> sel) ==
              DataFrames.broadcast_pair(tab, sel .=> [sum length] .=> names(tab, sel)) ==
              res
    end

    @test_throws DimensionMismatch DataFrames.broadcast_pair(df, Not(:x1) .=> Between(:x2, :x4))
    @test_throws DimensionMismatch DataFrames.broadcast_pair(df, Not(:x1) .=> sum .=> Between(:x2, :x4))
    @test_throws DimensionMismatch DataFrames.broadcast_pair(df, Not(:x1) .=> Between(:x2, :x1))
    @test_throws DimensionMismatch DataFrames.broadcast_pair(df, Not(:x1) .=> sum .=> Between(:x2, :x1))
    # this is allowed due to how broadcasting rules are defined
    @test combine(df, Between(:x2, :x2) .=> sum .=> Between(:x1, :x3)) ==
          DataFrame(x1=610, x2=610, x3=610)
    @test combine(df, Between(:x2, :x2) .=> Between(:x1, :x3)) ==
          DataFrame(x1=df.x2, x2=df.x2, x3=df.x2)
    @test_throws ArgumentError DataFrames.broadcast_pair(df,
        [Between(:x1, :x2) .=> sin Between(:x2, :x3) .=> sin])
    @test_throws ArgumentError DataFrames.broadcast_pair(df,
        [1 .=> Between(:x1, :x2) 1 .=> Between(:x2, :x3)])
    @test_throws ArgumentError DataFrames.broadcast_pair(df,
        [1 .=> sum .=> Between(:x1, :x2) 1 .=> sum .=> Between(:x2, :x3)])
    # this is a case that we cannot handle correctly, note that properly
    # this broadcasting operation should error
    @test DataFrames.broadcast_pair(df, Between(:x1, :x2) .=> []) == []
    @test_throws ArgumentError DataFrames.broadcast_pair(df, Between(:x1, :x2) .=> [sin, cos, sin])
    @test_throws ArgumentError DataFrames.broadcast_pair(df, Between(:x1, :x2) .=> [sin cos
                                                                                   sin cos
                                                                                   sin cos])
    @test_throws ArgumentError DataFrames.broadcast_pair(df, 1:3 .=> Between(:x1, :x2))
    @test_throws ArgumentError DataFrames.broadcast_pair(df, 1:3 .=> sum .=> Between(:x1, :x2))
end

@testset "correct handling of copying" begin
    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a => :b, :a => :c)
    @test df.b !== df.c
    @test df.b === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a, :a => :c)
    @test df.a !== df.c
    @test df.a === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a => :c, :a)
    @test df.a !== df.c
    @test df.c === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a => :c, :)
    @test df.a !== df.c
    @test df.c === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a .=> [:c], :)
    @test df.a !== df.c
    @test df.c === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a .=> [:c], :, :a => :b)
    @test df.a !== df.c
    @test df.b !== df.c
    @test df.a !== df.b
    @test df.c === x

    df = DataFrame(a=1:3)
    x = df.a
    select!(df, :a .=> [:x1, :x2, :x3])
    @test df.x1 !== df.x2
    @test df.x1 !== df.x3
    @test df.x2 !== df.x3
    @test df.x1 === x

    df = DataFrame(a=1:3)
    x = df.a
    transform!(df, :a .=> [:b])
    @test df.a !== df.b
    @test df.a === x

    df = DataFrame(a=1:3)
    x = df.a
    transform!(df, :a .=> [:x1, :x2, :x3])
    @test df.a !== df.x1
    @test df.a !== df.x2
    @test df.a !== df.x3
    @test df.x1 !== df.x2
    @test df.x1 !== df.x3
    @test df.x2 !== df.x3
    @test df.a === x

    for cc in (true, false)
        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a => :b, :a => :c, copycols=cc)
        @test df2.b !== df2.c
        @test (df2.b === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a, :a => :c, copycols=cc)
        @test df2.a !== df2.c
        @test (df2.a === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a => :c, :a, copycols=cc)
        @test df2.a !== df2.c
        @test (df2.c === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a => :c, :, copycols=cc)
        @test df2.a !== df2.c
        @test (df2.c === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a .=> [:c], :, copycols=cc)
        @test df2.a !== df2.c
        @test (df2.c === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a .=> [:c], :, :a => :b, copycols=cc)
        @test df2.a !== df2.c
        @test df2.b !== df2.c
        @test df2.a !== df2.b
        @test (df2.c === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = select(df, :a .=> [:x1, :x2, :x3], copycols=cc)
        @test df2.x1 !== df2.x2
        @test df2.x1 !== df2.x3
        @test df2.x2 !== df2.x3
        @test (df2.x1 === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = transform(df, :a .=> [:b], copycols=cc)
        @test df2.a !== df2.b
        @test (df2.a === x) ⊻ cc

        df = DataFrame(a=1:3)
        x = df.a
        df2 = transform(df, :a .=> [:x1, :x2, :x3], copycols=cc)
        @test df2.a !== df2.x1
        @test df2.a !== df2.x2
        @test df2.a !== df2.x3
        @test df2.x1 !== df2.x2
        @test df2.x1 !== df2.x3
        @test df2.x2 !== df2.x3
        @test (df2.a === x) ⊻ cc
    end

    for sel in (:a, [:a], r"a")
        df = DataFrame(a=1:3)
        x = df.a
        select!(df, sel => identity => :b, sel => identity => :c)
        @test df.b !== df.c
        @test df.b === x

        df = DataFrame(a=1:3)
        x = df.a
        select!(df, :a, sel => identity => :c)
        @test df.a !== df.c
        @test df.a === x

        df = DataFrame(a=1:3)
        x = df.a
        select!(df, sel => identity => :c, :a)
        @test df.a !== df.c
        @test df.c === x

        df = DataFrame(a=1:3)
        x = df.a
        select!(df, sel => identity => :c, :)
        @test df.a !== df.c
        @test df.c === x

        for cc in (true, false)
            df = DataFrame(a=1:3)
            x = df.a
            df2 = select(df, sel => identity => :b, sel => identity => :c, copycols=cc)
            @test df2.b !== df2.c
            @test (df2.b === x) ⊻ cc

            df = DataFrame(a=1:3)
            x = df.a
            df2 = select(df, :a, sel => identity => :c, copycols=cc)
            @test df2.a !== df2.c
            @test (df2.a === x) ⊻ cc

            df = DataFrame(a=1:3)
            x = df.a
            df2 = select(df, sel => identity => :c, :a, copycols=cc)
            @test df2.a !== df2.c
            @test (df2.c === x) ⊻ cc

            df = DataFrame(a=1:3)
            x = df.a
            df2 = select(df, sel => identity => :c, :, copycols=cc)
            @test df2.a !== df2.c
            @test (df2.c === x) ⊻ cc
        end

        df = DataFrame(a=1:3)
        df2 = select(df, sel => (x -> (b=x, c=x)) => AsTable, copycols=false)
        @test df.a === df2.b
        @test df.a !== df2.c
        df = DataFrame(a=1:3)
        df2 = select(df, sel => (x -> DataFrame(b=x, c=x, copycols=false)) => AsTable, copycols=false)
        @test df.a === df2.b
        @test df.a !== df2.c
        df = DataFrame(a=1:3)
        df2 = transform(df, sel => (x -> (b=x, c=x)) => AsTable, copycols=false)
        @test df.a === df2.a
        @test df.a !== df2.b
        @test df.a !== df2.c
        @test df2.b !== df2.c
        df = DataFrame(a=1:3)
        df2 = transform(df, sel => (x -> DataFrame(b=x, c=x, copycols=false)) => AsTable, copycols=false)
        @test df.a === df2.a
        @test df.a !== df2.b
        @test df.a !== df2.c
        @test df2.b !== df2.c

        df = DataFrame(a=1:3)
        a = df.a
        select!(df, sel => (x -> (b=x, c=x)) => AsTable)
        @test a === df.b
        @test a !== df.c
        df = DataFrame(a=1:3)
        a = df.a
        select!(df, sel => (x -> DataFrame(b=x, c=x, copycols=false)) => AsTable)
        @test a === df.b
        @test a !== df.c
        df = DataFrame(a=1:3)
        a = df.a
        transform!(df, sel => (x -> (b=x, c=x)) => AsTable)
        @test a === df.a
        @test a !== df.b
        @test a !== df.c
        @test df.b !== df.c
        df = DataFrame(a=1:3)
        a = df.a
        transform!(df, sel => (x -> DataFrame(b=x, c=x, copycols=false)) => AsTable)
        @test a === df.a
        @test a !== df.b
        @test a !== df.c
        @test df.b !== df.c
    end

    df = DataFrame(a=1:3)
    df2 = select(df, :a, [:a, :a] => ((x, y) -> x) => :b, copycols=false)
    @test df.a === df2.a
    @test df.a == df2.b
    @test df.a !== df2.b
    @test df2.a !== df2.b
    df2 = transform(df, [:a, :a] => ((x, y) -> x) => :b, copycols=false)
    @test df.a === df2.a
    @test df.a == df2.b
    @test df.a !== df2.b
    @test df2.a !== df2.b

    df = DataFrame(a=1:3)
    df2 = select(df, :a, [:a, :a] => ((x, y) -> (b=x, c=y)) => AsTable, copycols=false)
    @test df.a === df2.a
    @test df.a == df2.b
    @test df.a !== df2.b
    @test df2.a !== df2.b
    @test df.a == df2.c
    @test df.a !== df2.c
    @test df2.a !== df2.c
    df2 = transform(df, [:a, :a] => ((x, y) -> (b=x, c=y)) => AsTable, copycols=false)
    @test df.a === df2.a
    @test df.a == df2.b
    @test df.a !== df2.b
    @test df2.a !== df2.b
    @test df.a == df2.c
    @test df.a !== df2.c
    @test df2.a !== df2.c
    df2 = transform(df, [:a, :a] => ((x, y) -> (b=x, c=y)) => AsTable, :a => :d, copycols=false)
    @test df.a === df2.a
    @test df.a == df2.b
    @test df.a !== df2.b
    @test df2.a !== df2.b
    @test df.a == df2.c
    @test df.a !== df2.c
    @test df2.a !== df2.c
    @test df.a == df2.d
    @test df.a !== df2.d
    @test df2.a !== df2.d

    df = DataFrame(a=1:3, b=11:13, c=21:23)
    df2 = select(df, [:a, :b, :c] => ((x, y, z) -> (c=x, d=y)) => AsTable, :, :c => :e, copycols=false)
    @test df2.c === df.a
    @test df2.d === df.b
    @test df2.a !== df.a
    @test df2.b !== df.b
    @test df2.a !== df2.c
    @test df2.b !== df2.d
    @test df2.a == df2.c
    @test df2.b == df2.d
    @test df2.e === df.c

    # multialias detection
    df = DataFrame(a=1:3)
    df.b = df.a
    df2 = select(df, [:a, :b] => ((x, y) -> x) => :c, :a, :b, copycols=false)
    @test df2.c === df.a === df.b
    @test df2.a == df2.b == df.a
    @test df2.a !== df.a
    @test df2.b !== df.b
    @test df2.a !== df2.c
    @test df2.b !== df2.c

    df = DataFrame(a=1:3)
    df2 = transform(df, :a => lag)
    # note that here a copy of `lag(df.a)` was performed as the result of this
    # transformation is a view of `df.a` and `copycols=true` so we make sure
    # that `df2.a_lag` column does not share data with `df.a` column.
    @test df2.a_lag isa Vector
    df2 = transform(df, :a => lag, copycols=false)
    @test df2.a_lag isa ShiftedVector
    transform!(df, :a => lag)
    @test df.a_lag isa ShiftedVector

    df = DataFrame(x=1:3)
    x = df.x
    select!(df, :x => copy => :y, :x => :z)
    @test df.y == df.z == x
    @test df.y !== x
    @test df.z === x
end

struct Identity
end

function (::Identity)(x)
    return x
end

@testset "correct handling of functors" begin
    i = Identity()
    df = DataFrame(x=1:3)
    @test_throws ArgumentError combine(df, :x => i) # i is not Callable
    @test combine(df, :x => identity∘i) == DataFrame(x_identity_function=1:3)
end

@testset "map on sparse array" begin
    Random.seed!(1234)
    df = DataFrame(x=spzeros(10))
    df2 = select(df, :x => ByRow(i -> rand()) => :y)
    @test allunique(df2.y)
end

@testset "improved error message when numbers requested and returned columns does not match" begin
    df = DataFrame(id=1:2, a=[(a=1,b=2),(a=3,b=4)])
    gdf = groupby(df, :id)
    @test_throws ArgumentError select(df, :a => [:x])
    @test_throws ArgumentError select(gdf, :a => [:x])
    @test select(df, :id, :a => [:x, :y]) == DataFrame(id=1:2, x=[1, 3], y=[2, 4])
    @test select(gdf, :a => [:x, :y]) == DataFrame(id=1:2, x=[1, 3], y=[2, 4])
    @test_throws ArgumentError select(df, :a => [:x, :y, :z])
    @test_throws ArgumentError select(gdf, :a => [:x, :y, :z])
end

@testset "handling of operation specification in select!/transform!" begin
    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    select!(df, :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df == DataFrame(b='a':'d', d=["p", "q", "r", "s"], e=[2, 4, 6, 8])

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    transform!(df, :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df == DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"],
                          d=["p", "q", "r", "s"], e=[2, 4, 6, 8])
    @test df.c !== df.d

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    select!(groupby(df, :c), :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df == DataFrame(c=["p", "q", "r", "s"], b='a':'d',
                          d=["p", "q", "r", "s"], e=[2, 4, 6, 8])
    @test df.c !== df.d

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    transform!(groupby(df, :c), :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df == DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"],
                          d=["p", "q", "r", "s"], e=[2, 4, 6, 8])
    @test df.c !== df.d

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    sdf = @view df[[2, 4], :]
    # note that in this test a column from parent of sdf is dropped
    select!(sdf, :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df ≅ DataFrame(b='a':'d', d=[missing, "q", missing, "s"],
                         e=[missing, 4, missing, 8])

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    sdf = @view df[[2, 4], :]
    transform!(sdf, :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df ≅ DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"],
                         d=[missing, "q", missing, "s"],
                         e=[missing, 4, missing, 8])

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    sdf = @view df[[2, 4], :]
    # note that in this test a column from parent of sdf is dropped
    select!(groupby(sdf, :c), :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df ≅ DataFrame(c=["p", "q", "r", "s"], b='a':'d',
                         d=[missing, "q", missing, "s"],
                         e=[missing, 4, missing, 8])

    df = DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"])
    sdf = @view df[[2, 4], :]
    transform!(groupby(sdf, :c), :b, :c => :d, :a => (x -> 2 * x) => :e)
    @test df ≅ DataFrame(a=1:4, b='a':'d', c=["p", "q", "r", "s"],
                         d=[missing, "q", missing, "s"],
                         e=[missing, 4, missing, 8])
end

@testset "selection on a view without copying" begin
    df = DataFrame(a=1:2)
    for dfv in (view(df, :, :), view(df, 1:2, 1:1))
        @test_throws ArgumentError select(dfv, x -> true, copycols=false)
        @test_throws ArgumentError select(dfv, :a => identity, copycols=false)
    end
end

@testset "corner cases of fast aggregations" begin
    df = DataFrame(id=[1, 1, 2],
                   a=[1, 2, 4], a2=Any[1, 2, 4],
                   b=Any[2, 3, 3],
                   c1=missing, c2=missing,
                   d1=missings(Int, 3), d2=missings(Int, 3))
    gdf = groupby(df, [])

    for table in (df, gdf)
        for mode in [true, false]
            p, q = mode ? (:a, :b) : (:b, :a)
            @test combine(table, AsTable([p, q]) => sum => :x) ==
                  DataFrame(x=3:2:7)
            @test combine(table, AsTable([p, q]) => ByRow(sum) => :x) ==
                  DataFrame(x=3:2:7)
            @test combine(table, AsTable([p, q]) => ByRow(sum∘skipmissing) => :x) ==
                  DataFrame(x=3:2:7)
            @test combine(table, AsTable([p, q]) => length => :x) ==
                  DataFrame(x=2)
            @test combine(table, AsTable([p, q]) => ByRow(length) => :x) ==
                  DataFrame(x=[2, 2, 2])
            if table isa DataFrame
                @test combine(table, AsTable([p, q]) => ByRow(length∘skipmissing) => :x) ==
                      DataFrame(x=[2, 2, 2])
            end
            @test combine(table, AsTable([p, q]) => mean => :x) ==
                  DataFrame(x=[1.5, 2.5, 3.5])
            @test combine(table, AsTable([p, q]) => ByRow(mean) => :x) ==
                  DataFrame(x=[1.5, 2.5, 3.5])
            @test combine(table, AsTable([p, q]) => ByRow(mean∘skipmissing) => :x) ==
                  DataFrame(x=[1.5, 2.5, 3.5])
            @test combine(table, AsTable([p, q]) => ByRow(var) => :x) ==
                  DataFrame(x=[0.5, 0.5, 0.5])
            @test combine(table, AsTable([p, q]) => ByRow(var∘skipmissing) => :x) ==
                  DataFrame(x=[0.5, 0.5, 0.5])
            @test combine(table, AsTable([p, q]) => ByRow(std) => :x) ≈
                  DataFrame(x=.√[0.5, 0.5, 0.5])
            @test combine(table, AsTable([p, q]) => ByRow(std∘skipmissing) => :x) ≈
                  DataFrame(x=.√[0.5, 0.5, 0.5])
            @test combine(table, AsTable([p, q]) => ByRow(median) => :x) ==
                  DataFrame(x=[1.5, 2.5, 3.5])
            @test combine(table, AsTable([p, q]) => ByRow(median∘skipmissing) => :x) ==
                  DataFrame(x=[1.5, 2.5, 3.5])
            @test combine(table, AsTable([p, q]) => minimum => :x) ==
                  DataFrame(x=[1, 2, 4])
            @test combine(table, AsTable([p, q]) => ByRow(minimum) => :x) ==
                  DataFrame(x=[1, 2, 3])
            @test combine(table, AsTable([p, q]) => ByRow(minimum∘skipmissing) => :x) ==
                  DataFrame(x=[1, 2, 3])
            @test combine(table, AsTable([p, q]) => maximum => :x) ==
                  DataFrame(x=[2, 3, 3])
            @test combine(table, AsTable([p, q]) => ByRow(maximum) => :x) ==
                  DataFrame(x=[2, 3, 4])
            @test combine(table, AsTable([p, q]) => ByRow(maximum∘skipmissing) => :x) ==
                  DataFrame(x=[2, 3, 4])
            @test combine(table, AsTable([p, q]) => ByRow(extrema) => :x) ==
                  DataFrame(x=[(1, 2), (2, 3), (3, 4)])
            @test combine(table, AsTable([p, q]) => ByRow(extrema∘skipmissing) => :x) ==
                  DataFrame(x=[(1, 2), (2, 3), (3, 4)])
        end

        for p in [:a, :a2], q in [:c1, :d1], mode in [true, false]
            if mode
                p, q = q, p
            end
            @test combine(table, AsTable([p, q]) => sum => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(sum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(sum∘skipmissing) => :x) ≅
                  DataFrame(x=[1, 2, 4])
            @test combine(table, AsTable([p, q]) => length => :x) ==
                  DataFrame(x=2)
            @test combine(table, AsTable([p, q]) => ByRow(length) => :x) ==
                  DataFrame(x=[2, 2, 2])
            if table isa DataFrame
                @test combine(table, AsTable([p, q]) => ByRow(length∘skipmissing) => :x) ==
                      DataFrame(x=[1, 1, 1])
            end
            @test combine(table, AsTable([p, q]) => mean => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(mean) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(mean∘skipmissing) => :x) ≅
                  DataFrame(x=[1, 2, 4])
            if table isa DataFrame
                @test combine(table, AsTable([p, q]) => ByRow(var) => :x) ≅
                      DataFrame(x=[missing, missing, missing])
                @test combine(table, AsTable([p, q]) => ByRow(var∘skipmissing) => :x) ≅
                      DataFrame(x=[NaN, NaN, NaN])
                @test combine(table, AsTable([p, q]) => ByRow(std) => :x) ≅
                      DataFrame(x=[missing, missing, missing])
                @test combine(table, AsTable([p, q]) => ByRow(std∘skipmissing) => :x) ≅
                      DataFrame(x=[NaN, NaN, NaN])
            end
            @test combine(table, AsTable([p, q]) => ByRow(median) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(median∘skipmissing) => :x) ==
                  DataFrame(x=[1, 2, 4])
            # a bit surprising how non-broadcasted and broadcasted minimum works
            @test combine(table, AsTable([p, q]) => minimum => :x) ==
                  DataFrame(x=[1, 2, 4])
            @test combine(table, AsTable([p, q]) => ByRow(minimum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(minimum∘skipmissing) => :x) ==
                  DataFrame(x=[1, 2, 4])
            @test combine(table, AsTable([p, q]) => maximum => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(maximum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(maximum∘skipmissing) => :x) ==
                  DataFrame(x=[1, 2, 4])
            @test combine(table, AsTable([p, q]) => ByRow(extrema) => :x) ≅
                  DataFrame(x=[(missing, missing), (missing, missing), (missing, missing)])
            @test combine(table, AsTable([p, q]) => ByRow(extrema∘skipmissing) => :x) ==
                  DataFrame(x=[(1, 1), (2, 2), (4, 4)])
        end

        for p in [:c1, :d1], q in [:c2, :d2]
            @test combine(table, AsTable([p, q]) => sum => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(sum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            if p == :c1 && q == :c2
                @test_throws ArgumentError combine(table, AsTable([p, q]) =>
                                                          ByRow(sum∘skipmissing) => :x)
            else
                @test combine(table, AsTable([p, q]) => ByRow(sum∘skipmissing) => :x) ≅
                      DataFrame(x=[0, 0, 0])
            end
            @test combine(table, AsTable([p, q]) => length => :x) ==
                  DataFrame(x=2)
            @test combine(table, AsTable([p, q]) => ByRow(length) => :x) ==
                  DataFrame(x=[2, 2, 2])
            if table isa DataFrame
                @test combine(table, AsTable([p, q]) => ByRow(length∘skipmissing) => :x) ==
                      DataFrame(x=[0, 0, 0])
            end
            @test combine(table, AsTable([p, q]) => mean => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(mean) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            if p == :c1 && q == :c2
                @test_throws ArgumentError combine(table, AsTable([p, q]) =>
                                                          ByRow(mean∘skipmissing) => :x)
            else
                @test combine(table, AsTable([p, q]) => ByRow(mean∘skipmissing) => :x) ≅
                      DataFrame(x=[NaN, NaN, NaN])
            end
            if table isa DataFrame
                @test combine(table, AsTable([p, q]) => ByRow(var) => :x) ≅
                      DataFrame(x=[missing, missing, missing])
                if p == :c1 && q == :c2
                    @test_throws Exception combine(table, AsTable([p, q]) =>
                                                   ByRow(var∘skipmissing) => :x)
                else
                    @test combine(table, AsTable([p, q]) => ByRow(var∘skipmissing) => :x) ≅
                          DataFrame(x=[NaN, NaN, NaN])
                end
                @test combine(table, AsTable([p, q]) => ByRow(std) => :x) ≅
                      DataFrame(x=[missing, missing, missing])
                if p == :c1 && q == :c2
                    @test_throws Exception combine(table, AsTable([p, q]) =>
                                                   ByRow(std∘skipmissing) => :x)
                else
                    @test combine(table, AsTable([p, q]) => ByRow(std∘skipmissing) => :x) ≅
                          DataFrame(x=[NaN, NaN, NaN])
                end
            end
            @test combine(table, AsTable([p, q]) => ByRow(median) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test_throws ArgumentError combine(table, AsTable([p, q]) => ByRow(median∘skipmissing) => :x)
            @test combine(table, AsTable([p, q]) => minimum => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(minimum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test_throws Union{MethodError, ArgumentError} combine(table, AsTable([p, q]) =>
                                                                          ByRow(minimum∘skipmissing) => :x)
            @test combine(table, AsTable([p, q]) => maximum => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test combine(table, AsTable([p, q]) => ByRow(maximum) => :x) ≅
                  DataFrame(x=[missing, missing, missing])
            @test_throws Union{MethodError, ArgumentError} combine(table, AsTable([p, q]) =>
                                                                          ByRow(maximum∘skipmissing) => :x)
            @test combine(table, AsTable([p, q]) => ByRow(extrema) => :x) ≅
                  DataFrame(x=[(missing, missing), (missing, missing), (missing, missing)])
            @test_throws Union{MethodError, ArgumentError} combine(table, AsTable([p, q]) =>
                                                                          ByRow(extrema∘skipmissing) => :x)
        end
    end
end

@testset "Tables.AbstractRow interface" begin
    cr = first(Tables.rows((a=1:2, b=3:4)))
    dr = first(Tables.dictrowtable((a=1:2, b=3:4)))
    ir1 = first(Tables.IteratorWrapper(Tables.rows((a=1:2, b=3:4))))
    ir2 = first(Tables.IteratorWrapper([(a=1, b=3), (a=2, b=4)]))
    mr = first(Tables.rows(Tables.table([1 3; 2 4], header=[:a, :b])))
    dfr = DataFrame(a=1:2, b=3:4)[1, :]
    for row in (cr, dr, ir1, ir2, mr, dfr,
                Tables.Row(cr), Tables.Row(dr), Tables.Row(ir1),
                Tables.Row(ir2), Tables.Row(mr), Tables.Row(dfr))
        df = DataFrame(x=[1, 1, 2])
        @test combine(df, :x => (x -> row) => AsTable) ==
            DataFrame(a=1, b=3)
        @test combine(df, x -> row) ==
            DataFrame(a=1, b=3)
        @test select(df, :x => (x -> row) => AsTable) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test select(df, x -> row) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test combine(df, :x => ByRow(x -> row) => AsTable) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test select(df, :x => ByRow(x -> row) => AsTable) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test select(df, :x => (x -> row) => AsTable) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test combine(df, :x => (x -> row) => [:p, :q]) ==
            DataFrame(p=1, q=3)
        @test select(df, :x => (x -> row) => [:p, :q]) ==
            DataFrame(p=[1, 1, 1], q=[3, 3, 3])
        @test combine(groupby(df, :x), :x => (x -> row) => AsTable) ==
            DataFrame(x=[1, 2], a=[1, 1], b=[3, 3])
        @test combine(groupby(df, :x), x -> row) ==
            DataFrame(x=[1, 2], a=[1, 1], b=[3, 3])
        @test select(groupby(df, :x), :x => (x -> row) => AsTable) ==
            DataFrame(x=[1, 1, 2], a=[1, 1, 1], b=[3, 3, 3])
        @test select(groupby(df, :x), x -> row) ==
            DataFrame(x=[1, 1, 2], a=[1, 1, 1], b=[3, 3, 3])
        @test combine(groupby(df, :x), :x => ByRow(x -> row) => AsTable) ==
            DataFrame(x=[1, 1, 2], a=[1, 1, 1], b=[3, 3, 3])
        @test select(groupby(df, :x), :x => ByRow(x -> row) => AsTable) ==
            DataFrame(x=[1, 1, 2], a=[1, 1, 1], b=[3, 3, 3])
        @test combine(groupby(df, :x), :x => (x -> row) => [:p, :q]) ==
            DataFrame(x=[1, 2], p=[1, 1], q=[3, 3])
        @test select(groupby(df, :x), :x => (x -> row) => [:p, :q]) ==
            DataFrame(x=[1, 1, 2], p=[1, 1, 1], q=[3, 3, 3])
        @test_throws ArgumentError combine(df, :x => x -> row)
        @test_throws ArgumentError select(df, :x => x -> row)
        @test combine(df, :x => ByRow(x -> row) => :v) ==
              DataFrame(v=[row, row, row])
        @test_throws ArgumentError combine(groupby(df, :x), :x => x -> row)
        @test_throws ArgumentError select(groupby(df, :x), :x => x -> row)
        @test combine(groupby(df, :x), :x => ByRow(x -> row) => :v) ==
              DataFrame(x=[1, 1, 2], v=[row, row, row])
    end

    # note the grouping of Tables.AbstractRow types
    # they have a matching type of return value of keys (tuple vs vector)
    for df in (DataFrame(id=[1, 1, 2], x=Any[cr, ir1, ir2]),
               DataFrame(id=[1, 1, 2], x=Any[dr, dfr, mr]))
        @test combine(df, :x => AsTable) ==
            DataFrame(a=[1, 1, 1], b=[3, 3, 3])
        @test combine(groupby(df, :id), :x => AsTable) ==
            DataFrame(id=[1, 1, 2], a=[1, 1, 1], b=[3, 3, 3])
    end

    # example from issue https://github.com/JuliaData/DataFrames.jl/issues/3335
    @test combine(groupby(DataFrame(:group=>[1, 1, 2, 2]), :group),
                  sdf -> Tables.Row((; foo="foo", boo=[1, 2]))) ==
          DataFrame(group=1:2, foo=["foo", "foo"], boo=[[1, 2], [1, 2]])
    @test combine(DataFrame(:group=>[1, 1, 2, 2]),
                  sdf -> Tables.Row((; foo="foo", boo=[1, 2]))) ==
          DataFrame(foo=["foo"], boo=[[1, 2]])

    gdf = groupby(DataFrame(x=1:2), :x)
    @test_throws ArgumentError combine(gdf, :x => (x -> x[1] == 1 ? "x" : cr))
    @test_throws ArgumentError combine(gdf, :x => (x -> x[1] == 2 ? "x" : cr) => AsTable)
end

@testset "empty vector" begin
    df = DataFrame(a=1:3)

    @test_throws ArgumentError select(df, :a => (x -> Vector{Any}[]))

    for T in (Vector{Any}, Any, NamedTuple{(:x,),Tuple{Int64}})
        v = combine(df, :a => (x -> T[])).a_function
        @test isempty(v)
        @test eltype(v) === T
    end

    @test size(combine(df, :a => (x -> Vector{Any}[]) => AsTable)) == (0, 0)
    @test size(combine(df, :a => (x -> Any[]) => AsTable)) == (0, 0)
    df2 = combine(df, :a => (x -> NamedTuple{(:x,),Tuple{Int64}}[]) => AsTable)
    @test size(df2) == (0, 1)
    @test eltype(df2.x) === Int64
end

end # module
