module TestInsertion

using DataFrames, Test, Logging, DataStructures, PooledArrays
const ≅ = isequal

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
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1, b=2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, Dict(:a=>1, :b=>2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
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
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, (a=1, b=2, c=3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError push!(df, Dict(:a=>1, :b=>2, :c=>3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    @test_throws AssertionError push!(df, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError push!(df, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame(a=1, b=2)
    push!(df, [11 12])
    @test df == DataFrame(a=[1, 11], b=[2, 12])
    push!(df, (111, 112))
    @test df == DataFrame(a=[1, 11, 111], b=[2, 12, 112])

    @test_throws ArgumentError push!(df, "ab")
end

@testset "pushfirst!(df, row)" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df = DataFrame(first=[3, 1, 2], second=["pear", "apple", "orange"])

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfc = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, Any[3, "pear"])
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, (3, "pear"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws InexactError pushfirst!(dfb, (33.33, "pear"))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    @test_throws DimensionMismatch pushfirst!(dfb, (1, "2", 3))
    @test dfc == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, ("coconut", 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, (11, 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, Dict(:first=>3, :second=>"pear"))
    @test df == dfb

    df = DataFrame(first=[3, 1, 2], second=["banana", "apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, Dict(:first=>3, :second=>"banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, (first=3, second="banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, (second=3, first=3))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    pushfirst!(dfb, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, Dict(:first=>true, :second=>false))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, Dict(:first=>"chicken", :second=>"stuff"))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError pushfirst!(dfb, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df = DataFrame(x=1)
    pushfirst!(df, Dict(:x=>2), Dict(:x=>3))
    @test df[!, :x] == [2, 3, 1]

    df = DataFrame(x=1, y=2)
    pushfirst!(df, [3, 4], [5, 6])
    @test df[!, :x] == [3, 5, 1] && df[!, :y] == [4, 6, 2]

    df = DataFrame(x=1, y=2)
    with_logger(sl) do
        @test_throws KeyError pushfirst!(df, Dict(:x=>1, "y"=>2))
    end
    @test df == DataFrame(x=1, y=2)
    @test occursin("Error adding value to column :y", String(take!(buf)))

    df = DataFrame()
    @test pushfirst!(df, (a=1, b=true)) === df
    @test df == DataFrame(a=1, b=true)

    df = DataFrame()
    df.a = [1, 2, 3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, [1, 2])
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, (a=1, b=2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, Dict(:a=>1, :b=>2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    @test_throws AssertionError pushfirst!(df, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame()
    df.a = [1, 2, 3, 4]
    df.b = df.a
    df.c = [1, 2, 3, 4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, [1, 2, 3])
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, (a=1, b=2, c=3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, Dict(:a=>1, :b=>2, :c=>3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    @test_throws AssertionError pushfirst!(df, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame(a=1, b=2)
    pushfirst!(df, [11 12])
    @test df == DataFrame(a=[11, 1], b=[12, 2])
    pushfirst!(df, (111, 112))
    @test df == DataFrame(a=[111, 11, 1], b=[112, 12, 2])

    @test_throws ArgumentError pushfirst!(df, "ab")
end

@testset "insert!(df, idx, row)" begin
    @test_throws ArgumentError insert!(DataFrame(), -1, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(), true, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(), 2, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(), -1, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(), true, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(), 2, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(), -1, DataFrame(a=1, b=2)[1, :])
    @test_throws ArgumentError insert!(DataFrame(), true, DataFrame(a=1, b=2)[1, :])
    @test_throws ArgumentError insert!(DataFrame(), 2, DataFrame(a=1, b=2)[1, :])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), -1, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), true, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), 3, [1, 2])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), -1, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), true, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), 3, (a=1, b=2))
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), -1, DataFrame(a=1, b=2)[1, :])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), true, DataFrame(a=1, b=2)[1, :])
    @test_throws ArgumentError insert!(DataFrame(a=1, b=2), 3, DataFrame(a=1, b=2)[1, :])

    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df = DataFrame(first=[1, 3, 2], second=["apple", "pear", "orange"])

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfc = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, Any[3, "pear"])
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, (3, "pear"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws InexactError insert!(dfb, 2, (33.33, "pear"))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    @test_throws DimensionMismatch insert!(dfb, 2, (1, "2", 3))
    @test dfc == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, ("coconut", 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, (11, 22))
    end
    @test dfc == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, Dict(:first=>3, :second=>"pear"))
    @test df == dfb

    df = DataFrame(first=[1, 3, 2], second=["apple", "banana", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, Dict(:first=>3, :second=>"banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, (first=3, second="banana"))
    @test df == dfb

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, (second=3, first=3))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    insert!(dfb, 2, (second="banana", first=3))
    @test df == dfb

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, Dict(:first=>true, :second=>false))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df0 = DataFrame(first=[1, 2], second=["apple", "orange"])
    dfb = DataFrame(first=[1, 2], second=["apple", "orange"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, Dict(:first=>"chicken", :second=>"stuff"))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=[1, 2, 3], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :first", String(take!(buf)))

    df0 = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    dfb = DataFrame(first=["1", "2", "3"], second=["apple", "orange", "pear"])
    with_logger(sl) do
        @test_throws MethodError insert!(dfb, 2, Dict(:first=>"chicken", :second=>1))
    end
    @test df0 == dfb
    @test occursin("Error adding value to column :second", String(take!(buf)))

    df = DataFrame(x=1, y=2)
    with_logger(sl) do
        @test_throws KeyError insert!(df, 2, Dict(:x=>1, "y"=>2))
    end
    @test df == DataFrame(x=1, y=2)
    @test occursin("Error adding value to column :y", String(take!(buf)))

    df = DataFrame()
    @test insert!(df, 1, (a=1, b=true)) === df
    @test df == DataFrame(a=1, b=true)

    df = DataFrame()
    df.a = [1, 2, 3]
    df.b = df.a
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, [1, 2])
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, (a=1, b=2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, Dict(:a=>1, :b=>2))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    @test_throws AssertionError insert!(df, 2, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame()
    df.a = [1, 2, 3, 4]
    df.b = df.a
    df.c = [1, 2, 3, 4]
    dfc = copy(df)
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, [1, 2, 3])
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, (a=1, b=2, c=3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, Dict(:a=>1, :b=>2, :c=>3))
    end
    @test df == dfc
    @test occursin("Error adding value to column :b", String(take!(buf)))
    @test_throws AssertionError insert!(df, 2, df[1, :])
    @test df == dfc
    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, dfc[1, :])
    end
    @test df == dfc

    df = DataFrame(a=1:4, b=11:14)
    insert!(df, 3, [-1 -2])
    @test df == DataFrame(a=[1, 2, -1, 3, 4], b=[11, 12, -2, 13, 14])
    insert!(df, 3, (-11, -12))
    @test df == DataFrame(a=[1, 2, -11, -1, 3, 4], b=[11, 12, -12, -2, 13, 14])

    @test_throws ArgumentError insert!(df, 2, "ab")
end

@testset "extra push! tests" begin
    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test push!(df, (b=1,), cols=:union) ≅
              DataFrame(a=[1, 2, 3, missing], b=[missing, missing, missing, 1])
        @test push!(df, (b=11,), cols=:union) ≅
              DataFrame(a=[1, 2, 3, missing, missing], b=[missing, missing, missing, 1, 11])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError push!(df, (b=1,), cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[1, 2, 3, missing, missing],
                             b=[missing, missing, missing, 1, 11], x=1:5)
        allowmissing!(df, :x)
        @test push!(df, (b=111,), cols=:union, promote=false) ≅
              DataFrame(a=[1, 2, 3, missing, missing, missing],
                        b=[missing, missing, missing, 1, 11, 111], x=[1:5; missing])
    end

    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test push!(df, DataFrame(b=1)[1, :], cols=:union) ≅
              DataFrame(a=[1, 2, 3, missing], b=[missing, missing, missing, 1])
        @test push!(df, DataFrame(b=11)[1, :], cols=:union) ≅
              DataFrame(a=[1, 2, 3, missing, missing], b=[missing, missing, missing, 1, 11])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError push!(df, DataFrame(b=1)[1, :], cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[1, 2, 3, missing, missing],
                             b=[missing, missing, missing, 1, 11], x=1:5)
        allowmissing!(df, :x)
        @test push!(df, DataFrame(b=111)[1, :], cols=:union, promote=false) ≅
              DataFrame(a=[1, 2, 3, missing, missing, missing],
                        b=[missing, missing, missing, 1, 11, 111], x=[1:5; missing])
    end

    @test_throws ArgumentError push!(DataFrame(), (a=1, b=2), cols=:unions)
    @test_throws ArgumentError push!(DataFrame(), DataFrame(a=1, b=2)[1, :], cols=:unions)
    @test_throws ArgumentError push!(DataFrame(), Dict('a'=>1, 'b'=>2), cols=:union)
end

@testset "extra pushfirst! tests" begin
    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test pushfirst!(df, (b=1,), cols=:union) ≅
              DataFrame(a=[missing, 1, 2, 3], b=[1, missing, missing, missing])
        @test pushfirst!(df, (b=11,), cols=:union) ≅
              DataFrame(a=[missing, missing, 1, 2, 3], b=[11, 1, missing, missing, missing])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError pushfirst!(df, (b=1,), cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[missing, missing, 1, 2, 3],
                             b=[11, 1, missing, missing, missing], x=1:5)
        allowmissing!(df, :x)
        @test pushfirst!(df, (b=111,), cols=:union, promote=false) ≅
              DataFrame(a=[missing, missing, missing, 1, 2, 3],
                        b=[111, 11, 1, missing, missing, missing], x=[missing; 1:5])
    end

    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test pushfirst!(df, DataFrame(b=1)[1, :], cols=:union) ≅
              DataFrame(a=[missing, 1, 2, 3], b=[1, missing, missing, missing])
        @test pushfirst!(df, DataFrame(b=11)[1, :], cols=:union) ≅
              DataFrame(a=[missing, missing, 1, 2, 3], b=[11, 1, missing, missing, missing])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError pushfirst!(df, DataFrame(b=1)[1, :], cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[missing, missing, 1, 2, 3],
                             b=[11, 1, missing, missing, missing], x=1:5)
        allowmissing!(df, :x)
        @test pushfirst!(df, DataFrame(b=111)[1, :], cols=:union, promote=false) ≅
              DataFrame(a=[missing, missing, missing, 1, 2, 3],
                        b=[111, 11, 1, missing, missing, missing], x=[missing; 1:5])
    end

    @test_throws ArgumentError pushfirst!(DataFrame(), (a=1, b=2), cols=:unions)
    @test_throws ArgumentError pushfirst!(DataFrame(), DataFrame(a=1, b=2)[1, :], cols=:unions)
    @test_throws ArgumentError pushfirst!(DataFrame(), Dict('a'=>1, 'b'=>2), cols=:union)
end

@testset "extra insert! tests" begin
    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test insert!(df, 2, (b=1,), cols=:union) ≅
              DataFrame(a=[1, missing, 2, 3], b=[missing, 1, missing, missing])
        @test insert!(df, 2, (b=11,), cols=:union) ≅
              DataFrame(a=[1, missing, missing, 2, 3], b=[missing, 11, 1, missing, missing])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError insert!(df, 2, (b=1,), cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[1, missing, missing, 2, 3],
                             b=[missing, 11, 1, missing, missing], x=1:5)
        allowmissing!(df, :x)
        @test insert!(df, 2, (b=111,), cols=:union, promote=false) ≅
              DataFrame(a=[1, missing, missing, missing, 2, 3],
                        b=[missing, 111, 11, 1, missing, missing], x=[1, missing, 2, 3, 4, 5])
    end

    for df in [DataFrame(a=Any[1, 2, 3]), DataFrame(a=1:3)]
        @test insert!(df, 2, DataFrame(b=1)[1, :], cols=:union) ≅
              DataFrame(a=[1, missing, 2, 3], b=[missing, 1, missing, missing])
        @test insert!(df, 2, DataFrame(b=11)[1, :], cols=:union) ≅
              DataFrame(a=[1, missing, missing, 2, 3], b=[missing, 11, 1, missing, missing])
        df.x = 1:5
        with_logger(SimpleLogger(IOBuffer())) do
            @test_throws MethodError insert!(df, 2, DataFrame(b=1)[1, :], cols=:union, promote=false)
        end
        @test df ≅ DataFrame(a=[1, missing, missing, 2, 3],
                             b=[missing, 11, 1, missing, missing], x=1:5)
        allowmissing!(df, :x)
        @test insert!(df, 2, DataFrame(b=111)[1, :], cols=:union, promote=false) ≅
              DataFrame(a=[1, missing, missing, missing, 2, 3],
                        b=[missing, 111, 11, 1, missing, missing], x=[1, missing, 2, 3, 4, 5])
    end

    @test_throws ArgumentError insert!(DataFrame(), 2, (a=1, b=2), cols=:unions)
    @test_throws ArgumentError insert!(DataFrame(), 2, DataFrame(a=1, b=2)[1, :], cols=:unions)
    @test_throws ArgumentError insert!(DataFrame(), 2, Dict('a'=>1, 'b'=>2), cols=:union)
end

@testset "push!/pushfirst!/insert! with :orderequal" begin
    for v in ((a=10, b=20, c=30),
              DataFrame(a=10, b=20, c=30)[1, :],
              OrderedDict(:a=>10, :b=>20, :c=>30))
        df = DataFrame(a=1:3, b=2:4, c=3:5)
        push!(df, v, cols=:orderequal)
        @test df == DataFrame(a=[1:3; 10], b=[2:4; 20], c=[3:5; 30])
        pushfirst!(df, v, cols=:orderequal)
        @test df == DataFrame(a=[10; 1:3; 10], b=[20; 2:4; 20], c=[30; 3:5; 30])
        insert!(df, 3, v, cols=:orderequal)
        @test df == DataFrame(a=[10; 1; 10; 2:3; 10], b=[20; 2; 20; 3:4; 20], c=[30; 3; 30; 4:5; 30])
    end

    for v in ((a=10, b=20, d=30), (a=10, c=20, b=30),
              DataFrame(a=10, c=20, b=30)[1, :],
              (a=10, b=20, c=30, d=0),
              DataFrame(a=10, b=20, c=30, d=0)[1, :],
              Dict(:a=>10, :b=>20, :c=>30),
              OrderedDict(:c=>10, :b=>20, :a=>30))
        df = DataFrame(a=1:3, b=2:4, c=3:5)
        @test_throws ArgumentError push!(df, v, cols=:orderequal)
        @test_throws ArgumentError pushfirst!(df, v, cols=:orderequal)
        @test_throws ArgumentError insert!(df, 2, v, cols=:orderequal)
        @test df == DataFrame(a=1:3, b=2:4, c=3:5)
    end
end

@testset "push!/pushfirst!/insert! with :subset" begin
    for v in (Dict(:a=>10, :b=>20, :d=>30), (a=10, b=20, d=30),
              DataFrame(a=10, b=20, d=30)[1, :])
        df = DataFrame(a=1:3, b=2:4, c=3:5)
        old_logger = global_logger(NullLogger())
        @test_throws MethodError push!(df, v, cols=:subset, promote=false)
        global_logger(old_logger)
        @test df == DataFrame(a=1:3, b=2:4, c=3:5)
        old_logger = global_logger(NullLogger())
        @test_throws MethodError pushfirst!(df, v, cols=:subset, promote=false)
        global_logger(old_logger)
        @test df == DataFrame(a=1:3, b=2:4, c=3:5)
        old_logger = global_logger(NullLogger())
        @test_throws MethodError insert!(df, 2, v, cols=:subset, promote=false)
        global_logger(old_logger)
        @test df == DataFrame(a=1:3, b=2:4, c=3:5)
    end

    for v in (Dict(:a=>10, :b=>20, :d=>30), (a=10, b=20, d=30),
              DataFrame(a=10, b=20, d=30)[1, :])
        df = DataFrame(a=1:3, b=2:4, c=3:5)
        allowmissing!(df, :c)
        push!(df, v, cols=:subset, promote=false)
        @test df ≅ DataFrame(a=[1, 2, 3, 10], b=[2, 3, 4, 20], c=[3, 4, 5, missing])
        old_logger = global_logger(NullLogger())
        @test_throws MethodError push!(df, Dict(), cols=:subset, promote=false)
        global_logger(old_logger)
        @test df ≅ DataFrame(a=[1, 2, 3, 10], b=[2, 3, 4, 20], c=[3, 4, 5, missing])
        allowmissing!(df, [:a, :b])
        push!(df, Dict(), cols=:subset)
        @test df ≅ DataFrame(a=[1, 2, 3, 10, missing], b=[2, 3, 4, 20, missing],
                             c=[3, 4, 5, missing, missing])

        df = DataFrame(a=1:3, b=2:4, c=3:5)
        allowmissing!(df, :c)
        pushfirst!(df, v, cols=:subset, promote=false)
        @test df ≅ DataFrame(a=[10, 1, 2, 3], b=[20, 2, 3, 4], c=[missing, 3, 4, 5])
        old_logger = global_logger(NullLogger())
        @test_throws MethodError pushfirst!(df, Dict(), cols=:subset, promote=false)
        global_logger(old_logger)
        @test df ≅ DataFrame(a=[10, 1, 2, 3], b=[20, 2, 3, 4], c=[missing, 3, 4, 5])
        allowmissing!(df, [:a, :b])
        pushfirst!(df, Dict(), cols=:subset)
        @test df ≅ DataFrame(a=[missing, 10, 1, 2, 3], b=[missing, 20, 2, 3, 4],
                             c=[missing, missing, 3, 4, 5])

        df = DataFrame(a=1:3, b=2:4, c=3:5)
        allowmissing!(df, :c)
        insert!(df, 2, v, cols=:subset, promote=false)
        @test df ≅ DataFrame(a=[1, 10, 2, 3], b=[2, 20, 3, 4], c=[3, missing, 4, 5])
        old_logger = global_logger(NullLogger())
        @test_throws MethodError insert!(df, 2, Dict(), cols=:subset, promote=false)
        global_logger(old_logger)
        @test df ≅ DataFrame(a=[1, 10, 2, 3], b=[2, 20, 3, 4], c=[3, missing, 4, 5])
        allowmissing!(df, [:a, :b])
        insert!(df, 2, Dict(), cols=:subset)
        @test df ≅ DataFrame(a=[1, missing, 10, 2, 3], b=[2, missing, 20, 3, 4],
                             c=[3, missing, missing, 4, 5])
    end
end

@testset "push!/pushfirst!/insert! with :intersect" begin
    for row in ((y=4, x=3), Dict(:y=>4, :x=>3), (z=1, y=4, x=3), Dict(:y=>4, :x=>3, :z=>1))
        df = DataFrame(x=[1, 1], y=[2, 2])
        push!(df, row, cols=:intersect)
        @test df == DataFrame(x=[1, 1, 3], y=[2, 2, 4])
        pushfirst!(df, row, cols=:intersect)
        @test df == DataFrame(x=[3, 1, 1, 3], y=[4, 2, 2, 4])
        insert!(df, 3, row, cols=:intersect)
        @test df == DataFrame(x=[3, 1, 3, 1, 3], y=[4, 2, 4, 2, 4])
    end

    old_logger = global_logger(NullLogger())
    for row in ((z=4, x=3), (z=1, p=4, x=3))
        df = DataFrame(x=1:3, y=2:4)
        @test_throws ErrorException push!(df, row, cols=:intersect)
        @test_throws ErrorException pushfirst!(df, row, cols=:intersect)
        @test_throws ErrorException insert!(df, 2, row, cols=:intersect)
        @test df == DataFrame(x=1:3, y=2:4)
    end

    for row in (Dict(:z=>4, :x=>3), Dict(:p=>4, :x=>3, :z=>1))
        df = DataFrame(x=1:3, y=2:4)
        @test_throws KeyError push!(df, row, cols=:intersect)
        @test_throws KeyError pushfirst!(df, row, cols=:intersect)
        @test_throws KeyError insert!(df, 2, row, cols=:intersect)
        @test df == DataFrame(x=1:3, y=2:4)
    end
    global_logger(old_logger)
end

@testset "push! with :union" begin
    df = DataFrame()
    push!(df, (a=1, b=2))
    a = df.a
    push!(df, (a=1, c=2), cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[2, missing],
                         c=[missing, 2])
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

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    push!(df, (a=11,), cols=:union)
    @test df.a === a
    push!(df, (a=12.0,), cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError push!(df, (a=1, b=2, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError push!(df, (a=1, b=2.0, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

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

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    push!(df, DataFrame(a=1)[1, :], cols=:union)
    @test df.a === a
    push!(df, DataFrame(a=1.0)[1, :], cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError push!(df, DataFrame(a=1, b=2, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError push!(df, DataFrame(a=1, b=2.0, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x
end

@testset "pushfirst! with :union" begin
    df = DataFrame()
    pushfirst!(df, (a=1, b=2))
    a = df.a
    pushfirst!(df, (a=1, c=2), cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[missing, 2],
                         c=[2, missing])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, (a=1, c=2), cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, (c=2,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, (c=missing,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    pushfirst!(df, (c="a", d=1), cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    pushfirst!(df, (a="b",), cols=:union)
    @test df ≅ DataFrame(a=["b", missing, missing],
                         c=[missing, "a", missing],
                         d=[missing, 1, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    pushfirst!(df, (a=11,), cols=:union)
    @test df.a === a
    pushfirst!(df, (a=12.0,), cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError pushfirst!(df, (a=1, b=2, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError pushfirst!(df, (a=1, b=2.0, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    df = DataFrame()
    pushfirst!(df, DataFrame(a=1, b=2)[1, :])
    a = df.a
    pushfirst!(df, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[missing, 2], c=[2, missing])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, DataFrame(c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, DataFrame(c=missing)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    pushfirst!(df, DataFrame(c="a", d=1)[1, :], cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    pushfirst!(df, DataFrame(a="b")[1, :], cols=:union)
    @test df ≅ DataFrame(a=["b", missing, missing],
                         c=[missing, "a", missing],
                         d=[missing, 1, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    pushfirst!(df, DataFrame(a=1)[1, :], cols=:union)
    @test df.a === a
    pushfirst!(df, DataFrame(a=1.0)[1, :], cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError pushfirst!(df, DataFrame(a=1, b=2, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError pushfirst!(df, DataFrame(a=1, b=2.0, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x
end

@testset "insert! with :union" begin
    df = DataFrame()
    insert!(df, 1, (a=1, b=2))
    a = df.a
    insert!(df, 1, (a=1, c=2), cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[missing, 2],
                         c=[2, missing])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    pushfirst!(df, (a=1, c=2), cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    insert!(df, 1, (c=2,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    insert!(df, 1, (c=missing,), cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    insert!(df, 1, (c="a", d=1), cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    insert!(df, 2, (a="b",), cols=:union)
    @test df ≅ DataFrame(a=[missing, "b", missing],
                         c=["a", missing, missing],
                         d=[1, missing, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    insert!(df, 2, (a=11,), cols=:union)
    @test df.a === a
    insert!(df, 2, (a=12.0,), cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError insert!(df, 2, (a=1, b=2, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError insert!(df, 2, (a=1, b=2.0, c=3), cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    df = DataFrame()
    insert!(df, 1, DataFrame(a=1, b=2)[1, :])
    a = df.a
    insert!(df, 1, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[1, 1], b=[missing, 2], c=[2, missing])
    @test df.a === a
    @test eltype(df.a) === Int

    df = DataFrame(a=Int[])
    insert!(df, 1, DataFrame(a=1, c=2)[1, :], cols=:union)
    @test df == DataFrame(a=[1], c=[2])
    @test eltype(df.a) === Int
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    insert!(df, 1, DataFrame(c=2)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[2])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Int

    df = DataFrame(a=Int[])
    insert!(df, 1, DataFrame(c=missing)[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing], c=[missing])
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Missing

    insert!(df, 1, DataFrame(c="a", d=1)[1, :], cols=:union)
    @test eltype(df.a) === Union{Int, Missing}
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    insert!(df, 2, DataFrame(a="b")[1, :], cols=:union)
    @test df ≅ DataFrame(a=[missing, "b", missing],
                         c=["a", missing, missing],
                         d=[1, missing, missing])
    @test eltype(df.a) === Any
    @test eltype(df.c) === Union{String, Missing}
    @test eltype(df.d) === Union{Int, Missing}

    a = [1, 2, 3]
    df = DataFrame(a=a, copycols=false)
    insert!(df, 2, DataFrame(a=1)[1, :], cols=:union)
    @test df.a === a
    insert!(df, 2, DataFrame(a=1.0)[1, :], cols=:union)
    @test df.a !== a
    @test eltype(df.a) === Float64

    x = [1, 2, 3]
    df = DataFrame(a=x, b=x, copycols=false)
    @test_throws AssertionError insert!(df, 2, DataFrame(a=1, b=2, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x

    @test_throws AssertionError insert!(df, 2, DataFrame(a=1, b=2.0, c=3)[1, :], cols=:union)
    @test df == DataFrame(a=x, b=x, copycols=false)
    @test df.a === df.b === x
end

@testset "push!/pushfirst!/insert! with promote options" begin
    df = DataFrame(a=1:3)
    with_logger(SimpleLogger(IOBuffer())) do
        @test_throws MethodError push!(df, ["a"])
    end
    @test push!(df, ["a"], promote=true) == DataFrame(a=[1:3; "a"])

    df = DataFrame(a=1:3)
    with_logger(SimpleLogger(IOBuffer())) do
        @test_throws MethodError pushfirst!(df, ["a"])
    end
    @test pushfirst!(df, ["a"], promote=true) == DataFrame(a=["a"; 1:3])

    df = DataFrame(a=1:3)
    with_logger(SimpleLogger(IOBuffer())) do
        @test_throws MethodError insert!(df, 2, ["a"])
    end
    @test insert!(df, 2, ["a"], promote=true) == DataFrame(a=[1, "a", 2, 3])

    for v in ((a="a",), DataFrame(a="a")[1, :])
        for cols in [:orderequal, :setequal, :intersect]
            df = DataFrame(a=1:3)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError push!(df, v, cols=cols)
            end
            @test push!(df, v, cols=cols, promote=true) == DataFrame(a=[1:3; "a"])

            df = DataFrame(a=1:3)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError pushfirst!(df, v, cols=cols)
            end
            @test pushfirst!(df, v, cols=cols, promote=true) == DataFrame(a=["a"; 1:3])

            df = DataFrame(a=1:3)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError insert!(df, 2, v, cols=cols)
            end
            @test insert!(df, 2, v, cols=cols, promote=true) == DataFrame(a=[1, "a", 2, 3])

        end
        for cols in [:subset, :union]
            df = DataFrame(a=1:3, b=11:13)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError push!(df, v, cols=cols, promote=false)
            end
            @test push!(df, v, cols=cols) ≅ DataFrame(a=[1:3; "a"], b=[11:13; missing])

            df = DataFrame(a=1:3, b=11:13)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError pushfirst!(df, v, cols=cols, promote=false)
            end
            @test pushfirst!(df, v, cols=cols) ≅ DataFrame(a=["a"; 1:3], b=[missing; 11:13])

            df = DataFrame(a=1:3, b=11:13)
            with_logger(SimpleLogger(IOBuffer())) do
                @test_throws MethodError insert!(df, 2, v, cols=cols, promote=false)
            end
            @test insert!(df, 2, v, cols=cols) ≅ DataFrame(a=[1, "a", 2, 3], b=[11, missing, 12, 13])
        end
    end
end

@testset "push!/pushfirst!/insert! with :setequal and wrong number of entries" begin
    df = DataFrame(a=1:3)
    @test_throws ArgumentError push!(df, (a=10, b=20))
    @test_throws ArgumentError push!(df, "a")
    @test_throws ArgumentError pushfirst!(df, (a=10, b=20))
    @test_throws ArgumentError pushfirst!(df, "a")
    @test_throws ArgumentError insert!(df, 2, (a=10, b=20))
    @test_throws ArgumentError insert!(df, 2, "a")
end

@testset "push!/pushfirst!/insert! with self" begin
    df = DataFrame(a=1:3, b=2:4, c=3:5)
    @test push!(df, df[2, :]) == DataFrame(a=[1:3; 2], b=[2:4; 3], c=[3:5; 4])
    @test pushfirst!(df, df[3, :]) == DataFrame(a=[3; 1:3; 2], b=[4; 2:4; 3], c=[5; 3:5; 4])
    @test insert!(df, 3, df[1, :]) == DataFrame(a=[3; 1; 3; 2:3; 2], b=[4; 2; 4; 3:4; 3], c=[5; 3; 5; 4:5; 4])
    df = DataFrame(a=1:3, b=2:4)
    df.c = df.a
    @test_throws AssertionError push!(df, df[2, :])
    @test_throws AssertionError pushfirst!(df, df[2, :])
    @test_throws AssertionError insert!(df, 2, df[2, :])
    @test df == DataFrame(a=1:3, b=2:4, c=1:3)
end

@testset "multicolumn aliasing" begin
    df = DataFrame(a1=1:3, b1=11:13)
    df.a2 = df.a1
    df.a3 = df.a1
    df.b2 = df.b1
    df.b3 = df.b1
    df.a4 = df.a1
    refdf = copy(df)

    buf = IOBuffer()
    sl = SimpleLogger(buf)

    with_logger(sl) do
        @test_throws AssertionError push!(df, 1:7)
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf

    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, 1:7)
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf

    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, 1:7)
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf

    with_logger(sl) do
        @test_throws AssertionError push!(df, (a1=1, b1=2, a2=3, a3=4, b2=5, b3=6, a4=7))
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf

    with_logger(sl) do
        @test_throws AssertionError pushfirst!(df, (a1=1, b1=2, a2=3, a3=4, b2=5, b3=6, a4=7))
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf

    with_logger(sl) do
        @test_throws AssertionError insert!(df, 2, (a1=1, b1=2, a2=3, a3=4, b2=5, b3=6, a4=7))
    end
    @test occursin("Error adding value to column :a2", String(take!(buf)))
    @test df == refdf
end

@testset "Tables.AbstractRow insertion" begin
    tab = Tables.table([1 2 3; 4 5 6; 9 10 11; 12 13 14], header=[:a, :b, :c])
    rows = tab |> Tables.rows |> collect
    df = DataFrame()
    @test push!(df, rows[2]) == DataFrame(a=4, b=5, c=6)
    @test pushfirst!(df, rows[1], promote=false, cols=:union) ==
          DataFrame(a=[1, 4], b=[2, 5], c=[3, 6])
    @test insert!(df, 2, rows[3], promote=true, cols=:intersect) ==
        DataFrame(a=[1, 9, 4], b=[2, 10, 5], c=[3, 11, 6])
    @test push!(df, rows[1], promote=true, cols=:orderequal) ==
        DataFrame(a=[1, 9, 4, 1], b=[2, 10, 5, 2], c=[3, 11, 6, 3])
    deleteat!(df, nrow(df))

    df2 = DataFrame(d="x", a="y")
    push!(df2, rows[1], cols=:union)
    @test df2 ≅ DataFrame(d=["x", missing], a=["y", 1], b=[missing, 2], c=[missing, 3])

    tab = Tables.table(Any[15 16.5], header=[:d, :c])
    row = tab |> Tables.rows |> first
    @test push!(df, row, cols=:union) ≅
                DataFrame(a=[1, 9, 4, missing],
                          b=[2, 10, 5, missing],
                          c=[3.0, 11.0, 6.0, 16.5],
                          d=[missing, missing, missing, 15])

    tab = Tables.table(Any[21 22.5], header=[:x, :b])
    row = tab |> Tables.rows |> first
    @test pushfirst!(df, row, cols=:subset) ≅
                     DataFrame(a=[missing, 1, 9, 4, missing],
                               b=[22.5, 2.0, 10.0, 5.0, missing],
                               c=[missing, 3.0, 11.0, 6.0, 16.5],
                               d=[missing, missing, missing, missing, 15])

    tab = Tables.table(["a" "b" "c" "d" "e"], header=[:a, :b, :c, :d, :e])
    row = tab |> Tables.rows |> first

    buf = IOBuffer()
    sl = SimpleLogger(buf)
    with_logger(sl) do
        @test_throws MethodError insert!(df, 3, row, cols=:intersect)
    end
    @test occursin("Error adding value to column :a", String(take!(buf)))

    @test insert!(df, 3, row, cols=:intersect, promote=true) ≅
                  DataFrame(a=[missing, 1, "a", 9, 4, missing],
                            b=[22.5, 2.0, "b", 10.0, 5.0, missing],
                            c=[missing, 3.0, "c", 11.0, 6.0, 16.5],
                            d=[missing, missing, "d", missing, missing, 15])
    for i in [1, 2, 4, 8, 16, 32, 64, 100, 1000, 10000, 20_000, 210_000]
        df = DataFrame()
        mat = Any[a + 100 * b + (iseven(b) ? 0.5 : 0) for a in 1:2, b in 1:i]
        tab = Tables.table(mat, header=Symbol.("x", 1:i))
        for row in Tables.rows(tab)
            push!(df, row)
        end
        @test eltype.(eachcol(df)) == [(isodd(i) ? Int : Float64) for i in 1:ncol(df)]
        @test df == DataFrame(mat, :auto)
    end
end

@testset "PooledArray error #3356" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    with_logger(sl) do
        @test_throws ErrorException append!(DataFrame(x=PooledArray(1:255, UInt8)),
                                            DataFrame(x=PooledArray(256:500, UInt8)))
        @test_throws ErrorException append!(DataFrame(x=PooledArray(1:255, UInt8)),
                                            DataFrame(x=PooledArray(256:500, UInt8)),
                                            promote=true)
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), [1000])
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), [1000],
                                          promote=true)
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), (x=1000,))
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), (x=1000,),
                                          promote=true)
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), (x=1000,),
                                          cols=:union, promote=true)
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), (x=1000,),
                                          cols=:union, promote=false)
        @test_throws ErrorException push!(DataFrame(x=PooledArray(1:255, UInt8)), (x="x",),
                                          cols=:union, promote=true)
        @test_throws MethodError push!(DataFrame(x=PooledArray(1:255, UInt8)), (x="x",),
                                       cols=:union, promote=false)
    end
end

@testset "multi element append!/prepend!/push!/pushfirst!" begin
    df = DataFrame(a=1, b=2)
    @test append!(df) == DataFrame(a=1, b=2)
    @test prepend!(df) == DataFrame(a=1, b=2)
    @test push!(df) == DataFrame(a=1, b=2)
    @test pushfirst!(df) == DataFrame(a=1, b=2)
    @test_throws ArgumentError append!(df, cols=:x) == DataFrame(a=1, b=2)
    @test_throws ArgumentError prepend!(df, cols=:x) == DataFrame(a=1, b=2)
    @test_throws ArgumentError push!(df, cols=:x) == DataFrame(a=1, b=2)
    @test_throws ArgumentError pushfirst!(df, cols=:x) == DataFrame(a=1, b=2)

    for x in (DataFrame(a=3, b=4), (a=[3], b=[4]), [(a=3, b=4)]),
        y in (DataFrame(a=5, b=6), (a=[5], b=[6]), [(a=5, b=6)]),
        z in (DataFrame(a=7, b=8), (a=[7], b=[8]), [(a=7, b=8)])
        @test append!(copy(df), x, y) ==
              DataFrame(a=1:2:5, b=2:2:6)
        @test append!(copy(df), x, y, z) ==
              DataFrame(a=1:2:7, b=2:2:8)
        @test prepend!(copy(df), x, y) ==
              DataFrame(a=[3, 5, 1], b=[4, 6, 2])
        @test prepend!(copy(df), x, y, z) ==
              DataFrame(a=[3, 5, 7, 1], b=[4, 6, 8, 2])
    end

    for x in (DataFrame(a=3, b=4)[1, :], (a=3, b=4)),
        y in (DataFrame(a=5, b=6)[1, :], (a=5, b=6)),
        z in (DataFrame(a=7, b=8)[1, :], (a=7, b=8))
        @test push!(copy(df), x, y) ==
              DataFrame(a=1:2:5, b=2:2:6)
        @test push!(copy(df), x, y, z) ==
              DataFrame(a=1:2:7, b=2:2:8)
        @test pushfirst!(copy(df), x, y) ==
              DataFrame(a=[3, 5, 1], b=[4, 6, 2])
        @test pushfirst!(copy(df), x, y, z) ==
              DataFrame(a=[3, 5, 7, 1], b=[4, 6, 8, 2])
        for cols in (:orderequal, :setequal, :union, :subset, :intersect)
            @test push!(copy(df), x, y, cols=cols) ==
                DataFrame(a=1:2:5, b=2:2:6)
            @test push!(copy(df), x, y, z, cols=cols) ==
                DataFrame(a=1:2:7, b=2:2:8)
            @test pushfirst!(copy(df), x, y, cols=cols) ==
                DataFrame(a=[3, 5, 1], b=[4, 6, 2])
            @test pushfirst!(copy(df), x, y, z, cols=cols) ==
                DataFrame(a=[3, 5, 7, 1], b=[4, 6, 8, 2])
        end
    end

    for x in ((3, 4), [3, 4]), y in ((5, 6), [5, 6]), z in ((7, 8), [7, 8])
        @test push!(copy(df), x, y) ==
              DataFrame(a=1:2:5, b=2:2:6)
        @test push!(copy(df), x, y, z) ==
              DataFrame(a=1:2:7, b=2:2:8)
        @test pushfirst!(copy(df), x, y) ==
              DataFrame(a=[3, 5, 1], b=[4, 6, 2])
        @test pushfirst!(copy(df), x, y, z) ==
              DataFrame(a=[3, 5, 7, 1], b=[4, 6, 8, 2])
    end

    for x in (DataFrame(a=3, b=4), (a=[3], b=[4]), [(a=3, b=4)]),
        y in (DataFrame(a=5, c=6), (a=[5], c=[6]), [(a=5, c=6)]),
        z in (DataFrame(a="7", d=8), (a=["7"], d=[8]), [(a="7", d=8)])
        @test append!(copy(df), x, y, cols=:union) ≅
              DataFrame(a=1:2:5, b=[2, 4, missing], c=[missing, missing, 6])
        @test append!(copy(df), x, y, z, cols=:union) ≅
              DataFrame(a=[1, 3, 5, "7"], b=[2, 4, missing, missing],
                        c=[missing, missing, 6, missing],
                        d=[missing, missing, missing, 8])
        @test prepend!(copy(df), x, y, cols=:union) ≅
              DataFrame(a=[3, 5, 1], b=[4, missing, 2], c=[missing, 6, missing])
        @test prepend!(copy(df), x, y, z, cols=:union) ≅
              DataFrame(a=[3, 5, "7", 1], b=[4, missing, missing, 2],
                        d=[missing, missing, 8, missing],
                        c=[missing, 6, missing, missing],)
    end

    for x in (DataFrame(a=3, b=4)[1, :], (a=3, b=4)),
        y in (DataFrame(a=5, c=6)[1, :], (a=5, c=6)),
        z in (DataFrame(a="7", d=8)[1, :], (a="7", d=8))
        @test push!(copy(df), x, y, cols=:union) ≅
              DataFrame(a=1:2:5, b=[2, 4, missing], c=[missing, missing, 6])
        @test push!(copy(df), x, y, z, cols=:union) ≅
              DataFrame(a=[1, 3, 5, "7"], b=[2, 4, missing, missing],
                        c=[missing, missing, 6, missing],
                        d=[missing, missing, missing, 8])
        @test pushfirst!(copy(df), x, y, cols=:union) ≅
              DataFrame(a=[3, 5, 1], b=[4, missing, 2], c=[missing, 6, missing])
        @test pushfirst!(copy(df), x, y, z, cols=:union) ≅
              DataFrame(a=[3, 5, "7", 1], b=[4, missing, missing, 2],
                        d=[missing, missing, 8, missing],
                        c=[missing, 6, missing, missing],)
    end

    @test_throws ArgumentError push!(df, (1, 2), cols=:union)
    @test_throws ArgumentError pushfirst!(df, (1, 2), cols=:union)

    @test_throws ArgumentError push!(df, (1, 2), (1, 2), cols=:union)
    @test_throws ArgumentError pushfirst!(df, (1, 2), (1, 2), cols=:union)

    @test_throws ArgumentError push!(df, (a=1, b=2), (1, 2))
    @test_throws ArgumentError pushfirst!(df, (a=1, b=2), (1, 2))
    @test_throws ArgumentError push!(df, (1, 2), (a=1, b=2))
    @test_throws ArgumentError pushfirst!(df, (1, 2), (a=1, b=2))

    @test insert!(DataFrame(a=1:3, b=11:13), 2, (0, 10), cols=:setequal) ==
          DataFrame(a=[1, 0, 2, 3], b=[11, 10, 12, 13])
    @test_throws ArgumentError insert!(df, 1, (1, 2), cols=:orderequal)
end

end # module
