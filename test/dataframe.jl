module TestDataFrame
    using Base.Test, DataFrames

    #
    # Equality
    #

    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) == DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2], b=[4, 5]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], c=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(b=[4, 5, 6], a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 2], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, null], b=[4, 5, 6]) ==
                  DataFrame(a=[1, 2, null], b=[4, 5, 6])

    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) == DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2], b=[4, 5]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], c=[4, 5, 6])
    @test DataFrame(a=[1, 2, 3], b=[4, 5, 6]) != DataFrame(b=[4, 5, 6], a=[1, 2, 3])
    @test DataFrame(a=[1, 2, 2], b=[4, 5, 6]) != DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    @test DataFrame(a=[1, 3, null], b=[4, 5, 6]) !=
             DataFrame(a=[1, 2, null], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, null], b=[4, 5, 6]) ==
                DataFrame(a=[1, 2, null], b=[4, 5, 6])
    @test DataFrame(a=[1, 2, null], b=[4, 5, 6]) !=
                DataFrame(a=[1, 2, 3], b=[4, 5, 6])

    #
    # Copying
    #

    df = DataFrame(a = Union{Int, Null}[2, 3],
                   b = Union{DataFrame, Null}[DataFrame(c = 1), DataFrame(d = 2)])
    dfc = copy(df)
    dfdc = deepcopy(df)

    df[1, :a] = 4
    df[1, :b][:e] = 5
    names!(df, [:f, :g])

    @test names(dfc) == [:a, :b]
    @test names(dfdc) == [:a, :b]

    @test dfc[1, :a] === 4
    @test dfdc[1, :a] === 2

    @test names(dfc[1, :b]) == [:c, :e]
    @test names(dfdc[1, :b]) == [:c]

    #

    x = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    v = DataFrame(a = [5, 6, 7], b = [8, 9, 10])

    z = vcat(v, x)

    z2 = z[:, [1, 1, 2]]
    @test names(z2) == [:a, :a_1, :b]

    #test_group("DataFrame assignment")
    # Insert single column
    x0 = x[Int[], :]
    @test_throws ErrorException x0[:d] = [1]
    @test_throws ErrorException x0[:d] = 1:3

    # Insert single value
    x[:d] = 3
    @test x[:d] == [3, 3, 3]

    x0[:d] = 3
    @test x0[:d] == Int[]

    # similar / nulls
    df = DataFrame(a = Union{Int, Null}[1],
                   b = Union{String, Null}["b"],
                   c = CategoricalArray{Union{Float64, Null}}([3.3]))
    nulldf = DataFrame(a = nulls(Int, 2),
                       b = nulls(String, 2),
                       c = CategoricalArray{Union{Float64, Null}}(2))
    @test nulldf == similar(df, 2)

    # Associative methods

    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    @test haskey(df, :a)
    @test !haskey(df, :c)
    @test get(df, :a, -1) === df.columns[1]
    @test get(df, :c, -1) == -1
    @test !isempty(df)

    @test empty!(df) === df
    @test isempty(df.columns)
    @test isempty(df)

    df = DataFrame(a=Union{Int, Null}[1, 2], b=Union{Float64, Null}[3.0, 4.0])
    @test_throws BoundsError insert!(df, 5, ["a", "b"], :newcol)
    @test_throws ErrorException insert!(df, 1, ["a"], :newcol)
    @test insert!(df, 1, ["a", "b"], :newcol) == df
    @test names(df) == [:newcol, :a, :b]
    @test df[:a] == [1, 2]
    @test df[:b] == [3.0, 4.0]
    @test df[:newcol] == ["a", "b"]

    df = DataFrame(a=[1, 2], b=[3.0, 4.0])
    df2 = DataFrame(b=["a", "b"], c=[:c, :d])
    @test merge!(df, df2) == df
    @test df == DataFrame(a=[1, 2], b=["a", "b"], c=[:c, :d])

    #test_group("Empty DataFrame constructors")
    df = DataFrame(Union{Int, Null}, 10, 3)
    @test size(df, 1) == 10
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == Vector{Union{Int, Null}}
    @test typeof(df[:, 2]) == Vector{Union{Int, Null}}
    @test typeof(df[:, 3]) == Vector{Union{Int, Null}}
    @test all(isnull, df[:, 1])
    @test all(isnull, df[:, 2])
    @test all(isnull, df[:, 3])

    df = DataFrame([Union{Int, Null}, Union{Float64, Null}, Union{String, Null}], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == Vector{Union{Int, Null}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Null}}
    @test typeof(df[:, 3]) == Vector{Union{String, Null}}
    @test all(isnull, df[:, 1])
    @test all(isnull, df[:, 2])
    @test all(isnull, df[:, 3])

    df = DataFrame([Union{Int, Null}, Union{Float64, Null}, Union{String, Null}],
                   [:A, :B, :C], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == Vector{Union{Int, Null}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Null}}
    @test typeof(df[:, 3]) == Vector{Union{String, Null}}
    @test all(isnull, df[:, 1])
    @test all(isnull, df[:, 2])
    @test all(isnull, df[:, 3])

    df = DataFrame([Union{Int, Null}, Union{Float64, Null}, Union{String, Null}],
                   [:A, :B, :C], [false, false, true], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == Vector{Union{Int, Null}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Null}}
    @test typeof(df[:, 3]) <: CategoricalVector{Union{String, Null}}
    @test all(isnull, df[:, 1])
    @test all(isnull, df[:, 2])
    @test all(isnull, df[:, 3])

    df = convert(DataFrame, zeros(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, ones(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, eye(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    @test DataFrame([Union{Int, Null}[1, 2, 3], Union{Float64, Null}[2.5, 4.5, 6.5]],
                    [:A, :B]) ==
        DataFrame(A = Union{Int, Null}[1, 2, 3], B = Union{Float64, Null}[2.5, 4.5, 6.5])

    # This assignment was missing before
    df = DataFrame(Column = [:A])
    df[1, :Column] = "Testing"

    # zero-row DataFrame and subDataFrame test
    df = DataFrame(x=[], y=[])
    @test nrow(df) == 0
    df = DataFrame(x=[1:3;], y=[3:5;])
    sdf = view(df, df[:x] .== 4)
    @test size(sdf, 1) == 0

    @test hash(convert(DataFrame, [1 2; 3 4])) == hash(convert(DataFrame, [1 2; 3 4]))
    @test hash(convert(DataFrame, [1 2; 3 4])) != hash(convert(DataFrame, [1 3; 2 4]))


    # push!(df, row)
    df=DataFrame( first=[1,2,3], second=["apple","orange","pear"] )

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, Any[3,"pear"])
    @test df == dfb

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, (3,"pear"))
    @test df == dfb

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, (33.33,"pear"))

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, ("coconut",22))

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, Dict(:first=>3, :second=>"pear"))
    @test df == dfb

    df=DataFrame( first=[1,2,3], second=["apple","orange","banana"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, Dict("first"=>3, "second"=>"banana"))
    @test df == dfb

    df0= DataFrame( first=[1,2], second=["apple","orange"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, Dict(:first=>true, :second=>false))
    @test df0 == dfb

    df0= DataFrame( first=[1,2], second=["apple","orange"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, Dict("first"=>"chicken", "second"=>"stuff"))
    @test df0 == dfb

    # delete!
    df = DataFrame(a=1, b=2, c=3, d=4, e=5)
    @test_throws ArgumentError delete!(df, 0)
    @test_throws ArgumentError delete!(df, 6)
    @test_throws KeyError delete!(df, :f)

    d = copy(df)
    delete!(d, [:a, :e, :c])
    @test names(d) == [:b, :d]
    delete!(d, :b)
    @test d == DataFrame(d=4)

    d = copy(df)
    delete!(d, [2, 5, 3])
    @test names(d) == [:a, :d]
    delete!(d, 2)
    @test d == DataFrame(a=1)

    # deleterows!
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

    df = DataFrame(a=Union{Int, Null}[1, 2], b=Union{Float64, Null}[3.0, 4.0])
    @test deleterows!(df, 1) === df
    @test df == DataFrame(a=[2], b=[4.0])

    df = DataFrame(a=Union{Int, Null}[1, 2], b=Union{Float64, Null}[3.0, 4.0])
    @test deleterows!(df, 2) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Null}[1, 2, 3], b=Union{Float64, Null}[3.0, 4.0, 5.0])
    @test deleterows!(df, 2:3) === df
    @test df == DataFrame(a=[1], b=[3.0])

    df = DataFrame(a=Union{Int, Null}[1, 2, 3], b=Union{Float64, Null}[3.0, 4.0, 5.0])
    @test deleterows!(df, [2, 3]) === df
    @test df == DataFrame(a=[1], b=[3.0])

    # describe
    #suppress output and test that describe() does not throw
    devnull = is_unix() ? "/dev/null" : "nul"
    open(devnull, "w") do f
        @test nothing == describe(f, DataFrame(a=[1, 2], b=Any["3", null]))
        @test nothing ==
              describe(f, DataFrame(a=Union{Int, Null}[1, 2],
                                    b=["3", null]))
        @test nothing ==
              describe(f, DataFrame(a=CategoricalArray([1, 2]),
                                    b=CategoricalArray(["3", null])))
        @test nothing == describe(f, [1, 2, 3])
        @test nothing == describe(f, [1, 2, 3])
        @test nothing == describe(f, CategoricalArray([1, 2, 3]))
        @test nothing == describe(f, Any["1", "2", null])
        @test nothing == describe(f, ["1", "2", null])
        @test nothing == describe(f, CategoricalArray(["1", "2", null]))
    end

    #Check the output of unstack
    df = DataFrame(Fish = CategoricalArray{Union{String, Null}}(["Bob", "Bob", "Batman", "Batman"]),
                   Key = Union{String, Null}["Mass", "Color", "Mass", "Color"],
                   Value = Union{String, Null}["12 g", "Red", "18 g", "Grey"])
    # Check that reordering levels does not confuse unstack
    levels!(df[1], ["XXX", "Bob", "Batman"])
    #Unstack specifying a row column
    df2 = unstack(df, :Fish, :Key, :Value)
    #Unstack without specifying a row column
    df3 = unstack(df, :Key, :Value)
    #The expected output
    df4 = DataFrame(Fish = Union{String, Null}["XXX", "Bob", "Batman"],
                    Color = Union{String, Null}[null, "Red", "Grey"],
                    Mass = Union{String, Null}[null, "12 g", "18 g"])
    @test df2 == df4
    @test typeof(df2[:Fish]) <: CategoricalVector{Union{String, Null}}
    # first column stays as CategoricalArray in df3
    @test df3[:, 2:3] == df4[2:3, 2:3]
    #Make sure unstack works with NULLs at the start of the value column
    df[1,:Value] = null
    df2 = unstack(df, :Fish, :Key, :Value)
    #This changes the expected result
    df4[2,:Mass] = null
    @test df2 == df4

    df = DataFrame(A = 1:10, B = 'A':'J')
    @test !(df[:,:] === df)

    @test append!(DataFrame(A = 1:2, B = 1:2), DataFrame(A = 3:4, B = 3:4)) == DataFrame(A=1:4, B = 1:4)
    df = DataFrame(A = Vector{Union{Int, Null}}(1:3), B = Vector{Union{Int, Null}}(4:6))
    DRT = CategoricalArrays.DefaultRefType
    @test all(c -> isa(c, Vector{Union{Int, Null}}), categorical!(deepcopy(df)).columns)
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
              categorical!(deepcopy(df), [1,2]).columns)
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
              categorical!(deepcopy(df), [:A,:B]).columns)
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
                    categorical!(deepcopy(df), [:A]).columns) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
                    categorical!(deepcopy(df), :A).columns) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
                    categorical!(deepcopy(df), [1]).columns) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Null}},
                    categorical!(deepcopy(df), 1).columns) == 1

    @testset "unstack nullable promotion" begin
        df = DataFrame(Any[repeat(1:2, inner=4), repeat('a':'d', outer=2), collect(1:8)],
                       [:id, :variable, :value])
        udf = unstack(df)
        @test udf == unstack(df, :variable, :value) == unstack(df, :id, :variable, :value)
        @test udf == DataFrame(Any[Union{Int, Null}[1, 2], Union{Int, Null}[1, 5],
                                   Union{Int, Null}[2, 6], Union{Int, Null}[3, 7],
                                   Union{Int, Null}[4, 8]], [:id, :a, :b, :c, :d])
        @test all(isa.(udf.columns, Vector{Union{Int, Null}}))
        df = DataFrame(Any[categorical(repeat(1:2, inner=4)),
                           categorical(repeat('a':'d', outer=2)), categorical(1:8)],
                       [:id, :variable, :value])
        udf = unstack(df)
        @test udf == unstack(df, :variable, :value) == unstack(df, :id, :variable, :value)
        @test udf == DataFrame(Any[Union{Int, Null}[1, 2], Union{Int, Null}[1, 5],
                                   Union{Int, Null}[2, 6], Union{Int, Null}[3, 7],
                                   Union{Int, Null}[4, 8]], [:id, :a, :b, :c, :d])
        @test all(isa.(udf.columns, CategoricalVector{Union{Int, Null}}))
    end

    @testset "duplicate entries in unstack warnings" begin
        df = DataFrame(id=Union{Int, Null}[1, 2, 1, 2], variable=["a", "b", "a", "b"], value=[3, 4, 5, 6])
        @static if VERSION >= v"0.6.0-dev.1980"
            @test_warn "Duplicate entries in unstack." unstack(df, :id, :variable, :value)
            @test_warn "Duplicate entries in unstack at row 3." unstack(df, :variable, :value)
        end
        a = unstack(df, :id, :variable, :value)
        b = unstack(df, :variable, :value)
        @test a == b == DataFrame(id = [1, 2], a = [5, null], b = [null, 6])

        df = DataFrame(id=1:2, variable=["a", "b"], value=3:4)
        @static if VERSION >= v"0.6.0-dev.1980"
            @test_nowarn unstack(df, :id, :variable, :value)
            @test_nowarn unstack(df, :variable, :value)
        end
        a = unstack(df, :id, :variable, :value)
        b = unstack(df, :variable, :value)
        @test a == b == DataFrame(id = [1, 2], a = [3, null], b = [null, 4])
    end

    @testset "rename" begin
        df = DataFrame(A = 1:3, B = 'A':'C')
        @test names(rename(df, :A, :A_1)) == [:A_1, :B]
        @test names(df) == [:A, :B]
        @test names(rename!(df, :A, :A_1)) == [:A_1, :B]
        @test names(df) == [:A_1, :B]
    end

    @testset "size" begin
        df = DataFrame(A = 1:3, B = 'A':'C')
        @test_throws ArgumentError size(df, 3)
        @test length(df) == 2
        @test ndims(df) == 2
    end

    @testset "description" begin
        df = DataFrame(A = 1:10)
        @test head(df) == DataFrame(A = 1:6)
        @test head(df, 1) == DataFrame(A = 1)
        @test tail(df) == DataFrame(A = 5:10)
        @test tail(df, 1) == DataFrame(A = 10)
    end

    @testset "misc" begin
        df = DataFrame(Any[collect('A':'C')])
        @test sprint(dump, df) == """
                                  DataFrames.DataFrame  3 observations of 1 variables
                                    x1: Array{Char}((3,))
                                      1: Char A
                                      2: Char B
                                      3: Char C
                                  """
        df = DataFrame(A = 1:12, B = repeat('A':'C', inner=4))
        # @test DataFrames.without(df, 1) == DataFrame(B = repeat('A':'C', inner=4))
    end

    @testset "column conversions" begin
        df = DataFrame(Any[collect(1:10), collect(1:10)])
        @test !isa(df[1], Vector{Union{Int, Null}})
        nullable!(df, 1)
        @test isa(df[1], Vector{Union{Int, Null}})
        @test !isa(df[2], Vector{Union{Int, Null}})
        nullable!(df, [1,2])
        @test isa(df[1], Vector{Union{Int, Null}}) && isa(df[2], Vector{Union{Int, Null}})
    end
end
