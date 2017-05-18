module TestDataTable
    using Base.Test
    using DataTables, Compat
    import Compat.String

    #
    # Equality
    #

    @test isequal(DataTable(a=[1, 2, 3], b=[4, 5, 6]), DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    @test !isequal(DataTable(a=[1, 2], b=[4, 5]), DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    @test !isequal(DataTable(a=[1, 2, 3], b=[4, 5, 6]), DataTable(a=[1, 2, 3]))
    @test !isequal(DataTable(a=[1, 2, 3], b=[4, 5, 6]), DataTable(a=[1, 2, 3], c=[4, 5, 6]))
    @test !isequal(DataTable(a=[1, 2, 3], b=[4, 5, 6]), DataTable(b=[4, 5, 6], a=[1, 2, 3]))
    @test !isequal(DataTable(a=[1, 2, 2], b=[4, 5, 6]), DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    @test isequal(DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]),
                  DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]))

    # FIXME: equality operators won't work until JuliaStats/NullableArrays#84 is merged
    #@test get(DataTable(a=[1, 2, 3], b=[4, 5, 6]) == DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    #@test get(DataTable(a=[1, 2], b=[4, 5]) != DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    #@test get(DataTable(a=[1, 2, 3], b=[4, 5, 6]) != DataTable(a=[1, 2, 3]))
    #@test get(DataTable(a=[1, 2, 3], b=[4, 5, 6]) != DataTable(a=[1, 2, 3], c=[4, 5, 6]))
    #@test get(DataTable(a=[1, 2, 3], b=[4, 5, 6]) != DataTable(b=[4, 5, 6], a=[1, 2, 3]))
    #@test get(DataTable(a=[1, 2, 2], b=[4, 5, 6]) != DataTable(a=[1, 2, 3], b=[4, 5, 6]))
    #@test get(DataTable(a=Nullable{Int}[1, 3, Nullable()], b=[4, 5, 6]) !=
    #          DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]))
    #@test isnull(DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]) ==
    #             DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]))
    #@test isnull(DataTable(a=Nullable{Int}[1, 2, Nullable()], b=[4, 5, 6]) ==
    #             DataTable(a=Nullable{Int}[1, 2, 3], b=[4, 5, 6]))

    #
    # Copying
    #

    dt = DataTable(a = NullableArray([2, 3]),
                   b = NullableArray([DataTable(c = 1), DataTable(d = 2)]))
    dtc = copy(dt)
    dtdc = deepcopy(dt)

    dt[1, :a] = 4
    get(dt[1, :b])[:e] = 5
    names!(dt, [:f, :g])

    @test names(dtc) == [:a, :b]
    @test names(dtdc) == [:a, :b]

    @test get(dtc[1, :a]) === 4
    @test get(dtdc[1, :a]) === 2

    @test names(get(dtc[1, :b])) == [:c, :e]
    @test names(get(dtdc[1, :b])) == [:c]

    #

    x = DataTable(a = [1, 2, 3], b = [4, 5, 6])
    v = DataTable(a = [5, 6, 7], b = [8, 9, 10])

    z = vcat(v, x)

    z2 = z[:, [1, 1, 2]]
    @test names(z2) == [:a, :a_1, :b]

    #test_group("DataTable assignment")
    # Insert single column
    x0 = x[Int[], :]
    @test_throws ErrorException x0[:d] = [1]
    @test_throws ErrorException x0[:d] = 1:3

    # Insert single value
    x[:d] = Nullable(3)
    @test isequal(x[:d], NullableArray([3, 3, 3]))

    x0[:d] = 3
    @test x0[:d] == Int[]

    # similar / nulls
    dt = DataTable(a = NullableArray([1]),
                   b = NullableArray(["b"]),
                   c = NullableCategoricalArray([3.3]))
    nulldt = DataTable(a = NullableArray{Int}(2),
                       b = NullableArray{String}(2),
                       c = NullableCategoricalArray{Float64}(2))
    @test isequal(nulldt, similar(dt, 2))

    # Associative methods

    dt = DataTable(a=[1, 2], b=[3., 4.])
    @test haskey(dt, :a)
    @test !haskey(dt, :c)
    @test get(dt, :a, -1) === dt.columns[1]
    @test get(dt, :c, -1) == -1
    @test !isempty(dt)

    @test empty!(dt) === dt
    @test isempty(dt.columns)
    @test isempty(dt)

    dt = DataTable(a=NullableArray([1, 2]), b=NullableArray([3., 4.]))
    @test_throws BoundsError insert!(dt, 5, ["a", "b"], :newcol)
    @test_throws ErrorException insert!(dt, 1, ["a"], :newcol)
    @test isequal(insert!(dt, 1, ["a", "b"], :newcol), dt)
    @test names(dt) == [:newcol, :a, :b]
    @test isequal(dt[:a], NullableArray([1, 2]))
    @test isequal(dt[:b], NullableArray([3., 4.]))
    @test isequal(dt[:newcol], ["a", "b"])

    dt = DataTable(a=[1, 2], b=[3., 4.])
    dt2 = DataTable(b=["a", "b"], c=[:c, :d])
    @test isequal(merge!(dt, dt2), dt)
    @test isequal(dt, DataTable(a=[1, 2], b=["a", "b"], c=[:c, :d]))

    #test_group("Empty DataTable constructors")
    dt = DataTable(Nullable{Int}, 10, 3)
    @test size(dt, 1) == 10
    @test size(dt, 2) == 3
    @test typeof(dt[:, 1]) == NullableVector{Int}
    @test typeof(dt[:, 2]) == NullableVector{Int}
    @test typeof(dt[:, 3]) == NullableVector{Int}
    @test all(isnull, dt[:, 1])
    @test all(isnull, dt[:, 2])
    @test all(isnull, dt[:, 3])

    dt = DataTable([Nullable{Int}, Nullable{Float64}, Nullable{String}], 100)
    @test size(dt, 1) == 100
    @test size(dt, 2) == 3
    @test typeof(dt[:, 1]) == NullableVector{Int}
    @test typeof(dt[:, 2]) == NullableVector{Float64}
    @test typeof(dt[:, 3]) == NullableVector{String}
    @test all(isnull, dt[:, 1])
    @test all(isnull, dt[:, 2])
    @test all(isnull, dt[:, 3])

    dt = DataTable([Nullable{Int}, Nullable{Float64}, Nullable{String}],
                   [:A, :B, :C], 100)
    @test size(dt, 1) == 100
    @test size(dt, 2) == 3
    @test typeof(dt[:, 1]) == NullableVector{Int}
    @test typeof(dt[:, 2]) == NullableVector{Float64}
    @test typeof(dt[:, 3]) == NullableVector{String}
    @test all(isnull, dt[:, 1])
    @test all(isnull, dt[:, 2])
    @test all(isnull, dt[:, 3])


    dt = DataTable([Nullable{Int}, Nullable{Float64}, Nullable{String}],
                   [:A, :B, :C], [false, false, true],100)
    @test size(dt, 1) == 100
    @test size(dt, 2) == 3
    @test typeof(dt[:, 1]) == NullableVector{Int}
    @test typeof(dt[:, 2]) == NullableVector{Float64}
    @test typeof(dt[:, 3]) == NullableCategoricalVector{Compat.UTF8String,UInt32}
    @test all(isnull, dt[:, 1])
    @test all(isnull, dt[:, 2])
    @test all(isnull, dt[:, 3])

    dt = convert(DataTable, zeros(10, 5))
    @test size(dt, 1) == 10
    @test size(dt, 2) == 5
    @test typeof(dt[:, 1]) == Vector{Float64}

    dt = convert(DataTable, ones(10, 5))
    @test size(dt, 1) == 10
    @test size(dt, 2) == 5
    @test typeof(dt[:, 1]) == Vector{Float64}

    dt = convert(DataTable, eye(10, 5))
    @test size(dt, 1) == 10
    @test size(dt, 2) == 5
    @test typeof(dt[:, 1]) == Vector{Float64}

    @test DataTable(NullableArray[[1,2,3],[2.5,4.5,6.5]], [:A, :B]) ==
        DataTable(A = NullableArray([1,2,3]), B = NullableArray([2.5,4.5,6.5]))

    # This assignment was missing before
    dt = DataTable(Column = [:A])
    dt[1, :Column] = "Testing"

    # zero-row datatable and subdatatable test
    dt = DataTable(x=[], y=[])
    @test nrow(dt) == 0
    dt = DataTable(x=[1:3;], y=[3:5;])
    sdt = view(dt, dt[:x] .== 4)
    @test size(sdt, 1) == 0

    @test hash(convert(DataTable, [1 2; 3 4])) == hash(convert(DataTable, [1 2; 3 4]))
    @test hash(convert(DataTable, [1 2; 3 4])) != hash(convert(DataTable, [1 3; 2 4]))


    # push!(dt, row)
    dt=DataTable( first=[1,2,3], second=["apple","orange","pear"] )

    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    push!(dtb, Any[3,"pear"])
    @test isequal(dt, dtb)

    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    push!(dtb, (3,"pear"))
    @test isequal(dt, dtb)

    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dtb, (33.33,"pear"))

    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dtb, ("coconut",22))

    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    push!(dtb, @compat(Dict(:first=>3, :second=>"pear")))
    @test isequal(dt, dtb)

    dt=DataTable( first=[1,2,3], second=["apple","orange","banana"] )
    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    push!(dtb, @compat(Dict("first"=>3, "second"=>"banana")))
    @test isequal(dt, dtb)

    dt0= DataTable( first=[1,2], second=["apple","orange"] )
    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dtb, @compat(Dict(:first=>true, :second=>false)))
    @test isequal(dt0, dtb)

    dt0= DataTable( first=[1,2], second=["apple","orange"] )
    dtb= DataTable( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dtb, @compat(Dict("first"=>"chicken", "second"=>"stuff")))
    @test isequal(dt0, dtb)

    # delete!
    dt = DataTable(a=1, b=2, c=3, d=4, e=5)
    @test_throws ArgumentError delete!(dt, 0)
    @test_throws ArgumentError delete!(dt, 6)
    @test_throws KeyError delete!(dt, :f)

    d = copy(dt)
    delete!(d, [:a, :e, :c])
    @test names(d) == [:b, :d]
    delete!(d, :b)
    @test isequal(d, dt[[:d]])

    d = copy(dt)
    delete!(d, [2, 5, 3])
    @test names(d) == [:a, :d]
    delete!(d, 2)
    @test isequal(d, dt[[:a]])

    # deleterows!
    dt = DataTable(a=[1, 2], b=[3., 4.])
    @test deleterows!(dt, 1) === dt
    @test isequal(dt, DataTable(a=[2], b=[4.]))

    dt = DataTable(a=[1, 2], b=[3., 4.])
    @test deleterows!(dt, 2) === dt
    @test isequal(dt, DataTable(a=[1], b=[3.]))

    dt = DataTable(a=[1, 2, 3], b=[3., 4., 5.])
    @test deleterows!(dt, 2:3) === dt
    @test isequal(dt, DataTable(a=[1], b=[3.]))

    dt = DataTable(a=[1, 2, 3], b=[3., 4., 5.])
    @test deleterows!(dt, [2, 3]) === dt
    @test isequal(dt, DataTable(a=[1], b=[3.]))

    dt = DataTable(a=NullableArray([1, 2]), b=NullableArray([3., 4.]))
    @test deleterows!(dt, 1) === dt
    @test isequal(dt, DataTable(a=NullableArray([2]), b=NullableArray([4.])))

    dt = DataTable(a=NullableArray([1, 2]), b=NullableArray([3., 4.]))
    @test deleterows!(dt, 2) === dt
    @test isequal(dt, DataTable(a=NullableArray([1]), b=NullableArray([3.])))

    dt = DataTable(a=NullableArray([1, 2, 3]), b=NullableArray([3., 4., 5.]))
    @test deleterows!(dt, 2:3) === dt
    @test isequal(dt, DataTable(a=NullableArray([1]), b=NullableArray([3.])))

    dt = DataTable(a=NullableArray([1, 2, 3]), b=NullableArray([3., 4., 5.]))
    @test deleterows!(dt, [2, 3]) === dt
    @test isequal(dt, DataTable(a=NullableArray([1]), b=NullableArray([3.])))

    # describe
    #suppress output and test that describe() does not throw
    devnull = is_unix() ? "/dev/null" : "nul"
    open(devnull, "w") do f
        @test nothing == describe(f, DataTable(a=[1, 2], b=Any["3", Nullable()]))
        @test nothing ==
              describe(f, DataTable(a=NullableArray([1, 2]),
                                    b=NullableArray(Nullable{String}["3", Nullable()])))
        @test nothing ==
              describe(f, DataTable(a=CategoricalArray([1, 2]),
                                    b=NullableCategoricalArray(Nullable{String}["3", Nullable()])))
        @test nothing == describe(f, [1, 2, 3])
        @test nothing == describe(f, NullableArray([1, 2, 3]))
        @test nothing == describe(f, CategoricalArray([1, 2, 3]))
        @test nothing == describe(f, Any["1", "2", Nullable()])
        @test nothing == describe(f, NullableArray(Nullable{String}["1", "2", Nullable()]))
        @test nothing == describe(f, NullableCategoricalArray(Nullable{String}["1", "2", Nullable()]))
    end

    #Check the output of unstack
    dt = DataTable(Fish = NullableCategoricalArray(["Bob", "Bob", "Batman", "Batman"]),
                   Key = Nullable{String}["Mass", "Color", "Mass", "Color"],
                   Value = Nullable{String}["12 g", "Red", "18 g", "Grey"])
    # Check that reordering levels does not confuse unstack
    levels!(dt[1], ["XXX", "Bob", "Batman"])
    #Unstack specifying a row column
    dt2 = unstack(dt,:Fish, :Key, :Value)
    #Unstack without specifying a row column
    dt3 = unstack(dt,:Key, :Value)
    #The expected output
    dt4 = DataTable(Fish = Nullable{String}["XXX", "Bob", "Batman"],
                    Color = Nullable{String}[Nullable(), "Red", "Grey"],
                    Mass = Nullable{String}[Nullable(), "12 g", "18 g"])
    @test isequal(dt2, dt4)
    @test typeof(dt2[:Fish]) <: NullableCategoricalArray{String,1,UInt32}
    # first column stays as CategoricalArray in dt3
    @test isequal(dt3[:, 2:3], dt4[2:3, 2:3])
    #Make sure unstack works with NULLs at the start of the value column
    dt[1,:Value] = Nullable()
    dt2 = unstack(dt,:Fish, :Key, :Value)
    #This changes the expected result
    dt4[2,:Mass] = Nullable()
    @test isequal(dt2, dt4)

    dt = DataTable(A = 1:10, B = 'A':'J')
    @test !(dt[:,:] === dt)

    @test append!(DataTable(A = 1:2, B = 1:2), DataTable(A = 3:4, B = 3:4)) == DataTable(A=1:4, B = 1:4)
    dt = DataTable(A = NullableArray(1:3), B = NullableArray(4:6))
    @test all(c -> isa(c, NullableArray), categorical!(deepcopy(dt)).columns)
    @test all(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), [1,2]).columns)
    @test all(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), [:A,:B]).columns)
    @test find(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), [:A]).columns) == [1]
    @test find(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), :A).columns) == [1]
    @test find(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), [1]).columns) == [1]
    @test find(c -> isa(c, NullableCategoricalArray), categorical!(deepcopy(dt), 1).columns) == [1]

    @testset "unstack nullable promotion" begin
        dt = DataTable(Any[repeat(1:2, inner=4), repeat('a':'d', outer=2), collect(1:8)],
                       [:id, :variable, :value])
        udt = unstack(dt)
        @test udt == unstack(dt, :variable, :value) == unstack(dt, :id, :variable, :value)
        @test udt == DataTable(Any[Nullable[1, 2], Nullable[1, 5], Nullable[2, 6],
                                   Nullable[3, 7], Nullable[4, 8]], [:id, :a, :b, :c, :d])
        @test all(typeof.(udt.columns) .== NullableVector{Int})
        dt = DataTable(Any[categorical(repeat(1:2, inner=4)),
                           categorical(repeat('a':'d', outer=2)), categorical(1:8)],
                       [:id, :variable, :value])
        udt = unstack(dt)
        @test udt == unstack(dt, :variable, :value) == unstack(dt, :id, :variable, :value)
        @test udt == DataTable(Any[Nullable[1, 2], Nullable[1, 5], Nullable[2, 6],
                                   Nullable[3, 7], Nullable[4, 8]], [:id, :a, :b, :c, :d])
        @test all(typeof.(udt.columns) .== NullableCategoricalVector{Int, UInt32})
    end

    @testset "duplicate entries in unstack warnings" begin
        dt = DataTable(id=NullableArray([1, 2, 1, 2]), variable=["a", "b", "a", "b"], value=[3, 4, 5, 6])
        @static if VERSION >= v"0.6.0-dev.1980"
            @test_warn "Duplicate entries in unstack." unstack(dt, :id, :variable, :value)
            @test_warn "Duplicate entries in unstack at row 3." unstack(dt, :variable, :value)
        end
        a = unstack(dt, :id, :variable, :value)
        b = unstack(dt, :variable, :value)
        @test a == b == DataTable(id = Nullable[1, 2], a = [5, Nullable()], b = [Nullable(), 6])

        dt = DataTable(id=NullableArray(1:2), variable=["a", "b"], value=3:4)
        @static if VERSION >= v"0.6.0-dev.1980"
            @test_nowarn unstack(dt, :id, :variable, :value)
            @test_nowarn unstack(dt, :variable, :value)
        end
        a = unstack(dt, :id, :variable, :value)
        b = unstack(dt, :variable, :value)
        @test a == b == DataTable(id = Nullable[1, 2], a = [3, Nullable()], b = [Nullable(), 4])
    end

    @testset "rename" begin
        dt = DataTable(A = 1:3, B = 'A':'C')
        @test names(rename(dt, :A, :A_1)) == [:A_1, :B]
        @test names(dt) == [:A, :B]
        @test names(rename!(dt, :A, :A_1)) == [:A_1, :B]
        @test names(dt) == [:A_1, :B]
    end

    @testset "size" begin
        dt = DataTable(A = 1:3, B = 'A':'C')
        @test_throws ArgumentError size(dt, 3)
        @test length(dt) == 2
        @test ndims(dt) == 2
    end

    @testset "description" begin
        dt = DataTable(A = 1:10)
        @test head(dt) == DataTable(A = 1:6)
        @test head(dt, 1) == DataTable(A = 1)
        @test tail(dt) == DataTable(A = 5:10)
        @test tail(dt, 1) == DataTable(A = 10)
    end

    @testset "misc" begin
        dt = DataTable(Any[collect('A':'C')])
        @test sprint(dump, dt) == """
                                  DataTables.DataTable  3 observations of 1 variables
                                    x1: Array{Char}((3,))
                                      1: Char A
                                      2: Char B
                                      3: Char C
                                  """
        dt = DataTable(A = 1:12, B = repeat('A':'C', inner=4))
        @test DataTables.without(dt, 1) == DataTable(B = repeat('A':'C', inner=4))
    end

    @testset "column conversions" begin
        dt = DataTable(Any[collect(1:10), collect(1:10)])
        @test !isa(dt[1], NullableArray)
        nullable!(dt, 1)
        @test isa(dt[1], NullableArray)
        @test !isa(dt[2], NullableArray)
        nullable!(dt, [1,2])
        @test isa(dt[1], NullableArray) && isa(dt[2], NullableArray)
    end
end
