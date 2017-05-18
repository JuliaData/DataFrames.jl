module TestCat
    using Base.Test
    using DataTables

    #
    # hcat
    #

    nvint = NullableArray(Nullable{Int}[1, 2, Nullable(), 4])
    nvstr = NullableArray(Nullable{String}["one", "two", Nullable(), "four"])

    dt2 = DataTable(Any[nvint, nvstr])
    dt3 = DataTable(Any[nvint])
    dt4 = convert(DataTable, [1:4 1:4])
    dt5 = DataTable(Any[NullableArray([1,2,3,4]), nvstr])

    dth = hcat(dt3, dt4)
    @test size(dth, 2) == 3
    @test names(dth) == [:x1, :x1_1, :x2]
    @test isequal(dth[:x1], dt3[:x1])
    @test isequal(dth, [dt3 dt4])
    @test isequal(dth, DataTables.hcat!(DataTable(), dt3, dt4))

    dth3 = hcat(dt3, dt4, dt5)
    @test names(dth3) == [:x1, :x1_1, :x2, :x1_2, :x2_1]
    @test isequal(dth3, hcat(dth, dt5))
    @test isequal(dth3, DataTables.hcat!(DataTable(), dt3, dt4, dt5))

    @test isequal(dt2, DataTables.hcat!(dt2))

    @testset "hcat ::AbstractDataTable" begin
        dt = DataTable(A = repeat('A':'C', inner=4), B = 1:12)
        gd = groupby(dt, :A)
        answer = DataTable(A = fill('A', 4), B = 1:4, A_1 = 'B', B_1 = 5:8, A_2 = 'C', B_2 = 9:12)
        @test hcat(gd...) == answer
        answer = answer[1:4]
        @test hcat(gd[1], gd[2]) == answer
    end

    @testset "hcat ::Vectors" begin
        dt = DataTable()
        DataTables.hcat!(dt, NullableCategoricalVector(1:10))
        @test isequal(dt[1], NullableCategoricalVector(1:10))
        DataTables.hcat!(dt, NullableArray(1:10))
        @test isequal(dt[2], NullableArray(1:10))
    end

    #
    # vcat
    #

    null_dt = DataTable(Int, 0, 0)
    dt = DataTable(Int, 4, 3)

    # Assignment of rows
    dt[1, :] = dt[1, :]
    dt[1:2, :] = dt[1:2, :]
    dt[[true,false,false,true], :] = dt[2:3, :]

    # Scalar broadcasting assignment of rows
    dt[1, :] = 1
    dt[1:2, :] = 1
    dt[[true,false,false,true], :] = 3

    # Vector broadcasting assignment of rows
    dt[1:2, :] = [2,3]
    dt[[true,false,false,true], :] = [2,3]

    # Assignment of columns
    dt[1] = zeros(4)
    dt[:, 2] = ones(4)

    # Broadcasting assignment of columns
    dt[:, 1] = 1
    dt[1] = 3
    dt[:x3] = 2

    # assignment of subtables
    dt[1, 1:2] = dt[2, 2:3]
    dt[1:2, 1:2] = dt[2:3, 2:3]
    dt[[true,false,false,true], 2:3] = dt[1:2,1:2]

    # scalar broadcasting assignment of subtables
    dt[1, 1:2] = 3
    dt[1:2, 1:2] = 3
    dt[[true,false,false,true], 2:3] = 3

    # vector broadcasting assignment of subtables
    dt[1:2, 1:2] = [3,2]
    dt[[true,false,false,true], 2:3] = [2,3]

    @test vcat(null_dt) == DataTable()
    @test vcat(null_dt, null_dt) == DataTable()
    @test_throws ArgumentError vcat(null_dt, dt)
    @test_throws ArgumentError vcat(dt, null_dt)
    @test eltypes(vcat(dt, dt)) == Type[Float64, Float64, Int]
    @test size(vcat(dt, dt)) == (size(dt,1)*2, size(dt,2))
    @test eltypes(vcat(dt, dt, dt)) == Type[Float64,Float64,Int]
    @test size(vcat(dt, dt, dt)) == (size(dt,1)*3, size(dt,2))

    alt_dt = deepcopy(dt)
    vcat(dt, alt_dt)

    # Don't fail on non-matching types
    dt[1] = zeros(Int, nrow(dt))
    vcat(dt, alt_dt)

    dtr = vcat(dt4, dt4)
    @test size(dtr, 1) == 8
    @test names(dt4) == names(dtr)
    @test isequal(dtr, [dt4; dt4])

    @test eltypes(vcat(DataTable(a = [1]), DataTable(a = [2.1]))) == Type[Float64]
    @test eltypes(vcat(DataTable(a = NullableArray(Int, 1)), DataTable(a = [2.1]))) == Type[Nullable{Float64}]

    # Minimal container type promotion
    dta = DataTable(a = NullableCategoricalArray([1, 2, 2]))
    dtb = DataTable(a = NullableCategoricalArray([2, 3, 4]))
    dtc = DataTable(a = NullableArray([2, 3, 4]))
    dtd = DataTable(Any[2:4], [:a])
    dtab = vcat(dta, dtb)
    dtac = vcat(dta, dtc)
    @test isequal(dtab[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isequal(dtac[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isa(dtab[:a], NullableCategoricalVector{Int})
    @test isa(dtac[:a], NullableCategoricalVector{Int})
    # ^^ container may flip if container promotion happens in Base/DataArrays
    dc = vcat(dtd, dtc)
    @test isequal(vcat(dtc, dtd), dc)

    # Zero-row DataTables
    dtc0 = similar(dtc, 0)
    @test isequal(vcat(dtd, dtc0, dtc), dc)
    @test eltypes(vcat(dtd, dtc0)) == eltypes(dc)

    # vcat should be able to concatenate different implementations of AbstractDataTable (PR #944)
    @test isequal(vcat(view(DataTable(A=1:3),2),DataTable(A=4:5)), DataTable(A=[2,4,5]))

    @testset "vcat >2 args" begin
        @test vcat(DataTable(), DataTable(), DataTable()) == DataTable()
        dt = DataTable(x = trues(1), y = falses(1))
        @test vcat(dt, dt, dt) == DataTable(x = trues(3), y = falses(3))
    end

    @testset "vcat mixed coltypes" begin
        drf = CategoricalArrays.DefaultRefType
        dt = vcat(DataTable([[1]], [:x]), DataTable([[1.0]], [:x]))
        @test dt == DataTable([[1.0, 1.0]], [:x])
        @test typeof.(dt.columns) == [Vector{Float64}]
        dt = vcat(DataTable([[1]], [:x]), DataTable([["1"]], [:x]))
        @test dt == DataTable([[1, "1"]], [:x])
        @test typeof.(dt.columns) == [Vector{Any}]
        dt = vcat(DataTable([NullableArray([1])], [:x]), DataTable([[1]], [:x]))
        @test dt == DataTable([NullableArray([1, 1])], [:x])
        @test typeof.(dt.columns) == [NullableVector{Int}]
        dt = vcat(DataTable([CategoricalArray([1])], [:x]), DataTable([[1]], [:x]))
        @test dt == DataTable([CategoricalArray([1, 1])], [:x])
        @test typeof.(dt.columns) == [CategoricalVector{Int, drf}]
        dt = vcat(DataTable([CategoricalArray([1])], [:x]),
                  DataTable([NullableArray([1])], [:x]))
        @test dt == DataTable([NullableCategoricalArray([1, 1])], [:x])
        @test typeof.(dt.columns) == [NullableCategoricalVector{Int, drf}]
        dt = vcat(DataTable([CategoricalArray([1])], [:x]),
                  DataTable([NullableCategoricalArray([1])], [:x]))
        @test dt == DataTable([NullableCategoricalArray([1, 1])], [:x])
        @test typeof.(dt.columns) == [NullableCategoricalVector{Int, drf}]
        dt = vcat(DataTable([NullableArray([1])], [:x]),
                  DataTable([NullableArray(["1"])], [:x]))
        @test dt == DataTable([NullableArray([1, "1"])], [:x])
        @test typeof.(dt.columns) == [NullableVector{Any}]
        dt = vcat(DataTable([CategoricalArray([1])], [:x]),
                  DataTable([CategoricalArray(["1"])], [:x]))
        @test dt == DataTable([CategoricalArray([1, "1"])], [:x])
        @test typeof.(dt.columns) == [CategoricalVector{Any, drf}]
        dt = vcat(DataTable([trues(1)], [:x]), DataTable([[false]], [:x]))
        @test dt == DataTable([[true, false]], [:x])
        @test typeof.(dt.columns) == [Vector{Bool}]
    end

    @testset "vcat errors" begin
        err = @test_throws ArgumentError vcat(DataTable(), DataTable(), DataTable(x=[]))
        @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
        err = @test_throws ArgumentError vcat(DataTable(), DataTable(), DataTable(x=[1]))
        @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
        dt1 = DataTable(A = 1:3, B = 1:3)
        dt2 = DataTable(A = 1:3)
        # right missing 1 column
        err = @test_throws ArgumentError vcat(dt1, dt2)
        @test err.value.msg == "column(s) B are missing from argument(s) 2"
        # left missing 1 column
        err = @test_throws ArgumentError vcat(dt2, dt1)
        @test err.value.msg == "column(s) B are missing from argument(s) 1"
        # multiple missing 1 column
        err = @test_throws ArgumentError vcat(dt1, dt2, dt2, dt2, dt2, dt2)
        @test err.value.msg == "column(s) B are missing from argument(s) 2, 3, 4, 5 and 6"
        # argument missing >1 columns
        dt1 = DataTable(A = 1:3, B = 1:3, C = 1:3, D = 1:3, E = 1:3)
        err = @test_throws ArgumentError vcat(dt1, dt2)
        @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2"
        # >1 arguments missing >1 columns
        err = @test_throws ArgumentError vcat(dt1, dt2, dt2, dt2, dt2)
        @test err.value.msg == "column(s) B, C, D and E are missing from argument(s) 2, 3, 4 and 5"
        # out of order
        dt2 = dt1[reverse(names(dt1))]
        err = @test_throws ArgumentError vcat(dt1, dt2)
        @test err.value.msg == "column order of argument(s) 1 != column order of argument(s) 2"
        # first group >1 arguments
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2)
        @test err.value.msg == "column order of argument(s) 1 and 2 != column order of argument(s) 3"
        # second group >1 arguments
        err = @test_throws ArgumentError vcat(dt1, dt2, dt2)
        @test err.value.msg == "column order of argument(s) 1 != column order of argument(s) 2 and 3"
        # first and second groups >1 argument
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2)
        @test err.value.msg == "column order of argument(s) 1, 2 and 3 != column order of argument(s) 4, 5 and 6"
        # >2 groups out of order
        srand(1)
        dt3 = dt1[shuffle(names(dt1))]
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2, dt3, dt3, dt3, dt3)
        @test err.value.msg == "column order of argument(s) 1, 2 and 3 != column order of argument(s) 4, 5 and 6 != column order of argument(s) 7, 8, 9 and 10"
        # missing columns throws error before out of order columns
        dt1 = DataTable(A = 1, B = 1)
        dt2 = DataTable(A = 1)
        dt3 = DataTable(B = 1, A = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3)
        @test err.value.msg == "column(s) B are missing from argument(s) 2"
        # unique columns for both sides
        dt1 = DataTable(A = 1, B = 1, C = 1, D = 1)
        dt2 = DataTable(A = 1, C = 1, D = 1, E = 1, F = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, and column(s) B are missing from argument(s) 2"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, and column(s) B are missing from argument(s) 3 and 4"
        dt3 = DataTable(A = 1, B = 1, C = 1, D = 1, E = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2, dt3, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2, dt3, dt3, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
        # dt4 is a superset of names found in all other datatables and won't be shown in error
        dt4 = DataTable(A = 1, B = 1, C = 1, D = 1, E = 1, F = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2, dt3, dt3, dt4, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2, dt3, dt3, dt3, dt4, dt4, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3, dt4, dt1, dt2, dt3, dt4, dt1, dt2, dt3, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 5 and 9, column(s) B are missing from argument(s) 2, 6 and 10, and column(s) F are missing from argument(s) 3, 7 and 11"
    end
    x = view(DataTable(A = NullableArray(1:3)), 2)
    y = DataTable(A = NullableArray(4:5))
    @test isequal(vcat(x, y), DataTable(A = NullableArray([2, 4, 5])))
end
