module TestCat
    using Base.Test, DataFrames

    #
    # hcat
    #

    nvint = [1, 2, null, 4]
    nvstr = ["one", "two", null, "four"]

    dt2 = DataFrame(Any[nvint, nvstr])
    dt3 = DataFrame(Any[nvint])
    dt4 = convert(DataFrame, [1:4 1:4])
    dt5 = DataFrame(Any[Union{Int, Null}[1,2,3,4], nvstr])

    dth = hcat(dt3, dt4)
    @test size(dth, 2) == 3
    @test names(dth) == [:x1, :x1_1, :x2]
    @test dth[:x1] == dt3[:x1]
    @test dth == [dt3 dt4]
    @test dth == DataFrames.hcat!(DataFrame(), dt3, dt4)

    dth3 = hcat(dt3, dt4, dt5)
    @test names(dth3) == [:x1, :x1_1, :x2, :x1_2, :x2_1]
    @test dth3 == hcat(dth, dt5)
    @test dth3 == DataFrames.hcat!(DataFrame(), dt3, dt4, dt5)

    @test dt2 == DataFrames.hcat!(dt2)

    @testset "hcat ::AbstractDataFrame" begin
        dt = DataFrame(A = repeat('A':'C', inner=4), B = 1:12)
        gd = groupby(dt, :A)
        answer = DataFrame(A = fill('A', 4), B = 1:4, A_1 = 'B', B_1 = 5:8, A_2 = 'C', B_2 = 9:12)
        @test hcat(gd...) == answer
        answer = answer[1:4]
        @test hcat(gd[1], gd[2]) == answer
    end

    @testset "hcat ::Vectors" begin
        dt = DataFrame()
        DataFrames.hcat!(dt, CategoricalVector{Union{Int, Null}}(1:10))
        @test dt[1] == collect(1:10)
        DataFrames.hcat!(dt, 1:10)
        @test dt[2] == collect(1:10)
    end

    @testset "hcat ::AbstractDataFrame" begin
        df = DataFrame(A = repeat('A':'C', inner=4), B = 1:12)
        gd = groupby(df, :A)
        answer = DataFrame(A = fill('A', 4), B = 1:4, A_1 = 'B', B_1 = 5:8, A_2 = 'C', B_2 = 9:12)
        @test hcat(gd...) == answer
        answer = answer[1:4]
        @test hcat(gd[1], gd[2]) == answer
    end

    @testset "hcat ::Vectors" begin
        df = DataFrame()
        DataFrames.hcat!(df, NullableCategoricalVector(1:10))
        @test isequal(df[1], NullableCategoricalVector(1:10))
        DataFrames.hcat!(df, NullableArray(1:10))
        @test isequal(df[2], NullableArray(1:10))
    end

    #
    # vcat
    #

    null_dt = DataFrame(Int, 0, 0)
    dt = DataFrame(Int, 4, 3)

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

    @test vcat(null_dt) == DataFrame()
    @test vcat(null_dt, null_dt) == DataFrame()
    @test_throws ArgumentError vcat(null_dt, dt)
    @test_throws ArgumentError vcat(dt, null_dt)
    @test eltypes(vcat(dt, dt)) == Type[Float64, Float64, Int]
    @test size(vcat(dt, dt)) == (size(dt, 1) * 2, size(dt, 2))
    @test eltypes(vcat(dt, dt, dt)) == Type[Float64, Float64, Int]
    @test size(vcat(dt, dt, dt)) == (size(dt, 1) * 3, size(dt, 2))

    alt_dt = deepcopy(dt)
    vcat(dt, alt_dt)

    # Don't fail on non-matching types
    dt[1] = zeros(Int, nrow(dt))
    vcat(dt, alt_dt)

    dtr = vcat(dt4, dt4)
    @test size(dtr, 1) == 8
    @test names(dt4) == names(dtr)
    @test dtr == [dt4; dt4]

    @test eltypes(vcat(DataFrame(a = [1]), DataFrame(a = [2.1]))) == Type[Float64]
    @test eltypes(vcat(DataFrame(a = nulls(Int, 1)), DataFrame(a = Union{Float64, Null}[2.1]))) == Type[Union{Float64, Null}]

    # Minimal container type promotion
    dta = DataFrame(a = CategoricalArray{Union{Int, Null}}([1, 2, 2]))
    dtb = DataFrame(a = CategoricalArray{Union{Int, Null}}([2, 3, 4]))
    dtc = DataFrame(a = Union{Int, Null}[2, 3, 4])
    dtd = DataFrame(Any[2:4], [:a])
    dtab = vcat(dta, dtb)
    dtac = vcat(dta, dtc)
    @test dtab[:a] == [1, 2, 2, 2, 3, 4]
    @test dtac[:a] == [1, 2, 2, 2, 3, 4]
    @test isa(dtab[:a], CategoricalVector{Union{Int, Null}})
    @test isa(dtac[:a], CategoricalVector{Union{Int, Null}})
    # ^^ container may flip if container promotion happens in Base/DataArrays
    dc = vcat(dtd, dtc)
    @test vcat(dtc, dtd) == dc

    # Zero-row DataFrames
    dtc0 = similar(dtc, 0)
    @test vcat(dtd, dtc0, dtc) == dc
    @test eltypes(vcat(dtd, dtc0)) == eltypes(dc)

    # vcat should be able to concatenate different implementations of AbstractDataFrame (PR #944)
    @test vcat(view(DataFrame(A=1:3),2),DataFrame(A=4:5)) == DataFrame(A=[2,4,5])

    @testset "vcat >2 args" begin
        @test vcat(DataFrame(), DataFrame(), DataFrame()) == DataFrame()
        dt = DataFrame(x = trues(1), y = falses(1))
        @test vcat(dt, dt, dt) == DataFrame(x = trues(3), y = falses(3))
    end

    @testset "vcat mixed coltypes" begin
        dt = vcat(DataFrame([[1]], [:x]), DataFrame([[1.0]], [:x]))
        @test dt == DataFrame([[1.0, 1.0]], [:x])
        @test typeof.(dt.columns) == [Vector{Float64}]
        dt = vcat(DataFrame([[1]], [:x]), DataFrame([["1"]], [:x]))
        @test dt == DataFrame([[1, "1"]], [:x])
        @test typeof.(dt.columns) == [Vector{Any}]
        dt = vcat(DataFrame([Union{Null, Int}[1]], [:x]), DataFrame([[1]], [:x]))
        @test dt == DataFrame([[1, 1]], [:x])
        @test typeof.(dt.columns) == [Vector{Union{Null, Int}}]
        dt = vcat(DataFrame([CategoricalArray([1])], [:x]), DataFrame([[1]], [:x]))
        @test dt == DataFrame([[1, 1]], [:x])
        @test typeof(dt[:x]) <: CategoricalVector{Int}
        dt = vcat(DataFrame([CategoricalArray([1])], [:x]),
                  DataFrame([Union{Null, Int}[1]], [:x]))
        @test dt == DataFrame([[1, 1]], [:x])
        @test typeof(dt[:x]) <: CategoricalVector{Union{Int, Null}}
        dt = vcat(DataFrame([CategoricalArray([1])], [:x]),
                  DataFrame([CategoricalArray{Union{Int, Null}}([1])], [:x]))
        @test dt == DataFrame([[1, 1]], [:x])
        @test typeof(dt[:x]) <: CategoricalVector{Union{Int, Null}}
        dt = vcat(DataFrame([Union{Int, Null}[1]], [:x]),
                  DataFrame([["1"]], [:x]))
        @test dt == DataFrame([[1, "1"]], [:x])
        @test typeof.(dt.columns) == [Vector{Any}]
        dt = vcat(DataFrame([CategoricalArray([1])], [:x]),
                  DataFrame([CategoricalArray(["1"])], [:x]))
        @test dt == DataFrame([[1, "1"]], [:x])
        @test typeof(dt[:x]) <: CategoricalVector{Any}
        dt = vcat(DataFrame([trues(1)], [:x]), DataFrame([[false]], [:x]))
        @test dt == DataFrame([[true, false]], [:x])
        @test typeof.(dt.columns) == [Vector{Bool}]
    end

    @testset "vcat errors" begin
        err = @test_throws ArgumentError vcat(DataFrame(), DataFrame(), DataFrame(x=[]))
        @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
        err = @test_throws ArgumentError vcat(DataFrame(), DataFrame(), DataFrame(x=[1]))
        @test err.value.msg == "column(s) x are missing from argument(s) 1 and 2"
        dt1 = DataFrame(A = 1:3, B = 1:3)
        dt2 = DataFrame(A = 1:3)
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
        dt1 = DataFrame(A = 1:3, B = 1:3, C = 1:3, D = 1:3, E = 1:3)
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
        dt1 = DataFrame(A = 1, B = 1)
        dt2 = DataFrame(A = 1)
        dt3 = DataFrame(B = 1, A = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3)
        @test err.value.msg == "column(s) B are missing from argument(s) 2"
        # unique columns for both sides
        dt1 = DataFrame(A = 1, B = 1, C = 1, D = 1)
        dt2 = DataFrame(A = 1, C = 1, D = 1, E = 1, F = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, and column(s) B are missing from argument(s) 2"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, and column(s) B are missing from argument(s) 3 and 4"
        dt3 = DataFrame(A = 1, B = 1, C = 1, D = 1, E = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2, dt3, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2, dt3, dt3, dt3)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
        # dt4 is a superset of names found in all other DataFrames and won't be shown in error
        dt4 = DataFrame(A = 1, B = 1, C = 1, D = 1, E = 1, F = 1)
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, column(s) B are missing from argument(s) 2, and column(s) F are missing from argument(s) 3"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt2, dt2, dt3, dt3, dt4, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1 and 2, column(s) B are missing from argument(s) 3 and 4, and column(s) F are missing from argument(s) 5 and 6"
        err = @test_throws ArgumentError vcat(dt1, dt1, dt1, dt2, dt2, dt2, dt3, dt3, dt3, dt4, dt4, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 2 and 3, column(s) B are missing from argument(s) 4, 5 and 6, and column(s) F are missing from argument(s) 7, 8 and 9"
        err = @test_throws ArgumentError vcat(dt1, dt2, dt3, dt4, dt1, dt2, dt3, dt4, dt1, dt2, dt3, dt4)
        @test err.value.msg == "column(s) E and F are missing from argument(s) 1, 5 and 9, column(s) B are missing from argument(s) 2, 6 and 10, and column(s) F are missing from argument(s) 3, 7 and 11"
    end
    x = view(DataFrame(A = Vector{Union{Null, Int}}(1:3)), 2)
    y = DataFrame(A = 4:5)
    @test vcat(x, y) == DataFrame(A = [2, 4, 5])
end
