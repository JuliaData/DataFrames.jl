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

    # assignment of subframes
    dt[1, 1:2] = dt[2, 2:3]
    dt[1:2, 1:2] = dt[2:3, 2:3]
    dt[[true,false,false,true], 2:3] = dt[1:2,1:2]

    # scalar broadcasting assignment of subframes
    dt[1, 1:2] = 3
    dt[1:2, 1:2] = 3
    dt[[true,false,false,true], 2:3] = 3

    # vector broadcasting assignment of subframes
    dt[1:2, 1:2] = [3,2]
    dt[[true,false,false,true], 2:3] = [2,3]

    vcat([])
    vcat(null_dt)
    vcat(null_dt, null_dt)
    vcat(null_dt, dt)
    vcat(dt, null_dt)
    vcat(dt, dt)
    vcat(dt, dt, dt)
    @test vcat(DataTable[]) == DataTable()

    alt_dt = deepcopy(dt)
    vcat(dt, alt_dt)

    # Don't fail on non-matching types
    dt[1] = zeros(Int, nrow(dt))
    vcat(dt, alt_dt)

    # Don't fail on non-matching names
    names!(alt_dt, [:A, :B, :C])
    vcat(dt, alt_dt)

    dtr = vcat(dt4, dt4)
    @test size(dtr, 1) == 8
    @test names(dt4) == names(dtr)
    @test isequal(dtr, [dt4; dt4])

    dtr = vcat(dt2, dt3)
    @test size(dtr) == (8,2)
    @test names(dt2) == names(dtr)
    @test isnull(dtr[8,:x2])

    # Eltype promotion
    # Fails on Julia 0.4 since promote_type(Nullable{Int}, Nullable{Float64}) gives Nullable{T}
    if VERSION >= v"0.5.0-dev"
        @test eltypes(vcat(DataTable(a = [1]), DataTable(a = [2.1]))) == [Nullable{Float64}]
        @test eltypes(vcat(DataTable(a = NullableArray(Int, 1)), DataTable(a = [2.1]))) == [Nullable{Float64}]
    else
        @test eltypes(vcat(DataTable(a = [1]), DataTable(a = [2.1]))) == [Nullable{Any}]
        @test eltypes(vcat(DataTable(a = NullableArray(Int, 1)), DataTable(a = [2.1]))) == [Nullable{Any}]
    end

    # Minimal container type promotion
    dta = DataTable(a = CategoricalArray([1, 2, 2]))
    dtb = DataTable(a = CategoricalArray([2, 3, 4]))
    dtc = DataTable(a = NullableArray([2, 3, 4]))
    dtd = DataTable(Any[2:4], [:a])
    dtab = vcat(dta, dtb)
    dtac = vcat(dta, dtc)
    @test isequal(dtab[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isequal(dtac[:a], Nullable{Int}[1, 2, 2, 2, 3, 4])
    @test isa(dtab[:a], NullableCategoricalVector{Int})
    # Fails on Julia 0.4 since promote_type(Nullable{Int}, Nullable{Float64}) gives Nullable{T}
    if VERSION >= v"0.5.0-dev"
        @test isa(dtac[:a], NullableCategoricalVector{Int})
    else
        @test isa(dtac[:a], NullableCategoricalVector{Any})
    end
    # ^^ container may flip if container promotion happens in Base/DataArrays
    dc = vcat(dtd, dtc)
    @test isequal(vcat(dtc, dtd), dc)

    # Zero-row DataTables
    dtc0 = similar(dtc, 0)
    @test isequal(vcat(dtd, dtc0, dtc), dc)
    @test eltypes(vcat(dtd, dtc0)) == eltypes(dc)

    # Missing columns
    rename!(dtd, :a, :b)
    dtda = DataTable(b = NullableArray(Nullable{Int}[2, 3, 4, Nullable(), Nullable(), Nullable()]),
                     a = NullableCategoricalVector(Nullable{Int}[Nullable(), Nullable(), Nullable(), 1, 2, 2]))
    @test isequal(vcat(dtd, dta), dtda)

    # Alignment
    @test isequal(vcat(dtda, dtd, dta), vcat(dtda, dtda))

    # vcat should be able to concatenate different implementations of AbstractDataTable (PR #944)
    @test isequal(vcat(view(DataTable(A=1:3),2),DataTable(A=4:5)), DataTable(A=[2,4,5]))
end
