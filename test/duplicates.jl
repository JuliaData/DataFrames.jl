module TestDuplicates
    using Base.Test
    using DataTables

    dt = DataTable(a = [1, 2, 3, 3, 4])
    udt = DataTable(a = [1, 2, 3, 4])
    @test isequal(nonunique(dt), [false, false, false, true, false])
    @test isequal(udt, unique(dt))
    unique!(dt)
    @test isequal(dt, udt)

    pdt = DataTable(a = NullableCategoricalArray(Nullable{String}["a", "a", Nullable(),
                                             Nullable(), "b", Nullable(), "a", Nullable()]),
                    b = NullableCategoricalArray(Nullable{String}["a", "b", Nullable(),
                                                              Nullable(), "b", "a", "a", "a"]))
    updt = DataTable(a = NullableCategoricalArray(Nullable{String}["a", "a", Nullable(), "b", Nullable()]),
                     b = NullableCategoricalArray(Nullable{String}["a", "b", Nullable(), "b", "a"]))
    @test isequal(nonunique(pdt), [false, false, false, true, false, false, true, true])
    @test isequal(nonunique(updt), falses(5) )
    @test isequal(updt, unique(pdt))
    unique!(pdt)
    @test isequal(pdt, updt)

    @testset "missing" begin
        dt = DataTable(A = 1:12, B = repeat('A':'C', inner=4))
        @test DataTables.colmissing(dt) == [0, 0]
    end
end
