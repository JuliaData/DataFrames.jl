module TestSort
    using Base.Test
    using DataTables

    dv1 = NullableArray(Nullable{Int}[9, 1, 8, Nullable(), 3, 3, 7, Nullable()])
    dv2 = NullableArray(Nullable{Int}[9, 1, 8, Nullable(), 3, 3, 7, Nullable()])
    dv3 = NullableArray(1:8)
    cv1 = NullableCategoricalArray(dv1, ordered=true)

    d = DataTable(dv1 = dv1, dv2 = dv2, dv3 = dv3, cv1 = cv1)

    @test sortperm(d) == sortperm(dv1)
    @test sortperm(d[[:dv3, :dv1]]) == sortperm(dv3)
    @test isequal(sort(d, cols=:dv1)[:dv3], NullableArray(sortperm(dv1)))
    @test isequal(sort(d, cols=:dv2)[:dv3], NullableArray(sortperm(dv1)))
    @test isequal(sort(d, cols=:cv1)[:dv3], NullableArray(sortperm(dv1)))
    @test isequal(sort(d, cols=[:dv1, :cv1])[:dv3], NullableArray(sortperm(dv1)))
    @test isequal(sort(d, cols=[:dv1, :dv3])[:dv3], NullableArray(sortperm(dv1)))

    dt = DataTable(rank=rand(1:12, 1000),
                   chrom=rand(1:24, 1000),
                   pos=rand(1:100000, 1000))

    @test issorted(sort(dt))
    @test issorted(sort(dt, rev=true), rev=true)
    @test issorted(sort(dt, cols=[:chrom,:pos])[[:chrom,:pos]])

    ds = sort(dt, cols=(order(:rank, rev=true),:chrom,:pos))
    @test issorted(ds, cols=(order(:rank, rev=true),:chrom,:pos))
    @test issorted(ds, rev=(true, false, false))

    ds2 = sort(dt, cols=(:rank, :chrom, :pos), rev=(true, false, false))
    @test issorted(ds2, cols=(order(:rank, rev=true), :chrom, :pos))
    @test issorted(ds2, rev=(true, false, false))

    @test isequal(ds2, ds)

    sort!(dt, cols=(:rank, :chrom, :pos), rev=(true, false, false))
    @test issorted(dt, cols=(order(:rank, rev=true), :chrom, :pos))
    @test issorted(dt, rev=(true, false, false))

    @test isequal(dt, ds)

    # Check that columns that shares the same underlying array are only permuted once PR#1072
    dt = DataTable(a=[2,1])
    dt[:b] = dt[:a]
    sort!(dt, cols=:a)
    @test dt == DataTable(a=[1,2],b=[1,2])
end
