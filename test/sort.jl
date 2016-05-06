module TestSort
    using Base.Test
    using DataFrames

    dv1 = NullableArray(Nullable{Int}[9, 1, 8, Nullable(), 3, 3, 7, Nullable()])
    dv2 = NullableArray(Nullable{Float64}[9, 1, 8, Nullable(), 3, 3, 7, Nullable()])
    dv3 = NullableArray(1:8)
    pdv1 = NullableNominalArray(dv1)

    d = DataFrame(dv1 = dv1, dv2 = dv2, dv3 = dv3, pdv1 = pdv1)

# FIXME: need an implementation of sortperm() for NullableArrays which accepts NULLs
#    @test sortperm(d) == sortperm(dv1)
#    @test sortperm(d[[:dv3, :dv1]]) == sortperm(dv3)
#    @test sort(d, cols=:dv1)[:dv3] == sortperm(dv1)
#    @test sort(d, cols=:dv2)[:dv3] == sortperm(dv1)
#    @test sort(d, cols=:pdv1)[:dv3] == sortperm(dv1)
#    @test sort(d, cols=[:dv1, :pdv1])[:dv3] == sortperm(dv1)
#    @test sort(d, cols=[:dv1, :dv3])[:dv3] == sortperm(dv1)

    df = DataFrame(rank=rand(1:12, 1000),
                   chrom=rand(1:24, 1000),
                   pos=rand(1:100000, 1000))

    @test issorted(sort(df))
    @test issorted(sort(df, rev=true), rev=true)
    @test issorted(sort(df, cols=[:chrom,:pos])[[:chrom,:pos]])

    ds = sort(df, cols=(order(:rank, rev=true),:chrom,:pos))
    @test issorted(ds, cols=(order(:rank, rev=true),:chrom,:pos))
    @test issorted(ds, rev=(true, false, false))

    ds2 = sort(df, cols=(:rank, :chrom, :pos), rev=(true, false, false))
    @test issorted(ds2, cols=(order(:rank, rev=true), :chrom, :pos))
    @test issorted(ds2, rev=(true, false, false))

    @test isequal(ds2, ds)

    sort!(df, cols=(:rank, :chrom, :pos), rev=(true, false, false))
    @test issorted(df, cols=(order(:rank, rev=true), :chrom, :pos))
    @test issorted(df, rev=(true, false, false))

    @test isequal(df, ds)


end
