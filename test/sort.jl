using Base.Test
using DataFrames

let dv1 = DataVector[9,1,8,NA,3,3,7,NA],
    dv2 = 1.0 * dv1,
    dv3 = DataArray([1:8]),
    pdv1 = PooledDataArray(dv1),
    idv1 = IndexedVector(dv1)

    @test sortperm(dv1) == sortperm(dv2)
    @test sortperm(dv1) == sortperm(pdv1)
    @test sortperm(dv1) == sortperm(idv1)
    @test isequal(sort(dv1), DataArray(sort(dv1)))
    @test isequal(sort(dv1), DataArray(sort(pdv1)))
    @test isequal(sort(dv1), DataArray(sort(idv1)))

    d = DataFrame(dv1 = dv1, dv2 = dv2, dv3 = dv3, pdv1 = pdv1, idv1 = idv1)

    @test sortperm(d) == sortperm(dv1)
    @test sortperm(d[["dv3","dv1"]]) == sortperm(dv3)
    @test sort(d, cols="dv1")["dv3"] == sortperm(dv1)
    @test sort(d, cols="dv2")["dv3"] == sortperm(dv1)
    @test sort(d, cols="pdv1")["dv3"] == sortperm(dv1)
    @test sort(d, cols="idv1")["dv3"] == sortperm(dv1)
    @test sort(d, cols=["dv1","pdv1"])["dv3"] == sortperm(dv1)
    @test sort(d, cols=["dv1","dv3"])["dv3"] == sortperm(dv1)
end


let df=DataFrame(rank=rand(1:12, 1000), chrom=rand(1:24,1000), pos=rand(1:100000,1000))

    @test issorted(sort(df))
    @test issorted(sort(df,rev=true), rev=true)
    @test issorted(sort(df, cols=["chrom","pos"])[["chrom","pos"]])

    ds = sort(df, cols=(order("rank",rev=true),"chrom","pos"))
    @test issorted(ds, cols=(order("rank",rev=true),"chrom","pos"))
    @test issorted(ds, rev=(true,false,false))

    ds2 = sort(df, cols=("rank","chrom","pos"), rev=(true,false,false))
    @test issorted(ds2, cols=(order("rank",rev=true),"chrom","pos"))
    @test issorted(ds2, rev=(true,false,false))

    @test ds == ds2
end