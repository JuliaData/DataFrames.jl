module TestIndexedVector
    using Base.Test
    using DataArrays
    using DataFrames

    #
    # IndexedVector tests
    #

    srand(1)
    a = DataArray(rand(1:5,20))
    a[1:2] = NA
    ia = IndexedVector(a)
    b = DataArray(rand(5:8,20))
    ib = IndexedVector(b)


    ia .== 4
    v = [1:20]
    @assert v[ia .== 4] == v[a .== 4]
    @assert sort(v[(ia .== 4) | (ia .== 5)]) == v[(a .== 4) | (a .== 5)]
    @assert sort(v[(ia .>= 4) & (ia .== 5)]) == v[(a .>= 4) & (a .== 5)]
    @assert sort(v[!(ia .== 4)]) == v[!(a .== 4)]
    @assert sort(v[findin(ia, [3:6])]) == v[findin(a, [3:6])]
    @assert sort(v[(ia .== 4) | (ib .== 6)]) == v[(a .== 4) | (b .== 6)]


    df = DataFrame(quote
             x1 = IndexedVector(vcat(fill([1:5],4)...))
             x2 = IndexedVector(vcat(fill(letters[1:10],2)...))
         end)

    df[:(x2 .== "a"), :]
    df[:( (x2 .== "a") | (x1 .== 2) ), :]
    df[:( ("b" .<= x2 .<= "c") | (x1 .== 5) ), :]
    df[:( (x1 .== 1) & (x2 .== "a") ), :]

    df[findin(df["x2"], ["c","e","X"]), :]
end
