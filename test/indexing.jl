module TestIndexing
    using Base.Test
    using DataFrames

    df = DataFrame(A = 1:3, B = [:a, :b, :c], C=["a", 1, "2"])

    # get single column
    @test df[:A] == 1:3
    @test names(df[[:A, :B]]) == [:A, :B]
    @test names(df[[:B, :A]]) == [:B, :A]
    @test df[[:A, :B]][:A] == 1:3
    @test df[[:A, :B]][:B] == [:a, :b, :c]
    # get row
    @test df[1, 1] == 1
    @test df[2, 2] == :b
    # get columns and rows
    @test names(df[3, [true, false, true]]) == [:A, :C]
    @test df[3, [true, false, true]][:A] == [3]
    @test df[3, [true, false, true]][:C] == ["2"]
    @test names(df[[1, 2], [true]]) == [:A]
    @test df[[1, 2], [true]][:A] == [1, 2]


    original = deepcopy(df)
    df[:A] = 4:6
    @test df[:A] == 4:6
    df = deepcopy(original)
    df[:A] = 4
    @test df[:A] == [4, 4, 4]
    df = deepcopy(original)
    df[1] = 1
    @test df[:A] == [1, 1, 1]
    df = deepcopy(original)
    df[:E] = "new"
    @test df[:E] == ["new", "new", "new"]
    df[1, :E] = "a"
    @test df[1, :E] == "a"

    df = deepcopy(original)
    df[:E] = 3
    @test df[:E] == [3, 3, 3]
    df[[1, 2], [:E, :A]] = 5
    @test df[:E] == [5, 5, 3]
    @test df[:A] == [5, 5, 3]

    df = deepcopy(original)
    df[[false, true], [true, false, true]] = 6
    @test df[:A] == [1, 6, 3]
    @test df[:B] == [:a, :b, :c]
    @test df[:C] == ["a", 6, "2"]

    # assigning one dataframe to another
    df = deepcopy(original)
    other = DataFrame(A = 4:6, B = [:A, :B, :C], D=5:7)
    df[[:A, :B]] = other
    @test df[:A] ≡ other[:A]
    @test df[:B] ≡ other[:B]

    df = deepcopy(original)
    other = DataFrame(A = 4:6, B = [:A, :B, :C], D=5:7)
    df[[2, 1], [:A, :B]] = other[1:2, :]
    @test df[:A] == [5, 4, 3]
    @test df[:B] == [:B, :A, :c]
    @test df[:C] == ["a", 1, "2"]
end
