module TestDataFrame
    using Base.Test
    using DataArrays
    using DataFrames

    #test_group("Operations on DataFrames that have column groupings")

    x = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    y = DataFrame(c = [1, 2, 3], d = [4, 5, 6])

    #
    # Equality
    #
    @test isequal(DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])), DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])))
    @test !isequal(DataFrame(a=@data([1, 2]), b=@data([4, 5])), DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])))
    @test !isequal(DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])), DataFrame(a=@data([1, 2, 3])))
    @test !isequal(DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])), DataFrame(a=@data([1, 2, 3]), c=@data([4, 5, 6])))
    @test !isequal(DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])), DataFrame(b=@data([4, 5, 6]), a=@data([1, 2, 3])))
    @test !isequal(DataFrame(a=@data([1, 2, 2]), b=@data([4, 5, 6])), DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])))
    @test isequal(DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6])), DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6])))

    @test DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])) == DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6]))
    @test DataFrame(a=@data([1, 2]), b=@data([4, 5])) != DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6]))
    @test DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])) != DataFrame(a=@data([1, 2, 3]))
    @test DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])) != DataFrame(a=@data([1, 2, 3]), c=@data([4, 5, 6]))
    @test DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])) != DataFrame(b=@data([4, 5, 6]), a=@data([1, 2, 3]))
    @test DataFrame(a=@data([1, 2, 2]), b=@data([4, 5, 6])) != DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6]))
    @test DataFrame(a=@data([1, 3, NA]), b=@data([4, 5, 6])) != DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6]))
    @test isna(DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6])) == DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6])))
    @test isna(DataFrame(a=@data([1, 2, NA]), b=@data([4, 5, 6])) == DataFrame(a=@data([1, 2, 3]), b=@data([4, 5, 6])))

    z = deepcopy(x)  

    z = hcat(x, y)

    v = DataFrame(
        a = [5,6,7],
        b = [8,9,10]
    )
    z = vcat(DataFrame[v, x])

    z = vcat(v, x)

    # Deleting columns removes any mention from groupings
    delete!(x, :a)
    # @test colnames(x) == [:b]

    ## del calls ref, which properly deals with groupings
    z2 = z[:,[1,1,2]]
    @test names(z2) == [:a, :a_1, :b]

    #test_group("DataFrame assignment")

    #test_group("Empty DataFrame constructors")
    df = DataFrame(10, 5)
    @assert size(df, 1) == 10
    @assert size(df, 2) == 5
    @assert typeof(df[:, 1]) == DataVector{Float64}

    df = DataFrame(Int, 10, 3)
    @assert size(df, 1) == 10
    @assert size(df, 2) == 3
    @assert typeof(df[:, 1]) == DataVector{Int}
    @assert typeof(df[:, 2]) == DataVector{Int}
    @assert typeof(df[:, 3]) == DataVector{Int}

    df = DataFrame({Int, Float64, ASCIIString}, 100)
    @assert size(df, 1) == 100
    @assert size(df, 2) == 3
    @assert typeof(df[:, 1]) == DataVector{Int}
    @assert typeof(df[:, 2]) == DataVector{Float64}
    @assert typeof(df[:, 3]) == DataVector{ASCIIString}

    df = DataFrame({Int, Float64, ASCIIString}, [:A, :B, :C], 100)
    @assert size(df, 1) == 100
    @assert size(df, 2) == 3
    @assert typeof(df[:, 1]) == DataVector{Int}
    @assert typeof(df[:, 2]) == DataVector{Float64}
    @assert typeof(df[:, 3]) == DataVector{ASCIIString}

    df = convert(DataFrame, zeros(10, 5))
    @assert size(df, 1) == 10
    @assert size(df, 2) == 5
    @assert typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, ones(10, 5))
    @assert size(df, 1) == 10
    @assert size(df, 2) == 5
    @assert typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, eye(10, 5))
    @assert size(df, 1) == 10
    @assert size(df, 2) == 5
    @assert typeof(df[:, 1]) == Vector{Float64}

    #test_group("Other DataFrame constructors")
    df = DataFrame([{:a=>1, :b=>'c'}, {:a=>3, :b=>'d'}, {:a=>5}])
    @assert size(df, 1) == 3
    @assert size(df, 2) == 2
    @assert typeof(df[:,:a]) == DataVector{Int}
    @assert typeof(df[:,:b]) == DataVector{Char}

    df = DataFrame([{:a=>1, :b=>'c'}, {:a=>3, :b=>'d'}, {:a=>5}], [:a, :b])
    @assert size(df, 1) == 3
    @assert size(df, 2) == 2
    @assert typeof(df[:,:a]) == DataVector{Int}
    @assert typeof(df[:,:b]) == DataVector{Char}

    data = {:A => [1, 2], :C => [:1, :2], :B => [3, 4]}
    df = DataFrame(data)
    # Specify column_names
    df = DataFrame(data, [:C, :A, :B])

    # This assignment was missing before
    df = DataFrame(Column = [:A])
    df[1, :Column] = "Testing"

    # flipud() tests
    df = DataFrame(A = 1:4)
    @assert isequal(flipud(df), DataFrame(A = [4, 3, 2, 1]))
    @assert isequal(df, DataFrame(A = [1, 2, 3, 4]))
    flipud!(df)
    @assert isequal(df, DataFrame(A = [4, 3, 2, 1]))

    # zero-row dataframe and subdataframe test
    df = DataFrame(x=[], y=[])
    @assert nrow(df) == 0
    df = DataFrame(x=[1:3], y=[3:5])
    sdf = sub(df, df[:x] .== 4)
    @assert nrow(sdf) == 0

    @assert hash(convert(DataFrame, [1 2; 3 4])) == hash(convert(DataFrame, [1 2; 3 4]))
    @assert hash(convert(DataFrame, [1 2; 3 4])) != hash(convert(DataFrame, [1 3; 2 4]))
end
