module TestDataFrame
    using Base.Test
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
    @test names(x) == [:b]

    ## del calls ref, which properly deals with groupings
    z2 = z[:,[1,1,2]]
    @test names(z2) == [:a, :a_1, :b]

    #test_group("DataFrame assignment")
    # Insert single column
    x[symbol("c")] = 1:3
    @test_throws ErrorException x[symbol("\u212b")] = 1:3
    @test_throws ErrorException x[symbol("end")] = 1:3
    @test_throws ErrorException x[symbol("1a")] = 1:3

    x0 = x[[], :]
    @test_throws ErrorException x0[:d] = [1]
    @test_throws ErrorException x0[:d] = 1:3

    # Insert single value
    x[:d] = 3
    @test x[:d] == [3, 3, 3]

    x0[:d] = 3
    @test x0[:d] == Int[]

    #test_group("Empty DataFrame constructors")
    df = DataFrame(Int, 10, 3)
    @test size(df, 1) == 10
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == DataVector{Int}
    @test typeof(df[:, 2]) == DataVector{Int}
    @test typeof(df[:, 3]) == DataVector{Int}

    df = DataFrame({Int, Float64, ASCIIString}, 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == DataVector{Int}
    @test typeof(df[:, 2]) == DataVector{Float64}
    @test typeof(df[:, 3]) == DataVector{ASCIIString}

    df = DataFrame({Int, Float64, ASCIIString}, [:A, :B, :C], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[:, 1]) == DataVector{Int}
    @test typeof(df[:, 2]) == DataVector{Float64}
    @test typeof(df[:, 3]) == DataVector{ASCIIString}

    df = convert(DataFrame, zeros(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, ones(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    df = convert(DataFrame, eye(10, 5))
    @test size(df, 1) == 10
    @test size(df, 2) == 5
    @test typeof(df[:, 1]) == Vector{Float64}

    #test_group("Other DataFrame constructors")
    df = DataFrame([{:a=>1, :b=>'c'}, {:a=>3, :b=>'d'}, {:a=>5}])
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test typeof(df[:,:a]) == DataVector{Int}
    @test typeof(df[:,:b]) == DataVector{Char}

    df = DataFrame([{:a=>1, :b=>'c'}, {:a=>3, :b=>'d'}, {:a=>5}], [:a, :b])
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test typeof(df[:,:a]) == DataVector{Int}
    @test typeof(df[:,:b]) == DataVector{Char}

    data = {:A => [1, 2], :C => [:1, :2], :B => [3, 4]}
    df = DataFrame(data)
    # Specify column_names
    df = DataFrame(data, [:C, :A, :B])

    # This assignment was missing before
    df = DataFrame(Column = [:A])
    df[1, :Column] = "Testing"

    # zero-row dataframe and subdataframe test
    df = DataFrame(x=[], y=[])
    @test nrow(df) == 0
    df = DataFrame(x=[1:3], y=[3:5])
    sdf = sub(df, df[:x] .== 4)
    @test size(sdf, 1) == 0

    @test hash(convert(DataFrame, [1 2; 3 4])) == hash(convert(DataFrame, [1 2; 3 4]))
    @test hash(convert(DataFrame, [1 2; 3 4])) != hash(convert(DataFrame, [1 3; 2 4]))
    
    
    # push!(df, row)
    df=DataFrame( first=[1,2,3], second=["apple","orange","pear"] )

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, {3,"pear"})
    @test df==dfb

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, (3,"pear"))
    @test df==dfb

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, (33.33,"pear"))

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, ("coconut",22))

    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, [ :first=>3,:second=>"pear" ])
    @test df==dfb
    
    df=DataFrame( first=[1,2,3], second=["apple","orange","banana"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    push!(dfb, [ "first"=>3,"second"=>"banana" ])
    @test df==dfb

    df0= DataFrame( first=[1,2], second=["apple","orange"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, [ :first=>true,:second=>false ])
    @test df0==dfb

    df0= DataFrame( first=[1,2], second=["apple","orange"] )
    dfb= DataFrame( first=[1,2], second=["apple","orange"] )
    @test_throws ArgumentError push!(dfb, ["first"=>"chicken", "second"=>"stuff" ])
    @test df0==dfb

end
