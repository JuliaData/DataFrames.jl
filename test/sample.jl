module TestSample
    using Base.Test
    using DataFrames
    
    df = DataFrame(A=1:10,B=11:20)
    srand(1)
    df_sample = sample(df, 5)
    @test df_sample[:A] == [1,8,7,4,2]
    @test df_sample[:B] == [11,18,17,14,12]
end
