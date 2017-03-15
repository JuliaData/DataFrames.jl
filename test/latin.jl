module TestIO
    using Base.Test
    using DataFrames, Compat

    #test_group("We can read various file types.")

    data = joinpath(dirname(@__FILE__), "data")

    # this fails for utf8 with exception
    df = readtable("$data/latin1/latin1.csv", encoding=:latin1)
    @test df[1, 2] == "Sí"
    
    df = readtable("$data/latin1/latin_normal_headers.csv", encoding=:latin1)
    @test df[1,2] == "Sí"
    
    # using utf it reads it incorrectly
    df = readtable("$data/latin1/latin_normal_headers.csv", encoding=:latin1)
    @test df[1,2] != "Sí"
    
end
