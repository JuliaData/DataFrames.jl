module TestDeprecated

using Test, DataFrames, Random
import DataFrames: identifier

# old sort(df; cols=...) syntax
df = DataFrame(a=[1, 3, 2], b=[6, 5, 4])
@test sort(df; cols=[:a, :b]) == DataFrame(a=[1, 2, 3], b=[6, 4, 5])
sort!(df; cols=[:a, :b])
@test df == DataFrame(a=[1, 2, 3], b=[6, 4, 5])

df = DataFrame(a=[1, 3, 2], b=[6, 5, 4])
@test sort(df; cols=[:b, :a]) == DataFrame(a=[2, 3, 1], b=[4, 5, 6])
sort!(df; cols=[:b, :a])
@test df == DataFrame(a=[2, 3, 1], b=[4, 5, 6])

@test first.(collect(pairs(DataFrameRow(df, 1, :)))) == [:a, :b]
@test last.(collect(pairs(DataFrameRow(df, 1, :)))) == [df[1, 1], df[1, 2]]

@testset "identifier" begin
    @test identifier("%_B*_\tC*") == :_B_C_
    @test identifier("2a") == :x2a
    @test identifier("!") == :x!
    @test identifier("\t_*") == :_
    @test identifier("begin") == :_begin
    @test identifier("end") == :_end
end

# Check that reserved words are up to date

@testset "reserved words" begin
    f = "$(Sys.BINDIR)/../../src/julia-parser.scm"
    if isfile(f)
        r1 = r"define initial-reserved-words '\(([^)]+)"
        r2 = r"define \(parse-block s(?: \([^)]+\))?\)\s+\(parse-Nary s (?:parse-eq '\([^(]*|down '\([^)]+\) '[^']+ ')\(([^)]+)"
        body = read(f, String)
        m1, m2 = match(r1, body), match(r2, body)
        if m1 == nothing || m2 == nothing
            error("Unable to extract keywords from 'julia-parser.scm'.")
        else
            s = replace(string(m1.captures[1]," ",m2.captures[1]), r";;.*?\n" => "")
            rw = Set(split(s, r"\W+"))
            @test rw == DataFrames.RESERVED_WORDS
        end
    else
        @warn("Unable to validate reserved words against parser. ",
              "Expected if Julia was not built from source.")
    end
end

# deprecated combine

df = DataFrame(a=[1, 1, 2, 2, 2], b=1:5)
gd = groupby(df, :a)
@test combine(gd) == combine(identity, gd)

@testset "colwise" begin
    Random.seed!(1)
    df = DataFrame(a = repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                   b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                   c = Vector{Union{Float64, Missing}}(randn(8)))

    missingfree = DataFrame([collect(1:10)], [:x1])

    @testset "::Function, ::AbstractDataFrame" begin
        cw = colwise(sum, df)
        answer = [20, 12, -0.4283098098931877]
        @test isa(cw, Vector{Real})
        @test size(cw) == (ncol(df),)
        @test cw == answer

        cw = colwise(sum, missingfree)
        answer = [55]
        @test isa(cw, Array{Int, 1})
        @test size(cw) == (ncol(missingfree),)
        @test cw == answer
    end

    @testset "::Function, ::GroupedDataFrame" begin
        gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
        @test colwise(length, gd) == [[2,2], [2,2]]
    end

    @testset "::Vector, ::AbstractDataFrame" begin
        cw = colwise([sum], df)
        answer = [20 12 -0.4283098098931877]
        @test isa(cw, Array{Real, 2})
        @test size(cw) == (length([sum]),ncol(df))
        @test cw == answer

        cw = colwise([sum, minimum], missingfree)
        answer = reshape([55, 1], (2,1))
        @test isa(cw, Array{Int, 2})
        @test size(cw) == (length([sum, minimum]), ncol(missingfree))
        @test cw == answer

        cw = colwise([Vector{Union{Int, Missing}}], missingfree)
        answer = reshape([Vector{Union{Int, Missing}}(1:10)], (1,1))
        @test isa(cw, Array{Vector{Union{Int, Missing}},2})
        @test size(cw) == (1, ncol(missingfree))
        @test cw == answer

        @test_throws MethodError colwise(["Bob", :Susie], DataFrame(A = 1:10, B = 11:20))
    end

    @testset "::Vector, ::GroupedDataFrame" begin
        gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
        @test colwise([length], gd) == [[2 2], [2 2]]
    end

    @testset "::Tuple, ::AbstractDataFrame" begin
        cw = colwise((sum, length), df)
        answer = Any[20 12 -0.4283098098931877; 8 8 8]
        @test isa(cw, Array{Real, 2})
        @test size(cw) == (length((sum, length)), ncol(df))
        @test cw == answer

        cw = colwise((sum, length), missingfree)
        answer = reshape([55, 10], (2,1))
        @test isa(cw, Array{Int, 2})
        @test size(cw) == (length((sum, length)), ncol(missingfree))
        @test cw == answer

        cw = colwise((CategoricalArray, Vector{Union{Int, Missing}}), missingfree)
        answer = reshape([CategoricalArray(1:10), Vector{Union{Int, Missing}}(1:10)],
                         (2, ncol(missingfree)))
        @test typeof(cw) == Array{AbstractVector,2}
        @test size(cw) == (2, ncol(missingfree))
        @test cw == answer

        @test_throws MethodError colwise(("Bob", :Susie), DataFrame(A = 1:10, B = 11:20))
    end

    @testset "::Tuple, ::GroupedDataFrame" begin
        gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
        @test colwise((length), gd) == [[2,2],[2,2]]
    end
end

end # module
