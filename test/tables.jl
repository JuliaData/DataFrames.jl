module TestTables
using Test, Tables, DataFrames, CategoricalArrays

struct NamedTupleIterator{T <: NamedTuple}
    elements::Vector{T}
end
Base.length(n::NamedTupleIterator) = length(n.elements)
Base.IteratorEltype(::Type{<:NamedTupleIterator}) = Base.HasEltype()
Base.eltype(n::NamedTupleIterator{T}) where {T} = T

function Base.iterate(nt::NamedTupleIterator, st=1)
    st > length(nt.elements) && return nothing
    return nt.elements[st], st + 1
end

struct EltypeUnknownIterator
    elements
end
Base.IteratorSize(::Type{EltypeUnknownIterator}) = Base.SizeUnknown()
Base.IteratorEltype(::Type{EltypeUnknownIterator}) = Base.EltypeUnknown()

function Base.iterate(nt::EltypeUnknownIterator, st=1)
    st > length(nt.elements) && return nothing
    return nt.elements[st], st + 1
end

@testset "Tables" begin
    df = DataFrame(a=[1, 2, 3], b=[:a, :b, :c])

    @testset "basics" begin
        @test Tables.istable(df)
        @test Tables.rowaccess(df)
        @test Tables.columnaccess(df)
        @test Tables.schema(df) === Tables.Schema((:a, :b), Tuple{Int64, Symbol})
        @test Tables.schema(df) == Tables.schema(Tables.rows(df)) == Tables.schema(Tables.columns(df))

        row = first(Tables.rows(df))
        @test propertynames(row) == [:a, :b]
        @test getproperty(row, :a) == 1
        @test getproperty(row, :b) == :a
    end

    @testset "Row-style" begin
        bare_rows = Tables.rowtable(df)
        for (actual, expected) in zip(bare_rows, eachrow(df))
            @test actual.a == expected.a
            @test actual.b == expected.b
        end

        and_back = DataFrame(bare_rows)
        @test and_back isa DataFrame
        @test names(and_back) == [:a, :b]
        @test and_back.a == df.a
        @test and_back.b == df.b
    end

    @testset "Column-style" begin
        cols = Tables.columntable(df)
        @test cols.b  ==  df.b
        @test cols.a  ==  df.a

        and_back = DataFrame(cols)
        @test and_back isa DataFrame
        @test names(and_back) == [:a, :b]
        @test and_back.a == df.a
        @test and_back.b == df.b
    end

    @testset "Extras" begin
        # with missing values
        df = DataFrame(a=[1, missing, 3], b=[missing, 'a', "hey"])
        @test isequal(df, DataFrame(Tables.rowtable(df)))
        @test isequal(df, DataFrame(Tables.columntable(df)))

        # non-Tables.jl constructor fallbacks
        nt = (a=1, b=:a, c=missing)
        nti = NamedTupleIterator([nt, nt, nt])
        df = DataFrame(nti)
        @test size(df) == (3, 3)
        @test df.a == [1, 1, 1]
        @test df.b == [:a, :a, :a]
        @test isequal(df.c, [missing, missing, missing])

        etu = EltypeUnknownIterator([nt, nt, nt])
        df = DataFrame(etu)
        @test size(df) == (3, 3)
        @test df.a == [1, 1, 1]
        @test df.b == [:a, :a, :a]
        @test isequal(df.c, [missing, missing, missing])

        append!(df, etu)
        @test size(df) == (6, 3)

        # categorical values
        cat = CategoricalVector(["hey", "there", "sailor"])
        cat2 = [c for c in cat] # Vector of CategoricalString
        nt = (a=cat, b=cat2)
        df = DataFrame(nt)
        @test df.a === cat
        @test df.b === cat2
        # test in the unknown schema case that a
        # Vector of CategoricalString is built into CategoricalVector
        ct = Tables.buildcolumns(nothing, Tables.rows(nt))
        @test ct.a !== cat
        @test ct.b !== cat2
        @test ct.a == cat
        @test ct.b == cat == cat2
    end
end
end
