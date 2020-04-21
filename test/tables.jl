module TestTables

using Test, DataFrames

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

struct DuplicateNamesTable
end
Tables.istable(::Type{DuplicateNamesTable}) = true
Tables.rowaccess(::Type{DuplicateNamesTable}) = true
Tables.rows(x::DuplicateNamesTable) = x
Tables.schema(x::DuplicateNamesTable) = Tables.Schema((:a, :a, :b), Tuple{Float64, Float64, Float64})

Base.length(x::DuplicateNamesTable) = 3
Base.eltype(x::DuplicateNamesTable) = DuplicateRow

function Base.iterate(d::DuplicateNamesTable, st=1)
    st > length(d) && return nothing
    return DuplicateNameRow(), st + 1
end

struct DuplicateNameRow
end
Base.getproperty(d::DuplicateNameRow, nm::Symbol) = 1.0

struct DuplicateNamesColumnTable
end
Tables.istable(::Type{DuplicateNamesColumnTable}) = true
Tables.columnaccess(::Type{DuplicateNamesColumnTable}) = true
Tables.columns(x::DuplicateNamesColumnTable) = x
Tables.schema(x::DuplicateNamesColumnTable) = Tables.Schema((:a, :a, :b), Tuple{Float64, Float64, Float64})
Base.getproperty(d::DuplicateNamesColumnTable, nm::Symbol) = [1.0, 2.0, 3.0]
Base.propertynames(d::DuplicateNamesColumnTable) = (:a, :a, :b)

@testset "Tables" begin
    df = DataFrame(a=Int64[1, 2, 3], b=[:a, :b, :c])

    @testset "basics $(nameof(typeof(table)))" for table in [
        df,
        view(df, :, :),
        eachrow(df),
        eachcol(df),
    ]
        @test Tables.istable(table)
        @test Tables.rowaccess(table)
        @test Tables.columnaccess(table)
        @test Tables.schema(table) === Tables.Schema((:a, :b), Tuple{Int64, Symbol})
        @test Tables.schema(table) == Tables.schema(Tables.rows(table)) == Tables.schema(Tables.columns(table))
        @test @inferred(Tables.materializer(table)(Tables.columns(table))) isa DataFrame

        row = first(Tables.rows(table))
        @test collect(propertynames(row)) == [:a, :b]
        @test getproperty(row, :a) == 1
        @test getproperty(row, :b) == :a
        @test Tables.getcolumn(row, :a) == 1
        @test Tables.getcolumn(row, 1) == 1
        @test collect(Tables.columnnames(row)) == [:a, :b]
        @test Tables.getcolumn(row, Int64, 1, :a) == 1
    end

    @testset "Row-style" begin
        bare_rows = Tables.rowtable(df)
        bare_rows_iterator = Tables.namedtupleiterator(df)
        for (actual, actual2, expected) in zip(bare_rows, bare_rows_iterator, eachrow(df))
            @test actual isa NamedTuple
            @test actual.a === actual2.a
            @test actual.b === actual2.b
            @test actual.a == expected.a
            @test actual.b == expected.b
        end

        and_back = DataFrame(bare_rows)
        @test and_back isa DataFrame
        @test names(and_back) == ["a", "b"]
        @test and_back.a == df.a
        @test and_back.b == df.b
    end

    @testset "Column-style" begin
        cols = Tables.columntable(df)
        @test cols.b  ==  df.b
        @test cols.a  ==  df.a

        and_back = DataFrame(cols)
        @test and_back isa DataFrame
        @test names(and_back) == ["a", "b"]
        @test and_back.a == df.a == Tables.getcolumn(df, :a) == Tables.getcolumn(df, 1)
        @test and_back.b == df.b
    end

    @testset "Extras" begin
        # with missing values
        df = DataFrame(a=[1, missing, 3], b=[missing, 'a', "hey"])
        @test isequal(df, DataFrame(Tables.rowtable(df)))
        @test isequal(df, DataFrame(Tables.columntable(df)))

        dn = DuplicateNamesTable()
        @test_throws ErrorException (dn |> DataFrame)

        dn = DuplicateNamesColumnTable()
        @test_throws ArgumentError (dn |> DataFrame)

        # non-Tables.jl constructor fallbacks
        @test DataFrame([(a = 0,), (a = 1,)]) == DataFrame(a = 0:1)

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

        append!(df, [nt])
        @test size(df) == (7, 3)

        # categorical values
        cat = CategoricalVector(["hey", "there", "sailor"])
        cat2 = [c for c in cat] # Vector of CategoricalValue
        nt = (a=cat, b=cat2)
        df = DataFrame(nt, copycols=false)
        @test df.a === cat
        @test df.b === cat2
        # test in the unknown schema case that a
        # Vector of CategoricalValue is built into CategoricalVector
        ct = Tables.buildcolumns(nothing, Tables.rows(nt))
        @test ct.a !== cat
        @test ct.b !== cat2
        @test ct.a == cat
        @test ct.b == cat == cat2
    end
end

@testset "DataFrame!" begin
    nt = (a=Int64[1, 2, 3], b=[:a, :b, :c])
    df1 = DataFrame!(nt)
    df2 = DataFrame!(df1)
    df3 = DataFrame(nt)
    @test Tables.columntable(df1) === nt
    @test Tables.columntable(df2) === nt
    @test Tables.columntable(df3) == nt
    @test Tables.columntable(df3) !== nt

    v = [(a=1,b=2), (a=3, b=4)]
    df = DataFrame(v)
    @test size(df) == (2, 2)
    @test df.a == [1, 3]
    @test df.b == [2, 4]
    @test DataFrame(v, copycols=false) == DataFrame(v, copycols=true) == df
    @test_throws ArgumentError DataFrame!(v)
end

@testset "columnindex" begin
    df = DataFrame(rand(3,4))
    @test columnindex.(Ref(df), names(df)) == 1:4
    @test columnindex(df, :a) == 0
    # @test_throws ErrorException columnindex(df, 1)
    # @test_throws ErrorException columnindex(df, "x1")
end

@testset "eachrow and eachcol integration" begin
    df = DataFrame(rand(3,4), [:a, :b, :c, :d])

    df2 = DataFrame(eachrow(df))
    @test df == df2
    @test !any(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame!(eachrow(df))
    @test df == df2
    @test all(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(pairs(eachcol(df)))
    @test df == df2
    @test !any(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame!(pairs(eachcol(df)))
    @test df == df2
    @test all(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(eachcol(df))
    @test propertynames(df2) == [:x1, :x2, :x3, :x4]
    @test all(((a,b),) -> a == b, zip(eachcol(df), eachcol(df2)))
    @test !any(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(eachcol(df))
    @test propertynames(df2) == [:x1, :x2, :x3, :x4]
    @test !any(((a,b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    @test Tables.rowtable(df) == Tables.rowtable(eachrow(df))
    @test Tables.rowtable(df) == Tables.rowtable(eachcol(df))
    @test Tables.columntable(df) == Tables.columntable(eachrow(df))
    @test Tables.columntable(df) == Tables.columntable(eachcol(df))

    for (a, b, c) in zip(Tables.rowtable(df),
                         Tables.namedtupleiterator(eachrow(df)),
                         Tables.namedtupleiterator(eachcol(df)))
        @test a isa NamedTuple
        @test a === b === c
    end

    @test Tables.getcolumn(eachcol(df), 1) == Tables.getcolumn(df, 1)
    @test Tables.getcolumn(eachcol(df), :a) == Tables.getcolumn(df, :a)
    @test Tables.columnnames(eachcol(df)) == Tables.columnnames(df)
    @test Tables.getcolumn(eachrow(df), 1) == Tables.getcolumn(df, 1)
    @test Tables.getcolumn(eachrow(df), :a) == Tables.getcolumn(df, :a)
    @test Tables.columnnames(eachrow(df)) == Tables.columnnames(df)
end

end # module
