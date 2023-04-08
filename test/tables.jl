module TestTables

using Test, DataFrames, CategoricalArrays

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

struct EmptyTableWithNames <: AbstractVector{Any}
end
Tables.isrowtable(::Type{EmptyTableWithNames}) = true
Tables.rows(x::EmptyTableWithNames) = x
Tables.schema(x::EmptyTableWithNames) = Tables.Schema((:a, :b, :c), Tuple{Float64, String, Float64})
Base.size(x::EmptyTableWithNames) = (0,)
Base.eltype(x::EmptyTableWithNames) = NamedTuple
Base.iterate(x::EmptyTableWithNames, st=1) = nothing

struct CopiedCols
end
Tables.istable(::Type{CopiedCols}) = true
Tables.columnaccess(::Type{CopiedCols}) = true
const COPIEDCOLS = (a=[1,2,3], b=[:a, :b, :c])
Tables.columns(x::CopiedCols) = Tables.CopiedColumns(COPIEDCOLS)

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
        @test DataFrame([(a=0,), (a=1,)]) == DataFrame(a=0:1)

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

        # https://github.com/JuliaData/CSV.jl/issues/702
        df = DataFrame(EmptyTableWithNames())
        @test size(df) == (0, 3)
        @test names(df) == ["a", "b", "c"]
        @test eltype.(eachcol(df)) == [Float64, String, Float64]
    end
end

@testset "DataFrame without copying cols" begin
    nt = (a=Int64[1, 2, 3], b=[:a, :b, :c])
    df1 = DataFrame(nt, copycols=false)
    df2 = DataFrame(df1, copycols=false)
    df3 = DataFrame(nt)
    @test Tables.columntable(df1) === nt
    @test Tables.columntable(df2) === nt
    @test Tables.columntable(df3) == nt
    @test Tables.columntable(df3) !== nt

    df4 = DataFrame(Tables.CopiedColumns(nt))
    @test df4.a === nt.a
    df4 = DataFrame(Tables.CopiedColumns(nt), copycols=true)
    @test df4.a !== nt.a

    v = [(a=1, b=2), (a=3, b=4)]
    df = DataFrame(v)
    @test size(df) == (2, 2)
    @test df.a == [1, 3]
    @test df.b == [2, 4]
    @test DataFrame(v, copycols=false) == DataFrame(v, copycols=true) == df

    df = DataFrame(CopiedCols())
    @test df.a === COPIEDCOLS.a
    df = DataFrame(CopiedCols(); copycols=true)
    @test df.a !== COPIEDCOLS.a
    @test df.a == COPIEDCOLS.a
end

@testset "columnindex" begin
    df = DataFrame(rand(3, 4), :auto)

    for x in (df, view(df, 1, :), view(df, 1:1, :))
        @test columnindex.(Ref(x), names(df)) == 1:4
        @test columnindex.(Ref(x), propertynames(df)) == 1:4
        @test columnindex(x, :a) == 0
        @test columnindex(x, "a") == 0
        @test_throws MethodError columnindex(x, 1)
    end

    for x in (view(df, 1, 4:3), view(df, 1:1, 4:3))
        @test all(==(0), columnindex.(Ref(x), [names(df); "a"]))
        @test all(==(0), columnindex.(Ref(x), [propertynames(df); :a]))
    end
    for x in (view(df, 1, [4, 3]), view(df, 1:1, [4, 3]))
        @test columnindex.(Ref(x), [names(df); "a"]) == [0, 0, 2, 1, 0]
        @test columnindex.(Ref(x), [propertynames(df); :a]) == [0, 0, 2, 1, 0]
    end
end

@testset "eachrow and eachcol integration" begin
    df = DataFrame(rand(3, 4), [:a, :b, :c, :d])

    df2 = DataFrame(eachrow(df))
    @test df == df2
    @test !any(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(eachrow(df), copycols=false)
    @test df == df2
    @test all(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(pairs(eachcol(df)))
    @test df == df2
    @test !any(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(pairs(eachcol(df)), copycols=false)
    @test df == df2
    @test all(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(eachcol(df))
    @test df == df2
    @test all(((a, b),) -> a == b, zip(eachcol(df), eachcol(df2)))
    @test !any(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

    df2 = DataFrame(eachcol(df))
    @test df == df2
    @test !any(((a, b),) -> a === b, zip(eachcol(df), eachcol(df2)))

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

@testset "test constructor with vectors" begin
    @test DataFrame(Any[]) == DataFrame()
    df = DataFrame(typeof((1, 1))[])
    @test names(df) == ["Column1", "Column2"]
    @test size(df) == (0, 2)
    @test eltype(df.Column1) == Int
    df = DataFrame(typeof((a=1, b=1))[])
    @test names(df) == ["a", "b"]
    @test size(df) == (0, 2)
    @test eltype(df.a) == Int
    @test DataFrame(Vector[], :auto) == DataFrame()
    @test DataFrame(Pair{Symbol, Vector}[], :auto) == DataFrame()
    @test DataFrame(Pair[]) == DataFrame()
    @test DataFrame([[1]], :auto) == DataFrame(x1=1)
    @test DataFrame(Any[[1]], :auto) == DataFrame(x1=1)
    @test DataFrame([:a => [1]]) == DataFrame(a=1)
    @test DataFrame(Any[:a => [1]]) == DataFrame(a=1)
    @test DataFrame(["a" => [1]]) == DataFrame(a=1)
    @test DataFrame(Any["a" => [1]]) == DataFrame(a=1)
    @test DataFrame([SubString("a", 1) => [1]]) == DataFrame(a=1)
    @test DataFrame(Any[SubString("a", 1) => [1]]) == DataFrame(a=1)
end

@testset "materializer" begin
    df = DataFrame(a=1)
    sdf1 = view(df, :, :)
    sdf2 = view(df, 1:1, 1:1)
    ec = eachcol(df)
    er = eachrow(df)

    for x in (df, sdf1, sdf2, ec, er)
        @test DataFrame === @inferred Tables.materializer(x)
        @test DataFrame === @inferred Tables.materializer(typeof(x))
    end

    @test DataFrame === @inferred Tables.materializer(AbstractDataFrame)
    @test DataFrame === @inferred Tables.materializer(SubDataFrame)
    @test DataFrame === @inferred Tables.materializer(DataFrames.DataFrameRows)
    @test DataFrame === @inferred Tables.materializer(DataFrames.DataFrameColumns)
end

@testset "Tables.subset" begin
    dfref = DataFrame(a=1:3, b=4:6)

    df = dfref
    res = @inferred Tables.subset(df, :)
    @test res isa DataFrame
    @test res == DataFrame(a=1:3, b=4:6)
    res = Tables.subset(df, :, viewhint=false)
    @test res isa DataFrame
    @test res == DataFrame(a=1:3, b=4:6)
    res = Tables.subset(df, :, viewhint=true)
    @test res isa SubDataFrame
    @test res == DataFrame(a=1:3, b=4:6)

    res = @inferred Tables.subset(df, [3, 1])
    @test res isa DataFrame
    @test res == DataFrame(a=[3, 1], b=[6, 4])
    res = Tables.subset(df, [3, 1], viewhint=false)
    @test res isa DataFrame
    @test res == DataFrame(a=[3, 1], b=[6, 4])
    res = Tables.subset(df, [3, 1], viewhint=true)
    @test res isa SubDataFrame
    @test res == DataFrame(a=[3, 1], b=[6, 4])

    res = @inferred Tables.subset(df, [true, false, true])
    @test res isa DataFrame
    @test res == DataFrame(a=[1, 3], b=[4, 6])
    res = Tables.subset(df, [1, 3], viewhint=false)
    @test res isa DataFrame
    @test res == DataFrame(a=[1, 3], b=[4, 6])
    res = Tables.subset(df, [1, 3], viewhint=true)
    @test res isa SubDataFrame
    @test res == DataFrame(a=[1, 3], b=[4, 6])

    df = eachcol(dfref)
    res = @inferred Tables.subset(df, :)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=1:3, b=4:6))
    res = Tables.subset(df, :, viewhint=false)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=1:3, b=4:6))
    res = Tables.subset(df, :, viewhint=true)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=1:3, b=4:6))

    res = @inferred Tables.subset(df, [3, 1])
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[3, 1], b=[6, 4]))
    res = Tables.subset(df, [3, 1], viewhint=false)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[3, 1], b=[6, 4]))
    res = Tables.subset(df, [3, 1], viewhint=true)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[3, 1], b=[6, 4]))

    res = @inferred Tables.subset(df, [true, false, true])
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[1, 3], b=[4, 6]))
    res = Tables.subset(df, [1, 3], viewhint=false)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[1, 3], b=[4, 6]))
    res = Tables.subset(df, [1, 3], viewhint=true)
    @test res isa DataFrames.DataFrameColumns
    @test res == eachcol(DataFrame(a=[1, 3], b=[4, 6]))

    df = eachrow(dfref)
    res = @inferred Tables.subset(df, :)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=1:3, b=4:6))
    res = Tables.subset(df, :, viewhint=false)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=1:3, b=4:6))
    res = Tables.subset(df, :, viewhint=true)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=1:3, b=4:6))

    res = @inferred Tables.subset(df, [3, 1])
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[3, 1], b=[6, 4]))
    res = Tables.subset(df, [3, 1], viewhint=false)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[3, 1], b=[6, 4]))
    res = Tables.subset(df, [3, 1], viewhint=true)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[3, 1], b=[6, 4]))

    res = @inferred Tables.subset(df, [true, false, true])
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[1, 3], b=[4, 6]))
    res = Tables.subset(df, [1, 3], viewhint=false)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[1, 3], b=[4, 6]))
    res = Tables.subset(df, [1, 3], viewhint=true)
    @test res isa DataFrames.DataFrameRows
    @test res == eachrow(DataFrame(a=[1, 3], b=[4, 6]))

    for df in (dfref, eachcol(dfref), eachrow(dfref))
        res = @inferred Tables.subset(df, 2)
        @test res isa DataFrameRow
        @test res == DataFrame(a=2, b=5)[1, :]
        res = Tables.subset(df, 2, viewhint=false)
        @test res isa NamedTuple{(:a, :b), Tuple{Int, Int}}
        @test res == (a=2, b=5)
        res = Tables.subset(df, 2, viewhint=true)
        @test res isa DataFrameRow
        @test res == DataFrame(a=2, b=5)[1, :]
    end
end

end # module
