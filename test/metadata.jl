module TestMetadata

using Test, DataFrames, Random, DataAPI

function check_allnotemetadata(x::Union{AbstractDataFrame,
                                        DataFrameRow,
                                        DataFrames.DataFrameRows,
                                        DataFrames.DataFrameColumns,
                                        GroupedDataFrame})
    p = parent(parent(x))
    getfield(p, :allnotemetadata) || return true
    if getfield(p, :metadata) !== nothing
        for (_, style) in values(getfield(p, :metadata))
            style == :note || return false
        end
    end
    if getfield(p, :colmetadata) !== nothing
        for colmeta in values(getfield(p, :colmetadata))
            for (_, style) in values(colmeta)
                style == :note || return false
            end
        end
    end
    return true
end

@testset "metadatasupport" begin
    df = DataFrame(a=1)

    @test DataAPI.metadatasupport(typeof(df)) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(view(df, :, :))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(view(df, :, 1:1))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(view(df, 1, :))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(view(df, 1, 1:1))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(eachrow(df))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(eachrow(view(df, :, :)))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(eachcol(df))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(eachcol(view(df, :, :)))) ==
          (read=true, write=true)
    @test DataAPI.metadatasupport(typeof(groupby(df, 1))) ==
          (read=false, write=false)
    @test DataAPI.metadatasupport(typeof(groupby(view(df, :, :), 1))) ==
          (read=false, write=false)

    @test DataAPI.colmetadatasupport(typeof(df)) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(view(df, :, :))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(view(df, :, 1:1))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(view(df, 1, :))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(view(df, 1, 1:1))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(eachrow(df))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(eachrow(view(df, :, :)))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(eachcol(df))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(eachcol(view(df, :, :)))) ==
          (read=true, write=true)
    @test DataAPI.colmetadatasupport(typeof(groupby(df, 1))) ==
          (read=false, write=false)
    @test DataAPI.colmetadatasupport(typeof(groupby(view(df, :, :), 1))) ==
          (read=false, write=false)
end

@testset "table-level metadata" begin
    for x in (DataFrame(), DataFrame(a=1))
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test check_allnotemetadata(x)
        @test isempty(metadatakeys(x))
        @test metadatakeys(x) isa Tuple
        @test check_allnotemetadata(x)
        metadata!(x, "name", "empty", style=:some)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadatakeys(x) isa Base.KeySet
        @test metadata(x, "name") == "empty"
        @test metadata(x, "name", style=true) == ("empty", :some)
        emptymetadata!(x)
        @test check_allnotemetadata(x)
        @test isempty(metadatakeys(x))
        @test metadatakeys(x) isa Tuple
        metadata!(x, "name1", "empty1", style=:note)
        metadata!(x, "name2", "empty2", style=:default)
        @test check_allnotemetadata(x)
        @test sort(collect(metadatakeys(x))) == ["name1", "name2"]
        deletemetadata!(x, "name2")
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name3")
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name1")
        @test isempty(metadatakeys(x))
        @test check_allnotemetadata(x)
        # this is just a no-op like for dictionaries
        @test deletemetadata!(x, "foobar") === x
    end

    for fun in (eachcol, eachrow,
                x -> x[1, :], x -> @view x[:, :])
        x = fun(DataFrame(a=1))
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test isempty(metadatakeys(x))
        metadata!(x, "name", "empty", style=:note)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test check_allnotemetadata(x)
        emptymetadata!(x)
        @test check_allnotemetadata(x)
        @test isempty(metadatakeys(x))
        metadata!(x, "name1", "empty1", style=:note)
        metadata!(x, "name2", "empty2", style=:note)
        @test check_allnotemetadata(x)
        @test sort(collect(metadatakeys(x))) == ["name1", "name2"]
        deletemetadata!(x, "name2")
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name3")
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name1")
        @test isempty(metadatakeys(x))
        @test check_allnotemetadata(x)
        # this is just a no-op like for dictionaries
        @test deletemetadata!(x, "foobar") === x
    end

    for fun in (eachcol, eachrow)
        x = fun(DataFrame(a=1))
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test isempty(metadatakeys(x))
        metadata!(x, "name", "empty", style=:default)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test metadata(x, "name", style=true) == ("empty", :default)
        emptymetadata!(x)
        @test check_allnotemetadata(x)
        @test isempty(metadatakeys(x))
        metadata!(x, "name1", "empty1", style=:default)
        metadata!(x, "name2", "empty2", style=:note)
        @test check_allnotemetadata(x)
        @test sort(collect(metadatakeys(x))) == ["name1", "name2"]
        deletemetadata!(x, "name2")
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name3")
        @test collect(metadatakeys(x)) == ["name1"]
        deletemetadata!(x, "name1")
        @test isempty(metadatakeys(x))
        @test check_allnotemetadata(x)
        # this is just a no-op like for dictionaries
        @test deletemetadata!(x, "foobar") === x
     end

    for fun in (x -> x[1, :], x -> @view x[:, :])
        x = fun(DataFrame(a=1))
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test check_allnotemetadata(x)
        @test isempty(metadatakeys(x))
        @test_throws ArgumentError metadata!(x, "name", "empty", style=:default)
        metadata!(parent(x), "name", "empty", style=:default)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test metadata(x, "foobar", 12) == 12
        @test metadata(x, "foobar", 12, style=true) == (12, :default)
        @test isempty(metadatakeys(x))
        @test collect(metadatakeys(parent(x))) == ["name"]
        metadata!(parent(x), "name1", "empty1", style=:default)
        metadata!(x, "name2", "empty2", style=:note)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError metadata(x, "foobar")
        @test sort(collect(metadatakeys(x))) == ["name2"]
        @test metadata(x, "name2") == "empty2"
        @test_throws ArgumentError metadata(x, "name1")
        deletemetadata!(x, "name2")
        @test isempty(metadatakeys(x))
        deletemetadata!(x, "name3")
        @test isempty(metadatakeys(x))
        deletemetadata!(x, "name1")
        @test isempty(metadatakeys(x))
        @test sort(collect(metadatakeys(parent(x)))) == ["name", "name1"]
        @test_throws ArgumentError metadata(x, "foobar")
        @test check_allnotemetadata(x)
        # this is just a no-op like for dictionaries
        @test deletemetadata!(x, "foobar") === x
    end

    df = DataFrame(a = 1:2)
    @test check_allnotemetadata(df)
    metadata!(df, "name", "value", style=:default)
    metadata!(df, "name2", "value2", style=:note)
    @test check_allnotemetadata(df)
    dfv = view(df, :, :)
    @test collect(metadatakeys(dfv)) == ["name2"]
    @test_throws ArgumentError metadata!(dfv, "name", "valuex", style=:note)
    metadata!(dfv, "name2", "value2x", style=:note)
    @test metadata(df, "name2") == "value2x"
    @test check_allnotemetadata(df)

    df = DataFrame(a = 1:2)
    @test check_allnotemetadata(df)
    metadata!(df, "name", "value", style=:default)
    metadata!(df, "name2", "value2", style=:note)
    @test check_allnotemetadata(df)
    dfr = df[1, :]
    @test collect(metadatakeys(dfr)) == ["name2"]
    @test_throws ArgumentError metadata!(dfr, "name", "valuex", style=:note)
    metadata!(dfr, "name2", "value2x", style=:note)
    @test metadata(df, "name2") == "value2x"
    @test check_allnotemetadata(df)
end

@testset "column-level metadata" begin
    for b in (:b, "b", 2, big(2)), a in (:a, "a", 1, big(1))
        x = DataFrame(a=1, b=2)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError colmetadata(x, a, "foobar")
        @test_throws BoundsError colmetadata(x, 10, "foobar")
        @test_throws ArgumentError colmetadata(x, "x", "foobar")
        @test colmetadata(x, a, "foobar", 12) == 12
        @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
        @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
        @test isempty(colmetadatakeys(x))
        @test colmetadatakeys(x) isa Tuple
        @test isempty(colmetadatakeys(x, a))
        @test colmetadatakeys(x, a) isa Tuple
        @test_throws ArgumentError colmetadatakeys(x, :c)
        colmetadata!(x, b, "name1", "empty1", style=:note)
        @test check_allnotemetadata(x)
        @test_throws ArgumentError colmetadata(x, a, "foobar")
        @test_throws ArgumentError colmetadata(x, b, "foobar")
        @test_throws ArgumentError colmetadata!(x, :c, "name", "empty", style=:note)
        @test colmetadata(x, a, "foobar", 12) == 12
        @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
        @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
        colmetadata!(x, b, "name2", "empty2", style=:note)
        colmetadata!(x, a, "name3", "empty3", style=:note)
        @test check_allnotemetadata(x)
        @test colmetadatakeys(x, a) == Set(["name3"])
        @test colmetadatakeys(x, b) == Set(["name1", "name2"])
        @test_throws ArgumentError colmetadatakeys(x, :c)
        @test Set(colmetadatakeys(x)) ==
              Set([:b => Set(["name1", "name2"]), :a => Set(["name3"])])
        @test colmetadata(x, b, "name1") == "empty1"
        @test colmetadata(x, b, "name1", style=true) == ("empty1", :note)
        @test_throws ArgumentError colmetadata(x, b, "namex")
        @test_throws ArgumentError colmetadata(x, :x, "name")
        emptycolmetadata!(x, a)
        @test check_allnotemetadata(x)
        @test isempty(colmetadatakeys(x, a))
        @test colmetadatakeys(x, b) == Set(["name1", "name2"])
        deletecolmetadata!(x, b, "name2")
        @test colmetadatakeys(x, b) == Set(["name1"])
        emptycolmetadata!(x)
        @test isempty((colmetadatakeys(x)))
        @test check_allnotemetadata(x)
        # this is just a no-op like for dictionaries
        @test deletecolmetadata!(x, a, "foobar") === x
        @test_throws ArgumentError deletecolmetadata!(x, :invalid, "foobar")

        for fun in (eachcol, eachrow,
                    x -> x[1, :], x -> @view(x[:, :]),
                    x -> x[1, 1:2], x -> @view x[:, 1:2])
            x = fun(DataFrame(a=1, b=2, d=3))
            @test check_allnotemetadata(x)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws BoundsError colmetadata(x, 10, "foobar")
            @test_throws ArgumentError colmetadata(x, "x", "foobar")
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            @test isempty(colmetadatakeys(x))
            @test colmetadatakeys(x) isa Tuple
            @test isempty(colmetadatakeys(x, a))
            @test colmetadatakeys(x, a) isa Tuple
            @test_throws ArgumentError colmetadatakeys(x, :c)
            colmetadata!(x, b, "name1", "empty1", style=:note)
            @test check_allnotemetadata(x)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws ArgumentError colmetadata(x, b, "foobar")
            @test_throws ArgumentError colmetadata!(x, :c, "name", "empty", style=:note)
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            colmetadata!(x, b, "name2", "empty2", style=:note)
            colmetadata!(x, a, "name3", "empty3", style=:note)
            @test check_allnotemetadata(x)
            @test collect(colmetadatakeys(x, a)) == ["name3"]
            @test sort(collect(colmetadatakeys(x, b))) == ["name1", "name2"]
            @test_throws ArgumentError colmetadatakeys(x, :c)
            @test Set([k => sort(collect(v)) for (k, v) in colmetadatakeys(x)]) ==
                Set([:b => ["name1", "name2"], :a => ["name3"]])
            @test colmetadata(x, b, "name1") == "empty1"
            @test colmetadata(x, b, "name1", style=true) == ("empty1", :note)
            @test_throws ArgumentError colmetadata(x, b, "namex")
            @test_throws ArgumentError colmetadata(x, :x, "name")
            emptycolmetadata!(x, a)
            @test check_allnotemetadata(x)
            @test isempty(colmetadatakeys(x, a))
            @test sort(collect(colmetadatakeys(x, b))) == ["name1", "name2"]
            deletecolmetadata!(x, b, "name2")
            @test check_allnotemetadata(x)
            @test collect(colmetadatakeys(x, b)) == ["name1"]
            emptycolmetadata!(x)
            @test isempty((colmetadatakeys(x)))
            @test check_allnotemetadata(x)
            # this is just a no-op like for dictionaries
            @test deletecolmetadata!(x, a, "foobar") === x
            @test_throws ArgumentError deletecolmetadata!(x, :invalid, "foobar")
        end

        for fun in (eachcol, eachrow)
            x = fun(DataFrame(a=1, b=2, d=3))
            @test check_allnotemetadata(x)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws BoundsError colmetadata(x, 10, "foobar")
            @test_throws ArgumentError colmetadata(x, "x", "foobar")
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            @test isempty(colmetadatakeys(x))
            @test colmetadatakeys(x) isa Tuple
            @test isempty(colmetadatakeys(x, a))
            @test colmetadatakeys(x, a) isa Tuple
            @test_throws ArgumentError colmetadatakeys(x, :c)
            colmetadata!(x, b, "name1", "empty1", style=:default)
            @test check_allnotemetadata(x)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws ArgumentError colmetadata(x, b, "foobar")
            @test_throws ArgumentError colmetadata!(x, :c, "name", "empty", style=:default)
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            colmetadata!(x, b, "name2", "empty2", style=:default)
            colmetadata!(x, a, "name3", "empty3", style=:default)
            @test check_allnotemetadata(x)
            @test collect(colmetadatakeys(x, a)) == ["name3"]
            @test sort(collect(colmetadatakeys(x, b))) == ["name1", "name2"]
            @test_throws ArgumentError colmetadatakeys(x, :c)
            @test Set([k => sort(collect(v)) for (k, v) in colmetadatakeys(x)]) ==
                Set([:b => ["name1", "name2"], :a => ["name3"]])
            @test colmetadata(x, b, "name1") == "empty1"
            @test colmetadata(x, b, "name1", style=true) == ("empty1", :default)
            @test_throws ArgumentError colmetadata(x, b, "namex")
            @test_throws ArgumentError colmetadata(x, :x, "name")
            emptycolmetadata!(x, a)
            @test check_allnotemetadata(x)
            @test isempty(colmetadatakeys(x, a))
            @test sort(collect(colmetadatakeys(x, b))) == ["name1", "name2"]
            deletecolmetadata!(x, b, "name2")
            @test check_allnotemetadata(x)
            @test collect(colmetadatakeys(x, b)) == ["name1"]
            emptycolmetadata!(x)
            @test isempty((colmetadatakeys(x)))
            @test check_allnotemetadata(x)
            # this is just a no-op like for dictionaries
            @test deletecolmetadata!(x, a, "foobar") === x
            @test_throws ArgumentError deletecolmetadata!(x, :invalid, "foobar")
        end

        for fun in (x -> x[1, :], x -> @view(x[:, :]),
                    x -> x[1, 1:2], x -> @view x[:, 1:2])
            x = fun(DataFrame(a=1, b=2, d=3))
            @test check_allnotemetadata(x)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws BoundsError colmetadata(x, 10, "foobar")
            @test_throws ArgumentError colmetadata(x, "x", "foobar")
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            p = parent(x)
            @test isempty(colmetadatakeys(x))
            @test colmetadatakeys(x) isa Tuple
            @test isempty(colmetadatakeys(x, a))
            @test colmetadatakeys(x, a) isa Tuple
            @test_throws ArgumentError colmetadatakeys(x, :c)
            @test_throws ArgumentError colmetadata!(x, b, "name1", "empty1", style=:default)
            @test_throws ArgumentError colmetadata(x, a, "foobar")
            @test_throws ArgumentError colmetadata(x, b, "foobar")
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            @test_throws ArgumentError colmetadata!(x, b, "name2", "empty2", style=:default)
            @test_throws ArgumentError colmetadata!(x, a, "name3", "empty3", style=:default)
            colmetadata!(p, b, "name1", "empty1", style=:default)
            colmetadata!(p, b, "name2", "empty2", style=:default)
            colmetadata!(p, a, "name3", "empty3", style=:default)
            @test check_allnotemetadata(x)
            @test isempty(colmetadatakeys(x, a))
            @test isempty(colmetadatakeys(x, b))
            @test_throws ArgumentError colmetadatakeys(x, :c)
            @test isempty(colmetadatakeys(x))
            emptycolmetadata!(x)
            @test check_allnotemetadata(x)
            @test colmetadatakeys(p, a) == Set(["name3"])
            @test colmetadatakeys(p, b) == Set(["name1", "name2"])
            colmetadata!(x, a, "label", "a", style=:note)
            @test check_allnotemetadata(x)
            @test colmetadata(x, a, "label") == "a"
            @test_throws ArgumentError colmetadata(x, a, "name3")
            if !("d" in names(x))
                @test_throws BoundsError colmetadata!(x, "d", "n", "e", style=:note)
            else
                colmetadata!(x, "d", "n", "e", style=:note)
                @test colmetadata(x, "d", "n") == "e"
            end
            @test check_allnotemetadata(x)
            @test colmetadata(x, a, "foobar", 12) == 12
            @test colmetadata(x, a, "foobar", 12, style=true) == (12, :default)
            @test_throws ArgumentError colmetadata(x, "x", "foobar", 12)
            # this is just a no-op like for dictionaries
            @test deletecolmetadata!(x, a, "foobar") === x
            @test_throws ArgumentError deletecolmetadata!(x, :invalid, "foobar")
        end
    end

    df = DataFrame(a = 1:2)
    @test check_allnotemetadata(df)
    colmetadata!(df, 1, "name", "value", style=:default)
    colmetadata!(df, 1, "name2", "value2", style=:note)
    @test check_allnotemetadata(df)
    dfv = view(df, :, :)
    @test collect(colmetadatakeys(dfv, 1)) == ["name2"]
    @test_throws ArgumentError colmetadata!(dfv, 1, "name", "valuex", style=:note)
    colmetadata!(dfv, 1, "name2", "value2x", style=:note)
    @test colmetadata(df, 1, "name2") == "value2x"
    @test check_allnotemetadata(df)

    df = DataFrame(a = 1:2)
    @test check_allnotemetadata(df)
    colmetadata!(df, 1, "name", "value", style=:default)
    colmetadata!(df, 1, "name2", "value2", style=:note)
    @test check_allnotemetadata(df)
    dfr = df[1, :]
    @test collect(colmetadatakeys(dfr, 1)) == ["name2"]
    @test_throws ArgumentError colmetadata!(dfr, 1, "name", "valuex", style=:note)
    colmetadata!(dfr, 1, "name2", "value2x", style=:note)
    @test colmetadata(df, 1, "name2") == "value2x"
    @test check_allnotemetadata(df)
end

@testset "metadata default" begin
    df = DataFrame(a=1)
    metadata!(df, "x", "y")
    colmetadata!(df, :a, "p", "q")
    @test metadata(df, "x", style=true) == ("y", :default)
    @test colmetadata(df, :a, "p", style=true) == ("q", :default)
    df = eachcol(DataFrame(a=1))
    metadata!(df, "x", "y")
    colmetadata!(df, :a, "p", "q")
    @test metadata(df, "x", style=true) == ("y", :default)
    @test colmetadata(df, :a, "p", style=true) == ("q", :default)
    df = view(DataFrame(a=1), :, :)
    @test_throws ArgumentError metadata!(df, "x", "y")
    @test_throws ArgumentError colmetadata!(df, :a, "p", "q")
end

@testset "fallback definitions of metadata and colmetadata" begin
    df = DataFrame()
    @test metadata(df) == Dict()
    @test metadata(df, style=true) == Dict()
    @test colmetadata(df) == Dict()
    @test colmetadata(df, style=true) == Dict()
    df.a = 1:2
    df.b = 2:3
    df.c = 3:4
    @test metadata(df) == Dict()
    @test metadata(df, style=true) == Dict()
    @test colmetadata(df) == Dict()
    @test colmetadata(df, style=true) == Dict()
    metadata!(df, "a1", "b1", style=:default)
    metadata!(df, "a2", "b2", style=:note)
    colmetadata!(df, :a, "x1", "y1", style=:default)
    colmetadata!(df, :a, "x2", "y2", style=:note)
    colmetadata!(df, :c, "x3", "y3", style=:note)
    @test metadata(df) == Dict("a1" => "b1", "a2" => "b2")
    @test metadata(df, style=true) == Dict("a1" => ("b1", :default),
                                           "a2" => ("b2", :note))
    @test colmetadata(df) == Dict(:a => Dict("x1" => "y1",
                                             "x2" => "y2"),
                                  :c => Dict("x3" => "y3"))
    @test colmetadata(df, style=true) == Dict(:a => Dict("x1" => ("y1", :default),
                                                         "x2" => ("y2", :note)),
                                              :c => Dict("x3" => ("y3", :note)))
    @test colmetadata(df, :a) == Dict("x1" => "y1",
                                      "x2" => "y2")
    @test colmetadata(df, :a, style=true) == Dict("x1" => ("y1", :default),
                                                  "x2" => ("y2", :note))
    @test colmetadata(df, :b) == Dict()
    @test colmetadata(df, :b, style=true) == Dict()
    @test colmetadata(df, :c) == Dict("x3" => "y3")
    @test colmetadata(df, :c, style=true) == Dict("x3" => ("y3", :note))
end

@testset "rename & rename!" begin
    df = DataFrame()
    df2 = rename(df)
    @test isempty(metadatakeys(df2))
    @test isempty(colmetadatakeys(df2))
    df2 = rename!(df)
    @test isempty(metadatakeys(df2))
    @test isempty(colmetadatakeys(df2))
    @test check_allnotemetadata(df)
    @test check_allnotemetadata(df2)

    df = DataFrame()
    @test check_allnotemetadata(df)
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "drop", "value", style=:default)
    @test check_allnotemetadata(df)
    df2 = rename(df)
    @test check_allnotemetadata(df2)
    @test metadatakeys(df2) == Set(["name"])
    @test metadata(df2, "name") == "empty"
    @test isempty(colmetadatakeys(df2))
    df2 = rename!(df)
    @test check_allnotemetadata(df2)
    @test metadatakeys(df2) == Set(["name"])
    @test metadata(df2, "name") == "empty"
    @test isempty(colmetadatakeys(df2))

    df = DataFrame(a=1, b=2, c=3, d=4, e=5, f=6, g=7, h=8)
    metadata!(df, "name", "empty", style=:note)
    colmetadata!(df, :a, "name", "a", style=:note)
    colmetadata!(df, :b, "name", "b", style=:note)
    colmetadata!(df, :c, "name", "c", style=:note)
    colmetadata!(df, :e, "name", "e", style=:note)
    colmetadata!(df, :g, "name", "g", style=:note)
    metadata!(df, "name2", "empty", style=:default)
    colmetadata!(df, :a, "name2", "a", style=:default)
    colmetadata!(df, :b, "name2", "b", style=:default)
    colmetadata!(df, :c, "name2", "c", style=:default)
    colmetadata!(df, :e, "name2", "e", style=:default)
    colmetadata!(df, :g, "name2", "g", style=:default)
    @test check_allnotemetadata(df)

    # other renaming methods rely on the same mechanism as the one tested below
    # so it is enough to run these tests
    df2 = rename(df, :a => :c, :b => :d, :c => :a, :d => :b, :e => :e1, :h => :h1)
    @test check_allnotemetadata(df2)
    @test metadatakeys(df2) == Set(["name"])
    @test metadata(df2, "name") == "empty"
    @test colmetadatakeys(df2, "c") == Set(["name"])
    @test colmetadata(df2, "c", "name") == "a"
    @test colmetadatakeys(df2, "d") == Set(["name"])
    @test colmetadata(df2, "d", "name") == "b"
    @test colmetadatakeys(df2, "a") == Set(["name"])
    @test colmetadata(df2, "a", "name") == "c"
    @test isempty(colmetadatakeys(df2, "b"))
    @test colmetadatakeys(df2, "e1") == Set(["name"])
    @test colmetadata(df2, "e1", "name") == "e"
    @test isempty(colmetadatakeys(df2, "f"))
    @test colmetadatakeys(df2, "g") == Set(["name"])
    @test colmetadata(df2, "g", "name") == "g"
    @test isempty(colmetadatakeys(df2, "h1"))

    dfv = view(df, 1:1, :)
    rename!(dfv, :a => :c, :b => :d, :c => :a, :d => :b, :e => :e1, :h => :h1)
    @test check_allnotemetadata(df)
    @test metadatakeys(df) == Set(["name"])
    @test metadata(df, "name") == "empty"
    @test colmetadatakeys(df, "c") == Set(["name"])
    @test colmetadata(df, "c", "name") == "a"
    @test colmetadatakeys(df, "d") == Set(["name"])
    @test colmetadata(df, "d", "name") == "b"
    @test colmetadatakeys(df, "a") == Set(["name"])
    @test colmetadata(df, "a", "name") == "c"
    @test isempty(colmetadatakeys(df, "b"))
    @test colmetadatakeys(df, "e1") == Set(["name"])
    @test colmetadata(df, "e1", "name") == "e"
    @test isempty(colmetadatakeys(df, "f"))
    @test colmetadatakeys(df, "g") == Set(["name"])
    @test colmetadata(df, "g", "name") == "g"
    @test isempty(colmetadatakeys(df, "h1"))
end

@testset "similar, empty, empty!" begin
    for fun in (x -> similar(x, 2), empty, empty!)
        df = DataFrame()
        df2 = fun(df)
        @test check_allnotemetadata(df2)
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing

        df = DataFrame(a=1, b=2)
        df2 = fun(df)
        @test check_allnotemetadata(df2)
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing
    end

    df = DataFrame(a=1, b=2)
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name2", "empty2", style=:default)
    colmetadata!(df, :b, "name", "some", style=:note)
    colmetadata!(df, :b, "name2", "some2", style=:default)
    @test check_allnotemetadata(df)

    for fun in (x -> similar(x, 2), empty), x in (df, view(df, :, :))
        df2 = fun(x)
        @test check_allnotemetadata(df2)
        @test metadatakeys(df2) == Set(["name"])
        @test metadata(df2, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(df2, :a))
        @test colmetadatakeys(df2, :b) == Set(["name"])
        @test colmetadata(df2, :b, "name", style=true) == ("some", :note)
    end

    empty!(df)
    @test check_allnotemetadata(df)
    @test metadatakeys(df) == Set(["name"])
    @test metadata(df, "name", style=true) == ("empty", :note)
    @test isempty(colmetadatakeys(df, :a))
    @test colmetadatakeys(df, :b) == Set(["name"])
    @test colmetadata(df, :b, "name", style=true) == ("some", :note)
end

@testset "only, first, last" begin
    for fun in (only, first, last,
                x -> first(x, 1, view=true),
                x -> last(x, 1, view=true))
        df = DataFrame(a=1, b=2)
        x = fun(df)
        @test getfield(parent(x), :metadata) === nothing
        @test getfield(parent(x), :colmetadata) === nothing
        @test check_allnotemetadata(x)

        df = DataFrame(a=1, b=2)
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :b, "name", "some", style=:note)
        colmetadata!(df, :b, "name2", "some2", style=:default)
        @test check_allnotemetadata(df)

        x = fun(df)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(x, :a))
        @test collect(colmetadatakeys(x, :b)) == ["name"]
        @test colmetadata(x, :b, "name", style=true) == ("some", :note)
        @test check_allnotemetadata(x)
    end

    for fun in (x -> first(x, 1),
                x -> last(x, 1))
        df = DataFrame(a=1, b=2)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test getfield(x, :metadata) === nothing
        @test getfield(x, :colmetadata) === nothing
        @test check_allnotemetadata(x)

        df = DataFrame(a=1, b=2)
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :b, "name", "some", style=:note)
        colmetadata!(df, :b, "name2", "some2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(x, :a))
        @test collect(colmetadatakeys(x, :b)) == ["name"]
        @test colmetadata(x, :b, "name", style=true) == ("some", :note)
    end
end

@testset "describe" begin
    df = DataFrame()
    @test check_allnotemetadata(df)
    x = describe(df)
    @test getfield(x, :metadata) === nothing
    @test getfield(x, :colmetadata) === nothing
    @test check_allnotemetadata(x)

    df = DataFrame(a=1, b="x")
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name2", "empty2", style=:default)
    colmetadata!(df, :b, "name", "some", style=:note)
    colmetadata!(df, :b, "name2", "some2", style=:default)
    @test check_allnotemetadata(df)
    x = describe(df)
    @test getfield(x, :metadata) === nothing
    @test getfield(x, :colmetadata) === nothing
    @test check_allnotemetadata(x)
end

@testset "functions that keep all metadata" begin
    df = DataFrame(a=1, b="x")
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name2", "empty2", style=:default)
    colmetadata!(df, :b, "name", "some", style=:note)
    colmetadata!(df, :b, "name2", "some2", style=:default)
    @test check_allnotemetadata(df)

    for fun in (copy,
                DataFrame,
                x -> DataFrame(eachrow(x)),
                x -> DataFrame(eachcol(x)),
                x -> DataFrame(x, copycols=false))
        df2 = fun(df)
        @test getfield(df, :metadata) == getfield(df2, :metadata)
        @test getfield(df, :colmetadata) == getfield(df2, :colmetadata)
        @test check_allnotemetadata(df2)
    end

    df = DataFrame(a=1, b="x")
    metadata!(df, "name", "empty", style=:note)
    colmetadata!(df, :b, "name", "some", style=:note)
    @test check_allnotemetadata(df)

    for fun in (copy,
                DataFrame,
                x -> DataFrame(eachrow(x)),
                x -> DataFrame(eachcol(x)),
                x -> DataFrame(x, copycols=false))
        df2 = fun(df)
        @test getfield(df, :metadata) == getfield(df2, :metadata)
        @test getfield(df, :colmetadata) == getfield(df2, :colmetadata)
        @test check_allnotemetadata(df2)
    end
end

@testset "functions that keep all :note-style metadata" begin
    # Tested functions:
    #   dropmissing, dropmissing!, filter, filter!, unique, unique!, repeat, repeat!,
    #   disallowmissing, allowmissing, disallowmissing!, allowmissing!, flatten,
    #   reverse, reverse!, permute!, invpermute!, shuffle, shuffle!,
    #   insertcols, insertcols!, mapcols, mapcols!, sort, sort!, subset, subset!,
    #   view, eachrow, eachcol
    #   deleteat!, keepat!, resize!, pop!, popfirst!, popat!
    #   getindex, setindex!, broadcasted assignment

    for fun in (dropmissing,
                x -> dropmissing(x, disallowmissing=false),
                x -> filter(v -> true, x),
                x -> filter(v -> false, x),
                unique,
                x -> repeat(x, 3),
                x -> repeat(x, inner=2, outer=2),
                x -> disallowmissing(x, error=false),
                allowmissing,
                x -> flatten(x, []),
                x -> flatten(x, 1),
                reverse,
                shuffle,
                x -> insertcols(x, :newcol => 1),
                x -> mapcols(v -> copy(v), x),
                sort,
                x -> subset(x, [] => ByRow(() -> true)),
                x -> subset(x, [] => ByRow(() -> false)),
                x -> parent(subset(groupby(x, []), [] => ByRow(() -> true), ungroup=false)),
                x -> parent(subset(groupby(x, []), [] => ByRow(() -> false), ungroup=false)))
        df = DataFrame(a=1)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test getfield(x, :metadata) === nothing
        @test getfield(x, :colmetadata) === nothing
        @test check_allnotemetadata(x)

        df = DataFrame(a=1, b="x")
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :b, "name", "some", style=:note)
        colmetadata!(df, :b, "name2", "some2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(x, :a))
        @test collect(colmetadatakeys(x, :b)) == ["name"]
        @test colmetadata(x, :b, "name", style=true) == ("some", :note)

        df = DataFrame(a=[1, 2], c=[1, 2], b=["x", missing])
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :b, "name", "some", style=:note)
        colmetadata!(df, :b, "name2", "some2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(x, :a))
        @test collect(colmetadatakeys(x, :b)) == ["name"]
        @test colmetadata(x, :b, "name", style=true) == ("some", :note)
        x = fun(view(df, :, 1:1))
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test isempty(colmetadatakeys(x, :a))
        x = fun(view(df, :, 2:3))
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name", style=true) == ("empty", :note)
        @test collect(colmetadatakeys(x, :b)) == ["name"]
        @test colmetadata(x, :b, "name", style=true) == ("some", :note)
    end

    df = DataFrame(a=[[1, 2], [3, 4]], b=["x", "y"])
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name2", "empty2", style=:default)
    colmetadata!(df, :a, "name", "a", style=:note)
    colmetadata!(df, :a, "name2", "a2", style=:default)
    colmetadata!(df, :b, "name", "b", style=:note)
    colmetadata!(df, :b, "name2", "b2", style=:default)
    @test check_allnotemetadata(df)
    x = flatten(df, 1)
    @test check_allnotemetadata(x)
    @test collect(metadatakeys(x)) == ["name"]
    @test metadata(x, "name") == "empty"
    @test collect(colmetadatakeys(x, :a)) == ["name"]
    @test colmetadata(x, :a, "name") == "a"
    @test collect(colmetadatakeys(x, :b)) == ["name"]
    @test colmetadata(x, :b, "name") == "b"
    x = flatten(view(df, 1:2, 1:2), 1)
    @test check_allnotemetadata(x)
    @test collect(metadatakeys(x)) == ["name"]
    @test metadata(x, "name") == "empty"
    @test collect(colmetadatakeys(x, :a)) == ["name"]
    @test colmetadata(x, :a, "name") == "a"
    @test collect(colmetadatakeys(x, :b)) == ["name"]
    @test colmetadata(x, :b, "name") == "b"

    for fun in (dropmissing!,
                x -> dropmissing!(x, disallowmissing=false),
                x -> dropmissing(x, view=true),
                x -> filter!(v -> true, x),
                x -> filter!(v -> false, x),
                x -> filter(v -> true, x, view=true),
                x -> filter(v -> false, x, view=true),
                x -> filter(v -> true, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=true),
                x -> filter(v -> false, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=true),
                unique!,
                x -> unique(x, view=true),
                x -> repeat!(x, 3),
                x -> repeat!(x, inner=2, outer=2),
                x -> disallowmissing!(x, error=false),
                allowmissing!,
                reverse!,
                x -> permute!(x, 1:nrow(x)),
                x -> invpermute!(x, 1:nrow(x)),
                shuffle!,
                x -> insertcols!(x, :newcol => 1, makeunique=true),
                x -> mapcols!(v -> copy(v), x),
                sort!,
                x -> subset!(x, [] => ByRow(() -> true)),
                x -> subset!(x, [] => ByRow(() -> false)),
                x -> subset(x, [] => ByRow(() -> true), view=true),
                x -> subset(x, [] => ByRow(() -> false), view=true),
                x -> parent(subset!(groupby(x, []), [] => ByRow(() -> true), ungroup=false)),
                x -> parent(subset!(groupby(x, []), [] => ByRow(() -> false), ungroup=false)),
                x -> view(x, :, :),
                x -> nrow(x) > 0 ? x[1, :] : view(x, :, :))
        df = DataFrame()
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test getfield(parent(x), :metadata) === nothing
        @test getfield(parent(x), :colmetadata) === nothing
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"

        df = DataFrame(a=1, b="x")
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test collect(colmetadatakeys(x, :a)) == ["name"]
        @test colmetadata(x, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(x, :b)))

        df = DataFrame(a=[1, missing], b=["x", "y"])
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test collect(colmetadatakeys(x, :a)) == ["name"]
        @test colmetadata(x, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(x, :b)))
    end

    for fun in (x -> deleteat!(x, 2),
                x -> keepat!(x, 2),
                x -> resize!(x, 2),
                pop!,
                popfirst!,
                x -> popat!(x, 2))
        df = DataFrame(a=1:3, b=["x", "y", "z"])
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        fun(df)
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["name"]
        @test metadata(df, "name") == "empty"
        @test collect(colmetadatakeys(df, :a)) == ["name"]
        @test colmetadata(df, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(df, :b)))
    end

    for fun in (x -> x[1:2, :],
                x -> x[1:2, 1:2],
                x -> x[:, :],
                x -> x[:, 1:2],
                x -> x[!, :],
                x -> x[!, 1:2])
        df = DataFrame(a=1:3, b=["x", "y", "z"])
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test collect(colmetadatakeys(x, :a)) == ["name"]
        @test colmetadata(x, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(x, :b)))
    end

    for fun in (x -> x[1:2, :],
                x -> x[1:2, 1:1],
                x -> x[:, :],
                x -> x[:, 1:1],
                x -> x[!, :],
                x -> x[!, 1:1])
        df = DataFrame(a=1:3)
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
        @test collect(colmetadatakeys(x, :a)) == ["name"]
        @test colmetadata(x, :a, "name") == "a"
    end

    for fun in (x -> x[1:0, :],
                x -> x[1:0, 1:0],
                x -> x[:, :],
                x -> x[:, 1:0],
                x -> x[!, :],
                x -> x[!, 1:0])
        df = DataFrame()
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        @test check_allnotemetadata(df)
        x = fun(df)
        @test check_allnotemetadata(x)
        @test collect(metadatakeys(x)) == ["name"]
        @test metadata(x, "name") == "empty"
    end

    df = DataFrame(a=1:3, b=["x", "y", "z"])
    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name2", "empty2", style=:default)
    colmetadata!(df, :a, "name", "a", style=:note)
    colmetadata!(df, :a, "name2", "a2", style=:default)
    @test check_allnotemetadata(df)
    df2 = df[:, 2:2]
    @test check_allnotemetadata(df2)
    @test collect(metadatakeys(df2)) == ["name"]
    @test metadata(df2, "name") == "empty"
    @test isempty(collect(colmetadatakeys(df2, :b)))
    df2 = df[!, 2:2]
    @test check_allnotemetadata(df2)
    @test collect(metadatakeys(df2)) == ["name"]
    @test metadata(df2, "name") == "empty"
    @test isempty(collect(colmetadatakeys(df2, :b)))

    for fun in (x -> (x.a = 11:13),
                x -> (x[:, :a] = 11:13),
                x -> (x[!, :a] = 11:13),
                x -> (x.c = 11:13),
                x -> (x[:, :a] .= 11:13),
                x -> (x[!, :a] .= 11:13),
                x -> (x.c = 11:13),
                x -> (x[:, :c] = 11:13),
                x -> (x[!, :c] = 11:13),
                x -> (x[1, :a] = 1),
                x -> (x[:, :] = DataFrame(a=1:3, b=["x", "y", "z"])),
                x -> (x[!, :] = DataFrame(a=1:3, b=["x", "y", "z"])),
                x -> (x[:, :] = [1 "x"; 2 "y"; 3 "z"]),
                x -> (x[!, :] = [1 "x"; 2 "y"; 3 "z"]),
                x -> (x[1:1, :a] .= 1),
                x -> (x[:, :] .= DataFrame(a=1:3, b=["x", "y", "z"])),
                x -> (x[!, :] .= DataFrame(a=1:3, b=["x", "y", "z"])),
                x -> (x[:, :] .= [1 "x"; 2 "y"; 3 "z"]),
                x -> (x[!, :] .= [1 "x"; 2 "y"; 3 "z"]))
        df = DataFrame(a=1:3, b=["x", "y", "z"])
        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        fun(df)
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["name"]
        @test metadata(df, "name") == "empty"
        @test collect(colmetadatakeys(df, :a)) == ["name"]
        @test colmetadata(df, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(df, :b)))

        df = view(DataFrame(a=1:3, b=["x", "y", "z"]), :, :)
        metadata!(df, "name", "empty", style=:note)
        metadata!(parent(df), "name2", "empty2", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(parent(df), :a, "name2", "a2", style=:default)
        @test check_allnotemetadata(df)
        fun(df)
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["name"]
        @test metadata(df, "name") == "empty"
        @test collect(colmetadatakeys(df, :a)) == ["name"]
        @test colmetadata(df, :a, "name") == "a"
        @test isempty(collect(colmetadatakeys(df, :b)))
    end

    # special case due to changes in handling of broadcasting in Julia 1.7
    if VERSION >= v"1.7.0"
        for fun in (x -> (x.a .= 11:13), x -> (x.a .= 1))
            df = DataFrame(a=1:3, b=["x", "y", "z"])
            metadata!(df, "name", "empty", style=:note)
            metadata!(df, "name2", "empty2", style=:default)
            colmetadata!(df, :a, "name", "a", style=:note)
            colmetadata!(df, :a, "name2", "a2", style=:default)
            @test check_allnotemetadata(df)
            fun(df)
            @test check_allnotemetadata(df)
            @test collect(metadatakeys(df)) == ["name"]
            @test metadata(df, "name") == "empty"
            @test collect(colmetadatakeys(df, :a)) == ["name"]
            @test colmetadata(df, :a, "name") == "a"
            @test isempty(collect(colmetadatakeys(df, :b)))

            df = view(DataFrame(a=1:3, b=["x", "y", "z"]), :, :)
            metadata!(df, "name", "empty", style=:note)
            metadata!(parent(df), "name2", "empty2", style=:default)
            colmetadata!(df, :a, "name", "a", style=:note)
            colmetadata!(parent(df), :a, "name2", "a2", style=:default)
            @test check_allnotemetadata(df)
            fun(df)
            @test check_allnotemetadata(df)
            @test collect(metadatakeys(df)) == ["name"]
            @test metadata(df, "name") == "empty"
            @test collect(colmetadatakeys(df, :a)) == ["name"]
            @test colmetadata(df, :a, "name") == "a"
            @test isempty(collect(colmetadatakeys(df, :b)))
        end
    end
end

@testset "fillcombinations" begin
    for df in (DataFrame(x=1:2, y='a':'b', z=["x", "y"]), DataFrame(x=[], y=[], z=[]))
        df2 = fillcombinations(df, [:x, :y])
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing
        @test check_allnotemetadata(df2)

        metadata!(df, "name", "empty", style=:note)
        metadata!(parent(df), "name2", "empty2", style=:default)
        colmetadata!(df, :y, "name", "y", style=:note)
        colmetadata!(parent(df), :y, "name2", "y2", style=:default)
        @test check_allnotemetadata(df)
        df2 = fillcombinations(df, [:x, :y])
        @test check_allnotemetadata(df2)
        @test collect(metadatakeys(df2)) == ["name"]
        @test metadata(df2, "name") == "empty"
        @test collect(colmetadatakeys(df2, :y)) == ["name"]
        @test colmetadata(df2, :y, "name") == "y"
        @test isempty(collect(colmetadatakeys(df2, :x)))
        @test isempty(collect(colmetadatakeys(df2, :z)))
    end
end

@testset "hcat" begin
    df1 = DataFrame(a=1:3, b=11:13)
    df2 = DataFrame(c=111:113, d=1111:1113)

    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    res = hcat(df1)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "name", "some", style=:note)
    metadata!(df2, "name2", "some2", style=:default)
    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df1,"type", "other", style=:note)
    metadata!(df1,"type2", "other2", style=:default)
    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    res = hcat(df1)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["type"]
    @test metadata(res, "type") == "other"
    @test getfield(res, :colmetadata) === nothing

    metadata!(df1, "name", "some", style=:default)
    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df1, "name", "some", style=:note)
    metadata!(df1, "name2", "some2", style=:note)
    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "some"
    @test getfield(res, :colmetadata) === nothing

    colmetadata!(df1, :b, "m1", "val1", style=:note)
    colmetadata!(df2, :d, "m2", "val2", style=:note)
    colmetadata!(df1, :a, "n1", "val1", style=:default)
    colmetadata!(df2, :c, "n2", "val2", style=:default)
    res = hcat(df1, df2)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "some"
    @test isempty(colmetadatakeys(res, :a))
    @test collect(colmetadatakeys(res, :b)) == ["m1"]
    @test colmetadata(res, :b, "m1") == "val1"
    @test isempty(colmetadatakeys(res, :c))
    @test collect(colmetadatakeys(res, :d)) == ["m2"]
    @test colmetadata(res, :d, "m2") == "val2"

    res = hcat(df1)
    @test check_allnotemetadata(res)
    @test sort(collect(metadatakeys(res))) == ["name", "name2", "type"]
    @test metadata(res, "name") == "some"
    @test metadata(res, "name2") == "some2"
    @test metadata(res, "type") == "other"
    @test isempty(colmetadatakeys(res, :a))
    @test collect(colmetadatakeys(res, :b)) == ["m1"]
    @test colmetadata(res, :b, "m1") == "val1"

    res = hcat(df1, df1, df1, makeunique=true)
    @test check_allnotemetadata(res)
    @test sort(collect(metadatakeys(res))) == ["name", "name2", "type"]
    @test metadata(res, "name") == "some"
    @test metadata(res, "name2") == "some2"
    @test metadata(res, "type") == "other"
    @test isempty(colmetadatakeys(res, :a))
    @test isempty(colmetadatakeys(res, :a_1))
    @test isempty(colmetadatakeys(res, :a_2))
    @test collect(colmetadatakeys(res, :b)) == ["m1"]
    @test colmetadata(res, :b, "m1") == "val1"
    @test collect(colmetadatakeys(res, :b_1)) == ["m1"]
    @test colmetadata(res, :b_1, "m1") == "val1"
    @test collect(colmetadatakeys(res, :b_2)) == ["m1"]
    @test colmetadata(res, :b_2, "m1") == "val1"
end

@testset "vcat" begin
    df1 = DataFrame(a=1)
    df2 = DataFrame(b=2)
    df3 = DataFrame(a=11, c=3)
    df4 = DataFrame(a=111, c=33, d=4)

    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df1, "a", 1, style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(DataFrame(), df1, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "a", 1, style=:note)
    metadata!(df3, "a", 1, style=:note)
    metadata!(df4, "a", 2, style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df4, "a", 1, style=:default)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df4, "a", 1, style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing

    colmetadata!(df1, :a, "x", "y", style=:note)
    colmetadata!(df1, :a, "x1", "y1", style=:default)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing

    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing

    res = vcat(df1)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"

    res = vcat(df1, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"

    res = vcat(df1, cols=[:c, :b, :a, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"

    colmetadata!(df3, :a, "x", "y", style=:note)
    colmetadata!(df3, :a, "x1", "y1", style=:default)
    colmetadata!(df4, :a, "x", "y", style=:default)
    colmetadata!(df4, :a, "x1", "y1", style=:default)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing

    colmetadata!(df4, :a, "x", "yz", style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing

    colmetadata!(df4, :a, "x", "y", style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"
    @test isempty(colmetadatakeys(res, :b))
    @test isempty(colmetadatakeys(res, :c))
    @test isempty(colmetadatakeys(res, :d))
    @test isempty(colmetadatakeys(res, :e))

    colmetadata!(df4, :c, "a", "b", style=:note)
    colmetadata!(df4, :d, "p", "q", style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"
    @test isempty(colmetadatakeys(res, :b))
    @test isempty(colmetadatakeys(res, :c))
    @test collect(colmetadatakeys(res, :d)) == ["p"]
    @test colmetadata(res, :d, "p") == "q"
    @test isempty(colmetadatakeys(res, :e))

    colmetadata!(df3, :c, "a", "b", style=:note)
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"
    @test isempty(colmetadatakeys(res, :b))
    @test collect(colmetadatakeys(res, :c)) == ["a"]
    @test colmetadata(res, :c, "a") == "b"
    @test collect(colmetadatakeys(res, :d)) == ["p"]
    @test colmetadata(res, :d, "p") == "q"
    @test isempty(colmetadatakeys(res, :e))

    res = vcat(df1, df2, df3, df4, cols=[:b, :a, :e, :c, :d])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"
    @test isempty(colmetadatakeys(res, :b))
    @test collect(colmetadatakeys(res, :c)) == ["a"]
    @test colmetadata(res, :c, "a") == "b"
    @test collect(colmetadatakeys(res, :d)) == ["p"]
    @test colmetadata(res, :d, "p") == "q"
    @test isempty(colmetadatakeys(res, :e))

    res = vcat(df1, df2, df3, df4, cols=:union)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test collect(colmetadatakeys(res, :a)) == ["x"]
    @test colmetadata(res, :a, "x") == "y"
    @test isempty(colmetadatakeys(res, :b))
    @test collect(colmetadatakeys(res, :c)) == ["a"]
    @test colmetadata(res, :c, "a") == "b"
    @test collect(colmetadatakeys(res, :d)) == ["p"]
    @test colmetadata(res, :d, "p") == "q"

    res = vcat(df1, df2, df3, df4, cols=:intersect)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["a"]
    @test metadata(res, "a") == 1
    @test getfield(res, :colmetadata) === nothing
end

@testset "stack" begin
    df = DataFrame(a=repeat(1:3, inner=2),
                   b=repeat(1:2, inner=3),
                   c=repeat(1:1, inner=6),
                   d=repeat(1:6, inner=1),
                   e=string.('a':'f'))
    res = stack(df, [:c, :d])
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = stack(df, [:c, :d], view=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df, "name", "empty", style=:note)
    metadata!(df, "name1", "empty1", style=:default)
    res = stack(df, [:c, :d])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test getfield(res, :colmetadata) === nothing
    res = stack(df, [:c, :d], view=true)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test getfield(res, :colmetadata) === nothing

    colmetadata!(df, :e, "name", "e", style=:note)
    colmetadata!(df, :d, "name", "d", style=:note)
    colmetadata!(df, :e, "name1", "e1", style=:default)
    colmetadata!(df, :d, "name1", "d1", style=:default)
    res = stack(df, [:c, :d])
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test isempty(colmetadatakeys(res, :a))
    @test isempty(colmetadatakeys(res, :b))
    @test collect(colmetadatakeys(res, :e)) == ["name"]
    @test colmetadata(res, :e, "name") == "e"
    @test isempty(colmetadatakeys(res, :variable))
    @test isempty(colmetadatakeys(res, :value))

    res = stack(df, [:c, :d], view=true)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test isempty(colmetadatakeys(res, :a))
    @test isempty(colmetadatakeys(res, :b))
    @test collect(colmetadatakeys(res, :e)) == ["name"]
    @test colmetadata(res, :e, "name") == "e"
    @test isempty(colmetadatakeys(res, :variable))
    @test isempty(colmetadatakeys(res, :value))
end

@testset "unstack" begin
    wide = DataFrame(id=1:6,
                     a=repeat(1:3, inner=2),
                     b=repeat(1.0:2.0, inner=3),
                     c=repeat(1.0:1.0, inner=6),
                     d=repeat(1.0:3.0, inner=2))
    long = stack(wide)

    res = unstack(long)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :id, :variable, :value)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :a, :variable, :value, combine=copy)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(long, "name", "empty", style=:note)
    metadata!(long, "name1", "empty1", style=:default)
    colmetadata!(long, :a, "name", "a", style=:note)
    colmetadata!(long, :a, "name1", "a1", style=:default)
    colmetadata!(long, :variable, "var", "var", style=:note)
    colmetadata!(long, :variable, "var1", "var1", style=:default)
    colmetadata!(long, :value, "val", "val", style=:note)
    colmetadata!(long, :value, "val1", "val1", style=:default)

    res = unstack(long)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test isempty(colmetadatakeys(res, :id))
    @test collect(colmetadatakeys(res, :a)) == ["name"]
    @test colmetadata(res, :a, "name") == "a"
    @test isempty(colmetadatakeys(res, :b))
    @test isempty(colmetadatakeys(res, :c))
    @test isempty(colmetadatakeys(res, :d))

    res = unstack(long, :id, :variable, :value)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test isempty(colmetadatakeys(res, :id))
    @test isempty(colmetadatakeys(res, :b))
    @test isempty(colmetadatakeys(res, :c))
    @test isempty(colmetadatakeys(res, :d))

    res = unstack(long, :a, :variable, :value, combine=copy)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "empty"
    @test collect(colmetadatakeys(res, :a)) == ["name"]
    @test colmetadata(res, :a, "name") == "a"
    @test isempty(colmetadatakeys(res, :b))
    @test isempty(colmetadatakeys(res, :c))
    @test isempty(colmetadatakeys(res, :d))
end

@testset "permutedims" begin
    for fun in (x -> permutedims(x, 1), permutedims, x -> permutedims(x, [:a, :b]))
        df = DataFrame(a=["x", "y"], b=[1.0, 2.0], c=[3, 4], d=[true, false])
        res = fun(df)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df, "name", "empty", style=:note)
        metadata!(df, "name1", "empty1", style=:default)
        colmetadata!(df, :a, "name", "a", style=:note)
        colmetadata!(df, :a, "name1", "a1", style=:default)
        colmetadata!(df, :b, "name", "b", style=:note)
        colmetadata!(df, :b, "name1", "b1", style=:default)
        colmetadata!(df, :c, "name", "c", style=:note)
        colmetadata!(df, :c, "name1", "c1", style=:default)
        colmetadata!(df, :d, "name", "d", style=:note)
        colmetadata!(df, :d, "name1", "d1", style=:default)
        res = permutedims(df, 1)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["name"]
        @test metadata(res, "name") == "empty"
        @test getfield(res, :colmetadata) === nothing
    end
end

@testset "broadcasting" begin
    df1 = DataFrame(a=1:3, b=11:13)
    df2 = DataFrame(a=-1:-1:-3, b=-11:-1:-13)

    res = log.(df1)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df1, "name", "some", style=:note)
    metadata!(df1, "name1", "some1", style=:default)
    colmetadata!(df1, :b, "x", "y", style=:note)
    colmetadata!(df1, :b, "x1", "y1", style=:default)

    res = log.(df1)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "some"
    @test isempty(colmetadatakeys(res, :a))
    @test collect(colmetadatakeys(res, :b)) == ["x"]
    @test colmetadata(res, :b, "x") == "y"

    res = log.(df1) .+ df2
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "name", "somex", style=:note)
    colmetadata!(df2, :b, "x", "yx", style=:note)
    res = log.(df1) .+ df2
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "name", "some", style=:default)
    colmetadata!(df2, :b, "x", "y", style=:default)
    res = log.(df1) .+ df2
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "name", "some", style=:note)
    colmetadata!(df2, :b, "x", "y", style=:note)
    res = log.(df1) .+ df2
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "some"
    @test isempty(colmetadatakeys(res, :a))
    @test collect(colmetadatakeys(res, :b)) == ["x"]
    @test colmetadata(res, :b, "x") == "y"

    metadata!(df1, "caption", "other", style=:note)
    res = log.(df1) .+ df2
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["name"]
    @test metadata(res, "name") == "some"
    @test isempty(colmetadatakeys(res, :a))
    @test collect(colmetadatakeys(res, :b)) == ["x"]
    @test colmetadata(res, :b, "x") == "y"

    # complex case
    df1 = DataFrame(a=1, b=2, c=3, d=4)
    df2 = DataFrame(a=1, b=2, c=3, d=4)
    df3 = DataFrame(a=1, b=2, c=3, d=4)
    colmetadata!(df1, :a, "label", "a", style=:note)
    colmetadata!(df2, :a, "label", "a", style=:note)
    colmetadata!(df3, :a, "label", "a", style=:note)
    colmetadata!(df1, :b, "label", "b", style=:note)
    colmetadata!(df2, :b, "label", "b", style=:note)
    colmetadata!(df3, :b, "label", "x", style=:note)
    colmetadata!(df1, :c, "label", "c", style=:note)
    colmetadata!(df2, :c, "label", "c", style=:note)
    colmetadata!(df3, :c, "labelx", "c", style=:note)
    colmetadata!(df1, :d, "label", "b", style=:note)
    colmetadata!(df2, :d, "label", "b", style=:note)
    colmetadata!(df3, :d, "label", "b", style=:default)
    df4 = df1 .+ df2 .+ df3
    @test check_allnotemetadata(df4)
    @test isempty(metadatakeys(df4))
    @test collect(colmetadatakeys(df4, :a)) == ["label"]
    @test colmetadata(df4, :a, "label") == "a"
    @test isempty(colmetadatakeys(df4, :b))
    @test isempty(colmetadatakeys(df4, :c))
    @test isempty(colmetadatakeys(df4, :d))
end

@testset "push!, pushfirst!, insert!" begin
    for fun in ((x, y) -> push!(x, y, cols=:union),
                (x, y) -> pushfirst!(x, y, cols=:union),
                (x, y) -> insert!(x, 2, y, cols=:union))
        df = DataFrame(a=1:3, b=2:4)
        fun(df, (a=10, b=20))
        @test check_allnotemetadata(df)
        @test getfield(df, :metadata) === nothing
        @test getfield(df, :colmetadata) === nothing

        df2 = DataFrame(a=1:3, b=2:4, c=1:3)
        metadata!(df2, "caption", "other2", style=:note)
        colmetadata!(df2, :c, "name", "c", style=:note)
        metadata!(df2, "caption1", "other3", style=:default)
        colmetadata!(df2, :c, "name1", "c2", style=:default)
        dfr = df2[1, :]

        fun(df, dfr)
        @test check_allnotemetadata(df)
        @test getfield(df, :metadata) === nothing
        @test getfield(df, :colmetadata) === nothing

        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, dfr)
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
    end
end

@testset "append!, prepend!" begin
    for fun in ((x, y) -> append!(x, y, cols=:union),
                (x, y) -> prepend!(x, y, cols=:union))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, b=2:4, c=1:3)

        fun(df, df2)
        @test check_allnotemetadata(df)
        @test getfield(df, :metadata) === nothing
        @test getfield(df, :colmetadata) === nothing

        metadata!(df2, "caption", "other2", style=:note)
        colmetadata!(df2, :a, "name", "a", style=:note)
        colmetadata!(df2, :c, "name", "c", style=:note)
        metadata!(df2, "caption1", "other3", style=:default)
        colmetadata!(df2, :c, "name1", "c2", style=:default)
        df = DataFrame(a=1:3, b=2:4)
        fun(df, df2)
        @test check_allnotemetadata(df)
        @test getfield(df, :metadata) === nothing
        @test isempty(colmetadatakeys(df, :a))
        @test isempty(colmetadatakeys(df, :b))
        @test collect(colmetadatakeys(df, :c)) == ["name"]
        @test colmetadata(df, :c, "name") == "c"

        df = DataFrame(a=1:3, b=2:4)
        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, df2)
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
        @test collect(colmetadatakeys(df, :c)) == ["name"]
        @test colmetadata(df, :c, "name") == "c"

        df = DataFrame(a=1:3, b=2:4)
        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, eachrow(df2))
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
        @test collect(colmetadatakeys(df, :c)) == ["name"]
        @test colmetadata(df, :c, "name") == "c"

        df = DataFrame(a=1:3, b=2:4)
        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, eachcol(df2))
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
        @test collect(colmetadatakeys(df, :c)) == ["name"]
        @test colmetadata(df, :c, "name") == "c"

        df = DataFrame(a=1:3, b=2:4)
        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, Tables.rowtable(df2))
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
        @test isempty(colmetadatakeys(df, :c))

        df = DataFrame(a=1:3, b=2:4)
        metadata!(df, "caption", "some", style=:note)
        colmetadata!(df, :b, "name", "b", style=:note)
        metadata!(df, "caption1", "some1", style=:default)
        colmetadata!(df, :b, "name1", "b2", style=:default)
        fun(df, Tables.columntable(df2))
        @test check_allnotemetadata(df)
        @test collect(metadatakeys(df)) == ["caption"]
        @test metadata(df, "caption") == "some"
        @test isempty(colmetadatakeys(df, :a))
        @test collect(colmetadatakeys(df, :b)) == ["name"]
        @test colmetadata(df, :b, "name") == "b"
        @test isempty(colmetadatakeys(df, :c))
    end
end

@testset "leftjoin!" begin
    df = DataFrame(a=1:3, b=2:4)
    df2 = DataFrame(a=1:3, c=1:3)

    leftjoin!(df, df2, on=:a)
    @test check_allnotemetadata(df)
    @test getfield(df, :metadata) === nothing
    @test getfield(df, :colmetadata) === nothing

    metadata!(df2, "caption", "some2", style=:note)
    colmetadata!(df2, :a, "name", "a2", style=:note)
    colmetadata!(df2, :c, "name", "c2", style=:note)
    metadata!(df2, "caption1", "some2x", style=:default)
    colmetadata!(df2, :a, "name1", "a2x", style=:default)
    colmetadata!(df2, :c, "name1", "c2x", style=:default)
    df = DataFrame(a=1:3, b=2:4)
    leftjoin!(df, df2, on=:a)
    @test check_allnotemetadata(df)
    @test getfield(df, :metadata) === nothing
    @test isempty(colmetadatakeys(df, :a))
    @test isempty(colmetadatakeys(df, :b))
    @test collect(colmetadatakeys(df, :c)) == ["name"]
    @test colmetadata(df, :c, "name") == "c2"

    df = DataFrame(a=1:3, b=2:4)
    metadata!(df, "caption", "some1", style=:note)
    colmetadata!(df, :a, "name", "a1", style=:note)
    colmetadata!(df, :b, "name", "b1", style=:note)
    metadata!(df, "caption1", "some1x", style=:default)
    colmetadata!(df, :a, "name1", "a1x", style=:default)
    colmetadata!(df, :b, "name1", "b1x", style=:default)
    leftjoin!(df, df2, on=:a)
    @test check_allnotemetadata(df)
    @test collect(metadatakeys(df)) == ["caption"]
    @test metadata(df, "caption") == "some1"
    @test collect(colmetadatakeys(df, :a)) == ["name"]
    @test colmetadata(df, :a, "name") == "a1"
    @test collect(colmetadatakeys(df, :b)) == ["name"]
    @test colmetadata(df, :b, "name") == "b1"
    @test collect(colmetadatakeys(df, :c)) == ["name"]
    @test colmetadata(df, :c, "name") == "c2"

    df = DataFrame(a=1:3, b=2:4)
    df2 = DataFrame(a=1:3, b=1:3)

    leftjoin!(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(df)
    @test getfield(df, :metadata) === nothing
    @test getfield(df, :colmetadata) === nothing

    metadata!(df2, "caption", "some2", style=:note)
    colmetadata!(df2, :a, "name", "a2", style=:note)
    colmetadata!(df2, :b, "name", "c2", style=:note)
    metadata!(df2, "caption1", "some2x", style=:default)
    colmetadata!(df2, :a, "name1", "a2x", style=:default)
    colmetadata!(df2, :b, "name1", "c2x", style=:default)
    df = DataFrame(a=1:3, b=2:4)
    leftjoin!(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(df)
    @test getfield(df, :metadata) === nothing
    @test isempty(colmetadatakeys(df, :a))
    @test isempty(colmetadatakeys(df, :b))
    @test collect(colmetadatakeys(df, :b_1)) == ["name"]
    @test colmetadata(df, :b_1, "name") == "c2"

    df = DataFrame(a=1:3, b=2:4)
    metadata!(df, "caption", "some1", style=:note)
    colmetadata!(df, :a, "name", "a1", style=:note)
    colmetadata!(df, :b, "name", "b1", style=:note)
    metadata!(df, "caption1", "some1x", style=:default)
    colmetadata!(df, :a, "name1", "a1x", style=:default)
    colmetadata!(df, :b, "name1", "b1x", style=:default)
    leftjoin!(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(df)
    @test collect(metadatakeys(df)) == ["caption"]
    @test metadata(df, "caption") == "some1"
    @test collect(colmetadatakeys(df, :a)) == ["name"]
    @test colmetadata(df, :a, "name") == "a1"
    @test collect(colmetadatakeys(df, :b)) == ["name"]
    @test colmetadata(df, :b, "name") == "b1"
    @test collect(colmetadatakeys(df, :b_1)) == ["name"]
    @test colmetadata(df, :b_1, "name") == "c2"
end

@testset "leftjoin" begin
    for fun in ((x, y) -> leftjoin(x, y, on=:a),
                (x, y) -> leftjoin(x, y, on=:a, renamecols = "_left" => "_right"))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, c=1:3)

        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df2, "caption", "some2", style=:note)
        colmetadata!(df2, :a, "name", "a2", style=:note)
        colmetadata!(df2, :c, "name", "c2", style=:note)
        metadata!(df2, "caption1", "some2x", style=:default)
        colmetadata!(df2, :a, "name1", "a2x", style=:default)
        colmetadata!(df2, :c, "name1", "c2x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test isempty(colmetadatakeys(res, 1))
        @test isempty(colmetadatakeys(res, 2))
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"

        metadata!(df, "caption", "some1", style=:note)
        colmetadata!(df, :a, "name", "a1", style=:note)
        colmetadata!(df, :b, "name", "b1", style=:note)
        metadata!(df, "caption1", "some1x", style=:default)
        colmetadata!(df, :a, "name1", "a1x", style=:default)
        colmetadata!(df, :b, "name1", "b1x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["caption"]
        @test metadata(res, "caption") == "some1"
        @test collect(colmetadatakeys(res, 1)) == ["name"]
        @test colmetadata(res, 1, "name") == "a1"
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"
    end

    df = DataFrame(a=1:3, b=2:4)
    df2 = DataFrame(a=1:3, b=1:3)

    res = leftjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df2, "caption", "some2", style=:note)
    colmetadata!(df2, :a, "name", "a2", style=:note)
    colmetadata!(df2, :b, "name", "c2", style=:note)
    metadata!(df2, "caption1", "some2x", style=:default)
    colmetadata!(df2, :a, "name1", "a2x", style=:default)
    colmetadata!(df2, :b, "name1", "c2x", style=:default)
    res = leftjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test isempty(colmetadatakeys(res, 1))
    @test isempty(colmetadatakeys(res, 2))
    @test collect(colmetadatakeys(res, 3)) == ["name"]
    @test colmetadata(res, 3, "name") == "c2"

    metadata!(df, "caption", "some1", style=:note)
    colmetadata!(df, :a, "name", "a1", style=:note)
    colmetadata!(df, :b, "name", "b1", style=:note)
    metadata!(df, "caption1", "some1x", style=:default)
    colmetadata!(df, :a, "name1", "a1x", style=:default)
    colmetadata!(df, :b, "name1", "b1x", style=:default)
    res = leftjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["caption"]
    @test metadata(res, "caption") == "some1"
    @test collect(colmetadatakeys(res, 1)) == ["name"]
    @test colmetadata(res, 1, "name") == "a1"
    @test collect(colmetadatakeys(res, 2)) == ["name"]
    @test colmetadata(res, 2, "name") == "b1"
    @test collect(colmetadatakeys(res, 3)) == ["name"]
    @test colmetadata(res, 3, "name") == "c2"
end

@testset "rightjoin" begin
    for fun in ((x, y) -> rightjoin(x, y, on=:a),
                (x, y) -> rightjoin(x, y, on=:a, renamecols = "_left" => "_right"))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, c=1:3)

        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df, "caption", "some1", style=:note)
        colmetadata!(df, :a, "name", "a1", style=:note)
        colmetadata!(df, :b, "name", "b1", style=:note)
        metadata!(df, "caption1", "some1x", style=:default)
        colmetadata!(df, :a, "name1", "a1x", style=:default)
        colmetadata!(df, :b, "name1", "b1x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test isempty(colmetadatakeys(res, 1))
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test isempty(colmetadatakeys(res, 3))

        metadata!(df2, "caption", "some2", style=:note)
        colmetadata!(df2, :a, "name", "a2", style=:note)
        colmetadata!(df2, :c, "name", "c2", style=:note)
        metadata!(df2, "caption1", "some2x", style=:default)
        colmetadata!(df2, :a, "name1", "a2x", style=:default)
        colmetadata!(df2, :c, "name1", "c2x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["caption"]
        @test metadata(res, "caption") == "some2"
        @test collect(colmetadatakeys(res, 1)) == ["name"]
        @test colmetadata(res, 1, "name") == "a2"
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"
    end

    df = DataFrame(a=1:3, b=2:4)
    df2 = DataFrame(a=1:3, b=1:3)

    res = rightjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df, "caption", "some1", style=:note)
    colmetadata!(df, :a, "name", "a1", style=:note)
    colmetadata!(df, :b, "name", "b1", style=:note)
    metadata!(df, "caption1", "some1x", style=:default)
    colmetadata!(df, :a, "name1", "a1x", style=:default)
    colmetadata!(df, :b, "name1", "b1x", style=:default)
    res = rightjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test isempty(colmetadatakeys(res, 1))
    @test collect(colmetadatakeys(res, 2)) == ["name"]
    @test colmetadata(res, 2, "name") == "b1"
    @test isempty(colmetadatakeys(res, 3))

    metadata!(df2, "caption", "some2", style=:note)
    colmetadata!(df2, :a, "name", "a2", style=:note)
    colmetadata!(df2, :b, "name", "c2", style=:note)
    metadata!(df2, "caption1", "some2x", style=:default)
    colmetadata!(df2, :a, "name1", "a2x", style=:default)
    colmetadata!(df2, :b, "name1", "c2x", style=:default)
    res = rightjoin(df, df2, on=:a, makeunique=true)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["caption"]
    @test metadata(res, "caption") == "some2"
    @test collect(colmetadatakeys(res, 1)) == ["name"]
    @test colmetadata(res, 1, "name") == "a2"
    @test collect(colmetadatakeys(res, 2)) == ["name"]
    @test colmetadata(res, 2, "name") == "b1"
    @test collect(colmetadatakeys(res, 3)) == ["name"]
    @test colmetadata(res, 3, "name") == "c2"
end

@testset "innerjoin, outerjoin" begin
    for fun in ((x, y) -> innerjoin(x, y, on=:a),
                (x, y) -> innerjoin(x, y, on=:a, renamecols = "_left" => "_right"),
                (x, y) -> outerjoin(x, y, on=:a),
                (x, y) -> outerjoin(x, y, on=:a, renamecols = "_left" => "_right"))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, c=1:3)

        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df, "caption", "some1", style=:note)
        colmetadata!(df, :a, "name", "a1", style=:note)
        colmetadata!(df, :b, "name", "b1", style=:note)
        metadata!(df, "caption1", "some1x", style=:default)
        colmetadata!(df, :a, "name1", "a1x", style=:default)
        colmetadata!(df, :b, "name1", "b1x", style=:default)
        metadata!(df2, "caption", "some2", style=:note)
        colmetadata!(df2, :a, "name", "a2", style=:note)
        colmetadata!(df2, :c, "name", "c2", style=:note)
        metadata!(df2, "caption1", "some2x", style=:default)
        colmetadata!(df2, :a, "name1", "a2x", style=:default)
        colmetadata!(df2, :c, "name1", "c2x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test isempty(colmetadatakeys(res, 1))
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"

        metadata!(df2, "caption", "some1", style=:note)
        colmetadata!(df2, :a, "name", "a1", style=:note)
        metadata!(df2, "caption1", "some1x", style=:default)
        colmetadata!(df2, :a, "name1", "a1x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["caption"]
        @test metadata(res, "caption") == "some1"
        @test collect(colmetadatakeys(res, 1)) == ["name"]
        @test colmetadata(res, 1, "name") == "a1"
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"
    end

    for fun in ((x, y) -> innerjoin(x, y, on=:a, makeunique=true),
                (x, y) -> outerjoin(x, y, on=:a, makeunique=true))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, b=1:3)

        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df, "caption", "some1", style=:note)
        colmetadata!(df, :a, "name", "a1", style=:note)
        colmetadata!(df, :b, "name", "b1", style=:note)
        metadata!(df, "caption1", "some1x", style=:default)
        colmetadata!(df, :a, "name1", "a1x", style=:default)
        colmetadata!(df, :b, "name1", "b1x", style=:default)
        metadata!(df2, "caption", "some2", style=:note)
        colmetadata!(df2, :a, "name", "a2", style=:note)
        colmetadata!(df2, :b, "name", "c2", style=:note)
        metadata!(df2, "caption1", "some2x", style=:default)
        colmetadata!(df2, :a, "name1", "a2x", style=:default)
        colmetadata!(df2, :b, "name1", "c2x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test isempty(colmetadatakeys(res, 1))
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"

        metadata!(df2, "caption", "some1", style=:note)
        colmetadata!(df2, :a, "name", "a1", style=:note)
        metadata!(df2, "caption1", "some1x", style=:default)
        colmetadata!(df2, :a, "name1", "a1x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["caption"]
        @test metadata(res, "caption") == "some1"
        @test collect(colmetadatakeys(res, 1)) == ["name"]
        @test colmetadata(res, 1, "name") == "a1"
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
        @test collect(colmetadatakeys(res, 3)) == ["name"]
        @test colmetadata(res, 3, "name") == "c2"
    end
end

@testset "semijoin, antijoin" begin
    for fun in ((x, y) -> semijoin(x, y, on=:a),
                (x, y) -> antijoin(x, y, on=:a))
        df = DataFrame(a=1:3, b=2:4)
        df2 = DataFrame(a=1:3, c=1:3)

        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test getfield(res, :colmetadata) === nothing

        metadata!(df2, "caption", "some2", style=:note)
        colmetadata!(df2, :a, "name", "a2", style=:note)
        colmetadata!(df2, :c, "name", "c2", style=:note)
        metadata!(df2, "caption1", "some2x", style=:default)
        colmetadata!(df2, :a, "name1", "a2x", style=:default)
        colmetadata!(df2, :c, "name1", "c2x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test getfield(res, :metadata) === nothing
        @test isempty(colmetadatakeys(res, 1))
        @test isempty(colmetadatakeys(res, 2))

        metadata!(df, "caption", "some1", style=:note)
        colmetadata!(df, :a, "name", "a1", style=:note)
        colmetadata!(df, :b, "name", "b1", style=:note)
        metadata!(df, "caption1", "some1x", style=:default)
        colmetadata!(df, :a, "name1", "a1x", style=:default)
        colmetadata!(df, :b, "name1", "b1x", style=:default)
        res = fun(df, df2)
        @test check_allnotemetadata(res)
        @test collect(metadatakeys(res)) == ["caption"]
        @test metadata(res, "caption") == "some1"
        @test collect(colmetadatakeys(res, 1)) == ["name"]
        @test colmetadata(res, 1, "name") == "a1"
        @test collect(colmetadatakeys(res, 2)) == ["name"]
        @test colmetadata(res, 2, "name") == "b1"
    end
end

@testset "crossjoin" begin
    df = DataFrame(a=1:3, b=2:4)
    df2 = DataFrame(a=1:3, c=1:3)

    res = crossjoin(df, df2, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata!(df, "caption", "some1", style=:note)
    colmetadata!(df, :a, "name", "a1", style=:note)
    colmetadata!(df, :b, "name", "b1", style=:note)
    metadata!(df, "caption1", "some1x", style=:default)
    colmetadata!(df, :a, "name1", "a1x", style=:default)
    colmetadata!(df, :b, "name1", "b1x", style=:default)
    metadata!(df2, "caption", "some2", style=:note)
    colmetadata!(df2, :a, "name", "a2", style=:note)
    colmetadata!(df2, :c, "name", "c2", style=:note)
    metadata!(df2, "caption1", "some2x", style=:default)
    colmetadata!(df2, :a, "name1", "a2x", style=:default)
    colmetadata!(df2, :c, "name1", "c2x", style=:default)
    res = crossjoin(df, df2, makeunique=true)
    @test check_allnotemetadata(res)
    @test getfield(res, :metadata) === nothing
    @test collect(colmetadatakeys(res, 1)) == ["name"]
    @test colmetadata(res, 1, "name") == "a1"
    @test collect(colmetadatakeys(res, 2)) == ["name"]
    @test colmetadata(res, 2, "name") == "b1"
    @test collect(colmetadatakeys(res, 3)) == ["name"]
    @test colmetadata(res, 3, "name") == "a2"
    @test collect(colmetadatakeys(res, 4)) == ["name"]
    @test colmetadata(res, 4, "name") == "c2"

    metadata!(df2, "caption", "some1", style=:note)
    colmetadata!(df2, :a, "name", "a1", style=:note)
    metadata!(df2, "caption1", "some1x", style=:default)
    colmetadata!(df2, :a, "name1", "a1x", style=:default)
    res = crossjoin(df, df2, makeunique=true)
    @test check_allnotemetadata(res)
    @test collect(metadatakeys(res)) == ["caption"]
    @test metadata(res, "caption") == "some1"
    @test collect(colmetadatakeys(res, 1)) == ["name"]
    @test colmetadata(res, 1, "name") == "a1"
    @test collect(colmetadatakeys(res, 2)) == ["name"]
    @test colmetadata(res, 2, "name") == "b1"
    @test collect(colmetadatakeys(res, 3)) == ["name"]
    @test colmetadata(res, 3, "name") == "a1"
    @test collect(colmetadatakeys(res, 4)) == ["name"]
    @test colmetadata(res, 4, "name") == "c2"
end

@testset "data frame combine, select, select!, transform, transform!" begin
    refdf = DataFrame(id=[1, 2, 3, 1, 2, 3], a=[1, 2, 3, 1, 2, 3], b=21:26, c=31:36)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    metadata!(refdf, "name_none", "none", style=:default)
    colmetadata!(refdf, :id, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :a, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :b, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :c, "name_none", "id_none", style=:default)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    metadata!(refdf, "name", "refdf", style=:note)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    colmetadata!(refdf, :id, "name", "id", style=:note)
    colmetadata!(refdf, :a, "name", "a", style=:note)
    colmetadata!(refdf, :c, "name", "c", style=:note)

    for fun in (combine, select, select!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => identity => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => copy => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => (x -> x) => :z)
            @test check_allnotemetadata(res)
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => sum => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => sum => :c)
            @test check_allnotemetadata(res)
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => sum)
            @test check_allnotemetadata(res)
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => (x -> x) => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => (x -> x) => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => (x -> x) => :c, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => (x -> x) => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => identity => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => copy => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => (x -> x) => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => identity => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => copy => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => AsTable)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => AsTable)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => [:a])
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => [:a])
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, identity)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(identity, df)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(x -> DataFrame(newcol=1:6), df)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df) do x
                v = DataFrame(newcol=1:6)
                colmetadata!(v, "newcol", "key", "value", style=:note)
            end
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    for fun in (transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => identity => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => copy => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => sum => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => sum => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => (x -> x) => :z)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :z))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :id => (x -> x) => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a => identity => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "a"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a => copy => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "a"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :a => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "a"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => (x -> x) => :c)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :b => (x -> x) => :c, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => (x -> x) => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => identity => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => copy => :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => (x -> x) => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => identity => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, [:c] => copy => :a, :)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => AsTable)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => AsTable)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => identity => [:a])
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, :c => copy => [:a])
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df, identity)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(identity, df)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(x -> DataFrame(id=x.id, a=x.a, b=x.b, c=x.c), df)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(df) do x
                v = DataFrame(id=x.id, a=x.a, b=x.b, c=x.c)
                colmetadata!(v, "a", "key", "value", style=:note)
            end
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end
end

@testset "grouped combine, select, select!, transform, transform!" begin
    refdf = DataFrame(id=[1, 2, 3, 1, 2, 3], a=[1, 2, 3, 1, 2, 3], b=21:26, c=31:36)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id))
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :a)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    metadata!(refdf, "name_none", "none", style=:default)
    colmetadata!(refdf, :id, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :a, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :b, "name_none", "id_none", style=:default)
    colmetadata!(refdf, :c, "name_none", "id_none", style=:default)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id))
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :a)
            @test check_allnotemetadata(res)
            @test getfield(parent(res), :metadata) === nothing
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    metadata!(refdf, "name", "refdf", style=:note)

    for fun in (combine, select, select!, transform, transform!)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id))
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :a)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test getfield(parent(res), :colmetadata) === nothing
        end
    end

    colmetadata!(refdf, :id, "name", "id", style=:note)
    colmetadata!(refdf, :a, "name", "a", style=:note)
    colmetadata!(refdf, :c, "name", "c", style=:note)

    for fun in (combine, select, select!), ug in (true, false)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => identity => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => copy => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => (x -> x) => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :z))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => sum => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => sum => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => (x -> x) => :id, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
        end
        if fun !== select!
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => (x -> x) => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                res = parent(parent(res))
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test getfield(parent(parent(res)), :colmetadata) === nothing
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => identity => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                res = parent(parent(res))
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => copy => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                res = parent(parent(res))
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                res = parent(parent(res))
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
            end
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => identity => :id)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => copy => :id)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => :id)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => (x -> x) => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => (x -> x) => :c, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :c))
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => (x -> x) => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => identity => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => copy => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => (x -> x) => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => identity => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => copy => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => AsTable, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => AsTable, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => [:a], ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => [:a], ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), identity, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(identity, groupby(df, :id), ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(x -> DataFrame(id=x.id, a=x.a, b=x.b, c=x.c), groupby(df, :id), ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), ungroup=ug) do x
                v = DataFrame(id=x.id, a=x.a, b=x.b, c=x.c)
                colmetadata!(v, "a", "key", "value", style=:note)
            end
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
    end

    for fun in (transform, transform!), ug in (true, false)
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => identity => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => copy => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test collect(colmetadatakeys(res, :z)) == ["name"]
            @test colmetadata(res, :z, "name") == "id"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => sum => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => sum => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => (x -> x) => :z, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :z))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :id => (x -> x) => :id, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        if fun !== transform!
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => (x -> x) => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test isempty(colmetadatakeys(res, :id))
                @test collect(colmetadatakeys(res, :a)) == ["name"]
                @test colmetadata(res, :a, "name") == "a"
                @test isempty(colmetadatakeys(res, :b))
                @test collect(colmetadatakeys(res, :c)) == ["name"]
                @test colmetadata(res, :c, "name") == "c"
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => identity => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
                @test collect(colmetadatakeys(res, :a)) == ["name"]
                @test colmetadata(res, :a, "name") == "a"
                @test isempty(colmetadatakeys(res, :b))
                @test collect(colmetadatakeys(res, :c)) == ["name"]
                @test colmetadata(res, :c, "name") == "c"
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => copy => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
                @test collect(colmetadatakeys(res, :a)) == ["name"]
                @test colmetadata(res, :a, "name") == "a"
                @test isempty(colmetadatakeys(res, :b))
                @test collect(colmetadatakeys(res, :c)) == ["name"]
                @test colmetadata(res, :c, "name") == "c"
            end
            for df in (copy(refdf), @view copy(refdf)[:, :])
                res = fun(groupby(df, :id), :a => :id, keepkeys=false)
                @test check_allnotemetadata(res)
                @test collect(metadatakeys(res)) == ["name"]
                @test metadata(res, "name") == "refdf"
                @test collect(colmetadatakeys(res, :id)) == ["name"]
                @test colmetadata(res, :id, "name") == "a"
                @test collect(colmetadatakeys(res, :a)) == ["name"]
                @test colmetadata(res, :a, "name") == "a"
                @test isempty(colmetadatakeys(res, :b))
                @test collect(colmetadatakeys(res, :c)) == ["name"]
                @test colmetadata(res, :c, "name") == "c"
            end
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => identity => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => copy => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            # note that here grouping column metadata is retained
            res = fun(groupby(df, :id), :a => :id)
            @test check_allnotemetadata(res)
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => (x -> x) => :c, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :b => (x -> x) => :c, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => (x -> x) => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => identity => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => copy => :a, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => (x -> x) => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => identity => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), [:c] => copy => :a, :, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "c"
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => AsTable, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => AsTable, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test collect(colmetadatakeys(res, :a)) == ["name"]
            @test colmetadata(res, :a, "name") == "a"
            @test isempty(colmetadatakeys(res, :x1))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => identity => [:a], ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), :c => copy => [:a], ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test collect(colmetadatakeys(res, :c)) == ["name"]
            @test colmetadata(res, :c, "name") == "c"
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :a))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), identity, ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(identity, groupby(df, :id), ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(x -> DataFrame(id=x.id, a=x.a, b=x.b, c=x.c), groupby(df, :id), ungroup=ug)
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
        for df in (copy(refdf), @view copy(refdf)[:, :])
            res = fun(groupby(df, :id), ungroup=ug) do x
                v = DataFrame(id=x.id, a=x.a, b=x.b, c=x.c)
                colmetadata!(v, "a", "key", "value", style=:note)
            end
            @test check_allnotemetadata(res)
            res = parent(parent(res))
            @test collect(metadatakeys(res)) == ["name"]
            @test metadata(res, "name") == "refdf"
            @test collect(colmetadatakeys(res, :id)) == ["name"]
            @test colmetadata(res, :id, "name") == "id"
            @test isempty(colmetadatakeys(res, :a))
            @test isempty(colmetadatakeys(res, :b))
            @test isempty(colmetadatakeys(res, :c))
        end
    end
end

@testset "insertcols! and insertcols" begin
    df = DataFrame(a=1, b=2)
    colmetadata!(df, :a, "x", "y", style=:note)
    colmetadata!(df, :a, "x1", "y1", style=:default)
    colmetadata!(df, :b, "p", "q", style=:note)
    colmetadata!(df, :b, "p1", "q1", style=:default)
    insertcols!(df, 2, :c => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1, :d => 4)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, :e => 5)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1, :f => 1, :g => 2, :h => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 6, :f2 => 1, :g2 => 2, :h2 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, :f3 => 1, :g3 => 2, :h3 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    @test colmetadata(df, :a, "x", style=true) == ("y", :note)
    @test colmetadata(df, :b, "p", style=true) == ("q", :note)

    df = DataFrame(a=1, b=2)
    colmetadata!(df, :a, "x", "y", style=:note)
    colmetadata!(df, :a, "x1", "y1", style=:default)
    colmetadata!(df, :b, "p", "q", style=:note)
    colmetadata!(df, :b, "p1", "q1", style=:default)
    df = view(df, :, :)
    insertcols!(df, 2, :c => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1, :d => 4)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, :e => 5)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 1, :f => 1, :g => 2, :h => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, 6, :f2 => 1, :g2 => 2, :h2 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    insertcols!(df, :f3 => 1, :g3 => 2, :h3 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    @test colmetadata(df, :a, "x", style=true) == ("y", :note)
    @test colmetadata(df, :b, "p", style=true) == ("q", :note)

    df = DataFrame(a=1, b=2)
    df2 = df
    colmetadata!(df, :a, "x", "y", style=:note)
    colmetadata!(df, :a, "x1", "y1", style=:default)
    colmetadata!(df, :b, "p", "q", style=:note)
    colmetadata!(df, :b, "p1", "q1", style=:default)
    df = insertcols(df, 2, :c => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, 1, :d => 4)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, :e => 5)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, 1)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, 1, :f => 1, :g => 2, :h => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, 6, :f2 => 1, :g2 => 2, :h2 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    df = insertcols(df, :f3 => 1, :g3 => 2, :h3 => 3)
    @test sort([k => collect(v) for  (k, v) in colmetadatakeys(df)]) ==
          [:a => ["x"], :b => ["p"]]
    @test colmetadata(df, :a, "x", style=true) == ("y", :note)
    @test colmetadata(df, :b, "p", style=true) == ("q", :note)
    @test sort([k => sort(collect(v)) for  (k, v) in colmetadatakeys(df2)]) ==
          [:a => ["x", "x1"], :b => ["p", "p1"]]
    @test colmetadata(df2, :a, "x", style=true) == ("y", :note)
    @test colmetadata(df2, :a, "x1", style=true) == ("y1", :default)
    @test colmetadata(df2, :b, "p", style=true) == ("q", :note)
    @test colmetadata(df2, :b, "p1", style=true) == ("q1", :default)
end

end # module
