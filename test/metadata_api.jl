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

