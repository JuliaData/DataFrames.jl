module TestReshape

using Test, DataFrames, Random, PooledArrays, CategoricalArrays
const ≅ = isequal

@testset "the output of unstack" begin
    df = DataFrame(Fish = CategoricalArray{Union{String, Missing}}(["Bob", "Bob", "Batman", "Batman"]),
                   Key = CategoricalArray{Union{String, Missing}}(["Mass", "Color", "Mass", "Color"]),
                   Value = Union{String, Missing}["12 g", "Red", "18 g", "Grey"])
    # Check that reordering levels does not confuse unstack
    levels!(df[!, 1], ["XXX", "Bob", "Batman"])
    levels!(df[!, 2], ["YYY", "Color", "Mass"])
    #Unstack specifying a row column
    df2 = unstack(df, :Fish, :Key, :Value)
    @test levels(df[!, 1]) == ["XXX", "Bob", "Batman"] # make sure we did not mess df[!, 1] levels
    @test levels(df[!, 2]) == ["YYY", "Color", "Mass"] # make sure we did not mess df[!, 2] levels
    #Unstack without specifying a row column
    df3 = unstack(df, :Key, :Value)
    # The expected output is in order of appearance
    df4 = DataFrame(Fish = Union{String, Missing}["Bob", "Batman"],
                    Mass = Union{String, Missing}["12 g", "18 g"],
                    Color = Union{String, Missing}["Red", "Grey"])
    @test df2 ≅ df4
    @test typeof(df2[!, :Fish]) <: CategoricalVector{Union{String, Missing}}
    # first column stays as CategoricalArray in df3
    @test df3 == df4
    #Make sure unstack works with missing values at the start of the value column
    df[1, :Value] = missing
    df2 = unstack(df, :Fish, :Key, :Value)
    #This changes the expected result
    df4[1, :Mass] = missing
    @test df2 ≅ df4

    df = DataFrame(Fish = CategoricalArray{Union{String, Missing}}(["Bob", "Bob", "Batman", "Batman"]),
                   Key = CategoricalArray{Union{String, Missing}}(["Mass", "Color", "Mass", "Color"]),
                   Value = Union{String, Missing}["12 g", "Red", "18 g", "Grey"])
    levels!(df[!, 1], ["XXX", "Bob", "Batman"])
    levels!(df[!, 2], ["YYY", "Color", "Mass"])
    df2 = unstack(df, :Fish, :Key, :Value, renamecols=x->string("_", uppercase(string(x)), "_"))
    df3 = unstack(df, :Key, :Value, renamecols=x->string("_", uppercase(string(x)), "_"))
    df4 = DataFrame(Fish = Union{String, Missing}["Bob", "Batman"],
                    _MASS_ = Union{String, Missing}["12 g", "18 g"],
                    _COLOR_ = Union{String, Missing}["Red", "Grey"])
    @test df2 == df4
    @test df3 == df4

    #The same as above but without CategoricalArray
    df = DataFrame(Fish = ["Bob", "Bob", "Batman", "Batman"],
                   Key = ["Mass", "Color", "Mass", "Color"],
                   Value = ["12 g", "Red", "18 g", "Grey"])
    #Unstack specifying a row column
    df2 = unstack(df, :Fish, :Key, :Value)
    #Unstack without specifying a row column
    df3 = unstack(df, :Key, :Value)
    df4 = DataFrame(Fish = ["Bob", "Batman"],
                    Mass = ["12 g", "18 g"],
                    Color = ["Red", "Grey"])
    @test df2 ≅ df4
    @test typeof(df2[!, :Fish]) <: Vector{String}
    # first column stays as CategoricalArray in df3
    @test df3 == df4
    #Make sure unstack works with missing values at the start of the value column
    allowmissing!(df, :Value)
    df[1, :Value] = missing
    df2 = unstack(df, :Fish, :Key, :Value)
    #This changes the expected result
    allowmissing!(df4, :Mass)
    df4[1, :Mass] = missing
    @test df2 ≅ df4

    df = DataFrame(Fish = ["Bob", "Bob", "Batman", "Batman"],
                   Key = ["Mass", "Color", "Mass", "Color"],
                   Value = ["12 g", "Red", "18 g", "Grey"])
    df2 = unstack(df, :Fish, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df3 = unstack(df, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df4 = DataFrame(Fish = ["Bob", "Batman"],
                    _MASS_ = ["12 g", "18 g"],
                    _COLOR_ = ["Red", "Grey"])
    @test df2 == df4
    @test df3 == df4

    # test empty set of grouping variables
    @test_throws ArgumentError unstack(df, Int[], :Key, :Value)
    @test_throws ArgumentError unstack(df, r"xxxxx", :Key, :Value)
    @test_throws ArgumentError unstack(df, Symbol[], :Key, :Value)
    @test_throws ArgumentError unstack(stack(DataFrame(rand(10, 10), :auto)),
                                  :id, :variable, :value)
    @test_throws TypeError unstack(df, :Key, :Value, renamecols=Symbol)

    # test missing value in grouping variable
    mdf = DataFrame(id=[missing, 1, 2, 3], a=1:4, b=1:4)
    @test unstack(stack(mdf, Not(:id)), :id, :variable, :value) ≅ mdf
    @test unstack(stack(mdf, Not(1)), :id, :variable, :value) ≅ mdf
    @test unstack(stack(mdf, Not(:id)), :id, :variable, :value) ≅ mdf
    @test unstack(stack(mdf, Not(1)), :id, :variable, :value) ≅ mdf

    # test more than one grouping column
    wide = DataFrame(id = 1:12,
                     a  = repeat([1:3;], inner = [4]),
                     b  = repeat([1:4;], inner = [3]),
                     c  = randn(12),
                     d  = randn(12))
    w2 = wide[:, [1, 2, 4, 5]]
    rename!(w2, [:id, :a, :_C_, :_D_])
    long = stack(wide)
    wide3 = unstack(long, [:id, :a], :variable, :value)
    @test wide3 == wide[:, [1, 2, 4, 5]]
    wide3 = unstack(long, [:id, :a], :variable, :value,
                    renamecols=x->string("_", uppercase(string(x)), "_"))
    @test wide3 == w2

    wide3 = unstack(long, r"^[ia]", :variable, :value)
    @test wide3 == wide[:, [1, 2, 4, 5]]
    wide3 = unstack(long, r"^[ia]", :variable, :value,
                    renamecols=x->string("_", uppercase(string(x)), "_"))
    @test wide3 == w2
end

@testset "unstack promotion to support missing values" begin
    df = DataFrame([repeat(1:2, inner=4), repeat('a':'d', outer=2), collect(1:8)],
                   [:id, :variable, :value])
    udf = unstack(df, :variable, :value)
    @test udf == unstack(df, :variable, :value) == unstack(df, :id, :variable, :value)
    @test udf == DataFrame([Union{Int, Missing}[1, 2], Union{Int, Missing}[1, 5],
                            Union{Int, Missing}[2, 6], Union{Int, Missing}[3, 7],
                            Union{Int, Missing}[4, 8]], [:id, :a, :b, :c, :d])
    @test isa(udf[!, 1], Vector{Int})
    @test all(i -> isa(eachcol(udf)[i], Vector{Union{Int, Missing}}), 2:5)
    df = DataFrame([categorical(repeat(1:2, inner=4)),
                       categorical(repeat('a':'d', outer=2)), categorical(1:8)],
                   [:id, :variable, :value])
    udf = unstack(df, :variable, :value)
    @test udf == unstack(df, :variable, :value) == unstack(df, :id, :variable, :value)
    @test udf == DataFrame([Union{Int, Missing}[1, 2], Union{Int, Missing}[1, 5],
                            Union{Int, Missing}[2, 6], Union{Int, Missing}[3, 7],
                            Union{Int, Missing}[4, 8]], [:id, :a, :b, :c, :d])
    @test isa(udf[!, 1], CategoricalVector{Int})
    @test all(i -> isa(eachcol(udf)[i], CategoricalVector{Union{Int, Missing}}), 2:5)
end

@testset "duplicate entries in unstack warnings" begin
    df = DataFrame(id=Union{Int, Missing}[1, 2, 1, 2],
                   id2=Union{Int, Missing}[1, 2, 1, 2],
                   variable=["a", "b", "a", "b"], value=[3, 4, 5, 6])
    @test_throws ArgumentError unstack(df, :id, :variable, :value)
    @test_throws ArgumentError unstack(df, :variable, :value)
    a = unstack(df, :id, :variable, :value, allowduplicates=true)
    b = unstack(df, :variable, :value, allowduplicates=true)
    @test a ≅ DataFrame(id = [1, 2], a = [5, missing], b = [missing, 6])
    @test b ≅ DataFrame(id = [1, 2], id2 = [1, 2], a = [5, missing], b = [missing, 6])

    df = DataFrame(id=1:2, variable=["a", "b"], value=3:4)
    a = unstack(df, :id, :variable, :value)
    b = unstack(df, :variable, :value)
    @test a ≅ b ≅ DataFrame(id = [1, 2], a = [3, missing], b = [missing, 4])

    df = DataFrame(variable=["x", "x"], value=[missing, missing], id=[1, 1])
    @test_throws ArgumentError unstack(df, :variable, :value)
    @test_throws ArgumentError unstack(df, :id, :variable, :value)
    @test unstack(df, :variable, :value, allowduplicates=true) ≅ DataFrame(id=1, x=missing)
    @test unstack(df, :id, :variable, :value, allowduplicates=true) ≅ DataFrame(id=1, x=missing)
end

@testset "missing values in colkey" begin
    df = DataFrame(id=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   variable=["a", "b", missing, "a", "b", "missing", "a", "b", "missing"],
                   value=[missing, 2.0, 3.0, 4.0, 5.0, missing, 7.0, missing, 9.0])
    @test_throws ArgumentError unstack(df, :variable, :value)
    @test_throws ArgumentError unstack(df, :variable, :value, allowmissing=true)
    udf = unstack(df, :variable, :value, allowmissing=true, renamecols=x -> coalesce(x, "MISSING"))
    @test propertynames(udf) == [:id, :a, :b, :MISSING, :missing]
    @test udf[!, :missing] ≅ [missing, missing, 9.0]
    @test udf[!, :MISSING] ≅ [3.0, missing, missing]

    df = DataFrame(id=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   id2=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   variable=["a", "b", missing, "a", "b", "missing", "a", "b", "missing"],
                   value=[missing, 2.0, 3.0, 4.0, 5.0, missing, 7.0, missing, 9.0])
    @test_throws ArgumentError unstack(df, 3, 4)
    @test_throws ArgumentError unstack(df, 3, 4, allowmissing=true)
    udf = unstack(df, 3, 4, allowmissing=true, renamecols=x -> coalesce(x, "MISSING"))

    @test propertynames(udf) == [:id, :id2, :a, :b, :MISSING, :missing]
    @test udf[!, :missing] ≅ [missing, missing, 9.0]
    @test udf[!, :MISSING] ≅ [3.0, missing, missing]
end

@testset "stack-unstack correctness" begin
    Random.seed!(1234)
    x = DataFrame(rand(100, 50), :auto)
    x[!, :id] = [1:99; missing]
    x[!, :id2] = string.("a", x[!, :id])
    x[!, :s] = [i % 2 == 0 ? randstring() : missing for i in 1:100]
    allowmissing!(x, :x1)
    x[1, :x1] = missing
    y = stack(x, Not([:id, :id2]))
    @test y ≅ stack(x, Not(r"id"))
    @test y ≅ stack(x, Not(Not(Not(r"id"))))
    z = unstack(y, :id, :variable, :value)
    @test all(isequal(z[!, n], x[!, n]) for n in names(z))
    z = unstack(y, :variable, :value)
    @test all(isequal(z[!, n], x[!, n]) for n in names(x))
end

@testset "reshape" begin
    d1 = DataFrame(a = Array{Union{Int, Missing}}(repeat([1:3;], inner = [4])),
                b = Array{Union{Int, Missing}}(repeat([1:4;], inner = [3])),
                c = Array{Union{Float64, Missing}}(randn(12)),
                d = Array{Union{Float64, Missing}}(randn(12)),
                e = Array{Union{String, Missing}}(map(string, 'a':'l')))

    @test propertynames(stack(d1, :a)) == [:b, :c, :d, :e, :variable, :value]
    d1s = stack(d1, [:a, :b])
    @test d1s == stack(d1, r"[ab]")
    @test d1s == stack(d1, Not(r"[cde]"))
    @test d1s == stack(d1, Not(Not(r"[ab]")))
    d1s2 = stack(d1, [:c, :d])
    @test d1s2 == stack(d1, r"[cd]")
    @test d1s2 == stack(d1, Not([1, 2, 5]))
    d1s3 = stack(d1)
    d1m = stack(d1, Not([:c, :d, :e]))
    @test d1m == stack(d1, Not(r"[cde]"))
    @test d1s[1:12, :c] == d1[!, :c]
    @test d1s[13:24, :c] == d1[!, :c]
    @test d1s2 == d1s3
    @test propertynames(d1s) == [:c, :d, :e, :variable, :value]
    @test d1s == d1m
    d1m = stack(d1[:, [1, 3, 4]], Not(:a))
    @test propertynames(d1m) == [:a, :variable, :value]

    # Test naming of measure/value columns
    d1s_named = stack(d1, [:a, :b], variable_name=:letter, value_name=:someval)
    @test d1s_named == stack(d1, r"[ab]", variable_name=:letter, value_name=:someval)
    @test propertynames(d1s_named) == [:c, :d, :e, :letter, :someval]
    d1m_named = stack(d1[:, [1, 3, 4]], Not(:a), variable_name=:letter, value_name=:someval)
    @test propertynames(d1m_named) == [:a, :letter, :someval]

    # test empty measures or ids
    dx = stack(d1, [], [:a])
    @test dx == stack(d1, r"xxx", r"a")
    @test size(dx) == (0, 3)
    @test propertynames(dx) == [:a, :variable, :value]
    dx = stack(d1, :a, [])
    @test dx == stack(d1, r"a", r"xxx")
    @test size(dx) == (12, 2)
    @test propertynames(dx) == [:variable, :value]
    dx = stack(d1, [:a], [])
    @test dx == stack(d1, r"a", r"xxx")
    @test size(dx) == (12, 2)
    @test propertynames(dx) == [:variable, :value]
    dx = stack(d1, [], :a)
    @test dx == stack(d1, r"xxx", r"a")
    @test size(dx) == (0, 3)
    @test propertynames(dx) == [:a, :variable, :value]

    @test stack(d1, :a, view=true) == stack(d1, [:a], view=true)
    @test all(isa.(eachcol(stack(d1, :a, view=true)),
                   [fill(DataFrames.RepeatedVector, 5);
                    DataFrames.StackedVector]))
    @test all(isa.(eachcol(stack(d1, Not([:b, :c, :d, :e]), view=true)),
                   [fill(DataFrames.RepeatedVector, 5);
                    DataFrames.StackedVector]))

    # Tests of RepeatedVector and StackedVector indexing
    d1s = stack(d1, [:a, :b], view=true)
    @test d1s == stack(d1, r"[ab]", view=true)
    @test d1s[!, 4] isa DataFrames.RepeatedVector
    @test ndims(d1s[!, 4]) == 1
    @test ndims(typeof(d1s[!, 4])) == 1
    @test d1s[!, 5] isa DataFrames.StackedVector
    @test ndims(d1s[!, 5]) == 1
    @test ndims(typeof(d1s[!, 2])) == 1
    @test d1s[!, 4][[1, 24]] == ["a", "b"]
    @test d1s[!, 5][[1, 24]] == [1, 4]
    @test_throws ArgumentError d1s[!, 4][true]
    @test_throws ArgumentError d1s[!, 5][true]
    @test_throws ArgumentError d1s[!, 4][1.0]
    @test_throws ArgumentError d1s[!, 5][1.0]

    d1ss = stack(d1, [:a, :b], view=true)
    @test d1ss[!, 4][[1, 24]] == ["a", "b"]
    @test d1ss[!, 4] isa DataFrames.RepeatedVector
    d1ss = stack(d1, [:a, :b], view=true, variable_eltype=String)
    @test d1ss[!, 4][[1, 24]] == ["a", "b"]
    @test d1ss[!, 4] isa DataFrames.RepeatedVector
    d1ss = stack(d1, [:a, :b], view=true, variable_eltype=Symbol)
    @test d1ss[!, 4][[1, 24]] == [:a, :b]
    @test d1ss[!, 4] isa DataFrames.RepeatedVector

    # Those tests check indexing RepeatedVector/StackedVector by a vector
    @test d1s[!, 4][trues(24)] == d1s[!, 4]
    @test d1s[!, 5][trues(24)] == d1s[!, 5]
    @test d1s[!, 4][:] == d1s[!, 4]
    @test d1s[!, 5][:] == d1s[!, 5]
    @test d1s[!, 4][1:24] == d1s[!, 4]
    @test d1s[!, 5][1:24] == d1s[!, 5]
    @test [d1s[!, 4][1:12]; d1s[!, 4][13:24]] == d1s[!, 4]
    @test [d1s[!, 5][1:12]; d1s[!, 5][13:24]] == d1s[!, 5]

    d1s2 = stack(d1, [:c, :d], view=true)
    @test d1s2 == stack(d1, r"[cd]", view=true)
    d1s3 = stack(d1, view=true)
    d1m = stack(d1, Not([:c, :d, :e]), view=true)
    @test d1m == stack(d1, Not(r"[cde]"), view=true)
    @test d1s[1:12, :c] == d1[!, :c]
    @test d1s[13:24, :c] == d1[!, :c]
    @test d1s2 == d1s3
    @test propertynames(d1s) == [:c, :d, :e, :variable, :value]
    @test d1s == d1m
    d1m = stack(d1[:, [1, 3, 4]], Not(:a), view=true)
    @test propertynames(d1m) == [:a, :variable, :value]

    d1s_named = stack(d1, [:a, :b], variable_name=:letter, value_name=:someval, view=true)
    @test d1s_named == stack(d1, r"[ab]", variable_name=:letter, value_name=:someval, view=true)
    @test propertynames(d1s_named) == [:c, :d, :e, :letter, :someval]
    d1m_named = stack(d1, Not([:c, :d, :e]), variable_name=:letter, value_name=:someval, view=true)
    @test d1m_named == stack(d1, Not(r"[cde]"), variable_name=:letter, value_name=:someval, view=true)
    @test propertynames(d1m_named) == [:c, :d, :e, :letter, :someval]

    d1s[!, :id] = Union{Int, Missing}[1:12; 1:12]
    d1s2[!, :id] =  Union{Int, Missing}[1:12; 1:12]
    d1us = unstack(d1s, :id, :variable, :value)
    d1us2 = unstack(d1s2, :id, :variable, :value)
    d1us3 = unstack(d1s2, :variable, :value)
    @test d1us[!, :a] == d1[!, :a]
    @test d1us2[!, :d] == d1[!, :d]
    @test d1us2[!, 3] == d1[!, :d]
    @test d1us3[!, :d] == d1[!, :d]
    @test d1us3 == unstack(d1s2)

    # test unstack with exactly one key column that is not passed
    df1 = stack(DataFrame(rand(10, 10), :auto))
    df1[!, :id] = 1:100
    @test size(unstack(df1, :variable, :value)) == (100, 11)
    @test unstack(df1, :variable, :value) ≅ unstack(df1)

    # test empty keycol
    @test_throws ArgumentError unstack(stack(DataFrame(rand(3, 2), :auto)), :variable, :value)
end

@testset "column names duplicates" begin
    wide = DataFrame(id = 1:12,
                     a  = repeat([1:3;], inner = [4]),
                     b  = repeat([1:4;], inner = [3]),
                     c  = randn(12),
                     d  = randn(12))
    w1 = wide[:, [1, 2, 4, 5]]
    rename!(w1, [:id, :a, :D, :d])
    w2 = wide[:, [1, 2, 4, 5]]
    rename!(w2, [:id, :a, :_D_, :_d_])
    long = stack(wide)
    long.variable = replace.(string.(long.variable), Ref("c" => "D"))
    wide3 = unstack(long, [:id, :a], :variable, :value)
    @test wide3 == w1
    wide3 = unstack(long, :id, :variable, :value)
    @test wide3 == w1[!, Not(2)]
    @test_throws ArgumentError unstack(long, [:id, :a], :variable, :value,
                                       renamecols=uppercase)
    wide3 = unstack(long, [:id, :a], :variable, :value,
                    renamecols=x->string("_", x, "_"))
    @test wide3 == w2
    wide3 = unstack(long, :id, :variable, :value,
                    renamecols=x->string("_", x, "_"))
    @test wide3 == w2[!, Not(2)]
end

@testset "flatten single column" begin
    df_vec = DataFrame(a = [1, 2], b = [[1, 2], [3, 4]])
    df_tup = DataFrame(a = [1, 2], b = [(1, 2), (3, 4)])
    ref = DataFrame(a = [1, 1, 2, 2], b = [1, 2, 3, 4])
    @test flatten(df_vec, :b) == flatten(df_tup, :b) == ref
    @test flatten(df_vec, "b") == flatten(df_tup, "b") == ref
    df_mixed_types = DataFrame(a = [1, 2], b = [[1, 2], ["x", "y"]])
    ref_mixed_types = DataFrame(a = [1, 1, 2, 2], b = [1, 2, "x", "y"])
    @test flatten(df_mixed_types, :b) == ref_mixed_types
    df_three = DataFrame(a = [1, 2, 3], b = [[1, 2], [10, 20], [100, 200, 300]])
    ref_three = DataFrame(a = [1, 1, 2, 2, 3, 3, 3], b = [1, 2, 10, 20, 100, 200, 300])
    @test flatten(df_three, :b) == ref_three
    @test flatten(df_three, "b") == ref_three
    df_gen = DataFrame(a = [1, 2], b = [(i for i in 1:5), (i for i in 6:10)])
    ref_gen = DataFrame(a = [fill(1, 5); fill(2, 5)], b = collect(1:10))
    @test flatten(df_gen, :b) == ref_gen
    @test flatten(df_gen, "b") == ref_gen
    df_miss = DataFrame(a = [1, 2], b = [Union{Missing, Int}[1, 2], Union{Missing, Int}[3, 4]])
    ref = DataFrame(a = [1, 1, 2, 2], b = [1, 2, 3, 4])
    @test flatten(df_miss, :b) == ref
    @test flatten(df_miss, "b") == ref
    v1 = [[1, 2], [3, 4]]
    v2 = [[5, 6], [7, 8]]
    v = [v1, v2]
    df_vec_vec = DataFrame(a = [1, 2], b = v)
    ref_vec_vec = DataFrame(a = [1, 1, 2, 2], b = [v1 ; v2])
    @test flatten(df_vec_vec, :b) == ref_vec_vec
    @test flatten(df_vec_vec, "b") == ref_vec_vec
    df_cat = DataFrame(a = [1, 2], b = [CategoricalArray([1, 2]), CategoricalArray([1, 2])])
    df_flat_cat = flatten(df_cat, :b)
    ref_cat = DataFrame(a = [1, 1, 2, 2], b = [1, 2, 1, 2])
    @test df_flat_cat == ref_cat
    @test df_flat_cat.b isa CategoricalArray
end

@testset "flatten multiple columns" begin
    df = DataFrame(a = [1, 2], b = [[1, 2], [3, 4]], c = [[5, 6], [7, 8]])
    @test flatten(df, []) == df
    ref = DataFrame(a = [1, 1, 2, 2], b = [1, 2, 3, 4], c = [5, 6, 7, 8])
    @test flatten(df, [:b, :c]) == ref
    @test flatten(df, [:c, :b]) == ref
    @test flatten(df, ["b", "c"]) == ref
    @test flatten(df, ["c", "b"]) == ref
    @test flatten(df, 2:3) == ref
    @test flatten(df, r"[bc]") == ref
    @test flatten(df, Not(:a)) == ref
    @test flatten(df, Between(:b, :c)) == ref
    df_allcols = DataFrame(b = [[1, 2], [3, 4]], c = [[5, 6], [7, 8]])
    ref_allcols = DataFrame(b = [1, 2, 3, 4], c = [5, 6, 7, 8])
    @test flatten(df_allcols, All()) == ref_allcols
    @test flatten(df_allcols, Cols(:)) == ref_allcols
    @test flatten(df_allcols, :) == ref_allcols
    df_bad = DataFrame(a = [1, 2], b = [[1, 2], [3, 4]], c = [[5, 6], [7]])
    @test_throws ArgumentError flatten(df_bad, [:b, :c])
end

@testset "stack categorical test" begin
    Random.seed!(1234)
    d1 = DataFrame(a = repeat([1:3;], inner = [4]),
                   b = repeat([1:4;], inner = [3]),
                   c = randn(12),
                   d = randn(12),
                   e = map(string, 'a':'l'))
    d1s = stack(d1, [:d, :c])
    @test d1s.variable isa PooledVector{String}
    @test levels(d1s.variable) == ["c", "d"]
    d1s = stack(d1, [:d, :c], view=true)
    @test d1s.variable isa DataFrames.RepeatedVector{String}
    @test levels(d1s.variable) == ["c", "d"]
    @test d1s[:, 4] isa Vector{String}
    @test levels(d1s[:, 4]) == ["c", "d"]

    d1s = stack(d1, [:d, :c], variable_eltype=CategoricalValue{String})
    @test d1s.variable isa CategoricalVector{String}
    @test levels(d1s.variable) == ["d", "c"]
    d1s = stack(d1, [:d, :c], view=true, variable_eltype=CategoricalValue{String})
    @test d1s.variable isa DataFrames.RepeatedVector{<:CategoricalValue{String}}
    @test levels(d1s.variable) == ["d", "c"]
    @test d1s[:, 4] isa CategoricalVector{String}
    @test levels(d1s[:, 4]) == ["d", "c"]

    d1s = stack(d1, [:d, :c], variable_eltype=Symbol)
    @test d1s.variable isa PooledVector{Symbol}
    @test levels(d1s.variable) == [:c, :d]
    d1s = stack(d1, [:d, :c], view=true, variable_eltype=Symbol)
    @test d1s.variable isa DataFrames.RepeatedVector{Symbol}
    @test levels(d1s.variable) == [:c, :d]
    @test d1s[:, 4] isa Vector{Symbol}
    @test levels(d1s[:, 4]) == [:c, :d]

    d2 = mapcols(categorical, d1)
    levels!(d2.a, [2, 1, 3])
    ordered!(d2.a, true)
    ref_levels = shuffle!(unique([levels(d2.c); levels(d2.d)]))
    levels!(d2.c, ref_levels)
    ordered!(d2.c, true)
    levels!(d2.d, ref_levels)
    ordered!(d2.d, true)
    d2s = stack(d2, [:d, :c], variable_eltype=CategoricalValue{String})
    for col in eachcol(d2s)
        @test col isa CategoricalVector
    end
    @test levels(d2s.value) == ref_levels
    @test isordered(d2s.value)
    @test levels(d2.a) == levels(d2s.a)
    @test levels(d2.b) == levels(d2s.b)
    @test levels(d2.e) == levels(d2s.e)
    @test isordered(d2.a) == isordered(d2s.a)
    @test isordered(d2.b) == isordered(d2s.b)
    @test isordered(d2.e) == isordered(d2s.e)
end

@testset "test stack eltype" begin
    df = DataFrame(rand(4, 5), :auto)
    sdf = stack(df)
    @test eltype(sdf.variable) === String
    @test eltype(typeof(sdf.variable)) === String
    @test eltype(sdf.value) === Float64
    @test eltype(typeof(sdf.value)) === Float64
    sdf2 = first(sdf, 3)
    @test eltype(sdf2.variable) === String
    @test eltype(typeof(sdf2.variable)) === String
    @test eltype(sdf2.value) === Float64
    @test eltype(typeof(sdf2.value)) === Float64
end

@testset "additional unstack tests" begin
    df = DataFrame(id=repeat(1:3, inner=3),
                   id2=repeat(1:3, inner=3),
                   var=repeat('a':'c', 3),
                   val=1:9)
    @test unstack(df, :id, :var, :val) == DataFrame(id=1:3, a=1:3:7, b=2:3:8, c=3:3:9)
    @test unstack(df, [:id, :id2], :var, :val) == unstack(df, :var, :val) ==
          DataFrame(id=1:3, id2=1:3, a=1:3:7, b=2:3:8, c=3:3:9)

    # make sure we always use order of appereance
    Random.seed!(1234)
    # Use a large value to test several orders of appearance
    for i in 1:16
        df = df[Random.shuffle(1:9), :]
        wide1 = unstack(df, :id, :var, :val)
        wide2 = unstack(df, [:id, :id2], :var, :val)
        wide3 = unstack(df, :var, :val)
        @test wide1[sortperm(unique(df.id)), [1; 1 .+ sortperm(unique(df.var))]] ==
              DataFrame(id=1:3, a=1:3:7, b=2:3:8, c=3:3:9)
        @test wide2[sortperm(unique(df.id)), [1:2; 2 .+ sortperm(unique(df.var))]] ==
              DataFrame(id=1:3, id2=1:3, a=1:3:7, b=2:3:8, c=3:3:9)
        @test wide2 == wide3

        df2 = copy(df)
        df2.id = PooledArray(df.id)
        df2.var = PooledArray(df.var)
        @test unstack(df2, :id, :var, :val) == wide1
        @test unstack(df2, [:id, :id2], :var, :val) == wide2
        @test unstack(df2, :var, :val) == wide3

        df2 = transform(df, 1:3 .=> categorical, renamecols=false)
        @test unstack(df2, :id, :var, :val) == wide1
        @test unstack(df2, [:id, :id2], :var, :val) == wide2
        @test unstack(df2, :var, :val) == wide3
        levels!(df2.id, [10, 2, 11, 3, 1, 12])
        levels!(df2.var, ['x', 'b', 'y', 'c', 'a', 'z'])
        @test unstack(df2, :id, :var, :val) == wide1
        @test unstack(df2, [:id, :id2], :var, :val) == wide2
        @test unstack(df2, :var, :val) == wide3
    end

    df = DataFrame(id=repeat(1:3, inner=3),
                   a=repeat(1:3, inner=3),
                   var=repeat('a':'c', 3),
                   val=1:9)
    @test unstack(df, :id, :var, :val) == DataFrame(id=1:3, a=1:3:7, b=2:3:8, c=3:3:9)
    @test_throws ArgumentError unstack(df, :a, :var, :val)
    @test_throws ArgumentError unstack(df, [:id, :a], :var, :val)

    df = DataFrame(id=repeat(1:3, inner=3),
                   id2=repeat(1:3, inner=3),
                   var=repeat('a':'c', 3),
                   val=1:9)
    df[4, 1:2] .= 1
    @test_throws ArgumentError unstack(df, :id, :var, :val)
    @test_throws ArgumentError unstack(df, [:id, :id2], :var, :val)
    @test unstack(df, :id, :var, :val, allowduplicates=true) ≅
          DataFrame(id=1:3, a=[4, missing, 7], b=2:3:8, c=3:3:9)
    @test unstack(df, [:id, :id2], :var, :val, allowduplicates=true) ≅
          DataFrame(id=1:3, id2=1:3, a=[4, missing, 7], b=2:3:8, c=3:3:9)

    df = DataFrame(id=repeat(1:3, inner=3),
                   id2=repeat(1:3, inner=3),
                   var=repeat('a':'c', 3),
                   val=1:9)
    allowmissing!(df, :var)
    df.var[4] = missing
    @test_throws ArgumentError unstack(df, :id, :var, :val)
    @test_throws ArgumentError unstack(df, [:id, :id2], :var, :val)
    @test unstack(df, :id, :var, :val, allowmissing=true) ≅
          DataFrame(id=1:3, a=[1, missing, 7], b=2:3:8, c=3:3:9, missing=[missing, 4, missing])
    @test unstack(df, [:id, :id2], :var, :val, allowmissing=true) ≅
          DataFrame(id=1:3, id2=1:3, a=[1, missing, 7], b=2:3:8, c=3:3:9, missing=[missing, 4, missing])
end

# test scenario when sorting fails both in grouping and in variable
struct A_TYPE
    x
end

@testset "additional unstack tests not sortable" begin
    df = DataFrame(id=repeat(A_TYPE.([2, 1, 3]), inner=3),
                   id2=repeat(A_TYPE.([2, 1, 3]), inner=3),
                   var=repeat(A_TYPE.([3, 2, 1]), 3),
                   val=1:9)
    @test unstack(df, :id, :var, :val, renamecols=x -> Symbol(:x, x.x)) ==
          DataFrame(id=A_TYPE.([2, 1, 3]), x3=1:3:7, x2=2:3:8, x1=3:3:9)
    @test unstack(df, [:id, :id2], :var, :val, renamecols=x -> Symbol(:x, x.x)) ==
          DataFrame(id=A_TYPE.([2, 1, 3]), id2=A_TYPE.([2, 1, 3]), x3=1:3:7, x2=2:3:8, x1=3:3:9)
end

@testset "permutedims" begin
    df1 = DataFrame(a=["x", "y"], b=rand(2), c=[1, 2], d=rand(Bool, 2))

    @test_throws ArgumentError transpose(df1)
    @test_throws ArgumentError permutedims(df1, :bar)

    df1_pd = permutedims(df1, 1)
    @test size(df1_pd, 1) == ncol(df1) - 1
    @test size(df1_pd, 2) == nrow(df1) + 1
    @test names(df1_pd) == ["a", "x", "y"]
    @test df1_pd == permutedims(df1, :a) == permutedims(df1, 1)
    @test names(permutedims(df1, :a, :foo)) == ["foo", "x", "y"]

    orignames1 = names(df1)[2:end]
    for (i, row) in enumerate(eachrow(df1_pd))
        @test Vector(row) == [orignames1[i]; df1[!, orignames1[i]]]
    end

    # All columns should be promoted
    @test eltype(df1_pd.x) == Float64
    @test eltype(df1_pd.y) == Float64

    df2 = DataFrame(a=["x", "y"], b=[1.0, "str"], c=[1, 2], d=rand(Bool, 2))

    df2_pd = permutedims(df2, :a)
    @test size(df2_pd, 1) == ncol(df2) - 1
    @test size(df2_pd, 2) == nrow(df2) + 1
    @test names(df2_pd) == ["a", "x", "y"]

    orignames2 = names(df2)[2:end]
    for (i, row) in enumerate(eachrow(df2_pd))
        @test Vector(row) == [orignames2[i]; df2[!, orignames2[i]]]
    end
    @test Any == eltype(df2_pd.x)
    @test Any == eltype(df2_pd.y)

    df3 = DataFrame(a=fill("x", 10), b=rand(10), c=rand(Int, 10), d=rand(Bool, 10))

    d3pd_names = ["a", "x", ("x_$i" for i in 1:9)...]
    @test_throws ArgumentError permutedims(df3, 1)
    @test names(permutedims(df3, 1, makeunique=true)) == d3pd_names
    @test_throws ArgumentError permutedims(df3[!, [:a]], 1) # single column branch
    @test names(permutedims(df3[!, [:a]], 1, makeunique=true)) == d3pd_names

    df4 = DataFrame(a=rand(2), b=rand(2), c=[1, 2], d=[1.0, missing],
                    e=["x", "y"], f=[:x, :y], # valid src
                    g=[missing, "y"], h=Union{Missing, String}["x", "y"] # invalid src
                    )

    @test permutedims(df4[!, [:a, :b, :c, :e]], :e) ==
          permutedims(df4[!, [:e, :a, :b, :c]], 1) ==
          permutedims(df4[!, [:a, :b, :c, :f]], :f, :e)
    # Can permute single-column
    @test permutedims(df4[!, [:e]], 1) == DataFrame(e=String[], x=[], y=[])
    # Can't index float Column
    @test_throws ArgumentError permutedims(df4[!, [:a, :b, :c]], 1)
    @test_throws ArgumentError permutedims(DataFrame(a=Float64[], b=Float64[]), 1)
    # Can't index columns that allow for missing
    @test_throws ArgumentError permutedims(df4[!, [:g, :a, :b, :c]], 1)
    @test_throws ArgumentError permutedims(df4[!, [:h, :a, :b]], 1)
    # Can't permute empty `df` ...
    @test_throws BoundsError permutedims(DataFrame(), 1)
    # ... but can permute zero-row df
    @test permutedims(DataFrame(a=String[], b=Float64[]), 1) == DataFrame(a=["b"])
end

@testset "stack view=true additional tests" begin
    df = DataFrame(a=1:3, b=11:13, c=101:103)
    sdf = stack(df, [:b, :c], view=true)
    @test reverse(sdf.a) == reverse(copy(sdf.a))
    @test IndexStyle(DataFrames.StackedVector) == IndexLinear()
end

@testset "empty unstack" begin
    df = DataFrame(a = [], b = [], c = [])
    dfu = unstack(df, :b, :c)
    @test isempty(dfu)
    @test names(dfu) == ["a"]
    @test dfu.a isa Vector{Any}
end

end # module
