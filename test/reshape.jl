module TestReshape

using Test, DataFrames, Random, Logging, PooledArrays
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
    #The expected output, XXX level should be dropped as it has no rows with this key
    df4 = DataFrame(Fish = Union{String, Missing}["Bob", "Batman"],
                    Color = Union{String, Missing}["Red", "Grey"],
                    Mass = Union{String, Missing}["12 g", "18 g"])
    @test df2 ≅ df4
    @test typeof(df2[!, :Fish]) <: CategoricalVector{Union{String, Missing}}
    # first column stays as CategoricalArray in df3
    @test df3 == df4
    #Make sure unstack works with missing values at the start of the value column
    df[1,:Value] = missing
    df2 = unstack(df, :Fish, :Key, :Value)
    #This changes the expected result
    df4[1,:Mass] = missing
    @test df2 ≅ df4

    df = DataFrame(Fish = CategoricalArray{Union{String, Missing}}(["Bob", "Bob", "Batman", "Batman"]),
                   Key = CategoricalArray{Union{String, Missing}}(["Mass", "Color", "Mass", "Color"]),
                   Value = Union{String, Missing}["12 g", "Red", "18 g", "Grey"])
    levels!(df[!, 1], ["XXX", "Bob", "Batman"])
    levels!(df[!, 2], ["YYY", "Color", "Mass"])
    df2 = unstack(df, :Fish, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df3 = unstack(df, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df4 = DataFrame(Fish = Union{String, Missing}["Bob", "Batman"],
                    _COLOR_ = Union{String, Missing}["Red", "Grey"],
                    _MASS_ = Union{String, Missing}["12 g", "18 g"])
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
    #The expected output, XXX level should be dropped as it has no rows with this key
    df4 = DataFrame(Fish = ["Batman", "Bob"],
                    Color = ["Grey", "Red"],
                    Mass = ["18 g", "12 g"])
    @test df2 ≅ df4
    @test typeof(df2[!, :Fish]) <: Vector{String}
    # first column stays as CategoricalArray in df3
    @test df3 == df4
    #Make sure unstack works with missing values at the start of the value column
    allowmissing!(df, :Value)
    df[1,:Value] = missing
    df2 = unstack(df, :Fish, :Key, :Value)
    #This changes the expected result
    allowmissing!(df4, :Mass)
    df4[2,:Mass] = missing
    @test df2 ≅ df4

    df = DataFrame(Fish = ["Bob", "Bob", "Batman", "Batman"],
                   Key = ["Mass", "Color", "Mass", "Color"],
                   Value = ["12 g", "Red", "18 g", "Grey"])
    df2 = unstack(df, :Fish, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df3 = unstack(df, :Key, :Value, renamecols=x->string("_", uppercase(x), "_"))
    df4 = DataFrame(Fish = ["Batman", "Bob"],
                    _COLOR_ = ["Grey", "Red"],
                    _MASS_ = ["18 g", "12 g"])
    @test df2 == df4
    @test df3 == df4

    # test empty set of grouping variables
    @test_throws ArgumentError unstack(df, Int[], :Key, :Value)
    @test_throws ArgumentError unstack(df, r"xxxxx", :Key, :Value)
    @test_throws ArgumentError unstack(df, Symbol[], :Key, :Value)
    @test_throws ArgumentError unstack(stack(DataFrame(rand(10, 10))),
                                  :id, :variable, :value)
    @test_throws TypeError unstack(df, :Key, :Value, renamecols=Symbol)

    # test missing value in grouping variable
    mdf = DataFrame(id=[missing,1,2,3], a=1:4, b=1:4)
    @test unstack(stack(mdf, Not(:id)), :id, :variable, :value)[1:3,:] == sort(mdf)[1:3,:]
    @test unstack(stack(mdf, Not(1)), :id, :variable, :value)[1:3,:] == sort(mdf)[1:3,:]
    @test unstack(stack(mdf, Not(:id)), :id, :variable, :value)[:, 2:3] == sort(mdf)[:, 2:3]
    @test unstack(stack(mdf, Not(1)), :id, :variable, :value)[:, 2:3] == sort(mdf)[:, 2:3]

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
    @test all(isa.(eachcol(udf)[2:end], Vector{Union{Int, Missing}}))
    df = DataFrame([categorical(repeat(1:2, inner=4)),
                       categorical(repeat('a':'d', outer=2)), categorical(1:8)],
                   [:id, :variable, :value])
    udf = unstack(df, :variable, :value)
    @test udf == unstack(df, :variable, :value) == unstack(df, :id, :variable, :value)
    @test udf == DataFrame([Union{Int, Missing}[1, 2], Union{Int, Missing}[1, 5],
                            Union{Int, Missing}[2, 6], Union{Int, Missing}[3, 7],
                            Union{Int, Missing}[4, 8]], [:id, :a, :b, :c, :d])
    @test isa(udf[!, 1], CategoricalVector{Int})
    @test all(isa.(eachcol(udf)[2:end], CategoricalVector{Union{Int, Missing}}))
end

@testset "duplicate entries in unstack warnings" begin
    df = DataFrame(id=Union{Int, Missing}[1, 2, 1, 2],
                   id2=Union{Int, Missing}[1, 2, 1, 2],
                   variable=["a", "b", "a", "b"], value=[3, 4, 5, 6])
    @test_logs (:warn, "Duplicate entries in unstack at row 3 for key 1 and variable a.") unstack(df, :id, :variable, :value)
    @test_logs (:warn, "Duplicate entries in unstack at row 3 for key (1, 1) and variable a.") unstack(df, :variable, :value)
    a, b = with_logger(NullLogger()) do
        unstack(df, :id, :variable, :value), unstack(df, :variable, :value)
    end
    @test a ≅ DataFrame(id = [1, 2], a = [5, missing], b = [missing, 6])
    @test b ≅ DataFrame(id = [1, 2], id2 = [1, 2], a = [5, missing], b = [missing, 6])

    df = DataFrame(id=1:2, variable=["a", "b"], value=3:4)
    @test_nowarn unstack(df, :id, :variable, :value)
    @test_nowarn unstack(df, :variable, :value)
    a = unstack(df, :id, :variable, :value)
    b = unstack(df, :variable, :value)
    @test a ≅ b ≅ DataFrame(id = [1, 2], a = [3, missing], b = [missing, 4])

    df = DataFrame(variable=["x", "x"], value=[missing, missing], id=[1,1])
    @test_logs (:warn, "Duplicate entries in unstack at row 2 for key 1 and variable x.") unstack(df, :variable, :value)
    @test_logs (:warn, "Duplicate entries in unstack at row 2 for key 1 and variable x.") unstack(df, :id, :variable, :value)
end

@testset "missing values in colkey" begin
    df = DataFrame(id=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   variable=["a", "b", missing, "a", "b", "missing", "a", "b", "missing"],
                   value=[missing, 2.0, 3.0, 4.0, 5.0, missing, 7.0, missing, 9.0])
    @test_logs (:warn, "Missing value in variable :variable at row 3. Skipping.") unstack(df, :variable, :value)
    udf = with_logger(NullLogger()) do
        unstack(df, :variable, :value)
    end
    @test propertynames(udf) == [:id, :a, :b, :missing]
    @test udf[!, :missing] ≅ [missing, 9.0, missing]
    df = DataFrame(id=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   id2=[1, 1, 1, missing, missing, missing, 2, 2, 2],
                   variable=["a", "b", missing, "a", "b", "missing", "a", "b", "missing"],
                   value=[missing, 2.0, 3.0, 4.0, 5.0, missing, 7.0, missing, 9.0])
    @test_logs (:warn, "Missing value in variable :variable at row 3. Skipping.") unstack(df, 3, 4)
    udf = with_logger(NullLogger()) do
        unstack(df, 3, 4)
    end
    @test propertynames(udf) == [:id, :id2, :a, :b, :missing]
    @test udf[!, :missing] ≅ [missing, 9.0, missing]
end

@testset "stack-unstack correctness" begin
    Random.seed!(1234)
    x = DataFrame(rand(100, 50))
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
    d1m = stack(d1[:, [1,3,4]], Not(:a))
    @test propertynames(d1m) == [:a, :variable, :value]

    # Test naming of measure/value columns
    d1s_named = stack(d1, [:a, :b], variable_name=:letter, value_name=:someval)
    @test d1s_named == stack(d1, r"[ab]", variable_name=:letter, value_name=:someval)
    @test propertynames(d1s_named) == [:c, :d, :e, :letter, :someval]
    d1m_named = stack(d1[:, [1,3,4]], Not(:a), variable_name=:letter, value_name=:someval)
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
    @test d1s[!, 4][[1,24]] == ["a", "b"]
    @test d1s[!, 5][[1,24]] == [1, 4]
    @test_throws ArgumentError d1s[!, 4][true]
    @test_throws ArgumentError d1s[!, 5][true]
    @test_throws ArgumentError d1s[!, 4][1.0]
    @test_throws ArgumentError d1s[!, 5][1.0]

    d1ss = stack(d1, [:a, :b], view=true)
    @test d1ss[!, 4][[1,24]] == ["a", "b"]
    @test d1ss[!, 4] isa DataFrames.RepeatedVector
    d1ss = stack(d1, [:a, :b], view=true, variable_eltype=String)
    @test d1ss[!, 4][[1,24]] == ["a", "b"]
    @test d1ss[!, 4] isa DataFrames.RepeatedVector
    d1ss = stack(d1, [:a, :b], view=true, variable_eltype=Symbol)
    @test d1ss[!, 4][[1,24]] == [:a, :b]
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
    d1m = stack(d1[:, [1,3,4]], Not(:a), view=true)
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
    df1 = stack(DataFrame(rand(10,10)))
    df1[!, :id] = 1:100
    @test size(unstack(df1, :variable, :value)) == (100, 11)
    @test unstack(df1, :variable, :value) ≅ unstack(df1)

    # test empty keycol
    @test_throws ArgumentError unstack(stack(DataFrame(rand(3,2))), :variable, :value)
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
    @test flatten(df_allcols, :) == ref_allcols
    df_bad = DataFrame(a = [1, 2], b = [[1, 2], [3, 4]], c = [[5, 6], [7]])
    @test_throws ArgumentError flatten(df_bad, [:b, :c])
end

@testset "test RepeatedVector for categorical" begin
    v = categorical(["a", "b", "c"], ordered=true)
    levels!(v, ["b", "c", "a"])
    rv = DataFrames.RepeatedVector(v, 1, 1)
    @test isordered(v)
    # uncomment after CategoricalArrays.jl is fixed
    # @test isordered(categorical(v))
    @test levels(v) == ["b", "c", "a"]
    @test levels(categorical(v)) == ["b", "c", "a"]

    v = categorical(["a", "b", "c"])
    levels!(v, ["b", "c", "a"])
    rv = DataFrames.RepeatedVector(v, 1, 1)
    @test !isordered(v)
    # uncomment after CategoricalArrays.jl is fixed
    # @test !isordered(categorical(v))
    @test levels(v) == ["b", "c", "a"]
    @test levels(categorical(v)) == ["b", "c", "a"]
end

@testset "stack categorical test" begin
    Random.seed!(1234)
    d1 = DataFrame(a = repeat([1:3;], inner = [4]),
                   b = repeat([1:4;], inner = [3]),
                   c = randn(12),
                   d = randn(12),
                   e = map(string, 'a':'l'))
    d1s = stack(d1, [:d, :c])
    @test d1s.variable isa CategoricalVector{String}
    @test levels(d1s.variable) == ["d", "c"]
    d1s = stack(d1, [:d, :c], view=true)
    @test d1s.variable isa DataFrames.RepeatedVector{<:CategoricalValue{String}}
    @test levels(d1s.variable) == ["d", "c"]
    @test d1s[:, 4] isa CategoricalVector{String}
    @test levels(d1s[:, 4]) == ["d", "c"]

    d1s = stack(d1, [:d, :c], variable_eltype=String)
    @test d1s.variable isa PooledVector{String}
    @test levels(d1s.variable) == ["c", "d"]
    d1s = stack(d1, [:d, :c], view=true, variable_eltype=String)
    @test d1s.variable isa DataFrames.RepeatedVector{String}
    @test levels(d1s.variable) == ["c", "d"]
    @test d1s[:, 4] isa Vector{String}
    @test levels(d1s[:, 4]) == ["c", "d"]

    d1s = stack(d1, [:d, :c], variable_eltype=Symbol)
    @test d1s.variable isa Vector{Symbol}
    @test levels(d1s.variable) == [:c, :d]
    d1s = stack(d1, [:d, :c], view=true, variable_eltype=Symbol)
    @test d1s.variable isa DataFrames.RepeatedVector{Symbol}
    @test levels(d1s.variable) == [:c, :d]
    @test d1s[:, 4] isa Vector{Symbol}
    @test levels(d1s[:, 4]) == [:c, :d]

    d2 = categorical(d1, :)
    levels!(d2.a, [2, 1, 3])
    ordered!(d2.a, true)
    ref_levels = shuffle!(unique([levels(d2.c); levels(d2.d)]))
    levels!(d2.c, ref_levels)
    ordered!(d2.c, true)
    levels!(d2.d, ref_levels)
    ordered!(d2.d, true)
    d2s = stack(d2, [:d, :c])
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
    df = DataFrame(rand(4,5))
    sdf = stack(df)
    @test eltype(sdf.variable) <: CategoricalValue{String}
    @test eltype(typeof(sdf.variable)) <: CategoricalValue{String}
    @test eltype(sdf.value) <: Float64
    @test eltype(typeof(sdf.value)) <: Float64
    sdf2 = first(sdf, 3)
    @test eltype(sdf2.variable) <: CategoricalValue{String}
    @test eltype(typeof(sdf2.variable)) <: CategoricalValue{String}
    @test eltype(sdf2.value) <: Float64
    @test eltype(typeof(sdf2.value)) <: Float64
end

end # module
