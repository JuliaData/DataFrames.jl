module TestJoin
    using Base.Test
    using DataFrames
    using DataArrays

    name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

    outer = DataFrame(ID = [1, 2, 3, 4],
                      Name = @data(["John Doe", "Jane Doe", "Joe Blogs", NA]),
                      Job = @data(["Lawyer", "Doctor", NA, "Farmer"]))

    # Tests use current column ordering but don't promote it
    right = outer[!isna(outer[:Job]), [:Name, :ID, :Job]]
    left = outer[!isna(outer[:Name]), :]
    inner = left[!isna(left[:Job]), :]
    semi = inner[:, [:ID, :Name]]
    anti = left[isna(left[:Job]), [:ID, :Name]]
    
    @test isequal(join(name, job), inner)
    @test isequal(join(name, job, kind = :inner), inner)
    @test isequal(join(name, job, kind = :outer), outer)
    @test isequal(join(name, job, kind = :left), left)
    @test isequal(join(name, job, kind = :right), right)
    @test isequal(join(name, job, kind = :semi), semi)
    @test isequal(join(name, job, kind = :anti), anti)

    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    # Join key detection expects a single shared column
    @test_throws join(df1, df2)

    join(df1, df2, on = [:A, :B])
end