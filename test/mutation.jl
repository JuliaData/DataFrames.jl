module TestMutation
    using Base.Test, DataFrames

    # columns should not alias if scalar broadcasted
    df = DataFrame(A=[0],B=[0])
    df[1:end] = 0.0
    df[1,:A] = 1.0
    @test df[1,:B] === 0.0

    # columns should not alias if vector assigned
    df = DataFrame(A=[0],B=[0])
    df[1:end] = [0.0]
    df[1,:A] = 1.0
    @test df[1,:B] === 0.0

    # columns should not alias if DataVector assigned
    df = DataFrame(A=[0],B=[0])
    df[:A] = df[:B]
    df[1,:A] = 1
    @test df[1,:B] === 0
end
