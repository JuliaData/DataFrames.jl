using Base.Test
using DataFrames

let
    N = 5000000
    x1 = rand(N)
    x2 = rand(N)
    df = DataFrame({x1, x2})
    cdf = cdataframe(df)

    function test_sum_1(d)
        res = 0.0
        for i = 1:nrow(d)
            res += d[i,"x1"] * d[i,"x2"]
        end
        res
    end

    function test_sum_2(d)
        res = 0.0
        for i = 1:nrow(d)
            res += d.x1[i] * d.x2[i]
        end
        res
    end

    function test_sum_3(x1,x2)
        res = 0.0
        for i = 1:length(x1)
            res += x1[i] * x2[i]
        end
        res
    end

    @time test_sum_1(df)
    @time test_sum_1(cdf)
    @time test_sum_2(cdf)
    @time test_sum_3(x1, x2)

end
