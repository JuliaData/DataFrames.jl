module TestPivot
    using Base.Test
    using DataFrames

    df = DataFrame(region   = ["US","US","US","US","EU","EU","EU","EU","US","US","US","US","EU","EU","EU","EU"],
                      product  = ["apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana","apple","apple","banana","banana"],
                      year     = [2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011,2010,2011],
                      produced = [3.3,3.2,2.3,2.1,2.7,2.8,1.5,1.3,  4.3,4.2,3.3,2.3,3.7,3.8,2.0,3.3],
                      consumed = [4.3,7.4,2.5,9.8,3.2,4.3,6.5,3.0,  5.3,7.4,3.5,9.8,4.2,6.3,8.5,4.0],
                      category = ['A','A','A','A','A','A','A','A', 'B','B','B','B','B','B','B','B',])
    longDf = stack(df,[:produced,:consumed])

    pivDf  = pivot(longDf, [:product, :region,], :year, :value,
                      ops    = [mean, var],
                      filter = Dict(:variable => [:consumed]),
                      sort   = [:product, (:region, true)]
             )

    pivDf_expected = wsv"""
    product  region op     2010 2011
    "apple"  "US"   "mean" 4.8  7.4
    "apple"  "US"   "var"  0.5  0.0
    "apple"  "EU"   "mean" 3.7  5.3
    "apple"  "EU"   "var"  0.5  2.0
    "banana" "US"   "mean" 3.0  9.8
    "banana" "US"   "var"  0.5  0.0
    "banana" "EU"   "mean" 7.5  3.5
    "banana" "EU"   "var"  2.0  0.5
    """
    rename!(pivDf_expected,Dict(:x2010 => Symbol(2010), :x2011 => Symbol(2011), ))

    @test pivDf == pivDf_expected

end
