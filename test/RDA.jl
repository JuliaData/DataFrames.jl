module TestRDA
    using Base.Test
    using DataFrames

    # R code generating test .rdas
        # df = data.frame(num=c(1.1, 2.2))
        # save(df, file='minimal.rda')
        # save(df, file='minimal_ascii.rda', ascii=TRUE)

        # df['int'] = c(1L, 2L)
        # df['logi'] = c(TRUE, FALSE)
        # df['chr'] = c('ab', 'c')
        # df['factor'] = factor(df$chr)
        # df['cplx'] = complex( real=c(1.1,0.0), imaginary=c(0.5,1.0) )
        # #utf=c('Ж', '∰')) R handles it, read_rda doesn't.
        # save(df, file='types.rda')

        # df[2, ] = NA
        # df[3, ] = df[2, ]
        # df[3,'num'] = NaN
        # df[,'cplx'] = complex( real=c(1.1,1,NaN), imaginary=c(NA,NaN,0) )
        # df['chr'] = NULL  # NA characters breaking read_rda
        # save(df, file='NAs.rda')

        # names(df) = c('end', '!', '1', '%_B*\tC*')
        # save(df, file='names.rda')

    testdir = dirname(@__FILE__)

    df = DataFrame(num = [1.1, 2.2])
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/minimal.rda")["df"]), df)
    @test isequal(DataFrame(open(read_rda,"$testdir/data/RDA/minimal_ascii.rda")["df"]), df)
    @test isequal(read_rda("$testdir/data/RDA/minimal.rda",convertdataframes=true)["df"], df)

    df[:int] = Int32[1, 2]
    df[:logi] = [true, false]
    df[:chr] = ["ab", "c"]
    df[:factor] = pool(df[:chr])
    df[:cplx] = complex128([1.1+0.5im, 1.0im])
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/types.rda")["df"]), df)

    df[2, :] = NA
    append!(df, df[2, :])
    df[3, :num] = NaN
    df[:, :cplx] = @data [NA, complex128(1,NaN), NaN]
    df = df[:, [:num, :int, :logi, :factor, :cplx]]  # (NA) chr breaks read_rda
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/NAs.rda")["df"]), df)

    rda_names = names(DataFrame(read_rda("$testdir/data/RDA/names.rda")["df"]))
    @test rda_names == [:_end, :x!, :x1, :_B_C_]

end
