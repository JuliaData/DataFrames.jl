module TestRDA
    using Base.Test
    using DataFrames
    using Compat

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
        # save(df, file='types_ascii.rda', ascii=TRUE)

        # df[2, ] = NA
        # df[3, ] = df[2, ]
        # df[3,'num'] = NaN
        # df[,'cplx'] = complex( real=c(1.1,1,NaN), imaginary=c(NA,NaN,0) )
        # save(df, file='NAs.rda')
        # save(df, file='NAs_ascii.rda', ascii=TRUE)

        # names(df) = c('end', '!', '1', '%_B*\tC*', NA, 'x')
        # save(df, file='names.rda')
        # save(df, file='names_ascii.rda', ascii=TRUE)

    testdir = dirname(@__FILE__)

    df = DataFrame(num = [1.1, 2.2])
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/minimal.rda")["df"]), df)
    @test isequal(DataFrame(open(read_rda,"$testdir/data/RDA/minimal_ascii.rda")["df"]), df)
    @test isequal(read_rda("$testdir/data/RDA/minimal.rda",convertdataframes=true)["df"], df)

    df[:int] = Int32[1, 2]
    df[:logi] = [true, false]
    df[:chr] = ["ab", "c"]
    df[:factor] = pool(df[:chr])
    df[:cplx] = Complex128[1.1+0.5im, 1.0im]
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/types.rda")["df"]), df)
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/types_ascii.rda")["df"]), df)

    df[2, :] = NA
    append!(df, df[2, :])
    df[3, :num] = NaN
    df[:, :cplx] = @data [NA, @compat(Complex128(1,NaN)), NaN]
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/NAs.rda")["df"]), df)
    # ASCII format saves NaN as NA
    df[3, :num] = NA
    df[:, :cplx] = @data [NA, NA, NA]
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/NAs_ascii.rda")["df"]), df)

    rda_names = names(DataFrame(read_rda("$testdir/data/RDA/names.rda")["df"]))
    expected_names = [:_end, :x!, :x1, :_B_C_, :x, :x_1]
    @test rda_names == expected_names
    rda_names = names(DataFrame(read_rda("$testdir/data/RDA/names_ascii.rda")["df"]))
    @test rda_names == [:_end, :x!, :x1, :_B_C_, :x, :x_1]

end
