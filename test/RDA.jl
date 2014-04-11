module TestRDA
    using Base.Test
    using DataFrames

    # R code generating test .rdas
        # df = data.frame(num=c(1.1, 2.2))
        # save(df, file='minimal.rda')

        # df['int'] = c(1L, 2L)
        # df['logi'] = c(TRUE, FALSE)
        # df['chr'] = c('ab', 'c')
        # df['factor'] = factor(df$chr)
        # #utf=c('Ж', '∰')) R handles it, read_rda doesn't.
        # save(df, file='types.rda')

        # df[2, ] = NA
        # df['chr'] = NULL  # NA characters breaking read_rda
        # save(df, file='NAs.rda')

        # names(df) = c('end', '!', '1', '%_B*\tC*')
        # save(df, file='names.rda')

    testdir = dirname(@__FILE__)

    df = DataFrame(num = [1.1, 2.2])
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/minimal.rda")["df"]), df)

    df[:int] = Int32[1, 2]
    df[:logi] = [true, false]
    df[:chr] = ["ab", "c"]
    df[:factor] = pool(df[:chr])
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/types.rda")["df"]), df)

    df[2, :] = NA
    df = df[:, [:num, :int, :logi, :factor]]  # (NA) chr breaks read_rda
    @test isequal(DataFrame(read_rda("$testdir/data/RDA/NAs.rda")["df"]), df)

    rda_names = names(DataFrame(read_rda("$testdir/data/RDA/names.rda")["df"]))
    @test rda_names == [:_end, :x!, :x1, :B_C]
end
