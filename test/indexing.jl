module TestIndexing
    using Base.Test
    using DataFrames

    #
    # DataVector and PooledDataVector Indexing
    #

    dv = DataArray([1, 2])
    pdv = PooledDataArray([1, 2])

    adv_indices = {1,
                   1.0,
                   1:2,
                   1:1:2,
                   1.0:2.0,
                   1.0:1.0:2.0,
                   [1, 2],
                   [1.0, 2.0],
                   [true, true],
                   trues(2),
                   @data([1, 2]),
                   @data([1.0, 2.0]),
                   @data([true, true])}

    single_adv_indices = {1,
                          1.0}

    multi_adv_indices = {1:2,
                         1:1:2,
                         1.0:2.0,
                         1.0:1.0:2.0,
                         [1, 2],
                         [1.0, 2.0],
                         [true, true],
                         trues(2),
                         @data([1, 2]),
                         @data([1.0, 2.0]),
                         @data([true, true])}

    # avd[Index]
    for adv_index in adv_indices
        dv[adv_index]
        pdv[adv_index]
    end

    # avd[SingleIndex] = Single Value
    for single_adv_index in single_adv_indices
        dv[single_adv_index] = NA
        dv[single_adv_index] = 1
        dv[single_adv_index] = 1.0
        pdv[single_adv_index] = NA
        pdv[single_adv_index] = 1
        pdv[single_adv_index] = 1.0
    end

    # avd[MultiIndex] = Multiple Values
    for multi_adv_index in multi_adv_indices
        dv[multi_adv_index] = [1, 2]
        dv[multi_adv_index] = [1.0, 2.0]
        dv[multi_adv_index] = 1:2
        dv[multi_adv_index] = 1:1:2
        dv[multi_adv_index] = 1.0:2.0
        dv[multi_adv_index] = 1.0:1.0:2.0
        pdv[multi_adv_index] = [1, 2]
        pdv[multi_adv_index] = [1.0, 2.0]
        pdv[multi_adv_index] = 1:2
        pdv[multi_adv_index] = 1:1:2
        pdv[multi_adv_index] = 1.0:2.0
        pdv[multi_adv_index] = 1.0:1.0:2.0
    end

    # avd[MultiIndex] = Single Value
    for multi_adv_index in multi_adv_indices
        dv[multi_adv_index] = 1
        dv[multi_adv_index] = 1.0
        pdv[multi_adv_index] = 1
        pdv[multi_adv_index] = 1.0
    end

    #
    # DataFrame indexing
    #

    df = DataFrame(A = 1:2, B = 3:4)

    row_indices = Any[1,
                      1.0,
                      1:2,
                      [1, 2],
                      [1.0, 2.0],
                      [true, true],
                      trues(2),
                      @data([1, 2]),
                      @data([1.0, 2.0]),
                      @data([true, true]),
                      Colon()]

    column_indices = Any[1,
                         1.0,
                         "A",
                         1:2,
                         [1, 2],
                         [1.0, 2.0],
                         [true, false],
                         trues(2),
                         ["A", "B"],
                         @data([1, 2]),
                         @data([1.0, 2.0]),
                         @data([true, false]),
                         @data(["A", "B"]),
                         :(names(_DF) .== "B"),
                         Colon()]

    #
    # getindex()
    #

    for column_index in column_indices
        df[column_index]
    end

    for row_index in row_indices
        for column_index in column_indices
            df[row_index, column_index]
        end
    end

    #
    # setindex!()
    #

    for column_index in column_indices
        df[column_index] = df[column_index]
    end

    for row_index in row_indices
        for column_index in column_indices
            df[row_index, column_index] = df[row_index, column_index]
        end
    end

    #
    # Broadcasting assignments
    #

    for column_index in column_indices
        df[column_index] = NA
        df[column_index] = 1
        df[column_index] = 1.0
        df[column_index] = "A"
        df[column_index] = DataArray([1 + 0im, 2 + 1im])
    end

    # Only assign into columns for which new value is type compatible
    for row_index in row_indices
        for column_index in column_indices
            df[row_index, column_index] = NA
            df[row_index, column_index] = 1
            df[row_index, column_index] = 1.0
            df[row_index, column_index] = "A"
            df[row_index, column_index] = DataArray([1 + 0im, 2 + 1im])
        end
    end
end
