unsorted_dv = DataVector[2, 1, NA]
sorted_dv = DataVector[NA, 1, 2]

@assert isequal(sort(unsorted_dv), sorted_dv)
@assert isequal(order(unsorted_dv), [3, 2, 1])
# TODO: Make this work
# tiedrank(dv)

@assert first(unsorted_dv) == 2
@assert isna(last(unsorted_dv))
