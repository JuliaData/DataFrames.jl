require("DataFrames")
using DataFrames

df = DataFrame(quote
                 A = 1:2
                 B = 3:4
               end)

row_indices = {1,
               1.0,
               1:2,
               [1, 2],
               [1.0, 2.0],
               [true, false],
               trues(2),
               DataVec[1, 2],
               DataVec[1.0, 2.0],
               DataVec[true, false],
               :(A .== 1)}

column_indices = {1,
                  1.0,
                  "A",
                  1:2,
                  [1, 2],
                  [1.0, 2.0],
                  [true, false],
                  trues(2),
                  ["A", "B"],
                  DataVec[1, 2],
                  DataVec[1.0, 2.0],
                  DataVec[true, false],
                  DataVec["A", "B"],
                  :(colmeans(_DF) .< 4)}

#
# ref()
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
# assign()
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
    df[column_index] = 1
    df[column_index] = 1.0
    df[column_index] = "A"
    # Mass assign a whole new column to one or more columns
end
 
# Only assign into columns for which new value is type compatible
for row_index in row_indices
  for column_index in column_indices
    df[row_index, column_index] = 1
    df[row_index, column_index] = 1.0
    df[row_index, column_index] = "A"
  end
end
