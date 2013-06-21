using DataFrames

filenames = ["test/data/big_data.csv",
             "test/data/bool.csv",
             "test/data/complex_data.csv",
             "test/data/corrupt_utf8.csv",
             "test/data/corrupt_utf8_short.csv",
             "test/data/messy.csv",
             "test/data/movies.csv",
             "test/data/sample_data.csv",
             "test/data/simple_data.csv",
             "test/data/space_after_delimiter.csv",
             "test/data/space_around_delimiter.csv",
             "test/data/space_before_delimiter.csv",
             "test/data/types.csv",
             "test/data/windows.csv",
             "test/data/utf8.csv"]

for filename in filenames
	df = readtable(filename)
end

filename = "test/data/sample_data.tsv"
df = readtable(filename, separator = '\t')

filename = "test/data/sample_data.wsv"
df = readtable(filename, separator = ' ')

filename = "test/data/os9.csv"
df = readtable(filename, eol = '\r')

#
# Confirm that we can read a large file
#

df = DataFrame()

nrows, ncols = 100_000, 100

for j in 1:ncols
    df[j] = randn(nrows)
end

filename = tempname()

writetable(filename, df, separator = ',')

df1 = readtable(filename, separator = ',')

all(df .== df1)

#
# Lots of rows
#

df = DataFrame()

nrows, ncols = 1_000_000, 10

for j in 1:ncols
    df[j] = randn(nrows)
end

filename = tempname()

writetable(filename, df, separator = ',')

df1 = readtable(filename, separator = ',')

all(df .== df1)
