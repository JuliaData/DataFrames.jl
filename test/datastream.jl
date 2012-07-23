# Load files
load("src/init.jl")
load("src/datastream.jl")

#
# MatrixDataStream
#

# Create a MatrixDataStream
m = eye(3)
mds = MatrixDataStream(m)

# Initialize
i = start(mds)

# Row 1
(state, i) = next(mds, i)
done(mds, i)

# Row 2
(state, i) = next(mds, i)
done(mds, i)

# Row 3
(state, i) = next(mds, i)
done(mds, i)

# An explicit for loop
for row = mds
  println(row)
end

#
# CSVDataStream
#

cds = CSVDataStream("test/data/sample_data.csv")

line = start(cds)
(df, line) = next(cds, line)
done(cds, line)

(df, line) = next(cds, line)
done(cds, line)

(df, line) = next(cds, line)
done(cds, line)

for row in cds
  println(row)
end

#
# TSVDataStream
#

tds = TSVDataStream("test/data/sample_data.tsv")

line = start(tds)
(df, line) = next(tds, line)
done(tds, line)

(df, line) = next(tds, line)
done(tds, line)

(df, line) = next(tds, line)
done(tds, line)

for row in tds
  println(row)
end

#
# WSVDataStream
#

wds = WSVDataStream("test/data/sample_data.wsv")

line = start(wds)
(df, line) = next(wds, line)
done(wds, line)

(df, line) = next(wds, line)
done(wds, line)

(df, line) = next(wds, line)
done(wds, line)

for row in wds
  println(row)
end


#
# colmeans
#

Nrow = 55
d = DataFrame({randn(Nrow), randn(Nrow)})
ds = DataFrameDataStream(d, 10)
colwise(d, :mean)
colmeans(ds)
