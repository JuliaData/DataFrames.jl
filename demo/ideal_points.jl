load("DataFrames")
using DataFrames

df = read_table("demo/senate112.csv")
senator_names = df[:, 425]

dm = DataMatrix(df[:, 1:424])

u, d, v = svd(dm, 2)

ideal_points = u * diagm(d)

df = DataFrame()
df["Senator"] = senator_names
df["X"] = DataVec(ideal_points[:, 1])
df["Y"] = DataVec(ideal_points[:, 2])

write_table(df, "demo/ideal_points.tsv")
