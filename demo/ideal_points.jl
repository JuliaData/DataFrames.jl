load("DataFrames")
using DataFrames

df = read_table("demo/senate112.csv")
senator_names = df[:, 425]

dm = DataMatrix(df[:, 1:424])

imputed_df, u, d, v = svd(dm, 2)

u = u * diagm(d)

open("demo/ideal_points.tsv", "w") do f
  for i = 1:nrow(df)
    println(f, join([senator_names[i], string(u[i, 1]), string(u[i, 2])], "\t"))
  end
end
