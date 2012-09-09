# Load DataFrame package.
load("src/init.jl")
load("src/svd.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/senate112.csv")
senator_names = df[:, 425]

df = df[:, 1:424]

imputed_df, u, d, v = missing_svd(df, 2)

u = u * diagm(d)

f = open("demo/ideal_points.tsv", "w")
for i = 1:nrow(df)
  println(f, join([senator_names[i], string(u[i, 1]), string(u[i, 2])], "\t"))
end
close(f)
