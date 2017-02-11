N = 10
folders = ["definedtypes", "factors", "newlines", "quoting",
           "scaling", "separators", "typeinference"]

cd(Pkg.dir("DataTables", "test", "data")) do
    for folder in folders
        for filename in readdir(folder)
            relpath = joinpath(folder, filename)
            df = benchmark(() -> readtable(relpath),
                           "DataTable I/O",
                           relpath,
                           N)
            # TODO: Keep permanent record
            printtable(df, header=false)
        end
    end
end
