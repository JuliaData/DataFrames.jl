
using Docile, Docile.Interface, Lexicon, DataFrames


myfilter(x::Module; files = [""]) = filter(metadata(x), files = files, categories = [:comment, :module, :function, :method, :type, :typealias, :macro, :global])
myfilter(x::Metadata; files = [""]) = filter(x, files = files, categories = [:comment, :module, :function, :method, :type, :typealias, :macro, :global])

# Stuff from Lexicon.jl:
writeobj(any) = string(any)
writeobj(m::Method) = first(split(string(m), "("))
# from base/methodshow.jl
function url(m)
    line, file = m
    try
        d = dirname(file)
        u = Pkg.Git.readchomp(`config remote.origin.url`, dir=d)
        u = match(Pkg.Git.GITHUB_REGEX,u).captures[1]
        root = cd(d) do # dir=d confuses --show-toplevel, apparently
            Pkg.Git.readchomp(`rev-parse --show-toplevel`)
        end
        if beginswith(file, root)
            commit = Pkg.Git.readchomp(`rev-parse HEAD`, dir=d)
            return "https://github.com/$u/tree/$commit/"*file[length(root)+2:end]*"#L$line"
        else
            return Base.fileurl(file)
        end
    catch
        return Base.fileurl(file)
    end
end


function mysave(file::String, m::Module, order = [:source])
    mysave(file, documentation(m), order)
end
function mysave(file::String, docs::Metadata, order = [:source])
    isfile(file) || mkpath(dirname(file))
    open(file, "w") do io
        info("writing documentation to $(file)")
        println(io)
        for (k,v) in EachEntry(docs, order = order)
            name = writeobj(k)
            source = v.data[:source]
            catgory = category(v)
            comment = catgory == :comment
            println(io)
            println(io)
            !comment && println(io, "## $name")
            println(io)
            println(io, v.docs.data)
            path = last(split(source[2], r"v[\d\.]+(/|\\)"))
            !comment && println(io, "[$(path):$(source[1])]($(url(source)))")
            println(io)
        end
    end
end


mysave("api/reshaping.md", myfilter(DataFrames, files = ["abstractdataframe/reshape.jl"]))

