function make_help_index(manual_name::String, output_file_dir::String)
    #
    
    output_file_name = file_path(output_file_dir, "_JL_INDEX_")
    #    
    f = open(output_file_name, "w")
    println(f, "DataFrames  manual  A package for working with tabular data in Julia")
    manlines = open(readlines, manual_name)
    # document manual headings
    funcrefloc = 0
    for idx in 1:length(manlines)
        m = match(r"^# (.*)", manlines[idx]) # find top-level headings
        if m != nothing
            title = m.captures[1]
            bookmark = lowercase(title)
            bookmark = replace(bookmark, r"[\']+", "") # delete apostrophies
            bookmark = replace(bookmark, r"[^\w]+", "-") # replace whitespace and punctuation with "-"
            keyword = "DataFrames-" * bookmark
            println(f, keyword, "  manual#", bookmark, "  ", title)
            funcrefloc = idx
        end
    end
    # document the function reference
    bookmark = ""
    for idx in funcrefloc:length(manlines)
        m = match(r"^## (.*)", manlines[idx]) # find 2nd-level headings
        if m != nothing
            title = m.captures[1]
            bookmark = lowercase(title)
            bookmark = replace(bookmark, r"[']+", "") # delete apostrophies
            bookmark = replace(bookmark, r"[^\w]+", "-") # replace whitespace and punctuation with "-"
            keyword = "DataFrames-Reference-" * bookmark
            println(f, keyword, "  manual#", bookmark, "  ", title)
        end
        m = match(r"^#### (.*)", manlines[idx]) # find functions in 4th-level headings
        if m != nothing
            title = m.captures[1]
            m = match(r"`(\w+\([^`]+\))`", title)
            if m != nothing
                for kdx in 1:length(m.captures)
                    title = m.captures[kdx]
                    keyword = replace(title, r"\(.*", "")
                    println(f, keyword, "  manual#", bookmark, "  ", title)
                end
            end
        end
    end
    close(f)
    
end
make_help_index(name::String) = make_help_index(name, file_path(julia_pkgdir(), "DataFrames", "doc"))
make_help_index() = make_help_index(file_path(julia_pkgdir(), "DataFrames", "doc", "manual.md"), file_path(julia_pkgdir(), "DataFrames", "doc"))

manual_name = file_path(julia_pkgdir(), "DataFrames", "doc", "manual.md")
output_file_dir = file_path(julia_pkgdir(), "DataFrames", "doc")
