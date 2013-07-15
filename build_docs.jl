# Synchronize documentation files with the master branch
run(`git checkout master -- doc`)

# To build the PDF manual, first concatenate all of the sections together
# into one large markdown file. Split sections using the Pandoc-specific
#
# \newpage
#
# ---
#
# trick, which will insert horizontal bars and force a page separation
# between chapters in the manual.

pdf_sections = ["01_introduction.md",
				"02_getting_started.md",
				"03_io.md",
				"04_subsets.md",
				"05_joins_and_indexing.md",
				"06_split_apply_combine.md",
				"07_reshaping_and_pivoting.md",
				"08_datastreams.md",
				"09_formulas.md",
				"10_pooling.md"]

pandoc_page_break = "\n\n\\newpage\n\n---\n\n"

text = join(map(pdf_section -> readall(joinpath("doc", "sections", pdf_section)),
				pdf_sections),
			pandoc_page_break)

io = open("downloads/manual.md", "w")
write(io, text)
close(io)

run(`pandoc downloads/manual.md -o downloads/manual.pdf`)
run(`rm downloads/manual.md`)

# To build the website, generate Jekyll-ready Markdown files by appending
# a YAML header of the form
#
# ---
#
# layout: slate
# title: TITLE_GOES_HERE
# ---
#
#


web_sections = ["01_introduction.md",
				"02_getting_started.md",
				"03_io.md",
				"04_subsets.md",
				"05_joins_and_indexing.md",
				"06_split_apply_combine.md",
				"07_reshaping_and_pivoting.md",
				"08_datastreams.md",
				"09_formulas.md",
				"10_pooling.md"]

web_titles = ["Why Use the DataFrames Package?",
			  "Getting Started",
			  "I/O",
			  "Accessing Subsets of Data",
			  "Joins and Indexing",
			  "Split-Apply-Combine Operations",
			  "Reshaping and Pivoting",
			  "Streaming Data Analysis",
			  "Formulas",
			  "PooledDataArray"]

web_urls = ["introduction.md",
			"getting_started.md",
			"io.md",
			"subsets.md",
			"joins_and_indexing.md",
			"split_apply_combine.md",
			"reshaping_and_pivoting.md",
			"datastreams.md",
			"formulas.md",
			"pooling.md"]

n = length(web_sections)
if n != length(web_titles) || n != length(web_urls)
	error("Web sections, web titles and web URLS are out of sync")
end

for i in 1:n
	web_section = web_sections[i]
	web_title = web_titles[i]
	web_url = web_urls[i]
	io = open(web_url, "w")
	println(io, "---")
	println(io, "")
	println(io, "layout: minimal")
	println(io, "title: $(web_title)")
	println(io, "")
	println(io, "---")
	println(io, "")
	print(io, readall(joinpath("doc", "sections", web_section)))
	close(io)
end
