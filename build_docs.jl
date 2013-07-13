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

pdf_sections = ["00_table_of_contents.md",
				"01_introduction.md",
				"02_getting_started.md",
				"10_io.md",
				"03_design_details.md",
				"04_specification.md",
				"05_function_reference_guide.md",
				"06_merging_and_indexing.md",
				"07_reshaping_and_pivoting.md",
				"08_split_apply_combine.md",
				"09_datastreams.md"]

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
				"10_io.md",
				"03_design_details.md",
				"04_specification.md",
				"05_function_reference_guide.md",
				"06_merging_and_indexing.md",
				"07_reshaping_and_pivoting.md",
				"08_split_apply_combine.md",
				"09_datastreams.md"]

web_titles = ["Why Use the DataFrames Package?",
			  "Getting Started",
			  "IO",
			  "The Design of DataFrames",
			  "Formal Specification of DataFrames",
			  "Function Reference Guide",
			  "Merging and Indexing",
			  "Reshaping and Pivoting",
			  "Split-Apply-Combine Operations",
			  "Streaming Data Analysis"]

web_urls = ["introduction.md",
			"getting_started.md",
			"io.md",
			"design_details.md",
			"specification.md",
			"function_reference_guide.md",
			"merging_and_indexing.md",
			"reshaping_and_pivoting.md",
			"split_apply_combine.md",
			"datastreams.md"]

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
