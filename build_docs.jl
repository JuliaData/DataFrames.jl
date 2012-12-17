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
				"03_design_details.md",
				"04_specification.md",
				"05_function_reference_guide.md"]

pandoc_page_break = "\n\n\\newpage\n\n---\n\n"

text = join(map(pdf_section -> readall(file_path("sections", pdf_section)),
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
				"03_design_details.md",
				"04_specification.md",
				"05_function_reference_guide.md"]

web_titles = ["Why Use the DataFrames Package?",
			  "Getting Started",
			  "The Design of DataFrames",
			  "Formal Specification of DataFrames",
			  "Function Reference Guide"]

web_urls = ["introduction.md",
			"getting_started.md",
			"design_details.md",
			"specification.md",
			"function_reference_guide.md"]

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
	println(io, "layout: slate")
	println(io, "title: $(web_title)")
	println(io, "")
	println(io, "---")
	println(io, "")
	print(io, readall(file_path("sections", web_section)))
	close(io)
end
