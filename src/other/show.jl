import IJulia: show, display
_css = "
<style>
table.classic {
  background-color: #EEEEEE;
  width: 100%;
  text-align: left;
  border-collapse: collapse;
}
table.classic td, table.classic th {
  border: 1px solid #AAAAAA;
  padding: 3px 2px;
}
table.classic tbody td {
  font-size: 17px;
  font-weight: bold;
}
table.classic tr:nth-child(even) {
  background: #FFFFFF;
}
table.classic thead {
  background: #6B0986;
}
table.classic thead th {
  font-size: 18px;
  font-weight: bold;
  color: #FFFFFF;
  text-align: center;
}
table.classic tfoot {
  font-size: 14px;
  font-weight: bold;
  color: #FFFFFF;
  background: #D0E4F5;
  background: -moz-linear-gradient(top, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
  background: -webkit-linear-gradient(top, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
  background: linear-gradient(to bottom, #dcebf7 0%, #d4e6f6 66%, #D0E4F5 100%);
  border-top: 2px solid #444444;
}
table.classic tfoot td {
  font-size: 14px;
}
table.classic tfoot .links {
  text-align: right;
}
table.classic tfoot .links a{
  display: inline-block;
  background: #1C6EA4;
  color: #FFFFFF;
  padding: 2px 8px;
  border-radius: 5px;
}

table.minimalist {
  border: 3px solid #000000;
  width: 100%;
  text-align: left;
  border-collapse: collapse;
}
table.minimalist td, table.minimalist th {
  border: 1px solid #000000;
  padding: 5px 4px;
}
table.minimalist tbody td {
  font-size: 13px;
}
table.minimalist thead {
  background: #CFCFCF;
  background: -moz-linear-gradient(top, #dbdbdb 0%, #d3d3d3 66%, #CFCFCF 100%);
  background: -webkit-linear-gradient(top, #dbdbdb 0%, #d3d3d3 66%, #CFCFCF 100%);
  background: linear-gradient(to bottom, #dbdbdb 0%, #d3d3d3 66%, #CFCFCF 100%);
  border-bottom: 3px solid #000000;
}
table.minimalist thead th {
  font-size: 15px;
  font-weight: bold;
  color: #000000;
  text-align: left;
}
table.minimalist tfoot {
  font-size: 14px;
  font-weight: bold;
  color: #000000;
  border-top: 3px solid #000000;
}
table.minimalist tfoot td {
  font-size: 14px;
}
table.steel {
  border: 4px solid #555555;
  background-color: #555555;
  width: 400px;
  text-align: center;
  border-collapse: collapse;
}
table.steel td, table.steel th {
  border: 1px solid #555555;
  padding: 5px 10px;
}
table.steel tbody td {
  font-size: 12px;
  font-weight: bold;
  color: #FFFFFF;
}
table.steel td:nth-child(even) {
  background: #398AA4;
}
table.steel thead {
  background: #398AA4;
  border-bottom: 10px solid #398AA4;
}
table.steel thead th {
  font-size: 15px;
  font-weight: bold;
  color: #FFFFFF;
  text-align: left;
  border-left: 2px solid #398AA4;
}
table.steel thead th:first-child {
  border-left: none;
}

table.steel tfoot td {
  font-size: 13px;
}
table.steel tfoot .links {
  text-align: right;
}
table.steel tfoot .links a{
  display: inline-block;
  background: #FFFFFF;
  color: #398AA4;
  padding: 2px 8px;
  border-radius: 5px;
}
</style>
"
function show(df::DataFrame; style = "")
    thead = "<thead>
<tr>"
    tfoot = """<tfoot>
<tr>
<td colspan="4">
<div class="links"><a href="#">&laquo;</a> <a class="active" href="#">1</a>
 <a href="#">2</a> <a href="#">3</a> <a href="#">4</a> <a href="#">&raquo;</a>
 </div>
</td>
</tr>
</tfoot>"""
    tbody = "<tbody>"
    for col in zip(names(df), eachcol(df))
        thead = string(thead, "<th>", string(col[1]), "</th>")
        tbody = string(tbody, "<tr>")
        for i in col[2]
            tbody = string(tbody, "<td>", i, "</td>")
        end
        tbody = string(tbody, "</tr>")
    end
    compisition = string(_css,"<table class = \"", style, "\">",
     thead, tbody, tfoot, "</table>")
    display("text/html", compisition)
end
