import os, pegs, strutils, streams, xmltree, xmlparser, stopwatch, asyncdispatch, tables
import ../rethinkdb.nim/rethinkdb

let
  startNode = peg"\s* '<node'"
  endNode = peg"\s* '</node>'"


var
  buffer: StringStream
  node: XmlNode
  ok = false

if paramCount() < 1:
  quit "Usage: osm2rethink uri osm-path", QuitFailure
let path = paramStr(1)
if not fileExists(path):
  quit "File $# does not exists" % path, QuitFailure


var c: clock

bench(c):

  var r = newRethinkclient(db="osm")
  waitFor r.connect()
  r.repl()
  for line in path.lines:
    if line =~ startNode:
      buffer = newStringStream()
      if line.endsWith("/>"):
        ok = true

    if line =~ endNode:
      ok = true

    if not isNil(buffer) and not isNil(buffer.data):
      buffer.write(line)

    if ok:
      ok = false
      buffer.setPosition(0)
      node = parseXml(buffer)
      echo node.attr("id")
      var tags = newTable[string, MutableDatum]()
      for n in node.items:
        tags[n.attr("k")] = &n.attr("v")

      discard waitFor r.table("nodes").insert([&*{
        "id": parseInt(node.attr("id")),
        "loc": r.point(parseFloat(node.attr("lat")), parseFloat(node.attr("lon"))),
        "timestamp": node.attr("timestamp"),
        "changeset": parseInt(node.attr("changeset")),
        "tags": tags
      }]).run()
      buffer.close()

  r.close()

echo c.seconds, "s"
