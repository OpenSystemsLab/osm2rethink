import os, pegs, strutils, streams, xmltree, xmlparser, stopwatch, asyncdispatch, tables, logging, json, times
import ../rethinkdb.nim/rethinkdb
setLogFilter(lvlNone)

let
  startNode = peg"\s* '<node'"
  endNode = peg"\s* '</node>'"
  startWay = peg"\s* '<way'"
  endWay = peg"\s* '</way>'"
  startRel = peg"\s* '<relation'"
  endRel = peg"\s* '</relation>'"


var
  buffer: StringStream
  xmlnode: XmlNode
  done = false
  ok = false
  r: RethinkClient
  counter = 0



if paramCount() < 1:
  quit "Usage: osm2rethink uri osm-path", QuitFailure
let path = paramStr(1)
if not fileExists(path):
  quit "File $# does not exists" % path, QuitFailure


r = newRethinkclient(db="osm")
waitFor r.connect()
r.repl()



proc signalHandler() {.noconv.} =
  done = true

setControlCHook(signalHandler)


proc processNode(node: XmlNode) {.async.} =
  var tags = newTable[string, MutableDatum]()
  for n in node.items:
    tags[n.attr("k")] = &n.attr("v")

  let
    id = parseInt(node.attr("id"))
    changeset = parseInt(node.attr("changeset"))
    long = parseFloat(node.attr("lon"))
    lat = parseFloat(node.attr("lat"))

  var ts = parse(node.attr("timestamp"), "yyyy-MM-ddThh:mm:ss")
  ts.timezone = 0

  var ret = await r.table("nodes").get(id).run()

  if ret.kind == JNull:
    discard await r.table("nodes").insert(&*{
      "id": id,
      "loc": r.point(long, lat),
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run()
  elif ret["changeset"].num != changeset:
    discard await r.table("nodes").get(id).update(&*{
      "loc": r.point(long, lat),
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run()
  else:
    discard await r.table("nodes").get(id).update(&*{"timestamp": ts}).run()

proc main() {.async.} =
  for line in path.lines:
    inc(counter)
    stdout.write("\r")
    stdout.write(counter)
    stdout.flushFile
    discard
    if line =~ startNode or line =~ startWay or line =~ startRel:
      buffer = newStringStream()
      if line.endsWith("/>"):
        ok = true

    if not ok and (line =~ endNode or line =~ endWay or line =~ endRel):
      ok = true

    if not isNil(buffer) and not isNil(buffer.data):
      buffer.write(line)

    if ok:
      ok = false
      buffer.setPosition(0)
      xmlnode = parseXml(buffer)
      buffer.close()
      if xmlnode.tag() == "node":
        discard
        await processNode(xmlnode)
      elif xmlnode.tag() == "way":
        echo "way"
      elif xmlnode.tag() == "relation":
        echo "rel"
  done = true
  r.close()

when isMainModule:
  var c: clock
  c.start()
  asyncCheck main()
  while not done:
    poll()
  c.stop()
  echo "\n"
  echo c.seconds, "s"
  echo "Speed: ", uint(counter/c.seconds.int), "lines/s"
