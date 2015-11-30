import os, pegs, strutils, streams, xmltree, xmlparser, stopwatch, asyncdispatch, tables, logging, json, times
import ../rethinkdb.nim/rethinkdb
setLogFilter(lvlWarn)

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
    version = parseInt(node.attr("version"))
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
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run(durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard await r.table("nodes").get(id).update(&*{
      "loc": r.point(long, lat),
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run(durability="soft", noreply=true)
#  else:
#    discard await r.table("nodes").get(id).update(&*{"timestamp": ts}).run()

proc processWay(node: XmlNode) {.async.} =
  var nodes: seq[int] = @[]
  var tags = newTable[string, MutableDatum]()
  for n in node.items:
    if n.tag() == "tag":
      tags[n.attr("k")] = &n.attr("v")
    if n.tag() == "nd":
      nodes.add(parseInt(n.attr("ref")))

  let
    id = parseInt(node.attr("id"))
    version = parseInt(node.attr("version"))
    changeset = parseInt(node.attr("changeset"))
  var ts = parse(node.attr("timestamp"), "yyyy-MM-ddThh:mm:ss")
  ts.timezone = 0

  var ret = await r.table("ways").get(id).run()

  if ret.kind == JNull:
    discard await r.table("ways").insert(&*{
      "id": id,
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "nodes": nodes
    }).run(durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard await r.table("ways").get(id).update(&*{
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "nodes": nodes
    }).run(durability="soft", noreply=true)

proc processRelation(node: XmlNode) {.async.} =
  var members: seq[MutableDatum] = @[]
  var tags = newTable[string, MutableDatum]()

  var
    typ, role: string
    referrence: int
  for n in node.items:
    if n.tag() == "tag":
      tags[n.attr("k")] = &n.attr("v")
    if n.tag() == "member":
      typ = n.attr("type")
      referrence = parseInt(n.attr("ref"))
      role = n.attr(role)
      members.add(&*{"type": typ, "ref": referrence, "role": role})

  let
    id = parseInt(node.attr("id"))
    version = parseInt(node.attr("version"))
    changeset = parseInt(node.attr("changeset"))
  var ts = parse(node.attr("timestamp"), "yyyy-MM-ddThh:mm:ss")
  ts.timezone = 0

  var ret = await r.table("relations").get(id).run()

  if ret.kind == JNull:
    discard await r.table("relations").insert(&*{
      "id": id,
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "members": members
    }).run(durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard await r.table("relations").get(id).update(&*{
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "members": members
    }).run(durability="soft", noreply=true)
  else:
    discard

proc main() {.async.} =
  for line in path.lines:
    #inc(counter)
    #stdout.write("\r")
    #stdout.write(counter)
    #stdout.flushFile
    if match(line, startNode) or match(line, startWay) or match(line, startRel):
      buffer = newStringStream()
      if line.endsWith("/>"):
        ok = true

    if not ok and (match(line, endNode) or match(line, endWay) or match(line, endRel)):
      ok = true

    if not isNil(buffer) and not isNil(buffer.data):
      buffer.write(line)

    if ok:
      ok = false
      buffer.setPosition(0)
      xmlnode = parseXml(buffer)
      buffer.close()
      if xmlnode.tag() == "node":
        await processNode(xmlnode)
      elif xmlnode.tag() == "way":
        await processWay(xmlnode)
      elif xmlnode.tag() == "relation":
        await processRelation(xmlnode)
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
