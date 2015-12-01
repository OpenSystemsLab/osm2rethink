import os, pegs, strutils, streams, xmltree, xmlparser, stopwatch, tables, logging, json, times, threadpool
import ../rethinkdb.nim/rethinkdb
setLogFilter(lvlWarn)
{.experimental.}
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
  counter = 0



if paramCount() < 1:
  quit "Usage: osm2rethink uri osm-path", QuitFailure
let path = paramStr(1)
if not fileExists(path):
  quit "File $# does not exists" % path, QuitFailure

var
  r {.threadvar.}: RethinkClient

proc signalHandler() {.noconv.} =
  done = true
setControlCHook(signalHandler)

proc parseTime(t: string): TimeInfo =
  if t =~ peg"{\d+} '-' {\d+} '-' {\d+} 'T' {\d+} ':' {\d+} ':' {\d+}":
    result.year = parseInt(matches[0])
    result.month = cast[Month](parseInt(matches[1])-1)
    result.monthday = parseInt(matches[2])
    result.hour = parseInt(matches[3])
    result.minute = parseInt(matches[4])
    result.second = parseInt(matches[5])

proc processNode(node: XmlNode) {.thread.} =
  if r.isNil:
    r = newRethinkclient(db="osm")
    r.connect()

  var tags = newTable[string, MutableDatum]()
  for n in node.items:
    tags[n.attr("k")] = &n.attr("v")

  let
    id = parseInt(node.attr("id"))
    version = parseInt(node.attr("version"))
    changeset = parseInt(node.attr("changeset"))
    long = parseFloat(node.attr("lon"))
    lat = parseFloat(node.attr("lat"))
    ts = parseTime(node.attr("timestamp"))

  var ret = r.table("nodes").get(id).run(r)

  if ret.kind == JNull:
    discard r.table("nodes").insert(&*{
      "id": id,
      "loc": r.point(long, lat),
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run(r, durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard r.table("nodes").get(id).update(&*{
      "loc": r.point(long, lat),
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "tags": tags
    }).run(r, durability="soft", noreply=true)

proc processWay(node: XmlNode) {.thread.} =
  if r.isNil:
    r = newRethinkclient(db="osm")
    r.connect()
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
    ts = parseTime(node.attr("timestamp"))

  var ret = r.table("ways").get(id).run(r)

  if ret.kind == JNull:
    discard r.table("ways").insert(&*{
      "id": id,
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "nodes": nodes
    }).run(r, durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard  r.table("ways").get(id).update(&*{
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "nodes": nodes
    }).run(r, durability="soft", noreply=true)

proc processRelation(node: XmlNode) {.thread.} =
  if r.isNil:
    r = newRethinkclient(db="osm")
    r.connect()

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
    ts = parseTime(node.attr("timestamp"))

  var ret = r.table("relations").get(id).run(r)

  if ret.kind == JNull:
    discard r.table("relations").insert(&*{
      "id": id,
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "members": members
    }).run(r, durability="soft", noreply=true)
  elif ret["changeset"].num != changeset:
    discard r.table("relations").get(id).update(&*{
      "version": version,
      "timestamp": ts,
      "changeset": changeset,
      "members": members
    }).run(r, durability="soft", noreply=true)

proc main() =
  parallel:
    for line in path.lines:
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
          spawn processNode(xmlnode)
        elif xmlnode.tag() == "way":
          spawn processWay(xmlnode)
        elif xmlnode.tag() == "relation":
          spawn processRelation(xmlnode)
  sync()

when isMainModule:
  setMinPoolSize(8)
  var c: clock
  c.start()
  main()
  c.stop()
  echo "\n"
  echo c.seconds, "s"
  echo "Speed: ", uint(counter/c.seconds.int), "lines/s"
