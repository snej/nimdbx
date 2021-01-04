# test3_Index.nim

{.experimental: "notnil".}

import unittest, strutils, sets, tables

import nimdbx


const DBPath = "test_db"
const CollectionName = "stuff"


iterator tokenize2*(s: string, seps: set[char] = Whitespace): tuple[
  token: string, start: int] =
  var i = 0
  while true:
    var j = i
    var isSep = j < s.len and s[j] in seps
    while j < s.len and (s[j] in seps) == isSep: inc(j)
    if j > i:
      if not isSep:
        yield (substr(s, i, j-1), i)
    else:
      break
    i = j

suite "Indexes":
    var db: Database
    var coll: Collection not nil
    var index: Index

    setup:
        eraseDatabase(DBPath)
        db = openDatabase(DBPath)
        coll = db.createCollection(CollectionName)

    teardown:
        if db != nil:
            db.closeAndDelete()

    proc addSomething() =
        coll.inTransaction do (ct: CollectionTransaction):
            ct.put("foo",   "I am the value of foo")
            ct.put("splat", "I am splat's value")
            ct.commit

    proc createLengthIndex() =
        index = coll.openIndex("lengths") do (value: DataOut, emit: EmitFunc):
            #debugEcho "INDEXING ", ($value).escape
            emit collatable(($value).len)

    proc createWordIndex() =
        ## Creates an index on the words in the source collection; a simple form of full-text search.
        ## In the index, each word maps to its position in the string.
        const Delimiters = Whitespace + {'.', ',', ';', ':', '"', '-'}
        const Stopwords = toHashSet(["a", "an", "and", "i", "of", "or", "the", "to"])
        proc tokenizeForIndex(sentence: string): Table[string, int] =
            ## Breaks a string into unique lowercase words, ignoring noise words like "the".
            for word, start in tokenize2(sentence, Delimiters):
                let word = word.toLower
                if not Stopwords.contains(word):
                    discard result.hasKeyOrPut(word, start)

        index = coll.openIndex("words") do (value: DataOut, emit: EmitFunc):
            for word, start in tokenizeForIndex(value):
                emit collatable(word), collatable(start)

    proc dumpIndex(snap: CollectionSnapshot): (seq[string], seq[string]) =
        var keys, vals: seq[string]
        for key, value in snap:
            echo "  ", key.asCollatable, " -> ", value.asCollatable
            keys.add($(key.asCollatable))
            vals.add($(value.asCollatable))
        return (keys, vals)


    test "Index populated DB":
        addSomething()
        createLengthIndex()
        check index.updateCount == 0

        let snap = index.beginSnapshot()
        check snap.entryCount == 2

        var (keys, vals) = dumpIndex(snap)
        check keys == @["[18]",  "[21]"]
        check vals == @["""["splat"]""", """["foo"]"""]


    test "Populate DB then index":
        createLengthIndex()
        addSomething()
        check index.updateCount == 2

        let snap = index.beginSnapshot()
        check snap.entryCount == 2

        var (keys, vals) = dumpIndex(snap)
        check keys == @["[18]",  "[21]"]
        check vals == @["""["splat"]""", """["foo"]"""]


    test "Update DB":
        addSomething()
        createLengthIndex()

        coll.inTransaction do (ct: CollectionTransaction):
            # Update, add, delete keys:
            check ct.update("foo", "bar")
            ct.put("longer", "I am the very model of a modern Major General.")
            check ct.del("splat")
            #check index.updateCount == 3

            # Changes that do not affect the index:
            check ct.update("foo", "bar")
            #check index.updateCount == 3
            check ct.update("foo", "rab")
            #check index.updateCount == 3
            discard ct.del("missing")
            #check index.updateCount == 3

            ct.commit

        let snap = index.beginSnapshot()
        var (keys, vals) = dumpIndex(snap)
        check keys == @["[3]",  "[46]"]
        check vals == @["""["foo"]""", """["longer"]"""]


    test "Multi-Emit":
        addSomething()
        createWordIndex()
        let snap = index.beginSnapshot()
        var (keys, vals) = dumpIndex(snap)
        check keys == @["[\"am\"]", "[\"am\"]", "[\"foo\"]", "[\"splat\\'s\"]", "[\"value\"]", "[\"value\"]"]
        check vals == @["""[2, "foo"]""", """[2, "splat"]""", """[18, "foo"]""", """[5, "splat"]""", """[9, "foo"]""", """[13, "splat"]"""]

        let searchFor = collatable("value")
        var hits: seq[string]
        var curs = snap[searchFor .. searchFor]
        for key, value in curs:
            echo key.asCollatable[0], " : ", value.asCollatable[1], " at ", value.asCollatable[0]
            hits.add value.asCollatable[1].stringValue & " @ " & $(value.asCollatable[0].intValue)
        check hits == @["foo @ 9", "splat @ 13"]


