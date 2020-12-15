# test3_Index.nim

{.experimental: "notnil".}

import unittest
import nimdbx


let DBPath = "test_db"
let CollectionName = "stuff"


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
        index = coll.openIndex("lengths") do (value: DataOut, outColumns: var Collatable):
            #debugEcho "INDEXING ", ($value).escape
            outColumns.add(($value).len)

    proc dumpIndex(snap: CollectionSnapshot): (seq[string], seq[string]) =
        var keys, vals: seq[string]
        for key, value in snap:
            echo "  ", key.asCollatable, " -> ", $value
            keys.add($(key.asCollatable))
            vals.add($value)
        return (keys, vals)


    test "Index populated DB":
        addSomething()
        createLengthIndex()
        check index.updateCount == 0

        let snap = index.beginSnapshot()
        check snap.entryCount == 2

        var (keys, vals) = dumpIndex(snap)
        check keys == @["[18]",  "[21]"]
        check vals == @["splat", "foo"]


    test "Populate DB then index":
        createLengthIndex()
        addSomething()
        check index.updateCount == 2

        let snap = index.beginSnapshot()
        check snap.entryCount == 2

        var (keys, vals) = dumpIndex(snap)
        check keys == @["[18]",  "[21]"]
        check vals == @["splat", "foo"]


    test "Update DB":
        addSomething()
        createLengthIndex()

        coll.inTransaction do (ct: CollectionTransaction):
            # Update, add, delete keys:
            check ct.update("foo", "bar")
            ct.put("longer", "I am the very model of a modern Major General.")
            check ct.del("splat")
            check index.updateCount == 3

            # Changes that do not affect the index:
            check ct.update("foo", "bar")
            check index.updateCount == 3
            check ct.update("foo", "rab")
            check index.updateCount == 3
            discard ct.del("missing")
            check index.updateCount == 3

            ct.commit

        let snap = index.beginSnapshot()
        var (keys, vals) = dumpIndex(snap)
        check keys == @["[3]",  "[46]"]
        check vals == @["foo",  "longer"]
