# test3_Index.nim

import unittest
import nimdbx


let DBPath = "test_db"
let CollectionName = "stuff"


suite "Database":
    var db: Database
    var coll: Collection
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

        let snap = index.beginSnapshot()
        check snap.entryCount == 2

        var (keys, vals) = dumpIndex(snap)
        check keys == @["[18]",  "[21]"]
        check vals == @["splat", "foo"]


    test "Populate DB then index":
        createLengthIndex()
        addSomething()

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
            ct.commit

        let snap = index.beginSnapshot()
        var (keys, vals) = dumpIndex(snap)
        check keys == @["[3]",  "[46]"]
        check vals == @["foo",  "longer"]
