import strformat, unittest
import nimdbx


let DBPath = "test_db"
let CollectionName = "stuff"


suite "Basic":
    test "CollectionFlags":
        var flags: CollectionFlags
        check cast[uint](flags) == 0
        flags = {ReverseKeys}
        check cast[uint](flags) == 2
        flags = {IntegerDup, ReverseDup}
        check cast[uint](flags) == 0x60
        flags = {DuplicateKeys, IntegerDup, ReverseDup}
        check cast[uint](flags) == 0x64


suite "Database":
    var db: Database
    var coll: Collection

    setup:
        eraseDatabase(DBPath)
        db = openDatabase(DBPath)
        coll = db.createCollection(CollectionName)

    teardown:
        if db != nil:
            db.closeAndDelete()

    test "create DB":
        check db.path == DBPath
        echo db.stats
        echo "DBI = ", ord(coll.dbi)
        echo "Stats = ", coll.stats

    test "Sequences":
        var cs = coll.beginSnapshot()
        check cs.lastSequence == 0

        coll.inTransaction do (ct: CollectionTransaction):
            check ct.lastSequence == 0
            check ct.nextSequence() == 1
            check ct.nextSequence(count=3) == 2   ## i.e. we get 2, 3, 4
            check ct.lastSequence == 4
            ct.commit()

        cs = coll.beginSnapshot()
        check cs.lastSequence == 4


    test "Put and get record":
        var ct = coll.beginTransaction()
        ct.put("foo", "I am the value of foo")
        ct.put("splat", "I am splat's value")
        check ct.entryCount == 2
        ct.commit()
        check coll.entryCount == 2

        var cs = coll.beginSnapshot()
        echo "foo = ", cs.get("foo")
        echo "splat = ", cs.get("splat")
        check cs.get("foo") == "I am the value of foo"
        check cs.get("splat") == "I am splat's value"
        check cs.get("bogus") == ""

        var key: Data = "moo"
        var nearest = cs.getGreaterOrEqual(key)
        check key == "splat"
        check nearest == "I am splat's value"

        key = "zz top"
        nearest = cs.getGreaterOrEqual(key)
        check not key
        check not nearest

        cs.finish()

        coll.inTransaction do (ct: CollectionTransaction):
            ct.put("foo", "XXX")
            ct.put("bogus", "equally bogus")
            check ct.del("splat")
            check not ct.del("missing")
            ct.abort()

        cs = coll.beginSnapshot()
        check cs.get("foo") == "I am the value of foo"
        check cs.get("splat") == "I am splat's value"
        check cs.get("bogus") == ""

        var gotVal = false
        cs.get("foo") do (val: openarray[char]):
            check val == "I am the value of foo"
            gotVal = true
        check gotVal == true


    test "Cursors":
        echo "-- Add keys --"
        coll.inTransaction do (ct: CollectionTransaction):
            for i in 0..99:
                ct.put(&"key-{i:02}", &"the value is {i}.")
            ct.commit()

        # NOTE: When using Cursor.key and Cursor.value with the `check` macro, you have to convert
        # the `Data` result to a specific type, otherwise the implementation of `check` will try to
        # copy it, which is disallowed. Thus we use `check $curs.key == ...`, not `check curs.key ==`.

        echo "-- Forwards iteration --"
        var cs = coll.beginSnapshot()
        var curs = makeCursor(cs)
        var i = 0
        while curs.next():
            check i < 100
            #echo curs.key, " = ", curs.value
            check $curs.key == &"key-{i:02}"
            check $curs.value == &"the value is {i}."
            i += 1
        check i == 100
        check cs.entryCount == 100

        echo "-- seek --"
        check curs.seek("key")
        check curs.hasValue
        check $curs.key == "key-00"
        check $curs.value == "the value is 0."

        check not curs.seek("key-999")
        check not curs.hasValue

        echo "-- seekExact --"
        check not curs.seekExact("key")
        check not curs.hasValue

        check curs.seekExact("key-23")
        check curs.hasValue
        check $curs.key == "key-23"
        check $curs.value == "the value is 23."

        echo "-- prev --"
        check curs.prev()
        check curs
        check $curs.key == "key-22"
        check $curs.value == "the value is 22."

        echo "-- first --"
        check curs.first()
        check curs
        check $curs.key == "key-00"
        check $curs.value == "the value is 0."
        check not curs.prev()

        echo "-- last --"
        check curs.last()
        check curs
        check $curs.key == "key-99"
        check $curs.value == "the value is 99."
        check not curs.next()

        echo "-- Create new cursor --"
        curs = makeCursor(cs)

        echo "-- Reverse iteration --"
        i = 99
        while curs.prev():
            check i >= 0
            #echo curs.key, " = ", curs.value
            check $curs.key == &"key-{i:02}"
            check $curs.value == &"the value is {i}."
            i -= 1
        check i == -1

        curs.close()


    test "Int Keys":
        let coll = db.createCollection("ints", {IntegerKeys})

        echo "-- Add keys --"
        coll.inTransaction do (ct: CollectionTransaction):
            for i in 0..99:
                ct.put(int32(i), &"the value is {i}.")
            ct.commit()

        echo "-- Get by key --"
        var cs = coll.beginSnapshot()
        for i in 0..99:
            let val = cs.get(int32(i)).asString
            #echo i, " = ", val
            check val == &"the value is {i}."

        echo "-- Forwards iteration --"
        var curs = makeCursor(cs)
        var i = 0
        while curs.next():
            check i < 100
            #echo curs.intKey, " = ", curs.value
            check curs.key.asInt64 == i
            check $curs.value == &"the value is {i}."
            i += 1
        check i == 100

