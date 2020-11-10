# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import strformat, unittest
import mdbx


# UTILITIES

let DBPath = "tests/test_db"
let CollectionName = "stuff"

proc openNewTestDB(): FlatDB =
    eraseDB(DBPath)
    return openDB(DBPath)


#### TESTS

test "CollectionFlags":
    var flags: CollectionFlags
    check cast[uint](flags) == 0
    flags = {ReverseKeys}
    check cast[uint](flags) == 2
    flags = {IntegerDup, ReverseDup}
    check cast[uint](flags) == 0x60
    flags = {DuplicateKeys, IntegerDup, ReverseDup}
    check cast[uint](flags) == 0x64


test "create DB":
    let db = openNewTestDB()
    check db.path == DBPath
    echo db.stats


test "create Collection":
    let db = openNewTestDB()
    let coll1 = db.createCollection("stuff")
    echo "DBI = ", ord(coll1.dbi)
    echo "Stats = ", coll1.stats

test "Sequences":
    let db = openNewTestDB()
    let coll = db.createCollection("stuff")

    var cs = coll.beginSnapshot()
    check cs.sequence == 0

    coll.inTransaction do (ct: CollectionTransaction):
        check ct.sequence == 0
        check ct.sequence(thenAdd = 1'u64) == 0
        check ct.sequence(thenAdd = 3'u64) == 1
        check ct.sequence == 4
        ct.commit()

    cs = coll.beginSnapshot()
    check cs.sequence == 4

test "Create record":
    let db = openNewTestDB()
    let coll = db.createCollection("stuff")

    var ct = coll.beginTransaction()
    ct.put("foo", "I am the value of foo")
    ct.put("splat", "I am splat's value")
    ct.commit()

    var cs = coll.beginSnapshot()
    echo "foo = ", cs.get("foo")
    echo "splat = ", cs.get("splat")
    check cs.get("foo") == "I am the value of foo"
    check cs.get("splat") == "I am splat's value"
    check cs.get("bogus") == ""
    #check cs.getNearest("moo") == ("splat", "I am splat's value")  #FIX: Why doesn't this work?
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
    let db = openNewTestDB()
    let coll = db.createCollection("stuff")
    coll.inTransaction do (ct: CollectionTransaction):
        for i in 0..99:
            ct.put(&"key-{i:02}", &"the value is {i}.")
        ct.commit()

    echo "-- Forwards iteration --"
    var cs = coll.beginSnapshot()
    var curs = makeCursor(cs)
    var i = 0
    while curs.next():
        check i < 100
        #echo curs.key, " = ", curs.value
        check curs.key == &"key-{i:02}"
        check curs.value == &"the value is {i}."
        i += 1
    check i == 100

    echo "-- seek --"
    check curs.seek("key")
    check curs.hasValue
    check curs.key == "key-00"
    check curs.value == "the value is 0."

    check not curs.seek("key-999")
    check not curs.hasValue

    echo "-- seekExact --"
    check not curs.seekExact("key")
    check not curs.hasValue

    check curs.seekExact("key-23")
    check curs.hasValue
    check curs.key == "key-23"
    check curs.value == "the value is 23."

    echo "-- prev --"
    check curs.prev()
    check curs
    check curs.key == "key-22"
    check curs.value == "the value is 22."

    echo "-- first --"
    check curs.first()
    check curs
    check curs.key == "key-00"
    check curs.value == "the value is 0."
    check not curs.prev()

    echo "-- last --"
    check curs.last()
    check curs
    check curs.key == "key-99"
    check curs.value == "the value is 99."
    check not curs.next()

    echo "-- Create new cursor --"
    curs = makeCursor(cs)

    echo "-- Reverse iteration --"
    i = 99
    while curs.prev():
        check i >= 0
        #echo curs.key, " = ", curs.value
        check curs.key == &"key-{i:02}"
        check curs.value == &"the value is {i}."
        i -= 1
    check i == -1

    curs.close()
