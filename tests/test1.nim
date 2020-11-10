# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest
import mdbx

let DBPath = "test_db"

proc openTestDB(): FlatDB =
    eraseDB(DBPath)
    return openDB(DBPath)


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
    let db = openTestDB()
    check db.path == DBPath
    echo db.stats


test "create Collection":
    let db = openTestDB()
    let coll1 = db.createCollection("toys")
    echo "DBI = ", ord(coll1.dbi)
    echo "Stats = ", coll1.stats


test "Create record":
    let db = openTestDB()
    let coll = db.createCollection("toys")

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
    cs.finish()

    coll.inTransaction do (ct: CollectionTransaction):
        ct.put("foo", "XXX")
        ct.del("splat")
        ct.put("bogus", "equally bogus")
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
