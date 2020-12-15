# testDatabase.nim

{.experimental: "notnil".}

import strformat, unittest
import nimdbx


let DBPath = "test_db"
let CollectionName = "stuff"


suite "Database":
    var db: Database
    var coll: Collection not nil

    setup:
        eraseDatabase(DBPath)
        db = openDatabase(DBPath)
        coll = db.createCollection(CollectionName)

        # coll.addChangeHook proc(key, oldval, newval: DataOut) =
        #     echo "\t\tChangeHook! key = ", $key.escape, "  oldVal = ", $oldVal.escape, "  newVal = ", $newVal.escape

    teardown:
        if db != nil:
            db.closeAndDelete()


    test "create DB":
        check db.path == DBPath
        echo db.stats
        echo "DBI = ", ord(coll.i_dbi)
        echo "Stats = ", coll.stats

        check db.getOpenCollection(CollectionName) == coll
        check db.openCollectionOrNil("missing", {}) == nil

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


    test "Put and get entries":
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

        var (nearestkey, nearestval) = cs.getGreaterOrEqual("moo")
        check $nearestkey == "splat"
        check $nearestval == "I am splat's value"

        var (nearestkey2, nearestval2) = cs.getGreaterOrEqual("zz top")
        check not nearestkey2
        check not nearestval2

        cs.finish()

        coll.inTransaction do (ct: CollectionTransaction):
            ct.put("foo", "XXX")
            ct.put("bogus", "equally bogus")
            check ct.del("splat")
            check not ct.del("missing")
            check ct.updateAndGet("missing", "new") == ""
            check ct.updateAndGet("bogus", "bogus-er") == "equally bogus"
            check ct.delAndGet("missing") == ""
            check ct.delAndGet("bogus") == "bogus-er"
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


    proc expectedKey(i: int): string = &"key-{i:02}"
    proc expectedValue(i: int): string = &"the value is {i}."

    proc createEntries() =
        echo "-- Create 100 entries --"
        coll.inTransaction do (ct: CollectionTransaction):
            for i in 0..99:
                ct.put(expectedKey(i), expectedValue(i))
            ct.commit()


    test "Cursors":
        createEntries()

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
            check $curs.key == expectedKey(i)
            if $curs.key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
            check $curs.value == expectedValue(i)
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

        echo "-- Reverse iteration --"
        curs = makeCursor(cs)
        i = 99
        while curs.prev():
            check i >= 0
            #echo curs.key, " = ", curs.value
            check $curs.key == expectedKey(i)
            if $curs.key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
            check $curs.value == expectedValue(i)
            i -= 1
        check i == -1
        curs.close()

        echo "-- 'for' loop with cursor"
        i = 0
        for key, value in cs:
            check i < 100
            check $key == expectedKey(i)
            if $key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
            check $value == expectedValue(i)
            i += 1
        check i == 100

        echo "-- reverse 'for' loop with cursor"
        i = 99
        for key, value in cs.reversed:
            check i < 100
            check $key == expectedKey(i)
            if $key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
            check $value == expectedValue(i)
            i -= 1
        check i == -1



    test "Int Keys":
        let coll = db.createCollection("ints", IntegerKeys)

        echo "-- Add keys --"
        coll.inTransaction do (ct: CollectionTransaction):
            for i in 0..99:
                ct.put(int32(i), expectedValue(i))
            check ct.entryCount() == 100
            ct.commit()

        echo "-- Get by key --"
        var cs = coll.beginSnapshot()
        check cs.entryCount() == 100
        for i in 0..99:
            let val = cs.get(int32(i))
            #echo i, " = ", val
            check val
            check $val == expectedValue(i)
            if $val != expectedValue(i): echo "oops, stopping"; break

        echo "-- Forwards iteration --"
        var curs = makeCursor(cs)
        var i = 0
        while curs.next():
            check i < 100
            #echo curs.intKey, " = ", curs.value
            check curs.key.asInt32 == i
            check curs.key.asInt64 == i
            check $curs.value == expectedValue(i)
            if curs.key.asInt32 != i: echo "oops, stopping"; break
            i += 1
        check i == 100


    test "Cursor subranges":
        createEntries()
        var cs = coll.beginSnapshot()

        proc checkCursor(minKey, maxKey: string;
                         first, last: int;
                         skipMin = false; skipMax = false) =
            # Forwards:
            echo "   -- forward"
            var curs = makeCursor(cs)
            if minKey != "": curs.minKey = minKey
            if maxKey != "": curs.maxKey = maxKey
            curs.skipMinKey = skipMin
            curs.skipMaxKey = skipMax
            var i = first
            while curs.next():
                #echo curs.key, " = ", curs.value
                check $curs.key == expectedKey(i)
                check $curs.value == expectedValue(i)
                if $curs.key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
                check i <= last
                i += 1
            check i == last + 1

            # Backwards:
            echo "   -- reverse"
            curs = makeCursor(cs)
            if minKey != "": curs.minKey = minKey
            if maxKey != "": curs.maxKey = maxKey
            curs.skipMinKey = skipMin
            curs.skipMaxKey = skipMax
            i = last
            while curs.prev():
                #echo curs.key, " = ", curs.value
                check $curs.key == expectedKey(i)
                check $curs.value == expectedValue(i)
                if $curs.key != expectedKey(i): echo "oops, stopping"; break   # Likely to produce 99 more failures...
                check i >= first
                i -= 1
            check i == first - 1

        echo "-- first"
        checkCursor("key-10", "", 10, 99)
        echo "-- first, out of range"
        checkCursor("a", "", 0, 99)
        echo "-- last"
        checkCursor("", "key-20", 0, 20)
        echo "-- last, out of range"
        checkCursor("", "z", 0, 99)
        echo "-- first and last"
        checkCursor("key-10", "key-20", 10, 20)
        echo "-- first and last, same key"
        checkCursor("key-10", "key-10", 10, 10)
        echo "-- first and last, empty range"
        checkCursor("key-20", "key-10", 20, 19)
        echo "-- first and last, too low"
        checkCursor("a", "b", 0, -1)
        echo "-- first and last, too high"
        checkCursor("y", "z", 0, -1)

        echo "-- skip first"
        checkCursor("key-10", "", 11, 99, skipMin = true)
        echo "-- skip last"
        checkCursor("", "key-20", 0, 19, skipMax = true)
        echo "-- skip first & last"
        checkCursor("key-10", "key-20", 11, 19, skipMin = true, skipMax = true)
        echo "-- skip first & last out of range"
        checkCursor("a", "z", 0, 99, skipMin = true, skipMax = true)


    test "Cursor subranges via subscripts":
        createEntries()
        var cs = coll.beginSnapshot()
        var curs = cs["a".."z"]
        check curs.minKey == "a"
        check curs.maxKey == "z"

        curs = cs[NoKey.."z"]
        check not curs.minKey.exists
        check curs.maxKey == "z"

        curs = cs["a"..NoKey]
        check curs.minKey == "a"
        check not curs.maxKey.exists


    test "Duplicate keys":
        let coll = db.openCollection("dups", {CreateCollection, DuplicateKeys},
                                     StringKeys, IntegerValues)
        echo "-- Create entries with dup keys --"
        coll.inTransaction do (ct: CollectionTransaction):
            for i in 0..99:
                let key = expectedKey(i)
                for val in 1'i32..10'i32:
                    assert ct.put(key, val, NoDupData)
            ct.commit()

        echo "-- Read entries back (no dups) --"
        var cs = coll.beginSnapshot()
        for i in 0..99:
            check cs.get(expectedKey(i)) == 1'i32

        echo "-- Iterate all entries --"
        block:
            var curs = makeCursor(cs)
            for i in 0..99:
                let key = expectedKey(i)
                for expectedVal in 1..10:
                    check curs.next()
                    check $curs.key == key
                    check curs.value.asInt == expectedVal
                    check curs.valueCount == 10
            check not curs.next()

        echo "-- Iterate backwards --"
        block:
            var curs = makeCursor(cs)
            for i in 0..99:
                let key = expectedKey(99 - i)
                for val in 1..10:
                    check curs.prev()
                    check $curs.key == key
                    check curs.value.asInt == 11 - val
                    check curs.valueCount == 10
            check not curs.prev()

        echo "-- nextDup --"
        block:
            var curs = makeCursor(cs)
            let key = expectedKey(23)
            curs.seek(key)
            for val in 1..10:
                check $curs.key == key
                check curs.value.asInt == val
                check curs.nextDup() == (val < 10)

        echo "-- nextKey --"
        block:
            var curs = makeCursor(cs)
            for i in 0..99:
                check curs.nextKey()
                check $curs.key == expectedKey(i)
                check curs.value.asInt == 1
            check not curs.nextKey()

    # TODO: Test duplicate keys + key ranges

    # TODO: Test read-only Database


    test "Collatable Keys":
        coll.inTransaction do (ct: CollectionTransaction):
            ct[collatable("hi", 12)] = "hi12"
            ct[collatable("hi", -12)] = "hi-12"
            ct[collatable("hi")] = "hi"
            ct[collatable("bye", 17)] = "bye17"
            ct[collatable("bye", "-ya")] = "bye-ya"
            ct[collatable(12345)] = "12345"
            ct[collatable(false)] = "false"
            ct.commit

        coll.inSnapshot do (ct: CollectionSnapshot):
            check ct[collatable("hi", 12)] == "hi12"
            check ct[collatable("hi", -12)] == "hi-12"
            check ct[collatable("hi")] == "hi"
            check ct[collatable("bye", 17)] == "bye17"
            check ct[collatable("bye", "-ya")] == "bye-ya"
            check ct[collatable("bye", "-ya")] == "bye-ya"
            check ct[collatable(12345)] == "12345"
            check ct[collatable(false)] == "false"

            var curs = makeCursor(ct)
            check curs.next
            check curs.key.asCollatable == collatable(false)
            check curs.next
            check curs.key.asCollatable == collatable(12345)
            check curs.next
            check curs.key.asCollatable == collatable("bye", 17)
            check curs.next
            check curs.key.asCollatable == collatable("bye", "-ya")
            check curs.next
            check curs.key.asCollatable == collatable("hi")
            check curs.next
            check curs.key.asCollatable == collatable("hi", -12)
            check curs.next
            check curs.key.asCollatable == collatable("hi", 12)
            check not curs.next
