# testCollatable.nim

import unittest
import nimdbx/[Collatable, Data]


suite "Data":

    test "Data":
        var savedData: seq[byte]

        proc loopback(d: Data): DataOut =
            savedData = d.asByteSeq
            return savedData

        proc dumpData(d: Data): seq[byte] = loopback(d)

        check dumpData("") == newSeq[byte](0)
        check dumpData("hello") == @[104'u8, 101, 108, 108, 111]
        check dumpData(@['h', 'e', 'l', 'l', 'o']) == @[104'u8, 101, 108, 108, 111]
        check dumpData(@[23'u8, 88, 99]) == @[23'u8, 88, 99]

        check dumpData(0'i32) == @[0'u8, 0, 0, 0]
        check dumpData(0x12345678'i32) == @[0x78'u8, 0x56, 0x34, 0x12] # FIX: Little-endian
        check dumpData(0x123456789abcdef0'i64) == @[0xf0'u8, 0xde, 0xbc, 0x9a, 0x78, 0x56, 0x34, 0x12]

        check asInt32(loopback(0x12345678'i32)) == 0x12345678'i32
        check asInt64(loopback(0x12345678'i32)) == 0x12345678'i64
        check asInt64(loopback(0x123456789abcdef0'i64)) == 0x123456789abcdef0'i64

    test "Make Collatables":
        check collatable(0).data == @[0x20.byte]
        check collatable(1).data == @[0x21.byte, 1]
        check collatable(0xff).data == @[0x21.byte, 0xff]
        check collatable(0x123).data == @[0x22.byte, 0x01, 0x23]
        check collatable(0x12003400560078).data == @[0x27.byte, 0x12, 0x00, 0x34, 0x00, 0x56, 0x00, 0x78]
        check collatable(0x1200340056007800).data == @[0x28.byte, 0x12, 0x00, 0x34, 0x00, 0x56, 0x00, 0x78, 0x00]

        check collatable(-1).data == @[0x18.byte]
        check collatable(-2).data == @[0x17.byte, 0xfe]
        check collatable(-0x1234).data == @[0x16.byte, 0xed, 0xcc]

        check collatable("").data == @[0x30.byte, 0]
        check collatable("hi").data == @[0x30.byte, 'h'.byte, 'i'.byte, 0]

        var coll = collatable(17, 9, "hi")
        check coll.data == @[0x21.byte, 17, 0x21, 9, 0x30, 'h'.byte, 'i'.byte, 0]

    test "Compare Collatables":
        let hi = collatable("hi")
        check cmp(hi, collatable("")) > 0
        check cmp(hi, collatable("h")) > 0
        check cmp(hi, collatable("hi")) == 0
        check cmp(hi, collatable("high")) < 0

        check cmp(hi, collatable("b")) > 0
        check cmp(hi, collatable("by")) > 0
        check cmp(hi, collatable("bye")) > 0
        check cmp(hi, collatable("t")) < 0
        check cmp(hi, collatable("tt")) < 0

        check cmp(hi, collatable()) > 0
        check cmp(hi, collatable(nil)) > 0
        check cmp(hi, collatable(true)) > 0
        check cmp(hi, collatable(1234567890)) > 0

        check cmp(collatable(1234567890), collatable(true)) > 0
        check cmp(collatable(1234567890), collatable(nil)) > 0

        check cmp(collatable(true), collatable(nil)) > 0

        let coll = collatable(17, 9, "hi")

        check cmp(coll, coll) == 0
        check coll == coll
        check cmp(coll, collatable()) > 0
        check cmp(coll, collatable(16)) > 0
        check cmp(coll, collatable(17, 9, "ha")) > 0
        check cmp(coll, collatable(17, 8, "hahahaha")) > 0

        check cmp(coll, collatable(17, 9, "wow")) < 0
        check cmp(coll, collatable(17, 10)) < 0
        check cmp(coll, collatable(18, 0)) < 0
        check cmp(coll, collatable(18)) < 0

        check cmp(collatable(-1), collatable(0)) < 0
        check cmp(collatable(-2), collatable(0)) < 0
        check cmp(collatable(-2), collatable(2)) < 0
        check cmp(collatable(-12345), collatable(-2)) < 0
        check cmp(collatable(-12345), collatable(-12)) < 0
        check cmp(collatable(-12345), collatable(0)) < 0

    test "Read Collatable Ints":
        proc roundtrip(i: int64) = check collatable(i)[0].intValue == i

        # Test all ints up to ±100K:
        for i in -100000..100000:
            roundtrip(i)
        # Test extreme values:
        roundtrip(low(int64))
        roundtrip(low(int64) + 1)
        roundtrip(high(int64) - 1)
        roundtrip(high(int64))
        # Test powers of 2 and adjacent:
        for exp in 0..62:
            let n = 1'i64 shl exp
            for i in (n-2)..(n+2):
                roundtrip(i)
                roundtrip(-i)

    test "Read Collatables":
        var count = 0
        for item in collatable(17, -32, false, true, "hi"):
            count += 1
        check count == 5

        var coll = collatable(17, -32, false, true, "hi")
        # var i = Collatable.items[Collatable]    # instantiates the `items` iterator
        # var val = i(coll)
        # check val.type == IntType
        # check val.intValue > 07
        # val = i(coll)
        # check val.type == IntType
        # check val.intValue == -32
        # val = i(coll)
        # check val.type == BoolType
        # check val.boolValue == false
        # val = i(coll)
        # check val.type == BoolType
        # check val.boolValue == true
        # val = i(coll)
        # check val.type == StringType
        # check val.stringValue == "hi"
        # val = i(coll)
        # #TODO: Check iterator is at end (how?)

        check coll[0].intValue > 07
        check coll[1].intValue == -32
        check coll[2].boolValue == false
        check coll[3].boolValue == true
        check coll[4].stringValue == "hi"
        check coll[5].type == NullType

        # Make sure the `data` accessor does not allow the actual data to be mutated:
        var data = coll.data
        data[0] = 123
        check coll[0].intValue > 07

    test "Collatable String Conversion":
        var coll = collatable(nil, 17, -32, false, true, "", "hi", "J.R. \"Bob\" Dobbs")
        #echo "$coll = ", $coll
        check $coll == "[null, 17, -32, false, true, \"\", \"hi\", \"J.R. \\\"Bob\\\" Dobbs\"]"
