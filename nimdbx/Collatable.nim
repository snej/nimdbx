# Collatable.nim

{.experimental: "strictFuncs".}

import macros, strutils
import private/[libmdbx, vals]



## Introduction
## ============
##
## The `Collatable` class encodes a series of data values (boolean, integer or string)
## into an opaque binary form that's comparable using regular binary collation (like `strcmp`).
##
## This binary form works as a string key in a `Collection`, allowing you to have Collections
## with primary/secondary/etc. keys. This is very useful for implementing indexes.
##
## The data types supported are `bool`, `int64`, `string` (or `seq[char]`),
## and a "null" type that can be used as a placeholder for missing items.
## (Floating point is not supported yet.)
##
## Ordering
## --------
##
## The sort order of Collatables is pretty intuitive. It's the same as arrays in JavaScript:
## - Corresponding items are compared pairwise, in order.
##   - If one is larger than the other, then its Collatable is greater.
## - If all items match:
##   - If one Collatable is longer, it's greater;
##   - else (i.e. the Collatables are the same length) they're equal.
##
## Items of the same type are compared like this:
## - `false` is less than `true`
## - integers are compared as integers
## - strings are compared as by C `strcmp`, i.e. simple-minded binary comparison.
##
## If items of different types are compared, the one with the higher "rank" wins.
## The ranks from low to high are: Null, Bool, Int, String.
##
## Usage
## -----
##
## To create a Collatable, you normally use the `collatable(...)` macro, which returns an
## instance encoding the parameters you gave it.
## You can also use the `add` function to add individual values.
## When complete, access the `data` property to get the encoded data.
##
## To read the encoded values, call `asCollatable` on a `seq[byte]` containing the encoded data.
## Then you can iterate over it with a `for` loop, or subscript it to get items by position.
## The items are expressed as type `Item`, a variant object with a `type` field that indicates
## the data type, and type-specific fields containing the values.


type
    Collatable* = object
        ## Stores encoded collatable data. See the Introduction above.
        data: seq[byte]    # The encoded binary data

    CollatableRef* = object
        ## A reference to encoded collatable data stored elsewhere. See the Introduction above.
        data: MDBX_val    # The encoded binary data

    CollatableAny* = Collatable | CollatableRef


#%%%%%%%% INTERNALS


# Tag/prefix bytes for different data types
const NullTag =       0.byte
const FalseTag =      1.byte
const TrueTag =       2.byte
const NegIntTags = 0x10.byte  # (8 - the length of the encoded int) is added to the tag
const PosIntTags = 0x20.byte  # the length of the following encoded int is added to the tag
const StringTags = 0x30.byte

# Integers are followed by the encoded int in big-endian alignment with leading 00 or FF suppressed.
# Strings are followed by the UTF-8 data, then a 0 byte.


proc writePositiveInt(buf: var openarray[byte], start: Natural, n: int64): int =
    # Writes a non-negative int `n` to `buf` starting at index `start` and returns the number of
    # bytes written. The data format is big-endian with leading zero bytes suppressed.
    if n == 0:
        buf[0] = 0
        return 1
    var n = n
    var len = 0
    for i in 0..7:
        var b = byte((n shr (56 - 8 * i)) and 0xff)
        if len > 0 or b != 0:
            buf[start + len] = b
            len += 1
    return len

proc writeNegativeInt(buf: var openarray[byte], start: Natural, n: int64): int =
    # Writes a negative int `n` to `buf` starting at index `start` and returns the number of
    # bytes written. The data format is big-endian with leading FF bytes suppressed.
    assert n < 0
    var n = n
    var len = 0
    for i in 0..7:
        var b = byte((n shr (56 - 8 * i)) and 0xff)
        if len > 0 or b != 0xff:
            buf[start + len] = b
            len += 1
    return len

proc readInt(buf: seq[byte] | MDBX_val, tag: byte, pos: var int): int64 =
    # Reads an encoded integer from `buf`.
    # - `tag` is the tag byte preceding the number.
    # - `pos` on entry is the offset of the start of the number (after the tag);
    #   on exit, it's one past the end.
    var len = int(tag and 0x0F)
    if (tag and 0xF0) == NegIntTags:
        len = 8 - len
        result = -1
    for i in 0..<len:
        result = (result shl 8) or int64(buf[pos])
        pos += 1


#%%%%%%%% WRITING


macro collatable*(args: varargs[typed]): Collatable =
    ## Creates a new Collatable and adds the arguments to it (by calling `add`.)
    ## With no arguments, it creates an empty Collatable that you can add items to with `add`.

    # dumpAstGen:
    #     block:
    #         var c: Collatable
    #         c.add(17)
    #         c0
    var body = nnkStmtList.newTree(
        nnkVarSection.newTree(
            nnkIdentDefs.newTree(
                ident"c",
                bindsym"Collatable",
                newEmptyNode() ) ) )
    for arg in args:
        body.add newCall("add", ident"c", arg)
    body.add ident"c"
    result = nnkStmtList.newTree(
        nnkBlockStmt.newTree(
            newEmptyNode(),
            body ) )


proc addNull*(coll: var Collatable) =
    ## Adds a 'null' value to a Collatable. This sorts before / lower than any other value.
    ## Null can be used to express a missing value, or a JSON `null`.
    coll.data.add(NullTag)

type NullPlaceholder = distinct bool
proc add*(coll: var Collatable, n: ptr NullPlaceholder) =
    ## Adds a 'null' value to a Collatable.
    ## (This function is a trick to allow you to write `coll.add(nil)`,
    ## and to allow `nil` to be passed to `addAll`, and `collatable`.)
    coll.addNull()

proc add*(coll: var Collatable, b: bool) =
    ## Adds a boolean value to a Collatable.
    coll.data.add( if b: TrueTag else: FalseTag )


proc add*(coll: var Collatable, n: int64) =
    ## Adds an integer to a Collatable.
    var buf: array[0..9, byte]
    var len: int
    if n == 0:
        # Zero is zero-length, so just the tag:
        coll.data.add(PosIntTags + 0)
    elif n > 0:
        # Fill buf[1..] with significant bytes of n (leading 00 bytes suppressed):
        len = writePositiveInt(buf, 1, Natural(n))
        # Set the first byte to the positive-int tag plus the byte count:
        buf[0] = PosIntTags + byte(len)
        coll.data.add(buf[0..len])
    else:
        # Making negative numbers compare correctly tricky.
        # - suppress leading FF bytes, not 00
        # - tag with 8-length, so that shorter values sort higher (they're closer to -1)
        # Note that NegIntTags < PosIntTags, ensuring negative ints sort before positive ones.
        len = writeNegativeInt(buf, 1, n)
        buf[0] = NegIntTags + byte(8 - len)
        coll.data.add(buf[0..len])


proc add*(coll: var Collatable, str: string) =
    ## Adds a string to a Collatable.
    ## Two strings added this way will be compared as case-sensitive, with lowercase letters
    ## greater than uppercase ones. Other characters are compared by their ASCII ordering.
    ##
    ## **Note:** this uses a simple byte-by-byte binary comparison like C's `strcmp`,
    ## so the sort order is not useful for non-ASCII text.
    ## Unicode characters will basically be compared by
    ## code-point, thanks to the UTF-8 encoding scheme.
    ## Running the string through `unidecode()` first may help.
    coll.data.add(StringTags)
    coll.data.add(cast[seq[byte]](str))
    coll.data.add(0)


proc add*(coll: var Collatable, str: openarray[char]) =
    ## Adds a case-sensitive string to a Collatable.
    ## (For details, see the variant that takes a `string`.)
    coll.data.add(StringTags)
    #FIXME: Crashes with ORC in 1.4! https://github.com/nim-lang/Nim/issues/16218
    # if str.len > 0: coll.data.add(toOpenarrayByte(str, 0, str.len - 1))
    # Workaround:
    for i in 0..<str.len:
        coll.data.add(byte(str[i]))
    coll.data.add(0)


proc addCaseInsensitive*(coll: var Collatable, str: string) =
    ## Adds a string to a Collatable, such that two ASCII strings added this way will be compared
    ## without considering case, i.e. "aBC123" will be equal to "Abc123" and less than "AbA".
    ##
    ## (All this function really does is call `toLowerAscii` on the string before adding it.)
    ##
    ## **Warning:** Only ASCII letters will be case-insensitive, not accented or non-Roman letters.
    ## As a workaround, run the string through `unidecode()` first.
    ##
    ## **Warning:** Reading back a case-insensitive string will return it as lowercase.
    coll.add(str.toLowerAscii)


proc add*(coll: var Collatable, other: Collatable) =
    ## Adds one Collatable's contents to another.
    coll.data.add(other.data)


proc `&`*(a, b: Collatable): Collatable =
    ## Concatenates two Collatables, returning a new one.
    Collatable(data: a.data & b.data)


macro addAll*(coll: var Collatable, args: varargs[typed]) =
    ## A utility macro that adds all its arguments to a Collatable.
    result = nnkStmtList.newTree()
    for arg in args:
        result.add(newCall(bindSym"add", coll, arg))


proc clear*(coll: var Collatable) =
    ## Resets the object back to an empty state.
    coll.data.setLen(0)


#%%%%%%%% ACCESSORS:


func isEmpty*(coll: Collatable): bool = coll.data.len == 0

func data*(coll: Collatable): lent seq[byte] =
    ## The encoded data.
    return coll.data


func data*(coll: CollatableRef): seq[byte] =
    ## The encoded data.
    if coll.data.len == 0:
        return @[]
    return @( toOpenArray(coll.data.unsafeBytes, 0, coll.data.len - 1) )

func val*(coll: Collatable)   : MDBX_val = mkVal(coll.data)
func val*(coll: CollatableRef): MDBX_val = coll.val


func cmp*(a, b: distinct CollatableAny): int =
    ## Compares two Collatables. This enables `\<`, `==`, `\>`, etc.
    if a.data.len == 0 or b.data.len == 0:
        return a.data.len - b.data.len      # (dataCmp barfs on 0 lengths)
    else:
        return dataCmp(unsafeAddr a.data[0], a.data.len, unsafeAddr b.data[0], b.data.len)

func `==`*(a, b: distinct CollatableAny): bool = (cmp(a, b) == 0)


#%%%%%%%% READING / ITERATING:


type CollatableType* = enum
    ## Types of values found in a Collatable
    NullType,
    BoolType,
    IntType,
    StringType,


type Item* = object
    ## A value read from a Collatable.
    case type*: CollatableType
        of NullType:    discard
        of BoolType:    boolValue*: bool
        of IntType:     intValue*: int64
        of StringType:  stringValue*: string


func asCollatable*(data: seq[byte]): Collatable =
    ## Wraps already-encoded data in a Collatable object so it can be added to.
    result.data = data

func asCollatableRef*(val: MDBX_val): CollatableRef =
    ## Wraps already-encoded data in a CollatableRef object, without copying it,
    ## so its items can be accessed.
    result.data = val


iterator items*(coll: CollatableAny): Item {.closure.} =
    ## Iterator over the items in a Collatable. Lets you say `for item in coll: ...`.
    var item: Item
    var pos = 0
    while pos < coll.data.len:
        let tag = coll.data[pos]
        pos += 1
        case tag:
            of NullTag:
                item.type = NullType
            of FalseTag:
                item.type = BoolType
                item.boolValue = false
            of TrueTag:
                item.type = BoolType
                item.boolValue = true
            of NegIntTags..PosIntTags+0x0f:
                item.type = IntType
                item.intValue = readInt(coll.data, tag, pos)
            of StringTags..StringTags+0x0f:
                item.type = StringType
                let start = pos
                while coll.data[pos] != 0:
                    pos += 1
                item.stringValue = cast[string](coll.data[start ..< pos])
                pos += 1
            else:
                raise newException(ValueError, "Corrupted Collatable (unknown tag)")
        yield item


func `[]`*(coll: CollatableAny, index: Natural): Item =
    ## Returns an item from a Collatable.
    ## If the index is out of range, it returns a `null` item.
    ##
    ## Note: This has to iterate through the Collatable using `items`, so performance is O(n).
    var index = index
    for item in coll:
        if index == 0:
            return item
        index -= 1
    return Item(type: NullType)


#%%%%%%%% STRING CONVERSION:


func `$`*(o: Item): string =
    ## Converts an item to a string, in JSON format.
    case o.type:
        of NullType:    return "null"
        of BoolType:    return $o.boolValue
        of IntType:     return $o.intValue
        of StringType:  return o.stringValue.escape


func `$`*(coll: CollatableAny): string =
    ## Converts a Collatable to a string, as a JSON array.
    result = newStringOfCap(coll.data.len)
    result.add("[")
    for item in coll:
        if result.len > 1: result.add(", ")
        result.add($item)
    result.add("]")
