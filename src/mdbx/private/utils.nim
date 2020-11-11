# utils.nim

import libmdbx


######## VALUES


converter toVal*(arr: openarray[char]): MDBX_val =
    return MDBX_val(base: unsafeAddr arr[0], len: csize_t(arr.len))


converter toString*(val: MDBX_val): string =
    result = newString(val.len)
    if val.len > 0:
        copyMem(addr result[0], val.base, val.len)


converter toChars*(val: MDBX_val): seq[char] =
    result = newSeq[char](val.len)
    if val.len > 0:
        copyMem(addr result[0], val.base, val.len)

converter toBytes*(val: MDBX_val): seq[uint8] =
    result = newSeq[uint8](val.len)
    if val.len > 0:
        copyMem(addr result[0], val.base, val.len)


######## ERRORS


type MDBXError* = object of CatchableError
    code*: MDBXErrorCode


proc throw*(code: MDBXErrorCode) {.noreturn.} =
    var x = newException(MDBXError, $mdbx_strerror(cint(code)))
    x.code = code
    echo "**** Raising ", x.msg
    raise x


proc check*(code: MDBXErrorCode) {.inline.} =
    if code != MDBX_SUCCESS and code != MDBX_RESULT_TRUE:
        throw(code)


proc checkOptional*(code: MDBXErrorCode): bool {.inline.} =
    if code == MDBX_SUCCESS or code == MDBX_RESULT_TRUE:
        return true
    elif code == MDBX_NOTFOUND:
        return false
    else:
        check(code)
