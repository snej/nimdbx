# utils.nim

import libmdbx


######## VALUES


converter toVal*(arr: openarray[char]): MDBX_val =
    return MDBX_val(base: unsafeAddr arr[0], len: csize_t(arr.len))


converter toString*(val: MDBX_val): string =
    result = newString(val.len)
    copyMem(addr result[0], val.base, val.len)


converter toChars*(val: MDBX_val): seq[char] =
    result = newSeq[char](val.len)
    copyMem(addr result[0], val.base, val.len)

converter toBytes*(val: MDBX_val): seq[uint8] =
    result = newSeq[uint8](val.len)
    copyMem(addr result[0], val.base, val.len)


######## ERRORS


type MDBXError* = object of CatchableError
    code*: MDBXErrorCode


proc check*(code: MDBXErrorCode) =
    if code != MDBX_SUCCESS:
        var x = newException(MDBXError, $mdbx_strerror(cint(code)))
        x.code = code
        echo "**** Raising ", x.msg
        raise x


proc checkOptional*(code: MDBXErrorCode): bool =
    if code == MDBX_SUCCESS:
        return true
    elif code == MDBX_NOTFOUND:
        return false
    else:
        check(code)
