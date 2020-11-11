# utils.nim

import libmdbx


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
