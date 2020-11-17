# utils.nim

import libmdbx


######## ERRORS


type MDBXError* = object of CatchableError
    ## A NimDBX exception.
    code*: MDBXErrorCode


proc throw*(code: MDBXErrorCode) {.noreturn.} =
    ## Raises an MDBXErrorCode as a Nim exception.
    var x = newException(MDBXError, $mdbx_strerror(cint(code)))
    x.code = code
    echo "**** libmdbx error ", int(code), ": ", x.msg # TEMP
    raise x


proc check*(code: MDBXErrorCode) {.inline.} =
    ## Postflights a libmdbx call.
    ## If the code is not ``MDBX_SUCCESS`` or ``MDBX_RESULT_TRUE``, raises an exception.
    if code != MDBX_SUCCESS and code != MDBX_RESULT_TRUE:
        throw(code)


proc checkOptional*(code: MDBXErrorCode): bool {.inline.} =
    ## Postflights a libmdbx call that can validly return ``MDBX_NOTFOUND``.
    ## Returns true on success, false on ``MDBX_NOTFOUND``, else raises an exception.
    if code == MDBX_SUCCESS or code == MDBX_RESULT_TRUE:
        return true
    elif code == MDBX_NOTFOUND:
        return false
    else:
        throw(code)
