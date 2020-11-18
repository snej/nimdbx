# utils.nim

import libmdbx


######## ERRORS


type MDBXError* = object of CatchableError
    ## A NimDBX exception.
    code*: MDBX_error_t


proc throw*(code: MDBX_error_t) {.noreturn.} =
    ## Raises a libmdbx error code as a Nim exception.
    var x = newException(MDBXError, $mdbx_strerror(cint(code)))
    x.code = code
    echo "**** libmdbx error ", int(code), ": ", x.msg # TEMP
    raise x


proc check*(code: MDBX_error_t) {.inline.} =
    ## Postflights a libmdbx call.
    ## If the code is not ``MDBX_SUCCESS`` or ``MDBX_RESULT_TRUE``, raises an exception.
    if code != MDBX_SUCCESS and code != MDBX_RESULT_TRUE:
        throw(code)


proc checkOptional*(code: MDBX_error_t): bool {.inline.} =
    ## Postflights a libmdbx call that can validly return ``MDBX_NOTFOUND``.
    ## Returns true on success, false on ``MDBX_NOTFOUND``, else raises an exception.
    if code == MDBX_SUCCESS or code == MDBX_RESULT_TRUE:
        return true
    elif code == MDBX_NOTFOUND:
        return false
    else:
        throw(code)


proc check*(code: int) {.inline.} = check MDBX_error_t(code)
proc checkOptional*(code: int): bool {.inline.} = checkOptional MDBX_error_t(code)
