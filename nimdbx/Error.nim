# Error.nim

import private/libmdbx


type MDBXError* = object of CatchableError
    ## A NimDBX exception.
    code*: MDBX_error_t     ## The libmdbx error code


proc throw*(code: MDBX_error_t, message: string) {.noreturn.} =
    ## Raises a libmdbx error code as a Nim exception of type MDBXError, or as an OSError.
    echo "**** libmdbx error ", int(code), ": ", message # TEMP
    let icode = cint(code)
    if icode < 0 or icode == MDBX_EINVAL:
        var x = newException(MDBXError, message)
        x.code = code
        raise x
    else:
        # Positive error codes are OS errors, i.e. `errno` values
        var x = newException(OSError, message)
        x.errorCode = icode
        raise x


proc throw*(code: MDBX_error_t) {.noreturn.} =
    ## Raises a libmdbx error code as a Nim exception of type MDBXError, or as an OSError.
    throw(code, $mdbx_strerror(cint(code)))


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

# The libmdbx C functions return `int` not `MDBX_error_t` (for annoying reasons related to C)
# so here are some overloads that take ints:

proc check*(code: int) {.inline.}               = check MDBX_error_t(code)
proc checkOptional*(code: int): bool {.inline.} = checkOptional MDBX_error_t(code)
