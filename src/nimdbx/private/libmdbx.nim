# libmdbx.nim
#
# Builds libmdbx C library and generates a Nim API wrapper, courtesy of Nimterop.
# The generated wrapper is implicitly imported by importing this file.
#

import nimterop/[build, cimport], os, strutils

#static: cDebug()  # Prints wrapper to stdout

#### Configuration for building the libmdbx C library:

const baseDir = currentSourcePath.parentDir() / "../../../../nimdbx/vendor/libmdbx"

setDefines(["mdbxStatic"])

getHeader(
    "mdbx.h",                      # The header file to wrap, full path is returned in `headerPath`
    outdir = baseDir,              # Where to find the header
    altNames = "libmdbx"           # Alterate names of the library binary, full path returned in `headerLPath`
)

# Linker flags for the Nim binary being built (libmxbx is statically linked but it uses pthreads)
when defined(linux):
    {.passL: "-lpthread".}

#### Configuration for generating the Nim header wrapper:

# Skip some symbols:
static:
    cSkipSymbol @["MDBX_PURE_FUNCTION", "MDBX_NOTHROW_PURE_FUNCTION", "MDBX_CONST_FUNCTION",
                  "MDBX_NOTHROW_CONST_FUNCTION", "MDBX_DEPRECATED",
                  "MDBX_LOGGER_DONTCHANGE",
                  "mdbx_version", "mdbx_build",   # FIX: nimterop can't parse their declarations...
                  "bool", "true", "false"]

# Rename some symbols:
cPlugin:
    import strutils
    proc onSymbol*(sym: var Symbol) {.exportc, dynlib.} =
        sym.name = sym.name.strip(chars = {'_'}).replace("__", "_")
        if sym.name == "intptr_t":
            sym.name = "ssize_t"      # Work around Nimterop bug handling intptr_t

# Add some missing declarations:
cOverride:
    type
        mdbx_pid_t* = cint
        mdbx_tid_t* = pointer
        mdbx_mode_t* = cint
        MDBX_val*  {.bycopy.} = object
            iov_base*: pointer
            iov_len*: csize_t

# Now generate the Nim interface:
cImport(mdbxPath, recurse = true)
