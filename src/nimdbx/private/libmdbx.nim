## Raw bindings to C API of libmdbx. Do not use directly.
## Hand-translated from mdbx.h

when defined(windows):
  const libmdbx = "libmdbx.dll"
elif defined(macosx):
  const libmdbx = "libmdbx.dylib"
else:
  const libmdbx = "libmdbx.so"

{.push dynlib: libmdbx.}

type EnvFlags* = distinct cuint                # mdbx.h:871
func `or`*(a, b: EnvFlags): EnvFlags {.inline.} = EnvFlags(cuint(a) or cuint(b))
func `and`*(a, b: EnvFlags): EnvFlags {.inline.} = EnvFlags(cuint(a) and cuint(b))
const
    MDBX_ENV_DEFAULTS    = EnvFlags(0x0000000)
    MDBX_NOSUBDIR*       = EnvFlags(0x0004000)
    MDBX_RDONLY*         = EnvFlags(0x0020000)
    MDBX_EXCLUSIVE*      = EnvFlags(0x0400000)
    MDBX_ACCEDE*         = EnvFlags(0x40000000)
    MDBX_WRITEMAP*       = EnvFlags(0x0080000)
    MDBX_NOTLS*          = EnvFlags(0x0200000)
    MDBX_NORDAHEAD*      = EnvFlags(0x0800000)
    MDBX_NOMEMINIT*      = EnvFlags(0x1000000)
    MDBX_COALESCE*       = EnvFlags(0x2000000)
    MDBX_LIFORECLAIM*    = EnvFlags(0x4000000)
    MDBX_PAGEPERTURB*    = EnvFlags(0x8000000)
    MDBX_NOMETASYNC*     = EnvFlags(0x0040000)
    MDBX_SAFE_NOSYNC*    = EnvFlags(0x0010000)
    MDBX_UTTERLY_NOSYNC* = EnvFlags(0x0110000)

type DBIFlags* = distinct cuint                # mdbx.h:1324
func `or`*(a, b: DBIFlags): DBIFlags {.inline.} = DBIFlags(cuint(a) or cuint(b))
const
    MDBX_DB_DEFAULTS* = DBIFlags(0x00)
    MDBX_REVERSEKEY* = DBIFlags(0x02)
    MDBX_DUPSORT* = DBIFlags(0x04)
    MDBX_INTEGERKEY* = DBIFlags(0x08)
    MDBX_DUPFIXED* = DBIFlags(0x10)
    MDBX_INTEGERDUP* = DBIFlags(0x20)
    MDBX_REVERSEDUP* = DBIFlags(0x40)
    MDBX_CREATE* = DBIFlags(0x40000)
    MDBX_DB_ACCEDE* = DBIFlags(MDBX_ACCEDE)

type DBIStateFlags* = distinct cuint
func `and`*(a, b: DBIStateFlags): DBIStateFlags {.inline.} = DBIStateFlags(cuint(a) or cuint(b))
const
    MDBX_TBL_DIRTY* = DBIStateFlags(0x01)
    MDBX_TBL_STALE* = DBIStateFlags(0x02)
    MDBX_TBL_FRESH* = DBIStateFlags(0x04)
    MDBX_TBL_CREAT* = DBIStateFlags(0x08)

type MDBXPutFlags* = distinct cuint                # mdbx.h:1347
func `or`*(a, b: MDBXPutFlags): MDBXPutFlags {.inline.} = MDBXPutFlags(cuint(a) or cuint(b))
const
    MDBX_UPSERT*    = MDBXPutFlags(0x00)
    MDBX_NOOVERWRITE* = MDBXPutFlags(0x10)
    MDBX_NODUPDATA* = MDBXPutFlags(0x20)
    MDBX_CURRENT* = MDBXPutFlags(0x40)
    MDBX_ALLDUPS* = MDBXPutFlags(0x80)
    MDBX_RESERVE* = MDBXPutFlags(0x10000)
    MDBX_APPEND* = MDBXPutFlags(0x20000)
    MDBX_APPENDDUP* = MDBXPutFlags(0x40000)
    MDBX_MULTIPLE* = MDBXPutFlags(0x80000)

type TxnFlags* = distinct cuint                # mdbx.h:1278
func `or`*(a, b: TxnFlags): TxnFlags {.inline.} = TxnFlags(cuint(a) or cuint(b))
const
    MDBX_TXN_READWRITE*     = TxnFlags(0x00)
    MDBX_TXN_RDONLY*        = TxnFlags(MDBX_RDONLY)
    MDBX_TXN_RDONLY_PREPARE* = TxnFlags(MDBX_RDONLY) or TxnFlags(MDBX_NOMEMINIT)
    MDBX_TXN_TRY*           = TxnFlags(0x10000000)
    MDBX_TXN_NOMETASYNC*    = TxnFlags(MDBX_NOMETASYNC)
    MDBX_TXN_NOSYNC*        = TxnFlags(MDBX_SAFE_NOSYNC)

type CursorOp* {.size: sizeof(cint).} = enum                # mdbx.h:1446
    MDBX_FIRST,
    MDBX_FIRST_DUP,
    MDBX_GET_BOTH,
    MDBX_GET_BOTH_RANGE,
    MDBX_GET_CURRENT,
    MDBX_GET_MULTIPLE,
    MDBX_LAST,
    MDBX_LAST_DUP,
    MDBX_NEXT,
    MDBX_NEXT_DUP,
    MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP,
    MDBX_PREV,
    MDBX_PREV_DUP,
    MDBX_PREV_NODUP,
    MDBX_SET,
    MDBX_SET_KEY,
    MDBX_SET_RANGE,
    MDBX_PREV_MULTIPLE

type MDBXErrorCode* {.size: sizeof(cint).} = enum
    MDBX_KEYEXIST = (-30799),
    MDBX_NOTFOUND = (-30798),
    MDBX_PAGE_NOTFOUND = (-30797),
    MDBX_CORRUPTED = (-30796),
    MDBX_PANIC = (-30795),
    MDBX_VERSION_MISMATCH = (-30794),
    MDBX_INVALID = (-30793),
    MDBX_MAP_FULL = (-30792),
    MDBX_DBS_FULL = (-30791),
    MDBX_READERS_FULL = (-30790),
    MDBX_TXN_FULL = (-30788),
    MDBX_CURSOR_FULL = (-30787),
    MDBX_PAGE_FULL = (-30786),
    MDBX_UNABLE_EXTEND_MAPSIZE = (-30785),
    MDBX_INCOMPATIBLE = (-30784),
    MDBX_BAD_RSLOT = (-30783),
    MDBX_BAD_TXN = (-30782),
    MDBX_BAD_VALSIZE = (-30781),
    MDBX_BAD_DBI = (-30780),
    MDBX_PROBLEM = (-30779),
    MDBX_BUSY = (-30778),
    MDBX_EMULTIVAL = (-30421),
    MDBX_EBADSIGN = (-30420),
    MDBX_WANNA_RECOVERY = (-30419),
    MDBX_EKEYMISMATCH = (-30418),
    MDBX_TOO_LARGE = (-30417),
    MDBX_THREAD_MISMATCH = (-30416),
    MDBX_TXN_OVERLAPPING = (-30415),
    MDBX_RESULT_TRUE = -1,
    MDBX_SUCCESS = 0,
    #MDBX_RESULT_FALSE = MDBX_SUCCESS,

type MDBXCopyFlags* = distinct cuint        # mdbx.h:1425
func `or`*(a, b: MDBXCopyFlags): MDBXCopyFlags {.inline.} = MDBXCopyFlags(cuint(a) or cuint(b))
const
    MDBX_CP_DEFAULTS           = MDBXCopyFlags(0x00)
    MDBX_CP_COMPACT            = MDBXCopyFlags(0x01)
    MDBX_CP_FORCE_DYNAMIC_SIZE = MDBXCopyFlags(0x02)

type MDBXEnvDeleteMode* {.size: sizeof(cint).} = enum        # mdbx.h:1850
    MDBX_ENV_JUST_DELETE = 0,
    MDBX_ENV_ENSURE_UNUSED,
    MDBX_ENV_WAIT_FOR_UNUSED

type MDBX_env* = ptr object
type MDBX_txn* = ptr object
type MDBX_cursor* = ptr object

type MDBX_dbi* = distinct uint32
func `==`*(a, b: MDBX_dbi): bool {.inline.} = ord(a) == ord(b)

type MDBX_val* {.byref.} = object
    base*: pointer
    len*: csize_t

type MDBX_stat* = object         # mdbx.h:1706
    ms_psize*: uint32
    ms_depth*: uint32
    ms_branch_pages*: uint64
    ms_leaf_pages*: uint64
    ms_overflow_pages*: uint64
    ms_entries*: uint64
    ms_mod_txnid*: uint64

type MDBX_envinfo_geo* = object
    lower*   :uint64
    upper*   :uint64
    current* :uint64
    shrink*  :uint64
    grow*    :uint64

type MDBX_envinfo_bootid* = array[8, uint64]

type MDBX_envinfo* = object      # mdbx.h:1740
    mi_geo*: MDBX_envinfo_geo
    mi_mapsize*: uint64
    mi_last_pgno*: uint64
    mi_recent_txnid*: uint64
    mi_latter_reader_txnid*: uint64
    mi_self_latter_reader_txnid*: uint64
    mi_meta0_txnid, mi_meta0_sign: uint64
    mi_meta1_txnid*, mi_meta1_sign: uint64
    mi_meta2_txnid*, mi_meta2_sign: uint64
    mi_maxreaders*: uint32
    mi_numreaders*: uint32
    mi_dxb_pagesize*: uint32
    mi_sys_pagesize*: uint32
    mi_bootid*: MDBX_envinfo_bootid
    mi_unsync_volume*: uint64
    mi_autosync_threshold*: uint64
    mi_since_sync_seconds16dot16*: uint32
    mi_autosync_period_seconds16dot16*: uint32
    mi_since_reader_check_seconds16dot16*: uint32
    mi_mode*: uint32

#### FUNCTIONS

proc mdbx_strerror*(errnum: cint): cstring  {.importc: "mdbx_strerror".}

proc mdbx_env_create*(penv: var MDBX_env): MDBXErrorCode  {.importc: "mdbx_env_create".}
proc mdbx_env_open*(env: MDBX_env, pathname: cstring, flags: EnvFlags, mode: int): MDBXErrorCode  {.importc: "mdbx_env_open".}
proc mdbx_env_close*(env: MDBX_env): MDBXErrorCode  {.importc: "mdbx_env_close".}
proc mdbx_env_copy*(env: MDBX_env, dstPath: cstring, flags: MDBXCopyFlags): MDBXErrorCode   {.importc: "mdbx_env_copy".}
proc mdbx_env_delete*(path: cstring, mode: MDBXEnvDeleteMode): MDBXErrorCode  {.importc: "mdbx_env_delete".}
proc mdbx_env_copy*(env: MDBX_env, dest: cstring, flags: EnvFlags): MDBXErrorCode  {.importc: "mdbx_env_copy".}
proc mdbx_env_get_flags*(env: MDBX_env, flags: var EnvFlags): MDBXErrorCode  {.importc: "mdbx_env_get_flags".}
proc mdbx_env_get_path*(env: MDBX_env, dest: var cstring): MDBXErrorCode  {.importc: "mdbx_env_get_path".}
proc mdbx_env_stat_ex*(env: MDBX_env, txn: MDBX_txn, stat: var MDBX_stat, bytes: csize_t): MDBXErrorCode  {.importc: "mdbx_env_stat_ex".}
proc mdbx_env_info_ex*(env: MDBX_env, txn: MDBX_txn, info: MDBX_envinfo, bytes: csize_t): MDBXErrorCode  {.importc: "mdbx_env_info_ex".}
proc mdbx_env_set_userctx*(env: MDBX_env, ctx: pointer): MDBXErrorCode  {.importc: "mdbx_env_set_userctx".}
proc mdbx_env_get_userctx*(env: MDBX_env): pointer  {.importc: "mdbx_env_get_userctx".}
proc mdbx_env_set_geometry*(env: MDBX_env, size_lower: int, size_now: int,
                size_upper: int, growth_step: int, shrink_threshold: int,
                pagesize: int): MDBXErrorCode  {.importc: "mdbx_env_set_geometry".}
proc mdbx_env_set_maxdbs*(env: MDBX_env, dbs: uint32): MDBXErrorCode  {.importc: "mdbx_env_set_maxdbs".}

proc mdbx_txn_begin*(env: MDBX_env, parent: MDBX_txn, flags: TxnFlags, txn: var MDBX_txn): MDBXErrorCode  {.importc: "mdbx_txn_begin".}
proc mdbx_txn_set_userctx*(txn: MDBX_txn, ctx: pointer): MDBXErrorCode  {.importc: "mdbx_txn_set_userctx".}
proc mdbx_txn_get_userctx*(txn: MDBX_txn): pointer  {.importc: "mdbx_txn_get_userctx".}
proc mdbx_txn_env*(txn: MDBX_txn): MDBX_env  {.importc: "mdbx_txn_env".}
proc mdbx_txn_commit*(txn: MDBX_txn): MDBXErrorCode  {.importc: "mdbx_txn_commit".}
proc mdbx_txn_abort*(txn: MDBX_txn): MDBXErrorCode  {.importc: "mdbx_txn_abort".}

proc mdbx_dbi_open*(txn: MDBX_txn, name: cstring, flags: DBIFlags, dbi: var MDBX_dbi): MDBXErrorCode  {.importc: "mdbx_dbi_open".}
proc mdbx_dbi_flags_ex*(txn: MDBX_txn, dbi: MDBX_dbi, flags: var DBIFlags, state: var DBIStateFlags): MDBXErrorCode  {.importc: "mdbx_dbi_flags_ex".}
proc mdbx_dbi_stat*(txn: MDBX_txn, dbi: MDBX_dbi, stat: var MDBX_stat, bytes: csize_t): MDBXErrorCode  {.importc: "mdbx_dbi_stat".}
proc mdbx_dbi_sequence*(txn: MDBX_txn, dbi: MDBX_dbi, result: var uint64, increment: uint64): MDBXErrorCode  {.importc: "mdbx_dbi_sequence".}
proc mdbx_drop*(txn: MDBX_txn, dbi: MDBX_dbi, del: bool): MDBXErrorCode  {.importc: "mdbx_drop".}

proc mdbx_get*(txn: MDBX_txn, dbi: MDBX_dbi, key: MDBX_val, data: var MDBX_val): MDBXErrorCode  {.importc: "mdbx_get".}
proc mdbx_get_equal_or_great*(txn: MDBX_txn, dbi: MDBX_dbi, key: var MDBX_val, data: var MDBX_val): MDBXErrorCode  {.importc: "mdbx_get_equal_or_great".}
proc mdbx_put*(txn: MDBX_txn, dbi: MDBX_dbi, key: MDBX_val, data: var MDBX_val, flags: MDBXPutFlags): MDBXErrorCode  {.importc: "mdbx_put".}
proc mdbx_put_PTR*(txn: MDBX_txn, dbi: MDBX_dbi, key: ptr MDBX_val, data: ptr MDBX_val, flags: MDBXPutFlags): MDBXErrorCode  {.importc: "mdbx_put".}
proc mdbx_del*(txn: MDBX_txn, dbi: MDBX_dbi, key: MDBX_val, data: ptr MDBX_val): MDBXErrorCode  {.importc: "mdbx_del".}

proc mdbx_cursor_open*(txn: MDBX_txn, dbi: MDBX_dbi, cursor: var MDBX_cursor): MDBXErrorCode  {.importc: "mdbx_cursor_open".}
proc mdbx_cursor_close*(cursor: MDBX_cursor)  {.importc: "mdbx_cursor_close".}
proc mdbx_cursor_get*(cursor: MDBX_cursor, key: var MDBX_val, data: var MDBX_val, op: CursorOp): MDBXErrorCode  {.importc: "mdbx_cursor_get".}
proc mdbx_cursor_put*(cursor: MDBX_cursor, key: MDBX_val, data: var MDBX_val, flags: MDBXPutFlags): MDBXErrorCode  {.importc: "mdbx_cursor_put".}
proc mdbx_cursor_del*(cursor: MDBX_cursor, flags: MDBXPutFlags): MDBXErrorCode  {.importc: "mdbx_cursor_del".}
proc mdbx_cursor_count*(cursor: MDBX_cursor, count: var csize_t): MDBXErrorCode  {.importc: "mdbx_cursor_count".}
proc mdbx_cursor_on_first*(cursor: MDBX_cursor): MDBXErrorCode  {.importc: "mdbx_cursor_on_first".}
proc mdbx_cursor_on_last*(cursor: MDBX_cursor): MDBXErrorCode  {.importc: "mdbx_cursor_on_last".}

{.pop.}
