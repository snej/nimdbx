import private/libmdbx, private/utils
from os import nil

let kDefaultFlags       = MDBX_LIFORECLAIM
let kDefaultFileMode    = 0o600
let kDefaultMinFileSize =   5 * 1024 * 1024
let kDefaultMaxFileSize = 400 * 1024 * 1024
let kFileGrowsBy        =  10 * 1024 * 1024
let kFileShrinksBy      =   5 * 1024 * 1024
let kDefaultMaxDBIs     =  20'u32


type
    FlatDBObj* = object
        env* {.requiresInit.}: MDBX_env
        #openDBIMutex: mutex
    FlatDB* = ref FlatDBObj


proc `=destroy`(db: var FlatDBObj) =
    discard mdbx_env_close(db.env)


proc getDB(env: MDBX_env): FlatDB =
    cast[FlatDB](mdbx_env_get_userctx(env))


proc stats*(db: FlatDB): MDBX_stat =
    check mdbx_env_stat_ex(db.env, nil, result, csize_t(sizeof(result)))


proc path*(db: FlatDB): string =
    var cpath: cstring
    check mdbx_env_get_path(db.env, cpath)
    return $cpath


proc openDB*(path: string,
             flags = kDefaultFlags,
             fileMode = kDefaultFileMode,
             minFileSize = kDefaultMinFileSize,
             maxFileSize = kDefaultMaxFileSize): FlatDB =
    var env: MDBX_env
    check mdbx_env_create(env)
    check mdbx_env_set_geometry(env,
                                minFileSize,        # lower bound
                                -1,                 # size_now (-1 = 'keep current size')
                                maxFileSize,        # upper bound
                                kFileGrowsBy,       # growth step
                                kFileShrinksBy,     # shrink threshold
                                -1)                 # page size
    check mdbx_env_set_maxdbs(env, kDefaultMaxDBIs)
    check mdbx_env_open(env, path, flags or MDBX_NOTLS, fileMode)

    let db = FlatDB(env: env)
    check mdbx_env_set_userctx(env, cast[pointer](db))
    return db


proc close*(db: FlatDB) =
    check mdbx_env_close(db.env)
    db.env = nil


proc existsDB*(path: string): bool =
    ## Returns true if
    os.dirExists(path)

proc eraseDB*(path: string) =
    if os.existsOrCreateDir(path):
        os.removeDir(path)
        discard os.existsOrCreateDir(path)

proc deleteDB*(path: string) =
    os.removeDir(path)
