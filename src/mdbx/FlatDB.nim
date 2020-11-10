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
        #openDBIMutex: mutex    # TODO: Implement this

    FlatDB* = ref FlatDBObj
        ## An open database file. Data is stored in Collections within it.


proc `=destroy`(db: var FlatDBObj) =
    discard mdbx_env_close(db.env)


proc getDB(env: MDBX_env): FlatDB =
    cast[FlatDB](mdbx_env_get_userctx(env))


proc stats*(db: FlatDB): MDBX_stat =
    ## Returns low-level information about the database file.
    check mdbx_env_stat_ex(db.env, nil, result, csize_t(sizeof(result)))


proc path*(db: FlatDB): string =
    ## The filesystem path the database was opened with.
    var cpath: cstring
    check mdbx_env_get_path(db.env, cpath)
    return $cpath


proc openDB*(path: string,
             flags: libmdbx.EnvFlags = kDefaultFlags,
             fileMode = kDefaultFileMode,
             minFileSize = kDefaultMinFileSize,
             maxFileSize = kDefaultMaxFileSize): FlatDB =
    ## Opens a Database.
    ## * path: The filesystem path of the database directory
    ## * flags: libmdbx environment flags. Default is ``MDBX_LIFORECLAIM``. Other useful flags are
    ##          ``MDBX_RDONLY`` (read-only) and ``MDBX_EXCLUSIVE`` (don't allow any other Database
    ##          instances, or other processes, to open the file; improves speed.)
    ##          Other flags are complex and can be dangerous. See mdbx.h for details.
    ## * fileMode: The POSIX permissions for the directory and files (defaults to 0o600)
    ## * minFileSize: The initial file size, in bytes. Will not shrink below this.
    ## * maxFileSize: The maximum size the file can grow to.
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
    ## Closes a Database. It is illegal to reference the Database and objects derived from it --
    ## Collections, Snapshots, Transactions, Cursors -- afterwards.
    ## (The Database is also closed when its object is destroyed.)
    check mdbx_env_close(db.env)
    db.env = nil


proc existsDB*(path: string): bool =
    ## Returns true if a Database exists at the given path.
    os.dirExists(path)

proc eraseDB*(path: string) =
    ## Erases the contents of a Database at the given path, by emptying the directory.
    ## The file must not be open!
    if os.existsOrCreateDir(path):
        os.removeDir(path)
        discard os.existsOrCreateDir(path)

proc deleteDB*(path: string) =
    ## Deletes the Database directory at the given path.
    ## The file must not be open!
    os.removeDir(path)
