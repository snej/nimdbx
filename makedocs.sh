#! /bin/bash -e

nim doc --project --index:on --git.url:https://github.com/snej/nimdbx --git.commit:main --outdir:htmldocs src/nimdbx.nim
rm -f htmldocs/nimdbx/private/libmdbx.*
