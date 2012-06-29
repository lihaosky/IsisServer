#!/bin/bash
gcc testMono.c `pkg-config --cflags --libs mono-2` -g
dmcs MonoIsis.cs Isis.cs -r:System.Data.dll -r:System.Web.Extensions.dll -r:Mono.Data.Sqlite.dll -d:MONO_MODE
