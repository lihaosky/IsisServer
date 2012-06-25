#!/bin/bash

dmcs NonblockingServer.cs -r:System.Data.dll -r:System.Web.Extensions.dll -r:Mono.Data.Sqlite.dll -d:MONO_MODE
