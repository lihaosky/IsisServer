#!/bin/bash

dmcs IsisServer.cs Isis.cs -r:System.Data.dll -r:System.Web.Extensions.dll -r:Mono.Data.Sqlite.dll -d:MONO_MODE
