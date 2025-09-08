#!/bin/sh
#
#
docker run --name dbtbl-postgres -e POSTGRES_PASSWORD=password -p 5432:5432 -v pgdata:/var/lib/postgresql/data -d postgres
