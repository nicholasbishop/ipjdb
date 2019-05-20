# ipjdb (in-process JSON database)

This is a very simple database for storing JSON data. Each database
consists of a number of collections. Collections contain items, which
are JSON files.

The database is read and modified through a library; there is no
separate server process.

## Storage

Opening a database for the first time creates a directory. Collections
are created as subdirectories within that root directory. Items in
each collection are files containing JSON data with a unique ID as the
file name. Example:

    my_db.ipjdb/
        my_first_collection/
            8f1c09b585c57a94
            2df515d82e2d8e59
            c11237553a8eeede
        my_second_collection/
            75d2bcbf589fb94b
            46b04bb7277a7e46

## Concurrency

File locking is used to make concurrent access to the database
safe. Locks are taken at the collection level. Write operations take
an exclusive lock and read operations take a shared lock.

## Safety

I make no promises as to the production-readiness of this library. It
needs a lot more tests, if nothing else. Patches welcome :)

## License

Apache-2.0
