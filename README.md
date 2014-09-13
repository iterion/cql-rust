# Rust CQL Client

Currently using version 2 of the CQL native protocol.

I was initially inspired by [rust-cql](https://github.com/yjh0502/rust-cql), the initial commit was much closer to the library but updated for v0.12.0 of Rust.

The design of [rust-postgres](https://github.com/sfackler/rust-postgres) also influenced some of my decisions.

# To Do

* [ ] TCP Connection
    * [x] Without Authentication
    * [ ] With Authentication
* [x] Querying
    * [x] Execute Queries
    * [x] Retrieve result rows
* [x] Error Responses
* [ ] Request/Response Compression
* [ ] A lot more...
