# TimeVault

A Rust library for persistently storing ordered records (events, IoT measurements, alarms, etc.) in
files, grouped by partitions (UUIDs).

Provides an out-of-the-box `RaftLogStorage` implementation for [OpenRaft](https://github.com/databendlabs/openraft).

> [!IMPORTANT]
> 
> Timevault is early experimental and it is mostly AI-written. 
> While I've paid attention to the code and reviewed what's been generated,
> it is NOT something I would use in production as of now. (I will, eventually,
> but only after a lot of testing and polishing.)
> 

## Status

🚧 Early experimental. Anything may change.

- There are no API level docs and examples yet, I'll write that sooner or later.
- There is no published crate, I'll create one only if someone asks for it.

There is a [design document](./doc/design.md) and in the `docs` directory.

## Rationale

What I wanted:

- store event and measurement data for my IoT system
- reliable, persistent and disaster-tolerant
- embedded, avoid external databases and services
- simple file formats, processable with command shell tools

Existing solutions offer parts of this, but I haven't found a good one that
meets all of these requirements.

## Project structure

- `docs` - documentation
- `examples` - examples of using the library
- `test-utils` - utilities for testing
- `timevault` - the library itself