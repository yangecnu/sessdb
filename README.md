# SessDB

A Big, Fast, Persistent Key/Value Store based on a variant of LSM(Log Structured Merge Tree).

## Feature Highlight:
1. **High Read/Write Performance**: write performance close to O(1) direct memory access, worst average read performance close to O(1) disk acess, tailored for session data scenarios, also suitable for caching data scenarios.
2. **Persistence**: all data is persisted in disk file, no data eviction issue as Memcached, suitable for session data scenarios.
3. **Big**: can store data size bigger than memory.
4. **Effective Memory Usage**: uses only a small amount of heap memory, leverages a hierarchical storage mechanism, only most recently inserted fresh data resides in heap memory, a big amount of less fresh data resides in memory mapped file, a huge amout of old data resides in disk file; hierarchical sotarge ensure high read/write performance, while heap GC has no big performance impact.
5. **Thread Safe**: supporting multi-threads concurrent and non-blocking access.
6. **Crash Resistance**: all data is durable, process crashes or dies, all data can be quickly restored by just restarting the machine or process.
7. **Compaction**: automatic expired and deleted data cleanup, avoiding disk and memory space waste.
8. **Light in Design & Implementation**: simple interface similar to Map, only supports Get/Put/Delete operations, cross platform Java based, small codebase size, embeddable.

## Performance Highlight:
Suppose 10 bytes key and 100 bytes value, on normal PC, random read can be **> 500,000** ops/sec, random write can be **> 200,000** ops/sec;
On server grade machine, random read can be **> 2,000,000*** ops/sec, random write can be **> 500,000** ops/sec.


## The Architecture
![sessdb architecture](https://raw.githubusercontent.com/ctriposs/sessdb/master/doc/sessdb_arch.png)

## How to Use
TODO

## Docs
1. [(Chinese)高性能Key/Value存储引擎SessionDB](https://github.com/ctriposs/sessdb/raw/master/doc/SessionDB.docx)


## Version History
TODO

##Copyright and License
Copyright 2012 ctriposs

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License. You may obtain a copy of the License in the LICENSE file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 