# CS739-p2
Distributed AFS built on top of FUSE

### Source Code:
The source code is in [`src`](/src). It contains its own `README.md` explaining how to run the project.

### Performance Scripts:
The performance scripts are in [`pythonfiles`](/pythonfiles). It contains its own `README.md` explaining how to run the performances.

# Rubric Points:

## 1. Functionality & Correctness

### 1.1 Posix Compliance
This part hosts the posix compliant fuse apis: https://github.com/hemal7735/CS739-p2/blob/main/src/fuse-client.cc#L333-L350

### 1.2 AFS Protocol and Semantics
- Protocol Primitive : [Protos](/src/protos/afs.proto) define the interface for client-server communications
- Update Visibility: This is in both `afs_client.cc` and `afs_server.cc` in `Close()` and `CloseStream()` operation.
- Stale Cache: This is in `afs_client.cc` in `Open()` and `OpenStream()` operations.

### 1.3 Durability
- Client-side FUSE-based file system is crash consistent: We are using `.temp` and `.consistent` files for crash consistency. More details in `report.pdf` and `slides.pdf`.
- Server-side persistence: Server stores files using `.temp` and atomically rename operation. It is in `Close()` and `CloseStream()` functions. More details in `report.pdf` and `slides.pdf`.

### 1.4 Crash Recovery Protocol
- Design for client crash: Client contains `Garbage Collector and Crash Recovery System` that gets activated during startup. Code: https://github.com/hemal7735/CS739-p2/blob/main/src/afs_client.cc#L100-L139
- Design for server crash: similar to client crash - https://github.com/hemal7735/CS739-p2/blob/main/src/afs_server.cc#L548-L565


## 2. Measurement

