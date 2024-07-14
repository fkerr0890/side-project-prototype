# Peer-to-peer Web Application Hosting and Distribution

## Description

An experimental framework for hosting and distributing web applications. This would provide an alternative to cloud hosting or dedicated servers. 

Applications would be containerized with no root privilege (i.e Podman) and no network access, running on the localhost.
Apps bound to localhost would communicate with a daemon built in Rust (see [backend](https://github.com/fkerr0890/side-project-prototype/tree/master/backend)) via HTTP, allowing users to host existing containerized server apps with few modifications. Clients would access the network through standard web browsers, which would send
HTTP requests to the daemon. A browser extension (see [client extension](https://github.com/fkerr0890/side-project-prototype/tree/master/client-extension)) would facilitate session management. A static IP is not required to become a host because most NATs support UDP hole punching.

When making a request to the index page of a web application hosted on the network, the flow of data looks like this:

Browser --HTTP--> Client daemon (requester) --UDP--> Peers (host daemons) --HTTP--> App container --HTTP--> Host daemon --UDP--> Requester --HTTP--> Browser

A lot more goes into it, but I'll only share the full details if people are interested. If you'd like to learn more, feel free to message me on [Linkedin](https://www.linkedin.com/in/frederick-kerr-4bb9a6183/).

## Getting Started

### Dependencies

The main dependencies for the Rust dameon are [tokio](https://tokio.rs/) for async, [serde](https://docs.rs/serde/latest/serde/index.html) for serialization traits, [bincode](https://docs.rs/bincode/latest/bincode/) for compact binary serialization, [hyper](https://docs.rs/hyper/latest/hyper/)
for HTTP, [ring](https://docs.rs/ring/latest/ring/) for cryptography, and Tokio's [tracing](https://docs.rs/tracing/latest/tracing/) for observability. Cargo.toml contains the full list of dependencies. Some of them are likely unnecessary, and I will remove them in the future.

### Running

[simulation.rs](https://github.com/fkerr0890/side-project-prototype/tree/master/backend/tests) contains a "test" that sets up simulated peers using a peer discovery protocol I invented. Thus running
```
cargo test -- --nocapture
```
will run all unit tests and set up peers so you can test serving an application. This project still contains hardcoded values and cannot be run easily yet. You'll have to fiddle around with the code if you want to run it. A CLI is coming.

## Authors

Fred Kerr [Linkedin](https://www.linkedin.com/in/frederick-kerr-4bb9a6183/)

## Acknowledgments

* [Peer-to-Peer Communication Across Network Address Translators](https://pdos.csail.mit.edu/papers/p2pnat.pdf)
* [Tailscale- How NAT Traversal Works](https://tailscale.com/blog/how-nat-traversal-works/)
* All the authors of and contributors to the Rust crates I use
