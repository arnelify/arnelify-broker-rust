<img src="https://static.wikia.nocookie.net/arnelify/images/c/c8/Arnelify-logo-2024.png/revision/latest?cb=20240701012515" style="width:336px;" alt="Arnelify Logo" />

![Arnelify Broker for Rust](https://img.shields.io/badge/Arnelify%20Broker%20for%20Rust-0.9.7-yellow) ![Rust](https://img.shields.io/badge/Rust-1.91.1-orange) ![Cargo](https://img.shields.io/badge/Cargo-1.91.1-blue)

## 🚀 About

**Arnelify® Broker for Rust** — a multi-language broker with RPC and UMQT support.

All supported protocols:
| **#** | **Protocol** | **Transport** |
| - | - | - |
| 1 | TCP2 | UMQT |
| 2 | UDP | UMQT |

## 📋 Minimal Requirements
> Important: It's strongly recommended to use in a container that has been built from the gcc v15.2.0 image.
* CPU: Apple M1 / Intel Core i7 / AMD Ryzen 7
* OS: Debian 11 / MacOS 15 / Windows 10 with <a href="https://learn.microsoft.com/en-us/windows/wsl/install">WSL2</a>.
* RAM: 4 GB

## 📦 Installation
Run in terminal:
```bash
cargo add arnelify_broker
```
## 🎉 RPC

**RPC** - operates on top of the transport layer, enabling remote function and procedure calls.

### 📚 Examples

```rust
use arnelify_broker::rpc::RPC;
use arnelify_broker::rpc::RPCLogger;
use arnelify_broker::rpc::RPCAction;
use arnelify_broker::rpc::BrokerBytes;
use arnelify_broker::rpc::BrokerCtx;
use arnelify_broker::rpc::RPCStream;
use std::sync::{Arc, Mutex};

type JSON = serde_json::Value;

fn main() -> () {
  let rpc: RPC = RPC::new();
  let rpc_logger: Arc<RPCLogger> = Arc::new(move |_level: &str, message: &str| -> () {
    println!("[Arnelify Broker]: {}", message);
  });

  rpc.logger(rpc_logger);
  let rpc_action: Arc<RPCAction> = Arc::new(
    move |ctx: Arc<Mutex<BrokerCtx>>,
          bytes: Arc<Mutex<BrokerBytes>>,
          stream: Arc<Mutex<RPCStream>>|
          -> () {
      let json: JSON = ctx.lock().unwrap().clone();
      let bytes: Vec<u8> = bytes.lock().unwrap().clone();

      let stream_lock: std::sync::MutexGuard<'_, RPCStream> = stream.lock().unwrap();
      stream_lock.push(&json, &bytes, true);
    },
  );

  rpc.on("connect", rpc_action);

  let json: JSON = serde_json::json!({"message": "Hello World"});
  let bytes: Vec<u8> = Vec::from(b"Hello World");

  let (ctx, bytes) = rpc.send("connect", &json, &bytes, true);

  let ctx: JSON = ctx.lock().unwrap().clone();
  let bytes: Vec<u8> = bytes.lock().unwrap().clone();

  println!("ctx: {}", ctx);
  println!("bytes: {:?}", bytes);
}
```
## 🎉 UMQT

**UMQT (UDP Message Query Transport)** - is a universal WEB3-transport designed for flexible transport-layer communication, supporting two messaging mechanisms: TCP2 and datagrams.

### 📚 Configuration

| **Option** | **Description** |
| - | - |
| **BLOCK_SIZE_KB**| The size of the allocated memory used for processing large packets. |
| **CERT_PEM**| Path to the TLS cert-file in PEM format. |
| **COMPRESSION**| If this option is enabled, the transport will use BROTLI compression if the client application supports it. This setting increases CPU resource consumption. The transport will not use compression if the data size exceeds the value of **BLOCK_SIZE_KB**. |
| **KEY_PEM**| Path to the TLS private key-file in PEM format. |
| **PORT**| Defines which port the transport will listen on. |
| **THREAD_LIMIT**| Defines the maximum number of threads that will handle requests. |

### 📚 Examples

```rust
use arnelify_broker::transport::UMQT;
use arnelify_broker::transport::UMQTBytes;
use arnelify_broker::transport::UMQTConsumer;
use arnelify_broker::transport::UMQTLogger;
use arnelify_broker::transport::UMQTOpts;
use std::sync::{Arc, Mutex};

fn main() {
  let umqt_opts: UMQTOpts = UMQTOpts {
    block_size_kb: 64,
    cert_pem: String::from("certs/cert.pem"),
    compression: true,
    key_pem: String::from("certs/key.pem"),
    port: 4433,
    thread_limit: 4,
  };

  let umqt: Arc<UMQT> = Arc::new(UMQT::new(umqt_opts));
  let umqt_logger: Arc<UMQTLogger> = Arc::new(move |_level: &str, message: &str| -> () {
    println!("[Arnelify Broker]: {}", message);
  });

  umqt.logger(umqt_logger);
  let umqt_consumer: Arc<UMQTConsumer> = Arc::new(move |bytes: Arc<Mutex<UMQTBytes>>| -> () {
    let bytes: Vec<u8> = bytes.lock().unwrap().clone();
    println!("bytes: {:?}", bytes);
  });

  umqt.add_server("connect", "127.0.0.1", 4433);
  umqt.on("connect", umqt_consumer);
  umqt.start();
}
```

## ⚖️ MIT License
This software is licensed under the <a href="https://github.com/arnelify/arnelify-broker-rust/blob/main/LICENSE">MIT License</a>. The original author's name, logo, and the original name of the software must be included in all copies or substantial portions of the software.

## 🛠️ Contributing
Join us to help improve this software, fix bugs or implement new functionality. Active participation will help keep the software up-to-date, reliable, and aligned with the needs of its users.

Run in terminal:
```bash
docker compose up -d --build
docker ps
docker exec -it <CONTAINER ID> bash
```
For RPC:
```bash
cargo run --bin test_rpc
```
For UMQT:
```bash
cargo run --bin test_umqt
```

## ⭐ Release Notes
Version 0.9.7 — a multi-language broker with RPC and UMQT support.

We are excited to introduce the Arnelify Broker for Rust! Please note that this version is raw and still in active development.

Change log:

* UMQT support.
* Async Runtime & Multi-Threading.
* Block processing in "on-the-fly" mode.
* BROTLI compression (still in development).
* FFI, PYO3 and NEON support.
* Significant refactoring and optimizations.

Please use this version with caution, as it may contain bugs and unfinished features. We are actively working on improving and expanding the broker's capabilities, and we welcome your feedback and suggestions.

## 🔗 Links

* <a href="https://github.com/arnelify/arnelify-pod-cpp">Arnelify POD for C++</a>
* <a href="https://github.com/arnelify/arnelify-pod-node">Arnelify POD for NodeJS</a>
* <a href="https://github.com/arnelify/arnelify-pod-python">Arnelify POD for Python</a>
* <a href="https://github.com/arnelify/arnelify-pod-rust">Arnelify POD for Rust</a>
* <a href="https://github.com/arnelify/arnelify-react-native">Arnelify React Native</a>