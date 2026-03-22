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
