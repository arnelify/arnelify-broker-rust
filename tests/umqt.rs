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