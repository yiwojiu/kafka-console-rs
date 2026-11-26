use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

/// 测试辅助：创建一个临时 topic，用来验证详情/加分区/删除，避免动到真实 topic
#[tokio::main]
async fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "kcs-temp-test".to_string());
    let partitions: i32 = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "196.168.89.17:9092")
        .create()
        .expect("create admin");
    let nt = NewTopic::new(&name, partitions, TopicReplication::Fixed(1));
    let res = admin
        .create_topics([&nt], &AdminOptions::new())
        .await
        .expect("create_topics request");
    println!("create {:?}", res);
}
