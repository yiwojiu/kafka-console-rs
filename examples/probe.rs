use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::Message;
use std::time::Duration;

fn main() {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "196.168.89.17:9092")
        .set("group.id", "probe-browse-test2")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("__consumer_offsets", 7, Offset::Beginning).unwrap();
    c.assign(&tpl).unwrap();
    c.seek("__consumer_offsets", 7, Offset::Beginning, Duration::from_secs(5)).unwrap();
    println!("position after seek: {:?}", c.position());
    for i in 0..5 {
        match c.poll(Duration::from_secs(2)) {
            Some(Ok(m)) => println!("msg {} offset={} len={}", i, m.offset(), m.payload().map_or(0, |p| p.len())),
            Some(Err(e)) => println!("err: {}", e),
            None => println!("poll {} -> None", i),
        }
    }
}
