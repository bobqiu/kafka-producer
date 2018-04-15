package com.qiu.partition;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MyPartitions implements Partitioner {
    private static Logger LOG = LoggerFactory.getLogger(MyPartitions.class);

    @Override
    public void configure(Map<String, ?> arg0) {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionNum = 0;
        try {
            partitionNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partitionNum = key.hashCode();
        }
//        System.out.println("kafkaMessage topic:"+ topic+" |key:"+ key+" |value:"+value);
        return Math.abs(partitionNum % numPartitions);
    }

    public void consumer(Consumer consumer) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records
                    .records(partition);
            for (ConsumerRecord<String, String> record : partitionRecords) {
                System.out.println(
                        "message==>key:" + record.key() + " value:" + record.value() + " offset:" + record.offset()
                                + " 分区:" + record.partition());
                if (record.value() == null || record.key() == null) {
                    consumer.commitSync();
                } else {
                    // dealMessage
                    //  KafkaServer.dealMessage(record.key(),record.value(),consumer);
//              consumer.commitSync();
                }
            }
        }
    }

}
