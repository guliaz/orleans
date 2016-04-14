package com.barley.orleans

import kafka.api._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer

import scala.util.control.Breaks._

object KafkaUtilities {
  def main(args: Array[String]): Unit = {
    val utilities: KafkaUtilities = new KafkaUtilities()
    utilities.run(10, "satbb-metrics", 0, List("tvip-m1-mw-kafkabroker.dish.com"), 9092, 0)
  }
}

class KafkaUtilities {

  val replica_brokers = List()

  def run(max_reads: Long, topic: String, partition: Int, seed_brokers: List[String], port: Int, readOffset: Long = 0): Unit = {
    var max_reads_var = max_reads
    val metadata = findLeader(seed_brokers, port, topic, partition)
    if (metadata == null) {
      println(s"Can't find metadata for Topic and Partition. Exiting")
      return
    }
    if (metadata.leader == null) {
      println(s"Can't find Leader for Topic and Partition. Exiting")
      return
    }
    var leaderBroker = metadata.leader.get.host
    val clientName = s"Client_${topic}_${partition}"

    var consumer = new SimpleConsumer(leaderBroker, port, 100000, 64 * 1024, clientName)
    var readOffsetFrom: Long = 0
    if (readOffset == 0)
      readOffsetFrom = getLastOffset(consumer, topic, partition, OffsetRequest.EarliestTime, clientName)
    else
      readOffsetFrom = readOffset

    var numErrors = 0
    while (max_reads_var > 0) {
      if (consumer == null)
        consumer = new SimpleConsumer(leaderBroker, port, 100000, 64 * 1024, clientName)

      val request = new FetchRequestBuilder()
        .clientId(clientName)
        .addFetch(topic, partition, readOffsetFrom, 100000)
        .build()

      val response = consumer.fetch(request)

      if (response.hasError) {
        numErrors += 1
        // Something went wrong
        val code = response.errorCode(topic, partition)
        println(s"Error fetching data from the Broker: ${leaderBroker} Reason: ${code}")
        breakable {
          if (numErrors > 5)
            break
        }

        if (code == ErrorMapping.OffsetOutOfRangeCode) {
          readOffsetFrom = getLastOffset(consumer, topic, partition, OffsetRequest.LatestTime, clientName)
        } else {
          consumer.close()
          consumer = null
          leaderBroker = findNewleader(leaderBroker, topic, partition, port)
        }
      } else {
        numErrors = 0
        var numRead = 0
        for (messageAndOffset <- response.messageSet(topic, partition)) {
          val currentOffset = messageAndOffset.offset
          if (currentOffset < readOffsetFrom) {
            println(s"Found an old offset: ${currentOffset} Expecting: ${readOffsetFrom}")
          } else {
            readOffsetFrom = messageAndOffset.nextOffset
            val payload = messageAndOffset.message.payload.array()
            println(s"${messageAndOffset.offset}: ${new String(payload, "UTF-8")}")
            numRead += 1
            max_reads_var = max_reads_var - 1
          }
        }

        if (numRead == 0)
          sleepASec
      }
    }
    if (consumer != null)
      consumer.close()

  }

  def getLastOffset(simpleConsumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, client_name: String): Long = {
    val topicPartition = new TopicAndPartition(topic, partition)
    var requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = collection.immutable.Map[TopicAndPartition, PartitionOffsetRequestInfo]()
    requestInfo = requestInfo + (topicPartition -> new PartitionOffsetRequestInfo(whichTime, 1))
    val offsetRequest = new OffsetRequest(requestInfo)
    val offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest)

    if (offsetResponse.hasError || offsetResponse.partitionErrorAndOffsets.size <= 0) {
      println(s"Error fetching data Offset Data the Broker. Reason: ${offsetResponse.partitionErrorAndOffsets.get(topicPartition).get.error}")
      return 0
    }
    val offsets = offsetResponse.partitionErrorAndOffsets.get(topicPartition).get.offsets
    offsets(0)
  }

  def findLeader(seed_brokers: List[String],
                 port: Int,
                 topic: String,
                 partition: Int): PartitionMetadata = {
    var returnMetadata: PartitionMetadata = null

    for (seed <- seed_brokers) {
      var simpleConsumer: SimpleConsumer = null
      try {
        simpleConsumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup")
        val topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, TopicMetadataRequest.DefaultClientId, Seq(topic))
        val topicMetadataResponse = simpleConsumer.send(topicMetadataRequest)
        val metadata = topicMetadataResponse.topicsMetadata
        breakable {
          for (item <- metadata; part <- item.partitionsMetadata) {
            if (part.partitionId == partition) {
              returnMetadata = part
              // break now
              break
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (simpleConsumer != null)
          simpleConsumer.close()
      }
    }
    returnMetadata
  }

  def findNewleader(old_leader: String,
                    topic: String,
                    partition: Int,
                    port: Int): String = {
    for (i <- 0 to 3) {
      var goToSleep = false
      val metadata = findLeader(replica_brokers, port, topic, partition)
      if (metadata == null || metadata.leader == null)
        goToSleep = true
      else if (old_leader.equalsIgnoreCase(metadata.leader.get.host) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        goToSleep = true
      } else {
        return metadata.leader.get.host
      }

      if (goToSleep)
        sleepASec
    }

    println("Unable to find new leader after Broker failure. Exiting")
    throw new Exception("Unable to find new leader after Broker failure. Exiting")
  }

  def sleepASec = {
    try {
      Thread.sleep(1000)
    } catch {
      case ie: InterruptedException => {}
    }
  }

}
