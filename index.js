import { check } from "k6"
import {
  Writer,
  Reader,
  Connection,
  SchemaRegistry,
  SCHEMA_TYPE_JSON,
  SASL_AWS_IAM,
  TLS_1_2,
} from "k6/x/kafka"

export const options = {
  scenarios: {
    sasl_auth: {
      executor: "constant-vus",
      vus: 1,
      duration: "10s",
      gracefulStop: "1s",
    },
  },
}

const brokers = [
  "b-1.myTestCluster.123z8u.c2.kafka.us-west-1.amazonaws.com:9098",
  "b-2.myTestCluster.123z8u.c2.kafka.us-west-1.amazonaws.com:9098",
  "b-3.myTestCluster.123z8u.c2.kafka.us-west-1.amazonaws.com:9098",
]
const topic = "xk6_kafka_json_topic"
const saslConfig = {
  algorithm: SASL_AWS_IAM,
}

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: true,
  minVersion: TLS_1_2,
}

const offset = 0
const partition = 0
const numPartitions = 1
const replicationFactor = 1

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  sasl: saslConfig,
  tls: tlsConfig,
})
const reader = new Reader({
  brokers: brokers,
  topic: topic,
  partition: partition,
  offset: offset,
  sasl: saslConfig,
  tls: tlsConfig,
})
const connection = new Connection({
  address: brokers[0],
  sasl: saslConfig,
  tls: tlsConfig,
})
const schemaRegistry = new SchemaRegistry()

if (__VU == 0) {
  connection.createTopic({
    topic: topic,
    numPartitions: numPartitions,
    replicationFactor: replicationFactor,
  })
  console.log("Existing topics: ", connection.listTopics(saslConfig, tlsConfig))
}

export default function () {
  for (let index = 0; index < 100; index++) {
    let messages = [
      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-abc-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
      {
        key: schemaRegistry.serialize({
          data: {
            correlationId: "test-id-def-" + index,
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
        value: schemaRegistry.serialize({
          data: {
            name: "xk6-kafka",
          },
          schemaType: SCHEMA_TYPE_JSON,
        }),
      },
    ]

    writer.produce({ messages: messages })
  }

  let messages = reader.consume({ limit: 10 })
  check(messages, {
    "10 messages returned": (msgs) => msgs.length == 10,
    "key is correct": (msgs) =>
      schemaRegistry
        .deserialize({ data: msgs[0].key, schemaType: SCHEMA_TYPE_JSON })
        .correlationId.startsWith("test-id-"),
    "value is correct": (msgs) =>
      schemaRegistry.deserialize({
        data: msgs[0].value,
        schemaType: SCHEMA_TYPE_JSON,
      }).name == "xk6-kafka",
  })
}

export function teardown(data) {
  if (__VU == 0) {
    connection.deleteTopic(topic)
  }
  writer.close()
  reader.close()
  connection.close()
}
