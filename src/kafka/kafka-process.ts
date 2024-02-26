import { create } from "domain";
const EventEmitter = require('node:events');
const { Kafka, Partitioners } = require('kafkajs')
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })
const admin = kafka.admin()

const produceMessages = async (i, qo) => {
    
    // Producing Messages
    await producer.connect()
    console.log('producer sending message');
    await producer.send({
        topic: qo.request.topic,
        messages: [
            { value: 'Kafka Testing message from producer' + i },
        ],
    })

    //await consumeMessages(i, qo)
}

const consumeMessages = async (i, qo) => {
    
    // Consuming Messages
    await consumer.connect()
    await consumer.subscribe({ topic: 'topic_test_1' })
    console.log('Started consuming messages')
    try {
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log('kafka consumed ' + i + ' = ' + message.value.toString() + ' partition : ' + partition + ' offset : ' + message.offset)
            },
        })
    } catch (e) {
        console.log('Consumer error =', e)
    }
}

const consumerNotified = async (i, qo) => {
    const kafkaEvents = new EventEmitter();

    try {
        const consumer = kafka.consumer({ groupId: 'group' + i });
        await consumer.connect();
        await consumer.subscribe(['topic_test_1']);
        await consumer.run({
            eachMessage: async (messagePayload: any) => {
                kafkaEvents.emit('message', JSON.parse(messagePayload), i);
            }
        });

        return new Promise((resolve) => {
            const handleMessage = async (notification: { id: string; status: string; }, currentId: string) => {
              if (notification.id === currentId) {
                await consumer.stop();
                await consumer.disconnect();
                console.log('disconnecting consumer'+ i)
                resolve(notification.status.toUpperCase());
              }
            };
            kafkaEvents.on('message', handleMessage);
        });

    } catch (error) {
        console.log('Error:', error);
    }
}

const createTopic = async (qo) => {
    
    // Topic Creation
    await admin.connect()
    await admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [{
            topic: qo.request.topic,
            numPartitions: 4
        }]
    })
    await admin.disconnect()

    return true
}

const deleteTopic = async (qo) => {
    // Topic Deletion
    await admin.connect()
    await admin.deleteTopics({
        topics: [qo.request.topic],
        timeout: 20000, // default: 5000
    })

    await admin.disconnect()

    return true
}

module.exports = {
    createTopic: createTopic,
    deleteTopic: deleteTopic,
    produceMessages: produceMessages,
    consumeMessages: consumeMessages,
    consumerNotified: consumerNotified
}