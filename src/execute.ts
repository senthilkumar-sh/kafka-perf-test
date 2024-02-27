const _ = require('lodash')
const async = require('async')
const worker = require("workerpool")
const pool = worker.pool("./asyncWorker")
const randomstring = require("randomstring");
const EventEmitter = require('node:events');
const percentile = require("percentile");
const { Kafka, CompressionTypes } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })
const admin = kafka.admin()

const run = async (qo, cb) => {
    await _executeWorkers(qo)
    await _constructReport(qo)
    cb(qo)
}

const _executeWorkers = async (qo) => {

    // If topic not provided in request query param
    if (qo.request.canCreateTopic) {
        // Create Topic
        createTopic(qo)
    }

    try {
        // Consuming
        await consumer.connect()
        await consumer.subscribe({ topic: qo.request.topic, fromBeginning: false })

        // Test start time
        const start = performance.now();

        // Producer connect
        await producer.connect()

        // Produce messages
        await _execute(qo)
        console.log('counter calc = ', (parseInt(qo.request.totalHits) * parseInt(qo.request.producers)))

        await consumeMessages(1, qo, start)

    } catch (err: any) {
        console.log('error = ', err.toString())
        qo.response.error = err.toString()
    }
}

const consumeMessages = async (consumerCount, qo, start) => {
    const kafkaEvents = new EventEmitter();
    let counter = 1

    // Consumer run receive each messages
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            kafkaEvents.emit( 'message', message, counter);
            //console.log('message = ' + message.key + ', Length = ', message.value.toString());
            counter++
        },
    })

    // Promise to handle message from consumer and stop once consumer receives all the messages
    return new Promise((resolve) => {
        const handleMessage = async (message: any, counter: number) => {

            if ((parseInt(qo.request.totalHits) * parseInt(qo.request.producers)) === counter) {
                await consumer.stop();
                await consumer.disconnect();
                console.log('disconnecting consumer')
                
                // Test end time
                const end = performance.now();

                qo.response.duration = Math.round(end - start)
                qo.response.executionTime = new Date().toUTCString()
                // If topic not provided in request query param
                if (qo.request.canCreateTopic) {
                    // Delete topic
                    //deleteTopic(qo)
                }
                resolve(message);
            }
        };

        kafkaEvents.on('message', handleMessage);
    });
}

const produceMessages = async (producerCount, qo) => {
    let message = randomstring.generate({
        length: (parseInt(qo.request.messageSizeKB) * 1000),
        charset: 'alphabetic'
      });

    let latencyTime: any = 0
    let maxLatency = 0
    let minLatency = 0
    let producerName = 'Producer' + producerCount
    let latencies = new Array()
    const producerStart = performance.now();

    for (let i = 1; i <= qo.request.totalHits; i++) {
        const start = performance.now();

        await producer.send({
            topic: qo.request.topic,
            compression: CompressionTypes.GZIP,
            messages: [{ 
                key: 'producer-' + producerCount + '-key',
                value: 'producer' + producerCount + ' Message'+ i + " - " + message
            }],
        })

        const end = performance.now();
        const currentLatency = (end - start)
        latencies.push(currentLatency.toFixed(2))
        latencyTime = latencyTime + currentLatency
        //console.log('latencyTime = ', latencyTime)
        if (maxLatency < currentLatency) {
            maxLatency = currentLatency;
        }

        if (i == 1) {
            minLatency = currentLatency
        } else if (minLatency > currentLatency) {
            minLatency = currentLatency
        }
        //console.log(producerName + ' minLatency per call = ', minLatency)
    }

    const producerEnd = performance.now();

    let producerTimeTaken = (producerEnd - producerStart)
    // Calculating percentiles
    const percentiles = percentile(
        [90, 95, 99], // calculates 70p, 80p and 90p in one pass
        latencies
      );

    // Assigning latency metrics
    const metrics: any = {}
    metrics.name = producerName
    metrics.timeTaken = producerTimeTaken
    metrics.timeTaken = Math.round(metrics.timeTaken.toFixed(2))
    const rps = parseFloat((qo.request.totalHits / (metrics.timeTaken / 1000)).toFixed(2))
    metrics.recordsPerSec = Math.round(rps)
    const mbs =  (qo.request.totalHits *  (qo.request.messageSizeKB / 1000))
    const tps = metrics.timeTaken / 1000
    metrics.throughputMBPerSec = Math.round(mbs / tps)

    const latency: any = {}
    latency.avg = parseFloat((latencyTime / parseInt(qo.request.totalHits)).toFixed(2))
    latency.max = parseFloat(maxLatency.toFixed(2))
    latency.min = parseFloat(minLatency.toFixed(2))
    latency.p90 = parseFloat(percentiles[0])
    latency.p95 = parseFloat(percentiles[1])
    latency.p99 = parseFloat(percentiles[2])

    metrics.latency = latency
    qo.producerMetrics.push(metrics)
}

const _execute = async (qo) => {
    const producers = qo.request.producers
    const promises = new Array()
    //_createTopic(qo)

    for (let i = 1; i <= producers; i++) {
        promises.push(produceMessages(i, qo));
    }

    console.log('promises = ', promises)
    worker.Promise.all(promises).then((values => {
        //_deleteTopic(qo)
        return qo
    }));
}

const _constructReport = async (qo) => {
    //console.log('qo ', qo.producerMetrics)
    let maxLatency: number = 0
    let minLatency: number = 0
    let averageLatency: number = 0
    let latencies = new Array()
    let recordsPerSec: number = 0
    let throughputMBPerSec: number = 0

    _.forEach(qo.producerMetrics, function(metrics, i) {
        //console.log(metrics);
        if (maxLatency < metrics.latency.max) {
            maxLatency = metrics.latency.max;
        }

        if (i == 1) {
            minLatency = metrics.latency.min
        } else if (minLatency > metrics.latency.min) {
            minLatency = metrics.latency.min
        }

        averageLatency = (averageLatency + parseFloat(metrics.latency.avg))
        console.log('averageLatency = ', averageLatency)
        recordsPerSec = recordsPerSec + metrics.recordsPerSec
        throughputMBPerSec = throughputMBPerSec + metrics.throughputMBPerSec

        latencies.push(metrics.latency.p90)
        latencies.push(metrics.latency.p95)
        latencies.push(metrics.latency.p99)
      });
    let report: any = {}
    report.status = 'success'
    report.message = 'Metrics calculated'

    let producerMetrics : any = {}
    producerMetrics.maxLatency = maxLatency
    producerMetrics.minLatency = minLatency
    producerMetrics.avgLatency = parseFloat((averageLatency / qo.request.producers).toFixed(2))
    producerMetrics.recordsPerSec = (recordsPerSec / qo.request.producers)
    producerMetrics.throughputMBPerSec = (throughputMBPerSec / qo.request.producers)
    
    const percentiles = percentile(
        [90, 95, 99], // calculates 70p, 80p and 90p in one pass
        latencies
      );

    producerMetrics.p90 = parseFloat(percentiles[0])
    producerMetrics.p95 = parseFloat(percentiles[1])
    producerMetrics.p99 = parseFloat(percentiles[2])

    qo.response.producerMetrics = producerMetrics

    qo.report = report
    return qo
}


const createTopic = async (qo) => {
    
    // Topic Creation
    await admin.connect()
    await admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [{
            topic: qo.request.topic,
            numPartitions: 4,
            replicationFactor: 1
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

exports.run = run;