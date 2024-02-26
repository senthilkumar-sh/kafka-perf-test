const async = require('async')
const worker = require("workerpool")
const pool = worker.pool("./asyncWorker.js")
const _ = require('lodash')
const { Kafka } = require('kafkajs')
const EventEmitter = require('node:events');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })
const admin = kafka.admin()
//const workerpool = require("workerpool")

const run = async (qo, cb) => {

    //Executing the workers asynchronously
    // async.waterfall([
    //     async.apply(_executeWorkers, qo),
    //     _constructReport
    // ], function (err, result) {
    //     cb(result);
    // });
    
    await _executeWorkers(qo)
    await _constructReport(qo)
    cb(qo)
}

const _executeWorkers = async (qo) => {
    const kafkaEvents = new EventEmitter();

    // Create Topic
    createTopic(qo)
    // Producing
    await producer.connect()

    // for (let i = 1; i <= qo.request.totalHits; i++) {
    //     await producer.send({
    //         topic: 'topic_test_1',
    //         messages: [
    //         { value: 'Hello KafkaJS user!' },
    //         ],
    //     })
    // }

    await _execute(qo)
    console.log('counter calc = ', (parseInt(qo.request.totalHits) * parseInt(qo.request.producers)))
    let counter = 1
    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: 'topic_test_1', fromBeginning: false })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log('consumed message')
            kafkaEvents.emit( 'message', message, counter);
            console.log('consumer counter = ', counter);
            console.log('message = ', message.value.toString());
            counter++
        },
    })
    return new Promise((resolve) => {
        const handleMessage = async (message: any, counter: number) => {
            //console.log('promise counter = ', counter);
          if ((parseInt(qo.request.totalHits) * parseInt(qo.request.producers)) === counter) {
            await consumer.stop();
            await consumer.disconnect();
            console.log('disconnecting consumer')
            deleteTopic(qo)
            resolve(message);
          }
        };
        kafkaEvents.on('message', handleMessage);
    });
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

const produceMessages = async (producerCount, qo) => {
    for (let i = 1; i <= qo.request.totalHits; i++) {
        //console.log('producer' + producerCount + ' counter = ', i);
        await producer.send({
            topic: 'topic_test_1',
            messages: [
            { value: 'producer' + producerCount + ' Message'+ i},
            ],
        })
    }
}

const _execute = async (qo) => {
    const producers = qo.request.producers
    const promises = new Array()
    //_createTopic(qo)

    for (let i = 1; i <= producers; i++) {
        promises.push(produceMessages(i, qo));
    }

    //promises.push(_consumerPool(1, qo));
    console.log('promises = ', promises)
    worker.Promise.all(promises).then((values => {
        //console.log('values = ', values);
        //console.log('qo = ', qo);
        qo.response.result = new Array()

        _.forEach(values, function (value, i) {
            qo.response.result[i] = value.response
        });
        //_deleteTopic(qo)
        return qo
    }));
}

function _createTopic(qo) {
        pool.exec("createTopic", [qo]).then(result => {
            return true
        }).catch(err => {
            console.log(err);
            return false
        }).then(() => {
            console.log('then topic create done')
            pool.terminate()
        })
}

function _deleteTopic(qo) {
    pool.exec("deleteTopic", [qo]).then(result => {
        return true
    }).catch(err => {
        console.log(err);
        return false
    }).then(() => {
        console.log('then topic delete done')
        pool.terminate()
    })
}


function _producerPool(index, qo) {
    const promiseObj = new Promise((resolve, reject) => {
        //console.log('producer pool = ', qo.request.producers)
        pool.exec("produceMessages", [index, qo], {
            on: function (payload) {
                //console.log('status', payload.status);
            }
        }).then(result => {
            //worker.Promise.all(_consumerPool(index, qo)).then((values => {
                resolve(result)
            //}));
        }).catch(err => {
            console.log(err);
            reject(resolve);
        }).then(() => {
            console.log('then producer done')
            pool.terminate()
        })
    });

    return promiseObj
}

function _consumerPool(index, qo) {
    const promiseObj = new Promise((resolve, reject) =>  {
        pool.exec("consumeMessages", [index, qo]).then(result => {
            //setTimeout(resolve(result), 1000)
            setTimeout(function() { resolve(result); }, 10000);
            //resolve(result)
        }).catch(err => {
            console.log(err);
            reject(resolve);
        }).then(() => {
            console.log('then consumer done')
            pool.terminate()
        })
    });

    return promiseObj
}

const _constructReport = async (params) => {
    params.report = 'done'
    //callback(null, params);
    return params
}

exports.run = run;