var exec = require('child_process').exec;
const express =  require('express');
const router = express.Router();
const async = require('async');
const _ = require('lodash');
let kafkaTopic = 'koffshop-events1';

router.get('/', (req, res) => {
    console.log('test');
    // _executeTest(function (result) {
    //     console.log('executed');
    //     console.log('result');
    //     res.send(result);
    // });
    let noOfRecords = 100
    if (req.query.nor) {
        noOfRecords = req.query.nor
    }
    let recordSize = 100
    if (req.query.rs) {
        recordSize = req.query.rs
    }

    console.log('noOfRecords = ', noOfRecords)
    console.log('recordSize = ', recordSize)
    let params = {
        noOfRecords: noOfRecords,
        recordSize: recordSize
    }
    async.waterfall([
        async.apply(_produceMessages, params),
        _consumeMessages
    ], function (err, result) {
        // result now equals 'done'
        if (err) {
            return next(err);
        }
        res.send(result);
    });

    //return res.send(result);
});


function _produceMessages (params, callback) {
    var command = __dirname + '/bin/kafka-producer-perf-test.sh --topic '+ kafkaTopic +' --num-records '+ params.noOfRecords +' --record-size '+ params.recordSize +' --throughput -1 --producer-props bootstrap.servers=localhost:9092 batch.size=10000 acks=1 linger.ms=100000 buffer.memory=4294967296 compression.type=gzip request.timeout.ms=300000 --command-config /bin/config.properties';
    let sData = "";
    var child = exec(command, function(err, out, code) {
        if(err) {
            console.log('err = ', err);
        }
    });
    child.stdout.on('data',
        (data) => {
            console.log(`stdout: ${data}`);
            sData += _.replace(data, '\n', ', ');
        });
    
    child.stderr.on('data',
        (data) => {
            console.error(`stderr: ${data}`);
        });
    
    child.on('close',
        (code) => {
            console.log(
                `child process exited with code ${code}`
            );
            //let producedData = '2 records sent, 8.032129 records/sec (0.01 MB/sec), 128.50 ms avg latency, 241.00 ms max latency, 241 ms 50th, 241 ms 95th, 241 ms 99th, 241 ms 99.9th.\n';

            console.log('sData = ', sData)
            let producedDataArr = _.split(sData, ', ')

            console.log(producedDataArr)
            let produced = {}
            let pDataArr = []

            _.forEach(producedDataArr, function (data, i) {
                let pData = {}
                arr = _.split(data, ' ');
                console.log(arr);
                pData.value = arr[0]
                pData.unit = arr[1]
                pData.response = arr[2]
                if (arr[3]) {
                    pData.label = arr[3]
                }
                
                if (!produced.records && arr[1] == 'records') {
                    produced.totalHits = 1;
                    let records = {}
                    records.value = _.toNumber(arr[0])
                    records.response = arr[2]
                    produced.records = records
                } else if (arr[1] == 'records') {
                    produced.totalHits += 1;
                    let records = produced.records
                    records.value = records.value + _.toNumber(arr[0])
                    produced.records = records
                }

                if (!produced.recordsSec && arr[1] == 'records/sec') {
                    let recordsSec = {}
                    recordsSec.value = _.toNumber(arr[0])
                    recordsSec.unit = arr[1]
                    recordsSec.response = arr[2] + _.replace(arr[3], '.', '')
                    produced.recordsSec = recordsSec
                } else if (arr[1] == 'records/sec') {
                    let recordsSec = produced.recordsSec
                    recordsSec.value = recordsSec.value + _.toNumber(arr[0])
                    produced.recordsSec = recordsSec
                }

                if (!produced.avg && arr[2] == 'avg') {
                    let avg = {}
                    avg.value = _.toNumber(arr[0])
                    avg.unit = arr[1]
                    avg.response = arr[3]
                    produced.avg = avg
                } else if (arr[2] == 'avg') {
                    let avg = produced.avg
                    avg.value = (avg.value + _.toNumber(arr[0]))
                    produced.avg = avg
                }

                if (!produced.max && arr[2] == 'max') {
                    let max = {}
                    max.value = _.toNumber(arr[0])
                    max.unit = arr[1]
                    max.response =  _.replace(arr[3], '.', '')
                    produced.max = max
                } else if (arr[2] == 'max') {
                    let max = produced.max
                    max.value = (max.value > _.toNumber(arr[0])) ? max.value : _.toNumber(arr[0])
                    produced.max = max
                }

                if (!produced.percentile50th && arr[2] == '50th') {
                    console.log('index = ', i)
                    let avg = produced.avg
                    produced.avg.value = produced.avg.value / produced.totalHits
                    let fiftieth = {}
                    fiftieth.value = _.toNumber(arr[0])
                    fiftieth.unit = arr[1]
                    produced.percentile50th = fiftieth
                }

                if (!produced.percentile95th && arr[2] == '95th') {
                    let ninetyFifth = {}
                    ninetyFifth.value = _.toNumber(arr[0])
                    ninetyFifth.unit = arr[1]
                    produced.percentile95th = ninetyFifth
                }

                if (!produced.percentile99th && arr[2] == '99th') {
                    let ninetyNinth = {}
                    ninetyNinth.value = _.toNumber(arr[0])
                    ninetyNinth.unit = arr[1]
                    produced.percentile99th = ninetyNinth
                }

                if (!produced.percentile99Point9th && arr[2] == '99.9th') {
                    let ninetyNinth9th = {}
                    ninetyNinth9th.value = _.toNumber(arr[0])
                    ninetyNinth9th.unit = arr[1]
                    produced.percentile99Point9th = ninetyNinth9th
                }

                //pDataArr.push(pData)
            });
            //console.log(pDataArr)
            console.info('produce params = ', params)
            callback(null, params, {
                status: 'Success',
                dataProduced: produced
            });
        });
}

function _consumeMessages(params, result, callback) {
    console.log('consumer params = ', params)
    var command = __dirname + '/bin/kafka-consumer-perf-test.sh --topic ' + kafkaTopic + ' --bootstrap-server localhost:9092 --messages '+ params.noOfRecords +' --threads 1 --command-config /bin/config.properties';
    let sData = "";
    var child = exec(command, function(err, out, code) {
        if(err) {
            console.log('consume err = ', err);
        }
    });
    child.stdout.on('data',
        (data) => {
            console.log(`consume stdout: ${data}`);
            sData += data;
        });
    
    child.stderr.on('data',
        (data) => {
            console.error(`consume stderr: ${data}`);
        });
    
    child.on('close',
        (code) => {
            console.log(
                `consume child process exited with code ${code}`
            );
            console.log('sData = ', sData)
            //let consumedData = 'WARNING: option [threads] and [num-fetch-threads] have been deprecated and will be ignored by the test\nstart.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\nWARNING: Exiting before consuming the expected number of messages: timeout (10000 ms) exceeded. You can use the --timeout option to increase the timeout.\n2024-02-20 21:20:09:315, 2024-02-20 21:20:19:720, 4.4379, 0.4265, 39304, 3777.4147, 244, 10161, 0.4368, 3868.1232\n'
            let consumedDataArr = _.split(sData, 'fetch.nMsg.sec\n')
            console.log(consumedDataArr)
            let cDataArr = []
            _.forEach(consumedDataArr, function (data, i) {
                console.log('data = ', data)
                if (!_.startsWith(data, 'WARNING')) {
                    console.log('data = ', data)    
                    let cData = {}
                    arr = _.split(data, ',');
                     console.log(arr);
                    cData.startTime = arr[0]
                    cData.endTime = arr[1]
                    cData.dataConsumedInMB = _.toNumber(_.replace(arr[2], ' ', ''))
                    cData.mbPerSec = _.toNumber(_.replace(arr[3], ' ', ''))
                    cData.consumedMsg = _.toNumber(_.replace(arr[4], ' ', ''))
                    cData.consumedMsgSec = _.toNumber(_.replace(arr[5], ' ', ''))
                    cData.rebalanceTimeMs = _.toNumber(_.replace(arr[6], ' ', ''))
                    cData.fetchTimeMs = _.toNumber(_.replace(arr[7], ' ', ''))
                    cData.fetchMBSec = _.toNumber(_.replace(arr[8], ' ', ''))
                    let fetchMsg = _.replace(arr[9], '\n', '');
                    cData.fetchMessageSec = _.toNumber(_.replace(fetchMsg, ' ', ''))
                //     pData.unit = arr[1]
                //     pData.response = arr[2]
                //     if (arr[3]) {
                //         pData.label = arr[3]
                //     }
                    cDataArr.push(cData)
                }
            });
            console.log(cDataArr)

            result.dataConsumed = cDataArr
            callback(null, result);
        });
}


module.exports = router;