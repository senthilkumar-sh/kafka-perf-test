//const producer = require('./kafka/producer-process')
const express = require('express')
const routes = require('./routes')
const app = express()
const cors = require('cors')
const _ = require('lodash');
const port = 3000

// Third-Party Middleware

app.use(cors());

// Built-In Middleware

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use('/perf', routes.process);

//let producedData = '2 records sent, 8.032129 records/sec (0.01 MB/sec), 128.50 ms avg latency, 241.00 ms max latency, 241 ms 50th, 241 ms 95th, 241 ms 99th, 241 ms 99.9th.\n';
// let producedData = '20528 records sent, 4105.6 records/sec (3.92 MB/sec), 1760.7 ms avg latency, 3313.0 ms max latency., 25536 records sent, 5107.2 records/sec (4.87 MB/sec), 5166.5 ms avg latency, 6839.0 ms max latency., 23985 records sent, 4797.0 records/sec (4.57 MB/sec), 8090.6 ms avg latency, 9098.0 ms max latency., 17230 records sent, 3446.0 records/sec (3.29 MB/sec), 10677.6 ms avg latency, 11854.0 ms max latency., 30157 records sent, 6031.4 records/sec (5.75 MB/sec), 12561.9 ms avg latency, 13075.0 ms max latency., 59911 records sent, 11982.2 records/sec (11.43 MB/sec), 10605.9 ms avg latency, 12633.0 ms max latency., 200000 records sent, 6293.860339 records/sec (6.00 MB/sec), 8712.20 ms avg latency, 13075.00 ms max latency, 9111 ms 50th, 12825 ms 95th, 13014 ms 99th, 13073 ms 99.9th.,'
// let producedDataArr = _.split(producedData, ', ')

// console.log(producedDataArr)
// let produced = {}
// let pDataArr = []

// _.forEach(producedDataArr, function (data, i) {
//     let pData = {}
//     arr = _.split(data, ' ');
//     console.log(arr);
//     pData.value = arr[0]
//     pData.unit = arr[1]
//     pData.response = arr[2]
//     if (arr[3]) {
//         pData.label = arr[3]
//     }
    
//     if (!produced.records && arr[1] == 'records') {
//         produced.totalHits = 1;
//         let records = {}
//         records.value = _.toNumber(arr[0])
//         records.response = arr[2]
//         produced.records = records
//     } else if (arr[1] == 'records') {
//         produced.totalHits += 1;
//         let records = produced.records
//         records.value = records.value + _.toNumber(arr[0])
//         produced.records = records
//     }

//     if (!produced.recordsSec && arr[1] == 'records/sec') {
//         let recordsSec = {}
//         recordsSec.value = _.toNumber(arr[0])
//         recordsSec.unit = arr[1]
//         recordsSec.response = arr[2] + _.replace(arr[3], '.', '')
//         produced.recordsSec = recordsSec
//     } else if (arr[1] == 'records/sec') {
//         let recordsSec = produced.recordsSec
//         recordsSec.value = recordsSec.value + _.toNumber(arr[0])
//         produced.recordsSec = recordsSec
//     }

//     if (!produced.avg && arr[2] == 'avg') {
//         let avg = {}
//         avg.value = _.toNumber(arr[0])
//         avg.unit = arr[1]
//         avg.response = arr[3]
//         produced.avg = avg
//     } else if (arr[2] == 'avg') {
//         let avg = produced.avg
//         avg.value = (avg.value + _.toNumber(arr[0]))
//         produced.avg = avg
//     }

//     if (!produced.max && arr[2] == 'max') {
//         let max = {}
//         max.value = _.toNumber(arr[0])
//         max.unit = arr[1]
//         max.response =  _.replace(arr[3], '.', '')
//         produced.max = max
//     } else if (arr[2] == 'max') {
//         let max = produced.max
//         max.value = (max.value > _.toNumber(arr[0])) ? max.value : _.toNumber(arr[0])
//         produced.max = max
//     }

//     if (!produced.percentile50th && arr[2] == '50th') {
//         console.log('index = ', i)
//         let avg = produced.avg
//         produced.avg.value = produced.avg.value / produced.totalHits
//         let fiftieth = {}
//         fiftieth.value = _.toNumber(arr[0])
//         fiftieth.unit = arr[1]
//         produced.percentile50th = fiftieth
//     }

//     if (!produced.percentile95th && arr[2] == '95th') {
//         let ninetyFifth = {}
//         ninetyFifth.value = _.toNumber(arr[0])
//         ninetyFifth.unit = arr[1]
//         produced.percentile95th = ninetyFifth
//     }

//     if (!produced.percentile99th && arr[2] == '99th') {
//         let ninetyNinth = {}
//         ninetyNinth.value = _.toNumber(arr[0])
//         ninetyNinth.unit = arr[1]
//         produced.percentile99th = ninetyNinth
//     }

//     if (!produced.percentile99Point9th && arr[2] == '99.9th') {
//         let ninetyNinth9th = {}
//         ninetyNinth9th.value = _.toNumber(arr[0])
//         ninetyNinth9th.unit = arr[1]
//         produced.percentile99Point9th = ninetyNinth9th
//     }

//     pDataArr.push(pData)
// });
//console.log(produced)

// let consumedData = 'WARNING: option [threads] and [num-fetch-threads] have been deprecated and will be ignored by the test\nstart.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\nWARNING: Exiting before consuming the expected number of messages: timeout (10000 ms) exceeded. You can use the --timeout option to increase the timeout.\n2024-02-20 21:20:09:315, 2024-02-20 21:20:19:720, 4.4379, 0.4265, 39304, 3777.4147, 244, 10161, 0.4368, 3868.1232\n'
// let consumedDataArr = _.split(consumedData, '.\n')
// console.log(consumedDataArr)
// let cDataArr = []
// _.forEach(consumedDataArr, function (data, i) {
    
//     if (i == 1) {
//         console.log('data = ', data)    
//          let cData = {}
//          arr = _.split(data, ',');
//     //     console.log(arr);
//            cData.startTime = arr[0]
//            cData.endTime = arr[1]
//            cData.dataConsumedInMB = arr[2]
//            cData.mbPerSec = arr[3]
//            cData.consumedMsg = arr[4]
//            cData.consumedMsgSec = arr[5]
//            cData.rebalanceTimeMs = arr[6]
//            cData.fetchTimeMs = arr[7]
//            cData.fetchMBSec = arr[8]
//            cData.fetchMessageSec = _.replace(arr[9], '\n', '');
//     //     pData.unit = arr[1]
//     //     pData.response = arr[2]
//     //     if (arr[3]) {
//     //         pData.label = arr[3]
//     //     }
//          cDataArr.push(cData)
//     }
// });
// console.log(cDataArr)
app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})