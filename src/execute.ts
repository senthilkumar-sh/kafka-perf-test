const async = require('async')
const worker = require("workerpool")
const pool = worker.pool("./asyncWorker.js")
const _ = require('lodash')

const executeTest = (params, cb) => {
    async.waterfall([
        async.apply(_executeWorkers, params),
        _constructReport
    ], function (err, result) {
        cb(result);
    });
}

function _executeWorkers (qo, callback) {
    const producers = 4
    const promises = new Array()

    for (let i = 1; i <= producers; i++) {
        promises.push(_producerPool(i, qo));
    }
    console.log('promises = ', promises)
    worker.Promise.all(promises).then((values => {
        console.log('values = ', values);
        console.log('qo = ', qo);
        qo.response.result = new Array()

        _.forEach(values, function (value, i) {
            qo.response.result[i] = value.response
        });
        callback(null, qo);
    }));
}

function _producerPool(index, qo) {
    const promiseObj = new Promise((resolve, reject) => {
        pool.exec("produceMessages", [index, qo]).then(result => {
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
    const promiseObj = new Promise((resolve, reject) => {
        pool.exec("consumeMessages", [index, qo]).then(result => {
            resolve(result)
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

function _constructReport (params, callback) {
    params.report = 'done'
    callback(null, params);
}

exports.executeTest = executeTest;