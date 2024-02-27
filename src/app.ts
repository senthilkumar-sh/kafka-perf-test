import express, { Application, Request, Response } from 'express'
const execute = require('./execute')
const app : Application = express()
const port : number = 3000

app.get('/', (req: Request, res: Response) => {

    //Preparing query object which holds the request params and response calculations
    let qo = _getQueryObject(req, res)

    if (qo.report && qo.report.status == 'error') {
        res.send(qo)
    } else {
        //Executing test
        execute.run(qo, function(result) {
            res.send(result)
        })
    }
})

function _getQueryObject(req: Request, res: Response) {
    let qo: any = {}
    let request: any = {}
    let response: any = {}
    request.topic = req.query.topic
    request.totalHits = req.query.totalHits
    request.producers = req.query.producers
    request.consumers = req.query.consumers
    request.partitions = req.query.partitions
    request.messageSizeKB = req.query.messageSizeKB

    if(!req.query.topic) {
        request.canCreateTopic = true
        request.topic = 'topic_test_' + Math.floor(Math.random() * 900000) 
    }

    if(!req.query.producers) {
        request.producers = 1
    } else {
        request.producers = parseInt(request.producers)
    }

    if(!req.query.consumers) {
        request.consumers = 1
    } else {
        request.consumers = parseInt(request.consumers)
    }

    if(!req.query.totalHits) {
        request.totalHits = 10000
    } else {
        request.totalHits = parseInt(request.totalHits)
    }

    if(!req.query.partitions) {
        request.partitions = 2
    } else {
        request.partitions = parseInt(request.partitions)
    }

    if(!req.query.messageSizeKB) {
        request.messageSizeKB = 1
    } else if (req.query.messageSizeKB) {
        console.log("Size = ", parseInt(request.messageSizeKB) > 1000)
        if (parseInt(request.messageSizeKB) > 1000) {
            let report: any = {}
            report.status = 'error'
            report.message = 'Message size should be less than 1000 KB (1MB)'
            qo.report = report
        } else {
            request.messageSizeKB = parseInt(request.messageSizeKB)
        }
    }
    //request.totalHitsPerProducer = request.totalHits / request.producers

    qo.producerMetrics = new Array()
    qo.consumerMetrics = new Array()
    qo.request = request
    qo.response = response

    return qo
}

app.listen(port, () => {
    console.log(`App Started ${port}`);
})