import express, {Application, Request, Response } from 'express'
const execute = require('./execute')
const app : Application = express()
const port : number = 3000

app.get('/', (req: Request, res: Response) => {

    //Preparing query object which holds the request params and response calculations
    let qo = _getQueryObject(req, res)

    //Executing test
    execute.run(qo, function(result) {
        res.send(result)
    })
})

function _getQueryObject(req: Request, res: Response) {
    let qo: any = {}
    let request: any = {}
    let response: any = {}
    request.totalHits = req.query.totalHits
    request.producers = req.query.producers
    request.consumers = req.query.consumers
    request.partitions = req.query.partitions
    request.topic = req.query.topic

    if(!req.query.producers) {
        request.producers = 1
    }

    if(!req.query.consumers) {
        request.consumers = 1
    }

    if(!req.query.totalHits) {
        request.totalHits = 10000
    }

    if(!req.query.partitions) {
        request.partitions = 2
    }

    if(!req.query.topic) {
        request.topic = 'topic_test_' + Math.floor(Math.random() * 900000) 
    }

    request.totalHitsPerProducer = request.totalHits / request.producers

    qo.request = request
    qo.response = response

    return qo
}

app.listen(port, () => {
    console.log(`App Started ${port}`);
})