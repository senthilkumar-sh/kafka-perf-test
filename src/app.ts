import express, {Application, Request, Response } from 'express'
const execute = require('./execute')

const app : Application = express()
const port : number = 3000

app.get('/', (req: Request, res: Response) => {
    let qo: any = {}
    qo.request = {}
    qo.response = {}
    qo.request.producers = req.query.producers
    qo.request.consumers = req.query.consumers
    qo.request.partitions = req.query.partitions

    execute.executeTest(qo, function(result) {
        console.log('execute = ', result)
        res.send(result)
    })
})

app.listen(port, () => {
    console.log(`App Started ${port}`);
})