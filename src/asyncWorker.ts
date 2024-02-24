const workerpool = require("workerpool")
const produceMessages = (i, qo) => {
    let counter = 0;
    while (counter < 900) {
        counter++;
    }

    qo.response.producer = `Produced messages request${i}. ${i} completed!`
    qo.response.producerName = 'producer' + i
    console.log('producer' + i)
    return consumeMessages(i, qo);
}

const consumeMessages = (i, qo) => {
    let counter = 0;
    while (counter < 9000000) {
        counter++;
    }
    console.log('consume')
    qo.response.consumer = `Consumed messages request${i}. ${i} completed!`
    qo.response.consumerName = 'consumer' + i
    return qo;
}
workerpool.worker({
    produceMessages: produceMessages,
    consumeMessages: consumeMessages
})