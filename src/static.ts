//const { parentPort } = require('node:worker_threads');
const { DynamicPool } = require('node-worker-threads-pool');

const dynamicPool = new DynamicPool(4);

dynamicPool
  .exec({
    task: (n: number) => n + 1,
    param: 1
  })
  .then((result: number) => {
    console.log(result); // result will be 2.
  });

dynamicPool
  .exec({
    task: (n: number) => n + 2,
    param: 1
  })
  .then((result: number) => {
    console.log(result); // result will be 3.
  });