'use strict'
const process = require('process');
const child_process = require('child_process');
console.log(process.pid)
let cpuTotal = 0;
let memTotal = 0;
let cpuCount = 0;
//實驗用
// setInterval(()=>{
//     let workerProcess = child_process.exec('ps -p '+process.pid+' -o %cpu,%mem,',
//         function (error, stdout, stderr) {
//             if (error) {
//                 console.log(error.stack);
//                 console.log('Error code: '+error.code);
//                 console.log('Signal received: '+error.signal);
//             }
//             // console.log('stdout: ' + stdout);
//             let arr = stdout.split(" ");
//             // console.log(arr);
//             let cpu = arr[3]||arr[4];
//             // let mem = arr[5]||arr[6]||arr[4];
//
//             const used = process.memoryUsage().rss / 1024 / 1024;
//             let mem = Math.round(used * 100) / 100;
//             memTotal += mem;
//             cpuTotal+=Number(cpu);
//             cpuCount++;
//             console.log(cpu,mem,cpuCount);
//             // console.log(cpuTotal,memTotal,cpuCount)
//             // console.log('stderr: ' + stderr);
//         });
//     // workerProcess.on('exit', function (code) {
//     //   console.log('Child process exited with exit code '+code);
//     // });
// },1000)
// setTimeout(()=>{
//     console.log('平均cpu:',cpuTotal/cpuCount,'平均mem:',memTotal/cpuCount);
//     process.exit();
// },1000*65)




const aedes = require('./aedes')()
const server = require('net').createServer(aedes.handle)
const httpServer = require('http').createServer()
const ws = require('websocket-stream')
const port = 1883
const wsPort = 8888

server.listen(port, function () {
  console.log('server listening on port', port)
})

// ws.createServer({
//   server: httpServer
// }, aedes.handle)
//
// httpServer.listen(wsPort, function () {
//   console.log('websocket server listening on port', wsPort)
// })

aedes.on('clientError', function (client, err) {
  console.log('client error', client.id, err.message, err.stack)
})

aedes.on('connectionError', function (client, err) {
  console.log('client error', client, err.message, err.stack)
})

aedes.on('publish', function (packet, client) {
  if (client) {
    // console.log('!!message from client', client.id)
  }
})

aedes.on('subscribe', function (subscriptions, client) {
  if (client) {
    console.log('subscribe from client', subscriptions, client.id)
  }
})

aedes.on('client', function (client) {
  console.log('新客戶端 new client id ', client.id)
})
