'use strict'

const mqtt = require('mqtt-packet')

function write (client, packet, done) {
  console.log('下面發送封包顯示', packet)

  switch (packet.cmd) {
    case 'connack':
      console.log('\x1b[37m %s \x1B[0m', '回覆端封包', packet.cmd)
      break
    case 'pingresp':
      console.log('\x1b[37m %s \x1B[0m', '回覆端封包', packet.cmd)
      break
      // 發送端
    case 'puback':
      console.log('\x1b[34m %s \x1B[0m', '回覆發送端封包', packet.cmd)
      break
    case 'pubrec':
      console.log('\x1b[34m %s \x1B[0m', '回覆發送端封包', packet.cmd)
      break
    case 'pubcomp':
      console.log('\x1b[34m %s \x1B[0m', '回覆發送端封包', packet.cmd)
      break
      // 訂閱端
    case 'suback':
      console.log('\x1b[31m %s \x1B[0m', '回覆訂閱端封包', packet.cmd)
      break
    case 'unsuback':
      console.log('\x1b[31m %s \x1B[0m', '回覆訂閱端封包', packet.cmd)
      break
    case 'publish':
      console.log('\x1b[31m %s \x1B[0m', '發送訂閱端封包', packet.cmd)
      break
    case 'pubrel':
      console.log('\x1b[31m %s \x1B[0m', '發送訂閱端封包', packet.cmd)
      break
    default:
      break
  }

  let error = null
  if (client.connecting || client.connected) {
    try {
      const result = mqtt.writeToStream(packet, client.conn, { protocolVersion: 5 })
      if (!result && !client.errored) {
        client.conn.once('drain', done)
        return
      }
    } catch (e) {
      error = new Error('packet received not valid')
    }
  } else {
    error = new Error('connection closed')
  }

  setImmediate(done, error, client)
}

module.exports = write
