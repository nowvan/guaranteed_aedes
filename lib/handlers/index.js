'use strict'

const handleConnect = require('./connect')
const handleSubscribe = require('./subscribe')
const handleUnsubscribe = require('./unsubscribe')
const handlePublish = require('./publish')
const handlePuback = require('./puback')
const handlePubrel = require('./pubrel')
const handlePubrec = require('./pubrec')
const handlePing = require('./ping')

function handle (client, packet, done) {
  // console.log("收到封包 handle ",packet.cmd )
  switch (packet.cmd) {
    case 'connect':
      console.log('\x1b[37m %s \x1B[0m',"收到端封包",packet.cmd);
      break
    case 'disconnect':
      console.log('\x1b[37m %s \x1B[0m',"收到端封包",packet.cmd);
      break
    case 'pingreq':
      console.log('\x1b[37m %s \x1B[0m',"收到端封包",packet.cmd);
      break
      //發送端
    case 'publish':
      console.log('\x1b[34m %s \x1B[0m',"收到發送端封包",packet.cmd);
      break
    case 'pubrel':
      console.log('\x1b[34m %s \x1B[0m',"收到發送端封包",packet.cmd);
      break
      //訂閱端
    case 'subscribe':
      console.log('\x1b[31m %s \x1B[0m',"收到訂閱端封包",packet.cmd);
      break
    case 'unsubscribe':
      console.log('\x1b[31m %s \x1B[0m',"收到訂閱端封包",packet.cmd);
      break
    case 'puback':
      console.log('\x1b[31m %s \x1B[0m',"收到訂閱端封包",packet.cmd);
      break
    case 'pubrec':
      console.log('\x1b[31m %s \x1B[0m',"收到訂閱端封包",packet.cmd);
      break
    case 'pubcomp':
      console.log('\x1b[31m %s \x1B[0m',"收到訂閱端封包",packet.cmd);
      break
    default:
      break
  }
  if (packet.cmd === 'connect') {
    if (client.connecting || client.connected) {
      // [MQTT-3.1.0-2]
      finish(client.conn, packet, done)
      return
    }
    handleConnect(client, packet, done)
    return
  }

  if (!client.connecting && !client.connected) {
    // [MQTT-3.1.0-1]
    finish(client.conn, packet, done)
    return
  }

  switch (packet.cmd) {
    case 'publish':
      handlePublish(client, packet, done)
      break
    case 'subscribe':
      handleSubscribe(client, packet, false, done)
      break
    case 'unsubscribe':
      handleUnsubscribe(client, packet, done)
      break
    case 'pubcomp':
    case 'puback':
      handlePuback(client, packet, done)
      break
    case 'pubrel':
      handlePubrel(client, packet, done)
      break
    case 'pubrec':
      handlePubrec(client, packet, done)
      break
    case 'pingreq':
      handlePing(client, packet, done)
      break
    case 'disconnect':
      // [MQTT-3.14.4-3]
      client._disconnected = true
      // [MQTT-3.14.4-1] [MQTT-3.14.4-2]
      client.conn.destroy()
      done()
      return
    default:
      client.conn.destroy()
      done()
      return
  }

  if (client._keepaliveInterval > 0) {
    client._keepaliveTimer.reschedule(client._keepaliveInterval)
  }
}

function finish (conn, packet, done) {
  conn.destroy()
  const error = new Error('Invalid protocol')
  error.info = packet
  done(error)
}

module.exports = handle
