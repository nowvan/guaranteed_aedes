'use strict'

const write = require('../write')
const QoSPacket = require('../qos-packet')

// function PubRel (packet) {
//   this.cmd = 'pubrel'
//   this.messageId = packet.messageId
// }

function handlePubrec (client, packet, done) {
  packet.cmd = 'pubrel'
  const pubrel = new QoSPacket(packet,client)

  if (client.clean) {
    write(client, pubrel, done)
    return
  }

  // client.broker.persistence.outgoingUpdate(client, pubrel, reply)
  ///更新outbox換成pubrel的packet
  client.broker.persistence.updateSubOutbox(client, pubrel, reply)
  // TODO: updatesuboutbox更新換成pubrel


  function reply (err) {
    if (err) {
      done(err)
    } else {
      write(client, pubrel, done)
    }
  }
}

module.exports = handlePubrec
