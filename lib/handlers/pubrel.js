'use strict'

const write = require('../write')

function ClientPacketStatus (client, packet) {
  this.client = client
  this.packet = packet
}

function PubComp (packet) {
  this.cmd = 'pubcomp'
  this.messageId = packet.messageId
}

const pubrelActions = [
  pubrelGet,
  pubrelPublish,
  pubrelWrite,
  pubrelDel
]
function handlePubrel (client, packet, done) {
  client.broker._series(
    new ClientPacketStatus(client, packet),
    pubrelActions, {}, done)
}

function pubrelGet (arg, done) {
  const persistence = this.client.broker.persistence
  persistence.incomingGetPacket(this.client, this.packet, reply)

  function reply (err, packet) {
    arg.packet = packet
    done(err)
  }
}

function pubrelPublish (arg, done) {
  //TODO: @@加上pub_outbox地方 如果有要比較retainLimit大小
  let client = this.client

  if (arg.packet.properties.userProperties.retainLimit === -1 || arg.packet.properties.userProperties.retainLimit >= 1) {
    client.broker.persistence.createPubOutbox(client, arg.packet, function isCreateSuccess (err) {
      if(!err){
        client.broker.publish(arg.packet, client, done)
      }
    })
  }else{
    client.broker.publish(arg.packet, client, done)
  }


}

function pubrelWrite (arg, done) {
  write(this.client, new PubComp(arg.packet), done)
}

function pubrelDel (arg, done) {
  const persistence = this.client.broker.persistence
  persistence.incomingDelPacket(this.client, arg.packet, done)
}

module.exports = handlePubrel
