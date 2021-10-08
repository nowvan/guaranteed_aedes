'use strict'

function handlePubcomp (client, packet, done) {
  // TODO: dequeueSubOutbox 接著執行回執部分 回執資料要可以撈的到
  const persistence = client.broker.persistence
  persistence.dequeueSubOutbox(client, packet, function (err, receiptPacket) {

    if(receiptPacket === false) {

    }
    else{
      const message = {
        topic: receiptPacket.properties.responseTopic,
        payload: 'client ' + client.id + ' successfully received',
        properties: receiptPacket.properties,
        qos: 0
      }

      client.broker.mq.emit(message, done)
      // client.broker.emit('receipt', receiptPacket, client)
      done(err)
    }
  })
}

module.exports = handlePubcomp
