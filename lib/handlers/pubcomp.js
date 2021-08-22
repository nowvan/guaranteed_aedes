'use strict'

function handlePubcomp (client, packet, done) {
  // TODO: dequeueSubOutbox 接著執行回執部分 回執資料要可以撈的到
  const persistence = client.broker.persistence
  persistence.dequeueSubOutbox(client, packet, function (err, receiptPacket) {
    const message = { topic: receiptPacket.properties.responseTopic, payload: 'client ' + client.name + ' successfully received' }
    client.broker.mq.emit(message, done)
    // client.broker.emit('receipt', receiptPacket, client)
    done(err)
  })
}

module.exports = handlePubcomp
