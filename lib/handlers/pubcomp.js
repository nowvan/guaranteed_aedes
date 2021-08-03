'use strict'

function handlePubcomp (client, packet, done) {
    //TODO: 成功收到接收端接收成功處理 回執部分
    const persistence = client.broker.persistence
    persistence.outgoingClearMessageId(client, packet, function (err, origPacket) {
        client.broker.emit('ack', origPacket, client)
        done(err)
    })
}

module.exports = handlePubcomp
