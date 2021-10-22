'use strict'

class E2eState {
    constructor(receiptPacket) {
        this.e2eCount = receiptPacket.properties.userProperties.e2eCount;
        this.responseTopic = receiptPacket.properties.responseTopic;
        // this.correlationData = receiptPacket.properties.correlationData;
        this.properties = receiptPacket.properties;
        this.clientIdArray = new Array;
    }

    isCountReached() {
        if (this.clientIdArray.length >= this.e2eCount) {
            let message = {
                topic: this.responseTopic,
                payload: 'The number of e2eCount has reached 100.' + this.clientIdArray.toString() + ' successfully received',
                properties: this.properties,
                qos: 0
            }
            this.clientIdArray.length = 0
            return message
        } else {
            return false
        }
    }
}

let e2eStatebyTD = {}

function handlePubcomp(client, packet, done) {
    // TODO: dequeueSubOutbox 接著執行回執部分 回執資料要可以撈的到
    const persistence = client.broker.persistence
    persistence.dequeueSubOutbox(client, packet, function (err, receiptPacket) {
        if (receiptPacket === false) {
            return
        }
        let responseTopic = receiptPacket.properties.responseTopic;
        let correlationData = receiptPacket.properties.correlationData;
        let e2eCount = receiptPacket.properties.userProperties.e2eCount;
        //有存到suboutbox才會有這訊息
        e2eStatebyTD[responseTopic + correlationData] = e2eStatebyTD[responseTopic + correlationData] || new E2eState(receiptPacket)
        //e2eCount 為1時每次有成功傳送都將回執訊息給發送端
        if (e2eCount == 1) {
            const message = {
                topic: responseTopic,
                payload: 'client ' + client.id + ' successfully received',
                properties: receiptPacket.properties,
                qos: 0
            }
            client.broker.mq.emit(message, done)
            // client.broker.emit('receipt', receiptPacket, client)
            done(err)
        } else if (e2eCount > 1) {
            e2eStatebyTD[responseTopic + correlationData].clientIdArray.push(client.id);
            let message = e2eStatebyTD[responseTopic + correlationData].isCountReached();
            if (message) {
                client.broker.mq.emit(message, done)
            } else {
                //e2ecount數量還沒到不用做事
            }
            done(err)
        } else if (e2eCount == 0) {
            done(err)
        }
    })
}

module.exports = handlePubcomp
