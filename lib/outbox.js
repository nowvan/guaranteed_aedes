'use strict'

const from2 = require('from2')
const QlobberSub = require('qlobber/aedes/qlobber-sub')
const { QlobberTrue } = require('qlobber')
const Packet = require('./aedes-packet')
const QlobberOpts = {
    wildcard_one: '+',
    wildcard_some: '#',
    separator: '/'
}

class Outobx {
    constructor(number) {
        this.size = number;
        this._outbox = new Array();
    }
// 從後面塞一個
    enqueue(val) {
        if(this._outbox.length+1 > this.size) {
            //可能之後要通知
            this._outbox.shift();
        }
        this._outbox.push(val);
    }
// 從後面拿掉一個
    dequeue() {
        return this._outbox.shift();
    }
// 從前面拿出一個
    pop() {
        return this._outbox.pop();
    }
// 從前面拿出全部
    popall() {
        let temp = this._outbox.map(x => x);
        this._outbox.length = 0;
        return temp;
    }
}


function MemoryPersistence () {
    if (!(this instanceof MemoryPersistence)) {
        return new MemoryPersistence()
    }

    this._retained = []
    // clientId -> topic -> qos
    this._subscriptions = new Map()
    this._clientsCount = 0
    this._trie = new QlobberSub(QlobberOpts)
    this._outgoing = {}
    this._incoming = {}
    this._wills = {}

    this._subOutboxs= {}
    this._pubOutboxsByT= {}
}



function matchTopic (p) {
    return p.topic !== this.topic
}

MemoryPersistence.prototype.storeRetained = function (packet, cb) {
    packet = Object.assign({}, packet)
    this._retained = this._retained.filter(matchTopic, packet)

    if (packet.payload.length > 0) this._retained.push(packet)

    cb(null)
}

function matchingStream (current, pattern) {
    const matcher = new QlobberTrue(QlobberOpts)

    if (Array.isArray(pattern)) {
        for (var i = 0; i < pattern.length; i += 1) {
            matcher.add(pattern[i])
        }
    } else {
        matcher.add(pattern)
    }

    return from2.obj(function match (size, next) {
        var entry

        while ((entry = current.shift()) != null) {
            if (matcher.test(entry.topic)) {
                setImmediate(next, null, entry)
                return
            }
        }

        if (!entry) this.push(null)
    })
}

///////////////////////
MemoryPersistence.prototype.createSubOutbox = function (client, packet, cb) {
    // TODO: 或是原本就存在sub_outbox 就不需要在重新建立在suback回應
    const id = client.id
    const subOutbox = this._subOutboxs[id] || []

    this._subOutboxs[id] = subOutbox
    subOutbox[packet.subscriptions[0]] = new Outobx(packet.properties.userProperties.retainCount)
    cb(null, client)
}

MemoryPersistence.prototype.enqueueSubOutbox = function (client, packet, cb) {
    // TODO: 在哪一時間點要新增Suboutbox
    const id = client.id
    const subOutbox = this._subOutboxs[id] || []

    this._subOutboxs[id] = subOutbox
    subOutbox[packet.subscriptions[0]] = new Outobx(packet.properties.userProperties.retainCount)
    cb(null, client)
}

//創建puboutbox並新增第一項 如果已經有就是新增一項
MemoryPersistence.prototype.createPubOutbox = function (client, packet, cb) {
    // TODO: pub_outbox 創建 以及 enqueue 新封包
    let pubOutbox = this._pubOutboxsByT[packet.topic] || []
    this._pubOutboxsByT[packet.topic] = pubOutbox

    if (pubOutbox.length === 0){
        pubOutbox = new Outobx(packet.properties.userProperties.retainLimit)

    }else if (packet.properties.userProperties.retainLimit > pubOutbox.size){
        pubOutbox.size = packet.properties.userProperties.retainLimit
    }
    pubOutbox.enqueue(packet)

    cb(null, client)
}




///////////////////////


MemoryPersistence.prototype.createRetainedStream = function (pattern) {
    return matchingStream([].concat(this._retained), pattern)
}

MemoryPersistence.prototype.createRetainedStreamCombi = function (patterns) {
    return matchingStream([].concat(this._retained), patterns)
}

MemoryPersistence.prototype.addSubscriptions = function (client, subs, cb) {
    var stored = this._subscriptions.get(client.id)
    const trie = this._trie

    //檢查是否有儲存過
    if (!stored) {
        stored = new Map()
        this._subscriptions.set(client.id, stored)
        this._clientsCount++
    }

    for (var i = 0; i < subs.length; i += 1) {
        const sub = subs[i]
        const qos = stored.get(sub.topic)
        const hasQoSGreaterThanZero = (qos !== undefined) && (qos > 0)
        //再研究
        if (sub.qos > 0) {
            trie.add(sub.topic, {
                clientId: client.id,
                topic: sub.topic,
                qos: sub.qos
            })
        } else if (hasQoSGreaterThanZero) {
            trie.remove(sub.topic, {
                clientId: client.id,
                topic: sub.topic
            })
        }
        stored.set(sub.topic, sub.qos)
    }

    cb(null, client)
}

MemoryPersistence.prototype.removeSubscriptions = function (client, subs, cb) {
    const stored = this._subscriptions.get(client.id)
    const trie = this._trie

    if (stored) {
        for (var i = 0; i < subs.length; i += 1) {
            const topic = subs[i]
            const qos = stored.get(topic)
            if (qos !== undefined) {
                if (qos > 0) {
                    trie.remove(topic, { clientId: client.id, topic })
                }
                stored.delete(topic)
            }
        }

        if (stored.size === 0) {
            this._clientsCount--
            this._subscriptions.delete(client.id)
        }
    }

    cb(null, client)
}

MemoryPersistence.prototype.subscriptionsByClient = function (client, cb) {
    var subs = null
    const stored = this._subscriptions.get(client.id)
    if (stored) {
        subs = []
        for (const topicAndQos of stored) {
            subs.push({ topic: topicAndQos[0], qos: topicAndQos[1] })
        }
    }
    cb(null, subs, client)
}

MemoryPersistence.prototype.countOffline = function (cb) {
    return cb(null, this._trie.subscriptionsCount, this._clientsCount)
}

MemoryPersistence.prototype.subscriptionsByTopic = function (pattern, cb) {
    console.log(this._trie.match(pattern))
    //{clientId: "mqttjs_sub", topic: "test1", qos: 2}
    cb(null, this._trie.match(pattern))
}

MemoryPersistence.prototype.cleanSubscriptions = function (client, cb) {
    const trie = this._trie
    const stored = this._subscriptions.get(client.id)

    if (stored) {
        for (const topicAndQos of stored) {
            if (topicAndQos[1] > 0) {
                const topic = topicAndQos[0]
                trie.remove(topic, { clientId: client.id, topic })
            }
        }

        this._clientsCount--
        this._subscriptions.delete(client.id)
    }

    cb(null, client)
}

MemoryPersistence.prototype.outgoingEnqueue = function (sub, packet, cb) {
    _outgoingEnqueue.call(this, sub, packet)
    process.nextTick(cb)
}

MemoryPersistence.prototype.outgoingEnqueueCombi = function (subs, packet, cb) {
    for (var i = 0; i < subs.length; i++) {
        _outgoingEnqueue.call(this, subs[i], packet)
    }
    process.nextTick(cb)
}

function _outgoingEnqueue (sub, packet) {
    const id = sub.clientId
    const queue = this._outgoing[id] || []

    this._outgoing[id] = queue
    const p = new Packet(packet)
    queue[queue.length] = p
}

MemoryPersistence.prototype.outgoingUpdate = function (client, packet, cb) {
    const clientId = client.id
    const outgoing = this._outgoing[clientId] || []
    var temp

    this._outgoing[clientId] = outgoing

    for (var i = 0; i < outgoing.length; i++) {
        temp = outgoing[i]
        if (temp.brokerId === packet.brokerId) {
            if (temp.brokerCounter === packet.brokerCounter) {
                temp.messageId = packet.messageId
                return cb(null, client, packet)
            }
            /*
            Maximum of messageId (packet identifier) is 65535 and will be rotated,
            brokerCounter is to ensure the packet identifier be unique.
            The for loop is going to search which packet messageId should be updated
            in the _outgoing queue.
            If there is a case that brokerCounter is different but messageId is same,
            we need to let the loop keep searching
            */
        } else if (temp.messageId === packet.messageId) {
            outgoing[i] = packet
            return cb(null, client, packet)
        }
    }

    cb(new Error('no such packet'), client, packet)
}

MemoryPersistence.prototype.outgoingClearMessageId = function (client, packet, cb) {
    const clientId = client.id
    const outgoing = this._outgoing[clientId] || []
    var temp

    this._outgoing[clientId] = outgoing

    for (var i = 0; i < outgoing.length; i++) {
        temp = outgoing[i]
        if (temp.messageId === packet.messageId) {
            outgoing.splice(i, 1)
            return cb(null, temp)
        }
    }

    cb()
}

MemoryPersistence.prototype.outgoingStream = function (client) {
    const queue = [].concat(this._outgoing[client.id] || [])

    return from2.obj(function match (size, next) {
        var entry

        if ((entry = queue.shift()) != null) {
            setImmediate(next, null, entry)
            return
        }

        if (!entry) this.push(null)
    })
}

MemoryPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}

    this._incoming[id] = store

    store[packet.messageId] = new Packet(packet)
    store[packet.messageId].messageId = packet.messageId

    cb(null)
}

MemoryPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}
    var err = null

    this._incoming[id] = store

    if (!store[packet.messageId]) {
        err = new Error('no such packet')
    }

    cb(err, store[packet.messageId])
}

MemoryPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
    const id = client.id
    const store = this._incoming[id] || {}
    const toDelete = store[packet.messageId]
    var err = null

    if (!toDelete) {
        err = new Error('no such packet')
    } else {
        delete store[packet.messageId]
    }

    cb(err)
}

MemoryPersistence.prototype.putWill = function (client, packet, cb) {
    packet.brokerId = this.broker.id
    packet.clientId = client.id
    this._wills[client.id] = packet
    cb(null, client)
}

MemoryPersistence.prototype.getWill = function (client, cb) {
    cb(null, this._wills[client.id], client)
}

MemoryPersistence.prototype.delWill = function (client, cb) {
    const will = this._wills[client.id]
    delete this._wills[client.id]
    cb(null, will, client)
}

MemoryPersistence.prototype.streamWill = function (brokers) {
    const clients = Object.keys(this._wills)
    const wills = this._wills
    brokers = brokers || {}
    return from2.obj(function match (size, next) {
        var entry

        while ((entry = clients.shift()) != null) {
            if (!brokers[wills[entry].brokerId]) {
                setImmediate(next, null, wills[entry])
                return
            }
        }

        if (!entry) {
            this.push(null)
        }
    })
}

MemoryPersistence.prototype.getClientList = function (topic) {
    const clientSubs = this._subscriptions
    const entries = clientSubs.entries(clientSubs)
    return from2.obj(function match (size, next) {
        var entry
        while (!(entry = entries.next()).done) {
            if (entry.value[1].has(topic)) {
                setImmediate(next, null, entry.value[0])
                return
            }
        }
        next(null, null)
    })
}

MemoryPersistence.prototype.destroy = function (cb) {
    this._retained = null
    if (cb) {
        cb(null)
    }
}

module.exports = MemoryPersistence
module.exports.Packet = Packet
