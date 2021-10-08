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

class Outbox {
  constructor (size,property,packetarray = new Array ) {
    this.property = property
    this.size = size
    this._outbox = packetarray
  }

  clone() {
    return new Outbox(this.size, this.property, this._outbox)
  }

  pubenqueue (packet) {
    if (this._outbox.length + 1 > this.size) {
      // 可能之後要通知
      this._outbox.shift()
    }
    this._outbox.push(packet)
  }

  // 從後面塞一個
  enqueue (broker, id, packet) {
    if (this._outbox.length + 1 > this.size) {
      // 可能之後要通知
      let throwPacket = this._outbox.shift()
      const message = { topic: throwPacket.properties.responseTopic, payload: 'client ' + id + ' throw packet because overflow', properties:throwPacket.properties, qos:0 }
      broker.mq.emit(message, ()=>{})
      this._outbox.shift()
    }
    this._outbox.push(packet)
  }

  // 從後面拿掉一個
  dequeue () {
    return this._outbox.shift()
  }

  // 從前面拿出一個
  pop () {
    return this._outbox.pop()
  }

  // 從前面拿出全部
  popall () {
    const temp = this._outbox.map(x => x)
    // this._outbox.length = 0
    return temp
  }

  take (packet) {
    let temp
    for (let i = 0; i < this._outbox.length; i++) {
      temp = this._outbox[i]
      if (temp.messageId === packet.messageId) {
        this._outbox.splice(i, 1)
        return temp
      }
    }
    return false
  }

  update (packet) {
    let temp
    for (let i = 0; i < this._outbox.length; i++) {
      temp = this._outbox[i]
      if (temp.brokerId === packet.brokerId) {
        //確認都是同一個publish
        if (temp.brokerCounter === packet.brokerCounter) {
          temp.messageId = packet.messageId
          return true
          //可能還需要更多的確認 不是temp.brokerCounter === packet.brokerCounter就判斷為不一樣就隨便改
          //正常程序下的收到pubrel並更改cmd
        }else if(packet.cmd === "pubrel" && temp.messageId === packet.messageId){
          this._outbox[i].cmd = packet.cmd
          return true
        }
      //第一次訂閱的broker直接發送publish後收到的pubrec  需要更改裡面的變成pubrel 還有messageID
      }else if (temp.brokerId === undefined && packet.cmd === "pubrel"){

        this._outbox[i].cmd = packet.cmd
        temp.messageId = packet.messageId
        // this._outbox[i] = packet
        return true
      }
    }
    return false
  }

  clearMessageId(packet){
    let temp
    for (let i = 0; i < this._outbox.length; i++) {
      temp = this._outbox[i]
      if (temp.messageId === packet.messageId) {
        this._outbox.splice(i, 1)
        return temp
      }
    }
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

  this._subOutboxsbyC = {}
  this._pubOutboxsByT = {}
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
    for (let i = 0; i < pattern.length; i += 1) {
      matcher.add(pattern[i])
    }
  } else {
    matcher.add(pattern)
  }

  return from2.obj(function match (size, next) {
    let entry

    while ((entry = current.shift()) != null) {
      if (matcher.test(entry.topic)) {
        setImmediate(next, null, entry)
        return
      }
    }

    if (!entry) this.push(null)
  })
}

function copyPubToSub(client, patterns, subOutboxs ,pubOutboxsByT){
  for (let i = 0; i < patterns.length; i++) {
    if(Object.keys(pubOutboxsByT).length !== 0){
      for (const box in subOutboxs) {
        if (box === patterns[i]) {
          subOutboxs[box] = pubOutboxsByT[patterns[i]].clone();
          console.log("copyPubToSub成功");
          return;
        } else {
          console.log("copyPubToSub沒有找到對應topic");
        }
      }
    }


  }
}

MemoryPersistence.prototype.createSubOutboxStreamCombi = function (patterns, client) {
  let subOutboxs = this._subOutboxsbyC[client.id] || [];
  this._subOutboxsbyC[client.id] = subOutboxs;
  //要修suboutbox的數值不能變pub一樣
  let pubOutboxsByT = this._pubOutboxsByT || [];
  copyPubToSub(client, patterns, subOutboxs , pubOutboxsByT);

//暫時先處理訂閱1份的
  if(subOutboxs[patterns[0]]){
    return matchingStream([].concat(subOutboxs[patterns[0]]._outbox), patterns)
  }else{
    return matchingStream([], patterns)
  }
}

/// ////////////////////
MemoryPersistence.prototype.createSubOutbox = function (client, packet, cb) {
  // TODO: 或是原本就存在sub_outbox 就不需要在重新建立在suback回應
  const id = client.id
  let subOutboxs = this._subOutboxsbyC[id] || []

  this._subOutboxsbyC[id] = subOutboxs
  subOutboxs[packet.subscriptions[0].topic] = new Outbox(packet.properties.userProperties.retainCount,packet.subscriptions[0])
  cb(null, client)
}

MemoryPersistence.prototype.enqueueSubOutbox = function (broker, subs, packet, cb) {
  for (let i = 0; i < subs.length; i++) {
    _subOutboxEnqueue.call(this,broker, subs[i], packet)
  }
  process.nextTick(cb)
}

function _subOutboxEnqueue (broker, sub, packet) {
  const id = sub.clientId
  let subOutboxs = this._subOutboxsbyC[id] || []

  this._subOutboxsbyC[id] = subOutboxs
  const p = new Packet(packet)
  subOutboxs[sub.topic].enqueue(broker, id, p)
}

MemoryPersistence.prototype.updateSubOutbox = function (client, packet, cb) {
  // TODO: 修改新封包Suboutbox
  const id = client.id
  let subOutboxs = this._subOutboxsbyC[id] || []
  this._subOutboxsbyC[id] = subOutboxs
  let result

  for (const box in subOutboxs) {
    result = subOutboxs[box].update(packet)
    if (result) {
      console.log("updateSubOutbox成功（更新messageId 或 換成pubrel）")
      return cb(null, client, packet)
    } else {
      console.log("subOutbox可能已經溢出不需要據繼續賦予messageId")
    }
  }
}

MemoryPersistence.prototype.dequeueSubOutbox = function (client, packet, cb) {
  // TODO: 發送成功要去除的資料Suboutbox
  const id = client.id
  let subOutboxs = this._subOutboxsbyC[id] || []
  let result
  for (const box in subOutboxs) {
    result = subOutboxs[box].take(packet)
    if (result) {
      console.log(result)
      cb(null, result)
    } else {
        cb(null, result)
    }
  }
}

// 創建puboutbox並新增第一項 如果已經有就是新增一項
MemoryPersistence.prototype.createPubOutbox = function (client, packet, cb) {
  // TODO: pub_outbox 創建 以及 enqueue 新封包
  let pubOutboxs = this._pubOutboxsByT || []
  this._pubOutboxsByT = pubOutboxs

  if (pubOutboxs[packet.topic] === undefined) {
    pubOutboxs[packet.topic] = new Outbox(packet.properties.userProperties.retainLimit)
  } else if (packet.properties.userProperties.retainLimit > pubOutboxs[packet.topic].size) {
    pubOutboxs[packet.topic].size = packet.properties.userProperties.retainLimit
  }
  pubOutboxs[packet.topic].pubenqueue(packet)

  cb(null, client)
}

MemoryPersistence.prototype.outgoingSubOutboxStream = function (client) {
  const subOutboxs = this._subOutboxsbyC[client.id] || []
  this._subOutboxsbyC[client.id] = subOutboxs
  let queue = [];
  for (const box in subOutboxs){
    queue = queue.concat(subOutboxs[box].popall())
    console.log(queue);
  }

  return from2.obj(function match (size, next) {
    let entry

    if ((entry = queue.shift()) != null) {
      setImmediate(next, null, entry)
      return
    }

    if (!entry) this.push(null)
  })
}

MemoryPersistence.prototype.outgoingSubOutboxClearMessageId = function (client, packet, cb) {

  const subOutboxs = this._subOutboxsbyC[client.id] || []
  this._subOutboxsbyC[client.id] = subOutboxs
  let result

  for (const box in subOutboxs) {
    result = subOutboxs[box].clearMessageId(packet)
    if (result) {
      console.log("updateSubOutbox成功clearMessageId")
      return cb(null, result)
    } else {
      console.log("subOutbox可能已經溢出不需要據繼續賦予messageId")
    }
  }
  cb()
}

/// ////////////////////

MemoryPersistence.prototype.createRetainedStream = function (pattern) {
  return matchingStream([].concat(this._retained), pattern)
}

MemoryPersistence.prototype.createRetainedStreamCombi = function (patterns) {
  return matchingStream([].concat(this._retained), patterns)
}

MemoryPersistence.prototype.addSubscriptions = function (client, subs, cb) {
  let stored = this._subscriptions.get(client.id)
  const trie = this._trie

  // 檢查是否有儲存過
  if (!stored) {
    stored = new Map()
    this._subscriptions.set(client.id, stored)
    this._clientsCount++
  }

  for (let i = 0; i < subs.length; i += 1) {
    const sub = subs[i]
    const qos = stored.get(sub.topic)
    const hasQoSGreaterThanZero = (qos !== undefined) && (qos > 0)
    // 再研究
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
    for (let i = 0; i < subs.length; i += 1) {
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
  let subs = null
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
  // {clientId: "mqttjs_sub", topic: "test1", qos: 2}
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
  for (let i = 0; i < subs.length; i++) {
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
  let temp

  this._outgoing[clientId] = outgoing

  for (let i = 0; i < outgoing.length; i++) {
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
  let temp

  this._outgoing[clientId] = outgoing

  for (let i = 0; i < outgoing.length; i++) {
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
    let entry

    if ((entry = queue.shift()) != null) {
      setImmediate(next, null, entry)
      return
    }

    if (!entry) this.push(null)
  })
}

MemoryPersistence.prototype.incomingStorePacket = function (client, packet, cb) {
  const id = client.id
  const store = this._incoming[id] || []

  this._incoming[id] = store

  store[packet.messageId] = new Packet(packet)
  store[packet.messageId].messageId = packet.messageId

  cb(null)
}

MemoryPersistence.prototype.incomingGetPacket = function (client, packet, cb) {
  const id = client.id
  const store = this._incoming[id] || []
  let err = null

  this._incoming[id] = store

  if (!store[packet.messageId]) {
    err = new Error('no such packet')
  }

  cb(err, store[packet.messageId])
}

MemoryPersistence.prototype.incomingDelPacket = function (client, packet, cb) {
  const id = client.id
  const store = this._incoming[id] || []
  const toDelete = store[packet.messageId]
  let err = null

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
  brokers = brokers || []
  return from2.obj(function match (size, next) {
    let entry

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
    let entry
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
