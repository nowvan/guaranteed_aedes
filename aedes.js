'use strict'

const EventEmitter = require('events')
const util = require('util')
const parallel = require('fastparallel')
const series = require('fastseries')
const { v4: uuidv4 } = require('uuid')
const bulk = require('bulk-write-stream')
const reusify = require('reusify')
const { pipeline } = require('readable-stream')
const Packet = require('./lib/aedes-packet')
const memory = require('./lib/outbox')
const mqemitter = require('mqemitter')
const Client = require('./lib/client')
const { $SYS_PREFIX } = require('./lib/utils')

module.exports = Aedes.Server = Aedes

const defaultOptions = {
  concurrency: 100,
  heartbeatInterval: 60000, // 1 minute
  connectTimeout: 300000, // 30 secs
  decodeProtocol: null,
  preConnect: defaultPreConnect,
  authenticate: defaultAuthenticate,
  authorizePublish: defaultAuthorizePublish,
  authorizeSubscribe: defaultAuthorizeSubscribe,
  authorizeForward: defaultAuthorizeForward,
  published: defaultPublished,
  trustProxy: false,
  trustedProxies: [],
  queueLimit: 42,
  maxClientsIdLength: 60
}

function Aedes (opts) {
  const that = this

  if (!(this instanceof Aedes)) {
    return new Aedes(opts)
  }
  // 合併
  opts = Object.assign({}, defaultOptions, opts)

  this.id = opts.id || uuidv4()
  // +1 when construct a new aedes-packet
  // internal track for last brokerCounter
  this.counter = 0
  this.queueLimit = opts.queueLimit
  this.connectTimeout = opts.connectTimeout
  this.maxClientsIdLength = opts.maxClientsIdLength
  this.mq = opts.mq || mqemitter({
    concurrency: opts.concurrency,
    matchEmptyLevels: true // [MQTT-4.7.1-3]
  })
  this.handle = function handle (conn, req) {
    conn.setMaxListeners(opts.concurrency * 2)
    // create a new Client instance for a new connection
    // return, just to please standard
    return new Client(that, conn, req)
  }
  this.persistence = opts.persistence || memory()
  this.persistence.broker = this
  this._parallel = parallel()
  this._series = series()
  // reusify再利用
  this._enqueuers = reusify(DoEnqueues)

  this.preConnect = opts.preConnect
  this.authenticate = opts.authenticate
  this.authorizePublish = opts.authorizePublish
  this.authorizeSubscribe = opts.authorizeSubscribe
  this.authorizeForward = opts.authorizeForward
  this.published = opts.published

  this.decodeProtocol = opts.decodeProtocol
  this.trustProxy = opts.trustProxy
  this.trustedProxies = opts.trustedProxies

  this.clients = {}
  this.brokers = {}

  const heartbeatTopic = $SYS_PREFIX + that.id + '/heartbeat'
  // TODO: 測試用 broker發送到特定topic反映他還活著 暫時不用檢查broker是否活著
  this._heartbeatInterval = null
  // setInterval(heartbeat, opts.heartbeatInterval)

  const bufId = Buffer.from(that.id, 'utf8')

  function heartbeat () {
    that.publish({
      topic: heartbeatTopic,
      payload: bufId
    }, noop)
  }

  function deleteOldBrokers (broker) {
    if (that.brokers[broker] + (3 * opts.heartbeatInterval) < Date.now()) {
      delete that.brokers[broker]
    }
  }

  this._clearWillInterval = setInterval(function () {
    Object.keys(that.brokers).forEach(deleteOldBrokers)

    pipeline(
      that.persistence.streamWill(that.brokers),
      bulk.obj(receiveWills),
      function done (err) {
        if (err) {
          that.emit('error', err)
        }
      }
    )
  }, opts.heartbeatInterval * 4)

  function receiveWills (chunks, done) {
    that._parallel(that, checkAndPublish, chunks, done)
  }

  function checkAndPublish (will, done) {
    const needsPublishing =
      !that.brokers[will.brokerId] ||
      that.brokers[will.brokerId] + (3 * opts.heartbeatInterval) <
      Date.now()

    if (needsPublishing) {
      // randomize this, so that multiple brokers
      // do not publish the same wills at the same time
      that.publish(will, function publishWill (err) {
        if (err) {
          return done(err)
        }

        that.persistence.delWill({
          id: will.clientId,
          brokerId: will.brokerId
        }, done)
      })
    } else {
      done()
    }
  }

  this.mq.on($SYS_PREFIX + '+/heartbeat', function storeBroker (packet, done) {
    that.brokers[packet.payload.toString()] = Date.now()
    done()
  })

  this.mq.on($SYS_PREFIX + '+/new/clients', function closeSameClients (packet, done) {
    const serverId = packet.topic.split('/')[1]
    const clientId = packet.payload.toString()

    if (that.clients[clientId] && serverId !== that.id) {
      that.clients[clientId].close(done)
    } else {
      done()
    }
  })

  // metadata
  this.connectedClients = 0
  this.closed = false
}

util.inherits(Aedes, EventEmitter)

function storeRetained (packet, done) {
  if (packet.retain) {
    this.broker.persistence.storeRetained(packet, done)
  } else {
    done()
  }
}

function emitPacket (packet, done) {
  this.broker.mq.emit(packet, done)
}

function enqueueOffline (packet, done) {
  // TODO: 原先是將訊息先放進persistence的地方 在這放入更過的sub_outbox
  //  不只是還在線上的沒斷線得也要檢查

  const enqueuer = this.broker._enqueuers.get()

  enqueuer.complete = done
  enqueuer.packet = packet
  enqueuer.topic = packet.topic
  enqueuer.broker = this.broker
  //
  this.broker.persistence.subscriptionsByTopic(
    packet.topic,
    // call doneEnqueue ()
    enqueuer.done
  )
}

// reuse的方法 enqueueOffline用到這個
function DoEnqueues () {
  this.next = null
  this.complete = null
  this.packet = null
  this.topic = null
  this.broker = null

  const that = this

  this.done = function doneEnqueue (err, subs) {
    const broker = that.broker

    if (err) {
      // is this really recoverable?
      // let's just error the whole aedes
      // https://nodejs.org/api/events.html#events_error_events
      broker.emit('error', err)
      return
    }

    if (that.topic.indexOf($SYS_PREFIX) === 0) {
      subs = subs.filter(removeSharp)
    }

    const packet = that.packet
    const complete = that.complete

    that.packet = null
    that.complete = null
    that.topic = null

    // broker.persistence.outgoingEnqueueCombi(subs, packet, complete)
    // TODO: subs得地方要改改 改成有sub_outbox也有一份 是unsubscribe處理不掉的嘿嘿
    // TODO: enqueueSubOutbox 加入到這裡但是順序跟callback要處理 先不處理callback 直接用小段的方法處理 先傳空的callback
    broker.persistence.enqueueSubOutbox(broker, subs, packet, complete)

    broker._enqueuers.release(that)
  }
}

// + is 43
// # is 35
function removeSharp (sub) {
  const code = sub.topic.charCodeAt(0)
  return code !== 43 && code !== 35
}

function callPublished (_, done) {
  this.broker.published(this.packet, this.client, done)
  this.broker.emit('publish', this.packet, this.client)
}

const publishFuncsSimple = [
  // if (packet.retain) {
  //     this.broker.persistence.storeRetained(packet, done)
  storeRetained,
  // this.broker.mq.emit(packet, done)
  emitPacket,
  // this.broker.published(this.packet, this.client, done)
  //   this.broker.emit('publish', this.packet, this.client)
  callPublished
]
const publishFuncsQoS = [
  // if (packet.retain) {
  //     this.broker.persistence.storeRetained(packet, done)
  storeRetained,
  // const enqueuer = this.broker._enqueuers.get()
  //
  // this.broker.persistence.subscriptionsByTopic(
  //   packet.topic,
  //   enqueuer.done
  // )
  enqueueOffline,
  // this.broker.mq.emit(packet, done)
  emitPacket,
  // this.broker.published(this.packet, this.client, done)
  //   this.broker.emit('publish', this.packet, this.client)
  callPublished
]
Aedes.prototype.publish = function (packet, client, done) {
  if (typeof client === 'function') {
    done = client
    client = null
  }
  const p = new Packet(packet, this)
  const publishFuncs = p.qos > 0 ? publishFuncsQoS : publishFuncsSimple

  this._series(new PublishState(this, client, packet), publishFuncs, p, done)
}

Aedes.prototype.subscribe = function (topic, func, done) {
  this.mq.on(topic, func, done)
}

Aedes.prototype.unsubscribe = function (topic, func, done) {
  this.mq.removeListener(topic, func, done)
}

Aedes.prototype.registerClient = function (client) {
  const that = this
  if (this.clients[client.id]) {
    // [MQTT-3.1.4-2]
    this.clients[client.id].close(function closeClient () {
      that._finishRegisterClient(client)
    })
  } else {
    this._finishRegisterClient(client)
  }
}

Aedes.prototype._finishRegisterClient = function (client) {
  this.connectedClients++
  this.clients[client.id] = client
  this.emit('client', client)
  // TODO: 測試用 先不用完成訂閱後發送系統的通知

  // this.publish({
  //   topic: $SYS_PREFIX + this.id + '/new/clients',
  //   payload: Buffer.from(client.id, 'utf8')
  // }, noop)
}

Aedes.prototype.unregisterClient = function (client) {
  this.connectedClients--
  delete this.clients[client.id]
  this.emit('clientDisconnect', client)
  this.publish({
    topic: $SYS_PREFIX + this.id + '/disconnect/clients',
    payload: Buffer.from(client.id, 'utf8')
  }, noop)
}

function closeClient (client, cb) {
  this.clients[client].close(cb)
}

Aedes.prototype.close = function (cb = noop) {
  const that = this
  if (this.closed) {
    return cb()
  }
  this.closed = true
  clearInterval(this._heartbeatInterval)
  clearInterval(this._clearWillInterval)
  this._parallel(this, closeClient, Object.keys(this.clients), doneClose)
  function doneClose () {
    that.emit('closed')
    that.mq.close(cb)
  }
}

Aedes.prototype.version = require('./package.json').version

function defaultPreConnect (client, packet, callback) {
  callback(null, true)
}

function defaultAuthenticate (client, username, password, callback) {
  callback(null, true)
}

function defaultAuthorizePublish (client, packet, callback) {
  if (packet.topic.startsWith($SYS_PREFIX)) {
    return callback(new Error($SYS_PREFIX + ' topic is reserved'))
  }
  callback(null)
}

function defaultAuthorizeSubscribe (client, sub, callback) {
  callback(null, sub)
}

function defaultAuthorizeForward (client, packet) {
  return packet
}

function defaultPublished (packet, client, callback) {
  callback(null)
}

function PublishState (broker, client, packet) {
  this.broker = broker
  this.client = client
  this.packet = packet
}

function noop () {}

// Aedes.on('receipt',function (receiptPacket,client) {
//   console.log('receipt ', receiptPacket, client)
//   this.broker.mq.emit(receiptPacket, done)
// })
