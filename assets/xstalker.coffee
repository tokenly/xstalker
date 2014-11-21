###
#
# This simple server loads events into a beanstalkd queue
#
###

insightHost = process.env.INSIGHT_HOST or '127.0.0.1'
insightPort = process.env.INSIGHT_PORT or 3000

beanstalkHost = process.env.BEANSTALK_HOST or '127.0.0.1'
beanstalkPort = process.env.BEANSTALK_PORT or 11300

http = require('http')

socket = require('socket.io-client')("http://#{insightHost}:#{insightPort}")

dataAPIVersion = 1
beanstalkClient = require('nodestalker').Client("#{beanstalkHost}:#{beanstalkPort}")


socket.on 'connect', ()->
    console.log "socket client connected"

    # subscribe to the inv room, where all events are published
    socket.emit('subscribe', 'inv')

    return

socket.on 'disconnect', ()->
    console.log "socket client disconnected"
    return

socket.on "tx", (data) ->
    # console.log "received tx:",data

    transactionTimestamp = 0 + Date.now()

    # query insight about this tx
    options = {
        host: insightHost,
        port: insightPort,
        path: "/api/tx/#{data.txid}"
    }
    # console.log("loading #{options.path}")

    http.get options, (res)->
        # console.log("Got response (#{res.statusCode}) ")

        # get the transaction
        body = ''
        res.on 'data', (chunk)->
            body += chunk
            return
        .on 'end', ()->
            txData = JSON.parse(body)
            # console.log("body: ", body)
            console.log("IN: #{txData.valueIn}, OUT: #{txData.valueOut}")

            # insert job
            data = {
                ver: dataAPIVersion
                ts: transactionTimestamp
                tx: txData
            }
            insertJobIntoBeanstalk('BTCTransactionJob', data)

            return
        
        return
    .on 'error', (e)->
        console.log("Got error: " + e.message)
        return


    return

socket.on "block", (data) ->
    console.log "received block:",data
    return


# beanstalk
insertJobIntoBeanstalk = (jobType, data)->
    beanstalkClient.use('btctx').onSuccess ()->
        beanstalkClient.put JSON.stringify({
            job: "Tokenly\\XChainListener\\Job\\#{jobType}"
            data: data
        })
        .onSuccess ()->
            console.log "job loaded #{jobType} (#{data.tx.txid})"
            return
        .onError ()->
            console.log "error loading job #{jobType} (#{data.tx.txid})"
        return
    .onError ()->
        console.log "error connecting to beanstalk"
    return

return
