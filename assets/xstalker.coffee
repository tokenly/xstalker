###
#
# This simple server loads events into a beanstalkd queue
#
###

insightHost = process.env.INSIGHT_HOST or '127.0.0.1'
insightPort = process.env.INSIGHT_PORT or 3000

beanstalkHost = process.env.BEANSTALK_HOST or '127.0.0.1'
beanstalkPort = process.env.BEANSTALK_PORT or 11300

DEBUG = process.env.DEBUG or false

http = require('http')

socket = require('socket.io-client')("http://#{insightHost}:#{insightPort}")
figlet = require('figlet')

dataAPIVersion = 1


############################################################################################################

runXStalker = ()->

    console.log "[#{new Date().toString()}] connecting to beanstalk at #{beanstalkHost}:#{beanstalkPort}"
    beanstalkClient = require('nodestalker').Client("#{beanstalkHost}:#{beanstalkPort}")


    socket.on 'connect', ()->
        console.log "[#{new Date().toString()}] socket client connected to insight at http://#{insightHost}:#{insightPort}"

        # subscribe to the inv room, where all events are published
        socket.emit('subscribe', 'inv')

        return

    socket.on 'disconnect', ()->
        console.log "[#{new Date().toString()}] socket client disconnected"
        return

    socket.on "tx", (data) ->
        # console.log "[#{new Date().toString()}] received tx:",data

        transactionTimestamp = 0 + Date.now()

        # query insight about this tx
        options = {
            host: insightHost,
            port: insightPort,
            path: "/api/tx/#{data.txid}"
        }
        # console.log("[#{new Date().toString()}] loading #{options.path}")

        http.get options, (res)->
            # console.log("[#{new Date().toString()}] Got response (#{res.statusCode}) ")

            # get the transaction
            body = ''
            res.on 'data', (chunk)->
                body += chunk
                return
            .on 'end', ()->
                txData = JSON.parse(body)
                # console.log("[#{new Date().toString()}] body: ", body)
                # if DEBUG then console.log("[#{new Date().toString()}] IN: #{txData.valueIn}, OUT: #{txData.valueOut}")

                # insert job
                data = {
                    ver: dataAPIVersion
                    ts: transactionTimestamp
                    tx: txData
                }
                insertJobIntoBeanstalk('btctx', 'BTCTransactionJob', data, "#{txData.valueIn} => #{txData.valueOut}")

                return
            
            return
        .on 'error', (e)->
            console.log("[#{new Date().toString()}] Got error: " + e.message)
            return


        return

    socket.on "block", (data) ->
        if DEBUG then console.log "[#{new Date().toString()}] received block:",data
        blockTimestamp = 0 + Date.now()

        # query insight about this tx
        options = {
            host: insightHost,
            port: insightPort,
            path: "/api/block/#{data}"
        }
        # console.log("[#{new Date().toString()}] loading #{options.path}")

        http.get options, (res)->
            # console.log("[#{new Date().toString()}] Got response (#{res.statusCode}) ")

            # get the transaction
            body = ''
            res.on 'data', (chunk)->
                body += chunk
                return
            .on 'end', ()->
                blockData = JSON.parse(body)

                # insert job
                data = {
                    ver: dataAPIVersion
                    ts: blockTimestamp
                    block: blockData
                }
                insertJobIntoBeanstalk('btcblock', 'BTCBlockJob', data, "Block #{blockData.height} (#{blockData.hash})")

                return
            
            return
        .on 'error', (e)->
            console.log("[#{new Date().toString()}] Got error: " + e.message)
            return

        return


    # beanstalk
    insertJobIntoBeanstalk = (queueName, jobType, data, msg)->
        beanstalkClient.use(queueName).onSuccess ()->
            beanstalkClient.put JSON.stringify({
                job: "App\\Listener\\Job\\#{jobType}"
                data: data
            })
            .onSuccess ()->
                if DEBUG then console.log "[#{new Date().toString()}] loaded #{queueName}/#{jobType} (#{msg})"
                return
            .onError ()->
                console.log "[#{new Date().toString()}] error loading job #{queueName}/#{jobType}"
            return
        .onError ()->
            console.log "[#{new Date().toString()}] error connecting to beanstalk"
        return

    return

############################################################################################################

figlet.text('Tokenly XStalker', 'Slant', (err, data)->
    process.stdout.write data
    process.stdout.write "\n\n"
    runXStalker()

    return
)
