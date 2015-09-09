var geohash = require('ngeohash');
var geolib = require('geolib');
var _ = require('underscore');
var amqp = require('amqp');
var config = require('config');

var expressio = require('express.io');
var app = expressio().http().io();
var io = app.io;

app.use('/static', expressio.static('../../../../testApi'));

var bunyan = require('bunyan');

var log = bunyan.createLogger({
  name: 'index.js',
  stream: process.stdout,
  level: config.app.logLevel
});

/*
var app = module.exports = express();
var Server = require('http').Server;
var server = new Server(app);
var io = require('socket.io')(server);
*/

//var cassandra = require('cassandra-driver');
//var TimeUuid = cassandra.types.TimeUuid;

var BITSIZE = 26; // +- 3 miles since we use listen to adjacent sections this is fine


/*
var conn = amqp.createConnection(config.amqpuri);
var once = false;

conn.on('ready', function(){
  if (once) return;
  once = true;

  log.info('ready');


  var queue;
  var headers;

  var options = {
    type: 'headers'
    , passive: false
    , durable: false
    , autoDelete: false
    , noDeclare: false
    , confirm: false
  };

  conn.exchange('whisper', options, function(exchange){
    log.info('exchange');

    conn.queue('q', function(q){
      log.info('queue');

      queue = q;
      //bind queue to nearby sectors
      headers = { 'x-match': 'any', 'where': 'asdf' };
      queue.bind_headers(exchange, headers);

      //monitor for new messages
      queue.subscribe(function(msg, msgHeaders, deliveryInfo, messageObject ){
        log.info('in: ' + JSON.stringify(msg));
      });

      exchange.publish(
        '',
        ['test'],
        {'contentType':'application/json', 'headers':{'where': 'asdf'} },
        function(res){
          log.info(res);
        }
      );

    });
  });
});
*/

//var cassclient = new cassandra.Client(
// { contactPoints: ['SaltStackMaster.local'], keyspace: 'whisper'});

//var middleware = require('../middleware').concatedMiddlewares;

var conn = amqp.createConnection(config.amqpuri);
var once = false;

conn.on('ready', function(){
  if (once) return;
  once = true;

  log.info('ready');

  var options = {
    type: 'headers'
    , passive: false
    , durable: false
    , autoDelete: false
    , noDeclare: false
    , confirm: false
  };

  conn.exchange('whisper', options, function(exchange)
  {
    io.on('connection', function(socket){
      log.info('connection');

      //var id = TimeUuid.now();

      //listen for my messages
      socket.on('msg', function(data){
        log.info( 'in:' + JSON.stringify(data) );

        var hash = geohash.encode_int(data.lat, data.long, BITSIZE);

        log.info(hash);

        var headers = {};
        headers[hash] = 1;

        //broadcast those messges
        exchange.publish(
          '',
          data,
          { 'contentType': 'application/json', 'headers': headers },
          function(res){
            log.info('published:' + res);
          }
        );
      });

      var queue = null;
      var headers;

      //cleanup queues
      socket.on('disconnect', function () {
        if (queue != null) queue.destroy();
      });

      //upload my location
      var lastLat;
      var lastLong;
      socket.on('ping', function(data){
        try{
          var lat = data.lat;
          var long = data.long;

          log.info('ping:' + lat + ',' + long);

          lastLat = lat;
          lastLong = long;

          var hash = geohash.encode_int(lat, long, BITSIZE);

          //this could be improved to only lookup 4 sectors since a sector is larger than our radius
          var neighbors = geohash.neighbors_int(hash, BITSIZE);
          neighbors.push(hash); //dont forget the sector were standing in

          //convert array of locations into object for header matching
          var newHeaders = _.reduce(
            neighbors,
            function(memo, geo){
              memo[geo] = 1;
              return memo;
            },
            { 'x-match': 'any' });

          if (queue != null)
          {
            log.info('update queue info');

            //update my listening channel to monitor nearby sectors
            queue.unbind_headers(exchange, headers);

            headers = newHeaders;

            queue.bind_headers(exchange, headers);
          }
          else
          {
            //establish queue
            log.info('queue create');
            conn.queue('', function(q){
              log.info('queue:' + q);

              queue = q;
              //bind queue to nearby sectors
              headers = newHeaders;
              queue.bind_headers(exchange, headers);

              //monitor for new messages
              queue.subscribe(function(msg){ //, msgHeaders, deliveryInfo, messageObject ){
                log.info( 'out: ' + JSON.stringify(msg) );

                var dist = geolib.getDistance(
                  { latitude: lastLat, longitude: lastLong },
                  { latitude: msg.lat, longitude: msg.long });

                if(dist < 1609.344 * 5) //5 miles
                {
                  log.info('emit');
                  socket.emit('msg', msg);
                }
              });
            });
          }
        }
        catch(err)
        {
          console.log(err);
        }
      });
    });
  });
});


app.listen(3000, function(){
  log.info('Listening on port ' + 3000);
});


/*

app.get('/api/v1/whisper', middleware, function (req, res, next) {
  var id = TimeUuid.now();
  var lat = parseFloat(req.query.lat);
  var long = parseFloat(req.query.long);
  var hash = geohash.encode_int(req.query.lat, req.query.long, BITSIZE);

  cassclient.execute(
    'INSERT INTO posts (hash, posttime, lat, long, subject, text) VALUES (?,?,?,?,?,?)',
    [
    hash, id,
    lat, long,
    req.query.subject, req.query.text
    ], { 'prepare': true }, function(err){
      if (err) next(err);
      else res.json('ok');
    });
});

app.get('/api/v1/ping', middleware, function (req, res, next) {
  var id = TimeUuid.now();
  var lat = parseFloat(req.query.lat);
  var long = parseFloat(req.query.long);

  cassclient.execute(
    'INSERT INTO ping (uid, pingtime, lat, long) VALUES (?,?,?,?)',
    [
    req.query.uid, id,
    lat, long
    ], { 'prepare': true }, function(err){
      if (err) next(err);
      else res.json('ok');
    });
});

app.get('/api/v1/listen', middleware, function (req, res, next) {
  var hash = geohash.encode_int(req.query.lat, req.query.long, BITSIZE);

  //this could be improved to only lookup 4 sectors since a sector is larger than our radius
  var neighbors = geohash.neighbors_int(hash, BITSIZE);

  neighbors.push(hash);

  cassclient.execute(
    'SELECT hash, posttime, lat, long, subject, text ' +
    'FROM posts WHERE hash in (?,?,?,?,?,?,?,?,?) ORDER BY posttime LIMIT 10',
    neighbors, { 'prepare': true }, function(err, result){
      if (err) return next(err);

      res.header('Content-Type', 'application/json');
      res.json(_.map(_.filter(result.rows,function(row){
        var dist = geolib.getDistance(
          {latitude:row.lat, longitude:row.long},
          {latitude:req.query.lat, longitude:req.query.long});

        return dist < 1609.344 * 5; //5 miles
      }),function(row){
        log.info(row.posttime);
        var ts = row.posttime.getDate();

        return {posttime:ts, subject:row.subject, subject:row.text};
      }));;
    });
});
*/
