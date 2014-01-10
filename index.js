var fs = require('fs'),
    async = require('async'),
    utils = require(__dirname + '/utils'),
    path = require('path'),
    Emitter = require('events').EventEmitter,
    knox = require('knox');

exports.createClient = function(config){
  function Client(config){
    if (!config) config = {};
    if (!config.bucket) {
      config.bucket = './';
    }
    this.urlBase = "aws-faux.com"
    Client.prototype.getFile = function(uri, headers, callback){
        if (!callback && typeof(headers) == "function") {
          callback = headers;
          headers = {};
        }
        var stream = fs.createReadStream(path.join(config.bucket, uri));
        var responded = false;
        function cancelLocalListeners(){
          stream.removeListener('error', bad);
          stream.removeListener('readable', good);
          stream.removeListener('close', close);
        }
        function bad(e){
          if (!responded) {
            responded = true;
            cancelLocalListeners();
            if(e.code === 'ENOENT') {
              stream.statusCode = 404;
              stream.headers = {};
              return callback(null, stream);
            }
          }
        }
        function good(){
          if (!responded) {
            responded = true;
            stream.headers = {};
            stream.statusCode = 200;
            cancelLocalListeners();
            return callback(null, stream);
          }
        }
        function close(){
          if (!responded) {
            responded = true;
            cancelLocalListeners();

            // Deal with empty files
            var dummyStream = new Emitter;
            dummyStream.headers = {};
            dummyStream.statusCode = 200;            
            var r = callback(null, dummyStream);
            setTimeout(function(){
              dummyStream.emit("data", "");
              dummyStream.emit("end");
              dummyStream.emit("close");
            }, 0);
            return r;
          }
        }
        stream.on('error', bad);
        stream.on('readable', good);
        stream.on('close', close);
    };

    Client.prototype.putFile = function(src, filename, headers, fn){
      var emitter = new Emitter;

      if ('function' == typeof headers) {
        fn = headers;
        headers = {};
      }

      function checkToPath(cb){
        utils.checkToPath(path.join(config.bucket, filename), cb);
      }
      function checkFromPath(cb){
        fs.stat(src, cb);
      };
      async.series([checkFromPath, checkToPath], function(err){
        if (err) {
          console.log("ERROR", err);
          return fn(err);
        }
        var r = fs.createReadStream(src),
            w = fs.createWriteStream(path.join(config.bucket, filename));
        w.resume = function(){};
        r.pipe(w);
        fn(null, w);
      });
      return emitter;
    }
    Client.prototype.putBuffer = function(buffer, to, headers, callback){
      utils.checkToPath(path.join(config.bucket, to), function(){
        fs.writeFile(path.join(config.bucket, to), buffer, function(err){
          if (err) {
            return callback(err);
          }
          return callback(null, {headers:{}, statusCode:201});
        });
      });
    }
    Client.prototype.deleteFile = function(file, callback){
      fs.unlink(path.join(config.bucket, file), function(err){
        return callback(null, {headers:{}, statusCode: err ? 404 : 204});
      });
    }
  }
  Client.prototype.http = knox.prototype.http;
  Client.prototype.https = knox.prototype.https;
  return new Client(config);
};


