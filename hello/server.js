var http = require('http');
var port = process.env.VCAP_APP_PORT || process.env.PORT || 3000

var instanceAddr = "" + process.env.CF_INSTANCE_INDEX + "/" + process.env.CF_INSTANCE_ADDR;
var requestNum = 0;

http.createServer(function (req, res) {
  res.writeHead(200, {'Content-Type': 'text/plain'});
  body = 'Hello World to instance: ' + instanceAddr + ", request num: " + requestNum + "\n";
  requestNum++;  
 
  var v = 0.0  
  for (var i = 0; i < 100000000; i++) {
    v = v + Math.sqrt(i);
  }
  res.write("" + v + ", ")
  res.end(body);

}).listen(port);
console.log('Server running at http://127.0.0.1:' + port);

