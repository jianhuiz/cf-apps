//module.exports = require('./lib/tty.js');

var tty = require('./lib/tty.js');

var app = tty.createServer({
  port:  process.env.VCAP_APP_PORT || process.env.PORT || 8080,
  shell: 'bash',
  term: {
    geometry: [120, 32],
    screenKeys: true
  }
});

app.listen();

