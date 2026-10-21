const http = require('http');
const req = http.request({ hostname: 'localhost', port: 3000, path: '/api/system/reset', method: 'POST' }, res => {
  console.log('System reset successful, status code:', res.statusCode);
});
req.end();
