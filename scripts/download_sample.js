const fs = require('fs');
const https = require('https');
const http = require('http');
const path = require('path');

const targetDir = path.join(__dirname, '..', 'server', 'data');
if (!fs.existsSync(targetDir)) {
  fs.mkdirSync(targetDir, { recursive: true });
}

const targetFile = path.join(targetDir, 'stanford_lecture_sample.mp4');

function download(url, dest, cb) {
  const file = fs.createWriteStream(dest);
  const client = url.startsWith('https') ? https : http;
  client.get(url, function(response) {
    if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
      console.log('Redirecting to:', response.headers.location);
      return download(response.headers.location, dest, cb);
    }
    if (response.statusCode !== 200) {
      return cb(new Error(`Server returned ${response.statusCode}`));
    }
    response.pipe(file);
    file.on('finish', function() {
      file.close(() => cb(null, fs.statSync(dest).size));
    });
  }).on('error', function(err) {
    fs.unlink(dest, () => cb(err));
  });
}

console.log('Downloading sample video...');
download('https://www.w3schools.com/html/mov_bbb.mp4', targetFile, (err, size) => {
  if (err) {
    console.error('Download failed:', err.message);
  } else {
    console.log(`Success! Video saved: ${targetFile} (${size} bytes)`);
  }
});
