const vc = require('../server/video-chunker');
console.log('Filename field:', vc.currentFilename);
const status = vc.getStatus();
console.log('getStatus().currentFilename:', status.currentFilename);
console.log('totalChunks:', status.totalChunks);
process.exit(0);
