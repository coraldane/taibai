const fs = require('fs');
const path = 'E:/data/monax/main/stock_tick_multi/seg_00001.dat';
const f = fs.openSync(path, 'r');
const stat = fs.fstatSync(f);
const len = stat.size;
console.log('File size:', len);

const head = Buffer.alloc(64);
fs.readSync(f, head, 0, 64, 0);
console.log('First 64 bytes:', head.toString('hex').match(/.{1,8}/g).join(' '));

const tail = Buffer.alloc(64);
fs.readSync(f, tail, 0, 64, len - 64);
console.log('Last 64 bytes:', tail.toString('hex').match(/.{1,8}/g).join(' '));

const last8 = Buffer.alloc(8);
fs.readSync(f, last8, 0, 8, len - 8);
const footerOffset = last8.readBigUInt64LE(0);
console.log('Footer offset (last 8 bytes):', footerOffset.toString(), 'hex:', footerOffset.toString(16));

fs.closeSync(f);
