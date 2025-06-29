// const SerialPort = require('serialport');
const { SerialPort } = require('serialport')
const { ReadlineParser } = require('@serialport/parser-readline')
const GPS = require('gps');


// #################
// Create a new serial port instance
// Make sure to replace '/dev/tty.usbserial-83110' with your actual serial port path

const port = new SerialPort(
    {
        path: '/dev/tty.usbserial-83110',
        baudRate: 115200
    }
);

const parser = port.pipe(new ReadlineParser({ delimiter: '\r\n' }))
parser.on('data', console.log)


// const gps = new GPS;

// gps.on('data', data => {
//   console.log(data, gps.state);
// })

// port.on('data', data => {
//   gps.updatePartial(data);
// })