import 'dotenv/config';
import { SerialPort }           from 'serialport';
import { ReadlineParser }       from '@serialport/parser-readline';
import net                      from 'net';
// import GPS                      from'gps';


// Get serial port and baud rate from .env
const SERIAL_PORT               = process.env.SERIAL_PORT || '/dev/tty.usbserial-83110';
const BAUD_RATE                 = parseInt(process.env.BAUD_RATE, 10) || 115200;


// ################################
// INSTANTIATIONS
// GPS instance
// const gps = new GPS;

// Create a serial port instance
const serial_port  = new SerialPort({path: SERIAL_PORT, baudRate: BAUD_RATE});

// Create a serial stream
const serial_stream = serial_port.pipe(new ReadlineParser({ delimiter: '\r\n' }))



// ################################
// VARIABLES
// TCP server to broadcast serial data
const clients = [];

// TCP server port
const TCP_PORT = parseInt(process.env.TCP_PORT, 10) || 10110;


// ################################
// HANDLERS
// 
const tcp_name_server = net.createServer(socket => {
    clients.push(socket);
    socket.on('end', () => {
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
    });
    socket.on('error', () => {
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
    });
});

tcp_name_server.listen(TCP_PORT, () => {
    console.log(`TCP NMEA server listening on port ${TCP_PORT}`);
    console.log(clients);
});

serial_stream.on('data', data => {
    // console.log(data);
    clients.forEach(socket => {
        if (socket.writable) socket.write(data + '\n');
    });
});

// gps.on('data', data => {
//   console.log(data, gps.state);
// })



