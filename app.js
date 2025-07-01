import 'dotenv/config';
import { SerialPort }           from 'serialport';
import { ReadlineParser }       from '@serialport/parser-readline';
import net                      from 'net';
// import GPS                      from'gps';


// ################################
// VARIABLES
const SERIAL_PORT               = process.env.SERIAL_PORT || process.env.UART_PORT || '/dev/tty.usbserial-83110';
const BAUD_RATE                 = parseInt(process.env.BAUD_RATE, 10) || 115200;
const TCP_PORT                  = parseInt(process.env.TCP_PORT, 10) || 10110;
const TCP_HOST                  = parseInt(process.env.TCP_HOST) || 'localhost';
const TCP_MAX_CLIENTS           = parseInt(process.env.TCP_MAX_CLIENTS, 10) || 5;


// ################################
// OBJECTS

// TCP server clients array - this will hold all connected clients
const clients                   = [];



// ################################
// INSTANTIATIONS

// GPS instance
// const gps = new GPS;

// Create a serial port instance
const serial_port  = new SerialPort({path: SERIAL_PORT, baudRate: BAUD_RATE});

// Create a serial stream
const serial_stream = serial_port.pipe(new ReadlineParser({ delimiter: '\r\n' }))


// ################################
// Handle Ctrl+C (SIGINT) gracefully
process.on('SIGINT', () => {
    console.log('\nCtrl+C received - gracefully shutting down...');

    // Close all client sockets
    clients.forEach(socket => {
        if (socket.writable) socket.end('Server shutting down\n');
    });

    // Close TCP server
    tcp_nmea_server.close(() => {
        console.log('App terminated. Goodbye!');
        process.exit(0);
    });
    // In case server doesn't close in 2 seconds, force exit
    setTimeout(() => process.exit(0), 4000);
});


// ################################
// TCP server - intstantiate & handle events

// instantiate TCP server
const tcp_nmea_server = net.createServer(socket => {
    clients.push(socket);

    // Handle socket errors to prevent server crash
    socket.on('error', (err) => {
        console.error(`Socket error from ${socket.remoteAddress}:${socket.remotePort} -`, err.message);
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
        socket.destroy();
    });

    // Handle socket close event
    socket.on('close', () => {
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
        console.log(`TCP NMEA client disconnected: ${socket.remoteAddress}:${socket.remotePort}`);
    });
});


// Set the maximum number of clients
tcp_nmea_server.maxConnections = TCP_MAX_CLIENTS;

// Event: 'connection'
tcp_nmea_server.on('connection',function(socket){
    console.log('New TCP NMEA client connected:', socket.remoteAddress, socket.remotePort);
    clients.push(socket);
});

// Event: 'error'
tcp_nmea_server.on('error', (err) => {
    console.error('TCP NMEAserver error:', err.message);
});

// Event: 'drop'
tcp_nmea_server.on('drop', () => {
    const idx = clients.indexOf(socket);
    if (idx !== -1) clients.splice(idx, 1);
    console
});

// Event: 'close' - when TCP NMEA socket is closed
tcp_nmea_server.on('close', () => {
    console.log('NMEA TCP Server closing...')
});



tcp_nmea_server.listen(TCP_PORT, () => {
    console.log(`TCP NMEA server listening on port ${TCP_PORT}`);
});




// ################################
// NMEA Serial stream events
serial_stream.on('data', data => {
    // console.log(data);
    clients.forEach(socket => {
        if (socket.writable) socket.write(data + '\n');
    });
});

// error
serial_stream.on('error', error => {
    console.error('Error reading from serial port:', error);
});

// close
serial_stream.on('close', () => {
    console.log('Serial port closed');
    clients.forEach(socket => {
        if (socket.writable) socket.write('Serial port closed\n');
    });
});

//open    
serial_stream.on('open', () => {
    console.log(`Serial port ${SERIAL_PORT} opened at ${BAUD_RATE} baud`);
    clients.forEach(socket => {
        if (socket.writable) socket.write(`Serial port ${SERIAL_PORT} opened at ${BAUD_RATE} baud\n`);
    });
});

// end
serial_stream.on('end', () => {
    console.log('Serial stream ended');
    clients.forEach(socket => {
        if (socket.writable) socket.write('Serial stream ended\n');
    });
});








// gps.on('data', data => {
//   console.log(data, gps.state);
// })



