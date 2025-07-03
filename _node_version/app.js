import 'dotenv/config';
import { SerialPort }           from 'serialport';
import { ReadlineParser }       from '@serialport/parser-readline';
import net                      from 'net';
// import GPS                      from'gps';
import { Buffer }               from 'buffer';
import http                     from 'http';
import https                    from 'https';


// ################################
// VARIABLES
const SERIAL_PORT               = process.env.SERIAL_PORT || process.env.UART_PORT || '/dev/tty.usbserial-83110';
const BAUD_RATE                 = parseInt(process.env.BAUD_RATE, 10) || 115200;
const TCP_PORT                  = parseInt(process.env.TCP_PORT, 10) || 10110;
const TCP_HOST                  = process.env.TCP_HOST || 'localhost';
const TCP_MAX_CLIENTS           = parseInt(process.env.TCP_MAX_CLIENTS, 10) || 5;

const TCP_ALLOW                 = process.env.TCP_ALLOW || 'RMC,VTG,GGA'
const TCP_ALLOW_LIST            = TCP_ALLOW.split(',')

const TCP_ONLY_RTK_FIXED        = process.env.TCP_ONLY_RTK_FIXED === 'true' || false;

const NTRIP_HOST                = process.env.NTRIP_HOST;
const NTRIP_PORT                = parseInt(process.env.NTRIP_PORT, 10) || 2101;
const NTRIP_MOUNTPOINT          = process.env.NTRIP_MOUNTPOINT;
const NTRIP_USERNAME            = process.env.NTRIP_USERNAME;
const NTRIP_PASSWORD            = process.env.NTRIP_PASSWORD;
const NTRIP_USE_HTTPS           = process.env.NTRIP_USE_HTTPS === 'true';
const NTRIP_USER_AGENT          = process.env.NTRIP_USER_AGENT || 'NodeNTRIPClient';




// ################################
// OBJECTS

// TCP server clients array - this will hold all connected clients
const clients                   = [];

// Store latest GNSS info
let latestGGA                   = null;


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
// NTRIP Client 
function startNtripStream() {
    const auth = Buffer.from(`${NTRIP_USERNAME}:${NTRIP_PASSWORD}`).toString('base64');
    const options = {
        host: NTRIP_HOST,
        port: NTRIP_PORT,
        path: `/${NTRIP_MOUNTPOINT}`,
        method: 'GET',
        headers: {
            'Ntrip-Version': 'Ntrip/2.0',
            'User-Agent': NTRIP_USER_AGENT,
            'Authorization': `Basic ${auth}`,
            'Connection': 'close'
        }
    };

    const protocol = NTRIP_USE_HTTPS ? https : http;
    const req = protocol.request(options, (res) => {
        if (res.statusCode !== 200) {
            console.error(`NTRIP connection failed: ${res.statusCode} ${res.statusMessage}`);
            res.resume();
            return;
        }
        console.log('NTRIP connection established, streaming RTCM corrections...');
        res.on('data', chunk => {
            // Write RTCM corrections to GNSS module via serial port
            serial_port.write(chunk);
        });
        res.on('end', () => {
            console.log('NTRIP stream ended.');
        });
    });

    req.on('error', (err) => {
        console.error('NTRIP request error:', err.message);
    });

    // Periodically send latest GGA sentence to NTRIP caster
    const ggaInterval = setInterval(() => {
        if (latestGGA && latestGGA.raw) {
            req.write(latestGGA.raw + '\r\n');
            console.error('NTRIP GGA sent:', latestGGA.raw);
        }
    },60000);

    req.on('close', () => clearInterval(ggaInterval));
    
}

// Start NTRIP stream
startNtripStream();


// ################################
// Helkper  function.
function cleanAddress(addr) {
    // Remove IPv6-mapped IPv4 prefix if present
    return addr.replace(/^::ffff:/, '');
}

// ################################
// Logging helper functions

// Helper to parse NMEA latitude/longitude
function parseNmeaCoord(coord, dir) {
    if (!coord || coord.length < 4) return null;
    const deg = parseInt(coord.slice(0, dir === 'N' || dir === 'S' ? 2 : 3), 10);
    const min = parseFloat(coord.slice(dir === 'N' || dir === 'S' ? 2 : 3));
    let val = deg + min / 60;
    if (dir === 'S' || dir === 'W') val *= -1;
    return val;
}

// Helper to parse GGA sentence
function parseGGA(sentence) {
    // $GPGGA,time,lat,N,lon,E,fix,sats,...
    const parts = sentence.split(',');
    if (parts.length < 15) return null;
    return {
        lat: parseNmeaCoord(parts[2], parts[3]),
        lon: parseNmeaCoord(parts[4], parts[5]),
        fix: parts[6], // Fix status
        time: parts[1], // UTC time
        sats: parseInt(parts[7], 10)
    };
}


// RTK Fix status helper
function getFixStatus(fix) {
    switch (fix) {
        case '0': return 'No Fix';
        case '1': return 'GPS Fix';
        case '2': return 'DGPS Fix';
        case '3': return 'PPS Fix';
        case '4': return 'RTK Fix';
        case '5': return 'RTK Float';
        case '6': return 'Dead Reckoning';
        case '7': return 'Manual Input';
        case '8': return 'Simulation';
        default:  return 'Unknown';
    }
}

// Log GNSS info every 15 seconds
setInterval(() => {
    if (latestGGA) {
        console.log(
            ` GNSS $GPGGA - Lat: ${latestGGA.lat?.toFixed(7) ?? '---'}, Lon: ${latestGGA.lon?.toFixed(7) ?? '---'}, ` +
            `Sats: ${latestGGA.sats ?? '--'}, Fix: ${getFixStatus(latestGGA.fix)}`
        );
    } else {
        console.log('[GNSS] No GGA data yet...');
    }
}, 15000);


// ################################
// TCP server - intstantiate & handle events

// instantiate TCP server
const tcp_nmea_server = net.createServer(socket => {
    clients.push(socket);

    // Handle socket errors to prevent server crash
    socket.on('error', (err) => {
        console.error(`Socket error from ${cleanAddress(socket.remoteAddress)}:${socket.remotePort} -`, err.message);
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
        socket.destroy();
    });

    // Handle socket close event
    socket.on('close', () => {
        const idx = clients.indexOf(socket);
        if (idx !== -1) clients.splice(idx, 1);
        console.log(`TCP NMEA client disconnected: ${cleanAddress(socket.remoteAddress)}:${socket.remotePort}`);
    });
});


// Set the maximum number of clients
tcp_nmea_server.maxConnections = TCP_MAX_CLIENTS;

// Event: 'connection'
tcp_nmea_server.on('connection',function(socket){
    console.log(`TCP NMEA client connected: ${cleanAddress(socket.remoteAddress)}:${socket.remotePort}`);
    // clients.push(socket);
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



// #################################################################
// ################################
// NMEA Serial stream events
serial_stream.on('data', data => {

    let NMEA_SUFFIX    = data.substring(0, 6).substring(3, 6);

    // skip NMEA messages n NOT in our allow list
    if (!TCP_ALLOW_LIST.includes(NMEA_SUFFIX)) {
        return; // Skip this sentence
    }

    if (data.startsWith('$GPGGA') || data.startsWith('$GNGGA')) {
        const gga = parseGGA(data);
        if (gga) {
            gga.raw = data; // <-- Store the raw NMEA sentence
            latestGGA = gga;
        }
    }

    // Only RTK Fixed if TCP_ONLY_RTK_FIXED===true
    if(TCP_ONLY_RTK_FIXED){
        if (data.startsWith('$GNGGA') || data.startsWith('$GPGGA')) {
            const fields = data.split(',');
            if (fields.length > 6 && fields[6] !== '4') {  // RTK fixed
                return; // Skip if not RTK fixed
            }
        }
        if (data.startsWith('$GNRMC') || data.startsWith('$GPRMC')) {
            const fields = data.split(',');
            if (fields.length > 12 && fields[12] !== 'R') {  // RTK fixed
                return; // Skip if not RTK fixed
            }
        }
    }



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


