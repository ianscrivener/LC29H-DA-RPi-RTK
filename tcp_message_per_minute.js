import 'dotenv/config';
import net from 'net';

// Read TCP settings from environment variables
const TCP_HOST = process.env.TCP_HOST || 'localhost';
const TCP_PORT = parseInt(process.env.TCP_PORT, 10) || 10110;

// Object to hold counts of each NMEA sentence type
const nmeaCounts = {};

// Connect to TCP server
const client = net.createConnection({ host: TCP_HOST, port: TCP_PORT }, () => {
    console.log(`Connected to TCP server at ${TCP_HOST}:${TCP_PORT}`);
});

let leftover = '';

// Process incoming TCP data
client.on('data', (data) => {
    // Handle chunked data and split by line
    const lines = (leftover + data.toString()).split(/\r?\n/);
    leftover = lines.pop(); // Save incomplete line for next chunk

    for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.startsWith('$')) {
            const match = trimmed.match(/^\$([A-Z]{5})/);
            if (match) {
                const type = match[1];
                nmeaCounts[type] = (nmeaCounts[type] || 0) + 1;
            }
        }
    }
});

client.on('end', () => {
    console.log('Disconnected from TCP server.');
});

client.on('error', (err) => {
    console.error('TCP client error:', err.message);
});

// Log counts every minute
setInterval(() => {
    console.log('NMEA sentence counts (last minute):');
    if (Object.keys(nmeaCounts).length === 0) {
        console.log('  No NMEA sentences received yet.');
    } else {
        for (const [type, count] of Object.entries(nmeaCounts)) {
            console.log(`  ${type}: ${count}`);
        }
    }
    // Reset counts for the next minute
    for (const key in nmeaCounts) nmeaCounts[key] = 0;
}, 60000);

console.log(`NMEA sentence counter started. Connecting to TCP ${TCP_HOST}:${TCP_PORT}...`);