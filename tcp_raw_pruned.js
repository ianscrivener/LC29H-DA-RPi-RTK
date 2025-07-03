import 'dotenv/config';
import net from 'net';

const TCP_HOST = process.env.TCP_HOST || '0.0.0.0';
const TCP_PORT = parseInt(process.env.TCP_PORT, 10) || 10112;

// Load ESSENTIAL_NMEA_TAGS from .env and convert wildcards to regex
const ESSENTIAL_NMEA_PATTERNS = (process.env.ESSENTIAL_NMEA_TAGS || '')
  .split(',')
  .map(tag => tag.trim())
  .filter(tag => tag.length > 0)
  .map(pattern =>
    new RegExp('^' + pattern.replace(/\*\*/g, '.*') + '$')
  );

const client = net.createConnection({ host: TCP_HOST, port: TCP_PORT }, () => {
  console.log(`Connected to NMEA TCP stream at ${TCP_HOST}:${TCP_PORT}`);
});

client.on('data', (data) => {
  // Split data into lines (NMEA sentences)
  const lines = data.toString().split(/\r?\n/);
  lines
    .map(line => line.trim())
    .filter(line => line.length > 0)
    .forEach(line => {
      // Extract NMEA tag (e.g., GPGGA, GPRMC, GPGSV, etc.)
      const tag = line.startsWith('$') ? line.substring(1, 6) : '';
      console.log(line);

    });
});


// client.on('data', (data) => {
//   // Split data into lines (NMEA sentences)
//   const lines = data.toString().split(/\r?\n/);
//   lines.forEach(line => {
//     if (line.trim().length > 0) {
//       // Extract NMEA tag (e.g., GPGGA, GPRMC, GPGSV, etc.)
//       const tag = line.startsWith('$') ? line.substring(1, 6) : '';
//       // Output only if tag matches any pattern in ESSENTIAL_NMEA_PATTERNS
//       const isEssential = ESSENTIAL_NMEA_PATTERNS.some(re => re.test(tag));
//       if (isEssential) {
//         console.log(line);
//       }
//     }
//   });
// });

client.on('end', () => {
  console.log('Disconnected from NMEA TCP stream');
});

client.on('error', (err) => {
  console.error('TCP connection error:', err.message);
});