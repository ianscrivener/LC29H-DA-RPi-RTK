import { SerialPort } from 'serialport';
import dotenv from 'dotenv';

dotenv.config();

const portPath = process.env.UART_PORT || '/dev/tty.usbserial'; // fallback if not set
const baudRate = parseInt(process.env.BAUDRATE || '115200', 10);

const commands = [

    // '$PQTMCFGMSGRATE,W,GGA,1*0A',       // Enable GGA
    // '$PQTMCFGMSGRATE,W,GLL,0*0B',       // Disable GLL  
    // '$PQTMCFGMSGRATE,W,GSA,0*08',       // Disable GSA
    // '$PQTMCFGMSGRATE,W,GSV,0*09',       // Disable GSV
    // '$PQTMCFGMSGRATE,W,RMC,1*0E',       // Enable RMC
    // '$PQTMCFGMSGRATE,W,VTG,1*0F',       // Enable VTG
    // '$PQTMCFGMSGRATE,W,ZDA,0*0C',       // Disable ZDA (if supported)
    // '$PQTMCFGMSGRATE,W,GRS,0*0D',       // Disable GRS (if supported)
    // '$PQTMCFGMSGRATE,W,GST,0*0E',       // Disable GST (if supported)
    // '$PQTMSAVEPAR*5A'                  // Save configuration

    '$PAIR063,-1*22'   // Get all current NMEA output rates

];

async function sendCommands() {
  const port = new SerialPort({ path: portPath, baudRate });

  port.on('data', (data) => {
    console.log('Response:', data.toString());
  });

  port.on('open', () => {
    console.log(`Serial port opened: ${portPath} @ ${baudRate} baud`);
    sendNext(0);
  });

  port.on('error', (err) => {
    console.error('Serial port error:', err.message);
  });

//   let timeout = setTimeout(() => {
//     console.log('Timeout waiting for completion');
//     port.close();
//   }, 10000);  

//   timeout();

  function sendNext(idx) {
    if (idx >= commands.length) {
      console.log('All commands sent.');
      port.close();
      return;
    }
    const cmd = commands[idx] + '\r\n';
    port.write(cmd, (err) => {
      if (err) {
        console.error('Error writing command:', err.message);
        port.close();
        return;
      }
      console.log(`Sent: ${commands[idx]}`);
      setTimeout(() => sendNext(idx + 1), 300); // 300ms delay between commands
    });
  }
}

sendCommands();