#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import https from 'https';

const PRICE_URLS = [
  { clientId: '2893128', warehouse: '1020005000191845', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=UYX&Key=5ECFED83-D629-485E-8964-6CF8AF484BC9&Qty=True' },
  { clientId: '2792968', warehouse: '1020005000285600', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=VRU&Key=5ECFED83-D629-485E-8964-6CF8AF484BC9&Qty=True' },
  { clientId: '2792968', warehouse: '1020005000285498', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=QMX&Key=5ECFED83-D629-485E-8964-6CF8AF484BC9&Qty=True' },
  { clientId: '3078847', warehouse: '1020005000237852', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=NWY&Key=5ECFED83-D629-485E-8964-6CF8AF484BC9&Qty=True' },
  { clientId: '2893128', warehouse: '1020005000176328', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=UYX&Key=51F82C10-C106-4717-9DCA-85E4DA48C46B&Qty=True' },
  { clientId: '2792968', warehouse: '1020005000158685', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=VRU&Key=51F82C10-C106-4717-9DCA-85E4DA48C46B&Qty=True' },
  { clientId: '2792968', warehouse: '1020005000285489', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=QMX&Key=51F82C10-C106-4717-9DCA-85E4DA48C46B&Qty=True' },
  { clientId: '3078847', warehouse: '1020005000233916', url: 'https://mikado-parts.ru/api/Price/GetSellerPriceExcel?Seller=NWY&Key=51F82C10-C106-4717-9DCA-85E4DA48C46B&Qty=True' },
];

const targetDir = path.resolve('prices');
if (!fs.existsSync(targetDir)) fs.mkdirSync(targetDir, { recursive: true });

function download(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);
    https
      .get(url, (res) => {
        if (res.statusCode && res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        res.pipe(file);
        file.on('finish', () => file.close(() => resolve()));
      })
      .on('error', (err) => {
        fs.unlink(filePath, () => reject(err));
      });
  });
}

(async () => {
  for (const entry of PRICE_URLS) {
    const name = `${entry.clientId}_${entry.warehouse}.xlsx`;
    const dest = path.join(targetDir, name);
    process.stdout.write(`→ ${name} ... `);
    try {
      await download(entry.url, dest);
      console.log('ok');
    } catch (e) {
      console.log(`fail (${e.message})`);
    }
  }
})();