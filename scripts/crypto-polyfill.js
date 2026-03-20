/* crypto.subtle.digest polyfill for HTTP (non-secure) contexts.
   SheetJS (xlsx) may call crypto.subtle.digest even when subtle is missing.
   This polyfill provides SHA-1 and SHA-256 only, sufficient for xlsx parsing. */
(function(){
  if (typeof window === 'undefined') return;
  if (!window.crypto) window.crypto = {};
  if (!window.crypto.subtle) window.crypto.subtle = {};

  if (typeof window.crypto.subtle.digest === 'function') return;

  function toUint8(data){
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    return new Uint8Array(data);
  }

  function rotl(x,n){ return (x<<n) | (x>>> (32-n)); }

  function sha1(bytes){
    var ml = bytes.length * 8;
    var withOne = new Uint8Array(((bytes.length + 9 + 63) >> 6) << 6);
    withOne.set(bytes);
    withOne[bytes.length] = 0x80;
    var dv = new DataView(withOne.buffer);
    dv.setUint32(withOne.length - 4, ml >>> 0, false);
    dv.setUint32(withOne.length - 8, Math.floor(ml / 0x100000000) >>> 0, false);

    var h0=0x67452301, h1=0xEFCDAB89, h2=0x98BADCFE, h3=0x10325476, h4=0xC3D2E1F0;
    var w = new Uint32Array(80);

    for (var i=0; i<withOne.length; i+=64){
      for (var t=0; t<16; t++) w[t] = dv.getUint32(i + t*4, false);
      for (t=16; t<80; t++) w[t] = rotl(w[t-3]^w[t-8]^w[t-14]^w[t-16],1);

      var a=h0,b=h1,c=h2,d=h3,e=h4,f,k,temp;
      for (t=0; t<80; t++){
        if (t<20){ f=(b&c)|((~b)&d); k=0x5A827999; }
        else if (t<40){ f=b^c^d; k=0x6ED9EBA1; }
        else if (t<60){ f=(b&c)|(b&d)|(c&d); k=0x8F1BBCDC; }
        else { f=b^c^d; k=0xCA62C1D6; }
        temp = (rotl(a,5) + f + e + k + (w[t]>>>0))>>>0;
        e=d; d=c; c=rotl(b,30)>>>0; b=a; a=temp;
      }
      h0=(h0+a)>>>0; h1=(h1+b)>>>0; h2=(h2+c)>>>0; h3=(h3+d)>>>0; h4=(h4+e)>>>0;
    }

    var out = new Uint8Array(20);
    var odv = new DataView(out.buffer);
    odv.setUint32(0,h0,false); odv.setUint32(4,h1,false); odv.setUint32(8,h2,false);
    odv.setUint32(12,h3,false); odv.setUint32(16,h4,false);
    return out.buffer;
  }

  function sha256(bytes){
    // Minimal SHA-256 implementation (public domain style)
    var K = new Uint32Array([
      0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
      0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
      0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
      0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
      0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
      0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
      0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
      0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
    ]);

    function rotr(x,n){ return (x>>>n) | (x<<(32-n)); }
    function ch(x,y,z){ return (x&y) ^ (~x&z); }
    function maj(x,y,z){ return (x&y) ^ (x&z) ^ (y&z); }
    function SIG0(x){ return rotr(x,2) ^ rotr(x,13) ^ rotr(x,22); }
    function SIG1(x){ return rotr(x,6) ^ rotr(x,11) ^ rotr(x,25); }
    function sig0(x){ return rotr(x,7) ^ rotr(x,18) ^ (x>>>3); }
    function sig1(x){ return rotr(x,17)^ rotr(x,19)^ (x>>>10); }

    var ml = bytes.length * 8;
    var withOne = new Uint8Array(((bytes.length + 9 + 63) >> 6) << 6);
    withOne.set(bytes);
    withOne[bytes.length] = 0x80;
    var dv = new DataView(withOne.buffer);
    dv.setUint32(withOne.length - 4, ml >>> 0, false);
    dv.setUint32(withOne.length - 8, Math.floor(ml / 0x100000000) >>> 0, false);

    var H = new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);
    var W = new Uint32Array(64);

    for (var i=0; i<withOne.length; i+=64){
      for (var t=0; t<16; t++) W[t] = dv.getUint32(i+t*4,false);
      for (t=16; t<64; t++) W[t] = (sig1(W[t-2]) + W[t-7] + sig0(W[t-15]) + W[t-16])>>>0;

      var a=H[0],b=H[1],c=H[2],d=H[3],e=H[4],f=H[5],g=H[6],h=H[7];
      for (t=0; t<64; t++){
        var T1 = (h + SIG1(e) + ch(e,f,g) + K[t] + W[t])>>>0;
        var T2 = (SIG0(a) + maj(a,b,c))>>>0;
        h=g; g=f; f=e; e=(d + T1)>>>0; d=c; c=b; b=a; a=(T1 + T2)>>>0;
      }
      H[0]=(H[0]+a)>>>0; H[1]=(H[1]+b)>>>0; H[2]=(H[2]+c)>>>0; H[3]=(H[3]+d)>>>0;
      H[4]=(H[4]+e)>>>0; H[5]=(H[5]+f)>>>0; H[6]=(H[6]+g)>>>0; H[7]=(H[7]+h)>>>0;
    }

    var out = new Uint8Array(32);
    var odv = new DataView(out.buffer);
    for (var j=0; j<8; j++) odv.setUint32(j*4, H[j], false);
    return out.buffer;
  }

  window.crypto.subtle.digest = function(alg, data){
    try {
      var name = (typeof alg === 'string') ? alg : (alg && alg.name) ? alg.name : '';
      name = String(name).toUpperCase();
      var bytes = toUint8(data);
      if (name === 'SHA-1' || name === 'SHA1') return Promise.resolve(sha1(bytes));
      if (name === 'SHA-256' || name === 'SHA256') return Promise.resolve(sha256(bytes));
      return Promise.reject(new Error('Unsupported digest algorithm: ' + name));
    } catch (e) {
      return Promise.reject(e);
    }
  };
})();