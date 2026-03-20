(() => {
  function asYmd(x){
    if(!x) return '';
    const s = String(x).trim();
    return (s.length >= 10) ? s.slice(0,10) : '';
  }
  function ymdToDate(ymd){
    const s = asYmd(ymd);
    const d = new Date(s + 'T00:00:00.000Z');
    return isFinite(d) ? d : null;
  }
  function dateToYmd(d){
    return new Date(d.getTime()).toISOString().slice(0,10);
  }
  function diffDaysIncl(aYmd, bYmd){
    const a = ymdToDate(aYmd), b = ymdToDate(bYmd);
    if(!a || !b) return 1;
    const x = Date.UTC(a.getUTCFullYear(), a.getUTCMonth(), a.getUTCDate());
    const y = Date.UTC(b.getUTCFullYear(), b.getUTCMonth(), b.getUTCDate());
    const n = Math.floor((y - x)/86400000) + 1;
    return n > 0 ? n : 1;
  }
  function toIsoStart(ymd){ return asYmd(ymd) + "T00:00:00.000Z"; }
  function toIsoEnd(ymd){ return asYmd(ymd) + "T23:59:59.999Z"; }

  // Главная функция: дробит период [from..to] на чанки по maxDays (в днях, включительно)
  function splitYmdRange(fromYmd, toYmd, maxDays){
    const from = asYmd(fromYmd), to = asYmd(toYmd);
    const max = Math.max(1, Number(maxDays || 31) || 31);

    let a = ymdToDate(from);
    let b = ymdToDate(to);
    if(!a || !b) return [];

    // нормализуем порядок
    if(a.getTime() > b.getTime()){
      const tmp = a; a = b; b = tmp;
    }

    const out = [];
    let cur = new Date(a.getTime());
    for(let guard=0; guard<10000; guard++){
      if(cur.getTime() > b.getTime()) break;

      const end = new Date(cur.getTime());
      end.setUTCDate(end.getUTCDate() + (max - 1));
      if(end.getTime() > b.getTime()) end.setTime(b.getTime());

      const f = dateToYmd(cur);
      const t = dateToYmd(end);
      out.push({
        fromYmd: f,
        toYmd: t,
        sinceISO: toIsoStart(f),
        toISO: toIsoEnd(t),
        days: diffDaysIncl(f,t)
      });

      end.setUTCDate(end.getUTCDate() + 1);
      cur = end;
    }
    return out;
  }

  function splitIsoRange(sinceISO, toISO, maxDays){
    const a = new Date(String(sinceISO||''));
    const b = new Date(String(toISO||''));
    if(!isFinite(a) || !isFinite(b)) return [];
    const from = a.toISOString().slice(0,10);
    const to   = b.toISOString().slice(0,10);
    return splitYmdRange(from, to, maxDays);
  }

  window.ozonSplitYmdRange = splitYmdRange;
  window.ozonSplitIsoRange = splitIsoRange;
  window.ozonDiffDaysInclusive = diffDaysIncl;
})();
