from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from html.parser import HTMLParser
from datetime import datetime
import json, os, time, ssl, re

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
ROOT=os.path.dirname(os.path.abspath(__file__))
CTX=ssl.create_default_context()

BESTBLAZE='https://bestblaze.com.br/doubleRodadas'
BLAZE_SOURCES=[
    'https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/1',
    'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/1',
    'https://blaze.com/api/roulette_games/recent',
]

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(); self.parts=[]
    def handle_data(self,data):
        s=' '.join(data.split())
        if s: self.parts.append(s)


def color_from_roll(roll):
    if roll==0: return 0
    if 1 <= roll <= 7: return 1
    if 8 <= roll <= 14: return 2
    return -1


def fetch_bestblaze():
    req=Request(BESTBLAZE,headers={
        'User-Agent':'Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 Chrome/140 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':'pt-BR,pt;q=0.9',
        'Cache-Control':'no-cache',
    })
    with urlopen(req,timeout=6,context=CTX) as r:
        raw=r.read().decode('utf-8','replace')
    p=TextExtractor(); p.feed(raw)
    text='\n'.join(p.parts)

    # Captura pares número + data/hora. Quando o zero é renderizado apenas visualmente,
    # o número pode não aparecer no texto; nesse caso inferimos 0 para aquele timestamp.
    dt_pat=re.compile(r'\b(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\b')
    matches=list(dt_pat.finditer(text))
    rows=[]
    last_end=0
    for m in matches:
        before=text[last_end:m.start()]
        nums=re.findall(r'(?<!\d)(1[0-4]|[0-9])(?!\d)', before[-80:])
        roll=int(nums[-1]) if nums else 0
        if not (0 <= roll <= 14):
            last_end=m.end(); continue
        ds=m.group(1)
        try:
            dt=datetime.strptime(ds,'%d/%m/%Y %H:%M:%S')
        except Exception:
            last_end=m.end(); continue
        rows.append({
            'id': ds,
            'roll': roll,
            'color': color_from_roll(roll),
            'created_at': dt.isoformat(),
            '_dt': dt,
        })
        last_end=m.end()

    # Remove duplicados e ordena do mais novo para o mais antigo.
    dedup={}
    for x in rows:
        dedup[x['id']]=x
    rows=list(dedup.values())
    rows.sort(key=lambda x:x['_dt'],reverse=True)
    for x in rows: x.pop('_dt',None)
    if len(rows) < 6:
        raise RuntimeError('BestBlaze respondeu, mas não foi possível extrair rodadas suficientes')
    return rows[:50], BESTBLAZE


def normalize(data):
    if isinstance(data, dict):
        data=data.get('records') or data.get('results') or data.get('data') or data.get('items') or data
    if not isinstance(data, list):
        raise ValueError('Resposta inesperada da fonte')
    out=[]
    for x in data[:50]:
        if not isinstance(x, dict): continue
        roll=x.get('roll',x.get('number',x.get('value',-1)))
        try: roll=int(roll)
        except: roll=-1
        color=x.get('color',x.get('color_code'))
        if isinstance(color,str):
            c=color.strip().lower()
            if c in ('white','branco','w'): color=0
            elif c in ('red','vermelho','v','r'): color=1
            elif c in ('black','preto','p','b'): color=2
        if color not in (0,1,2) and 0 <= roll <= 14:
            color=color_from_roll(roll)
        out.append({'id':x.get('id') or x.get('_id') or x.get('created_at') or str(time.time()),'roll':roll,'color':color,'created_at':x.get('created_at') or x.get('created_date') or x.get('date')})
    if not out: raise ValueError('Fonte respondeu sem resultados')
    return out


def fetch_blaze_direct():
    errors=[]
    for url in BLAZE_SOURCES:
        try:
            req=Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json,text/plain,*/*','Referer':'https://blaze.bet.br/','Origin':'https://blaze.bet.br'})
            with urlopen(req,timeout=3,context=CTX) as r:
                raw=r.read().decode('utf-8','replace')
            return normalize(json.loads(raw)),url
        except Exception as e:
            errors.append(f'{url} => {type(e).__name__}: {e}')
    raise RuntimeError(' | '.join(errors))


def fetch_live():
    errors=[]
    # Fonte brasileira primeiro para contornar o bloqueio geográfico/451 da Blaze no Render.
    try:
        return fetch_bestblaze()
    except Exception as e:
        errors.append(f'BestBlaze => {type(e).__name__}: {e}')
    try:
        return fetch_blaze_direct()
    except Exception as e:
        errors.append(f'Blaze => {type(e).__name__}: {e}')
    raise RuntimeError(' | '.join(errors))


class Handler(BaseHTTPRequestHandler):
    def _send(self,code,body,ctype='application/json; charset=utf-8'):
        data=body if isinstance(body,bytes) else body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type',ctype)
        self.send_header('Content-Length',str(len(data)))
        self.send_header('Cache-Control','no-store')
        self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers(); self.wfile.write(data)

    def do_HEAD(self):
        self.send_response(200); self.end_headers()

    def do_GET(self):
        if self.path.startswith('/health'):
            self._send(200,json.dumps({'ok':True,'service':'double-live-radar'})); return
        if self.path.startswith('/api/double'):
            started=time.time()
            try:
                out,source=fetch_live()
                print(f'[DOUBLE][OK] {len(out)} resultados via {source}',flush=True)
                self._send(200,json.dumps({'ok':True,'source':source,'latency_ms':int((time.time()-started)*1000),'results':out},ensure_ascii=False))
            except Exception as e:
                print('[DOUBLE][SOURCE_ERROR]',repr(e),flush=True)
                self._send(502,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        path='index.html' if self.path.split('?',1)[0] in ('/','') else self.path.split('?',1)[0].lstrip('/')
        full=os.path.join(ROOT,path)
        if not os.path.isfile(full):
            self._send(404,'not found','text/plain; charset=utf-8'); return
        ext=os.path.splitext(full)[1].lower()
        ctype={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8'}.get(ext,'application/octet-stream')
        with open(full,'rb') as f: self._send(200,f.read(),ctype)

    def log_message(self,fmt,*args):
        print('[DOUBLE]',fmt % args,flush=True)

if __name__=='__main__':
    print(f'Double Live em http://localhost:{PORT}',flush=True)
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
