from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
import json, os, time, ssl

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
ROOT=os.path.dirname(os.path.abspath(__file__))

SOURCES=[
    os.environ.get('BLAZE_URL','').strip(),
    'https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/1',
    'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/1',
    'https://blaze.com/api/roulette_games/recent',
]
SOURCES=[u for i,u in enumerate(SOURCES) if u and u not in SOURCES[:i]]

CTX=ssl.create_default_context()


def normalize(data):
    if isinstance(data, dict):
        data=data.get('records') or data.get('results') or data.get('data') or data.get('items') or data
    if not isinstance(data, list):
        raise ValueError('Resposta inesperada da fonte')
    out=[]
    for x in data[:50]:
        if not isinstance(x, dict):
            continue
        roll=x.get('roll', x.get('number', x.get('value', -1)))
        try: roll=int(roll)
        except: roll=-1
        color=x.get('color', x.get('color_code'))
        if isinstance(color,str):
            c=color.strip().lower()
            if c in ('white','branco','w'): color=0
            elif c in ('red','vermelho','v','r'): color=1
            elif c in ('black','preto','p','b'): color=2
        out.append({
            'id': x.get('id') or x.get('_id') or x.get('created_at') or str(time.time()),
            'roll': roll,
            'color': color,
            'created_at': x.get('created_at') or x.get('created_date') or x.get('date')
        })
    if not out:
        raise ValueError('Fonte respondeu sem resultados')
    return out


def fetch_live():
    errors=[]
    for url in SOURCES:
        try:
            req=Request(url,headers={
                'User-Agent':'Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 Chrome/140 Safari/537.36',
                'Accept':'application/json,text/plain,*/*',
                'Accept-Language':'pt-BR,pt;q=0.9,en;q=0.8',
                'Referer':'https://blaze.bet.br/',
                'Origin':'https://blaze.bet.br',
                'Cache-Control':'no-cache',
            })
            with urlopen(req,timeout=5,context=CTX) as r:
                raw=r.read().decode('utf-8','replace')
                if r.status != 200:
                    raise RuntimeError(f'HTTP {r.status}')
            data=json.loads(raw)
            return normalize(data),url
        except Exception as e:
            errors.append(f'{url} => {type(e).__name__}: {e}')
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

    def do_GET(self):
        if self.path.startswith('/health'):
            self._send(200,json.dumps({'ok':True,'service':'double-live-radar'})); return
        if self.path.startswith('/api/double'):
            started=time.time()
            try:
                out,source=fetch_live()
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
