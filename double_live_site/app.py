from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
import json, os, time

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
BLAZE_URL=os.environ.get('BLAZE_URL','https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/1')
ROOT=os.path.dirname(os.path.abspath(__file__))

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype='application/json; charset=utf-8'):
        data = body if isinstance(body, bytes) else body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control','no-store')
        self.end_headers(); self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith('/api/double'):
            try:
                req=Request(BLAZE_URL, headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
                with urlopen(req, timeout=8) as r:
                    raw=r.read().decode('utf-8')
                data=json.loads(raw)
                if isinstance(data, dict):
                    data=data.get('records') or data.get('results') or data.get('data') or data
                if not isinstance(data, list):
                    raise ValueError('Resposta inesperada da fonte')
                out=[]
                for x in data[:50]:
                    roll=int(x.get('roll', x.get('number', -1)))
                    color=x.get('color')
                    if isinstance(color,str):
                        c=color.lower()
                        color = 0 if c in ('white','branco') else 1 if c in ('red','vermelho') else 2 if c in ('black','preto') else color
                    out.append({
                        'id': x.get('id') or x.get('_id') or x.get('created_at') or str(time.time()),
                        'roll': roll,
                        'color': color,
                        'created_at': x.get('created_at') or x.get('created_date') or x.get('date')
                    })
                self._send(200,json.dumps({'ok':True,'source':BLAZE_URL,'results':out},ensure_ascii=False))
            except Exception as e:
                self._send(502,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        path='index.html' if self.path in ('/','') else self.path.lstrip('/')
        full=os.path.join(ROOT,path)
        if not os.path.isfile(full):
            self._send(404,'not found','text/plain; charset=utf-8'); return
        ext=os.path.splitext(full)[1].lower()
        ctype={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8'}.get(ext,'application/octet-stream')
        with open(full,'rb') as f: self._send(200,f.read(),ctype)

    def log_message(self, fmt, *args):
        print('[DOUBLE]', fmt % args)

if __name__=='__main__':
    print(f'Double Live em http://localhost:{PORT}')
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
