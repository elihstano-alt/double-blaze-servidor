from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from datetime import datetime
import json, os, time, ssl

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
ROOT=os.path.dirname(os.path.abspath(__file__))
CTX=ssl.create_default_context()

OLD_HISTORY='https://web-production-25658.up.railway.app/historico?limite=100'
DIRECT=[
 'https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/1',
 'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/1',
]

def color_from_roll(n):
    try: n=int(n)
    except: return -1
    if n==0: return 0
    if 1<=n<=7: return 1
    if 8<=n<=14: return 2
    return -1

def pick_list(obj):
    if isinstance(obj,list): return obj
    if isinstance(obj,dict):
        for k in ('rodadas','historico','history','results','records','data','items'):
            v=obj.get(k)
            if isinstance(v,list): return v
            if isinstance(v,dict):
                z=pick_list(v)
                if z: return z
    return []

def parse_dt(v):
    if not v: return None
    s=str(v).strip()
    for fmt in ('%d/%m/%Y %H:%M:%S','%Y-%m-%dT%H:%M:%S','%Y-%m-%d %H:%M:%S'):
        try: return datetime.strptime(s[:19],fmt)
        except: pass
    return None

def normalize(obj):
    data=pick_list(obj)
    out=[]
    for i,x in enumerate(data[:100]):
        if isinstance(x,(int,float,str)):
            try: roll=int(x)
            except: continue
            out.append({'id':str(i)+'-'+str(roll),'roll':roll,'color':color_from_roll(roll),'created_at':None,'_dt':None})
            continue
        if not isinstance(x,dict): continue
        roll=x.get('roll',x.get('number',x.get('numero',x.get('value',x.get('resultado',-1)))))
        try: roll=int(roll)
        except: roll=-1
        c=x.get('color',x.get('cor',x.get('color_code')))
        if isinstance(c,str):
            s=c.lower().strip()
            if s in ('white','branco','w'): c=0
            elif s in ('red','vermelho','v','r'): c=1
            elif s in ('black','preto','p','b'): c=2
        if c not in (0,1,2): c=color_from_roll(roll)
        created=(x.get('created_at') or x.get('created_date') or x.get('data_hora') or
                 x.get('date') or x.get('data') or x.get('timestamp') or x.get('hora'))
        rid=x.get('id') or x.get('_id') or x.get('game_id') or created or f'{i}-{roll}'
        if 0<=roll<=14 and c in (0,1,2):
            out.append({'id':str(rid),'roll':roll,'color':c,'created_at':created,'_dt':parse_dt(created)})
    if not out: raise RuntimeError('fonte respondeu sem rodadas reconheciveis')
    if any(x.get('_dt') for x in out):
        out.sort(key=lambda x: x.get('_dt') or datetime.min, reverse=True)
    else:
        out.reverse()
    for x in out: x.pop('_dt',None)
    return out

def fetch_json(url,timeout=5):
    req=Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json,text/plain,*/*','Cache-Control':'no-cache'})
    with urlopen(req,timeout=timeout,context=CTX) as r:
        raw=r.read().decode('utf-8','replace')
    return json.loads(raw)

def fetch_live():
    errors=[]
    try:
        payload=fetch_json(OLD_HISTORY,6)
        rows=normalize(payload)
        print(f'[DOUBLE][RAILWAY_OK] quantidade_payload={payload.get("quantidade") if isinstance(payload,dict) else "?"} normalizadas={len(rows)}',flush=True)
        return rows[:50],OLD_HISTORY
    except Exception as e:
        errors.append('Railway antigo => '+type(e).__name__+': '+str(e))
    for u in DIRECT:
        try:
            rows=normalize(fetch_json(u,4))
            return rows[:50],u
        except Exception as e:
            errors.append(u+' => '+type(e).__name__+': '+str(e))
    raise RuntimeError(' | '.join(errors))

class Handler(BaseHTTPRequestHandler):
    def _send(self,code,body,ctype='application/json; charset=utf-8'):
        data=body if isinstance(body,bytes) else body.encode('utf-8')
        try:
            self.send_response(code)
            self.send_header('Content-Type',ctype)
            self.send_header('Content-Length',str(len(data)))
            self.send_header('Cache-Control','no-store')
            self.send_header('Access-Control-Allow-Origin','*')
            self.end_headers(); self.wfile.write(data)
        except (BrokenPipeError,ConnectionResetError): pass
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def do_GET(self):
        if self.path.startswith('/health'):
            self._send(200,json.dumps({'ok':True})); return
        if self.path.startswith('/api/double'):
            t=time.time()
            try:
                rows,src=fetch_live()
                print(f'[DOUBLE][OK] {len(rows)} via {src}',flush=True)
                self._send(200,json.dumps({'ok':True,'source':src,'latency_ms':int((time.time()-t)*1000),'results':rows},ensure_ascii=False))
            except Exception as e:
                print('[DOUBLE][SOURCE_ERROR]',repr(e),flush=True)
                self._send(502,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        p=self.path.split('?',1)[0]
        path='index.html' if p in ('/','') else p.lstrip('/')
        full=os.path.join(ROOT,path)
        if not os.path.isfile(full): self._send(404,'not found','text/plain; charset=utf-8'); return
        ext=os.path.splitext(full)[1].lower()
        ct={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8'}.get(ext,'application/octet-stream')
        with open(full,'rb') as f: self._send(200,f.read(),ct)
    def log_message(self,fmt,*args): print('[DOUBLE]',fmt%args,flush=True)

if __name__=='__main__':
    print(f'Double Live em http://localhost:{PORT}',flush=True)
    try:
        rows,src=fetch_live()
        print(f'[DOUBLE][STARTUP_OK] {len(rows)} rodadas recebidas via {src}',flush=True)
    except Exception as e:
        print('[DOUBLE][STARTUP_FAIL]',repr(e),flush=True)
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
