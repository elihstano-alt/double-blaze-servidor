from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from datetime import datetime
import json, os, time, ssl, threading

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
ROOT=os.path.dirname(os.path.abspath(__file__))
CTX=ssl.create_default_context()

OLD_HISTORY='https://web-production-25658.up.railway.app/historico?limite=100'
BLAZE_BASE='https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/{}'
DIRECT=[BLAZE_BASE.format(1),'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/1']

DEMO_START=1000.0
DEMO_BET=10.0
DEMO_FILE='/tmp/double_live_demo_state.json'
DEMO_LOCK=threading.RLock()
DEMO_STATE=None

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
    for i,x in enumerate(data[:200]):
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
        created=(x.get('created_at') or x.get('created_date') or x.get('data_hora') or x.get('date') or x.get('data') or x.get('timestamp') or x.get('hora'))
        rid=x.get('id') or x.get('_id') or x.get('game_id') or created or f'{i}-{roll}'
        if 0<=roll<=14 and c in (0,1,2):
            out.append({'id':str(rid),'roll':roll,'color':int(c),'created_at':created,'_dt':parse_dt(created)})
    if not out: raise RuntimeError('fonte respondeu sem rodadas reconheciveis')
    if any(x.get('_dt') for x in out): out.sort(key=lambda x:x.get('_dt') or datetime.min,reverse=True)
    else: out.reverse()
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
        payload=fetch_json(OLD_HISTORY,4)
        rows=normalize(payload)
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

def fetch_history_pages(max_pages=6):
    all_rows=[]; seen=set(); pages_ok=0
    for p in range(1,max_pages+1):
        try:
            rows=normalize(fetch_json(BLAZE_BASE.format(p),4))
            pages_ok+=1
            for r in rows:
                if r['id'] not in seen:
                    seen.add(r['id']); all_rows.append(r)
        except Exception as e:
            if p==1: raise
            break
    def key(r): return parse_dt(r.get('created_at')) or datetime.min
    if any(parse_dt(r.get('created_at')) for r in all_rows): all_rows.sort(key=key,reverse=True)
    print(f'[DEMO][HISTORY] paginas={pages_ok} rodadas={len(all_rows)}',flush=True)
    return all_rows

def calc_pick(rows):
    a=rows[:30]; t=len(a)
    if t<6: return None
    cnt={0:0,1:0,2:0}
    for x in a: cnt[x['color']]=cnt.get(x['color'],0)+1
    red=cnt[1]/t; black=cnt[2]/t
    current=a[0]['color']; nxt={0:0,1:0,2:0}; samples=0
    for i in range(1,len(a)-1):
        if a[i]['color']==current:
            nxt[a[i-1]['color']]+=1; samples+=1
    s1=(nxt[1]/samples if samples else .5)*.68+(1-red)*.32
    s2=(nxt[2]/samples if samples else .5)*.68+(1-black)*.32
    pick=1 if s1>=s2 else 2
    conf=max(51,min(78,round(50+abs(s1-s2)*52+min(samples,8))))
    return {'pick':pick,'confidence':conf}

def default_demo():
    return {'balance':DEMO_START,'wins':0,'losses':0,'pending':None,'history':[],'last_round_id':None,'updated_at':None,'running':True}

def load_demo():
    try:
        with open(DEMO_FILE,'r',encoding='utf-8') as f:
            d=json.load(f)
        if not isinstance(d,dict): raise ValueError()
        base=default_demo(); base.update(d); return base
    except: return default_demo()

def save_demo_locked():
    DEMO_STATE['updated_at']=datetime.utcnow().isoformat()+'Z'
    tmp=DEMO_FILE+'.tmp'
    with open(tmp,'w',encoding='utf-8') as f: json.dump(DEMO_STATE,f,ensure_ascii=False)
    os.replace(tmp,DEMO_FILE)

def settle_round_locked(round_row,history_at_round):
    p=DEMO_STATE.get('pending')
    if p:
        actual=int(round_row['color']); win=(actual==int(p['pick']))
        if win:
            DEMO_STATE['balance']=round(float(DEMO_STATE['balance'])+DEMO_BET,2); DEMO_STATE['wins']+=1
        else:
            DEMO_STATE['balance']=round(float(DEMO_STATE['balance'])-DEMO_BET,2); DEMO_STATE['losses']+=1
        DEMO_STATE['history'].insert(0,{
            'result':'WIN' if win else 'LOSS','pick':int(p['pick']),'actual':actual,'roll':int(round_row['roll']),
            'round_id':round_row['id'],'created_at':round_row.get('created_at'),'bet':DEMO_BET,
            'balance_after':DEMO_STATE['balance']
        })
        DEMO_STATE['history']=DEMO_STATE['history'][:200]
        print(f"[DEMO][{'WIN' if win else 'LOSS'}] aposta={p['pick']} saiu={actual} roll={round_row['roll']} saldo={DEMO_STATE['balance']}",flush=True)
    sig=calc_pick(history_at_round)
    if sig and DEMO_STATE['balance']>=DEMO_BET:
        DEMO_STATE['pending']={'pick':sig['pick'],'confidence':sig['confidence'],'base_round_id':round_row['id'],'bet':DEMO_BET}
    else: DEMO_STATE['pending']=None
    DEMO_STATE['last_round_id']=round_row['id']
    save_demo_locked()

def process_demo_once():
    rows=fetch_history_pages(6)
    if not rows: return
    with DEMO_LOCK:
        last=DEMO_STATE.get('last_round_id')
        if not last:
            DEMO_STATE['last_round_id']=rows[0]['id']
            sig=calc_pick(rows)
            if sig:
                DEMO_STATE['pending']={'pick':sig['pick'],'confidence':sig['confidence'],'base_round_id':rows[0]['id'],'bet':DEMO_BET}
            save_demo_locked()
            print(f"[DEMO][INIT] saldo={DEMO_STATE['balance']} rodada={rows[0]['id']} pending={DEMO_STATE.get('pending')}",flush=True)
            return
        idx=next((i for i,r in enumerate(rows) if r['id']==last),None)
        if idx is None:
            # O histórico disponível não alcançou a última rodada conhecida; não inventa resultados.
            DEMO_STATE['last_round_id']=rows[0]['id']
            sig=calc_pick(rows)
            DEMO_STATE['pending']={'pick':sig['pick'],'confidence':sig['confidence'],'base_round_id':rows[0]['id'],'bet':DEMO_BET} if sig else None
            save_demo_locked()
            print('[DEMO][RESYNC] ultima rodada antiga fora da janela; retomando sem liquidar lacuna',flush=True)
            return
        if idx==0: return
        # unseen = rows[0:idx], processa do mais antigo para o mais novo
        for i in range(idx-1,-1,-1):
            round_row=rows[i]
            history_at_round=rows[i:]
            settle_round_locked(round_row,history_at_round)

def demo_worker():
    while True:
        try: process_demo_once()
        except Exception as e: print('[DEMO][ERROR]',repr(e),flush=True)
        time.sleep(2.0)

def demo_snapshot():
    with DEMO_LOCK:
        d=json.loads(json.dumps(DEMO_STATE))
    d['start_balance']=DEMO_START; d['bet']=DEMO_BET; d['profit']=round(float(d['balance'])-DEMO_START,2)
    return d

def reset_demo():
    global DEMO_STATE
    rows=[]
    try: rows=fetch_history_pages(2)
    except: pass
    with DEMO_LOCK:
        DEMO_STATE=default_demo()
        if rows:
            DEMO_STATE['last_round_id']=rows[0]['id']
            sig=calc_pick(rows)
            if sig: DEMO_STATE['pending']={'pick':sig['pick'],'confidence':sig['confidence'],'base_round_id':rows[0]['id'],'bet':DEMO_BET}
        save_demo_locked()
        return demo_snapshot()

class Handler(BaseHTTPRequestHandler):
    def _send(self,code,body,ctype='application/json; charset=utf-8'):
        data=body if isinstance(body,bytes) else body.encode('utf-8')
        try:
            self.send_response(code); self.send_header('Content-Type',ctype); self.send_header('Content-Length',str(len(data)))
            self.send_header('Cache-Control','no-store'); self.send_header('Access-Control-Allow-Origin','*'); self.end_headers(); self.wfile.write(data)
        except (BrokenPipeError,ConnectionResetError): pass
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def do_POST(self):
        if self.path.startswith('/api/demo/reset'):
            try: self._send(200,json.dumps({'ok':True,'demo':reset_demo()},ensure_ascii=False))
            except Exception as e: self._send(500,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        self._send(404,'not found','text/plain; charset=utf-8')
    def do_GET(self):
        if self.path.startswith('/health'):
            self._send(200,json.dumps({'ok':True,'demo_running':True})); return
        if self.path.startswith('/api/demo'):
            self._send(200,json.dumps({'ok':True,'demo':demo_snapshot()},ensure_ascii=False)); return
        if self.path.startswith('/api/double'):
            t=time.time()
            try:
                rows,src=fetch_live(); self._send(200,json.dumps({'ok':True,'source':src,'latency_ms':int((time.time()-t)*1000),'results':rows},ensure_ascii=False))
            except Exception as e:
                print('[DOUBLE][SOURCE_ERROR]',repr(e),flush=True); self._send(502,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        p=self.path.split('?',1)[0]; path='index.html' if p in ('/','') else p.lstrip('/'); full=os.path.join(ROOT,path)
        if not os.path.isfile(full): self._send(404,'not found','text/plain; charset=utf-8'); return
        ext=os.path.splitext(full)[1].lower(); ct={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8'}.get(ext,'application/octet-stream')
        with open(full,'rb') as f: self._send(200,f.read(),ct)
    def log_message(self,fmt,*args): print('[DOUBLE]',fmt%args,flush=True)

if __name__=='__main__':
    DEMO_STATE=load_demo()
    print(f'Double Live em http://localhost:{PORT}',flush=True)
    try:
        rows,src=fetch_live(); print(f'[DOUBLE][STARTUP_OK] {len(rows)} rodadas recebidas via {src}',flush=True)
    except Exception as e: print('[DOUBLE][STARTUP_FAIL]',repr(e),flush=True)
    threading.Thread(target=demo_worker,name='demo-worker',daemon=True).start()
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
