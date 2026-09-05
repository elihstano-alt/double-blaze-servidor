from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from datetime import datetime
import json, os, time, ssl, threading, random, re

HOST='0.0.0.0'
PORT=int(os.environ.get('PORT','8000'))
ROOT=os.path.dirname(os.path.abspath(__file__))
CTX=ssl.create_default_context()
OLD_HISTORY='https://web-production-25658.up.railway.app/historico?limite=100'
BLAZE_BASE='https://blaze.bet.br/api/singleplayer-originals/originals/roulette_games/recent/{}'
DIRECT=[BLAZE_BASE.format(1),'https://blaze.com/api/singleplayer-originals/originals/roulette_games/recent/1']
MIGRATE_BASE=os.environ.get('MIGRATE_BASE','https://double-live-radar-eu.onrender.com')
DEMO_START=1000.0
DEMO_BET=10.0
DEMO_COUNT=10
DEMO_FILE='/tmp/double_live_demos_state.json'
DEMO_LOCK=threading.RLock()
DEMOS_STATE=None
STRATEGIES_STATE=[]
LIVE_LOCK=threading.RLock()
LIVE_ROWS=[]
LIVE_SOURCE=None
LIVE_UPDATED_AT=None
LIVE_ERROR=None

BASE_STRATEGIES=[
 {'id':'radar','name':'Radar atual','kind':'single','components':['radar'],'description':'Transições recentes + equilíbrio de cores.'},
 {'id':'majority','name':'Maioria recente','kind':'single','components':['majority'],'description':'Segue a cor dominante nas rodadas recentes.'},
 {'id':'minority','name':'Contrária recente','kind':'single','components':['minority'],'description':'Prioriza a cor menos frequente entre vermelho e preto.'},
 {'id':'transition','name':'Transição','kind':'single','components':['transition'],'description':'Usa o que historicamente veio após a cor atual.'},
 {'id':'reverse_streak','name':'Reversão de sequência','kind':'single','components':['reverse_streak'],'description':'Após sequência, procura a cor oposta; fora disso usa o Radar.'},
 {'id':'combo_radar_transition','name':'Combo Radar + Transição','kind':'combo','components':['radar','transition'],'description':'Combina Radar e Transição por votação/confiança.'}
]

def color_from_roll(n):
    try:n=int(n)
    except:return -1
    if n==0:return 0
    if 1<=n<=7:return 1
    if 8<=n<=14:return 2
    return -1

def pick_list(obj):
    if isinstance(obj,list):return obj
    if isinstance(obj,dict):
        for k in ('rodadas','historico','history','results','records','data','items'):
            v=obj.get(k)
            if isinstance(v,list):return v
            if isinstance(v,dict):
                z=pick_list(v)
                if z:return z
    return []

def parse_dt(v):
    if not v:return None
    s=str(v).strip()
    for fmt in ('%d/%m/%Y %H:%M:%S','%Y-%m-%dT%H:%M:%S','%Y-%m-%d %H:%M:%S'):
        try:return datetime.strptime(s[:19],fmt)
        except:pass
    return None

def normalize(obj):
    data=pick_list(obj);out=[]
    for i,x in enumerate(data[:200]):
        if isinstance(x,(int,float,str)):
            try:roll=int(x)
            except:continue
            out.append({'id':str(i)+'-'+str(roll),'roll':roll,'color':color_from_roll(roll),'created_at':None,'_dt':None});continue
        if not isinstance(x,dict):continue
        roll=x.get('roll',x.get('number',x.get('numero',x.get('value',x.get('resultado',-1)))))
        try:roll=int(roll)
        except:roll=-1
        c=x.get('color',x.get('cor',x.get('color_code')))
        if isinstance(c,str):
            s=c.lower().strip()
            if s in ('white','branco','w'):c=0
            elif s in ('red','vermelho','v','r'):c=1
            elif s in ('black','preto','p','b'):c=2
        if c not in (0,1,2):c=color_from_roll(roll)
        created=(x.get('created_at') or x.get('created_date') or x.get('data_hora') or x.get('date') or x.get('data') or x.get('timestamp') or x.get('hora'))
        rid=x.get('id') or x.get('_id') or x.get('game_id') or created or f'{i}-{roll}'
        if 0<=roll<=14 and c in (0,1,2):out.append({'id':str(rid),'roll':roll,'color':int(c),'created_at':created,'_dt':parse_dt(created)})
    if not out:raise RuntimeError('fonte respondeu sem rodadas reconheciveis')
    if any(x.get('_dt') for x in out):out.sort(key=lambda x:x.get('_dt') or datetime.min,reverse=True)
    else:out.reverse()
    for x in out:x.pop('_dt',None)
    return out

def fetch_json(url,timeout=5):
    req=Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json,text/plain,*/*','Cache-Control':'no-cache'})
    with urlopen(req,timeout=timeout,context=CTX) as r:return json.loads(r.read().decode('utf-8','replace'))

def fetch_live():
    errors=[]
    try:return normalize(fetch_json(OLD_HISTORY,2))[:50],OLD_HISTORY
    except Exception as e:errors.append('Railway antigo => '+type(e).__name__+': '+str(e))
    for u in DIRECT:
        try:return normalize(fetch_json(u,2))[:50],u
        except Exception as e:errors.append(u+' => '+type(e).__name__+': '+str(e))
    raise RuntimeError(' | '.join(errors))

def fetch_history_pages(max_pages=6):
    all_rows=[];seen=set();pages_ok=0
    for p in range(1,max_pages+1):
        try:
            rows=normalize(fetch_json(BLAZE_BASE.format(p),3));pages_ok+=1
            for r in rows:
                if r['id'] not in seen:seen.add(r['id']);all_rows.append(r)
        except Exception:
            if p==1:raise
            break
    if any(parse_dt(r.get('created_at')) for r in all_rows):all_rows.sort(key=lambda r:parse_dt(r.get('created_at')) or datetime.min,reverse=True)
    print(f'[DEMO][HISTORY] paginas={pages_ok} rodadas={len(all_rows)}',flush=True)
    return all_rows

def radar_pick(rows):
    a=rows[:30];t=len(a)
    if t<6:return None
    cnt={0:0,1:0,2:0}
    for x in a:cnt[x['color']]=cnt.get(x['color'],0)+1
    red=cnt[1]/t;black=cnt[2]/t;current=a[0]['color'];nxt={0:0,1:0,2:0};samples=0
    for i in range(1,len(a)-1):
        if a[i]['color']==current:nxt[a[i-1]['color']]+=1;samples+=1
    s1=(nxt[1]/samples if samples else .5)*.68+(1-red)*.32
    s2=(nxt[2]/samples if samples else .5)*.68+(1-black)*.32
    pick=1 if s1>=s2 else 2;conf=max(51,min(78,round(50+abs(s1-s2)*52+min(samples,8))))
    return {'pick':pick,'confidence':conf,'source':'radar'}

def component_pick(component,rows):
    a=rows[:30]
    if len(a)<6:return None
    if component=='radar':return radar_pick(rows)
    red=sum(1 for x in a if x['color']==1);black=sum(1 for x in a if x['color']==2);total=max(1,red+black)
    if component=='majority':
        p=1 if red>=black else 2;return {'pick':p,'confidence':max(51,min(75,50+round(abs(red-black)/total*50))),'source':component}
    if component=='minority':
        p=1 if red<=black else 2;return {'pick':p,'confidence':max(51,min(72,51+round(abs(red-black)/total*35))),'source':component}
    if component=='transition':
        cur=a[0]['color'];n={1:0,2:0};s=0
        for i in range(1,len(a)-1):
            if a[i]['color']==cur and a[i-1]['color'] in (1,2):n[a[i-1]['color']]+=1;s+=1
        if not s:return radar_pick(rows)
        p=1 if n[1]>=n[2] else 2;return {'pick':p,'confidence':max(51,min(76,50+round(abs(n[1]-n[2])/s*40))),'source':component}
    if component=='reverse_streak':
        streak=1
        for i in range(1,len(a)):
            if a[i]['color']==a[0]['color']:streak+=1
            else:break
        if streak>=2 and a[0]['color'] in (1,2):return {'pick':2 if a[0]['color']==1 else 1,'confidence':min(72,54+streak*3),'source':component}
        return radar_pick(rows)
    return radar_pick(rows)

def strategy_by_id(strategy_id):
    for s in STRATEGIES_STATE:
        if s.get('id')==strategy_id:return s
    return STRATEGIES_STATE[0] if STRATEGIES_STATE else BASE_STRATEGIES[0]

def calc_strategy_pick(rows,strategy_id='radar'):
    s=strategy_by_id(strategy_id);comps=s.get('components') or ['radar'];votes=[]
    for c in comps:
        r=component_pick(c,rows)
        if r:votes.append(r)
    if not votes:return radar_pick(rows)
    score={1:0.0,2:0.0}
    for v in votes:score[v['pick']]+=float(v.get('confidence',51))
    p=1 if score[1]>=score[2] else 2
    conf=round(sum(v.get('confidence',51) for v in votes if v['pick']==p)/max(1,sum(1 for v in votes if v['pick']==p)))
    if len(votes)>1 and all(v['pick']==p for v in votes):conf=min(82,conf+4)
    return {'pick':p,'confidence':max(51,min(82,conf)),'strategy_id':s.get('id','radar'),'strategy_name':s.get('name','Radar atual')}

def default_demo(demo_id):
    mode='next_signal' if demo_id>=6 else 'same_signal'
    return {'id':demo_id,'name':f'Demo {demo_id}','balance':DEMO_START,'wins':0,'losses':0,'pending':None,'history':[],'last_round_id':None,'updated_at':None,'running':True,'max_gale':0,'gale_mode':mode,'strategy_id':'radar'}

def sanitize_demo(d,demo_id):
    base=default_demo(demo_id)
    if isinstance(d,dict):base.update(d)
    base['id']=demo_id;base['name']=f'Demo {demo_id}';base['max_gale']=max(0,min(5,int(base.get('max_gale',0))))
    if base.get('gale_mode') not in ('same_signal','next_signal'):base['gale_mode']='next_signal' if demo_id>=6 else 'same_signal'
    if not isinstance(base.get('history'),list):base['history']=[]
    base['history']=base['history'][:200]
    if not base.get('strategy_id'):base['strategy_id']='radar'
    return base

def sanitize_strategy(s):
    if not isinstance(s,dict):return None
    name=str(s.get('name') or 'Estratégia').strip()[:60]
    comps=[c for c in (s.get('components') or []) if c in ('radar','majority','minority','transition','reverse_streak')]
    if not comps:return None
    sid=str(s.get('id') or re.sub(r'[^a-z0-9]+','_',name.lower())).strip('_')[:50] or ('strategy_'+str(int(time.time())))
    return {'id':sid,'name':name,'kind':'combo' if len(comps)>1 else 'single','components':comps[:5],'description':str(s.get('description') or 'Estratégia personalizada.')[:160]}

def migrate_remote():
    demos=[];strategies=[]
    try:
        payload=fetch_json(MIGRATE_BASE+'/api/demo',8);arr=payload.get('demos') if isinstance(payload,dict) else None
        if isinstance(arr,list) and arr:
            demos=[sanitize_demo(arr[i],i+1) for i in range(min(len(arr),DEMO_COUNT))]
            print(f'[DEMO][MIGRATE] {len(demos)} demos recuperadas do deploy anterior',flush=True)
    except Exception as e:print('[DEMO][MIGRATE] indisponivel:',repr(e),flush=True)
    try:
        payload=fetch_json(MIGRATE_BASE+'/api/strategies',4);arr=payload.get('strategies') if isinstance(payload,dict) else None
        if isinstance(arr,list):strategies=[x for x in (sanitize_strategy(s) for s in arr) if x]
    except:pass
    return demos,strategies

def load_state():
    global STRATEGIES_STATE
    try:
        with open(DEMO_FILE,'r',encoding='utf-8') as f:d=json.load(f)
        arr=d.get('demos') if isinstance(d,dict) else None;st=d.get('strategies') if isinstance(d,dict) else None
        if isinstance(arr,list) and arr:
            out=[sanitize_demo(arr[i] if i<len(arr) else {},i+1) for i in range(DEMO_COUNT)]
            custom=[x for x in (sanitize_strategy(s) for s in (st or [])) if x]
            STRATEGIES_STATE=merge_strategies(custom);return out
    except:pass
    old,custom=migrate_remote();STRATEGIES_STATE=merge_strategies(custom);out=list(old)
    for i in range(len(out)+1,DEMO_COUNT+1):out.append(default_demo(i))
    return out

def merge_strategies(custom):
    result=[dict(s) for s in BASE_STRATEGIES];ids={s['id'] for s in result}
    for s in custom:
        if s['id'] not in ids:result.append(s);ids.add(s['id'])
    return result

def save_state_locked():
    now=datetime.utcnow().isoformat()+'Z'
    for d in DEMOS_STATE:d['updated_at']=now
    tmp=DEMO_FILE+'.tmp'
    with open(tmp,'w',encoding='utf-8') as f:json.dump({'demos':DEMOS_STATE,'strategies':STRATEGIES_STATE},f,ensure_ascii=False)
    os.replace(tmp,DEMO_FILE)

def new_signal_pending(demo,round_row,history_at_round,bet=None,gale_level=0):
    sig=calc_strategy_pick(history_at_round,demo.get('strategy_id','radar'));value=DEMO_BET if bet is None else float(bet)
    if not sig or demo['balance']<value:return None
    return {'pick':sig['pick'],'confidence':sig['confidence'],'base_round_id':round_row['id'],'bet':value,'gale_level':gale_level,'strategy_id':sig.get('strategy_id','radar'),'strategy_name':sig.get('strategy_name','Radar atual')}

def settle_round_locked(demo,round_row,history_at_round):
    p=demo.get('pending');next_pending=None
    if p:
        actual=int(round_row['color']);pick=int(p['pick']);bet=float(p.get('bet',DEMO_BET));gale=int(p.get('gale_level',0));win=(actual==pick)
        if win:demo['balance']=round(float(demo['balance'])+bet,2);demo['wins']+=1
        else:demo['balance']=round(float(demo['balance'])-bet,2);demo['losses']+=1
        demo['history'].insert(0,{'result':'WIN' if win else 'LOSS','pick':pick,'actual':actual,'roll':int(round_row['roll']),'round_id':round_row['id'],'created_at':round_row.get('created_at'),'bet':bet,'gale_level':gale,'gale_mode':demo.get('gale_mode','same_signal'),'strategy_id':p.get('strategy_id',demo.get('strategy_id','radar')),'strategy_name':p.get('strategy_name'),'balance_after':demo['balance']})
        demo['history']=demo['history'][:200]
        print(f"[DEMO{demo['id']}][{'WIN' if win else 'LOSS'}] G{gale} estrategia={demo.get('strategy_id')} modo={demo.get('gale_mode')} aposta={pick} valor={bet} saiu={actual} saldo={demo['balance']}",flush=True)
        if (not win) and gale<int(demo.get('max_gale',0)):
            next_bet=round(bet*2,2)
            if demo['balance']>=next_bet:
                if demo.get('gale_mode')=='next_signal':next_pending=new_signal_pending(demo,round_row,history_at_round,next_bet,gale+1)
                else:next_pending={'pick':pick,'confidence':p.get('confidence'),'base_round_id':round_row['id'],'bet':next_bet,'gale_level':gale+1,'strategy_id':p.get('strategy_id',demo.get('strategy_id','radar')),'strategy_name':p.get('strategy_name')}
    if next_pending is None:next_pending=new_signal_pending(demo,round_row,history_at_round)
    demo['pending']=next_pending;demo['last_round_id']=round_row['id']

def process_one_demo_locked(demo,rows):
    last=demo.get('last_round_id')
    if not last:demo['last_round_id']=rows[0]['id'];demo['pending']=new_signal_pending(demo,rows[0],rows);return
    idx=next((i for i,r in enumerate(rows) if r['id']==last),None)
    if idx is None:demo['last_round_id']=rows[0]['id'];demo['pending']=new_signal_pending(demo,rows[0],rows);return
    if idx==0:return
    for i in range(idx-1,-1,-1):settle_round_locked(demo,rows[i],rows[i:])

def process_demo_rows(rows):
    if not rows:return
    with DEMO_LOCK:
        for demo in DEMOS_STATE:process_one_demo_locked(demo,rows)
        save_state_locked()

def live_worker():
    global LIVE_ROWS,LIVE_SOURCE,LIVE_UPDATED_AT,LIVE_ERROR
    first=True
    while True:
        started=time.time()
        try:
            if first:
                try:rows=fetch_history_pages(6);src='history-pages'
                except:rows,src=fetch_live()
                first=False
            else:rows,src=fetch_live()
            with LIVE_LOCK:LIVE_ROWS=rows[:50];LIVE_SOURCE=src;LIVE_UPDATED_AT=datetime.utcnow().isoformat()+'Z';LIVE_ERROR=None
            process_demo_rows(rows)
        except Exception as e:
            with LIVE_LOCK:LIVE_ERROR=str(e)
            print('[LIVE][ERROR]',repr(e),flush=True)
        time.sleep(max(0.0,0.2-(time.time()-started)))

def demo_snapshot_one(demo):
    d=json.loads(json.dumps(demo));d['start_balance']=DEMO_START;d['base_bet']=DEMO_BET;d['profit']=round(float(d['balance'])-DEMO_START,2);d['gale_options']=[0,1,2,3,4,5];d['gale_modes']=['same_signal','next_signal'];return d

def demo_snapshot():
    with DEMO_LOCK:return [demo_snapshot_one(d) for d in DEMOS_STATE]

def live_snapshot():
    with LIVE_LOCK:return json.loads(json.dumps({'rows':LIVE_ROWS,'source':LIVE_SOURCE,'updated_at':LIVE_UPDATED_AT,'error':LIVE_ERROR}))

def reset_demo(demo_id=1):
    with LIVE_LOCK:rows=json.loads(json.dumps(LIVE_ROWS))
    with DEMO_LOCK:
        i=max(1,min(DEMO_COUNT,int(demo_id)))-1;old=DEMOS_STATE[i];keep_gale=int(old.get('max_gale',0));keep_mode=old.get('gale_mode','same_signal');keep_strategy=old.get('strategy_id','radar')
        DEMOS_STATE[i]=default_demo(i+1);DEMOS_STATE[i]['max_gale']=keep_gale;DEMOS_STATE[i]['gale_mode']=keep_mode;DEMOS_STATE[i]['strategy_id']=keep_strategy
        if rows:DEMOS_STATE[i]['last_round_id']=rows[0]['id'];DEMOS_STATE[i]['pending']=new_signal_pending(DEMOS_STATE[i],rows[0],rows)
        save_state_locked();return demo_snapshot_one(DEMOS_STATE[i])

def set_demo_config(demo_id,max_gale=None,gale_mode=None,strategy_id=None):
    with DEMO_LOCK:
        i=max(1,min(DEMO_COUNT,int(demo_id)))-1
        if max_gale is not None:DEMOS_STATE[i]['max_gale']=max(0,min(5,int(max_gale)))
        if gale_mode in ('same_signal','next_signal'):DEMOS_STATE[i]['gale_mode']=gale_mode
        if strategy_id and any(s['id']==strategy_id for s in STRATEGIES_STATE):DEMOS_STATE[i]['strategy_id']=strategy_id
        save_state_locked();return demo_snapshot_one(DEMOS_STATE[i])

def add_strategy(name,components):
    global STRATEGIES_STATE
    clean=[c for c in components if c in ('radar','majority','minority','transition','reverse_streak')][:5]
    if not clean:raise ValueError('Escolha ao menos um componente')
    baseid=re.sub(r'[^a-z0-9]+','_',str(name).lower()).strip('_') or 'estrategia';sid=baseid+'_'+str(int(time.time()*1000))[-6:]
    s={'id':sid,'name':str(name or 'Estratégia personalizada')[:60],'kind':'combo' if len(clean)>1 else 'single','components':clean,'description':'Combinação personalizada: '+', '.join(clean)}
    with DEMO_LOCK:STRATEGIES_STATE.append(s);save_state_locked()
    return s

def generate_strategy():
    pool=['radar','majority','minority','transition','reverse_streak'];n=random.choice([2,2,3]);comps=random.sample(pool,n);name='Combo '+ ' + '.join(c.replace('_',' ').title() for c in comps)
    return add_strategy(name,comps)

class Handler(BaseHTTPRequestHandler):
    def _send(self,code,body,ctype='application/json; charset=utf-8'):
        data=body if isinstance(body,bytes) else body.encode('utf-8')
        try:self.send_response(code);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','no-store');self.send_header('Access-Control-Allow-Origin','*');self.end_headers();self.wfile.write(data)
        except (BrokenPipeError,ConnectionResetError):pass
    def _json_body(self):
        try:n=int(self.headers.get('Content-Length','0'));raw=self.rfile.read(n) if n>0 else b'{}';return json.loads(raw.decode('utf-8'))
        except:return {}
    def do_HEAD(self):self.send_response(200);self.end_headers()
    def do_POST(self):
        if self.path.startswith('/api/demo/reset'):
            try:b=self._json_body();self._send(200,json.dumps({'ok':True,'demo':reset_demo(b.get('demo_id',1))},ensure_ascii=False))
            except Exception as e:self._send(500,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        if self.path.startswith('/api/demo/config'):
            try:b=self._json_body();self._send(200,json.dumps({'ok':True,'demo':set_demo_config(b.get('demo_id',1),b.get('max_gale'),b.get('gale_mode'),b.get('strategy_id'))},ensure_ascii=False))
            except Exception as e:self._send(400,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        if self.path.startswith('/api/strategies/create'):
            try:b=self._json_body();s=add_strategy(b.get('name','Estratégia personalizada'),b.get('components') or []);self._send(200,json.dumps({'ok':True,'strategy':s,'strategies':STRATEGIES_STATE},ensure_ascii=False))
            except Exception as e:self._send(400,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        if self.path.startswith('/api/strategies/generate'):
            try:s=generate_strategy();self._send(200,json.dumps({'ok':True,'strategy':s,'strategies':STRATEGIES_STATE},ensure_ascii=False))
            except Exception as e:self._send(500,json.dumps({'ok':False,'error':str(e)},ensure_ascii=False))
            return
        self._send(404,'not found','text/plain; charset=utf-8')
    def do_GET(self):
        if self.path.startswith('/health'):
            snap=live_snapshot();self._send(200,json.dumps({'ok':True,'demo_running':True,'demo_count':DEMO_COUNT,'strategy_count':len(STRATEGIES_STATE),'live_updated_at':snap['updated_at'],'live_error':snap['error']}));return
        if self.path.startswith('/api/demo'):
            demos=demo_snapshot();self._send(200,json.dumps({'ok':True,'demo':demos[0],'demos':demos},ensure_ascii=False));return
        if self.path.startswith('/api/strategies'):
            self._send(200,json.dumps({'ok':True,'strategies':STRATEGIES_STATE,'components':[{'id':'radar','name':'Radar atual'},{'id':'majority','name':'Maioria recente'},{'id':'minority','name':'Contrária recente'},{'id':'transition','name':'Transição'},{'id':'reverse_streak','name':'Reversão de sequência'}]},ensure_ascii=False));return
        if self.path.startswith('/api/double'):
            snap=live_snapshot()
            if snap['rows']:self._send(200,json.dumps({'ok':True,'source':snap['source'],'updated_at':snap['updated_at'],'results':snap['rows']},ensure_ascii=False))
            else:self._send(503,json.dumps({'ok':False,'error':snap['error'] or 'aguardando primeira leitura'},ensure_ascii=False))
            return
        p=self.path.split('?',1)[0];path='index.html' if p in ('/','') else p.lstrip('/');full=os.path.join(ROOT,path)
        if not os.path.isfile(full):self._send(404,'not found','text/plain; charset=utf-8');return
        ext=os.path.splitext(full)[1].lower();ct={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'application/javascript; charset=utf-8'}.get(ext,'application/octet-stream')
        with open(full,'rb') as f:self._send(200,f.read(),ct)
    def log_message(self,fmt,*args):pass

if __name__=='__main__':
    DEMOS_STATE=load_state()
    try:
        with DEMO_LOCK:save_state_locked()
    except:pass
    print(f'Double Live em http://localhost:{PORT} • demos={len(DEMOS_STATE)} • strategies={len(STRATEGIES_STATE)}',flush=True)
    threading.Thread(target=live_worker,name='live-worker',daemon=True).start()
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
