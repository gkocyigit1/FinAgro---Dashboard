"""FinAgro Risk Dashboard v2"""
import os, json, math, datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

try:
    import pandas as pd
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable,"-m","pip","install","pandas","openpyxl","numpy"])
    import pandas as pd

PORT = int(os.environ.get("PORT", 5050))
BASE = "/app" if os.path.exists("/app") else os.path.dirname(os.path.abspath(__file__))
UPLOAD  = os.path.join(BASE, "uploaded.xlsx")
DFILE   = os.path.join(BASE, "latest_data.json")
NFILE   = os.path.join(BASE, "notes.json")
CEK_FILE  = os.path.join(BASE, "uploaded_cek.xlsx")
FON_FILE  = os.path.join(BASE, "uploaded_fon.xlsx")
CEK_DFILE = os.path.join(BASE, "cek_data.json")
FON_DFILE = os.path.join(BASE, "fon_data.json")

def clean(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    if isinstance(obj, dict): return {k: clean(v) for k,v in obj.items()}
    if isinstance(obj, list): return [clean(i) for i in obj]
    return obj

def load_notes():
    try:
        if os.path.exists(NFILE): return json.load(open(NFILE, encoding='utf-8'))
    except: pass
    return {}

def save_notes(n):
    json.dump(n, open(NFILE,'w',encoding='utf-8'), ensure_ascii=False)

def sf(v):
    try: return float(v) if pd.notna(v) else 0.0
    except: return 0.0

def si(v):
    try: return int(v) if pd.notna(v) else 0
    except: return 0

def ss(v):
    s = str(v) if pd.notna(v) else ''
    return s if s not in ('nan','None','NaT','') else '-'

def sd(v):
    try:
        if pd.notna(v): return str(v)[:10]
    except: pass
    return '-'

def mrow(r):
    try:
        return {
            'kesideci': ss(r.get('KEŞİDECİ ADI','')),
            'bayi':     ss(r.get('BAYİ','')),
            'banka':    ss(r.get('BANKA','')),
            'avukat':   ss(r.get('AVUKAT ADI','')),
            'vade':     sd(r.get('VADE TARİHİ')),
            'tahsil':   sd(r.get('TAHSİL TARİHİ')),
            'gun':      si(r.get('GUN_GECEN',0)),
            'senet':    sf(r.get('SENET TUTARI',0)),
            'odenen':   sf(r.get('ÖDENEN TUTAR',0)),
            'kalan':    sf(r.get('KALAN TUTAR',0)),
            'dosya_no': ss(r.get('DOSYA NO','')),
            'akibet':   ss(r.get('AKİBET','')),
        }
    except: return None

def yas_grp(df):
    bins   = [0,30,60,90,180,365,9999]
    labels = ['0-30','31-60','61-90','91-180','181-365','365+']
    df2 = df.copy()
    df2['YAS'] = pd.cut(df2['GUN_GECEN'].clip(lower=0), bins=bins, labels=labels, right=True)
    res = []
    for lbl in labels:
        g = df2[df2['YAS']==lbl]
        detay = [x for x in (mrow(r) for _,r in g.sort_values('KALAN TUTAR',ascending=False).head(100).iterrows()) if x]
        res.append({'grup':lbl,'sayi':int(len(g)),'tutar':float(g['KALAN TUTAR'].sum()),'detay':detay})
    return res

def parse_excel(fp):
    today = pd.Timestamp(datetime.date.today())

    # YASAL TAKİP
    yt = pd.read_excel(fp, sheet_name='YASAL TAKİP')
    for c in ['SENET TUTARI','KALAN TUTAR','ÖDENEN TUTAR']:
        if c in yt.columns: yt[c] = pd.to_numeric(yt[c], errors='coerce').fillna(0)
    yt['VADE TARİHİ']   = pd.to_datetime(yt.get('VADE TARİHİ'),   errors='coerce')
    yt['TAHSİL TARİHİ'] = pd.to_datetime(yt.get('TAHSİL TARİHİ'), errors='coerce')
    yt['GUN_GECEN']     = (today - yt['VADE TARİHİ']).dt.days.fillna(0)
    yt = yt[yt['KALAN TUTAR'] < 10_000_000].copy()

    # PROTESTO
    prt = pd.DataFrame()
    try:
        raw = pd.read_excel(fp, sheet_name='PROTESTO', header=None)
        hdr = 0
        for i,row in raw.iterrows():
            if row.astype(str).str.contains('BAYİ|KESİDECİ', case=False, na=False).any():
                hdr = i; break
        prt = pd.read_excel(fp, sheet_name='PROTESTO', header=hdr)
        prt.columns = [str(c).strip() for c in prt.columns]
        cmap = {}
        for c in prt.columns:
            cu = c.upper()
            if 'KESİDECİ' in cu: cmap[c]='KEŞİDECİ ADI'
            elif 'BAYİ' in cu:   cmap[c]='BAYİ'
            elif 'BANKA' in cu:  cmap[c]='BANKA'
            elif 'KALAN' in cu:  cmap[c]='KALAN TUTAR'
            elif 'ÖDENEN' in cu or 'ODENEN' in cu: cmap[c]='ÖDENEN TUTAR'
            elif 'SENET TUTARI' in cu or ('TUTAR' in cu and 'KALAN' not in cu and 'ÖDENEN' not in cu and 'ODENEN' not in cu): cmap[c]='SENET TUTARI'
            elif 'VADE' in cu:   cmap[c]='VADE TARİHİ'
            elif c=='"' or c=="'":   cmap[c]='VADE TARİHİ'
            elif 'AKİBET' in cu: cmap[c]='AKİBET'
        prt = prt.rename(columns=cmap)
        for c in ['KALAN TUTAR','ÖDENEN TUTAR','SENET TUTARI']:
            if c in prt.columns: prt[c] = pd.to_numeric(prt[c], errors='coerce').fillna(0)
        if 'VADE TARİHİ' in prt.columns:
            prt['VADE TARİHİ'] = pd.to_datetime(prt['VADE TARİHİ'], errors='coerce')
            prt['GUN_GECEN']   = (today - prt['VADE TARİHİ']).dt.days.fillna(0)
        else:
            prt['GUN_GECEN'] = 0
        if 'KALAN TUTAR' in prt.columns:
            prt = prt[prt['KALAN TUTAR'].fillna(0) > 0].copy()
    except Exception as e:
        print(f'Protesto hatasi: {e}')

    yt_senet   = float(yt['SENET TUTARI'].sum()) if 'SENET TUTARI' in yt.columns else 0.0
    yt_kalan   = float(yt['KALAN TUTAR'].sum())
    yt_odenen  = float(yt['ÖDENEN TUTAR'].sum()) if 'ÖDENEN TUTAR' in yt.columns else 0.0
    prt_kalan  = float(prt['KALAN TUTAR'].sum()) if len(prt) and 'KALAN TUTAR' in prt.columns else 0.0
    prt_odenen = float(prt['ÖDENEN TUTAR'].sum()) if len(prt) and 'ÖDENEN TUTAR' in prt.columns else 0.0
    prt_sayi   = int(len(prt))

    hic = yt[yt['ÖDENEN TUTAR'].fillna(0)==0] if 'ÖDENEN TUTAR' in yt.columns else pd.DataFrame()
    hic_sayi  = int(len(hic))
    hic_tutar = float(hic['KALAN TUTAR'].sum()) if len(hic) else 0.0
    hic_detay = [x for x in (mrow(r) for _,r in hic.sort_values('KALAN TUTAR',ascending=False).iterrows()) if x]

    yap_bas = 0
    try:
        yap = pd.read_excel(fp, sheet_name='YAPILANDIRMA SENETLER')
        yap_bas = int(yap[yap.apply(lambda r: r.astype(str).str.contains('ODENMEDI|ODE.*MEDI',case=False,na=False).any(),axis=1)].shape[0])
    except: pass

    r1 = 25 if yt_senet>0 and yt_kalan/yt_senet>0.8 else 18 if yt_senet>0 and yt_kalan/yt_senet>0.6 else 10
    r2 = 20 if hic_sayi>100 else 12 if hic_sayi>50 else 5
    cutoff = today - pd.Timedelta(days=90)
    son90  = int((yt['VADE TARİHİ']>=cutoff).sum()) if 'VADE TARİHİ' in yt.columns else 0
    r3 = 20 if len(yt)>0 and son90/len(yt)>0.3 else 12 if len(yt)>0 and son90/len(yt)>0.2 else 5
    r4 = 15 if yap_bas>=3 else 8 if yap_bas>=1 else 0
    tb, bor = '-', 0.0
    if 'BAYİ' in yt.columns:
        bg = yt.groupby('BAYİ')['KALAN TUTAR'].sum()
        if len(bg): tb=str(bg.idxmax()); bor=float(bg.max()/yt_kalan*100) if yt_kalan>0 else 0.0
    r5 = 10 if bor>10 else 5 if bor>5 else 2
    ti, bor2 = '-', 0.0
    if 'Senet Adresi' in yt.columns:
        yt['IL'] = yt['Senet Adresi'].astype(str).str.split('/').str[0].str.strip()
        ig = yt.groupby('IL')['KALAN TUTAR'].sum()
        if len(ig): ti=str(ig.idxmax()); bor2=float(ig.max()/yt_kalan*100) if yt_kalan>0 else 0.0
    r6 = 10 if bor2>30 else 5 if bor2>20 else 2
    risk = r1+r2+r3+r4+r5+r6

    yt3 = yt.copy()
    yt3['AY'] = yt3['VADE TARİHİ'].dt.to_period('M')
    aylik = []
    for _,r in yt3.groupby('AY').agg(tutar=('KALAN TUTAR','sum'),adet=('KALAN TUTAR','count')).reset_index().sort_values('AY').tail(12).iterrows():
        g = yt3[yt3['AY']==r['AY']]
        d = [x for x in (mrow(dr) for _,dr in g.sort_values('KALAN TUTAR',ascending=False).head(50).iterrows()) if x]
        aylik.append({'ay':str(r['AY']),'tutar':float(r['tutar']),'adet':int(r['adet']),'detay':d})

    bayi_risk = []
    if 'BAYİ' in yt.columns:
        for bayi,g in yt.groupby('BAYİ'):
            d=[x for x in (mrow(r) for _,r in g.sort_values('KALAN TUTAR',ascending=False).iterrows()) if x]
            bayi_risk.append({'bayi':str(bayi),'tutar':float(g['KALAN TUTAR'].sum()),'sayi':int(len(g)),'detay':d})
        bayi_risk.sort(key=lambda x:x['tutar'],reverse=True)

    banka_risk = []
    if 'BANKA' in yt.columns:
        bg2 = yt.groupby('BANKA')['KALAN TUTAR'].sum().sort_values(ascending=False).reset_index()
        banka_risk=[{'banka':str(r['BANKA']),'tutar':float(r['KALAN TUTAR'])} for _,r in bg2.iterrows() if r['KALAN TUTAR']>0]

    avukat_risk = []
    if 'AVUKAT ADI' in yt.columns:
        for av,g in yt.groupby('AVUKAT ADI'):
            d=[x for x in (mrow(r) for _,r in g.sort_values('KALAN TUTAR',ascending=False).iterrows()) if x]
            avukat_risk.append({'avukat':str(av),'sayi':int(len(g)),'tutar':float(g['KALAN TUTAR'].sum()),'detay':d})
        avukat_risk.sort(key=lambda x:x['tutar'],reverse=True)

    prt_detay = [x for x in (mrow(r) for _,r in prt.sort_values('KALAN TUTAR',ascending=False).iterrows()) if x] if len(prt) else []

    yt_avg  = round(float(yt['GUN_GECEN'].mean()),0)  if len(yt)  else 0.0
    prt_avg = round(float(prt['GUN_GECEN'].mean()),0) if len(prt) else 0.0
    toplam_avg = round(float(pd.concat([yt[['GUN_GECEN']], prt[['GUN_GECEN']] if len(prt) else pd.DataFrame(columns=['GUN_GECEN'])], ignore_index=True)['GUN_GECEN'].mean()),0) if (len(yt)+len(prt))>0 else 0.0

    now = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')

    result = {
        'kpis': {
            'yt_senet': yt_senet, 'yt_kalan': yt_kalan, 'yt_odenen': yt_odenen,
            'tahsilat_orani': round(yt_odenen/yt_senet*100,1) if yt_senet>0 else 0.0,
            'prt_kalan': prt_kalan, 'prt_odenen': prt_odenen, 'prt_sayi': prt_sayi,
            'hic_sayi': hic_sayi, 'hic_tutar': hic_tutar,
            'risk_skoru': risk,
            'risk_detay': {
                'kalan_oran':    {'puan':r1,'max':25,'aciklama':f'Kalan/Toplam: %{round(yt_kalan/yt_senet*100,1) if yt_senet>0 else 0}'},
                'hic_odeme':     {'puan':r2,'max':20,'aciklama':f'{hic_sayi} dosyada hic odeme yok'},
                'son90':         {'puan':r3,'max':20,'aciklama':f'Son 90 gunde {son90} yeni dosya'},
                'yap_basarisiz': {'puan':r4,'max':15,'aciklama':f'{yap_bas} yapilandirma basarisiz'},
                'bayi_yogun':    {'puan':r5,'max':10,'aciklama':f'En buyuk bayi {tb}: %{round(bor,1)}'},
                'bolge_yogun':   {'puan':r6,'max':10,'aciklama':f'En yogun il {ti}: %{round(bor2,1)}'},
            },
            'yap_bas': yap_bas,
            'top_bayi': tb, 'bayi_oran': round(bor,1),
            'yt_avg_gun': yt_avg, 'prt_avg_gun': prt_avg, 'toplam_avg_gun': toplam_avg,
        },
        'yt_yaslandirma':  yas_grp(yt),
        'prt_yaslandirma': yas_grp(prt) if len(prt) else [],
        'aylik_trend':     aylik,
        'bayi_risk':       bayi_risk,
        'banka_risk':      banka_risk,
        'avukat_risk':     avukat_risk,
        'hic_detay':       hic_detay,
        'prt_detay':       prt_detay,
        'son_guncelleme':  now,
        'filename':        os.path.basename(fp),
    }
    return clean(result)


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*a): pass

    def _res(self,code,ct,body):
        if isinstance(body,str): body=body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type',ct)
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Content-Length',len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type')
        self.end_headers()

    def do_GET(self):
        p = urlparse(self.path).path
        if p in ('/','index.html','/index.html'):
            try:
                html=open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'index.html'),'rb').read()
                self._res(200,'text/html; charset=utf-8',html)
            except Exception as e:
                self._res(500,'text/plain',f'Hata: {e}')
        elif p=='/api/data':
            if os.path.exists(DFILE):
                data=json.load(open(DFILE,encoding='utf-8'))
                data['notes']=load_notes()
                self._res(200,'application/json',json.dumps(data,ensure_ascii=False))
            else:
                self._res(200,'application/json',json.dumps({'error':'Excel henuz yuklenmedi.'}))
        elif p=='/api/cek':
            if os.path.exists(CEK_DFILE):
                self._res(200,'application/json',open(CEK_DFILE,encoding='utf-8').read())
            else:
                self._res(200,'application/json',json.dumps({'error':'Cek/Senet Excel henuz yuklenmedi.'}))
        elif p=='/api/fon':
            if os.path.exists(FON_DFILE):
                self._res(200,'application/json',open(FON_DFILE,encoding='utf-8').read())
            else:
                self._res(200,'application/json',json.dumps({'error':'Fon Excel henuz yuklenmedi.'}))
        else:
            self._res(404,'text/plain','Not found')

    def do_POST(self):
        p = urlparse(self.path).path
        if p=='/api/upload':
            try:
                ct=self.headers.get('Content-Type','')
                ln=int(self.headers.get('Content-Length',0))
                body=self.rfile.read(ln)
                bnd=ct.split('boundary=')[-1].encode()
                parts=body.split(b'--'+bnd)
                fd=None
                for part in parts:
                    if b'filename=' in part:
                        fd=part.split(b'\r\n\r\n',1)[1].rsplit(b'\r\n',1)[0]; break
                if not fd: raise ValueError('Dosya bulunamadi')
                open(UPLOAD,'wb').write(fd)
                data=parse_excel(UPLOAD)
                json.dump(data,open(DFILE,'w',encoding='utf-8'),ensure_ascii=False)
                data['notes']=load_notes()
                self._res(200,'application/json',json.dumps(data,ensure_ascii=False))
            except Exception as e:
                self._res(500,'application/json',json.dumps({'error':str(e)}))
        elif p=='/api/upload/cek':
            try:
                ct=self.headers.get('Content-Type','')
                ln=int(self.headers.get('Content-Length',0))
                body=self.rfile.read(ln)
                bnd=ct.split('boundary=')[-1].encode()
                parts=body.split(b'--'+bnd)
                fd=None
                for part in parts:
                    if b'filename=' in part:
                        fd=part.split(b'\r\n\r\n',1)[1].rsplit(b'\r\n',1)[0]; break
                if not fd: raise ValueError('Dosya bulunamadi')
                open(CEK_FILE,'wb').write(fd)
                data=parse_cek_senet(CEK_FILE)
                json.dump(data,open(CEK_DFILE,'w',encoding='utf-8'),ensure_ascii=False)
                self._res(200,'application/json',json.dumps(data,ensure_ascii=False))
            except Exception as e:
                self._res(500,'application/json',json.dumps({'error':str(e)}))

        elif p=='/api/upload/fon':
            try:
                ct=self.headers.get('Content-Type','')
                ln=int(self.headers.get('Content-Length',0))
                body=self.rfile.read(ln)
                bnd=ct.split('boundary=')[-1].encode()
                parts=body.split(b'--'+bnd)
                fd=None
                for part in parts:
                    if b'filename=' in part:
                        fd=part.split(b'\r\n\r\n',1)[1].rsplit(b'\r\n',1)[0]; break
                if not fd: raise ValueError('Dosya bulunamadi')
                open(FON_FILE,'wb').write(fd)
                data=parse_fon(FON_FILE)
                json.dump(data,open(FON_DFILE,'w',encoding='utf-8'),ensure_ascii=False)
                self._res(200,'application/json',json.dumps(data,ensure_ascii=False))
            except Exception as e:
                self._res(500,'application/json',json.dumps({'error':str(e)}))

        elif p=='/api/notes/save':
            try:
                ln=int(self.headers.get('Content-Length',0))
                body=self.rfile.read(ln)
                nd=json.loads(body.decode('utf-8'))
                notes=load_notes()
                k=nd.get('kesideci','')
                if k:
                    ex=notes.get(k,{})
                    notes[k]={
                        'ops_not':     nd.get('ops_not',     ex.get('ops_not','')),
                        'avukat_not':  nd.get('avukat_not',  ex.get('avukat_not','')),
                        'durum':       nd.get('durum',       ex.get('durum','')),
                        'avukat':      nd.get('avukat',      ex.get('avukat','')),
                        'odeme_sozu':  nd.get('odeme_sozu',  ex.get('odeme_sozu','')),
                        'son_gorusme': nd.get('son_gorusme', ex.get('son_gorusme','')),
                        'odeme_tarihi':nd.get('odeme_tarihi',ex.get('odeme_tarihi','')),
                        'odeme_tutari':nd.get('odeme_tutari',ex.get('odeme_tutari','')),
                        'guncelleme':  datetime.datetime.now().strftime('%d.%m.%Y %H:%M'),
                    }
                    save_notes(notes)
                self._res(200,'application/json',json.dumps({'ok':True}))
            except Exception as e:
                self._res(500,'application/json',json.dumps({'error':str(e)}))
        else:
            self._res(404,'text/plain','Not found')


if __name__=='__main__':
    if os.path.exists(UPLOAD):
        try:
            data=parse_excel(UPLOAD)
            json.dump(data,open(DFILE,'w',encoding='utf-8'),ensure_ascii=False)
            print('Varsayilan Excel yuklendi.')
        except Exception as e:
            print(f'Excel hatasi: {e}')
    else:
        print('Varsayilan Excel bulunamadi.')
    print(f'Server baslatildi: http://0.0.0.0:{PORT}')
    HTTPServer(('0.0.0.0',PORT),Handler).serve_forever()

# ── EK PARSE FONKSİYONLARI ─────────────────────────────────────────────────


def _ss(v):
    try:
        s = str(v) if pd.notna(v) else ''
        return s if s not in ('nan','None','NaT','') else '-'
    except: return '-'

def _sd(v):
    try:
        if pd.notna(v): return str(v)[:10]
    except: pass
    return '-'

def _sf(v):
    try: return float(v) if pd.notna(v) else 0.0
    except: return 0.0


def parse_cek_senet(fp):
    now = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
    result = {'son_guncelleme': now, 'filename': os.path.basename(fp)}

    def _parse_sheet(sheet_name, tutar_key='TUTAR-TL', extra_cols=None):
        df = pd.read_excel(fp, sheet_name=sheet_name)
        df.columns = [str(c).strip() for c in df.columns]
        tutar_col = next((c for c in df.columns if tutar_key in c.upper() or ('TUTAR' in c.upper() and 'FATURA' not in c.upper())), None)
        vade_col  = next((c for c in df.columns if 'VADE' in c.upper()), None)
        banka_col = next((c for c in df.columns if 'BANKA' in c.upper() and 'ŞUBE' not in c.upper()), None)
        if tutar_col: df['_T'] = pd.to_numeric(df[tutar_col], errors='coerce').fillna(0)
        else: df['_T'] = 0
        rows = []
        for _, r in df.iterrows():
            bayi = _ss(r.get('BAYİ',''))
            if bayi == '-': continue
            row = {
                'no':       _ss(r.get('NO', r.get('no',''))),
                'bayi':     bayi,
                'adres':    _ss(r.get('Senet Adresi','')),
                'banka':    _ss(r.get(banka_col,'')) if banka_col else '-',
                'vade':     _sd(r.get(vade_col)) if vade_col else '-',
                'kesideci': _ss(r.get('KEŞİDECİ ADI','')),
                'tutar':    _sf(r['_T']),
            }
            if extra_cols:
                for src, dst in extra_cols.items():
                    row[dst] = _ss(r.get(src,''))
            rows.append(row)
        toplam = sum(r['tutar'] for r in rows)
        bdk = {}
        for r in rows:
            bdk[r['banka']] = bdk.get(r['banka'], 0) + r['tutar']
        banka_list = sorted([{'banka':k,'tutar':v} for k,v in bdk.items()], key=lambda x:x['tutar'], reverse=True)
        return {'rows': rows, 'toplam': toplam, 'sayi': len(rows), 'banka_dagilim': banka_list}

    try:
        result['guncel'] = _parse_sheet('Güncel Çek&Senet', extra_cols={
            'Bayi Faturası Ödeme Bilgisi': 'fatura_durum',
            'KISMI ÖDEME': 'kismi',
            'YAPILANDIRILAN SENET NO': 'yapilandirma',
        })
    except Exception as e:
        result['guncel'] = {'rows':[], 'toplam':0, 'sayi':0, 'error': str(e)}

    try:
        result['sonuclanan'] = _parse_sheet('Sonuçlanan Çek&Senet', extra_cols={
            'TAHSİL TARİHİ': 'tahsil',
            'PROTESTO DURUMU': 'protesto',
            'AÇIKLAMA': 'aciklama',
        })
    except Exception as e:
        result['sonuclanan'] = {'rows':[], 'toplam':0, 'sayi':0, 'error': str(e)}

    try:
        result['teminat'] = _parse_sheet('TEMİNAT ÇEKLERİ', extra_cols={'AÇIKLAMA': 'aciklama'})
    except Exception as e:
        result['teminat'] = {'rows':[], 'toplam':0, 'sayi':0, 'error': str(e)}

    return clean(result)


def parse_fon(fp):
    now = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')
    result = {'son_guncelleme': now, 'filename': os.path.basename(fp)}

    # LİMİT-RİSK
    try:
        raw = pd.read_excel(fp, sheet_name='LİMİT-RİSK', header=None)
        tarih = _sd(raw.iloc[3, 1]) if pd.notna(raw.iloc[3, 1]) else '-'
        rows = []
        for i in range(5, 20):
            if i >= len(raw): break
            r = raw.iloc[i]
            banka = _ss(r.iloc[1])
            if banka == '-': continue
            rows.append({
                'banka': banka, 'tl_bakiye': _sf(r.iloc[2]),
                'limit': _sf(r.iloc[3]), 'nakit_kredi': _sf(r.iloc[4]),
                'gayri_nakit': _sf(r.iloc[5]), 'limit_boslugu': _sf(r.iloc[6]),
                'cek_senet': _sf(r.iloc[8]), 'fazla_marj': _ss(r.iloc[9]),
                'ana_para_faiz': _sf(r.iloc[11]),
            })
        toplam_l = {}
        for i in range(14, 30):
            if i >= len(raw): break
            if 'TOPLAM' in str(raw.iloc[i, 1]).upper():
                r = raw.iloc[i]
                toplam_l = {'limit': _sf(r.iloc[3]), 'nakit_kredi': _sf(r.iloc[4]), 'cek_senet': _sf(r.iloc[8])}
                break
        result['limit_risk'] = {'rows': rows, 'toplam': toplam_l, 'tarih': tarih}
    except Exception as e:
        result['limit_risk'] = {'rows': [], 'error': str(e)}

    # BANKA BAKİYELERİ
    try:
        raw2 = pd.read_excel(fp, sheet_name='BANKA BAKİYELERİ', header=None)
        tarih2 = _sd(raw2.iloc[0, 7]) if pd.notna(raw2.iloc[0, 7]) else '-'
        rows2, toplam_bak = [], 0.0
        for i in range(7, 35):
            if i >= len(raw2): break
            r = raw2.iloc[i]
            banka = _ss(r.iloc[2])
            if banka == '-': continue
            if 'TOPLAM' in banka.upper():
                toplam_bak = _sf(r.iloc[7]); break
            rows2.append({
                'banka': banka, 'sube': _ss(r.iloc[3]),
                'devir': _sf(r.iloc[4]), 'giris': _sf(r.iloc[5]),
                'cikis': _sf(r.iloc[6]), 'bakiye': _sf(r.iloc[7]),
                'not': _ss(r.iloc[8]),
            })
        result['banka_bakiyeleri'] = {'rows': rows2, 'toplam': toplam_bak, 'tarih': tarih2}
    except Exception as e:
        result['banka_bakiyeleri'] = {'rows': [], 'error': str(e)}

    # BANKA MASRAF
    try:
        raw3 = pd.read_excel(fp, sheet_name='BANKA MASRAF', header=None)
        rows3 = []
        for i in range(2, 15):
            if i >= len(raw3): break
            r = raw3.iloc[i]
            banka = _ss(r.iloc[0])
            if banka == '-': continue
            rows3.append({
                'banka': banka, 'eft': _ss(r.iloc[1]), 'havale': _ss(r.iloc[2]),
                'cek_takas': _ss(r.iloc[3]), 'cek_iade': _ss(r.iloc[4]),
                'senet': _ss(r.iloc[5]), 'senet_iade': _ss(r.iloc[6]), 'dosya': _ss(r.iloc[7]),
            })
        result['banka_masraf'] = {'rows': rows3}
    except Exception as e:
        result['banka_masraf'] = {'rows': [], 'error': str(e)}

    # TEMİNAT MEKTUPLARI
    try:
        raw4 = pd.read_excel(fp, sheet_name='TEMİNAT MEKTUPLARI', header=None)
        rows4 = []
        for i in range(1, len(raw4)):
            r = raw4.iloc[i]
            ad = _ss(r.iloc[1])
            if ad == '-': continue
            rows4.append({
                'no': _ss(r.iloc[0]), 'ad': ad, 'tarih': _sd(r.iloc[2]),
                'banka': _ss(r.iloc[3]), 'referans': _ss(r.iloc[4]),
                'numara': _ss(r.iloc[5]), 'tutar': _sf(r.iloc[6]), 'komisyon': _sf(r.iloc[7]),
            })
        result['teminat_mektuplari'] = {'rows': rows4, 'toplam': sum(r['tutar'] for r in rows4)}
    except Exception as e:
        result['teminat_mektuplari'] = {'rows': [], 'error': str(e)}

    return clean(result)
