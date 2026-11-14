from PIL import Image
S=2174/1331
im=Image.open('/home/user/AVD/work/pz1/screenshots/43_ui_jobhistory_counters_mr_daily_ohlc.png').convert('RGB')
def X(v): return round(v*S)
def band(y0,y1,segs):
    return [im.crop((X(a),X(y0),X(b),X(y1))) for a,b in segs]
# columns: name part (297..500), numbers (871..1318)
NA,NB=297,500; MA,MB=871,1318
def row(yc):
    h=19.6; y0,y1=yc-h,yc+h
    a=im.crop((X(NA),X(y0),X(NB),X(y1))); b=im.crop((X(MA-3),X(y0),X(MB+2),X(y1)))
    r=Image.new('RGB',(a.width+b.width,a.height)); r.paste(a,(0,0)); r.paste(b,(a.width,0)); return r
def header(yc):
    y0,y1=yc-23,yc+23
    a=im.crop((X(NA),X(y0),X(NB),X(y1))); b=im.crop((X(MA-3),X(y0),X(MB+2),X(y1)))
    t=im.crop((X(556),X(yc-11),X(612),X(yc+11)))
    a.paste(t,((a.width-t.width)//2,(a.height-t.height)//2))
    r=Image.new('RGB',(a.width+b.width,a.height)); r.paste(a,(0,0)); r.paste(b,(a.width,0)); return r
parts=[header(543),row(586),header(1024),row(1302),row(1693),row(1850),row(1889)]
W=parts[0].width; H=sum(p.height for p in parts)
out=Image.new('RGB',(W,H),(255,255,255)); y=0
for p in parts: out.paste(p,(0,y)); y+=p.height
out.save(FIG/'ui_jh_counters.png'); print(out.size)
