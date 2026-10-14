"""Render the checked Archify topology as a readable README PNG/GIF.

The graph, compiled polylines, labels and components come from architecture-en.json
and the frozen architecture-en.html. Stage highlights explain ordering only.
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import hashlib, json, math, re, xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
SPEC = json.loads((OUT/'architecture-en.json').read_text())
SVG = ET.fromstring(re.search(r'<svg[\s\S]*?</svg>', (OUT/'architecture-en.html').read_text()).group())
W,H,S = 1320,1080,2
OX,OY = 40,132
FONT='/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc'
BOLD='/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc'
MONO='/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf'
BG=(7,17,33); PANEL=(12,28,49); BOX=(15,35,60); BORDER=(39,67,101)
INK=(230,242,255); MUTED=(154,181,210); LINE=(69,102,139)
CYAN=(69,221,238); BLUE=(93,154,255); PURPLE=(178,151,243)
COLORS=[CYAN,CYAN,BLUE,BLUE,CYAN,PURPLE]
EDGES={}
for e in SVG.iter('path'):
 if 'data-edge-id' in e.attrib:
  EDGES[e.attrib['data-edge-id']]={'points':[tuple(map(float,p.split(','))) for p in e.attrib['data-composition-points'].split(';')], 'label':e.attrib['data-edge-label'],'from':e.attrib['data-edge-from'],'to':e.attrib['data-edge-to']}
LABELS={}
for e in SVG.iter('g'):
 if 'data-edge-id' in e.attrib:
  t=e.find('text')
  if t is not None:LABELS[e.attrib['data-edge-id']]=(float(t.attrib['x']),float(t.attrib['y'])-4)
NODES={n['id']:n for n in SPEC['components']}
PHASES=[
 {'title':'01  Market data enters the shared pipeline','desc':'data-collect captures market feeds; data-pipeline prepares inputs for historical replay.','edges':['market-data'],'nodes':['collect','pipeline']},
 {'title':'02  Public observations and hidden matching data diverge','desc':'Public observations reach shared-run through SIM; hidden matching data is visible only to SIM.','edges':['hidden-input','prepared-data'],'nodes':['pipeline','shared','sim']},
 {'title':'03  The strategy reads market and account state','desc':'shared-run combines visible observations and account state for baseline-mm to make decisions.','edges':['feedback','observation'],'nodes':['ledger','shared','strategy']},
 {'title':'04  Trading intents become coordinated submissions','desc':'shared-run coordinates fund reservations, then submits orders to SIM through intent-bridge.','edges':['decision','submit','orders'],'nodes':['strategy','shared','intent','sim']},
 {'title':'05  Execution feedback updates the next decision','desc':'Execution feedback is normalized; shared-run consumes the events and applies them to ledger.','edges':['execution','normalized','feedback'],'nodes':['sim','noe','ledger','shared']},
 {'title':'06  Reports support research; training remains separate','desc':'backtest produces reports for visualization; model-train is a separate research branch; training is paused.','edges':['report-output','training-input'],'nodes':['reports','visual','train']},
]

def font(size,bold=False,mono=False):return ImageFont.truetype(MONO if mono else BOLD if bold else FONT,round(size*S))
def point(p):return (round((p[0]+OX)*S),round((p[1]+OY)*S))
def rect(x,y,w,h):return (round(x*S),round(y*S),round((x+w)*S),round((y+h)*S))
def tx(d,x,y,text,size=16,fill=INK,bold=False,mono=False,anchor='mm'):
 d.text((round(x*S),round(y*S)),text,font=font(size,bold,mono),fill=fill,anchor=anchor)
def line(d,ps,fill,width=2):d.line([point(p) for p in ps],fill=fill,width=round(width*S),joint='curve')
def arrow(d,ps,fill,width=2):
 line(d,ps,fill,width)
 a,b=ps[-2],ps[-1];ang=math.atan2(b[1]-a[1],b[0]-a[0]);r=8
 end=point(b);a1=point((b[0]-r*math.cos(ang)+r*.52*math.sin(ang),b[1]-r*math.sin(ang)-r*.52*math.cos(ang)));a2=point((b[0]-r*math.cos(ang)-r*.52*math.sin(ang),b[1]-r*math.sin(ang)+r*.52*math.cos(ang)))
 d.polygon([end,a1,a2],fill=fill)
def base(stage=None):
 im=Image.new('RGB',(W*S,H*S),BG);d=ImageDraw.Draw(im)
 for x in range(40,W,40):d.line([(x*S,142*S),(x*S,883*S)],fill=(10,24,43),width=1)
 for y in range(142,884,40):d.line([(40*S,y*S),((W-40)*S,y*S)],fill=(10,24,43),width=1)
 tx(d,48,31,'btc-updown-marketmaker',15,BLUE,mono=True,anchor='lm')
 tx(d,48,74,'From market data to strategy decisions',33,bold=True,anchor='lm')
 tx(d,48,112,'Market data  →  Decisions  →  Execution  →  Account feedback',17,MUTED,anchor='lm')
 tx(d,W-48,45,'Illustrative flow · not a live monitor',13,MUTED,anchor='rm')
 tx(d,W-48,77,'Phase highlights do not show elapsed time',12,MUTED,anchor='rm')
 # The host boundary is the exact compiled backtest region.
 d.rounded_rectangle(rect(460*.89-35+OX,40+OY,(1269.6-460)*.89+70,617),radius=14*S,fill=(10,25,44),outline=(36,71,108),width=2*S)
 tx(d,(460*.89-19)+OX,57+OY,'backtest · historical run host and reports',16,BLUE,bold=True,anchor='lm')
 active=set(PHASES[stage]['edges']) if stage is not None else set()
 color=COLORS[stage] if stage is not None else CYAN
 for k,e in EDGES.items():arrow(d,e['points'],color if k in active else LINE,2.5 if k in active else 1.7)
 for k,e in EDGES.items():
  x,y=LABELS[k];label=e['label'];fs=14
  if k in ('normalized','execution'):
   x=(e['points'][0][0]+e['points'][-1][0])/2;y=533
   tx(d,x+OX,y+OY,'Posting' if k=='normalized' else 'Feedback',13,color if k in active else MUTED)
   continue
  if k=='prepared-data':label='Public observations (via SIM)';fs=13
  if k=='hidden-input':x,y=590*.89,695;label='Hidden matching data · SIM only'
  if k=='observation':x,y=655*.89,239
  if k=='decision':x,y=485*.89,214
  bbox=d.textbbox((0,0),label,font=font(fs));tw=(bbox[2]-bbox[0])/S
  d.rounded_rectangle(rect(x+OX-tw/2-7,y+OY-11,tw+14,22),radius=4*S,fill=(10,25,44) if x>425 and y<657 else BG)
  tx(d,x+OX,y+OY,label,fs,color if k in active else MUTED)
 active_nodes=set(PHASES[stage]['nodes']) if stage is not None else set()
 for k,n in NODES.items():
  x,y=n['pos'];w,h=n['size'];on=k in active_nodes
  fill=(14,46,72) if on else BOX
  d.rounded_rectangle(rect(x+OX,y+OY,w,h),radius=10*S,fill=fill,outline=color if on else BORDER,width=(3 if on else 2)*S)
  d.rounded_rectangle(rect(x+OX+12,y+OY+13,4,h-26),radius=2*S,fill=color if on else (45,86,132))
  fs=13 if k=='noe' else 18
  tx(d,x+OX+w/2,y+OY+30,n['label'],fs,INK,mono=(k!='reports'))
  sub=n['sublabel'];fs=14 if k in ('sim','train','shared','visual') else 15
  words=sub.split();lines=[];row=''
  for word in words:
   candidate=(row+' '+word).strip()
   if row and d.textlength(candidate,font=font(fs)) > (w-44)*S:
    lines.append(row);row=word
   else:row=candidate
  if row:lines.append(row)
  for j,t in enumerate(lines):
   tx(d,x+OX+w/2,y+OY+59+(j-(len(lines)-1)/2)*17,t,fs,color if on else MUTED)
 # Central open space gives the run-coordination responsibility a visible explanation.
 tx(d,876*.89+OX,434+OY,'shared-run coordinates execution and posting',14,MUTED)
 tx(d,876*.89+OX,463+OY,'Account updates feed the next decision',14,MUTED)
 # Reports are a host output, so this is a caption rather than an invented trade edge.
 tx(d,865*.89+OX,181+OY,'↑ Results aggregated by the host',14,PURPLE if stage==5 else MUTED)
 d.rounded_rectangle(rect(48,882,W-96,55),radius=9*S,fill=PANEL,outline=BORDER,width=S)
 tx(d,68,909,'contracts',17,BLUE,mono=True,anchor='lm')
 tx(d,191,909,'Shared types and semantics',14,INK,anchor='lm')
 tx(d,W-68,909,'Separate training branch · Full PAPER / LIVE hosts planned',13,MUTED,anchor='rm')
 # A separate caption band remains legible even at README display widths.
 d.rounded_rectangle(rect(48,957,W-96,85),radius=10*S,fill=PANEL,outline=BORDER,width=S)
 if stage is None:
  tx(d,68,981,'Strategies use public observations; hidden data stays inside SIM.',20,INK,bold=True,anchor='lm')
  tx(d,68,1016,'shared-run applies normalized execution events to ledger. Arrows show business relationships.',16,MUTED,anchor='lm')
 else:
  tx(d,68,981,PHASES[stage]['title'],21,color,bold=True,anchor='lm')
  tx(d,68,1016,PHASES[stage]['desc'],16,INK,anchor='lm')
 for j in range(6):
  x=W-242+j*28
  d.rounded_rectangle(rect(x,978,17,5),radius=2*S,fill=COLORS[j] if stage==j else BORDER)
 return im.resize((W,H),Image.Resampling.LANCZOS)

def at(ps,f):
 lengths=[math.dist(a,b) for a,b in zip(ps,ps[1:])];target=sum(lengths)*max(0,min(1,f))
 for a,b,l in zip(ps,ps[1:],lengths):
  if target<=l:return (a[0]+(b[0]-a[0])*target/l+OX,a[1]+(b[1]-a[1])*target/l+OY)
  target-=l
 return (ps[-1][0]+OX,ps[-1][1]+OY)

static=base();static.save(OUT/'architecture-static-en.png')
bases=[base(i) for i in range(6)]
# One shared palette prevents frame-to-frame colour noise and preserves readable type.
swatch=Image.new('RGB',(W,H*7));swatch.paste(static,(0,0))
for i,b in enumerate(bases):swatch.paste(b,(0,H*(i+1)))
palette=swatch.quantize(colors=192,method=Image.Quantize.MEDIANCUT)
frames=[];durations=[]
for stage in range(6):
 print(f'Phase {stage+1}/6: rendering animation frames',flush=True)
 es=PHASES[stage]['edges']
 for j in range(36):
  im=bases[stage].copy();d=ImageDraw.Draw(im)
  progress=j/36
  # All motion remains on the checked directed relationship polylines.
  edge_index=min(len(es)-1,int(progress*len(es)));frac=progress*len(es)-edge_index
  for offset,r,c in ((-.035,5,tuple(int(x*.45) for x in COLORS[stage])),(0,4,COLORS[stage])):
   x,y=at(EDGES[es[edge_index]]['points'],max(0,frac+offset));d.ellipse((x-r,y-r,x+r,y+r),fill=c)
  frame=im.quantize(palette=palette,dither=Image.Dither.NONE)
  frames.append(frame);durations.append(80 if j%3 else 90)
frames[0].save(OUT/'architecture-flow-en.gif',save_all=True,append_images=frames[1:],duration=durations,loop=0,optimize=True,disposal=1)
with Image.open(OUT/'architecture-flow-en.gif') as gif:
 times=[]
 for i in range(gif.n_frames):
  gif.seek(i);times.append(gif.info['duration'])
 metadata={'width':gif.width,'height':gif.height,'frames':gif.n_frames,'duration_ms':sum(times),'loop':gif.info.get('loop'),'bytes':(OUT/'architecture-flow-en.gif').stat().st_size,'sha256':hashlib.sha256((OUT/'architecture-flow-en.gif').read_bytes()).hexdigest(),'specification_sha256':hashlib.sha256((OUT/'architecture-en.json').read_bytes()).hexdigest(),'artifact_sha256':hashlib.sha256((OUT/'architecture-en.html').read_bytes()).hexdigest(),'method':'Pillow rendering of the checked Archify graph and compiled polylines; six illustrative phases, not measured runtime timings.'}
print(json.dumps(metadata,ensure_ascii=False),flush=True)
