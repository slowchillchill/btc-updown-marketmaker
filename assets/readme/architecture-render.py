"""Render the checked Archify topology as a readable README PNG/GIF.

The graph, compiled polylines, labels and components come from architecture-v2.json
and the frozen architecture.html. Stage highlights explain ordering only.
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import hashlib, json, math, re, xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
SPEC = json.loads((OUT/'architecture-v2.json').read_text())
SVG = ET.fromstring(re.search(r'<svg[\s\S]*?</svg>', (OUT/'architecture.html').read_text()).group())
W,H,S = 1320,1220,2
OX,OY = 40,272
FONT='/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc'
BOLD='/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc'
MONO='/usr/share/fonts/truetype/noto/NotoSansMono-Regular.ttf'
# PMM VI 0.4 implementation values, confirmed for this README visual.
BG=(255,255,255); PANEL=BG; BOX=BG; BORDER=(220,225,229)
INK=(23,27,32); MUTED=(83,96,108); LINE=(57,117,138)
BLUE=(13,65,162); ACTIVE=(220,235,231); NOTICE=(163,79,69)
COLORS=[BLUE]*6
# Reproduce style.css's visible window, not the noisy alpha bounding box.
LOGO_SOURCE=OUT.parents[1]/'visualization/ui/public/pmm-cobalt.png'
with Image.open(LOGO_SOURCE) as original:
 scale=original.width/232.66
 LOGO=original.crop((36.33*scale,32.13*scale,(36.33+160)*scale,(32.13+52.07)*scale))
LOGO=LOGO.resize((160*S,round(52.07*S)),Image.Resampling.LANCZOS)
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
 {'title':'01  行情进入公共数据流水线','desc':'data-collect 采集多源行情，data-pipeline 准备历史回放输入。','edges':['market-data'],'nodes':['collect','pipeline']},
 {'title':'02  公开观察与隐藏撮合材料分流','desc':'公开观察经 SIM 驱动交给运行层；隐藏撮合材料只交给 SIM。','edges':['hidden-input','prepared-data'],'nodes':['pipeline','shared','sim']},
 {'title':'03  策略读取行情与账户','desc':'shared-run 组合可见观察与账户信息，交给 baseline-mm 决策。','edges':['feedback','observation'],'nodes':['ledger','shared','strategy']},
 {'title':'04  策略返回意图，运行层协调提交','desc':'shared-run 协调资金预留，再通过 intent-bridge 向 SIM 提交订单。','edges':['decision','submit','orders'],'nodes':['strategy','shared','intent','sim']},
 {'title':'05  执行反馈入账，形成下一次决策输入','desc':'反馈先规范化，再由 shared-run 领取并应用到 ledger，更新账户。','edges':['execution','normalized','feedback'],'nodes':['sim','noe','ledger','shared']},
 {'title':'06  回测报告用于研究，训练保持独立分支','desc':'backtest 输出报告供 visualization 读取；model-train 的历史验证保持独立分支，当前训练暂停。','edges':['report-output','training-input'],'nodes':['reports','visual','train']},
]

def font(size,bold=False,mono=False):return ImageFont.truetype(MONO if mono else BOLD if bold else FONT,round(size*S),index=2 if FONT.endswith(".ttc") and not mono else 0)
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
 # The original logo window is 160×52.07; 26.04px clearance surrounds it.
 im.paste(LOGO,(74*S,64*S),LOGO)
 d.text((280*S,90*S),'Prediction Market Maker',font=ImageFont.truetype('/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf',30*S),fill=BLUE,anchor='lm')
 d.line([(48*S,146*S),((W-48)*S,146*S)],fill=BORDER,width=S)
 tx(d,48,171,'btc-updown-marketmaker',15,BLUE,mono=True,anchor='lm')
 tx(d,48,214,'从市场数据到策略决策',33,bold=True,anchor='lm')
 tx(d,48,252,'行情  →  决策  →  执行  →  账户反馈',17,MUTED,anchor='lm')
 tx(d,W-48,185,'流程示意 · 非实时监控',15,NOTICE,anchor='rm')
 tx(d,W-48,217,'阶段高亮不代表实际耗时',14,MUTED,anchor='rm')
 # The host boundary is the exact compiled backtest region.
 d.rounded_rectangle(rect(460*.89-35+OX,40+OY,(1269.6-460)*.89+70,617),radius=14*S,fill=BG,outline=BORDER,width=2*S)
 tx(d,(460*.89-19)+OX,57+OY,'backtest · 历史回测宿主与报告',16,BLUE,bold=True,anchor='lm')
 active=set(PHASES[stage]['edges']) if stage is not None else set()
 color=COLORS[stage] if stage is not None else BLUE
 for k,e in EDGES.items():arrow(d,e['points'],color if k in active else LINE if k in ('hidden-input','training-input','report-output') else BLUE,2.5 if k in active else 1.7)
 for k,e in EDGES.items():
  x,y=LABELS[k];label=e['label'];fs=15
  if k in ('normalized','execution'): # a two-line caption fits the short inter-module gap
   text1,text2=(('协调','入账') if k=='normalized' else ('执行','反馈'));x=(e['points'][0][0]+e['points'][-1][0])/2;y=e['points'][0][1]-28
   for dy,t in ((-9,text1),(9,text2)):
    tx(d,x+OX,y+OY+dy,t,fs,color if k in active else MUTED)
   continue
  if k=='prepared-data':label='公开观察（经 SIM）';fs=13
  if k=='hidden-input':x,y=590*.89,695;label='隐藏撮合材料 · 仅 SIM 可见'
  if k=='observation':x,y=655*.89,239
  if k=='decision':x,y=485*.89,214
  bbox=d.textbbox((0,0),label,font=font(fs));tw=(bbox[2]-bbox[0])/S
  d.rounded_rectangle(rect(x+OX-tw/2-7,y+OY-11,tw+14,22),radius=4*S,fill=BG)
  tx(d,x+OX,y+OY,label,fs,color if k in active else MUTED)
 active_nodes=set(PHASES[stage]['nodes']) if stage is not None else set()
 for k,n in NODES.items():
  x,y=n['pos'];w,h=n['size'];on=k in active_nodes
  fill=ACTIVE if on else BOX
  d.rounded_rectangle(rect(x+OX,y+OY,w,h),radius=10*S,fill=fill,outline=color if on else BORDER,width=(3 if on else 2)*S)
  d.rounded_rectangle(rect(x+OX+12,y+OY+13,4,h-26),radius=2*S,fill=color if on else BORDER)
  fs=13 if k=='noe' else 18
  tx(d,x+OX+w/2,y+OY+30,n['label'],fs,INK,mono=(k!='reports'))
  sub=n['sublabel'];fs=14 if k in ('sim','train','shared','visual') else 15
  tx(d,x+OX+w/2,y+OY+59,sub,fs,color if on else MUTED)
 # Central open space gives the run-coordination responsibility a visible explanation.
 tx(d,876*.89+OX,434+OY,'shared-run 协调执行与入账',16,MUTED)
 tx(d,876*.89+OX,463+OY,'每次账户更新进入下一轮决策',14,MUTED)
 # Reports are a host output, so this is a caption rather than an invented trade edge.
 tx(d,865*.89+OX,181+OY,'↑ 宿主汇总输出',14,BLUE if stage==5 else MUTED)
 d.rounded_rectangle(rect(48,1022,W-96,55),radius=9*S,fill=PANEL,outline=BORDER,width=S)
 tx(d,68,1049,'contracts',17,BLUE,mono=True,anchor='lm')
 tx(d,191,1049,'公共类型与跨模块语义',16,INK,anchor='lm')
 tx(d,W-68,1049,'模型训练为独立研究分支 · 完整 PAPER / LIVE 宿主规划中',14,MUTED,anchor='rm')
 # A separate caption band remains legible even at README display widths.
 d.rounded_rectangle(rect(48,1097,W-96,85),radius=10*S,fill=PANEL,outline=BORDER,width=S)
 if stage is None:
  tx(d,68,1121,'公开观察供策略决策，隐藏材料仅用于模拟撮合。',20,INK,bold=True,anchor='lm')
  tx(d,68,1156,'规范化执行事件由 shared-run 协调入账；图中箭头表达业务关系。',16,MUTED,anchor='lm')
 else:
  tx(d,68,1121,PHASES[stage]['title'],21,color,bold=True,anchor='lm')
  tx(d,68,1156,PHASES[stage]['desc'],16,INK,anchor='lm')
 for j in range(6):
  x=W-242+j*28
  d.rounded_rectangle(rect(x,1118,17,5),radius=2*S,fill=COLORS[j] if stage==j else BORDER)
 return im.resize((W,H),Image.Resampling.LANCZOS)

def at(ps,f):
 lengths=[math.dist(a,b) for a,b in zip(ps,ps[1:])];target=sum(lengths)*max(0,min(1,f))
 for a,b,l in zip(ps,ps[1:],lengths):
  if target<=l:return (a[0]+(b[0]-a[0])*target/l+OX,a[1]+(b[1]-a[1])*target/l+OY)
  target-=l
 return (ps[-1][0]+OX,ps[-1][1]+OY)

static=base();static.save(OUT/'architecture-static.png')
bases=[base(i) for i in range(6)]
# One shared palette prevents frame-to-frame colour noise and preserves readable type.
swatch=Image.new('RGB',(W,H*7));swatch.paste(static,(0,0))
for i,b in enumerate(bases):swatch.paste(b,(0,H*(i+1)))
palette=swatch.quantize(colors=192,method=Image.Quantize.MEDIANCUT)
frames=[];durations=[]
for stage in range(6):
 print(f'阶段 {stage+1}/6：生成动画帧',flush=True)
 es=PHASES[stage]['edges']
 for j in range(36):
  im=bases[stage].copy();d=ImageDraw.Draw(im)
  progress=j/36
  # All motion remains on the checked directed relationship polylines.
  edge_index=min(len(es)-1,int(progress*len(es)));frac=progress*len(es)-edge_index
  for offset,r,c in ((-.035,5,LINE),(0,4,COLORS[stage])):
   x,y=at(EDGES[es[edge_index]]['points'],max(0,frac+offset));d.ellipse((x-r,y-r,x+r,y+r),fill=c)
  frame=im.quantize(palette=palette,dither=Image.Dither.NONE)
  frames.append(frame);durations.append(80 if j%3 else 90)
frames[0].save(OUT/'architecture-flow.gif',save_all=True,append_images=frames[1:],duration=durations,loop=0,optimize=True,disposal=1)
with Image.open(OUT/'architecture-flow.gif') as gif:
 times=[]
 for i in range(gif.n_frames):gif.seek(i);times.append(gif.info['duration'])
 metadata={'width':gif.width,'height':gif.height,'frames':gif.n_frames,'duration_ms':sum(times),'loop':gif.info.get('loop'),'bytes':(OUT/'architecture-flow.gif').stat().st_size,'sha256':hashlib.sha256((OUT/'architecture-flow.gif').read_bytes()).hexdigest(),'specification_sha256':hashlib.sha256((OUT/'architecture-v2.json').read_bytes()).hexdigest(),'artifact_sha256':hashlib.sha256((OUT/'architecture.html').read_bytes()).hexdigest(),'method':'Pillow rendering of the checked Archify graph and compiled polylines; six illustrative phases, not measured runtime timings.'}
print(json.dumps(metadata,ensure_ascii=False),flush=True)
