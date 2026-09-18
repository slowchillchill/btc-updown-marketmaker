"""固定、可追溯的纯图技术切片，非公共账本/支持集/模拟器。

#6 1.5 分配、正式续篇1 B/D、续篇2 §3/5：开户 2000，O 为空；
P 骨架为合法 post-only GTC，每腿5份，A价0.2、B价0.3，尚无成交。
这里列出的截面是夹具的独立已知预期；不推进交易或复刻账本算法。
"""
from decimal import Decimal, localcontext
import torch


def log_value(value):
    value = Decimal(str(value))
    with localcontext() as ctx:
        ctx.prec = 80
        return float((abs(value) + 1).ln().copy_sign(value))


class Row:
    def __init__(self, width, source):
        self.values = [0.0] * width
        self.trace = [{"index": i, "source": source, "value": 0,
                       "meaning": "该切片未观测、未选或结构性不适用；由相邻掩码/类别区分"}
                      for i in range(width)]

    def put(self, index, value, meaning, raw=None):
        self.values[index] = float(value)
        self.trace[index].update(value=float(value), meaning=meaning, raw=str(raw) if raw is not None else None)

    def triple(self, index, raw, meaning, applicable=True, exists=True, price=False):
        known = applicable and exists
        value = (2 * float(raw) - 1 if price else log_value(raw)) if known else 0
        self.put(index, value, meaning, raw if known else None)
        self.put(index + 1, int(known), meaning + "：存在")
        self.put(index + 2, int(applicable), meaning + "：适用")

    def tensor(self):
        return torch.tensor(self.values, dtype=torch.float32)


def account_slice(pair_count=0, removed_a=False):
    """已接纳全部未成交GTC截面；一次取消A后只保留B，其余原pair完整。"""
    a, b = pair_count - int(removed_a), pair_count
    # 所有量是有限场景预期，不从被测公共投影反算。
    reserved = Decimal(a) + Decimal(b) * Decimal('1.5')
    r = Row(412, "#6 正式续篇1 B；正式续篇2 §3")
    scalar_starts = list(range(0, 93, 3)) + list(range(95, 356, 3)) + list(range(364, 412, 3))
    for i in scalar_starts:
        r.triple(i, 0, "开户/尚无成交/空旧集合的已知结构性零")
    r.put(93, 1, "当前交易帧")
    r.triple(95, reserved, "当前真实本金占用；post-only即时费0")
    r.triple(98, 5*a, "当前A未终局承诺")
    r.triple(101, 5*b, "当前B未终局承诺")
    r.triple(81, a+b, "当前开放订单数")
    for i in (110, 116): r.triple(i, Decimal(2000)-reserved, "实际/合格可用现金")
    for i in (113, 119): r.triple(i, reserved, "实际/合格预留现金")
    for i in (314, 317): r.triple(i, 2000, "开户两种NAV；仅预留不改变资产")
    r.put(356, 1, "当前结算类别Unresolved")
    r.put(362, 1, "当前结算状态存在")
    r.put(363, 1, "当前结算状态适用")
    for i in (382, 385): r.triple(i, 1, "当前订单形成于截止前1秒", applicable=bool(a+b))
    for i in (394, 397): r.triple(i, 0, "完整空旧订单集合年龄不适用", applicable=False)
    r.triple(388, 5*(a+b), "当前活动未匹配总份额")
    r.triple(391, a+b, "当前真实可撤订单数")
    r.triple(406, a+b, "当前未闭合实际订单身份数")
    return r


def cancel_slices(pair_count=0, removed_a=False):
    """稳定源序Single、原pair组、Market；每组原ordinal A/B保持。"""
    orders = [(i, leg) for i in range(pair_count) for leg in (0, 1)
              if not (removed_a and i == 0 and leg == 0)]

    def template(r, offset, leg):
        for i in (offset+leg, offset+2, offset+3, offset+4, offset+8, offset+9,
                  offset+11, offset+12, offset+13):
            r.put(i, 1, "真实腿、GTC、post_only=true及各存在/适用位")
        price = Decimal('0.2') if leg == 0 else Decimal('0.3')
        for start, raw, name in ((14,price,"原报价"),(17,5,"原目标份额"),
                                 (23,0,"实际累计匹配量"),(26,5,"活动未匹配量"),
                                 (29,price*5,"当前本金占用"),(32,0,"当前即时费占用")):
            r.triple(offset+start, raw, name, price=start==14)
        for start in (35,41):
            for j,v in enumerate((log_value(1),log_value(1),0,1,1,1)):
                r.put(offset+start+j,v,"形成/最新直接接纳反馈：S=R=C−1000ms")

    def obj(kind, members):
        r=Row(129,"#6 正式续篇1 B 撤单联合向量")
        r.put(kind,1,"Single/Paired/Market类别"); r.put(3,1,"属于当前c")
        if kind != 2:
            for n,(_,leg) in enumerate(members): template(r,5+50*n,leg)
        else:
            counts=[sum(leg==l for _,leg in members) for l in (0,1)]
            for start,vals,name in ((105,counts,"可撤订单数"),(111,[5*n for n in counts],"活动余量"),
                                    (117,[Decimal(counts[0]),Decimal(counts[1])*Decimal('1.5')],"本金占用"),
                                    (123,[0,0],"即时费占用")):
                for leg in (0,1):r.triple(start+3*leg,vals[leg],name)
        return r
    result=[obj(0,[o]) for o in orders]
    result += [obj(1,[(i,0),(i,1)]) for i in range(pair_count) if not (removed_a and i==0)]
    if orders: result.append(obj(2,orders))
    return result


def market_slice(step=0):
    """合法已激活市场截面；静态盘口，1秒后仍为同一原事实，时间不刷新。"""
    r=Row(836,"#6 市场/规则完整布局及37维参考表")
    age=log_value(step)
    r.put(0,1,"适用Chainlink现货参考");r.put(5,1,"OfficialAnchor")
    for i in (7,14):r.put(i,log_value(60000),"同价正式锚点/现货USD/BTC",60000)
    for i in (11,12,15,19,20,36):r.put(i,1,"参考/现货/判定距离存在")
    for i in (8,9,16,17):r.put(i,age,"直接原事实年龄",step)
    for base,pm in ((37,True),(155,True),(273,False)):
        r.put(base,1,"Fresh公共盘口")
        for side in (0,1):
            o=base+6+56*side
            r.put(o,1,"侧数量已知");r.put(o+1,log_value(1),"正数量价位1")
            r.put(o+2,log_value(100),"全侧量100")
            for j in (5,6,7,8):r.put(o+j,age,"同一直接价位事实年龄",step)
            for j in (11,12,13):r.put(o+j,1,"直接时间已知比例")
            price=Decimal('0.4') if side==0 else Decimal('0.6')
            value=2*float(price)-1 if pm else log_value(59999 if side==0 else 60001)
            r.put(o+14,value,"唯一已知价位",price if pm else (59999 if side==0 else 60001))
            r.put(o+15,log_value(100),"该价位量",100);r.put(o+16,1,"档位存在")
            for j in (17,18):r.put(o+j,age,"该价位原锚点年龄",step)
            for j in (20,21):r.put(o+j,1,"原时间存在")
    for base in (391,419):
        for j in (0,6):r.put(base+j,1,"Fresh直接BBA存在")
        for o,p in ((7,Decimal('.4')),(14,Decimal('.6')),(21,Decimal('.2'))):
            r.put(base+o,log_value(p) if o==21 else 2*float(p)-1,"直接BBA买/卖/价差",p)
            for j in (1,5,6):r.put(base+o+j,1,"值/原时间存在")
            for j in (2,3):r.put(base+o+j,age,"BBA原事实年龄",step)
    for base in (447,465,483):r.put(base+3,1,"最近完整成交UnknownNotObserved")
    # 本组恰一条Chainlink现货；无成交，其他组统计全零。
    r.put(575+12,log_value(1),"本组Chainlink现货事实1条")
    for i in (575+23+2*9+6,575+23+2*9+7,575+23+2*9+8):r.put(i,1,"该原事实直接时间比例")
    r.put(679,log_value(60+step),"市场开始后秒数",60+step)
    r.put(680,log_value(240-step),"距市场结束秒数",240-step)
    for i in (681,687,690,702,708,709,710,721,731,732):r.put(i,1,"Fresh/Active/交易状态真实值与存在")
    r.put(742+3,1,"判定UnknownNotObserved")
    for block in range(6):
        # 三中价和Chainlink各仅一个实际消费样本；未观测TWAP无样本。
        if block<4:r.put(782+4*block,1/16,"已消费1个价格样本")
    q=Row(30,"#6 H规则：两腿tick .01、LIMIT 5、BUY最低1、delay启用1秒、费率.07指数1仅Taker")
    for o in (0,7):
        q.put(o,log_value('.01'),"当前tick",'.01');q.put(o+1,1,"tick存在")
        for j in (2,3):q.put(o+j,age,"原tick时间年龄",step)
        for j in (5,6):q.put(o+j,1,"原tick时间存在")
    for o,v in ((14,5),(16,1),(20,1),(24,'.07'),(26,1)):
        q.put(o,log_value(v),"有效分类下限/延迟秒/费率/指数",v);q.put(o+1,1,"原值存在")
    for o in (18,19,22,23,28,29):q.put(o,1,"delay/fees/takerOnly真实true及存在")
    for i in range(30):r.put(806+i,q.values[i],q.trace[i]['meaning'],q.trace[i].get('raw'))
    return r,q


def fixture(batch=1, pair_count=0, removed_a=False, step=0, mixed=False):
    fast,rule=market_slice(step)
    rows=cancel_slices(pair_count,removed_a)
    n=len(rows); k=max(1,n)
    features=torch.zeros(batch,k,129);mask=torch.zeros(batch,k,dtype=torch.bool)
    slow=[]
    for b in range(batch):
        empty=mixed and b==0
        slow.append(account_slice(0 if empty else pair_count,False if empty else removed_a).tensor())
        if n and not empty:
            features[b]=torch.stack([row.tensor() for row in rows]);mask[b]=True
    prefix=torch.zeros(batch,60);prefix[:,7]=1
    return (fast.tensor().repeat(batch,1),torch.stack(slow),rule.tensor().repeat(batch,1),
            prefix,features,mask)


def paired_gtc_prefixes(batch=1):
    """合法q=5双腿post-only GTC路径的评分前切片；不求完整支持集。"""
    p = torch.zeros(batch, 60)
    p[:, 2] = 1; p[:, 6] = 1
    p[:, 21] = log_value(5); p[:, 22:24] = 1
    p[:, 28] = 1; p[:, 40] = 1; p[:, 46] = 1; p[:, 58] = 1
    result = []
    def snapshot(stage, leg):
        p[:, 7:14] = 0; p[:, 7+stage] = 1
        p[:, 14:16] = 0; p[:, 14+leg] = 1; p[:, 16] = 1
        result.append(p.clone())
    for leg, offset in ((0, 24), (1, 42)):
        snapshot(2, leg)  # Variant评分前
        p[:, offset] = 1; p[:, offset+5] = 1; p[:, offset+8] = 1
        snapshot(3, leg)  # PostOnly评分前
        p[:, offset+7] = 1; p[:, offset+9] = 1
    snapshot(5, 0)  # Price A评分前；两腿属性均已选
    p[:, 39] = -.6; p[:, 41] = 1
    snapshot(5, 1)  # Price B评分前；A原报价0.2
    return result
