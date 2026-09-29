"""导出逐坐标审计表；事实来源为当前编码器及 feature_schema，不作为运行时输入合同。"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ROWS = []
Q = ['Fresh', 'Stale', 'Missing', 'UnknownNotObserved', 'ConfirmedGap', 'Invalid']
T = ['来源年龄', '接收年龄', '传输时间', '来源时间存在', '接收时间存在']
E = ['来源年龄最小', '来源年龄最大', '接收年龄最小', '接收年龄最大', '传输时间最小', '传输时间最大', '来源时间已知比例', '接收时间已知比例', '成对时间已知比例']
THREE = ['值', '存在', '适用']
STATE = ['未判定', '已判定当前无支付', '应收未请求', '请求在途', '已赎回', '请求失败无作用']


def row(part, index, name, source, use, actual='依可见事实；数值按现有精确变换后转 f32',
        duplicate='未证明与其他输入等价', anomaly=False, action='保留', cost='原生构造及对应输入编码', consumer=None):
    ROWS.append(dict(输入=part, 索引=index, 字段=name, 来源与业务含义=source+'；只在当前因果截止点可见',
                     决策用途=use, 实际取值=actual, 重复关系=duplicate,
                     异常信息关联='异常/缺失/时间表达原样保留' if anomaly else '不以该字段制造异常交易门',
                     实际消费者=consumer or ('状态编码器' if part in ('fast','slow','book') else '动作评分编码器'),
                     建议处置=action, 成本归属=cost))


def block(part, start, prefix, names, source, use, **kw):
    for i, name in enumerate(names):
        protected=any(s in prefix+'.'+name for s in ('时间','年龄','存在','适用','已知','质量','缺失')) or name in Q
        row(part,start+i,prefix+'.'+name,source,use,anomaly=protected,**kw)


def triple(part, start, name, source, use):
    unit=('秒' if '年龄' in name or '有效秒' in name else '订单/请求/记录个数' if any(s in name for s in ('数','数量'))
          else '份' if any(s in name for s in ('份额','持仓','paired','orphan','承诺','缺口','修复','目标')) and not any(s in name for s in ('本金','成本','价值'))
          else 'pUSD')
    for i,label in enumerate(THREE):
        actual=[f'Known: S(x/{unit}单位)；Unavailable/NotApplicable: 数值0，区别见后两位；先精确归约再一次f32舍入',
                '0或1；Known=1，Unavailable与NotApplicable=0',
                '0或1；NotApplicable=0，Known与Unavailable=1'][i]
        row(part,start+i,name+'.'+label,source,use,actual=actual,anomaly=i>0,
            duplicate='同一 V3 状态的三个坐标共同区分真实零/未知/不适用；无证明可删的相同范围字段')


def request(part, start, prefix, source):
    for i, name in enumerate(['在途数','成功撤销数','未撤销无作用数','证据未知无作用数']):
        triple(part,start+i*3,prefix+'.'+name,source,'区分请求进行中和真实结果；自主决定后续撤单')
    block(part,start+12,prefix+'.最近响应',T+['适用'],source,'判断撤单反馈年龄和响应是否可见')


def build_fast():
    block('fast',0,'参考',['规则现货','规则TWAP30','规则TWAP60','有效参考缺失','临时TWAP','官方锚点','固定临时参考存在','有效价格']+T+['临时窗口秒'],
          'market.rs::reference；effective_reference/settlement_reference/temporary_reference','判断结算参考来源与价格位置')
    for i,name in enumerate(['现货','TWAP30','TWAP60']):
        block('fast',14+7*i,'参考.'+name,['价格','值存在']+T,'market.rs::reference；chainlink_'+name,'定价、领先关系和来源异常学习')
    block('fast',35,'参考偏差',['参考价格减有效锚点','存在'],'market.rs::reference；同帧所选参考与有效锚点','定价偏离的派生摘要',duplicate='可从精确原值推导，不保证从已舍入特征无损恢复')
    side=['已知','正数量档数','总数量','总数量变化','变化存在']+E+['最优价变化','最优价变化存在']
    for i,name in enumerate(['PM_A','PM_B','Binance']):
        base=37+38*i
        block('fast',base,name+'.质量',Q,'market.rs::encode；公共盘口质量','识别不可见、陈旧、缺口和无效事实')
        for j,direction in enumerate(['买','卖']):
            block('fast',base+6+j*16,name+'.'+direction,side,'market.rs::side；完整侧及前一帧统计','流动性、变化速度及侧来源时间；摘要是否有增益仍未训练验证',duplicate='档数/总量来自同份完整价量；变化依赖前一帧')
    for i,name in enumerate(['A','B']):
        base=151+28*i
        block('fast',base,name+'.直接报价',Q+['整体存在'],'market.rs::bba；独立 best_bid_ask 观测','报价可见性，不以完整盘口派生值冒充该来源')
        for j,field in enumerate(['买价','卖价','直接价差']):
            block('fast',base+7+j*7,name+'.直接报价.'+field,['值','存在']+T,'market.rs::bba；同一直接报价消息','当前价位与来源年龄',duplicate='三个字段的 T5 完全相同；保留输入，仅共享已有一次计算')
        block('fast',207+18*i,name+'.最新成交',Q+['存在','价格','数量','BUY','SELL','费率bps','费率存在']+T,
              'market.rs::pm_trade；public_trade','最新成交方向、数量、费用及来源质量')
    block('fast',243,'Binance.最新成交',Q+['存在','价格','数量','买方maker']+T,'market.rs::binance_trade；binance_trade','外部市场成交与领先信号')
    pm=['次数','首价','末价','最低价','最高价','均价','已知数量','数量已知比例','价格乘数量合计','BUY次数','SELL次数','Unknown次数','无方向次数','BUY数量','SELL数量','BUY数量已知比例','SELL数量已知比例','已知费用均值bps','费用已知比例']+E
    bn=['次数','首价','末价','最低价','最高价','均价','数量','金额','主动BUY次数','主动SELL次数','BUY数量','SELL数量']+E
    for i,name in enumerate(['A','B']):
        block('fast',258+28*i,name+'.本组成交',pm,'market.rs::trades；该资产 group_events 成交原成员','本组交易流量；与最新成交状态范围不同')
    block('fast',314,'Binance.本组成交',bn,'market.rs::trades；Binance group_events 成交','外部交易流量与方向')
    events=['PM全量盘口','PM绝对数量变化','PM直接报价','PM_tick','PM成交','PM新市场','固定身份','初始tick','生命周期','交易状态','终态','质量边界','Chainlink现货','TWAP30','TWAP60','官方锚点','参考规则','Binance深度','Binance成交','RTDS_InvalidType','RTDS_InvalidTopic','RTDS_InvalidSymbol','RTDS_InvalidTimestamp']
    for i,name in enumerate(events):
        row('fast',335+i,'事件次数.'+name,'market.rs::event_class/events；完整原组事实分类计数','感知事件构成、市场更新或公共异常；不代表工程复核通过',anomaly=i in (11,19,20,21,22),duplicate='全组统计可跨同组市场行共用；不按当前资产过滤',cost='事件遍历及精确计数投影')
    for i,name in enumerate(['PM消息','市场事实','现货','TWAP30','TWAP60','锚点参考','Binance深度','Binance成交','RTDS异常']):
        block('fast',358+9*i,'事件时间.'+name,E,'market.rs::events；原成员 direct_sources','来源年龄、传输与已知程度',duplicate='接收时间均已知时，来源已知比例=成对已知比例；原样保留')
    base=439
    block('fast',base,'市场时钟',['已过秒','剩余秒'],'market.rs::lifecycle；窗口与当前 cutoff','市场阶段和剩余期限',duplicate='固定300秒窗口下二者可推导；当前保持')
    block('fast',base+2,'生命周期',Q+['存在','Candidate','Prefetched','Active','Retired','UnknownUnmodeled']+E,'market.rs::lifecycle；公共 lifecycle','区别业务生命周期与模型时间资格')
    block('fast',base+23,'交易状态',Q+['存在'],'market.rs::lifecycle；同次替换的交易状态公共字段','交易状态是否可见')
    for i,name in enumerate(['active','closed','accepting_orders']):
        block('fast',base+30+11*i,name,['值','存在']+E,'market.rs::lifecycle；'+name,'平台三个不同状态，不自行合成 can_trade')
    block('fast',base+63,'终态',Q+['存在','A赢家','B赢家']+E,'market.rs::lifecycle；terminal','已可见的判定结果及其时间')
    for i,name in enumerate(['A','B']):
        block('fast',base+81+11*i,name+'.支付',['值','存在']+E,'market.rs::lifecycle；terminal.payout_vector','每份支付价值；保留合法终态语义',duplicate='此 E9 与赢家及另一腿支付 E9 相同；构造已共享一次计算')
    for i,name in enumerate(['PM_A中间价','PM_B中间价','Binance中间价','现货','TWAP30','TWAP60']):
        block('fast',542+4*i,'历史.'+name,['样本数除16','最新变化','连续差分RMS','样本时间跨度'],'market.rs::history；来源身份变化触发、至多16样本','提供近期变化摘要，不能用 GRU 存在证明其无用',actual='恒零（当前无逐档锚点，不产生该历史）' if i==2 else '随合法来源新样本变化；同来源复用',action='新版本精简候选；当前保持' if i==2 else '保留')
    rules=[]
    for name in ['A_tick','B_tick']:
        rules += [name+'.'+n for n in ['值','存在']+T]
    rules += [name+'.'+n for name in ['LIMIT最小份额','市价最小支出','延迟启用','延迟秒','费用启用','费率','费率指数','仅taker收费'] for n in ['值','存在']]
    for i,name in enumerate(rules):
        for part,index in [('rule',i),('fast',566+i)]:
            row(part,index,name,'market.rs::rules_with_static/static_rules；同帧公共规则','费用、订单规则和价格单位；平台最小份额作为学习输入，不另造模型最低份数门',
                actual='恒1（冻结规则非可选）' if i>=14 and i%2 else '同帧规则；静态尾随进程冻结',
                duplicate='rule[i] 与 fast[566+i] 逐值相同，分别进入动作与状态分支',anomaly='存在' in name or any(t in name for t in T),
                cost='动态 tick/T5 每行；静态16首次构建后复用',consumer='状态编码器和动作规则编码器（同组多消费者）')
    coords=['价格','数量','level_present']+T+['距本侧最优价']
    block('book',0,'逐档',coords,'market.rs::side；六侧全部可见正数量档','价量关联、距最佳价与逐档观测时间',duplicate='有效行 level_present=1，book_mask 已独立筛选；Binance T5 五槽恒零；当前保留异常表达',cost='逐档投影、数组复制、打包及9→32→32前端')


def build_account():
    for scope,b in [('当前',0),('旧市场',27)]:
        for j,state in enumerate(['发送前本地','发送后未知','平台确认待经济']):
            for k,unit in enumerate(['本金pUSD','即时期费pUSD','数量']):triple('slow',b+j*9+k*3,scope+'.'+state+'.'+unit,'account.rs::encode；LedgerProjectionV2','真实占款/执行进度，避免把待确认当可用资金')
    for g,scope in enumerate(['当前','旧市场']):
        for c,field in enumerate(['持仓份额','本金成本pUSD']):
            for s,leg in enumerate(['A','B']):triple('slow',54+g*12+c*6+s*3,scope+'.'+leg+'.'+field,'account.rs::encode；账本按身份映射的 outcome_balances','持仓和真实成本')
        for k,field in enumerate(['账本开放订单数','账本待成交数']):triple('slow',81+g*6+k*3,scope+'.'+field,'account.rs::encode；LedgerProjectionV2','账本经济视角状态；与未闭合订单身份数范围不同')
        for s,leg in enumerate(['A','B']):
            triple('slow',98+g*6+s*3,scope+'.'+leg+'.未终局承诺份额','account.rs::encode；read.commitments','避免重复补腿，区分承诺和实际成功')
            for c,field in enumerate(['成功终局份额','成功本金成本pUSD']):triple('slow',122+c*12+g*6+s*3,scope+'.'+leg+'.'+field,'account.rs::encode；AccountEconomicsReadV1','终局成功量及配对成本，不等同所有matched数量')
        for s,field in enumerate(['paired','orphanA','orphanB']):triple('slow',146+g*9+s*3,scope+'.'+field,'account.rs::paired_shares/encode；公共经济读取','已配对及孤腿风险')
        for r,role in enumerate(['maker','taker']):
            for k,field in enumerate(['成交份额','本金pUSD','实际费用pUSD']):triple('slow',176+g*18+r*9+k*3,scope+'.'+role+'.'+field,'account.rs::encode；executed[role]','真实成交和实际费用')
            for k,field in enumerate(['estimated','accrued_unmatured','below_threshold','confirmed_unpaid','paid','locked_remainder']):triple('slow',212+g*36+r*18+k*3,scope+'.'+role+'.返佣.'+field,'account.rs::encode_rebates；公共返佣状态','估计、未成熟、未支付与真实现金区分')
        for k,field in enumerate(['保证持仓价值','配对应收','赢家孤腿应收','未付taker费','未付结算费']):triple('slow',284+g*15+k*3,scope+'.'+field,'account.rs::encode；AccountEconomicsReadV1','资金可用性和真实经济义务')
        for k,field in enumerate(['已确认净损益','已确认净亏损','最大已确认净亏损','未决孤腿本金']):triple('slow',332+g*12+k*3,scope+'.'+field,'account.rs::encode；公共经济投影','历史损益/风险摘要，不形成额度或动作门')
        for k,field in enumerate(['最早形成订单年龄','最近形成订单年龄','活动未匹配份额','可撤订单数']):triple('slow',382+g*12+k*3,scope+'.'+field,'account.rs::order_ages/encode；完整未闭合订单','撤单与等待决策；空订单年龄不适用，未知形成时间不取已知子集')
        triple('slow',406+g*3,scope+'.未闭合订单身份数','account.rs::encode；read.orders','包括不可撤但经济或承诺未终局的订单')
    triple('slow',78,'当前A减B持仓','account.rs::encode；账本两腿余额','配平摘要；可由原始两腿持仓推导')
    block('slow',93,'角色',['DecisionRequired','ContextOnly'],'root.rs::consume；当前行角色','当前交易与下一市场热身角色')
    triple('slow',95,'当前全部占款','account.rs::encode；current_all_in_commitment_pusd','真实资金占用')
    for i,name in enumerate(['实际可用','实际预留','做市可用','做市预留']):triple('slow',110+i*3,name,'account.rs::encode；公共cash','双现金和身份预留，不能混用')
    for base,name in [(164,'成功修复缺口'),(170,'缺口减承诺上限')]:
        for s,leg in enumerate(['A','B']):triple('slow',base+s*3,leg+'.'+name,'account.rs::encode；visible FinalSuccess 与同侧承诺','单腿修复；可见成功早于经济应用，不能只看已入账余额')
    for i,name in enumerate(['保守NAV','方向中性NAV','方向残差','其他账户残差','其他应收','其他负债']):triple('slow',314+i*3,name,'account.rs::encode；AccountEconomicsReadV1','全账户经济结果及组成；可推导摘要，不是重复入账')
    block('slow',356,'当前结算',STATE+['存在','适用'],'account.rs::status；真实请求及经济投影','判定与兑现进度；不是第二状态机')
    for i,name in enumerate(STATE):triple('slow',364+i*3,'旧市场结算数.'+name,'account.rs::status；旧市场读取集合','旧市场仍占用或待兑现事项')
    triple('slow',412,'待入账记录数','account.rs::encode；ActionBeforeRead.pending_noe_record_count','已领取但依赖未满足；零不证明未来齐全，不用它禁单')
    request('slow',415,'全账户撤单请求','values.rs::request_block；cancellation_requests(None)')


def build_cancel():
    block('cancel',0,'对象',['Single','Paired','Market','当前市场','旧市场','范围已知'],'cancel.rs::encode；真实对象及当前/旧市场集合','选择撤单对象，保留原始稳定顺序')
    for b,label in [(6,'订单0'),(74,'订单1')]:
        block('cancel',b,label,['A','B','资产已知','资产适用','GTC','GTD','FOK','FAK','类型存在','类型适用','PostOnly_false','PostOnly_true','PostOnly存在','PostOnly适用'],
              'cancel.rs::template；订单origin及公共已知属性','旧订单真实类型；当前新动作只选GTC/FOK不允许删旧类型读取')
        for off,name in [(14,'原价phi'),(17,'原目标份额'),(20,'原支出pUSD'),(23,'已匹配份额'),(26,'活动未匹配份额'),(29,'本金占款'),(32,'费用占款')]:triple('cancel',b+off,label+'.'+name,'cancel.rs::template；订单原意图/读取及order_funds','撤单经济影响和执行进度')
        for off,name in [(35,'形成'),(41,'最新直接反馈')]:block('cancel',b+off,label+'.'+name,T+['适用'],'cancel.rs::template；原NOE直接记录','订单与反馈时间，不能从依赖链冒充直接时间')
        triple('cancel',b+47,label+'.GTD剩余有效秒','cancel.rs::template；原到期减60秒再减当前时间','旧GTD的真实有效期；新动作不选择GTD不意味着恒不适用')
        block('cancel',b+50,label+'.条件适用已知',['价格','目标','Base数量','支出'],'cancel.rs::template；origin/base_quantity_applicability','区分不知道是否适用与已知不适用')
        triple('cancel',b+54,label+'.可撤','cancel.rs::template；order.cancellable','真实撤单可作用性')
        block('cancel',b+57,label+'.最近撤单结果',['Canceled','NotCanceledNoEffect','EvidenceUnknownNoEffect','存在','适用'],'cancel.rs::template；latest_cancel_feedback','撤单反馈；不把未知当成功')
        block('cancel',b+62,label+'.最近撤单响应',T+['适用'],'cancel.rs::template；同一结果原直接响应','对应反馈的时间')
    for j,name in enumerate(['成员数','活动未匹配份额','本金占款','费用占款']):
        for s,leg in enumerate(['A','B']):triple('cancel',142+j*6+s*3,'市场对象.'+leg+'.'+name,'cancel.rs::encode；本对象已观测成员','市场撤单影响；与全账户统计范围不同')
    request('cancel',166,'对象撤单请求','values.rs::request_block；cancellation_requests(object.request_scope())')


def build_prefix():
    names=['NoTrade','SubmitSingleBuy','SubmitPairedBuy','CancelOrder','CancelOrders','CancelMarketOrders','族已选','阶段Family','阶段QuantityBranch','阶段Variant','阶段Price','阶段CancelObject','腿A','腿B','腿适用','L(U)','lo/U','hi/U','区间适用','L(n/100)','数量适用','数量已选']
    names += [leg+'.'+x for leg in ['A','B'] for x in ['GTC','FOK','类型适用','类型已选','价格phi','价格适用','价格已选']]
    block('prefix',0,'前缀',names,'prefix.rs::PrefixState::encode；同一决定精确前缀及冻结tick','条件参数打分；不得由浮点输入反推精确订单值',cost='动作解码阶段，不在native.consume计时内')


def domains(by):
    """按真实编码类型补足定义域；样本统计另加，不用窄样本重定义合同。"""
    default='依可见事实；数值按现有精确变换后转 f32'
    boolean_words=('存在','适用','已知','已选')
    for (part,index),r in by.items():
        name=r['字段'];leaf=name.rsplit('.',1)[-1]
        r['实际消费者']={'fast':'graphs.BookState→BookEncoder汇总拼接→ResidualGRU.fast_encoder（FeatureEncoder/LN）',
                        'slow':'graphs.BookState→ResidualGRU.slow_encoder（FeatureEncoder/LN）',
                        'book':'book.BookEncoder；PM四侧与BN两侧各自9→32→32，再逐侧sum/mean/max成576维',
                        'rule':'ActionDecoder.rule→FamilyGraph.rule_context；同值还进入状态fast[566:596]',
                        'cancel':'ActionDecoder.object_encoder→撤单摘要/评分（ObjectGraph/CancelGraph）',
                        'prefix':'ActionDecoder.prefix→Quantity/Variant/Price/CancelContextGraph'}[part]
        if part=='fast' and index>=566:r['实际消费者']+='；同值rule输入还被ActionDecoder.rule消费'
        if part=='prefix':
            r['异常信息关联']='同次动作的条件选择状态，不是采集异常；区别未选择与不适用，当前语义保持'
        if r['实际取值']!=default:continue
        if '比例' in leaf or leaf=='样本数除16':
            r['实际取值']='[0,1]；整数比精确除法后最近偶数f32；分母0输出0'
        elif leaf in Q or any(s in leaf for s in boolean_words) or '条件适用已知' in name:
            r['实际取值']='0或1；原状态指示位，缺失/未知时保持各自原编码；不等同数值0'
        elif any(s in leaf for s in ('年龄','传输时间','样本时间跨度')):
            r['实际取值']='S(精确i128毫秒差/1000秒)；负时间差保留；无对应直接时间时0，不用间接依赖时间填充'
        elif any(s in leaf for s in ('次数','档数','身份数','成员数')) or name.startswith('事件次数.'):
            r['实际取值']='L(n)，n为本范围实际非负整数；n=0时0，无归一化上限；单位为次/档/个'
        elif '费' in leaf and ('bps' in leaf or leaf=='费率bps'):
            r['实际取值']='S(原费率/1bps)；均值先在精确域求和除以已知成员数；缺失0并保留原已知位/比例'
        elif '数量' in leaf or leaf in ('BUY数量','SELL数量'):
            unit='BTC' if 'Binance' in name else '份'
            r['实际取值']=f'L(非负精确数量/1{unit})；变化字段为S(精确差/1{unit})；无成员/不可见时0且保留对应原状态'
        elif leaf=='连续差分RMS':
            r['实际取值']='L(sqrt(精确相邻差平方均值))>=0；少于2样本时0；PM单位pUSD/份，参考价格单位USD/BTC'
        elif leaf=='最新变化':
            r['实际取值']='S(最近两个精确样本价差)；可负/零/正；少于2样本0，PM pUSD/份或参考USD/BTC'
        else:
            r['实际取值']='0或1；原分类/布尔位按当前分支置位，不可见或非该类保持0'
    def actual(part,indices,value,dup=None):
        for index in indices:
            by[part,index]['实际取值']=value
            if dup is not None:by[part,index]['重复关系']=dup
    actual('fast',[7,14,21,28],'S(精确USD/BTC价格/1 USD/BTC)；已知正价格为正，缺失数值0，原存在/来源位区分状态')
    actual('fast',[13],'L(固定临时参考实际窗口秒/1秒)>=0；无该参考0')
    actual('fast',[35],'S(精确所选参考价−有效锚点价，单位USD/BTC)；有符号，无可配对原值时0且存在位0')
    for base in [37,75,113]:
        unit='BTC' if base==113 else '份'
        punit='USDT/BTC' if base==113 else 'pUSD/份'
        for off in [6,22]:
            actual('fast',[base+off+2],f'L(本侧全部正数量档精确和/1{unit})；已知空侧0，未知侧0由known/Q6区别')
            actual('fast',[base+off+3],f'S((本帧总量−立即前帧总量)/1{unit})；前帧未知时0且变化存在=0')
            actual('fast',[base+off+14],f'S((本帧最优价−立即前帧最优价)/1 {punit})；任一为空时0且存在=0')
    pm_prices=[158,165,186,193,214,232,520,531]
    for base in [258,286]:pm_prices+=list(range(base+1,base+6))
    actual('fast',pm_prices,'phi=2*p−1，p单位pUSD/份；价格为(0,1)内、支付可为0/1；不可见/无成员数值0，原状态保留')
    actual('fast',[172,200],'S(直接报价原spread/1 pUSD/份)；直接消息字段，不重新以已舍入bid/ask求差')
    actual('fast',[250]+list(range(315,320)),'S(精确价格/1 USDT/BTC)；原成交/精确均价；无成交数值0')
    actual('fast',[266,294],'L(已知价格×数量的精确和/1pUSD)>=0；不是先舍入各成员再相加')
    actual('fast',[264,292],'L(本组已知精确数量和/1份)>=0；未知数量成员不参与，数量已知比例另保留')
    actual('fast',[218,236,275,303],'S(原费率或已知费率精确均值/1bps)；缺失数值0，原存在位/已知比例保留')
    actual('fast',[321],'L(精确成交金额/1USDT)>=0；无本组成交0')
    actual('fast',[439,440],'S(精确窗口已过/剩余毫秒差/1000秒)；ContextOnly已过可负；保留符号')
    for base,unit in [(542,'pUSD/份'),(546,'pUSD/份'),(550,'USDT/BTC'),(554,'USD/BTC'),(558,'USD/BTC'),(562,'USD/BTC')]:
        if base==550:
            actual('fast',range(base,base+4),'恒0；当前没有逐档直接来源身份，不形成Binance中间价历史；不填整本时间')
            by['fast',base+3]['建议处置']='保持当前时间/来源缺失表达；新布局如改变位置仍须保留原值及意义'
            continue
        actual('fast',[base],'m/16，m为0..16的来源新样本个数；离散取值0,1/16,…,1')
        actual('fast',[base+1],f'S(最近两个精确样本差/1 {unit})；少于2样本0；允许负数')
        actual('fast',[base+2],f'L(sqrt(精确相邻差平方均值)/1 {unit})>=0；少于2样本0')
        actual('fast',[base+3],'S((末样本被选cutoff−首样本被选cutoff)/1000秒)；少于2样本0，沿原样本身份更新')
    for part,offset in [('rule',0),('fast',566)]:
        actual(part,[offset,offset+7],'L(精确tick/1 pUSD/份)；本轮可见tick仅.01或.001，缺失0且存在位0')
        for index,unit in [(14,'份'),(16,'pUSD'),(20,'秒'),(24,'无量纲费率'),(26,'无量纲整数指数')]:
            actual(part,[offset+index],f'L(冻结规则精确值/{unit}单位)>=0；随规则快照而异，非跨进程恒值')
        actual(part,[offset+18,offset+22,offset+28],'0或1；冻结规则bool；非跨进程恒值')
    actual('prefix',range(36),'0或1；条件选择/阶段one-hot；不适用和未选择按原分支保持0')
    actual('prefix',[15],'L(U)，U为完整合法数量区间原整数上界；非该阶段0')
    actual('prefix',[16,17],'lo/U、hi/U∈[0,1]，在精确整数比上最近偶数舍入；非该阶段0')
    actual('prefix',[19],'L(n/100份)，原精确数量整数n保持独立；尚未选择时0')
    actual('prefix',[26,33],'phi(k*tick)=2*k*tick−1；k为原精确全局价格索引，未选时0；不反推执行价格')
    for base in [6,74]:
        actual('cancel',[base+14],'Known时phi(原价格)=2*p−1；未知/不适用数值0，以后两位区别')
        actual('cancel',[base+54],'Known时原bool转0或1；不是L(bool)；未知/不适用数值0，以后两位区别')
    for base in list(range(110,122,3))+list(range(314,332,3))+list(range(364,382,3))+[412,415]:
        actual('slow',[base+1,base+2],'恒1；本字段由实际同组全账户读取构造Known，保留原已知/适用语义')
        by['slow',base]['实际取值']=by['slow',base]['实际取值'].split('；Unavailable')[0]+'；本字段在当前编码合同为Known，零是实际零'
        for index in [base+1,base+2]:
            by['slow',index].update(异常信息关联='当前构造恒Known；本位不携带异常/未知变化，不将名字作为硬保留依据',
                重复关系='当前构造恒1；此坐标不区分本字段未定义的Unavailable/NotApplicable分支',
                建议处置='本轮维持旧模型数值；新布局常量折叠候选，须说明LN影响和净收益')
    for part,offset in [('rule',0),('fast',566)]:
        for index in range(15,30,2):
            by[part,offset+index].update(异常信息关联='冻结规则非可选，当前恒1；本位不是实际采集异常状态',
                重复关系='当前构造恒1；rule[i]与fast[566+i]仍分别服务动作/状态消费者',
                建议处置='本轮维持旧模型数值；新布局常量折叠候选，须同时处理两个消费者和LN')
    book_domains=[
        ('PM phi(p)=2p−1，p单位pUSD/份；BN L(p)，p单位USDT/BTC；全部为原精确价格的一次投影','自身非重复；跨腿不能仅凭镜像规则还原实际因果观测'),
        ('L(q)>0；PM q单位份，BN q单位BTC；只输出原正数量档','总量是全部档数量的精确摘要，不能从舍入后的qlog无损恢复精确总量'),
        ('有效行恒1；padding由book_mask=false标记','与有效行book_mask有结构重复；新布局候选，旧输入仍保留'),
        ('PM S((cutoff−source)/1000秒)；源时间不存在0；BN恒0','可与同锚点档相同，但不是同价格另一腿的时间'),
        ('PM S((cutoff−received)/1000秒)；唯一直接锚点不存在0；BN恒0','同接收时间可共享数值投影，不能消除原档来源含义'),
        ('PM S((received−source)/1000秒)，负数保留；来源缺失0；BN恒0','同精确差值可共享投影，区别于来源年龄/接收年龄'),
        ('PM 0或1，唯一直接锚点且源时间存在才为1；BN恒0','是时间存在语义，不因当前大多1/BN恒0而删除'),
        ('PM 0或1，唯一直接锚点存在即1；BN恒0','是接收时间存在语义，不以book_mask替代'),
        ('L(同侧非负距离)；bid为best−p、ask为p−best；PM单位pUSD/份，BN USDT/BTC；最优档恒0','可由精确价与本侧最优价推导；不能从已舍入价格无损恢复原距离投影')]
    for i,(domain,duplicate) in enumerate(book_domains):
        actual('book',[i],domain,duplicate)
        by['book',i]['来源与业务含义']+='；PM来自assets[A/B]的数量事实，BN来自binance_depth；两者保持各自原货币/数量单位'


def main():
    build_fast(); build_account(); build_cancel(); build_prefix()
    by={(r['输入'],r['索引']):r for r in ROWS}
    # 这是审计导出的覆盖自检，不在生产链加入字段校验。
    for part,n in [('fast',596),('slow',433),('book',9),('rule',30),('cancel',184),('prefix',36)]:
        assert all((part,i) in by for i in range(n)), (part, [i for i in range(n) if (part,i) not in by])
    assert len(by)==len(ROWS)==1288
    domains(by)
    for i in list(range(124,133))+list(range(140,149)):
        by['fast',i]['实际取值']='恒零；Binance 无逐档直接锚点，禁止用整本时间填充'
        by['fast',i]['建议处置']='保留缺失/时间表达，构造可直接复用零块'
    with (Path(__file__).parent/'field-audit.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(ROWS[0]),lineterminator='\n');writer.writeheader()
        writer.writerows(sorted(ROWS,key=lambda r:(r['输入'],r['索引'])))


if __name__ == '__main__':main()
