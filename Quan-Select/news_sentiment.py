# -*- coding: utf-8 -*-
"""
消息面情绪评分模块
基于巨潮资讯网公告数据，对个股消息面进行量化评价
"""

import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import time
import re


class NewsLevel(Enum):
    """消息面等级"""
    STRONG_BULLISH = 5   # 重大利好
    BULLISH = 4          # 利好
    SLIGHT_BULLISH = 3   # 偏利好
    NEUTRAL = 2          # 中性
    SLIGHT_BEARISH = 1   # 偏利空
    BEARISH = 0          # 利空
    STRONG_BEARISH = -1  # 重大利空


@dataclass
class NewsEvent:
    """消息事件"""
    date: str
    title: str
    event_type: str
    level: NewsLevel
    score: float  # 原始分数
    weight: float  # 权重


@dataclass
class SentimentScore:
    """情绪评分结果"""
    code: str
    name: str
    total_score: float  # 0-100分
    level: str
    event_count: int
    bullish_count: int
    bearish_count: int
    recent_events: List[NewsEvent]
    summary: str


# 关键词映射表 - 用于识别公告类型
KEYWORD_MAP = {
    # 重大利好
    NewsLevel.STRONG_BULLISH: [
        "净利润增长", "业绩大增", "扭亏为盈", "超预期", "大幅增长",
        "重大合同", "中标", "签订", "战略合作协议", "合作框架",
        "并购重组", "收购", "重组通过", "重大资产重组",
        "股份回购", "增持", "员工持股", "股权激励",
        "新产品", "技术突破", "专利", "获批", "认证",
        "产能扩张", "项目投产", "订单饱满", "供不应求",
    ],
    # 利好
    NewsLevel.BULLISH: [
        "业绩增长", "营收增长", "毛利率提升", "盈利能力提升",
        "分红", "派息", "送转", "利润分配",
        "市场拓展", "新签订单", "产能释放",
        "机构调研", "券商推荐", "买入评级", "增持评级",
        "获得资质", "通过认证", "入选", "荣誉",
    ],
    # 偏利好
    NewsLevel.SLIGHT_BULLISH: [
        "经营正常", "业务稳定", "经营稳健",
        "股东大会通过", "董事会决议", "正常经营",
    ],
    # 利空
    NewsLevel.BEARISH: [
        "业绩下滑", "营收下降", "亏损扩大", "毛利率下降",
        "减持", "减持计划", "减持进展", "大宗交易减持",
        "解禁", "限售股", "限售股解禁",
        "质押", "股权质押", "质押率", "补充质押",
        "诉讼", "仲裁", "纠纷", "索赔",
        "行政处罚", "监管函", "关注函", "问询函",
    ],
    # 重大利空
    NewsLevel.STRONG_BEARISH: [
        "业绩预亏", "大幅亏损", "ST风险", "退市", "退市风险警示",
        "立案调查", "违规", "造假", "信息披露违规",
        "债务违约", "逾期", "破产", "重整", "资不抵债",
        "实控人变更", "控制权争夺", "股权争斗",
        "停产", "限产", "环保处罚", "安全事故",
        "关联交易", "资金占用", "违规担保",
    ],
}

# 基础分值映射
LEVEL_BASE_SCORE = {
    NewsLevel.STRONG_BULLISH: 20,
    NewsLevel.BULLISH: 12,
    NewsLevel.SLIGHT_BULLISH: 6,
    NewsLevel.NEUTRAL: 0,
    NewsLevel.SLIGHT_BEARISH: -4,
    NewsLevel.BEARISH: -10,
    NewsLevel.STRONG_BEARISH: -20,
}

# 公告类型权重
ANNOUNCEMENT_WEIGHTS = {
    "业绩预告": 1.5,
    "业绩快报": 1.3,
    "年度报告": 1.2,
    "半年度报告": 1.1,
    "季度报告": 1.0,
    "重大合同": 1.4,
    "并购重组": 1.5,
    "股权激励": 1.2,
    "回购": 1.3,
    "增持": 1.2,
    "减持": 1.3,
    "解禁": 1.2,
    "质押": 0.9,
    "诉讼": 1.2,
    "处罚": 1.4,
    "风险提示": 1.3,
}


class NewsSentimentAnalyzer:
    """消息面情绪分析器"""
    
    def __init__(self, delay: float = 0.3):
        self.delay = delay
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        }
        self._org_cache = {}  # 缓存orgId
    
    def _get_org_id(self, code: str) -> Optional[str]:
        """获取股票的orgId"""
        if code in self._org_cache:
            return self._org_cache[code]
        
        url = "http://www.cninfo.com.cn/new/information/topSearch/query"
        params = {"keyWord": code, "maxNum": "10"}
        
        try:
            r = requests.post(url, data=params, headers=self.headers, timeout=10)
            data = r.json()
            for item in data:
                if item.get("code") == code:
                    org_id = item.get("orgId")
                    self._org_cache[code] = org_id
                    return org_id
        except Exception as e:
            print(f"  获取orgId失败: {e}")
        return None
    
    def fetch_announcements(self, code: str, days: int = 30) -> List[Dict]:
        """获取个股公告列表"""
        org_id = self._get_org_id(code)
        if not org_id:
            return []
        
        # 确定板块
        if code.startswith("6"):
            column = "sse"
            plate = "sh"
            column_title = "沪市公告"
        elif code.startswith("0") or code.startswith("3"):
            column = "szse"
            plate = "sz"
            column_title = "深市公告"
        else:
            column = "bjse"
            plate = "bj"
            column_title = "北市公告"
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        se_date = f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"
        
        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        params = {
            "pageNum": "1",
            "pageSize": "50",
            "tabName": "fulltext",
            "column": column,
            "stock": f"{code},{org_id}",
            "searchkey": "",
            "secid": "",
            "plate": plate,
            "category": "category_all",
            "trade": "",
            "columnTitle": column_title,
            "sortName": "",
            "sortType": "",
            "limit": "",
            "showTitle": "",
            "seDate": se_date,
        }
        
        try:
            time.sleep(self.delay)
            r = requests.post(url, data=params, headers=self.headers, timeout=15)
            data = r.json()
            
            announcements = data.get("announcements") or []
            result = []
            for a in announcements:
                # 解析时间戳
                time_str = str(a.get("announcementTime", ""))
                if time_str.isdigit():
                    # 时间戳格式
                    try:
                        dt = datetime.fromtimestamp(int(time_str[:10]))
                        date_str = dt.strftime("%Y-%m-%d")
                    except:
                        date_str = ""
                else:
                    date_str = time_str[:10] if time_str else ""
                
                result.append({
                    "date": date_str,
                    "title": a.get("announcementTitle", ""),
                    "type": self._classify_type(a.get("announcementTitle", "")),
                })
            
            return result
        except Exception as e:
            print(f"  获取公告失败: {e}")
            return []
    
    def _classify_type(self, title: str) -> str:
        """根据标题分类公告类型"""
        title_lower = title.lower()
        
        type_keywords = {
            "业绩预告": ["业绩预告", "业绩预增", "业绩预减"],
            "业绩快报": ["业绩快报"],
            "年度报告": ["年度报告"],
            "半年度报告": ["半年度报告"],
            "季度报告": ["季度报告", "一季度", "三季度"],
            "重大合同": ["重大合同", "中标", "签订"],
            "并购重组": ["重组", "收购", "并购"],
            "股权激励": ["股权激励", "员工持股"],
            "回购": ["回购", "回购股份"],
            "增持": ["增持"],
            "减持": ["减持"],
            "解禁": ["解禁", "限售股"],
            "质押": ["质押"],
            "诉讼": ["诉讼", "仲裁", "纠纷"],
            "处罚": ["处罚", "调查", "立案"],
            "风险提示": ["风险提示", "退市风险"],
        }
        
        for ann_type, keywords in type_keywords.items():
            if any(kw in title for kw in keywords):
                return ann_type
        return "其他"
    
    def analyze_sentiment(self, title: str, ann_type: str = "") -> Tuple[NewsLevel, float]:
        """分析单条公告的情绪等级和分数"""
        text = (title + " " + ann_type).lower()
        
        # 匹配关键词
        level_scores = {}
        for level, keywords in KEYWORD_MAP.items():
            score = sum(2 if kw.lower() in text else 0 for kw in keywords)
            if score > 0:
                level_scores[level] = score
        
        if not level_scores:
            return NewsLevel.NEUTRAL, 0
        
        # 取最高分的等级
        best_level = max(level_scores, key=level_scores.get)
        base_score = LEVEL_BASE_SCORE[best_level]
        
        # 计算权重
        weight = ANNOUNCEMENT_WEIGHTS.get(ann_type, 1.0)
        
        final_score = base_score * weight
        return best_level, final_score
    
    def calculate_sentiment_score(self, code: str, name: str = "") -> SentimentScore:
        """计算个股消息面情绪评分"""
        announcements = self.fetch_announcements(code, days=30)
        
        if not announcements:
            return SentimentScore(
                code=code,
                name=name,
                total_score=50.0,
                level="中性",
                event_count=0,
                bullish_count=0,
                bearish_count=0,
                recent_events=[],
                summary="近期无公告数据"
            )
        
        # 分析每条公告
        news_events = []
        total_raw_score = 0
        bullish_count = 0
        bearish_count = 0
        
        for item in announcements[:20]:  # 最多分析20条
            level, score = self.analyze_sentiment(item["title"], item["type"])
            
            # 时间衰减因子
            days_ago = 0
            try:
                if item["date"]:
                    item_date = datetime.strptime(item["date"], "%Y-%m-%d")
                    days_ago = (datetime.now() - item_date).days
            except:
                pass
            time_decay = max(0.3, 1 - days_ago / 30)
            
            weighted_score = score * time_decay
            
            news_events.append(NewsEvent(
                date=item["date"],
                title=item["title"][:50] + "..." if len(item["title"]) > 50 else item["title"],
                event_type=item["type"],
                level=level,
                score=score,
                weight=time_decay
            ))
            
            total_raw_score += weighted_score
            
            if level.value >= 4:
                bullish_count += 1
            elif level.value <= 0:
                bearish_count += 1
        
        # 标准化到0-100分
        base_score = 50
        adjusted_score = base_score + total_raw_score
        
        # 消息密度加成
        density_factor = min(len(news_events) / 10, 1.0)
        
        # 多空对比调整
        if bullish_count + bearish_count > 0:
            sentiment_ratio = (bullish_count - bearish_count) / (bullish_count + bearish_count)
            adjusted_score += sentiment_ratio * 10 * density_factor
        
        # 限制范围
        final_score = max(0, min(100, adjusted_score))
        
        # 确定等级
        if final_score >= 80:
            level_str = "重大利好"
        elif final_score >= 65:
            level_str = "利好"
        elif final_score >= 55:
            level_str = "偏利好"
        elif final_score >= 45:
            level_str = "中性"
        elif final_score >= 35:
            level_str = "偏利空"
        elif final_score >= 20:
            level_str = "利空"
        else:
            level_str = "重大利空"
        
        # 生成摘要
        summary = self._generate_summary(news_events, final_score, bullish_count, bearish_count)
        
        return SentimentScore(
            code=code,
            name=name,
            total_score=round(final_score, 1),
            level=level_str,
            event_count=len(news_events),
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            recent_events=news_events[:10],
            summary=summary
        )
    
    # 主题词提取映射 - 用于生成概括性摘要
    TOPIC_KEYWORDS = {
        # 业绩类
        "净利润增长": ["净利润增长", "业绩大增", "业绩预增", "扭亏为盈", "大幅增长", "业绩快报", "业绩增长"],
        "营收增长": ["营收增长", "营业收入", "营收同比"],
        "业绩亏损": ["业绩预亏", "大幅亏损", "亏损扩大", "由盈转亏"],
        # 资本运作类
        "股东增持": ["增持", "股东增持", "实控人增持", "控股股东增持"],
        "股东减持": ["减持", "股东减持", "实控人减持", "控股股东减持"],
        "股份回购": ["回购", "股份回购", "回购股份"],
        "股权激励": ["股权激励", "员工持股", "限制性股票", "股票期权"],
        "并购重组": ["并购重组", "重大资产重组", "收购", "并购"],
        "定增": ["非公开发行", "定向增发", "定增"],
        # 业务类
        "重大合同": ["重大合同", "中标", "签订合同", "战略合作协议"],
        "产能扩张": ["产能扩张", "项目投产", "产能释放", "扩产"],
        "技术突破": ["技术突破", "新产品", "专利", "获批", "认证"],
        "市场拓展": ["市场拓展", "新签订单", "业务扩展"],
        # 风险类
        "限售解禁": ["解禁", "限售股", "限售股解禁"],
        "股权质押": ["质押", "股权质押", "补充质押"],
        "法律诉讼": ["诉讼", "仲裁", "纠纷", "索赔"],
        "监管处罚": ["行政处罚", "监管函", "关注函", "问询函", "立案调查"],
        "退市风险": ["退市", "ST风险", "退市风险警示"],
        "债务违约": ["债务违约", "逾期", "破产", "重整"],
        "关联交易": ["关联交易", "资金占用", "违规担保"],
        "停产限产": ["停产", "限产", "环保处罚", "安全事故"],
        "控制权变更": ["实控人变更", "控制权", "股权争斗"],
        # 其他
        "分红派息": ["分红", "派息", "送转", "利润分配"],
        "机构关注": ["机构调研", "券商推荐", "买入评级", "增持评级"],
    }
    
    def _extract_topics(self, events: List[NewsEvent]) -> List[str]:
        """从事件中提取关键主题词"""
        topic_counts = {}
        
        for e in events:
            text = (e.title + " " + e.event_type).lower()
            for topic, keywords in self.TOPIC_KEYWORDS.items():
                for kw in keywords:
                    if kw.lower() in text:
                        topic_counts[topic] = topic_counts.get(topic, 0) + 1
                        break
        
        # 按出现次数排序，取前3个
        sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
        return [t[0] for t in sorted_topics[:3]]
    
    def _generate_summary(self, events: List[NewsEvent], score: float, 
                          bullish: int, bearish: int) -> str:
        """生成消息摘要 - 提取关键主题词"""
        if not events:
            return "近期无公告"
        
        # 提取关键主题
        topics = self._extract_topics(events)
        
        if topics:
            # 根据评分确定基调前缀
            if score >= 65:
                prefix = "利好"
            elif score >= 45:
                prefix = "中性"
            else:
                prefix = "利空"
            
            # 组合主题词
            if len(topics) == 1:
                return f"{prefix}: {topics[0]}"
            else:
                return f"{prefix}: {', '.join(topics[:2])}"
        
        # 无明确主题时，返回数量统计
        if bullish > bearish:
            return f"利好偏多({bullish}利好 vs {bearish}利空)"
        elif bearish > bullish:
            return f"利空偏多({bearish}利空 vs {bullish}利好)"
        else:
            return "多空消息均衡"


def batch_analyze(codes: List[Tuple[str, str]], delay: float = 0.3) -> List[SentimentScore]:
    """批量分析多只股票"""
    analyzer = NewsSentimentAnalyzer(delay=delay)
    results = []
    
    for i, (code, name) in enumerate(codes):
        print(f"[{i+1}/{len(codes)}] 分析 {code} {name}...")
        try:
            result = analyzer.calculate_sentiment_score(code, name)
            results.append(result)
        except Exception as e:
            print(f"  分析失败: {e}")
            results.append(SentimentScore(
                code=code, name=name, total_score=50, level="分析失败",
                event_count=0, bullish_count=0, bearish_count=0,
                recent_events=[], summary=f"获取数据失败: {e}"
            ))
    
    return results


def print_sentiment_report(results: List[SentimentScore]):
    """打印消息面评分报告"""
    print("\n" + "=" * 100)
    print("消息面情绪评分报告")
    print("=" * 100)
    
    # 按分数排序
    sorted_results = sorted(results, key=lambda x: x.total_score, reverse=True)
    
    print(f"\n{'排名':<4} {'代码':<8} {'名称':<10} {'评分':<8} {'等级':<10} {'利好/利空':<12} {'摘要'}")
    print("-" * 100)
    
    for i, r in enumerate(sorted_results, 1):
        bull_bear = f"{r.bullish_count}/{r.bearish_count}"
        print(f"{i:<4} {r.code:<8} {r.name:<10} {r.total_score:<8} {r.level:<10} "
              f"{bull_bear:<12} {r.summary}")
    
    print("\n" + "-" * 100)
    print("评分说明:")
    print("  80-100分: 重大利好 - 有重大积极公告，建议重点关注")
    print("  65-79分:  利好 - 消息偏正面，可纳入考虑")
    print("  55-64分:  偏利好 - 略有积极信号")
    print("  45-54分:  中性 - 无明显消息或多空均衡")
    print("  35-44分:  偏利空 - 略有负面信号")
    print("  20-34分:  利空 - 消息偏负面，需谨慎")
    print("  0-19分:   重大利空 - 有重大负面公告，建议回避")
    print("=" * 100)


# 测试
if __name__ == "__main__":
    # 测试几只股票
    test_codes = [
        ("300750", "宁德时代"),
        ("600519", "贵州茅台"),
        ("000858", "五粮液"),
    ]
    
    results = batch_analyze(test_codes, delay=0.5)
    print_sentiment_report(results)
    
    # 打印详细消息
    print("\n\n详细公告列表:")
    for r in results:
        if r.recent_events:
            print(f"\n{r.code} {r.name} (评分: {r.total_score}, {r.level}):")
            for e in r.recent_events[:5]:
                level_str = "利好" if e.level.value >= 4 else "利空" if e.level.value <= 0 else "中性"
                print(f"  [{e.date}] [{e.event_type}] [{level_str}] {e.title}")