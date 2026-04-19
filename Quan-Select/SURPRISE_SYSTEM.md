# 超预期/预期差体系 — 完整参考

> 整理日期：2026-04-17（v2 更新：明确各策略实际可用组合）
> 核心原则：**surprise 不是独立策略，而是附加属性**（附加到 classic/growth 上才有意义）
> single_line 自带独立 qdiff 维度，不参与组合体系

---

## 一、两个维度，两个参数

| 参数 | 选项 | 控制什么 |
|------|------|---------|
| **`--surprise-mode`** | `forward` / `actual` | **方向**：正值意味着什么？ |
| **`--qdiff-mode`** | `quarter` / `ttm` | **口径**：用哪两套数据来比？ |

---

## 二、策略实际支持矩阵（重要！）

**不是所有理论组合都适用于所有策略**。代码实现的事实如下：

| 参数 | **surprise**<br>(pure/combo) | **single_line** | **classic / growth**<br>(无 surprise) |
|------|---------------------------|-----------------|-------------------------------------|
| `--surprise-mode` | ✅ 生效 | ✅ 生效 | ❌ 不适用 |
| `--qdiff-mode` | ❌ **固定 TTM**<br>(无此概念) | ✅ **quarter / ttm** | ❌ 不适用 |
| 数据源 | expect_yoy vs ttm_yoy<br>（年度EPS预期 vs TTM实际） | quarter: expected_np vs profit_single_q<br>ttm: expect_yoy vs ttm_yoy | — |

### 为什么 surprise 策略没有 qdiff_mode？

两套机制的设计定位不同：

| | **surprise 策略的"超预期信号"(30分)** | **single_line 的"季度预期差"(8分)** |
|--|--|--|
| 定位 | 整个策略的核心维度，100分里占30分 | 7个维度之一，8/100分 |
| 视角 | **年度级**：券商对全年业绩的一致预期 | **季度级**：单季利润 vs 季度预测 |
| 数据 | 东财一致预期 API (RPT_WEB_RESPREDICT) | report_rc 表 (东财季度一致预期) + 通达信单季 |
| 口径切换 | 无需切换，固定看年度预期 vs TTM | 支持 quarter(默认) / ttm 两种 |

**简单说：surprise 策略本身就是"年度预期 vs TTM"，不存在季度口径的概念；single_line 才是同时覆盖两个口径的策略。**

---

## 三、四种理论组合 → 实际可用映射

### 组合 1：`forward + quarter`

```
命令：python main.py --fund-strategy single_line                    （默认值，回车即得）
```

| 项目 | 内容 |
|------|------|
| **对比对象** | 券商季度净利润预期(expected_np) vs 实际单季利润(profit_single_q) |
| **数据来源** | report_rc 表（东财季度一致预期） vs 通达信财务 zip 单季数据 |
| **forward 含义** | `qdiff = -deviation = 预期 - 实际方向` → **正值=市场预期未来更好** |
| **找什么** | **"预期差催化"**——实际低于券商季度预测，说明市场认为后续会改善 |
| **典型场景** | 财报发布前后，找被低估的催化标的 |
| **✅ 适用策略** | **single_line only** |
| **❌ 不适用** | surprise / classic,surprise / growth,surprise（这些固定用 TTM） |

**举例**：券商预计本季净利 1000万，实际 800万
- deviation = (800-1000)/1000 = -20%
- **forward: qdiff = +20% → "市场预期加速"，强催化信号 ✅**

---

### 组合 2：`actual + quarter`

```
命令：python main.py --fund-strategy single_line --surprise-mode actual
```

| 项目 | 内容 |
|------|------|
| **对比对象** | 同上：expected_np vs profit_single_q |
| **actual 含义** | `qdiff = deviation = 实际 - 预期` → **正值=财报跑赢预期** |
| **找什么** | **"惊喜验证"**——实际业绩超过券商预测，财报超预期确认 |
| **典型场景** | 财报已发布后，验证是否真超预期 |
| **✅ 适用策略** | **single_line only** |

**举例**：券商预计 1000万，实际 1200万
- **actual: qdiff = +20% → "Q超预期+20% 强催化" ✅**

---

### 组合 3：`forward + ttm`

```
命令：python main.py --fund-strategy classic,surprise              （surprise 默认 forward+TTM）
       python main.py --fund-strategy growth,surprise               （同上）
       python main.py --fund-strategy surprise                     （纯 surprise，默认）
       python main.py --fund-strategy single_line --qdiff-mode ttm （single_line 显式指定）
```

| 项目 | 内容 |
|------|------|
| **对比对象** | 年度EPS预期增速(expect_yoy) vs TTM实际净利增速(ttm_yoy) |
| **数据来源** | 东财一致预期 API (RPT_WEB_RESPREDICT) vs 通达信 zip TTM 计算 |
| **forward 含义** | `qdiff = expect_yoy - ttm_yoy` → **正值=券商预期未来加速** |
| **找什么** | **"年度预期加速"**——一致预期增速高于当前TTM实际增速 |
| **典型场景** | 中长线判断：市场对公司未来两年的预期是否比当前更强 |
| **✅ 适用策略** | **全部含超预期的策略**（surprise/combo/single_line 均可） |

**举例**：expect_yoy=30%（券商预期今年+30%），ttm_yoy=15%（TTM实际只+15%）
- **forward: qdiff = +15% → "TTM预期差+15% 催化" ✅**

---

### 组合 4：`actual + ttm`

```
命令：python main.py --fund-strategy classic,surprise --surprise-mode actual
       python main.py --fund-strategy surprise --surprise-mode actual
       python main.py --fund-strategy single_line --qdiff-mode ttm --surprise-mode actual
```

| 项目 | 内容 |
|------|------|
| **对比对象** | 同上：expect_yoy vs ttm_yoy |
| **actual 含义** | `qdiff = ttm_yoy - expect_yoy` → **正值=实际跑赢年度预期** |
| **找什么** | **"TTM超预期"**——滚动利润增速已经超过了券商年度预期 |
| **典型场景** | 验证公司业绩是否在加速赶超市场预期 |
| **✅ 适用策略** | **全部含超预期的策略** |

**举例**：expect_yoy=30%，ttm_yoy=45%
- **actual: qdiff = +15% → "TTM超预期+15% 显著" ✅**

---

## 四、速查表

| | **quarter**（单季口径） | **ttm**（年度口径） |
|--|--|--|
| **forward**（前向/前瞻）<br>正值=预期>实际<br>→ 找**催化/预期差** | **Q1. forward+quarter** 🟢<br>仅 single_line<br>券商季度预期 vs 单季实际<br>👉 短线首选 | **Q3. forward+ttm** 🔵<br>全策略通用<br>年度EPS预期 vs TTM实际<br>👉 中线趋势判断 |
| **actual**（后向/验证）<br>正值=实际>预期<br>→ 找**超预期/惊喜** | **Q2. actual+quarter** 🟢<br>仅 single_line<br>券商季度预期 vs 单季实际<br>👉 财报季后验证 | **Q4. actual+ttm** 🔵<br>全策略通用<br>年度EPS预期 vs TTM实际<br>👉 业绩加速确认 |

图例：
- 🟢 = 仅 single_line 可用（需 `--qdiff-mode quarter` 或默认值）
- 🔵 = 全部策略通用（surprise 系默认就是 TTM；single_line 需 `--qdiff-mode ttm`）

---

## 五、各场景推荐配置

### 场景 A：日常精选（中长线保护）

```bash
# 稳健 + 超预期特征（默认 forward+TTM）
python main.py --fund-strategy classic,surprise

# 成长 + 超预期特征（默认 forward+TTM）
python main.py --fund-strategy growth,surprise
```
- surprise 作为附加层，组合平均评分
- forward 模式找"基本面好 + 有催化空间"的标的
- **注意：这里用的是 TTM 口径（Q3），不是 quarter！**

### 场景 B：短线爆发

```bash
# 默认就是 forward+quarter（Q1，最灵敏的短线催化信号）
python main.py --fund-strategy single_line

# 改用 ttm 口径看中期趋势（Q3）
python main.py --fund-strategy single_line --qdiff-mode ttm

# 财报季后验证模式（Q2）
python main.py --fund-strategy single_line --surprise-mode actual
```
- single_line 内置 qdiff（8分），不需要额外加 surprise
- quarter 更短线灵敏，ttm 更偏中线趋势

### 场景 C：财报季后扫雷（surprise_only）

```bash
# 扫描所有 diff>0 的股票（不限TOP，forward+TTM 即 Q3）
python main.py --fund-strategy surprise              # forward+ttm（默认）

# 改为 actual 验证模式（Q4，找已确认超预期的）
python main.py --fund-strategy surprise --surprise-mode actual
```
- 纯 surprise 策略自动触发 surprise_only 行为（过滤 diff>0 + 不截断）
- 固定使用 TTM 口径，**不支持 quarter**
- 定位是"筛选器"而非"排名器"

### 场景 D：个股诊断回测

```bash
# 回测某股在历史日期的评分
python backtest.py --code 300243 --date 2026-04-17 --fund-strategy classic,surprise
```
- 前端可切换 strategy / surprise_mode / qdiff_mode
- single_line 时 qdiff 口径选择器可用
- surprise / combo 时 qdiff 选择器无意义（自动隐藏或灰显）

---

## 六、菜单交互设计（daily_analysis.bat）

```
=======================================================================
 Fund Strategy (surprise is an add-on, not standalone)
=======================================================================

 --- Mid/Long-term Protection ---
    1. classic              Stable Value               [default]
    2. classic + surprise   Stable + Catalyst          (TTM only, Q3/Q4)
    3. growth               Bull Growth
    4. growth + surprise    Growth + Catalyst           (TTM only, Q3/Q4)

 --- Short-term Momentum ---
    5. single_line         Short-line Burst            (built-in qdiff)

 --- Screening Tool ---
    6. surprise only       Surprise Scanner            (TTM only, no TOP limit)

选 [5] 后出现 [A] qdiff 口径选择（quarter/ttm）：
选任意后出现 [B] 方向选择（forward/actual）：全策略共用
```

**关键点**：
- 选 2/4/6 时，[A] qdiff 口径选择**不应出现**（因为只有 TTM）
- 只有选 5(single_line) 时才显示 [A]
- [B] 方向选择始终出现（所有策略都支持 forward/actual）

---

## 七、参数传递链路

```
main.py 命令行参数
  ├── --surprise-mode   → main() → step_score() → fund_score_combo()
  │                        ↓                ↓                ├──→ _surprise()          ← 用 TTM
  │                        ↓                ↓                └──→ _single_line()      ← 用 qdiff_mode 决定
  │                     backtest.py      score.py
  │                        ↓
  └── --qdiff-mode      → analyze()    → fund_strategies.py
                             ↓              └──→ _single_line(): if qdiff_mode=="ttm" → TTM路径
                                                └──→ _single_line(): else          → quarter路径
                                               （_surprise() 完全忽略此参数）
```

**关键规则**：
1. `--surprise-mode` 同时影响 **surprise 策系** 和 **single_line 的 qdiff**
2. `--qdiff-mode` **仅影响 single_line**（surprise/combo 固定走 TTM，不读此参数）
3. classic/growth 本身不用这两个参数（通过 combo 附加上 surprise 时才间接生效）

---

## 八、代码位置索引

| 函数 | 文件:行号 | 作用 |
|------|----------|------|
| `_calc_surprise_diff()` | fund_strategies.py:1300 | 核心 diff 计算，surprise 策系专用（expect_yoy vs ttm_yoy） |
| `_apply_qdiff_score()` | fund_strategies.py:1496 | single_line qdiff 分档与标签输出 |
| `_surprise()` | fund_strategies.py:1336 | 纯 surprise 策略评分（100分制） |
| `_single_line()` | fund_strategies.py:1530 | single_line 策略评分（内置 qdiff 8分维度） |
| `fund_score_combo()` | fund_strategies.py:1815 | 策略分发：单策略直跑，多策略取平均 |
| `fetch_consensus_eps()` | fund_strategies.py:308 | 获取东财年度一致预期（expect_yoy） |
| `fetch_quarterly_consensus()` | score.py | 获取东财季度一致预期（expected_np，single_line quarter 模式用） |
| `calc_ttm_profit_growth()` | fund_strategies.py:853 | TTM 滚动净利同比计算 |

---

*本文档是超预期体系的完整参考。修改任何相关逻辑前应先更新此文档。*
