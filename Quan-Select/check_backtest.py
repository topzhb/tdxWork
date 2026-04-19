import backtest
res = backtest.analyze('603876', '2026-03-27', window=10, strategy='classic')

print('回测结果详情:')
print(f'error字段: {res.get("error")}')
print(f'技术分: {res["tech"].get("total")}')
print(f'基本面分: {res["fund"].get("total")}')
print(f'总分: {res.get("total_score")}')
print(f'最终分: {res.get("final_score")}')
print(f'财务数据源: {res["fund"].get("fin_source")}')
print(f'净利同比: {res["fund"].get("profit_yoy")}')
print(f'营收同比: {res["fund"].get("revenue_yoy")}')
print(f'ROE: {res["fund"].get("roe")}')