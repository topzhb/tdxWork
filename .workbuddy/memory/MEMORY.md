# MEMORY.md - 项目长期记忆

## 项目概况
- **tdxWork** (e:\tdxWork) 包含两个子项目：
  - **Quan-Select**：见龙在田精选系统，Python 量化选股工具
  - **tdx-analysis**：通达信板块分析工具
- 两个项目共享 `db/` 目录下的 SQLite 数据库

## 配置架构
- Quan-Select 集中配置：`qs_config.py`（通达信路径等）
- tdx-analysis 集中配置：`tool_config.py`（通达信路径等）
- tdx-analysis 还有 `config.py`（优先从 tool_config 导入 VIPDOC_DIR）
- 所有脚本的通达信路径统一从配置文件导入，回退值使用通用示例 `C:\TongDaXin\vipdoc`

## GitHub 发布准备
- 2026-04-19 完成发布前清理：
  - 创建根目录 `.gitignore`（排除 .rar、db/*.db、db/*.json、__pycache__ 等）
  - 所有硬编码路径 `F:\tongdx\main7.62\vipdoc` 已清除，改为从配置文件导入或通用示例
  - `最佳核心分析流程.md` 示例路径改为占位符
  - `RELEASE_NOTES.md` 变更表格中旧路径脱敏
  - 敏感路径残留仅存在于 .gitignore 排除的文件中（真实核心任务制品清单.md、picks/*.html）
- 仍缺 LICENSE 文件（建议改进，非阻塞）
- tdx-analysis/.gitignore 建议补充 `*.log` 规则
