@echo off
cd /d "%~dp0"

:: 计算今日日期和 Run ID
for /f %%a in ('powershell -command "Get-Date -Format 'yyyyMMdd'"') do set TODAY=%%a
set RUN_ID=%TODAY%_HIST

:: Python 可用性检查
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  [错误] 未找到 Python，请确保已安装并加入 PATH
    echo.
    pause
    exit /b 1
)

:MENU
cls
echo.
echo  ========================================================================
echo   概念板块分析工具  ^|  主菜单
echo   今日日期: %TODAY%    Run ID: %RUN_ID%
echo  ========================================================================
echo.
echo    日常流程
echo    -------
echo    1.  每日完整流程      [分析 + 报告]  每日收盘后常规操作
echo    2.  仅执行数据分析[NO SKIP]
echo    3.  仅生成 HTML 报告  [使用收盘数据]
echo    4.  盘中实时报告      [使用盘中缓存数据]
echo.
echo    扩展选项
echo    -------
echo    5.  指定日期生成报告  [手动输入 Run ID]
echo    6.  生成完整历史报告  [--full 模式，近三个月数据]
echo    7.  分析 + 报告 + 导出 Excel
echo.
echo    维护操作
echo    -------
echo    8.  更新概念板块数据  [仅在概念板块.txt 有变化时执行]
echo    9.  补录历史数据      [指定天数回填]
echo.
echo    0.  退出
echo.
echo  ========================================================================
echo.
set /p CHOICE= 请输入选项 (0-9): 

if "%CHOICE%"=="1" goto FULL_DAILY
if "%CHOICE%"=="2" goto ANALYSIS_ONLY
if "%CHOICE%"=="3" goto REPORT_ONLY
if "%CHOICE%"=="4" goto REPORT_INTRADAY
if "%CHOICE%"=="5" goto REPORT_CUSTOM
if "%CHOICE%"=="6" goto REPORT_FULL
if "%CHOICE%"=="7" goto REPORT_EXPORT
if "%CHOICE%"=="8" goto UPDATE_CONCEPT
if "%CHOICE%"=="9" goto BACKFILL
if "%CHOICE%"=="0" goto EXIT

echo.
echo  [无效输入] 请重新选择...
timeout /t 1 >nul
goto MENU


:: ========================================================================
:FULL_DAILY
:: ========================================================================
echo.
echo  ========================================================================
echo   每日完整流程  [分析 + HTML报告 + 趋势分析]
echo  ========================================================================
echo.
echo  [1/22] 日常数据分析...
echo.
python concept_tool.py backfill --days 1
if errorlevel 1 (
    echo.
    echo  [失败] 日常分析失败，流程中止
    echo         请确认通达信已运行且日线数据已更新
    echo.
    pause
    goto DONE
)
echo  [OK] 步骤 1 完成

echo.
echo  [2/2] 生成 HTML 报告...
echo.
python gen_concept_html_V1.3.py --run-id %RUN_ID%
if errorlevel 1 (
    echo  [警告] HTML 报告生成失败，继续后续步骤
) else (
    echo  [OK] 步骤 2 完成
)

call :OPEN_REPORT
goto DONE


:: ========================================================================
:ANALYSIS_ONLY
:: ========================================================================
echo.
echo  ========================================================================
echo   仅执行数据分析
echo  ========================================================================
echo.
python concept_tool.py backfill --days 1 --no-skip
if errorlevel 1 (
    echo.
    echo  [失败] 数据分析失败，请检查通达信是否已运行
    echo.
    pause
) else (
    echo.
    echo  [OK] 数据分析完成
    echo       下一步建议：选项 3 生成 HTML 报告
)
goto DONE


:: ========================================================================
:REPORT_ONLY
:: ========================================================================
echo.
echo  ========================================================================
echo   仅生成 HTML 报告  [收盘数据]
echo  ========================================================================
echo.
echo  Run ID: %RUN_ID%
echo.
python gen_concept_html_V1.3.py --run-id %RUN_ID%
if errorlevel 1 (
    echo.
    echo  [失败] 报告生成失败，请确认数据库中有今日数据（先运行选项 2）
    echo.
    pause
) else (
    echo  [OK] 报告已生成
    call :OPEN_REPORT
)
goto DONE


:: ========================================================================
:REPORT_INTRADAY
:: ========================================================================
echo.
echo  ========================================================================
echo   盘中实时报告  [使用通达信缓存数据]
echo  ========================================================================
echo.
echo  Run ID: %RUN_ID%  (盘中模式)
echo.
python gen_concept_html_V1.3.py --run-id %RUN_ID% --intraday
if errorlevel 1 (
    echo.
    echo  [失败] 报告生成失败，请确认通达信正在运行且有盘中缓存数据
    echo.
    pause
) else (
    echo  [OK] 盘中报告已生成
    call :OPEN_REPORT
)
goto DONE


:: ========================================================================
:REPORT_CUSTOM
:: ========================================================================
echo.
echo  ========================================================================
echo   指定日期生成报告
echo  ========================================================================
echo.
echo  格式示例: 20260325_HIST
echo.
set /p CUSTOM_ID= 请输入 Run ID: 
if "%CUSTOM_ID%"=="" (
    echo  [取消] 未输入 Run ID
    goto DONE
)
echo.
python gen_concept_html_V1.3.py --run-id %CUSTOM_ID%
if errorlevel 1 (
    echo.
    echo  [失败] 报告生成失败，请确认该 Run ID 在数据库中存在
    echo.
    pause
) else (
    echo  [OK] 报告已生成
    set CUSTOM_DATE=%CUSTOM_ID:~0,8%
    if exist "ConceptReport\concept_report_%CUSTOM_DATE%.html" (
        start "" "ConceptReport\concept_report_%CUSTOM_DATE%.html"
    )
)
goto DONE


:: ========================================================================
:REPORT_FULL
:: ========================================================================
echo.
echo  ========================================================================
echo   完整历史报告  [近三个月数据，--full 模式]
echo  ========================================================================
echo.
echo  注意：此模式读取近三个月所有数据，生成时间较长...
echo.
python gen_concept_html_V1.3.py --full
if errorlevel 1 (
    echo.
    echo  [失败] 完整报告生成失败
    echo.
    pause
) else (
    echo  [OK] 完整历史报告已生成
    if exist "ConceptReport\concept_report_full.html" (
        start "" "ConceptReport\concept_report_full.html"
    )
)
goto DONE


:: ========================================================================
:REPORT_EXPORT
:: ========================================================================
echo.
echo  ========================================================================
echo   数据分析 + HTML报告 + 导出 Excel
echo  ========================================================================
echo.
echo  [1/2] 日常数据分析...
echo.
python concept_tool.py backfill --days 1
if errorlevel 1 (
    echo.
    echo  [失败] 数据分析失败，流程中止
    echo.
    pause
    goto DONE
)
echo  [OK] 步骤 1 完成
echo.
echo  [2/2] 生成 HTML 报告并导出 Excel...
echo.
python gen_concept_html_V1.3.py --run-id %RUN_ID% --export
if errorlevel 1 (
    echo.
    echo  [失败] 报告或 Excel 生成失败
    echo.
    pause
) else (
    echo  [OK] 报告和 Excel 均已生成
    call :OPEN_REPORT
)
goto DONE


:: ========================================================================
:UPDATE_CONCEPT
:: ========================================================================
echo.
echo  ========================================================================
echo   更新概念板块基础数据  [低频操作]
echo  ========================================================================
echo.
echo  ! 注意：此操作将清空并重建数据库中的板块基础数据
echo  ! 触发时机：概念板块.txt 有新版本或内容变化时
echo.
if not exist "..\db\概念板块.txt" (
    echo  [失败] 未找到数据源文件 ..\db\概念板块.txt，请先放置最新文件
    echo.
    pause
    goto DONE
)
set /p CONFIRM= 确认执行？(Y/N): 
if /i not "%CONFIRM%"=="Y" (
    echo  已取消
    goto DONE
)
echo.
python update_concept_data_with_filter.py
if errorlevel 1 (
    echo.
    echo  [失败] 概念数据更新失败，请检查日志
    echo.
    pause
) else (
    echo.
    echo  [OK] 概念板块数据更新成功
    echo       建议接着执行选项 1 重新生成报告
)
goto DONE


:: ========================================================================
:BACKFILL
:: ========================================================================
echo.
echo  ========================================================================
echo   补录历史数据
echo  ========================================================================
echo.
echo  输入要补录的天数（例如：5 表示补录最近5个交易日）
echo.
set /p DAYS= 请输入天数: 
if "%DAYS%"=="" (
    echo  [取消] 未输入天数
    goto DONE
)
echo.
python concept_tool.py backfill --days %DAYS% --no-skip
if errorlevel 1 (
    echo.
    echo  [失败] 补录失败，请确认通达信已运行
    echo.
    pause
) else (
    echo.
    echo  [OK] 补录完成，共处理 %DAYS% 天数据
    echo       建议接着执行选项 3 生成今日报告
)
goto DONE


:: ========================================================================
:EXIT
:: ========================================================================
echo.
echo  再见！
echo.
exit /b 0


:: ========================================================================
:: 公共子程序
:: ========================================================================

:OPEN_REPORT
if exist "ConceptReport\concept_report_%TODAY%.html" (
    echo.
    echo  正在打开报告: ConceptReport\concept_report_%TODAY%.html
    start "" "ConceptReport\concept_report_%TODAY%.html"
)
goto :EOF

:DONE
echo.
echo  ------------------------------------------------------------------------
echo.
set BACK=
set /p BACK= 按 Enter 或者 输入 0 退出: 
if "%BACK%"=="0" goto EXIT
goto EXIT
