@echo off
REM === Build ChanlunX 32-bit DLL for TDX ===
REM 编译环境: VS2019 BuildTools (x64 host → x86 target 交叉编译)
REM 输出: E:\workClaw\tdx-dll\output\ChanlunX.dll
REM 部署: 复制到通达信 T0002\dlls\ 目录，公式管理器绑定即可

set "MSVC=C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\MSVC\14.29.30133"
set "SDK=C:\Program Files (x86)\Windows Kits\10"

set "CL_EXE=%MSVC%\bin\Hostx64\x86\cl.exe"
set "PATH=%MSVC%\bin\Hostx64\x86;%MSVC%\bin\Hostx64;%PATH%"
set "INCLUDE=%MSVC%\include;%SDK%\Include\10.0.19041.0\ucrt;%SDK%\Include\10.0.19041.0\um;%SDK%\Include\10.0.19041.0\shared"
set "LIB=%MSVC%\lib\x86;%SDK%\Lib\10.0.19041.0\ucrt\x86;%SDK%\Lib\10.0.19041.0\um\x86"

if not exist "E:\workClaw\tdx-dll\output" mkdir "E:\workClaw\tdx-dll\output"
cd /d "%~dp0"

echo Building ChanlunX 32-bit DLL...
"%CL_EXE%" /nologo /W3 /O2 /MT /LD /EHsc /utf-8 ^
    Main.cpp Bi.cpp Duan.cpp ZhongShu.cpp KxianChuLi.cpp BiChuLi.cpp ^
    /Fe:"E:\workClaw\tdx-dll\output\ChanlunX.dll" ^
    /Fo:"E:\workClaw\tdx-dll\output\"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================
    echo  Build SUCCESS
    echo  Output: E:\workClaw\tdx-dll\output\ChanlunX.dll
    echo ============================================
    echo.
    echo 部署步骤:
    echo   1. 复制 ChanlunX.dll 到通达信 T0002\dlls\ 目录
    echo   2. 在公式管理器中绑定 DLL，函数编号 1-9:
    echo      1=简笔  2=标准笔  3=段(标准)  4=段(1+1终结)
    echo      5=中枢高  6=中枢低  7=中枢起止  8=中枢方向  9=同向第几中枢
    echo.
) else (
    echo.
    echo ============================================
    echo  Build FAILED (exit code: %ERRORLEVEL%)
    echo ============================================
)
