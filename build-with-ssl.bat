@echo off
rem ===========================================================================
rem Windows 编译脚本（OpenSSL 从源码编译，需要完整的 Perl + MSVC 工具链）
rem
rem 为什么需要它：
rem   1. openssl-src 需要一个「完整」的 Perl。Git 自带的 Perl 是精简版，
rem      缺 ExtUtils::MakeMaker / Locale::Maketext::Simple，会编译失败。
rem   2. OpenSSL 用 nmake 构建，nmake / link.exe / INCLUDE / LIB 都得先就位，
rem      而普通 cmd 窗口里这些都不在 PATH 上。
rem
rem 首次编译 OpenSSL 约 25 分钟，之后增量编译正常。
rem 不要直接 cargo build —— 环境不全时 build script 失败还会清掉已编译产物。
rem ===========================================================================
setlocal

rem --- 1. 找 Perl（优先 scoop 安装的 Strawberry Perl，其次 PATH 里的） --------
set "PERL_EXE="
for /d %%d in ("%USERPROFILE%\scoop\apps\perl\*") do (
  if exist "%%d\perl\bin\perl.exe" set "PERL_EXE=%%d\perl\bin\perl.exe"
)
if not defined PERL_EXE for /f "delims=" %%i in ('where perl 2^>nul') do (
  if not defined PERL_EXE set "PERL_EXE=%%i"
)
if not defined PERL_EXE (
  echo [ERROR] Perl not found. Install a full Perl first: scoop install perl
  exit /b 1
)
for %%i in ("%PERL_EXE%") do set "PERL_DIR=%%~dpi"
echo [info] perl  = %PERL_EXE%

rem openssl-src 会把 PERL 环境变量当作 perl 可执行文件路径，所以这里用
rem OPENSSL_SRC_PERL 传绝对路径，绝不要设置 PERL。
set "OPENSSL_SRC_PERL=%PERL_EXE%"

rem --- 2. 找 MSVC 与 Windows SDK（用 vswhere，不写死版本号） -----------------
set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%VSWHERE%" (
  echo [ERROR] vswhere.exe not found. Install Visual Studio with "Desktop development with C++".
  exit /b 1
)
for /f "usebackq tokens=*" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSPATH=%%i"
if not defined VSPATH (
  echo [ERROR] MSVC toolset not found. Install "Desktop development with C++" workload.
  exit /b 1
)

set "MSVC="
for /d %%d in ("%VSPATH%\VC\Tools\MSVC\*") do set "MSVC=%%d"
if not defined MSVC (
  echo [ERROR] MSVC tools directory missing under %VSPATH%\VC\Tools\MSVC
  exit /b 1
)
echo [info] msvc  = %MSVC%

set "SDK=%ProgramFiles(x86)%\Windows Kits\10"
set "SDKVER="
for /d %%d in ("%SDK%\Include\10.*") do set "SDKVER=%%~nxd"
if not defined SDKVER (
  echo [ERROR] Windows 10/11 SDK not found under %SDK%\Include
  exit /b 1
)
echo [info] sdk   = %SDKVER%

set "PATH=%PERL_DIR%;%MSVC%\bin\Hostx64\x64;%SDK%\bin\%SDKVER%\x64;%PATH%"
set "INCLUDE=%MSVC%\include;%SDK%\Include\%SDKVER%\ucrt;%SDK%\Include\%SDKVER%\um;%SDK%\Include\%SDKVER%\shared;%SDK%\Include\%SDKVER%\winrt"
set "LIB=%MSVC%\lib\x64;%SDK%\Lib\%SDKVER%\ucrt\x64;%SDK%\Lib\%SDKVER%\um\x64"

cd /d "%~dp0"
cargo build --release %*
exit /b %ERRORLEVEL%
