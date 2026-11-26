@echo off
rem ===========================================================================
rem Windows build helper: OpenSSL is compiled from source, which needs a full
rem Perl plus the MSVC toolchain on PATH.
rem
rem Why this script exists:
rem   1. openssl-src needs a *complete* Perl. The Perl bundled with Git for
rem      Windows is trimmed and lacks ExtUtils::MakeMaker / Locale::Maketext,
rem      so the build fails. Install a real one:  scoop install perl
rem   2. OpenSSL is built with nmake, so nmake / link.exe / INCLUDE / LIB must
rem      already be in the environment. This script sets them up.
rem
rem First build compiles OpenSSL and takes ~25 minutes; later builds are fast.
rem Do NOT run a bare "cargo build" on Windows: with an incomplete environment
rem the OpenSSL build script fails and wipes already-built artifacts.
rem ===========================================================================
setlocal

rem --- 1. locate Perl (scoop install first, then PATH) ------------------------
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
echo [info] perl = %PERL_EXE%

rem openssl-src reads the PERL env var as the perl executable path, so pass the
rem absolute path through OPENSSL_SRC_PERL and never set PERL itself.
set "OPENSSL_SRC_PERL=%PERL_EXE%"

rem --- 2. locate MSVC and the Windows SDK via vswhere (no hardcoded versions) -
set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%VSWHERE%" (
  echo [ERROR] vswhere.exe not found. Install Visual Studio with the
  echo         "Desktop development with C++" workload.
  exit /b 1
)
set "VSPATH="
for /f "usebackq tokens=*" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSPATH=%%i"
if not defined VSPATH (
  echo [ERROR] MSVC toolset not found. Install the "Desktop development with C++" workload.
  exit /b 1
)

set "MSVC="
for /d %%d in ("%VSPATH%\VC\Tools\MSVC\*") do set "MSVC=%%d"
if not defined MSVC (
  echo [ERROR] MSVC tools directory missing under "%VSPATH%\VC\Tools\MSVC"
  exit /b 1
)
echo [info] msvc = %MSVC%

set "SDK=%ProgramFiles(x86)%\Windows Kits\10"
set "SDKVER="
for /d %%d in ("%SDK%\Include\10.*") do set "SDKVER=%%~nxd"
if not defined SDKVER (
  echo [ERROR] Windows 10/11 SDK not found under "%SDK%\Include"
  exit /b 1
)
echo [info] sdk  = %SDKVER%

set "PATH=%PERL_DIR%;%MSVC%\bin\Hostx64\x64;%SDK%\bin\%SDKVER%\x64;%PATH%"
set "INCLUDE=%MSVC%\include;%SDK%\Include\%SDKVER%\ucrt;%SDK%\Include\%SDKVER%\um;%SDK%\Include\%SDKVER%\shared;%SDK%\Include\%SDKVER%\winrt"
set "LIB=%MSVC%\lib\x64;%SDK%\Lib\%SDKVER%\ucrt\x64;%SDK%\Lib\%SDKVER%\um\x64"

cd /d "%~dp0"
cargo build --release %*
exit /b %ERRORLEVEL%
