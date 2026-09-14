@echo off
SETLOCAL ENABLEDELAYEDEXPANSION
SET DIR=%~dp0
SET VER=8.4
SET GRADLE_DIR=%DIR%\.gradle\gradle-%VER%
IF NOT EXIST "%GRADLE_DIR%\bin\gradle.bat" (
  echo Installing Gradle %VER% to %GRADLE_DIR%
  mkdir "%DIR%\.gradle" 2>nul || echo.
  powershell -Command "[Net.ServicePointManager]::SecurityProtocol = 'Tls12'; Invoke-WebRequest -Uri 'https://services.gradle.org/distributions/gradle-%VER%-bin.zip' -OutFile '%TEMP%\\gradle.zip'"
  powershell -Command "Expand-Archive -LiteralPath '%TEMP%\\gradle.zip' -DestinationPath '%DIR%\.gradle' -Force"
  del "%TEMP%\gradle.zip"
)
"%GRADLE_DIR%\bin\gradle.bat" %*
