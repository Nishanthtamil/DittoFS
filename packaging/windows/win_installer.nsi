!define PRODUCT "DittoFS"
!define VERSION "0.1.0"
OutFile "dist\dittofs-setup.exe"
InstallDir "$PROGRAMFILES\DittoFS"

Section
  SetOutPath "$INSTDIR"
  File "dist\dittofs.exe"
  CreateShortcut "$DESKTOP\DittoFS.lnk" "$INSTDIR\dittofs.exe" "tray"
  WriteRegStr HKCU "Software\Microsoft\Windows\CurrentVersion\Run" "DittoFS" '"$INSTDIR\dittofs.exe" tray'
SectionEnd