from PyInstaller.utils.hooks import collect_all
a = Analysis(
    ['src/dittofs/cli.py'],
    binaries=[],
    datas=[],
    hiddenimports=['pyfuse3'],
    name='dittofs',
)