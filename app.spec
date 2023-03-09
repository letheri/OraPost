# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

a.binaries = a.binaries + [('oci.dll','.\\instantclient_19_10\\oci.dll','BINARY')]+[('ocijdbc19.dll','.\\instantclient_19_10\\ocijdbc19.dll','BINARY')]+[('ociw32.dll','.\\instantclient_19_10\\ociw32.dll','BINARY')]+[('orannzsbb19.dll','.\\instantclient_19_10\\orannzsbb19.dll','BINARY')]+[('oraocci19.dll','.\\instantclient_19_10\\oraocci19.dll','BINARY')]+[('oraocci19d.dll','.\\instantclient_19_10\\oraocci19d.dll','BINARY')]+[('oraociicus19.dll','.\\instantclient_19_10\\oraociicus19.dll','BINARY')]+[('oraons.dll','.\\instantclient_19_10\\oraons.dll','BINARY')]+[('orasql19.dll','.\\instantclient_19_10\\orasql19.dll','BINARY')]

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='app',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
