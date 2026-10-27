"""仅抽取生产check_cancel；真实socket字节/EOF，不导入torch或模型。"""
import ast
import os
from pathlib import Path
import select
import socket
import sys
import types

source=Path(sys.argv[1])
tree=ast.parse(source.read_text())
service=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='Service')
method=next(node for node in service.body if isinstance(node,ast.FunctionDef) and node.name=='check_cancel')
namespace={'os':os,'select':select}
exec(compile(ast.fix_missing_locations(ast.Module(body=[method],type_ignores=[])),str(source),'exec'),namespace)
for mode in ('byte','eof'):
    reader,writer=socket.socketpair()
    try:
        state=types.SimpleNamespace(cancel_fd=reader.fileno(),cancelled=False)
        namespace['check_cancel'](state)
        if mode=='byte': writer.send(b'!')
        else: writer.close()
        try: namespace['check_cancel'](state)
        except InterruptedError: pass
        else: raise AssertionError(mode+' did not stop')
    finally:
        reader.close()
        writer.close()
print('production check_cancel: byte and EOF stop; no torch imported')
