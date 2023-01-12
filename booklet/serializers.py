#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 13 09:41:51 2023

@author: mike
"""
import io
import pickle
import json

imports = set()
try:
    import orjson
    imports.add('orjson')
except:
    pass
try:
    import zstandard as zstd
    imports.add('zstd')
except:
    pass
try:
    import numpy as np
    imports.add('numpy')
except:
    pass
try:
    import pandas as pd
    imports.add('pandas')
except:
    pass
try:
    import geopandas as gpd
    imports.add('geopandas')
except:
    pass
try:
    import pyarrow
    imports.add('pyarrow')
except:
    pass
# try:
#     import lz4
#     imports['lz4'] = True
# except:
#     imports['lz4'] = False


#######################################################
### Serializers

class Pickle:
    def dumps(obj):
        return pickle.dumps(obj, 5)
    def loads(obj):
        return pickle.loads(obj)

class Json:
    def dumps(obj):
        return json.dumps(obj).encode()
    def loads(obj):
        return json.loads(obj.decode())

class Orjson:
    def dumps(obj):
        return orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY)
    def loads(obj):
        return orjson.loads(obj)

class Str:
    def dumps(obj):
        return obj.encode()
    def loads(obj):
        return obj.decode()

class PickleZstd:
    def dumps(obj):
        return zstd.compress(pickle.dumps(obj, 5), 1)
    def loads(obj):
        return pickle.loads(zstd.decompress(obj))

class OrjsonZstd:
    def dumps(obj):
        return zstd.compress(orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY), 1)
    def loads(obj):
        return orjson.loads(zstd.decompress(obj))

class NumpyInt1:
    def dumps(obj):
        return obj.astype('i1').tobytes()
    def loads(obj):
        return np.frombuffer(obj, 'i1')

class NumpyInt2:
    def dumps(obj):
        return obj.astype('i2').tobytes()
    def loads(obj):
        return np.frombuffer(obj, 'i2')

class NumpyInt4:
    def dumps(obj):
        return obj.astype('i4').tobytes()
    def loads(obj):
        return np.frombuffer(obj, 'i4')

class NumpyInt8:
    def dumps(obj):
        return obj.astype('i8').tobytes()
    def loads(obj):
        return np.frombuffer(obj, 'i8')

class NumpyInt2Zstd:
    def dumps(obj):
        return zstd.compress(obj.astype('i2').tobytes(), 1)
    def loads(obj):
        return np.frombuffer(zstd.decompress(obj), 'i2')

class NumpyInt4Zstd:
    def dumps(obj):
        return zstd.compress(obj.astype('i4').tobytes(), 1)
    def loads(obj):
        return np.frombuffer(zstd.decompress(obj), 'i4')

class NumpyInt8Zstd:
    def dumps(obj):
        return zstd.compress(obj.astype('i8').tobytes(), 1)
    def loads(obj):
        return np.frombuffer(zstd.decompress(obj), 'i8')

class Uint1:
    def dumps(obj):
        return obj.to_bytes(1, 'little', signed=False)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=False)

class Int1:
    def dumps(obj):
        return obj.to_bytes(4, 'little', signed=True)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=True)

class Uint2:
    def dumps(obj):
        return obj.to_bytes(2, 'little', signed=False)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=False)

class Int2:
    def dumps(obj):
        return obj.to_bytes(2, 'little', signed=True)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=True)

class Uint4:
    def dumps(obj):
        return obj.to_bytes(4, 'little', signed=False)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=False)

class Int4:
    def dumps(obj):
        return obj.to_bytes(4, 'little', signed=True)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=True)

class Uint5:
    def dumps(obj):
        return obj.to_bytes(5, 'little', signed=False)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=False)

class Int5:
    def dumps(obj):
        return obj.to_bytes(5, 'little', signed=True)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=True)

class Uint8:
    def dumps(obj):
        return obj.to_bytes(8, 'little', signed=False)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=False)

class Int8:
    def dumps(obj):
        return obj.to_bytes(8, 'little', signed=True)
    def loads(obj):
        return int.from_bytes(obj, 'little', signed=True)

class GpdZstd:
    def dumps(obj):
        b1 = io.BytesIO()
        obj.to_feather(b1, compression='zstd', compression_level=1)
        b1.seek(0)
        return b1.read()
    def loads(obj):
        b1 = io.BytesIO(memoryview(obj))
        out = gpd.read_feather(b1)
        return out

class PdZstd:
    def dumps(obj):
        b1 = io.BytesIO()
        obj.to_feather(b1, compression='zstd', compression_level=1)
        b1.seek(0)
        return b1.read()
    def loads(obj):
        b1 = io.BytesIO(memoryview(obj))
        out = pd.read_feather(b1)
        return out


##########################################
## Serializer dict

serial_dict = {None: None, 'str': Str, 'pickle': Pickle, 'json': Json, 'orjson': Orjson, 'uint1': Uint1, 'int1': Int1, 'uint2': Uint2, 'int2': Int2, 'uint4': Uint4, 'int4': Int4, 'uint5': Uint5, 'int5': Int5, 'uint8': Uint8, 'int8': Int8, 'pickle_zstd': PickleZstd, 'orjson_zstd': OrjsonZstd, 'numpy_int1': NumpyInt1, 'numpy_int2': NumpyInt2, 'numpy_int4': NumpyInt4, 'numpy_int8': NumpyInt8, 'numpy_int2_zstd': NumpyInt2Zstd, 'numpy_int4_zstd': NumpyInt4Zstd, 'numpy_int8_zstd': NumpyInt8Zstd, 'pd_zstd': PdZstd, 'gpd_zstd': GpdZstd}

serial_name_dict = {n: i+1 for i, n in enumerate(serial_dict)}

serial_int_dict = {(i+1): v for i, v in enumerate(serial_dict.values())}














































