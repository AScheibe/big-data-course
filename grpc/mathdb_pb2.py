# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: mathdb.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cmathdb.proto\x12\x06mathdb\"(\n\nSetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x02\"\x1c\n\x0bSetResponse\x12\r\n\x05\x65rror\x18\x01 \x01(\t\"\x19\n\nGetRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\"+\n\x0bGetResponse\x12\r\n\x05value\x18\x01 \x01(\x02\x12\r\n\x05\x65rror\x18\x02 \x01(\t\"/\n\x0f\x42inaryOpRequest\x12\r\n\x05key_a\x18\x01 \x01(\t\x12\r\n\x05key_b\x18\x02 \x01(\t\"C\n\x10\x42inaryOpResponse\x12\r\n\x05value\x18\x01 \x01(\x02\x12\x11\n\tcache_hit\x18\x02 \x01(\x08\x12\r\n\x05\x65rror\x18\x03 \x01(\t2\xdd\x02\n\x06MathDb\x12\x30\n\x03Set\x12\x12.mathdb.SetRequest\x1a\x13.mathdb.SetResponse\"\x00\x12\x30\n\x03Get\x12\x12.mathdb.GetRequest\x1a\x13.mathdb.GetResponse\"\x00\x12:\n\x03\x41\x64\x64\x12\x17.mathdb.BinaryOpRequest\x1a\x18.mathdb.BinaryOpResponse\"\x00\x12:\n\x03Sub\x12\x17.mathdb.BinaryOpRequest\x1a\x18.mathdb.BinaryOpResponse\"\x00\x12;\n\x04Mult\x12\x17.mathdb.BinaryOpRequest\x1a\x18.mathdb.BinaryOpResponse\"\x00\x12:\n\x03\x44iv\x12\x17.mathdb.BinaryOpRequest\x1a\x18.mathdb.BinaryOpResponse\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mathdb_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_SETREQUEST']._serialized_start=24
  _globals['_SETREQUEST']._serialized_end=64
  _globals['_SETRESPONSE']._serialized_start=66
  _globals['_SETRESPONSE']._serialized_end=94
  _globals['_GETREQUEST']._serialized_start=96
  _globals['_GETREQUEST']._serialized_end=121
  _globals['_GETRESPONSE']._serialized_start=123
  _globals['_GETRESPONSE']._serialized_end=166
  _globals['_BINARYOPREQUEST']._serialized_start=168
  _globals['_BINARYOPREQUEST']._serialized_end=215
  _globals['_BINARYOPRESPONSE']._serialized_start=217
  _globals['_BINARYOPRESPONSE']._serialized_end=284
  _globals['_MATHDB']._serialized_start=287
  _globals['_MATHDB']._serialized_end=636
# @@protoc_insertion_point(module_scope)