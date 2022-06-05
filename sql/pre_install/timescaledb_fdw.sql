-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.
-- https://blog.csdn.net/weixin_33973600/article/details/92296186
CREATE FOREIGN DATA WRAPPER timescaledb_fdw
  HANDLER @extschema@.timescaledb_fdw_handler
  VALIDATOR @extschema@.timescaledb_fdw_validator;
