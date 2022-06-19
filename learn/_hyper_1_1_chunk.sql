/*
 Navicat Premium Data Transfer

 Source Server         : pg
 Source Server Type    : PostgreSQL
 Source Server Version : 120011
 Source Host           : localhost:5432
 Source Catalog        : postgres
 Source Schema         : _timescaledb_internal

 Target Server Type    : PostgreSQL
 Target Server Version : 120011
 File Encoding         : 65001

 Date: 19/06/2022 11:42:30
*/


-- ----------------------------
-- Table structure for _hyper_1_1_chunk
-- ----------------------------
DROP TABLE IF EXISTS "_timescaledb_internal"."_hyper_1_1_chunk";
CREATE TABLE "_timescaledb_internal"."_hyper_1_1_chunk" (
  "time" timestamp(6) NOT NULL,
  "value" varchar(255) COLLATE "pg_catalog"."default"
)
INHERITS ("public"."aiot")
;
ALTER TABLE "_timescaledb_internal"."_hyper_1_1_chunk" OWNER TO "a";

-- ----------------------------
-- Indexes structure for table _hyper_1_1_chunk
-- "pg_catalog"."timestamp_ops" 对应 opclass
-- NULLS FIRST 意思 nulls sort after non-nulls. This is the default when DESC is not specified.
-- ----------------------------
CREATE INDEX "_hyper_1_1_chunk_aiot_time_idx" ON "_timescaledb_internal"."_hyper_1_1_chunk" USING btree (
  "time" "pg_catalog"."timestamp_ops" DESC NULLS FIRST
);

-- ----------------------------
-- Checks structure for table _hyper_1_1_chunk
-- ----------------------------
ALTER TABLE "_timescaledb_internal"."_hyper_1_1_chunk" ADD CONSTRAINT "constraint_1" CHECK ("time" >= '2022-06-16 00:00:00'::timestamp without time zone AND "time" < '2022-06-23 00:00:00'::timestamp without time zone);
