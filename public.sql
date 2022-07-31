/*
 Navicat Premium Data Transfer

 Source Server         : pg
 Source Server Type    : PostgreSQL
 Source Server Version : 120011
 Source Host           : localhost:5432
 Source Catalog        : postgres
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 120011
 File Encoding         : 65001

 Date: 08/07/2022 08:07:12
*/


-- ----------------------------
-- Table structure for a
-- ----------------------------
DROP TABLE IF EXISTS "public"."a";
CREATE TABLE "public"."a" (
  "time" timestamp(6),
  "value" varchar(255) COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "public"."a" OWNER TO "a";

-- ----------------------------
-- Records of a
-- ----------------------------
BEGIN;
INSERT INTO "public"."a" ("time", "value") VALUES ('2022-06-25 06:58:30.177874', 'a');
INSERT INTO "public"."a" ("time", "value") VALUES ('2022-06-25 07:32:12.379379', 'a');
COMMIT;

-- ----------------------------
-- Table structure for aiot
-- ----------------------------
DROP TABLE IF EXISTS "public"."aiot";
CREATE TABLE "public"."aiot" (
  "time" timestamp(6) NOT NULL,
  "value" varchar(255) COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "public"."aiot" OWNER TO "a";

-- ----------------------------
-- Records of aiot
-- ----------------------------
BEGIN;
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 10:35:58.991432', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 10:06:21.714086', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 10:28:43.256292', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 10:34:59.94911', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 10:35:04.339159', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 11:15:05.206346', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 11:55:58.249647', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 12:07:33.037239', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 13:10:01.800099', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 13:20:14.017158', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 13:29:49.813882', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 13:36:30.986983', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 14:23:00.537691', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 15:10:56.981854', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 15:11:24.86144', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 16:06:08.898501', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-19 17:01:23.857323', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-10 10:14:48', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-24 12:24:17.15296', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-24 12:24:50.711776', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-24 19:10:18.960964', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-24 19:25:24.274776', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-27 08:33:00.358137', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-27 20:23:04.620188', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-28 07:47:55.833556', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-28 08:08:49.67604', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-28 08:10:43.663397', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-28 19:56:27.271677', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-06-29 09:21:43.082546', 'a');
INSERT INTO "public"."aiot" ("time", "value") VALUES ('2022-07-01 10:14:48', 'a');
COMMIT;

-- ----------------------------
-- Table structure for aiot1
-- ----------------------------
DROP TABLE IF EXISTS "public"."aiot1";
CREATE TABLE "public"."aiot1" (
  "time" timestamp(6) NOT NULL,
  "device_id" varchar(255) COLLATE "pg_catalog"."default",
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "value" float8
)
;
ALTER TABLE "public"."aiot1" OWNER TO "a";

-- ----------------------------
-- Records of aiot1
-- ----------------------------
BEGIN;
INSERT INTO "public"."aiot1" ("time", "device_id", "name", "value") VALUES ('2022-06-30 18:22:07.13476', 'dev1', NULL, 61.07);
INSERT INTO "public"."aiot1" ("time", "device_id", "name", "value") VALUES ('2022-06-30 18:30:53.882232', 'dev2', NULL, 61.07);
COMMIT;

-- ----------------------------
-- Table structure for child1
-- ----------------------------
DROP TABLE IF EXISTS "public"."child1";
CREATE TABLE "public"."child1" (
  "time" timestamp(6) NOT NULL,
  "value" varchar(255) COLLATE "pg_catalog"."default"
)
INHERITS ("public"."parent")
;
ALTER TABLE "public"."child1" OWNER TO "a";

-- ----------------------------
-- Records of child1
-- ----------------------------
BEGIN;
COMMIT;

-- ----------------------------
-- Table structure for ct
-- ----------------------------
DROP TABLE IF EXISTS "public"."ct";
CREATE TABLE "public"."ct" (
  "id" int4,
  "rowclass" text COLLATE "pg_catalog"."default",
  "rowid" text COLLATE "pg_catalog"."default",
  "attribute" text COLLATE "pg_catalog"."default",
  "val" text COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "public"."ct" OWNER TO "a";

-- ----------------------------
-- Records of ct
-- ----------------------------
BEGIN;
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (1, 'group1', 'test1', 'att1', 'val1');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (2, 'group1', 'test1', 'att2', 'val2');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (3, 'group1', 'test1', 'att3', 'val3');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (4, 'group1', 'test1', 'att4', 'val4');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (5, 'group1', 'test2', 'att1', 'val5');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (6, 'group1', 'test2', 'att2', 'val6');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (7, 'group1', 'test2', 'att3', 'val7');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (8, 'group1', 'test2', 'att4', 'val8');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (9, 'group2', 'test3', 'att1', 'val1');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (10, 'group2', 'test3', 'att2', 'val2');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (11, 'group2', 'test3', 'att3', 'val3');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (12, 'group2', 'test4', 'att1', 'val4');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (13, 'group2', 'test4', 'att2', 'val5');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (14, 'group2', 'test4', 'att3', 'val6');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (15, 'group1', NULL, 'att1', 'val9');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (16, 'group1', NULL, 'att2', 'val10');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (17, 'group1', NULL, 'att3', 'val11');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (18, 'group1', NULL, 'att4', 'val12');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (19, '1', '1', '1', '1');
INSERT INTO "public"."ct" ("id", "rowclass", "rowid", "attribute", "val") VALUES (19, '1', '1', '1', '1');
COMMIT;

-- ----------------------------
-- Table structure for parent
-- ----------------------------
DROP TABLE IF EXISTS "public"."parent";
CREATE TABLE "public"."parent" (
  "time" timestamp(6) NOT NULL,
  "value" varchar(255) COLLATE "pg_catalog"."default"
)
;
ALTER TABLE "public"."parent" OWNER TO "a";

-- ----------------------------
-- Records of parent
-- ----------------------------
BEGIN;
COMMIT;

-- ----------------------------
-- Table structure for user1
-- ----------------------------
DROP TABLE IF EXISTS "public"."user1";
CREATE TABLE "public"."user1" (
  "name" varchar(255) COLLATE "pg_catalog"."default",
  "area" varchar(255) COLLATE "pg_catalog"."default",
  "number1" int4,
  "number2" int4
)
;
ALTER TABLE "public"."user1" OWNER TO "a";

-- ----------------------------
-- Records of user1
-- ----------------------------
BEGIN;
INSERT INTO "public"."user1" ("name", "area", "number1", "number2") VALUES ('a', 'a', 2, 2);
INSERT INTO "public"."user1" ("name", "area", "number1", "number2") VALUES ('1~2', '7', 1, 1);
COMMIT;

-- ----------------------------
-- Indexes structure for table aiot
-- ----------------------------
CREATE INDEX "aiot_time_idx" ON "public"."aiot" USING btree (
  "time" "pg_catalog"."timestamp_ops" DESC NULLS FIRST
);

-- ----------------------------
-- Triggers structure for table aiot
-- ----------------------------
CREATE TRIGGER "ts_insert_blocker" BEFORE INSERT ON "public"."aiot"
FOR EACH ROW
EXECUTE PROCEDURE "_timescaledb_internal"."insert_blocker"();

-- ----------------------------
-- Indexes structure for table aiot1
-- ----------------------------
CREATE INDEX "aiot1_device_id_time_idx" ON "public"."aiot1" USING btree (
  "device_id" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST,
  "time" "pg_catalog"."timestamp_ops" DESC NULLS FIRST
);
CREATE INDEX "aiot1_time_idx" ON "public"."aiot1" USING btree (
  "time" "pg_catalog"."timestamp_ops" DESC NULLS FIRST
);

-- ----------------------------
-- Triggers structure for table aiot1
-- ----------------------------
CREATE TRIGGER "ts_insert_blocker" BEFORE INSERT ON "public"."aiot1"
FOR EACH ROW
EXECUTE PROCEDURE "_timescaledb_internal"."insert_blocker"();

-- ----------------------------
-- Checks structure for table child1
-- ----------------------------
ALTER TABLE "public"."child1" ADD CONSTRAINT "child1_constraint_1" CHECK ("time" >= '2022-06-16 00:00:00'::timestamp without time zone AND "time" < '2022-06-23 00:00:00'::timestamp without time zone);
