CREATE DATABASE `demo` /*!40100 CHARACTER SET utf8 COLLATE 'utf8_general_ci' */;

USE `demo`;

CREATE TABLE `users` (
  `user_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(20) NULL,
  `email` varchar(30) NULL,
  `create_time` int(11) unsigned NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB CHARSET=utf8;

CREATE TABLE `new_users` (
  `user_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(20) NULL,
  `email` varchar(30) NULL,
  `create_time` int(11) unsigned NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB CHARSET=utf8;

-- PG
--
-- CREATE TABLE "users" (
--   "user_id" int4 NOT NULL,
--   "name" varchar(20),
--   "email" varchar(30),
--   "create_time" int4,
--   CONSTRAINT "pk.users" PRIMARY KEY ("user_id")
-- );

-- ClickHouse
--
-- CREATE TABLE default.users
-- (
--
--     `user_id` Int64,
--     `name` String,
--     `email` String,
--     `create_time` Int64
-- )
-- ENGINE = MergeTree PARTITION BY user_id order by user_id;