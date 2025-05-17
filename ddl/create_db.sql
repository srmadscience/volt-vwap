
--
-- Copyright (C) 2025 Volt Active Data Inc.
--
-- Use of this source code is governed by an MIT
-- license that can be found in the LICENSE file or at
-- https://opensource.org/licenses/MIT.
--

load classes ../jars/volt-vwap-server.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE dummy
(x varchar(1) primary key);

CREATE TABLE stocktick_vmap 
(symbol varchar(10) not null 
,tickdate timestamp not null 
,timescale varchar(10) not null
,open_price decimal not null
,high_price decimal not null
,low_price decimal not null
,close_price decimal not null
,volume decimal not null
,total_value decimal not null
,vmap decimal 
,primary key (symbol,tickdate,timescale));

partition TABLE stocktick_vmap on column symbol;

CREATE VIEW stocktick_vmap_summary AS
SELECT tickdate,timescale, sum(volume) volume, sum(total_value) total_value, count(*) how_many
FROM stocktick_vmap
GROUP BY tickdate,timescale;

CREATE PROCEDURE 
   PARTITION ON TABLE stocktick_vmap COLUMN symbol
   FROM CLASS vwapdemo.server.ReportTick;
   
CREATE PROCEDURE ResetDatabase AS
DELETE FROM stocktick_vmap;

END_OF_BATCH

INSERT INTO dummy VALUES ('X');
