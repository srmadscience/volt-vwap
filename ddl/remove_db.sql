
--
-- Copyright (C) 2025 Volt Active Data Inc.
--
-- Use of this source code is governed by an MIT
-- license that can be found in the LICENSE file or at
-- https://opensource.org/licenses/MIT.
--


file -inlinebatch END_OF_BATCH

DROP PROCEDURE ReportTick IF EXISTS;
DROP PROCEDURE ResetDatabase IF EXISTS;

DROP VIEW stocktick_vmap_summary IF EXISTS;
DROP TABLE dummy IF EXISTS;
DROP TABLE stocktick_vmap IF EXISTS;

END_OF_BATCH

