/*
	qWat - QGIS Water Module

	SQL file :: bedding table
*/

/* CREATE */
DROP TABLE IF EXISTS qwat_vl.altitude_reference CASCADE;
CREATE TABLE qwat_vl.altitude_reference () INHERITS ( qwat_vl.value_list_base);
ALTER TABLE qwat_vl.altitude_reference ADD CONSTRAINT vl_altitude_reference_pk PRIMARY KEY (id);
COMMENT ON TABLE qwat_vl.altitude_reference IS 'Reference for altitudes';
/* VALUES */
INSERT INTO qwat_vl.altitude_reference (id,value_fr, value_en, value_ro) VALUES (9000,'inconnu','unknown','necunoscut');   
INSERT INTO qwat_vl.altitude_reference (id,value_fr, value_en, value_ro) VALUES (9001,'radier','apron','radier');   
INSERT INTO qwat_vl.altitude_reference (id,value_fr, value_en, value_ro) VALUES (9002,'sol','ground','sol');   
INSERT INTO qwat_vl.altitude_reference (id,value_fr, value_en, value_ro) VALUES (9003,'conduite','pipe','conductă');  




