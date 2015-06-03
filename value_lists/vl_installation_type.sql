/*
	qWat - QGIS Water Module
	
	SQL file :: installation_type table
*/

/* CREATE */
DROP TABLE IF EXISTS qwat_vl.installation_type CASCADE;
CREATE TABLE qwat_vl.installation_type () INHERITS ( qwat_vl.value_list_base);
ALTER TABLE qwat_vl.installation_type ADD CONSTRAINT vl_installation_type_pk PRIMARY KEY (id);
COMMENT ON TABLE qwat_vl.installation_type IS 'type for installation.';

/* COLUMNS*/
ALTER TABLE qwat_vl.installation_type ADD COLUMN schema_visible boolean default true;

/* VALUES */
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2001,'R'  ,'Réservoir'		,'R'	,'Rezervor');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2002,'S'  ,'Source'		,'S'	,'Sursă');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2003,'P'  ,'Pompage'		,'P'	,'Staţie pompare');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2004,'CV' ,'Chambres de vannes' 	,'CV'	,'Cămin Vane');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2005,'CC' ,'Chambre compteur'	,'CAp'  ,'Cămin Apometru');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2006,'CT' ,'Chambre de traitement','CT'	,'Cameră de Tratare?');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2007,'CR' ,'Chambre réducteur'	,'CR'	,'Cămin reductor presiune?');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2008,'CCP','Chambre coupe pression','CRp','Cămin reducere presiune');
INSERT INTO qwat_vl.installation_type (id,short_fr,value_fr,short_ro,value_ro) VALUES (2009,'CRA','Chambre de rassemblement',''	,'Cameră de captare/colectare');
