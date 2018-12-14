-- Table: qwat_od.pipe_reference

-- DROP TABLE qwat_od.pipe_reference;

CREATE TABLE qwat_od.pipe_reference
(
    id serial,
    fk_pipe integer,
    fk_node_a integer,
    fk_node_b integer,
    geometry geometry,
    CONSTRAINT pipe_reference_pkey PRIMARY KEY (id),
    CONSTRAINT pipe_reference_fk_pipe_fkey FOREIGN KEY (fk_pipe)
        REFERENCES qwat_od.pipe (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE qwat_od.pipe_reference
    OWNER to postgres;

-- Index: pipe_reference_fk_pipe_idx

-- DROP INDEX qwat_od.pipe_reference_fk_pipe_idx;

CREATE INDEX pipe_reference_fk_pipe_idx
    ON qwat_od.pipe_reference USING btree
    (fk_pipe)
    TABLESPACE pg_default;
    
-- FUNCTION: qwat_od.ft_pipe_(integer)

-- DROP FUNCTION qwat_od.ft_pipe_(integer);

CREATE OR REPLACE FUNCTION qwat_od.ft_pipe_(
	var_pipe_id integer)
RETURNS integer
    LANGUAGE 'plpgsql'
AS $BODY$DECLARE r record;
DECLARE fk integer;
DECLARE g1 geometry;
DECLARE ml record;
BEGIN
DELETE FROM qwat_od.pipe_reference WHERE fk_pipe=var_pipe_id;
FOR r IN (
	SELECT *,CASE WHEN ST_LineLocatePoint(tblpipe.pg, tblpipe.fk_ag)<ST_LineLocatePoint(tblpipe.pg, tblpipe_nodes.ng) THEN ST_Length(ST_LineSubstring(tblpipe.pg, ST_LineLocatePoint(tblpipe.pg, tblpipe.fk_ag), ST_LineLocatePoint(tblpipe.pg, tblpipe_nodes.ng))) ELSE 99999 END as distance
	FROM (
		SELECT vw_pipe.id as pid,n.id as fk_a,vw_pipe.fk_node_b as fk_b,n.geometry as fk_ag,vw_pipe.geometry as pg
		FROM qwat_od.vw_pipe join qwat_od.node as n on n.id=vw_pipe.fk_node_a
		WHERE vw_pipe.id=var_pipe_id
	) tblpipe join (
		SELECT vw_pipe.id as p_id,n.id as n,n.geometry as ng
		FROM qwat_od.vw_pipe join qwat_od.node as n on n.id not in (vw_pipe.fk_node_a,vw_pipe.fk_node_b) 
		WHERE vw_pipe.id=var_pipe_id and ST_Distance(n.geometry,vw_pipe.geometry)>=0 and ST_Distance(n.geometry,vw_pipe.geometry)<0.003 and n.id in (select pipe.fk_node_a from qwat_od.pipe union select pipe.fk_node_b from qwat_od.pipe)
	) tblpipe_nodes on tblpipe.pid=tblpipe_nodes.p_id
	ORDER BY distance
)
LOOP
	IF (fk IS NULL) THEN
		fk = r.fk_a;
	END IF;
	IF (g1 IS NULL) THEN
		g1 = r.pg;
	END IF;
	-- split line by nodes ST_Distance(g1,r.ng)*1.5
	SELECT ST_NumGeometries(ST_CollectionExtract(ST_Split(ST_Snap(g1,r.ng,0.003),r.ng),2)) as nr,
	ST_GeometryN(ST_CollectionExtract(ST_Split(ST_Snap(g1,r.ng,0.003),r.ng),2),1) as l1,
	ST_GeometryN(ST_CollectionExtract(ST_Split(ST_Snap(g1,r.ng,0.003),r.ng),2),2) as l2 INTO ml;
	-- insert line into pipe_reference
	INSERT INTO qwat_od.pipe_reference(fk_pipe,fk_node_a,fk_node_b,geometry) 
	SELECT r.pid,fk,r.n,ST_Force2D(ml.l1);
	fk = r.n;
	g1 = ml.l2;
END LOOP;
IF (r IS NOT NULL) THEN
	INSERT INTO qwat_od.pipe_reference(fk_pipe,fk_node_a,fk_node_b,geometry) 
	SELECT r.pid,fk,r.fk_b,ST_Force2D(g1);
END IF;
RETURN 0;
END;$BODY$;

-- FUNCTION: qwat_od.ft_element_valve_status(integer)

-- DROP FUNCTION qwat_od.ft_element_valve_status(integer);

CREATE OR REPLACE FUNCTION qwat_od.ft_element_valve_status(
	var_pipe_id integer)
RETURNS boolean
    LANGUAGE 'sql'
AS $BODY$SELECT bool_or(closed) as closed
FROM qwat_od.vw_element_valve
WHERE fk_pipe=var_pipe_id
GROUP by fk_pipe$BODY$;

-- FUNCTION: qwat_od.ft_pipe_path(integer, integer)

-- DROP FUNCTION qwat_od.ft_pipe_path(integer, integer);

CREATE OR REPLACE FUNCTION qwat_od.ft_pipe_path(
	var_nod_a integer,
	var_nod_b integer)
RETURNS TABLE(seq integer, path_seq integer, nod bigint, edge bigint, cost double precision, agg_cost double precision, geometry geometry, path text) 
    LANGUAGE 'sql'
AS $BODY$
SELECT path.*,geom.geometry,geom.json FROM pgr_dijkstra('
	select id,
		fk_node_a as source,
		fk_node_b as target,
		CASE WHEN qwat_od.ft_element_valve_status(fk_pipe) THEN -1 ELSE COALESCE(st_length(vw_pipe_reference.geometry),1) END as cost,
		CASE WHEN qwat_od.ft_element_valve_status(fk_pipe) THEN -1 ELSE COALESCE(st_length(vw_pipe_reference.geometry),1) END as reverse_cost 
	from qwat_od.vw_pipe_reference',var_nod_a,var_nod_b) as path
	LEFT JOIN (
	SELECT id,geometry,ST_AsGeoJSON(geometry) as json
	FROM qwat_od.vw_pipe_reference
	) as geom ON geom.id=path.edge
$BODY$;

-- View: qwat_od.vw_pipe_reference

-- DROP VIEW qwat_od.vw_pipe_reference;

CREATE OR REPLACE VIEW qwat_od.vw_pipe_reference AS
 SELECT sp.id,
    sp.fk_node_a,
    sp.fk_node_b,
    sp.geometry,
    sp.fk_pipe
   FROM qwat_od.pipe_reference sp
UNION
 SELECT p.id,
    p.fk_node_a,
    p.fk_node_b,
    p.geometry,
    p.id AS fk_pipe
   FROM qwat_od.vw_pipe p
  WHERE NOT (p.id IN ( SELECT pipe_reference.fk_pipe
           FROM qwat_od.pipe_reference));

-- FUNCTION: qwat_od.ft_epanet(character varying, integer, character varying)

-- DROP FUNCTION qwat_od.ft_epanet(character varying, integer, character varying);

CREATE OR REPLACE FUNCTION qwat_od.ft_epanet(
	var_bbox character varying,
	var_srid integer,
	var_function_list character varying)
RETURNS TABLE(type character varying, element text) 
    LANGUAGE 'sql'
AS $BODY$SELECT 'JUNCTIONS',row_to_json(tmp_junctions)::text
FROM (
	select id,round(AVG(elevation)) as elevation,0 as demand,'' as pattern,st_astext(geometry) as geometry
	FROM (
		select 'junction_'||pr.fk_node_a as id, 
			COALESCE(ROUND(ST_Z(n.geometry)::numeric,2),COALESCE(n.altitude,9999)) as elevation, 
-- calculate demand as volume, but I am not using it for the time being
				round(((to_number(COALESCE(pp._material_diameter,'0'),'9999')/2)^2)*3.14159*1/100) as demand, 
				n.geometry
		FROM qwat_od.vw_pipe_reference as pr
			join qwat_od.vw_qwat_node as n on n.id=pr.fk_node_a 
			join qwat_od.vw_pipe as pp on pr.fk_pipe=pp.id
		where st_intersects(pp.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
										from (
										select array_agg(el) e
										from (
										select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
											) t1
										) t2)
							)
			and CASE WHEN var_function_list='' THEN true ELSE pp.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END
			and n.geometry NOT IN (SELECT geometry FROM qwat_od.vw_element_installation WHERE installation_type = 'tank')
		union
		select 'junction_'||pr.fk_node_b as id, 
	 		COALESCE(ROUND(ST_Z(n.geometry)::numeric,2),COALESCE(n.altitude,9999)) as elevation, 
			 round(((to_number(COALESCE(pp._material_diameter,'0'),'9999')/2)^2)*3.14159*1/100) as demand, 
			 n.geometry
		FROM qwat_od.vw_pipe_reference as pr
			join qwat_od.vw_qwat_node as n on n.id=pr.fk_node_b
			join qwat_od.vw_pipe as pp on pr.fk_pipe=pp.id
		where st_intersects(pp.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
										from (
										select array_agg(el) e
										from (
										select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
											) t1
										) t2)
							)
			and CASE WHEN var_function_list='' THEN true ELSE pp.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END
			and n.geometry NOT IN (SELECT geometry FROM qwat_od.vw_element_installation WHERE installation_type = 'tank')
	) tmp1
	GROUP BY id,geometry
) tmp_junctions
UNION
	SELECT 'RESERVOIRS',row_to_json(tmp_reservoirs)::text
	FROM ( SELECT 'reservoir_'||id, COALESCE(height_max,0) as head, '' as head_pattern, st_astext(geometry) as geometry
			FROM qwat_od.vw_qwat_network_element as n
		  WHERE element_type::text = 'reservoirs' 
		  and st_intersects(n.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
		  ) tmp_reservoirs
UNION
	SELECT 'TANKS',row_to_json(tmp_tanks)::text
	FROM ( SELECT 'tank_'||id as id, 
		  		CASE WHEN altitude_overflow > 1 then
					altitude_overflow::DECIMAL - 4 
				ELSE 9999
				END as elevation, 
		  		2 as initlevel, 
		  		0 as minlevel, 
		  		2 as maxlevel, 
		  		--Diameter = 2 x ( (V alim +V inc.)  / (3.14 x 4m))^0.5   [en m à arrondir au cm]
				COALESCE(round(2 * ((COALESCE(storage_supply,0)+COALESCE(storage_fire,0))  / (3.14 * 4))^0.5),9999)  as diameter,
				'' as minvol, 
		  		'' as volcurve, 
				altitude_overflow::DECIMAL  AS waterlevel,
				storage_supply::INTEGER AS V_utilisation,
				storage_fire::INTEGER AS V_fire,
				LEFT(i.name,40) AS label,
		  		st_astext(geometry) as geometry
			FROM qwat_od.vw_element_installation as i
		  WHERE installation_type = 'tank'
		and st_intersects(i.geometry_polygon,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
		 ) tmp_tanks
UNION
	SELECT 'SOURCES',row_to_json(tmp_sources)::text
	FROM ( SELECT 'source_'||i.id as id, 
		  	replace(source_type.value_ro,' ','_') as type, 
		  	replace(source_quality.value_ro,' ','_') as quality, 
		  	st_astext(geometry) as geometry
			FROM qwat_od.vw_element_installation as i 
					join qwat_vl.source_type ON i.fk_source_type=source_type.id
					join qwat_vl.source_quality ON i.fk_source_quality=source_quality.id
		  WHERE installation_type = 'source'
		and st_intersects(i.geometry_polygon,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
		 ) tmp_sources
UNION
	SELECT 'VALVES',row_to_json(tmp_valves)::text
	FROM (
		select 'valve_'||v.id as id, 'junction_'||pr.fk_node_a as node1, 'junction_'||pr.fk_node_b as node2, 
			9999 as diameter, 
			--replace(valve_type.value_ro,' ','_') as type, 
			'PRV' as type,
			CASE WHEN v.closed THEN 1 ELSE 0 END as setting, 
			0 as minorloss,
			st_astext(v.geometry) as geometry
		from qwat_od.vw_element_valve as v	
			join qwat_vl.valve_type on v.fk_valve_type=valve_type.id
			join qwat_od.vw_pipe as pp on v.fk_pipe=pp.id
			join qwat_od.vw_pipe_reference as pr on  pr.fk_pipe=pp.id and ST_DWithin(v.geometry,pr.geometry,0.1)
		where CASE WHEN var_function_list='' THEN true ELSE pp.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END and st_intersects(pp.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
	) tmp_valves
UNION
	SELECT 'PUMPS',row_to_json(tmp_pumps)::text
	FROM (
		select 'pump_'||p.id as id, 'junction_'||pr1.fk_node_a as node1, 'junction_'||pr2.fk_node_b as node2,
			COALESCE(p.altitude::DECIMAL,9999) AS elevation, 
			' HEAD '||' POWER '||' SPEED '||' PATTERN ' as parameters,
			st_astext(p.geometry) as geometry
		from qwat_od.vw_element_installation as p	
			left join qwat_od.vw_pipe as p1 on p.fk_pipe_in=p1.id
				join qwat_od.vw_pipe_reference as pr1 on  pr1.fk_pipe=p1.id and ST_DWithin(p.geometry,pr1.geometry,0.1)
			left join qwat_od.vw_pipe as p2 on p.fk_pipe_out=p2.id
				join qwat_od.vw_pipe_reference as pr2 on  pr2.fk_pipe=p2.id and ST_DWithin(p.geometry,pr2.geometry,0.1)
		where (CASE WHEN var_function_list='' THEN true ELSE p1.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END 
				or 
		   CASE WHEN var_function_list='' THEN true ELSE p2.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END) 
			and ( st_intersects(p1.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
				or
			st_intersects(p2.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						) 
				  )	
	) tmp_pumps
UNION
SELECT 'PIPES',row_to_json(tmp_pipes)::text
FROM (
	select 'pipe_'||pr.id::varchar as id, 'junction_'||pr.fk_node_a as node1, 'junction_'||pr.fk_node_b as node2, 
		CASE WHEN ST_Length(pr.geometry)=0 THEN 0.1
			ELSE ST_Length(pr.geometry) END as length, 
		COALESCE(CASE WHEN pm.diameter_internal > 1 then
			round(pm.diameter_internal)::INTEGER 
		ELSE round(pm.diameter_nominal)::INTEGER 
		END,9999) as diameter, 
		0.1 as roughness, 0.1 as minorloss,
		CASE WHEN p._valve_closed = true OR p.fk_status = 1307 then
			'Closed'
		ELSE
			'Open'
		END as status, st_astext(pr.geometry) as geometry
	from qwat_od.vw_pipe_reference as pr
		inner join qwat_od.pipe as p on pr.fk_pipe=p.id
		left join qwat_vl.pipe_material as pm on p.id = pm.id
	where st_intersects(p.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
	and CASE WHEN var_function_list='' THEN true ELSE p.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END AND pr.fk_node_a IN (SELECT id FROM qwat_od.vw_qwat_node) AND pr.fk_node_b IN (SELECT id FROM qwat_od.vw_qwat_node)
	and ST_StartPoint(pr.geometry) NOT IN (SELECT geometry FROM qwat_od.vw_element_installation WHERE installation_type='tank')
	and ST_EndPoint(pr.geometry) NOT IN (SELECT geometry FROM qwat_od.vw_element_installation WHERE installation_type='tank')
	union
	select 'pipe_'||pr.id::varchar as id, 'tank_'||i.id as node1, 'junction_'||pr.fk_node_b as node2, 
		CASE WHEN ST_Length(pr.geometry)=0 THEN 0.1
			ELSE ST_Length(pr.geometry) END as length, 
		COALESCE(CASE WHEN pm.diameter_internal > 1 then
			round(pm.diameter_internal)::INTEGER 
		ELSE round(pm.diameter_nominal)::INTEGER 
		END,9999) as diameter, 
		0.1 as roughness, 0.1 as minorloss, 
		CASE WHEN p._valve_closed = true OR p.fk_status = 1307 then
			'Closed'
		ELSE
			'Open'
		END as status, 
		st_astext(pr.geometry) as geometry
	from qwat_od.vw_pipe_reference as pr
		inner join qwat_od.pipe as p on pr.fk_pipe=p.id
			left join qwat_vl.pipe_material as pm on p.id = pm.id
		inner join qwat_od.vw_element_installation as i on ST_DWithin(ST_StartPoint(pr.geometry), i.geometry, 0.01)
	where st_intersects(p.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
	and CASE WHEN var_function_list='' THEN true ELSE p.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END 
	and pr.fk_node_a IN (SELECT id FROM qwat_od.vw_qwat_node) and pr.fk_node_b IN (SELECT id FROM qwat_od.vw_qwat_node)
	and i.installation_type = 'tank'
	union
	select 'pipe_'||pr.id::varchar as id, 'junction_'||pr.id as node1, 'tank_'||i.id as node2, 
		CASE WHEN ST_Length(pr.geometry)=0 THEN 0.1
			ELSE ST_Length(pr.geometry) END as length, 
		COALESCE(CASE WHEN pm.diameter_internal > 1 then
			round(pm.diameter_internal)::INTEGER 
		ELSE round(pm.diameter_nominal)::INTEGER 
		END,9999) as diameter, 
		0.1 as roughness, 0.1 as minorloss, 
		CASE WHEN p._valve_closed = true OR p.fk_status = 1307 then
			'Closed'
		ELSE
			'Open'
		END as status, 
		st_astext(pr.geometry) as geometry
	from qwat_od.vw_pipe_reference as pr
		inner join qwat_od.pipe as p on pr.fk_pipe=p.id
			left join qwat_vl.pipe_material as pm on p.id = pm.id
		inner join qwat_od.vw_element_installation as i on ST_DWithin(ST_EndPoint(pr.geometry),i.geometry,0.01)
	where st_intersects(p.geometry,(select ST_SetSRID(ST_MakeEnvelope(e[1],e[2],e[3],e[4]), var_srid)
									from (
									select array_agg(el) e
									from (
									select to_number(unnest(string_to_array(var_bbox,',')),'9999999999999.99999999') el
										) t1
									) t2)
						)
	and CASE WHEN var_function_list='' THEN true ELSE p.fk_function IN (SELECT to_number(unnest(string_to_array(var_function_list,',')),'99999')) END 
	and pr.fk_node_a IN (SELECT id FROM qwat_od.vw_qwat_node) and pr.fk_node_b IN (SELECT id FROM qwat_od.vw_qwat_node)
	and i.installation_type = 'tank'
	) tmp_pipes
	
$BODY$;

