CREATE OR REPLACE PROCEDURE DB_TABELAO.DBO.PR_TABELAO_APAGA_INDEVIDOS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN

insert into dbo.log_execucao(evento) values (''inicio retira tributos'');

create or replace table tmp.tmp_analise_tabelao01
as
select id_cache_icms, id_cache_antec, null::int as ent_sai, null::int as id_config_icms, null::int as id_config_antec, 
 null::int as uf
from dbo.tributos_internos_tabelao
group by id_cache_icms, id_cache_antec ;

insert into dbo.log_execucao(evento) values (''retira tributos - 01'');

update tmp.tmp_analise_tabelao01 a
set id_config_icms = c.id_config
from dbo.tributos_internos_cache c where c.id_cliente = 55982
and c.id = a.id_cache_icms;

insert into dbo.log_execucao(evento) values (''retira tributos - 02'');

update tmp.tmp_analise_tabelao01 a
set id_config_antec = c.id_config
from dbo.tributos_internos_cache c 
where c.id_cliente = 55982
and c.id = a.id_cache_antec
and a.id_cache_antec is not null;

insert into dbo.log_execucao(evento) values (''retira tributos - 03'');

update tmp.tmp_analise_tabelao01 a
set ent_sai = 1
from dbo.tributos_internos_cache_config c
where c.id = a.id_config_icms and c.ent_sai = ''1'';

insert into dbo.log_execucao(evento) values (''retira tributos - 04'');

update tmp.tmp_analise_tabelao01 a
set uf = 1
from dbo.tributos_internos_cache_config c 
where c.id = a.id_config_icms and c.uf_origem = c.uf_destino
and a.ent_sai = 1;

insert into dbo.log_execucao(evento) values (''retira tributos - 05'');

create or replace table tmp.tmp_analise_tabelao02
as
select a.id_cache_icms, a.id_cache_antec, a.uf, null::int as cst, null::int as tpantecipacao 
from tmp.tmp_analise_tabelao01 a (nolock)
where a.ent_sai = 1
group by a.id_cache_icms, a.id_cache_antec, a.uf;

insert into dbo.log_execucao(evento) values (''retira tributos - 06'');

update tmp.tmp_analise_tabelao02 a  
set cst = 1
from dbo.tributos_internos_cache c 
where c.id = a.id_cache_icms
and trim(c.cst) in (''10'',''30'',''70'');

insert into dbo.log_execucao(evento) values (''retira tributos - 07'');

update tmp.tmp_analise_tabelao02 a 
set tpantecipacao = 1
from  dbo.tributos_internos_cache c 
where c.id = a.id_cache_antec
and coalesce(c.tipo_antecipacao,''0'') <> ''0''
and c.tipo_antecipacao <> ''''
and a.id_cache_antec is not null;

insert into dbo.log_execucao(evento) values (''retira tributos - 08'');

create or replace table tmp.tmp_analise_tabelao03
as
select a.id_cache_icms, a.id_cache_antec
from tmp.tmp_analise_tabelao02 a 
where 
id_cache_antec is not null 
and (
( coalesce(cst,0) = 1 and coalesce(tpantecipacao,0) = 1 )
)
group by a.id_cache_icms, a.id_cache_antec;

insert into dbo.log_execucao(evento) values (''retira tributos - 09'');

create or replace table tmp.tmp_analise_tabelao04
as
select a.id , 0::int as tipo
from dbo.tributos_internos_tabelao a 
inner join tmp.tmp_analise_tabelao03 b  on a.id_cache_icms = b.id_cache_icms
and a.id_cache_antec = b.id_cache_antec; 

insert into dbo.log_execucao(evento) values (''retira tributos - 10'');

--- 1) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 00, 20, 51, 10 ou 70) e (ICMS-Alíquota <> 4.0000 e 0.9456 e 1.2571) e (Origem Produto = 1 ou 2 ou 3 ou 8)
/** JIRA#PHPKB-2485 
 *	1) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 00, 20, 51, 10 ou 70) e (ICMS-Alíquota <> 4.0000 e 0.9456 e 1.2571) e (Origem Produto = 1 ou 2 ou 3 ou 8) e (NCM começa com <> 842959 E <> 843359 E <> 87 (exceto se NCM começa com = 8713)) 
 */
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 1::int as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where 
 (config.uf_origem <> ''EX'' and config.uf_origem  <> config.uf_destino ) and  
trim(icms.cst) in (''00'',''20'',''51'',''10'',''70'')
 and icms.aliquota <> ''4.0000'' and icms.aliquota <> ''0.9456'' and icms.aliquota <> ''1.2571''
and tab.origem_produto in (1,2,3,8)
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,6) in (''842959'',''843359''))
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,2) in (''87'') AND (SUBSTRING (ltrim(cpCli.ncm),1,4) <> ''8713''));


insert into dbo.log_execucao(evento) values (''retira tributos - 11'');

--- 2) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 40) e (ICMS-Alíquota_desonerada <> 4.0000 e 0.9456 e 1.2571) e (Origem Produto = 1 ou 2 ou 3 ou 8)
/** JIRA#PHPKB-2485
 *  2) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 40) e (ICMS-Alíquota_desonerada <> 4.0000 e 0.9456 e 1.2571) e (Origem Produto = 1 ou 2 ou 3 ou 8) e (NCM começa com <> 842959 E <> 843359 E <> 87 (exceto se NCM começa com = 8713))
 */
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 2 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where 
 (config.uf_origem <> ''EX'' and config.uf_origem  <> config.uf_destino )
and 
trim(icms.cst) in (''40'')
and icms.aliquota_desonerada <> ''4.0000'' and icms.aliquota_desonerada <> ''0.9456'' and icms.aliquota_desonerada <> ''1.2571''
and tab.origem_produto in (1,2,3,8)
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,6) in (''842959'',''843359''))
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,2) in (''87'') AND (SUBSTRING (ltrim(cpCli.ncm),1,4) <> ''8713''));

insert into dbo.log_execucao(evento) values (''retira tributos - 12'');

---- 3) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 00, 20, 51, 10 ou 70) e (ICMS-Alíquota <> 7.0000 e 12.0000 e 0.9456 e 1.2571) e (Origem Produto = 0 ou 4 ou 5 ou 6 ou 7)
/** JIRA#PHPKB-2485
 *   3) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 00, 20, 51, 10 ou 70) e (ICMS-Alíquota <> 7.0000 e 12.0000 e 0.9456 e 1.2571) e (Origem Produto = 0 ou 4 ou 5 ou 6 ou 7) e (NCM começa com <> 842959 E <> 843359 E <> 87 (exceto se NCM começa com = 8713))
 */
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 3 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where 
(config.uf_origem <> ''EX'' and config.uf_origem  <> config.uf_destino )
and trim(icms.cst) in (''00'',''20'',''51'',''10'',''70'')
and icms.aliquota <> ''7.0000'' and icms.aliquota <> ''12.0000'' and icms.aliquota <> ''0.9456'' and icms.aliquota <> ''1.2571''
and tab.origem_produto in (0,4,5,6,7)
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,6) in (''842959'',''843359''))
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,2) in (''87'') AND (SUBSTRING (ltrim(cpCli.ncm),1,4) <> ''8713''));

insert into dbo.log_execucao(evento) values (''retira tributos - 13'');

---- 4) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 40) e (ICMS-Alíquota_desonerada <> 7.0000 e 12.0000 e 0.9456 e 1.2571) e (Origem Produto = 0 ou 4 ou 5 ou 6 ou 7)
/** JIRA#PHPKB-2485
 *  4) (cache = A) e (UF_origem <> EX e <> UF_destino) e (CST = 40) e (ICMS-Alíquota_desonerada <> 7.0000 e 12.0000 e 0.9456 e 1.2571) e (Origem Produto = 0 ou 4 ou 5 ou 6 ou 7) e (NCM começa com <> 842959 E <> 843359 E <> 87 (exceto se NCM começa com = 8713))
 */
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 4 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where 
(config.uf_origem <> ''EX'' and config.uf_origem  <> config.uf_destino )
and icms.cst in (''40'')
and icms.aliquota_desonerada <> ''7.0000'' and icms.aliquota_desonerada <> ''12.0000'' and icms.aliquota_desonerada <> ''0.9456'' and icms.aliquota_desonerada <> ''1.2571''
and tab.origem_produto in (0,4,5,6,7)
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,6) in (''842959'',''843359''))
AND NOT (SUBSTRING (ltrim(cpCli.ncm),1,2) in (''87'') AND (SUBSTRING (ltrim(cpCli.ncm),1,4) <> ''8713''));

insert into dbo.log_execucao(evento) values (''retira tributos - 14'');

--- a) Se (grupo_cache = C) e (UF_Destino <>EX) e (ex_origem contenha  1 ou 24 ou 2, e não contenha 905 ou 623 ou 622 ou 230 ou 620 ou 1237 ou 28 ou 144 (24, somente se não contiver também 3 e origem material <> 1 E 6)) e (ex_destinação <> 2070 ou 40 ou 41 ou 23 ou 168 ou 417 ou 419) e  (cod Natureza de Operação = 120 ou 121) e ((NCM iniciada 2203) ou (NCM = 22021000 com ex_tipi = 01) ou (NCM = 22029900 com ex_tipi = 03 ou 04)) e (PIS_CST <> 03 e 02)														
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 5 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache pis  on tab.id_cache_pis = pis.id
inner join dbo.custom_prod cpRegra  on (tab.id_custom_prod_padrao = cpRegra.id)
cross join TABLE(SPLIT_TO_TABLE(replace(config.alternativo_ex_origem,''.'','',''), '','')) AS fc
cross join TABLE(SPLIT_TO_TABLE(replace(config.destinacao,''.'','',''), '','')) AS fcd
where config.uf_destino <> ''EX''
and (
        trim(fc.value) in (''1'',''2'') 
        or (
		 trim(fc.value) in (''24'') 
		    and not( 
                trim(fc.value) in (''3'')
                and tab.origem_produto not in (1,6) 
            )
        )
    )
and trim(fc.value) not in (''905'', ''623'',''622'',''230'',''620'',''1237'',''28'',''144'')
and trim(fcd.value) not in (''2070'',''40'',''41'',''23'',''168'',''417'',''419'') 
and config.cod_nat_op IN (''120'',''121'')
and (
    cpRegra.ncm like ''2203%'' OR 
    (cpRegra.ncm = ''22021000'' AND coalesce(cpRegra.ex_tipi,'''') = ''01'') or 
    (cpRegra.ncm = ''22029900'' and cpRegra.ex_tipi in (''03'',''04''))
)
and (trim(pis.cst) <> ''02'' AND trim(pis.cst) <> ''03'')
group by tab.id;

insert into dbo.log_execucao(evento) values (''retira tributos - 15'');


--- 6. b) Se (grupo_cache = C) e (UF_Destino <>EX) e (ex_origem contenha  1 ou 24 ou 2, e não contenha 905 ou 623 ou 622 ou 230 ou 620 ou 1237 ou 28 ou 144) e (ex_destinação <> 2070 ou 40 ou 41 ou 23 ou 168 ou 417 ou 419) e  (cod Natureza de Operação = 120 ou 121) e ((NCM iniciada 2203) ou (NCM = 22021000 com ex_tipi = 01) ou (NCM = 22029900 com ex_tipi = 03 ou 04)) e (PIS_CST <> 03 e 02)					
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 6 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.custom_prod cpCli  on tab.id_custom_prod = cpCli.id
inner join dbo.tributos_internos_cache cofins  on tab.id_cache_cofins = cofins.id
inner join dbo.custom_prod cpRegra  on (tab.id_custom_prod_padrao = cpRegra.id)
cross join TABLE(SPLIT_TO_TABLE(replace(config.alternativo_ex_origem,''.'','',''), '','')) AS fc
cross join TABLE(SPLIT_TO_TABLE(replace(config.destinacao,''.'','',''), '','')) AS fcd
where 
config.uf_destino <> ''EX''
and (
        (
        trim(fc.value) in (''1'',''2'') 
        or 
            (
                trim(fc.value) in (''24'') 
				and not ( 
                    trim(fc.value) in (''3'')
                        and tab.origem_produto not in (1,6)
                )
            )
        )
		and (
            trim(fc.value) not in (''905'', ''623'',''622'',''230'',''620'',''1237'',''28'',''144'')
			)
		)
and trim(fcd.value) not in (''2070'',''40'',''41'',''23'',''168'',''417'',''419'') 
and config.cod_nat_op IN (''120'',''121'')
and (
    cpRegra.ncm like ''2203%'' OR 
    (cpRegra.ncm = ''22021000'' AND coalesce(cpRegra.ex_tipi,'''') = ''01'') OR 
    (cpRegra.ncm = ''22029900'' and cpRegra.ex_tipi in (''03'',''04''))
)
and (trim(cofins.cst) <> ''02'' AND trim(cofins.cst) <> ''03'')
group by tab.id;

insert into dbo.log_execucao(evento) values (''retira tributos - 16'');

--- 7: https://systax.atlassian.net/browse/TI-788
------  1. (grupo_cache=A) e (ICMS_ST_bc_composição_ST = vazio)
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 7 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
inner join dbo.tributos_internos_cache_st st  on tab.id_cache_st = st.id
where tab.id_cache_st is not null
and trim(icms.cst) in (''10'',''30'',''70'',''201'',''202'')
and coalesce(st.bc_composicao,'''') = ''''
group by tab.id;

insert into dbo.log_execucao(evento) values (''retira tributos - 17'');

--- 8: https://systax.atlassian.net/browse/TI-788
------  2. (grupo_cache=A) e (CST=00,10,20,51,70) e (ICMS_bc_composição= vazio)

insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 8 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where trim(icms.cst) in (''00'',''10'',''20'',''51'',''70'')
and coalesce(icms.bc_composicao,'''') = '''';

insert into dbo.log_execucao(evento) values (''retira tributos - 18'');

--- #22 - (grupo_cache=A) e (CST=40) e (ICMS-Aliquota desonerada <> VAZIO) e (ICMS_bc_composição= vazio)
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 22 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where trim(icms.cst) in (''40'')
and icms.aliquota_desonerada IS NOT NULL
and coalesce(icms.bc_composicao,'''') = '''';

insert into dbo.log_execucao(evento) values (''retira tributos - 19'');

--- https://systax.atlassian.net/browse/TI-818
--- (grupo_cache=A) e (uf_origem= PR) e ((CST = 20, 30, 40, 41, 50, 51, 70, 90) e (ICMS-genérico "cBenef<>vazio") a informação do campo cBenef deve ser:
------ cBenef=PR + sequencia de 6 número, por exemplo cBenef=PR123456, nesse formato;
------ caso não atenda hipótese acima deve bloquear a entrega
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 9 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on config.id = tab.id_config
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where icms.cst in (''20'', ''30'', ''40'', ''41'', ''50'', ''51'', ''70'', ''90'')
		and config.uf_origem = ''PR''
		and icms.generico like ''%cBenef%''
		and dbo.valida_cBenef(generico, config.uf_origem) = 0;

insert into dbo.log_execucao(evento) values (''retira tributos - 20'');

------ grupo_cache=A) e (uf_origem= RJ) e ((CST = 20, 30, 40, 51,70, 90) e (ICMS-genérico "cBenef<>vazio") a informação do campo cBenef deve ser:
------ cBenef= RJ + sequencia de 6 número, por exemplo cBenef=RJ123456, nesse formato;
------ OU cBenef=SEM CBENEF
------ caso não atenda uma das 2 hipóteses acima deve bloquear a entrega
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 10 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on config.id = tab.id_config
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where trim(icms.cst) in (''20'', ''40'', ''51'', ''70'', ''90'')
		and config.uf_origem = ''RJ''
		and icms.generico like ''%cBenef%''
		and dbo.valida_cBenef(generico, config.uf_origem) = 0;

insert into dbo.log_execucao(evento) values (''retira tributos - 21'');        

------ (grupo_cache=A) e (uf_origem= RS) e ((CST = 10, 20, 30, 40, 41, 50, 51, 60, 70, 90) e (ICMS-genérico "cBenef<>vazio") a informação do campo cBenef deve ser:
------ cBenef=RS + sequencia de 6 número, por exemplo cBenef=RS123456, nesse formato;
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 11 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on config.id = tab.id_config
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where trim(icms.cst) in (''10'', ''20'', ''30'', ''40'', ''41'', ''50'', ''51'', ''60'', ''70'', ''90'')
		and config.uf_origem = ''RS''
		and icms.generico like ''%cBenef%''
		and dbo.valida_cBenef(generico, config.uf_origem) = 0;

insert into dbo.log_execucao(evento) values (''retira tributos - 22'');

create or replace table tmp.tmp_ex_origem_230
as
select 
LISTAGG(id_ex_origem, '', '') WITHIN GROUP (ORDER BY id_ex_origem) as str_id_ex_origem
from dbo.ex_origem_cache_familia   
where 230 IN (id_familia,id_ex_origem);

insert into dbo.log_execucao(evento) values (''retira tributos - 23'');

--- #13 - a)  Se (grupo_cache = A) e (ex_origem NÃO contenha  230 ou família descendente) e (ICMS_CST <> 00 ou 02 ou 10 ou 15 ou 20 ou 30 ou 40 ou 41 ou 50 ou 51 ou 53 ou 60 ou 61 ou 70 ou 90)
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 13 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
cross join tmp.tmp_ex_origem_230 tmpExOrigem
where trim(icms.cst) not in (''00'',''02'',''10'',''15'',''20'',''30'',''40'',''41'',''50'',''51'',''53'',''60'',''61'',''70'',''90'')
AND dbo.qtd_itens_substring(tmpExOrigem.str_id_ex_origem, coalesce(config.alternativo_ex_origem,'''') ) = 0
group by tab.id;

insert into dbo.log_execucao(evento) values (''retira tributos - 24'');

--- #14 - b)  Se (grupo_cache = A) e (ex_origem  CONTENHA 230 ou família descendente) e (ICMS_CST <> 02 ou 15 ou 53 ou 61 ou 101 ou 102 ou 103 ou 201 ou 202 ou 203 ou 300 ou 400 ou 500 ou 900)
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 14 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
cross join tmp.tmp_ex_origem_230 as tmpExOrigem
where trim(icms.cst) not in (''02'', ''15'', ''53'', ''61'', ''101'',''102'',''103'',''201'',''202'',''203'',''300'',''400'',''500'',''900'')
AND dbo.array_intersect(tmpExOrigem.str_id_ex_origem, replace(config.alternativo_ex_origem,''.'','','') ) = 1
group by tab.id
;

insert into dbo.log_execucao(evento) values (''retira tributos - 25'');

insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 15 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where 
(	
	( trim(icms.cst) IN (''10'',''30'',''70'') AND trim(icms.cfop) IN (''5102'',''5101'',''6102'',''6101'') )	OR	( trim(icms.cst) IN (''60'') AND trim(icms.cfop) IN (''5102'',''5101'') )
);

insert into dbo.log_execucao(evento) values (''retira tributos - 26'');

-- 28/07/2020: inserido nas procedures: dbo.pr_tabelao_apaga_indevidos / systax_app.[dbo].[pr_tabelao_apaga_indevidos_incremental] / systax_resultado.[dbo].[pr_tabelao_dp_apaga_indevidos]
-- #8	Trava ICMS:  redução de BC sem percentual de redução (grupo_cache=A) e (CST = 20 ou 70) e (ICMS-p-red-BC = vazio)
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 16 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
where trim(icms.cst) IN (''20'',''70'')
AND icms.p_red_bc IS NULL;

insert into dbo.log_execucao(evento) values (''retira tributos - 27'');


-- Se ''ICMS Antecipação'' e ''tipo antecipação'' = ''0'', trava a entrega da regra se: ICMS-ANTECIPAÇÃO-Encerra Trib <> vazio ICMS-ANTECIPAÇÃO-Responsável <> vazio ICMS-ANTECIPAÇÃO-BC Composição Fcp <> vazio ICMS-ANTECIPAÇÃO-Aliquota-Interna Destinação <> vazio ICMS-ANTECIPAÇÃO-Fcp <> vazio ICMS-ANTECIPAÇÃO-P Red BC-Interna Destinação <> vazio ICMS-ANTECIPAÇÃO-Carga Média <> vazio ICMS-ANTECIPAÇÃO-MVA <> vazio ICMS-ANTECIPAÇÃO-MVA Ajustado <> vazio ICMS-ANTECIPAÇÃO-Lista positiva ajustado <> vazio ICMS-ANTECIPAÇÃO-Lista negativa ajustado  <> vazio ICMS-ANTECIPAÇÃO-Lista neutra ajustado <> vazio ICMS-ANTECIPAÇÃO-Genérico  <> vazio ICMS-ANTECIPAÇÃO-Dispositivo legal <> vazio ICMS-ANTECIPAÇÃO-Observações  <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Indicador <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-CST Entrada <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Percentual Crédito <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Genérico <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Vigência de <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Vigência até <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Dispositivo legal <> vazio ICMS-ANTECIPAÇÃO-CRÉDITO-Observações <> vazio 
insert into tmp.tmp_analise_tabelao04(id, tipo)
select tab.id, 19 as tipo
from dbo.tributos_internos_tabelao tab 
inner join dbo.tributos_internos_cache_config config  on tab.id_config = config.id
inner join dbo.tributos_internos_cache icms  on tab.id_cache_icms = icms.id
inner join dbo.tributos_internos_cache antec  on tab.id_cache_antec = antec.id
where antec.tipo_antecipacao = ''0''
and (
	(coalesce(trim(antec.encerra_trib), '''') <> '''') or
	(coalesce(trim(antec.responsavel), '''') <> '''') or
	(coalesce(trim(antec.bc_composicao_fcp), '''') <> '''') or
	(coalesce(trim(antec.bc_composicao_fcp), '''') <> '''') or
	antec.uf_dest_aliq_interna_dest is not null or
	antec.fcp is not null  or
	antec.p_red_bc is not null  or
	antec.carga_media  is not null  or
	antec.ant_mva  is not null  or
	antec.ant_mva_ajustado  is not null  or
	antec.ant_mva_lista_positiva_ajustado  is not null  or
	antec.ant_mva_lista_negativa_ajustado  is not null  or
	antec.ant_mva_lista_neutra_ajustado  is not null  or
	((coalesce(antec.generico, '''')) != '''') or	
	((coalesce(antec.dispositivo_legal, '''')) != '''') or
	((coalesce(antec.observacoes, '''')) != '''') or
	(coalesce(antec.cred_indicador_credito,0) <> 0) or
	(coalesce(antec.cred_cst_entrada,'''') != '''') or
	antec.cred_percentual_credito is not null or
	((coalesce(antec.cred_generico, '''')) != '''') or
	antec.cred_vigencia_de is not null or
	antec.cred_vigencia_ate is not null or
	((coalesce(antec.cred_dispositivo_legal, '''')) != '''') or
	((coalesce(antec.cred_observacoes, '''')) != '''')
);

insert into dbo.log_execucao(evento) values (''retira tributos - 28'');

---- separa o id_cliente + id_config + cod_prod + orig_prod dos registros que devem ser apagados, para que também seja apagado do tabelao_incremental
create or replace table tmp.tmp_tabelao_excluir_problemas
as
select t.id_cliente, t.id_config, t.cod_prod, t.origem_produto
from dbo.tributos_internos_tabelao t   
inner join tmp.tmp_analise_tabelao04 tmp  on tmp.id = t.id;

insert into dbo.log_execucao(evento) values (''retira tributos - 29'');

------------------ PROXIMO 
delete from dbo.tributos_internos_tabelao t   
where exists (select 1 from  tmp.tmp_analise_tabelao04 tmp  where tmp.id = t.id);


insert into dbo.log_execucao(evento) values (''fim retira tributos'');

RETURN ''OK'';

END;
';