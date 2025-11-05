from collections import namedtuple

Consultas = namedtuple('Consultas',['consulta','tabela','limite'])

# Consultas no SQL Server para gerar arquivos parquet
sql_queries =   [
                    Consultas(consulta='SELECT id, id_controle, dt_criacao, ultima_alteracao, deletado, username_criacao, ativo, uf, municipio, id_ibge, aliquota, reducao_aliquota, perfil_prestador, perfil_tomador, perfil_executor, dispositivo_legal, informacoes_adicionais, observacoes, vigencia_de, vigencia_ate, uso_interno, cst, cod_class_trib, nbs, perfil_responsavel, aliquota_efetiva, percentual_dif, percentual_cashback, formula_base_calculo, descricao FROM systax_regras.dbo.ibs_mun;',  tabela='ibs_mun',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, aliquota, exclusao FROM systax_regras.dbo.ibs_mun_aliquota;',  tabela='ibs_mun_aliquota',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, aliquota_efetiva, exclusao FROM systax_regras.dbo.ibs_mun_aliquota_efetiva;',  tabela='ibs_mun_aliquota_efetiva',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, cod_class_trib, exclusao FROM systax_regras.dbo.ibs_mun_cod_class_trib;',  tabela='ibs_mun_cod_class_trib',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, cst, exclusao FROM systax_regras.dbo.ibs_mun_cst;',  tabela='ibs_mun_cst',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, descricao, exclusao FROM systax_regras.dbo.ibs_mun_descricao;',  tabela='ibs_mun_descricao',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, formula_base_calculo, exclusao FROM systax_regras.dbo.ibs_mun_formula_base_calculo;',  tabela='ibs_mun_formula_base_calculo',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, id_ibge, exclusao FROM systax_regras.dbo.ibs_mun_id_ibge;',  tabela='ibs_mun_id_ibge',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, municipio, exclusao FROM systax_regras.dbo.ibs_mun_municipio;',  tabela='ibs_mun_municipio',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, nbs, exclusao FROM systax_regras.dbo.ibs_mun_nbs;',  tabela='ibs_mun_nbs',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, percentual_cashback, exclusao FROM systax_regras.dbo.ibs_mun_percentual_cashback;',  tabela='ibs_mun_percentual_cashback',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, percentual_dif, exclusao FROM systax_regras.dbo.ibs_mun_percentual_dif;',  tabela='ibs_mun_percentual_dif',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, perfil_executor, exclusao FROM systax_regras.dbo.ibs_mun_perfil_executor;',  tabela='ibs_mun_perfil_executor',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, perfil_prestador, exclusao FROM systax_regras.dbo.ibs_mun_perfil_prestador;',  tabela='ibs_mun_perfil_prestador',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, perfil_responsavel, exclusao FROM systax_regras.dbo.ibs_mun_perfil_responsavel;',  tabela='ibs_mun_perfil_responsavel',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, perfil_tomador, exclusao FROM systax_regras.dbo.ibs_mun_perfil_tomador;',  tabela='ibs_mun_perfil_tomador',limite=100000),
                    Consultas(consulta='SELECT id, id_ibs_mun, reducao_aliquota, exclusao FROM systax_regras.dbo.ibs_mun_reducao_aliquota;',  tabela='ibs_mun_reducao_aliquota',limite=100000)
                ]
