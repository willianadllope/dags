from collections import namedtuple

Consultas = namedtuple('Consultas',['consulta','tabela','limite'])

# Consultas no SQL Server para gerar arquivos parquet
sql_queries =   [
        Consultas(consulta="""
                  SELECT cod_prod, origem_produto, id_cli
                    FROM systax_app.dbo.agrupamento_produtos (NOLOCK)
		            WHERE hierarquia = 1 AND (vigencia_ate IS NULL or vigencia_ate >= getdate());
                  """, 
                  tabela='agrupamento_produtos',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT ponteiro_tabelao as ts, convert(varchar(20),data,120) as datahora  
                        FROM systax_app.dbo.ts_diario (NOLOCK)
                        WHERE servidor = 'win04' 
                        ORDER BY ponteiro_tabelao DESC
                  """, 
                  tabela='ts_diario',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT id, id_emp_responsavel, tipo, entidade, assdigital, tratamento, nome, ddd, telefone, ramal, email, situacao, observacao, tipo_cobranca, filtro_prod_uf, filtro_prod_dest, filtro_prod_orig, alerta_custom, users_alerta_custom, notas, ativo, deletado, consulta_liberada, dt_registro, copy_custom, id_UsuarioCriador, flag_origem_cliente, flag_setor, filtro_prod_pis_cofins, flag_cean_incorreto, flag_show_coluna, flag_piscofigns_cumulativo, codacesso, sla_cadastro_fiscal, exOrigPadrao, exDestPadrao, natOpPadrao, finalidadePadrao, flag_nao_integrar, flag_nao_tem_cfm, cenario_padrao, flag_ecommerce, flag_alerta_criacao, flag_agrupamento_produto, regime_tributacao, data_agrupamento, flag_cst_entrada_60, flag_cad_produto_orig_dif, flag_cfm_ncm_igual, flag_igualar_codigual_origdif, flag_sem_tratamento_origem, limite_cadastro_produtos, cnpj, flag_cockipt, criacao_automatica_cenario_base, flag_mva_ajustado, flag_medicamento, flag_apaga_produto_site, flag_carga_expressa, flag_force_carga_expressa, flag_dfe, flag_exclusao_icms_ipi_cofins, flag_desativar_todas_origens, flag_agrupamento_semelhante, justifica_agrup_semelhante, flag_retorno_ipi, justifica_retorno_ipi, flag_cad_prod_sem_cest, flag_carga_expressa_cenario, flag_force_carga_expressa_cenario, flag_copy_servico_emp_filhas
                    FROM systax_app.dbo.clientes (NOLOCK) order by id;
                  """, 
                  tabela='clientes',
                  limite=1000000
                ),
        Consultas(consulta="""
                SELECT id, id_cliente, cod_prod_systax, cod_prod_cliente, menorts, convert(bigint,ts) as ts
                    FROM systax_app.dbo.custom_prod_figuras_fiscais a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_custom_prod_figuras_fiscais ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='custom_prod_figuras_fiscais',
                  limite=1000000),   
        Consultas(consulta="""
                  SELECT id, id_cliente, cod_produto, id_produto_configuracao, origem_produto, id_trib_cigarro, dt_cadastro 
                    FROM systax_app.dbo.custom_prod_rel_cigarros (NOLOCK);
                  """, 
                  tabela='custom_prod_rel_cigarros',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT id, 
                      id_usuario_pai, 
                      tipo, 
                      nome, 
                      ddd, 
                      telefone, 
                      ramal, 
                      email, 
                      username, 
                      senha, 
                      pre_credito, 
                      notas, 
                      ativo, 
                      deletado, 
                      nivel_admin, 
                      nome_revisado, 
                      flag_senha_primeiro_acesso, 
                      flag_expira_senha, 
                      convert(varchar(10),data_expira_senha,120) as data_expira_senha
                    FROM systax_app.dbo.usuarios (NOLOCK)
                  where id = 6040
                  ;
                  """, 
                  tabela='usuarios',
                  limite=100000
                ),
        Consultas(consulta="""
                  SELECT id, id_usuario, id_cliente
                    FROM systax_app.dbo.usuario_clientes(NOLOCK);
                  """, 
                  tabela='usuario_clientes',
                  limite=100000
                ),                
        Consultas(consulta="""
                  SELECT id, id_servico, id_usuario, id_cliente, id_cliente_sistema, id_sistema, saldo_creditos, limite_alerta, limite_alerta_enviado, dt_expiracao, controle_creditos, ativo
                    FROM systax_app.dbo.licencas_controle(NOLOCK);
                  """, 
                  tabela='licencas_controle',
                  limite=100000
                ),                                
        Consultas(consulta="""
                SELECT id,
                        id_cli,
                        cod_prod,
                        id_prod,
                        cean,
                        descricao,
                        ex_prod,
                        ex_benef,
                        ncm,
                        ncm_sugerida,
                        ncm_duvida,
                        status_integracao,
                        status,
                        cadastro_ok,
                        convert(varchar(10),vigente_de,120) as vigencia_de,
                        convert(varchar(10),vigente_ate,120) as vigencia_ate,
                        sem_produto,
                        ex_tipi,
                        id_grupo,
                        lista_id_tipo,
                        id_marca,
                        id_embalagem,
                        qtd,
                        pred_aliq,
                        classe_bebida,
                        controle,
                        convert(bigint,ts) as ts,
                        prod_padrao,
                        convert(varchar(10),dt_criacao,120) as dt_criacao,
                        questionamento,
                        origem_produto,
                        cean_original,
                        quest_config,
                        cean_revisado,
                        status_med,
                        ean_lista,
                        medicamento_lista,
                        medicamento_tipo,
                        flag_vinculacao_especial,
                        revisao_info,
                        cadastro_fiscal_ok,
                        convert(varchar(10),dh_entrega_cadastro_fiscal,120) as dh_entrega_cadastro_fiscal,
                        ncm_ex_sugestao,
                        flag_base_nao_usar,
                        flag_medicamento,
                        cean14_med,
                        tipo_vinculacao,
                        flag_fora_agrupamento_prod,
                        flag_multiplicacao_origem,
                        cean14 
                    FROM systax_app.dbo.custom_prod a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, cean14_padrao, cean14_vinculado, convert(bigint,ts) as ts, descricao_original 
                    FROM systax_app.dbo.cean_relacionado a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_cean_relacionado ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='cean_relacionado',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id,
                        id_cliente,
                        apelido,
                        grupo_cache,
                        cod_nat_op,
                        destinacao,
                        uf_origem,
                        uf_destino,
                        cnpj_ex_origem,
                        cod_prod_exemplo,
                        data_recalcular,
                        flag_exec,
                        alternativo_ex_origem,
                        id_cliente_prod,
                        id_cliente_config_prod,
                        frequencia,
                        id_mun_origem,
                        id_mun_destino,
                        dias_futuros,
                        id_config_principal,
                        id_config_icms,
                        id_config_ipi,
                        id_config_pis,
                        id_config_cofins,
                        id_config_antecipacao,
                        prioridade,
                        id_config_manual_pis,
                        id_config_manual_cofins,
                        id_config_manual_ipi,
                        id_config_manual_antecipacao,
                        cnae_destinatario,
                        grupo_str,
                        id_config_setor,
                        ent_sai,
                        id_rel_futuro,
                        finalidade,
                        total_produtos,
                        total_produtos_calculados,
                        flag_desconsiderar,
                        origem_produto_alternativo,
                        cenario_ind_prod,
                        id_prod_adic,
                        st_cest,
                        convert(varchar(20),dt_criacao,120) as dt_criacao,
                        origem_criacao,
                        flag_homolog,
                        trib_cigarro_calculo,
                        criacao_automatica,
                        flag_revisao,
                        flag_origem_escrituracao,
                        cenario_inativo
                    FROM systax_app.dbo.tributos_internos_cache_config a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache_config ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='tributos_internos_cache_config',
                  limite=100000),
        Consultas(consulta="""
                SELECT id, id_grupo, id_trib_inter_config, id_cli, convert(bigint,ts) as ts 
                    FROM systax_app.dbo.grupo_config a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_grupo_config ecp (NOLOCK) WHERE ecp.id = a.id);
                  """,
                  tabela='grupo_config',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, id_ex_origem, id_familia, ordem, ordem_inversa FROM systax_regras.dbo.ex_origem_cache_familia (NOLOCK);
                  """,
                  tabela='ex_origem_cache_familia',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_custom_prod;
                  """,
                  tabela='apagar_custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_cean_relacionado;
                  """,
                  tabela='apagar_cean_relacionado',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_usuarios;
                  """,
                  tabela='apagar_usuarios',
                  limite=1000000),                  
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_usuario_clientes;
                  """,
                  tabela='apagar_usuario_clientes',
                  limite=1000000),                                    
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_licencas_controle;
                  """,
                  tabela='apagar_licencas_controle',
                  limite=1000000),                                                      
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_custom_prod_rel_cigarros;
                  """,
                  tabela='apagar_custom_prod_rel_cigarros',
                  limite=1000000),                                    
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_custom_prod_figuras_fiscais;
                  """,
                  tabela='apagar_custom_prod_figuras_fiscais',
                  limite=1000000),                                                      
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_grupo_custom_prod;
                  """,
                  tabela='apagar_grupo_custom_prod',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_grupo_config;
                  """,
                  tabela='apagar_grupo_config',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_clientes;
                  """,
                  tabela='apagar_clientes',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache;
                  """,
                  tabela='apagar_tributos_internos_cache',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_st;
                  """,
                  tabela='apagar_tributos_internos_cache_st',
                  limite=1000000),
        Consultas(consulta="""
                    SELECT id FROM systax_app.snowflake.apagar_tributos_internos_cache_config;
                  """,
                  tabela='apagar_tributos_internos_cache_config',
                  limite=1000000),
        Consultas(consulta="""
                SELECT id, id_grupo, id_custom_prod, id_cli, convert(bigint,ts) as ts 
                    FROM systax_app.dbo.grupo_custom_prod a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_grupo_custom_prod ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='grupo_custom_prod',
                  limite=10000000),
        Consultas(consulta="""
                  SELECT  id,
                          id_tributos_internos,
                          p_red_bc,
                          aliquota_st,
                          mva,
                          mva_ajustado,
                          mva_lista_positiva_ajustado,
                          mva_lista_negativa_ajustado,
                          mva_lista_neutra_ajustado,
                          valor_pauta,
                          convert(varchar(10),vigencia_de,120) as vigencia_de,
                          convert(varchar(10),vigencia_ate,120) as vigencia_ate,
                          regra,
                          regra_bc,
                          regra_aliq,
                          convert(varchar(20),data,120) as data,
                          convert(bigint,ts) as ts,
                          id_config,
                          dispositivo_legal,
                          observacoes,
                          hash_config,
                          id_cache_rep,
                          unidade_pauta,
                          hash_retorno_erp,
                          carga_media,
                          hash_retorno_erp_debug,
                          flag_revisao,
                          bc_composicao,
                          generico,
                          severidade,
                          estatisticas,
                          fcp,
                          bc_composicao_fcp
                  FROM systax_app.dbo.tributos_internos_cache_st a(NOLOCK)
                  WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache_st ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='tributos_internos_cache_st',
                  limite=1000000),
        Consultas(consulta="""
                SELECT  id,
                        id_cliente,
                        id_config,
                        cod_produto,
                        cst,
                        cfop,
                        p_red_bc,
                        aliquota,
                        valor_pauta,
                        convert(varchar(10),vigencia_de,120) as vigencia_de,
                        convert(varchar(10),vigencia_ate,120) as vigencia_ate,
                        dispositivo_legal,
                        observacoes,
                        id_produto,
                        regra,
                        regra_bc,
                        regra_aliq,
                        convert(varchar(10),data,120) as data,
                        convert(bigint,ts) as ts,
                        dif_aliq_nao_contrib,
                        hash_config,
                        tipo_antecipacao,
                        encerra_trib,
                        responsavel,
                        perc_fixo,
                        ant_mva,
                        ant_mva_ajustado,
                        ant_mva_lista_positiva_ajustado,
                        ant_mva_lista_negativa_ajustado,
                        ant_mva_lista_neutra_ajustado,
                        id_cache_rep,
                        unidade_pauta,
                        hash_retorno_erp,
                        inf_adicionais,
                        p_red_aliq,
                        valor_unid_trib,
                        p_red_val_imp,
                        aliq_especifica,
                        hash_retorno_erp_debug,
                        flag_revisao,
                        codigo_natureza_receita,
                        bc_composicao,
                        p_red_bc_interna_dest,
                        aliq_interna_dest,
                        cred_indicador_credito,
                        cred_cst_entrada,
                        cred_percentual_credito,
                        convert(varchar(10),cred_vigencia_de,120) as cred_vigencia_de,
                        convert(varchar(10),cred_vigencia_ate,120) as cred_vigencia_ate,
                        cred_dispositivo_legal,
                        cred_observacoes,
                        cfop_entrada,
                        versao,
                        generico,
                        percentual_diferimento,
                        aliquota_desonerada,
                        cred_generico,
                        severidade,
                        estatisticas,
                        fcp,
                        cenq,
                        cest,
                        uf_dest_cst,
                        uf_dest_fcp,
                        uf_dest_dispositivo_legal,
                        convert(varchar(10),uf_dest_vigencia_de,120) as uf_dest_vigencia_de,
                        convert(varchar(10),uf_dest_vigencia_ate,120) as uf_dest_vigencia_ate,
                        uf_dest_aliq_interna_dest,
                        uf_dest_generico,
                        uf_dest_perc_partilha_dest,
                        uf_dest_observacoes,
                        uf_dest_bc_composicao,
                        uf_dest_valor_pauta,
                        uf_dest_p_red_bc,
                        uf_dest_unidade_pauta,
                        origem_produto,
                        prioridade,
                        usuario_aprovacao,
                        convert(varchar(20),data_aprovacao,120) as data_aprovacao,
                        tipo_aprovacao,
                        bc_composicao_sn_especial,
                        aliquota_sn_especial,
                        bc_composicao_fcp,
                        uf_dest_bc_composicao_fcp,
                        aliquota_cf,
                        carga_media
                    FROM systax_app.dbo.tributos_internos_cache a(NOLOCK)
                    WHERE exists(SELECT TOP 1 1 FROM systax_app.[SNOWFLAKE].enviar_tributos_internos_cache ecp (NOLOCK) WHERE ecp.id = a.id and ecp.posicao = [POSICAO] );
                  """,
                  tabela='tributos_internos_cache',
                  limite=1000000)
            ]
