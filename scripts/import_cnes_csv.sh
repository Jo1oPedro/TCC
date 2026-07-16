#!/bin/bash
#
# Importa dados CSV do CNES (Cadastro Nacional de Estabelecimentos de Saude)
# para o banco MySQL cnes_db. Os CSVs sao obtidos do portal de dados abertos
# do CNES e seguem o padrao de competencia 2026/02.
#
# Caracteristicas dos CSVs CNES:
#   - Delimitador: ponto-e-virgula (;)
#   - Qualificador de texto: aspas duplas (")
#   - Encoding: Latin-1 (ISO-8859-1)
#   - Datas no formato DD/MM/YYYY (Oracle TO_CHAR)
#   - Header na primeira linha
#
# Uso (de dentro do container mysql):
#   bash /var/lib/mysql-files/import_cnes_csv.sh
#
# Uso (do host via docker exec):
#   docker exec -it mysql bash /var/lib/mysql-files/import_cnes_csv.sh
#
# Pre-requisito: os CSVs devem estar montados em /var/lib/mysql-files/cnes-csv/
#   (volume no docker-compose: ./base_de_dados_cnes_202602:/var/lib/mysql-files/cnes-csv)

set -e

MYSQL_CMD="mysql -u appuser -pappuser123 cnes_db"
CSV_DIR="/var/lib/mysql-files/cnes-csv"

echo "==="
echo "  CNES CSV Import"
echo "  Source: $CSV_DIR"
echo "  Database: cnes_db"
echo "  Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==="
echo ""

# Verifica se o diretorio de CSVs existe
if [ ! -d "$CSV_DIR" ]; then
    echo "ERRO: Diretorio $CSV_DIR nao encontrado!"
    echo "Monte o volume no docker-compose:"
    echo "  mysql:"
    echo "    volumes:"
    echo "      - ./base_de_dados_cnes_202602:/var/lib/mysql-files/cnes-csv"
    exit 1
fi

# Funcao auxiliar para importar e reportar
import_table() {
    local table_name="$1"
    local csv_file="$2"
    local sql_command="$3"
    local full_path="$CSV_DIR/$csv_file"

    if [ ! -f "$full_path" ]; then
        echo "  [SKIP] $csv_file nao encontrado"
        return
    fi

    local row_count
    row_count=$(wc -l < "$full_path")
    row_count=$((row_count - 1))  # desconta header

    echo -n "  [$table_name] Importando $csv_file ($row_count linhas)... "

    $MYSQL_CMD -e "$sql_command" 2>&1

    local imported
    imported=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM $table_name" 2>/dev/null)
    echo "OK ($imported registros na tabela)"
}

# ===
# 1. DIMENSION TABLES (tabelas pequenas, carregamento completo)
# ===

echo "==="
echo "  FASE 1: Tabelas dimensionais"
echo "==="
echo ""

# 1.1 estados
import_table "estados" "tbEstado202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbEstado202602.csv'
INTO TABLE estados
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_uf, co_sigla, no_descricao);
"

# 1.2 municipios
import_table "municipios" "tbMunicipio202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbMunicipio202602.csv'
INTO TABLE municipios
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_municipio, no_municipio, co_sigla_estado, tp_cadastro, tp_pacto,
 tp_envia, tp_envia_cnes, tp_cib_sas, tp_pleno_origem, tp_mac,
 @nu_populacao, @nu_densidade, @cmtp_inicio_mac)
SET
    nu_populacao = NULLIF(TRIM(@nu_populacao), ''),
    nu_densidade = NULLIF(TRIM(@nu_densidade), ''),
    cmtp_inicio_mac = NULLIF(TRIM(@cmtp_inicio_mac), '');
"

# 1.3 tipo_estabelecimento
import_table "tipo_estabelecimento" "tbTipoEstabelecimento202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbTipoEstabelecimento202602.csv'
INTO TABLE tipo_estabelecimento
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_tipo_estabelecimento, ds_tipo_estabelecimento, @ds_conceito_tipo)
SET ds_conceito_tipo = NULLIF(TRIM(@ds_conceito_tipo), '');
"

# 1.4 tipo_unidade
import_table "tipo_unidade" "tbTipoUnidade202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbTipoUnidade202602.csv'
INTO TABLE tipo_unidade
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_tipo_unidade, ds_tipo_unidade);
"

# 1.5 natureza_juridica
import_table "natureza_juridica" "tbNaturezaJuridica202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbNaturezaJuridica202602.csv'
INTO TABLE natureza_juridica
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_natureza_jur, ds_natureza_jur);
"

# 1.6 tipo_equipamento
import_table "tipo_equipamento" "tbTipoEquipamento202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbTipoEquipamento202602.csv'
INTO TABLE tipo_equipamento
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_tipo_equipamento, ds_tipo_equipamento);
"

# 1.7 equipamentos_catalogo
import_table "equipamentos_catalogo" "tbEquipamento202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbEquipamento202602.csv'
INTO TABLE equipamentos_catalogo
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_equipamento, co_tipo_equipamento, ds_equipamento);
"

# 1.8 tipo_equipe
import_table "tipo_equipe" "tbTipoEquipe202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbTipoEquipe202602.csv'
INTO TABLE tipo_equipe
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(tp_equipe, ds_equipe, @co_grupo_equipe)
SET co_grupo_equipe = NULLIF(TRIM(@co_grupo_equipe), '');
"

# 1.9 grupo_equipe
import_table "grupo_equipe" "tbGrupoEquipe202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbGrupoEquipe202602.csv'
INTO TABLE grupo_equipe
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_grupo_equipe, no_grupo_equipe);
"

# 1.10 servico_especializado
import_table "servico_especializado" "tbServicoEspecializado202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbServicoEspecializado202602.csv'
INTO TABLE servico_especializado
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_servico_especializado, ds_servico_especializado);
"

# 1.11 atividade_profissional
import_table "atividade_profissional" "tbAtividadeProfissional202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbAtividadeProfissional202602.csv'
INTO TABLE atividade_profissional
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_cbo, ds_atividade_profissional, @tp_classificacao_profissional, @tp_cbo_saude, @st_cbo_regulamentado, @no_ano_cmpt)
SET
    tp_classificacao_profissional = NULLIF(TRIM(@tp_classificacao_profissional), ''),
    tp_cbo_saude = NULLIF(TRIM(@tp_cbo_saude), ''),
    st_cbo_regulamentado = NULLIF(TRIM(@st_cbo_regulamentado), ''),
    no_ano_cmpt = NULLIF(TRIM(@no_ano_cmpt), '');
"

# 1.12 conselho_classe
import_table "conselho_classe" "tbConselhoClasse202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbConselhoClasse202602.csv'
INTO TABLE conselho_classe
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_conselho_classe, ds_conselho_classe);
"

# 1.13 convenios
import_table "convenios" "tbConvenio202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbConvenio202602.csv'
INTO TABLE convenios
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_convenio, ds_convenio);
"

# 1.14 gestao
import_table "gestao" "tbGestao202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbGestao202602.csv'
INTO TABLE gestao
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_gestao, ds_gestao, @tp_prog)
SET tp_prog = NULLIF(TRIM(@tp_prog), '');
"

# 1.15 turno_atendimento
import_table "turno_atendimento" "tbTurnoAtendimento202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbTurnoAtendimento202602.csv'
INTO TABLE turno_atendimento
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_turno_atendimento, ds_turno_atendimento);
"

# 1.16 mod_vinculo
import_table "mod_vinculo" "tbModVinculo202602.csv" "
LOAD DATA INFILE '$CSV_DIR/tbModVinculo202602.csv'
INTO TABLE mod_vinculo
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(cd_vinculacao, ds_vinculacao);
"

echo ""
echo "==="
echo "  FASE 2: Tabelas transacionais (grandes)"
echo "==="
echo ""
echo "  AVISO: As tabelas a seguir sao muito grandes."
echo "  - tbEstabelecimento: ~605K linhas"
echo "  - tbDadosProfissionalSus: ~7.6M linhas"
echo "  - tbEquipe: ~124K linhas"
echo "  - tbCargaHorariaSus: ~6.5M linhas"
echo "  - rlEstabEquipamento: ~1.3M linhas"
echo "  - rlEstabServClass: ~1.4M linhas"
echo "  - rlEstabEquipeProf: ~856K linhas"
echo ""
echo "  O import pode levar varios minutos dependendo do hardware."
echo ""

# 2.1 estabelecimentos (~605K linhas)
echo -n "  [estabelecimentos] Importando tbEstabelecimento202602.csv (~605K linhas)... "
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/tbEstabelecimento202602.csv'
INTO TABLE estabelecimentos
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_unidade, co_cnes, @nu_cnpj_mantenedora, @tp_pfpj, @nivel_dep,
 no_razao_social, no_fantasia, no_logradouro, nu_endereco, @no_complemento,
 no_bairro, co_cep, @co_regiao_saude, @co_micro_regiao,
 @co_distrito_sanitario, @co_distrito_administrativo,
 nu_telefone, @nu_fax, no_email, @nu_cpf, @nu_cnpj,
 @co_atividade_csv, @co_clientela, @nu_alvara, @dt_expedicao,
 @tp_orgao_expedidor, @dt_val_lic_sani, @tp_lic_sani, @tp_unidade_csv,
 co_turno_atendimento, co_estado_gestor, co_municipio_gestor,
 @dt_atualizacao_str, @co_usuario, @co_cpfdiretorcln, @reg_diretorcln,
 @st_adesao_filantrop, @co_motivo_desab, @no_url,
 nu_latitude, nu_longitude,
 @dt_atu_geo, @no_usuario_geo, co_natureza_jur,
 @tp_estab_sempre_aberto, @st_geracredito, st_conexao_internet,
 co_tipo_unidade, @no_fantasia_abrev, tp_gestao,
 @dt_atualizacao_origem_str, co_tipo_estabelecimento, co_atividade_principal,
 @st_contrato_formalizado, @co_tipo_abrangencia, @st_coworking)
SET
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
ESTAB_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM estabelecimentos" 2>/dev/null)
echo "OK ($ESTAB_COUNT registros)"

# 2.2 profissionais (~7.6M linhas)
echo ""
echo -n "  [profissionais] Importando tbDadosProfissionalSus202602.csv (~7.6M linhas)... "
echo ""
echo "  AVISO: Esta tabela e a maior do CNES. Pode levar varios minutos."
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/tbDadosProfissionalSus202602.csv'
INTO TABLE profissionais
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_profissional_sus, co_cpf, no_profissional, co_cns,
 @dt_atualizacao_str, @co_usuario, @st_nmprof_cadsus,
 co_nacionalidade, @co_seq_inclusao,
 @dt_atualizacao_origem_str, @no_social)
SET
    no_social = NULLIF(TRIM(@no_social), ''),
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
PROF_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM profissionais" 2>/dev/null)
echo "OK ($PROF_COUNT registros)"

# 2.3 equipes (~124K linhas)
echo ""
echo -n "  [equipes] Importando tbEquipe202602.csv (~124K linhas)... "
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/tbEquipe202602.csv'
INTO TABLE equipes
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_municipio, co_area, seq_equipe, co_unidade, tp_equipe,
 @co_sub_tipo_equipe, no_referencia,
 @dt_ativacao_str, @dt_desativacao_str,
 @tp_pop_assist_quilombola, @tp_pop_assist_assentado,
 @tp_pop_assist_geral, @tp_pop_assist_escola,
 @tp_pop_assist_pronasci, @tp_pop_assist_indigena,
 @tp_pop_assist_ribeirinha, @tp_pop_assist_situacao_rua,
 @tp_pop_assist_priv_liberdade, @tp_pop_assist_conflito_lei,
 @tp_pop_assist_adol_conf_lei,
 @co_cnes_uom, @nu_ch_amb_uom, @cd_motivo_desativ, @cd_tp_desativ,
 @co_prof_sus_preceptor, @co_cnes_preceptor,
 co_equipe,
 @dt_atualizacao_str, @no_usuario, @dt_atualizacao_origem_str)
SET
    dt_ativacao = CASE
        WHEN TRIM(@dt_ativacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_ativacao_str), '%d/%m/%Y')
    END,
    dt_desativacao = CASE
        WHEN TRIM(@dt_desativacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_desativacao_str), '%d/%m/%Y')
    END,
    tp_pop_assist_quilombola = NULLIF(TRIM(@tp_pop_assist_quilombola), ''),
    tp_pop_assist_assentado = NULLIF(TRIM(@tp_pop_assist_assentado), ''),
    tp_pop_assist_geral = NULLIF(TRIM(@tp_pop_assist_geral), ''),
    tp_pop_assist_escola = NULLIF(TRIM(@tp_pop_assist_escola), ''),
    tp_pop_assist_pronasci = NULLIF(TRIM(@tp_pop_assist_pronasci), ''),
    tp_pop_assist_indigena = NULLIF(TRIM(@tp_pop_assist_indigena), ''),
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
EQUIPE_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM equipes" 2>/dev/null)
echo "OK ($EQUIPE_COUNT registros)"

# 2.4 carga_horaria (~6.5M linhas)
echo ""
echo -n "  [carga_horaria] Importando tbCargaHorariaSus202602.csv (~6.5M linhas)... "
echo ""
echo "  AVISO: Tabela grande. Pode levar varios minutos."
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/tbCargaHorariaSus202602.csv'
INTO TABLE carga_horaria
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_unidade, co_profissional_sus, co_cbo, tp_sus_nao_sus, ind_vinculacao,
 @tp_terceiro_sih, @qt_ch_amb, co_conselho_classe, nu_registro,
 @sg_uf_crm, @tp_preceptor, @tp_residente, @nu_cnpj_detalhamento,
 @dt_atualizacao_str, @co_usuario, @dt_atualizacao_origem_str,
 @qt_ch_outros, @qt_ch_hosp)
SET
    qt_carga_horaria_ambulatorial = CAST(NULLIF(TRIM(@qt_ch_amb), '') AS UNSIGNED),
    qt_carga_horaria_outros = CAST(NULLIF(TRIM(@qt_ch_outros), '') AS UNSIGNED),
    qt_carga_hor_hosp_sus = CAST(NULLIF(TRIM(@qt_ch_hosp), '') AS UNSIGNED),
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
CH_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM carga_horaria" 2>/dev/null)
echo "OK ($CH_COUNT registros)"

# 2.5 estab_equipamentos (~1.3M linhas)
echo ""
echo -n "  [estab_equipamentos] Importando rlEstabEquipamento202602.csv (~1.3M linhas)... "
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/rlEstabEquipamento202602.csv'
INTO TABLE estab_equipamentos
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_unidade, co_equipamento, co_tipo_equipamento,
 @qt_existente, @qt_uso, tp_sus, @qt_sus,
 @dt_atualizacao_str, @co_usuario, @dt_atualizacao_origem_str)
SET
    qt_existente = CAST(NULLIF(TRIM(@qt_existente), '') AS UNSIGNED),
    qt_uso = CAST(NULLIF(TRIM(@qt_uso), '') AS UNSIGNED),
    qt_sus = CAST(NULLIF(TRIM(@qt_sus), '') AS UNSIGNED),
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
EQ_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM estab_equipamentos" 2>/dev/null)
echo "OK ($EQ_COUNT registros)"

# 2.6 estab_servicos (~1.4M linhas)
echo ""
echo -n "  [estab_servicos] Importando rlEstabServClass202602.csv (~1.4M linhas)... "
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/rlEstabServClass202602.csv'
INTO TABLE estab_servicos
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_unidade, co_servico, co_classificacao,
 @tp_caracteristica, @co_cnpjcpf,
 co_ambulatorial, co_ambulatorial_sus,
 co_hospitalar, co_hospitalar_sus,
 @co_end_compl, @st_ativo_sn,
 @dt_atualizacao_str, @co_usuario)
SET
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
SERV_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM estab_servicos" 2>/dev/null)
echo "OK ($SERV_COUNT registros)"

# 2.7 equipe_profissionais (~856K linhas)
echo ""
echo -n "  [equipe_profissionais] Importando rlEstabEquipeProf202602.csv (~856K linhas)... "
$MYSQL_CMD -e "
LOAD DATA INFILE '$CSV_DIR/rlEstabEquipeProf202602.csv'
INTO TABLE equipe_profissionais
CHARACTER SET latin1
FIELDS TERMINATED BY ';' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(co_municipio, co_area, seq_equipe, co_profissional_sus,
 co_unidade, co_cbo, tp_sus_nao_sus, ind_vinculacao,
 @co_microarea,
 @dt_entrada_str, @dt_desligamento_str,
 @co_cnes_outra, @co_municipio_outra, @co_area_outra,
 @co_prof_sus_compl, @co_cbo_ch_compl,
 st_equipeminima,
 @co_mun_atuacao,
 @dt_atualizacao_str, @no_usuario, @dt_atualizacao_origem_str)
SET
    dt_entrada = CASE
        WHEN TRIM(@dt_entrada_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_entrada_str), '%d/%m/%Y')
    END,
    dt_desligamento = CASE
        WHEN TRIM(@dt_desligamento_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_desligamento_str), '%d/%m/%Y')
    END,
    dt_atualizacao = CASE
        WHEN TRIM(@dt_atualizacao_str) = '' THEN NULL
        ELSE STR_TO_DATE(TRIM(@dt_atualizacao_str), '%d/%m/%Y')
    END;
" 2>&1
EP_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM equipe_profissionais" 2>/dev/null)
echo "OK ($EP_COUNT registros)"

# Resumo final
echo ""
echo "==="
echo "  IMPORT CONCLUIDO"
echo "  Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo "==="
echo ""
echo "  Tabelas dimensionais:"
$MYSQL_CMD -N -e "
SELECT 'estados', COUNT(*) FROM estados
UNION ALL SELECT 'municipios', COUNT(*) FROM municipios
UNION ALL SELECT 'tipo_estabelecimento', COUNT(*) FROM tipo_estabelecimento
UNION ALL SELECT 'tipo_unidade', COUNT(*) FROM tipo_unidade
UNION ALL SELECT 'natureza_juridica', COUNT(*) FROM natureza_juridica
UNION ALL SELECT 'tipo_equipamento', COUNT(*) FROM tipo_equipamento
UNION ALL SELECT 'equipamentos_catalogo', COUNT(*) FROM equipamentos_catalogo
UNION ALL SELECT 'tipo_equipe', COUNT(*) FROM tipo_equipe
UNION ALL SELECT 'grupo_equipe', COUNT(*) FROM grupo_equipe
UNION ALL SELECT 'servico_especializado', COUNT(*) FROM servico_especializado
UNION ALL SELECT 'atividade_profissional', COUNT(*) FROM atividade_profissional
UNION ALL SELECT 'conselho_classe', COUNT(*) FROM conselho_classe
UNION ALL SELECT 'convenios', COUNT(*) FROM convenios
UNION ALL SELECT 'gestao', COUNT(*) FROM gestao
UNION ALL SELECT 'turno_atendimento', COUNT(*) FROM turno_atendimento
UNION ALL SELECT 'mod_vinculo', COUNT(*) FROM mod_vinculo;
" 2>/dev/null | while read name count; do
    printf "    %-30s %s registros\n" "$name" "$count"
done

echo ""
echo "  Tabelas transacionais:"
$MYSQL_CMD -N -e "
SELECT 'estabelecimentos', COUNT(*) FROM estabelecimentos
UNION ALL SELECT 'profissionais', COUNT(*) FROM profissionais
UNION ALL SELECT 'equipes', COUNT(*) FROM equipes
UNION ALL SELECT 'carga_horaria', COUNT(*) FROM carga_horaria
UNION ALL SELECT 'estab_equipamentos', COUNT(*) FROM estab_equipamentos
UNION ALL SELECT 'estab_servicos', COUNT(*) FROM estab_servicos
UNION ALL SELECT 'equipe_profissionais', COUNT(*) FROM equipe_profissionais;
" 2>/dev/null | while read name count; do
    printf "    %-30s %s registros\n" "$name" "$count"
done

echo ""
echo "==="
