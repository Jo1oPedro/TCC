<?php

namespace TCC\Pipeline;

/**
 * Transforma eventos brutos do CDC (Debezium) em registros limpos.
 *
 * Responsabilidades:
 * - Normalizar campos (tipos, formatos)
 * - Remover metadados internos do Debezium
 * - Enriquecer com informacoes de processamento
 * - Tratar operacoes de INSERT, UPDATE e DELETE
 */
class EventTransformer
{
    /**
     * Mapeia a operacao do Debezium para um nome legivel.
     */
    private const OP_MAP = [
        'c' => 'INSERT',
        'r' => 'SNAPSHOT', // read (snapshot inicial)
        'u' => 'UPDATE',
        'd' => 'DELETE',
    ];

    /**
     * Transforma um evento CDC bruto em um registro limpo para a camada Silver.
     *
     * @param string $table Nome da tabela
     * @param array  $event Evento CDC completo do Debezium
     * @return array|null Registro limpo, ou null se nao deve ser processado
     */
    public function transform(string $table, array $event): ?array
    {
        $operation = self::OP_MAP[$event['op'] ?? ''] ?? 'UNKNOWN';

        // Para DELETE, 'before'; para o resto, 'after'
        $data = ($operation === 'DELETE')
            ? ($event['before'] ?? null)
            : ($event['after'] ?? null);

        if ($data === null) {
            return null;
        }

        // Transformacao especifica por tabela
        $cleaned = match ($table) {
            'estabelecimentos'    => $this->transformEstabelecimento($data),
            'profissionais'       => $this->transformProfissional($data),
            'carga_horaria'       => $this->transformCargaHoraria($data),
            'equipes'             => $this->transformEquipe($data),
            'equipe_profissionais' => $this->transformEquipeProfissional($data),
            'estab_equipamentos'  => $this->transformEstabEquipamento($data),
            'estab_servicos'      => $this->transformEstabServico($data),
            default               => $data,
        };

        // Metadados padrao adicionados a todo registro silver
        $cleaned['_operation'] = $operation;
        $cleaned['_source_table'] = $table;
        $cleaned['_source_ts_ms'] = $event['ts_ms'] ?? null;
        $cleaned['_is_deleted'] = ($operation === 'DELETE');

        return $cleaned;
    }

    /**
     * Extrai o nome da tabela a partir do nome do topico Kafka.
     * Formato do topico Debezium: {prefix}.{database}.{table}
     *
     * Ex: "cdc.cnes_db.estabelecimentos" -> "estabelecimentos"
     */
    public function extractTableName(string $topic): string
    {
        $parts = explode('.', $topic);
        return end($parts);
    }

    // ============================================================
    // Transformacoes especificas por tabela
    // ============================================================

    private function transformEstabelecimento(array $data): array
    {
        return [
            'co_unidade'              => trim((string) ($data['co_unidade'] ?? '')),
            'co_cnes'                 => trim((string) ($data['co_cnes'] ?? '')),
            'no_razao_social'         => trim((string) ($data['no_razao_social'] ?? '')),
            'no_fantasia'             => trim((string) ($data['no_fantasia'] ?? '')),
            'no_logradouro'           => trim((string) ($data['no_logradouro'] ?? '')),
            'nu_endereco'             => trim((string) ($data['nu_endereco'] ?? '')),
            'no_bairro'               => trim((string) ($data['no_bairro'] ?? '')),
            'co_cep'                  => trim((string) ($data['co_cep'] ?? '')),
            'nu_telefone'             => trim((string) ($data['nu_telefone'] ?? '')),
            'no_email'                => trim((string) ($data['no_email'] ?? '')),
            'co_turno_atendimento'    => trim((string) ($data['co_turno_atendimento'] ?? '')),
            'co_estado_gestor'        => trim((string) ($data['co_estado_gestor'] ?? '')),
            'co_municipio_gestor'     => trim((string) ($data['co_municipio_gestor'] ?? '')),
            'nu_latitude'             => trim((string) ($data['nu_latitude'] ?? '')),
            'nu_longitude'            => trim((string) ($data['nu_longitude'] ?? '')),
            'co_natureza_jur'         => trim((string) ($data['co_natureza_jur'] ?? '')),
            'st_conexao_internet'     => trim((string) ($data['st_conexao_internet'] ?? '')),
            'co_tipo_unidade'         => trim((string) ($data['co_tipo_unidade'] ?? '')),
            'tp_gestao'               => trim((string) ($data['tp_gestao'] ?? '')),
            'co_tipo_estabelecimento' => trim((string) ($data['co_tipo_estabelecimento'] ?? '')),
            'co_atividade'            => trim((string) ($data['co_atividade'] ?? '')),
            'co_atividade_principal'  => trim((string) ($data['co_atividade_principal'] ?? '')),
            'dt_atualizacao'          => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
            'created_at'              => $this->convertTimestamp($data['created_at'] ?? null),
            'updated_at'              => $this->convertTimestamp($data['updated_at'] ?? null),
        ];
    }

    private function transformProfissional(array $data): array
    {
        return [
            'co_profissional_sus' => trim((string) ($data['co_profissional_sus'] ?? '')),
            'co_cpf'              => trim((string) ($data['co_cpf'] ?? '')),
            'no_profissional'     => strtoupper(trim((string) ($data['no_profissional'] ?? ''))),
            'co_cns'              => trim((string) ($data['co_cns'] ?? '')),
            'co_nacionalidade'    => trim((string) ($data['co_nacionalidade'] ?? '')),
            'no_social'           => trim((string) ($data['no_social'] ?? '')),
            'dt_atualizacao'      => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    private function transformCargaHoraria(array $data): array
    {
        return [
            'id'                              => (int) ($data['id'] ?? 0),
            'co_unidade'                      => trim((string) ($data['co_unidade'] ?? '')),
            'co_profissional_sus'             => trim((string) ($data['co_profissional_sus'] ?? '')),
            'co_cbo'                          => trim((string) ($data['co_cbo'] ?? '')),
            'tp_sus_nao_sus'                  => trim((string) ($data['tp_sus_nao_sus'] ?? '')),
            'ind_vinculacao'                  => trim((string) ($data['ind_vinculacao'] ?? '')),
            'qt_carga_horaria_ambulatorial'   => (int) ($data['qt_carga_horaria_ambulatorial'] ?? 0),
            'co_conselho_classe'              => trim((string) ($data['co_conselho_classe'] ?? '')),
            'nu_registro'                     => trim((string) ($data['nu_registro'] ?? '')),
            'qt_carga_horaria_outros'         => (int) ($data['qt_carga_horaria_outros'] ?? 0),
            'qt_carga_hor_hosp_sus'           => (int) ($data['qt_carga_hor_hosp_sus'] ?? 0),
            'dt_atualizacao'                  => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    private function transformEquipe(array $data): array
    {
        return [
            'id'                    => (int) ($data['id'] ?? 0),
            'co_municipio'          => trim((string) ($data['co_municipio'] ?? '')),
            'co_area'               => trim((string) ($data['co_area'] ?? '')),
            'seq_equipe'            => trim((string) ($data['seq_equipe'] ?? '')),
            'co_unidade'            => trim((string) ($data['co_unidade'] ?? '')),
            'tp_equipe'             => trim((string) ($data['tp_equipe'] ?? '')),
            'no_referencia'         => trim((string) ($data['no_referencia'] ?? '')),
            'dt_ativacao'           => $this->convertTimestamp($data['dt_ativacao'] ?? null),
            'dt_desativacao'        => $this->convertTimestamp($data['dt_desativacao'] ?? null),
            'tp_pop_assist_quilomb'        => $data['tp_pop_assist_quilomb'] ?? null,
            'tp_pop_assist_assent'         => $data['tp_pop_assist_assent'] ?? null,
            'tp_pop_assist_geral'          => $data['tp_pop_assist_geral'] ?? null,
            'tp_pop_assist_escola'         => $data['tp_pop_assist_escola'] ?? null,
            'tp_pop_assist_indigena'       => $data['tp_pop_assist_indigena'] ?? null,
            'tp_pop_assist_ribeirinha'     => $data['tp_pop_assist_ribeirinha'] ?? null,
            'tp_pop_assist_situacao_rua'   => $data['tp_pop_assist_situacao_rua'] ?? null,
            'tp_pop_assist_priv_liberdade' => $data['tp_pop_assist_priv_liberdade'] ?? null,
            'co_equipe'             => trim((string) ($data['co_equipe'] ?? '')),
            'dt_atualizacao'        => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    private function transformEquipeProfissional(array $data): array
    {
        return [
            'id'                    => (int) ($data['id'] ?? 0),
            'co_municipio'          => trim((string) ($data['co_municipio'] ?? '')),
            'co_area'               => trim((string) ($data['co_area'] ?? '')),
            'seq_equipe'            => trim((string) ($data['seq_equipe'] ?? '')),
            'co_profissional_sus'   => trim((string) ($data['co_profissional_sus'] ?? '')),
            'co_unidade'            => trim((string) ($data['co_unidade'] ?? '')),
            'co_cbo'                => trim((string) ($data['co_cbo'] ?? '')),
            'tp_sus_nao_sus'        => trim((string) ($data['tp_sus_nao_sus'] ?? '')),
            'ind_vinculacao'        => trim((string) ($data['ind_vinculacao'] ?? '')),
            'dt_entrada'            => $this->convertTimestamp($data['dt_entrada'] ?? null),
            'dt_desligamento'       => $this->convertTimestamp($data['dt_desligamento'] ?? null),
            'st_equipeminima'       => trim((string) ($data['st_equipeminima'] ?? '')),
            'dt_atualizacao'        => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    private function transformEstabEquipamento(array $data): array
    {
        return [
            'id'                    => (int) ($data['id'] ?? 0),
            'co_unidade'            => trim((string) ($data['co_unidade'] ?? '')),
            'co_equipamento'        => trim((string) ($data['co_equipamento'] ?? '')),
            'co_tipo_equipamento'   => trim((string) ($data['co_tipo_equipamento'] ?? '')),
            'qt_existente'          => (int) ($data['qt_existente'] ?? 0),
            'qt_uso'                => (int) ($data['qt_uso'] ?? 0),
            'tp_sus'                => trim((string) ($data['tp_sus'] ?? '')),
            'qt_sus'                => (int) ($data['qt_sus'] ?? 0),
            'dt_atualizacao'        => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    private function transformEstabServico(array $data): array
    {
        return [
            'id'                    => (int) ($data['id'] ?? 0),
            'co_unidade'            => trim((string) ($data['co_unidade'] ?? '')),
            'co_servico'            => trim((string) ($data['co_servico'] ?? '')),
            'co_classificacao'      => trim((string) ($data['co_classificacao'] ?? '')),
            'co_ambulatorial'       => trim((string) ($data['co_ambulatorial'] ?? '')),
            'co_ambulatorial_sus'   => trim((string) ($data['co_ambulatorial_sus'] ?? '')),
            'co_hospitalar'         => trim((string) ($data['co_hospitalar'] ?? '')),
            'co_hospitalar_sus'     => trim((string) ($data['co_hospitalar_sus'] ?? '')),
            'dt_atualizacao'        => $this->convertTimestamp($data['dt_atualizacao'] ?? null),
        ];
    }

    /**
     * Converte timestamps do Debezium.
     * O Debezium pode enviar timestamps como:
     * - milissegundos desde epoch (inteiro)
     * - string ISO 8601
     */
    private function convertTimestamp(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        // Debezium envia timestamps MySQL como ms desde epoch (UTC)
        if (is_numeric($value)) {
            $seconds = (int) ($value / 1000);
            return gmdate('Y-m-d H:i:s', $seconds);
        }

        return (string) $value;
    }
}
