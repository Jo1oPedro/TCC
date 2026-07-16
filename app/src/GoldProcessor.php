<?php

namespace TCC\Pipeline;

use PDO;

/**
 * Processa dados da camada Silver e gera agregacoes na camada Gold.
 *
 * Esta classe le EXCLUSIVAMENTE os arquivos Silver do lakehouse.
 * Em nenhum momento o MySQL e consultado. O fluxo completo:
 *
 *   MySQL -> Debezium -> Kafka -> Consumer -> Bronze -> Silver -> essa classe -> Gold
 *
 * Agregacoes produzidas:
 * - estab_summary:    visao desnormalizada de cada estabelecimento com totais
 * - municipal_health: indicadores de saude por municipio
 * - workforce:        perfil de cada profissional com vinculos e carga horaria
 */
class GoldProcessor
{
    private PDO $postgres;
    private LakehouseWriter $writer;
    private string $silverPath;

    public function __construct(PDO $postgres, LakehouseWriter $writer, string $lakehousePath)
    {
        $this->postgres = $postgres;
        $this->writer = $writer;
        $this->silverPath = rtrim($lakehousePath, '/') . '/silver';
    }

    /**
     * Executa todas as agregacoes gold a partir dos dados silver.
     */
    public function processAll(): array
    {
        // Carrega o estado atual de cada tabela a partir dos arquivos silver
        $estabelecimentos = $this->loadLatestState('estabelecimentos', 'co_unidade');
        $profissionais = $this->loadLatestState('profissionais', 'co_profissional_sus');
        $cargaHoraria = $this->loadLatestState('carga_horaria', 'id');
        $equipes = $this->loadLatestState('equipes', 'id');
        $equipeProfissionais = $this->loadLatestState('equipe_profissionais', 'id');
        $estabEquipamentos = $this->loadLatestState('estab_equipamentos', 'id');
        $estabServicos = $this->loadLatestState('estab_servicos', 'id');

        echo "[Gold] Silver loaded: "
            . count($estabelecimentos) . " estabelecimentos, "
            . count($profissionais) . " profissionais, "
            . count($cargaHoraria) . " carga_horaria, "
            . count($equipes) . " equipes, "
            . count($equipeProfissionais) . " equipe_prof, "
            . count($estabEquipamentos) . " equipamentos, "
            . count($estabServicos) . " servicos\n";

        return [
            'estab_summary' => $this->processEstabSummary(
                $estabelecimentos, $cargaHoraria, $equipes, $estabEquipamentos, $estabServicos
            ),
            'municipal_health' => $this->processMunicipalHealth(
                $estabelecimentos, $cargaHoraria, $equipes, $estabEquipamentos
            ),
            'workforce' => $this->processWorkforce(
                $profissionais, $cargaHoraria, $equipeProfissionais
            ),
        ];
    }

    /**
     * Le todos os arquivos silver de uma tabela e retorna o estado mais recente
     * de cada registro (por keyField), tratando INSERT, UPDATE e DELETE.
     *
     * Se um registro foi atualizado 3 vezes, mantem so a ultima versao.
     * Se um registro foi deletado (_is_deleted = true), ele e excluido.
     *
     * @param string $table    Nome da tabela
     * @param string $keyField Campo usado como chave primaria
     * @return array Mapa [keyValue => registro] com o estado mais recente
     */
    private function loadLatestState(string $table, string $keyField = 'id'): array
    {
        $dir = "{$this->silverPath}/{$table}";

        if (!is_dir($dir)) {
            return [];
        }

        $records = [];

        // Itera sobre todos os subdiretorios de data e arquivos jsonl
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \RecursiveDirectoryIterator::SKIP_DOTS)
        );

        foreach ($iterator as $file) {
            if (!$file->isFile() || $file->getExtension() !== 'jsonl') {
                continue;
            }

            $lines = file($file->getPathname(), FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
            if ($lines === false) continue;

            foreach ($lines as $line) {
                $record = json_decode($line, true);
                if ($record === null || !isset($record[$keyField])) continue;

                $key = $record[$keyField];

                // Se o registro foi deletado, remove do mapa
                if (!empty($record['_is_deleted'])) {
                    unset($records[$key]);
                    continue;
                }

                // Verifica se este registro e mais recente que o existente
                // Usa _processed_at como criterio de ordenacao
                if (isset($records[$key])) {
                    $existingTs = $records[$key]['_processed_at'] ?? '';
                    $newTs = $record['_processed_at'] ?? '';
                    if ($newTs <= $existingTs) {
                        continue; // registro existente e mais recente, ignora
                    }
                }

                $records[$key] = $record;
            }
        }

        return $records;
    }

    /**
     * Agregacao: resumo de cada estabelecimento de saude.
     * Cruza estabelecimentos com carga_horaria, equipes, equipamentos e servicos.
     */
    private function processEstabSummary(
        array $estabelecimentos,
        array $cargaHoraria,
        array $equipes,
        array $estabEquipamentos,
        array $estabServicos
    ): int {
        if (empty($estabelecimentos)) return 0;

        // Indexa dados auxiliares por co_unidade para lookup rapido
        $profByUnidade = [];
        foreach ($cargaHoraria as $ch) {
            $unidade = $ch['co_unidade'] ?? '';
            if ($unidade !== '') {
                $profByUnidade[$unidade] = ($profByUnidade[$unidade] ?? 0) + 1;
            }
        }

        $equipesByUnidade = [];
        foreach ($equipes as $eq) {
            $unidade = $eq['co_unidade'] ?? '';
            if ($unidade !== '') {
                $equipesByUnidade[$unidade] = ($equipesByUnidade[$unidade] ?? 0) + 1;
            }
        }

        $equipamentosByUnidade = [];
        foreach ($estabEquipamentos as $ee) {
            $unidade = $ee['co_unidade'] ?? '';
            if ($unidade !== '') {
                $equipamentosByUnidade[$unidade] = ($equipamentosByUnidade[$unidade] ?? 0)
                    + (int) ($ee['qt_existente'] ?? 0);
            }
        }

        $servicosByUnidade = [];
        foreach ($estabServicos as $es) {
            $unidade = $es['co_unidade'] ?? '';
            if ($unidade !== '') {
                $servicosByUnidade[$unidade] = ($servicosByUnidade[$unidade] ?? 0) + 1;
            }
        }

        $count = 0;

        $stmt = $this->postgres->prepare("
            INSERT INTO gold_estab_summary
                (co_unidade, co_cnes, razao_social, nome_fantasia,
                 tipo_estabelecimento, tipo_unidade, natureza_juridica,
                 municipio, uf, gestao, turno_atendimento,
                 total_profissionais, total_equipes, total_equipamentos, total_servicos,
                 latitude, longitude, dt_atualizacao, dt_processamento)
            VALUES
                (:co_unidade, :co_cnes, :razao_social, :nome_fantasia,
                 :tipo_estabelecimento, :tipo_unidade, :natureza_juridica,
                 :municipio, :uf, :gestao, :turno_atendimento,
                 :total_profissionais, :total_equipes, :total_equipamentos, :total_servicos,
                 :latitude, :longitude, :dt_atualizacao, NOW())
            ON CONFLICT (co_unidade) DO UPDATE SET
                co_cnes = EXCLUDED.co_cnes,
                razao_social = EXCLUDED.razao_social,
                nome_fantasia = EXCLUDED.nome_fantasia,
                tipo_estabelecimento = EXCLUDED.tipo_estabelecimento,
                tipo_unidade = EXCLUDED.tipo_unidade,
                natureza_juridica = EXCLUDED.natureza_juridica,
                municipio = EXCLUDED.municipio,
                uf = EXCLUDED.uf,
                gestao = EXCLUDED.gestao,
                turno_atendimento = EXCLUDED.turno_atendimento,
                total_profissionais = EXCLUDED.total_profissionais,
                total_equipes = EXCLUDED.total_equipes,
                total_equipamentos = EXCLUDED.total_equipamentos,
                total_servicos = EXCLUDED.total_servicos,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                dt_atualizacao = EXCLUDED.dt_atualizacao,
                dt_processamento = NOW()
        ");

        foreach ($estabelecimentos as $estab) {
            $coUnidade = $estab['co_unidade'] ?? '';

            $row = [
                'co_unidade'            => $coUnidade,
                'co_cnes'               => $estab['co_cnes'] ?? '',
                'razao_social'          => $estab['no_razao_social'] ?? '',
                'nome_fantasia'         => $estab['no_fantasia'] ?? '',
                'tipo_estabelecimento'  => $estab['co_tipo_estabelecimento'] ?? '',
                'tipo_unidade'          => $estab['co_tipo_unidade'] ?? '',
                'natureza_juridica'     => $estab['co_natureza_jur'] ?? '',
                'municipio'             => $estab['co_municipio_gestor'] ?? '',
                'uf'                    => $estab['co_estado_gestor'] ?? '',
                'gestao'                => $estab['tp_gestao'] ?? '',
                'turno_atendimento'     => $estab['co_turno_atendimento'] ?? '',
                'total_profissionais'   => $profByUnidade[$coUnidade] ?? 0,
                'total_equipes'         => $equipesByUnidade[$coUnidade] ?? 0,
                'total_equipamentos'    => $equipamentosByUnidade[$coUnidade] ?? 0,
                'total_servicos'        => $servicosByUnidade[$coUnidade] ?? 0,
                // Campos DECIMAL: string vazia nao e aceita pelo PostgreSQL,
                // entao normaliza para null quando ausente ou vazio.
                'latitude'              => self::numOrNull($estab['nu_latitude'] ?? null),
                'longitude'             => self::numOrNull($estab['nu_longitude'] ?? null),
                'dt_atualizacao'        => self::dateOrNull($estab['dt_atualizacao'] ?? null),
            ];

            // Grava no filesystem gold
            $this->writer->writeGold('estab_summary', $row);

            // Grava no PostgreSQL
            $stmt->execute([
                ':co_unidade'           => $row['co_unidade'],
                ':co_cnes'              => $row['co_cnes'],
                ':razao_social'         => $row['razao_social'],
                ':nome_fantasia'        => $row['nome_fantasia'],
                ':tipo_estabelecimento' => $row['tipo_estabelecimento'],
                ':tipo_unidade'         => $row['tipo_unidade'],
                ':natureza_juridica'    => $row['natureza_juridica'],
                ':municipio'            => $row['municipio'],
                ':uf'                   => $row['uf'],
                ':gestao'               => $row['gestao'],
                ':turno_atendimento'    => $row['turno_atendimento'],
                ':total_profissionais'  => $row['total_profissionais'],
                ':total_equipes'        => $row['total_equipes'],
                ':total_equipamentos'   => $row['total_equipamentos'],
                ':total_servicos'       => $row['total_servicos'],
                ':latitude'             => $row['latitude'],
                ':longitude'            => $row['longitude'],
                ':dt_atualizacao'       => $row['dt_atualizacao'],
            ]);

            $count++;
        }

        echo "[Gold] estab_summary: {$count} registros processados\n";
        return $count;
    }

    /**
     * Agregacao: indicadores de saude por municipio.
     * Agrupa estabelecimentos por co_municipio_gestor e calcula totais.
     */
    private function processMunicipalHealth(
        array $estabelecimentos,
        array $cargaHoraria,
        array $equipes,
        array $estabEquipamentos
    ): int {
        if (empty($estabelecimentos)) return 0;

        // Indexa estabelecimentos por municipio
        $estabByMunicipio = [];
        foreach ($estabelecimentos as $estab) {
            $mun = $estab['co_municipio_gestor'] ?? '';
            if ($mun === '') continue;
            $estabByMunicipio[$mun][] = $estab;
        }

        // Indexa carga_horaria por co_unidade
        $cargaByUnidade = [];
        foreach ($cargaHoraria as $ch) {
            $unidade = $ch['co_unidade'] ?? '';
            if ($unidade !== '') {
                $cargaByUnidade[$unidade][] = $ch;
            }
        }

        // Indexa equipes por co_unidade
        $equipesByUnidade = [];
        foreach ($equipes as $eq) {
            $unidade = $eq['co_unidade'] ?? '';
            if ($unidade !== '') {
                $equipesByUnidade[$unidade][] = $eq;
            }
        }

        // Indexa equipamentos por co_unidade
        $equipByUnidade = [];
        foreach ($estabEquipamentos as $ee) {
            $unidade = $ee['co_unidade'] ?? '';
            if ($unidade !== '') {
                $equipByUnidade[$unidade][] = $ee;
            }
        }

        $count = 0;

        $stmt = $this->postgres->prepare("
            INSERT INTO gold_municipal_health
                (co_municipio, nome_municipio, uf,
                 total_estabelecimentos, total_profissionais,
                 total_equipes_esf, total_equipes_outras,
                 total_equipamentos_sus,
                 qtd_ubs, qtd_hospitais, qtd_upas, qtd_outros,
                 horas_ambulatoriais_total, horas_hospitalares_total,
                 dt_processamento)
            VALUES
                (:co_municipio, :nome_municipio, :uf,
                 :total_estabelecimentos, :total_profissionais,
                 :total_equipes_esf, :total_equipes_outras,
                 :total_equipamentos_sus,
                 :qtd_ubs, :qtd_hospitais, :qtd_upas, :qtd_outros,
                 :horas_ambulatoriais_total, :horas_hospitalares_total,
                 NOW())
            ON CONFLICT (co_municipio) DO UPDATE SET
                nome_municipio = EXCLUDED.nome_municipio,
                uf = EXCLUDED.uf,
                total_estabelecimentos = EXCLUDED.total_estabelecimentos,
                total_profissionais = EXCLUDED.total_profissionais,
                total_equipes_esf = EXCLUDED.total_equipes_esf,
                total_equipes_outras = EXCLUDED.total_equipes_outras,
                total_equipamentos_sus = EXCLUDED.total_equipamentos_sus,
                qtd_ubs = EXCLUDED.qtd_ubs,
                qtd_hospitais = EXCLUDED.qtd_hospitais,
                qtd_upas = EXCLUDED.qtd_upas,
                qtd_outros = EXCLUDED.qtd_outros,
                horas_ambulatoriais_total = EXCLUDED.horas_ambulatoriais_total,
                horas_hospitalares_total = EXCLUDED.horas_hospitalares_total,
                dt_processamento = NOW()
        ");

        foreach ($estabByMunicipio as $coMunicipio => $estabs) {
            $totalEstab = count($estabs);

            // Coleta unidades deste municipio
            $unidadesMunicipio = [];
            foreach ($estabs as $estab) {
                $unidadesMunicipio[] = $estab['co_unidade'] ?? '';
            }

            // UF: pega do primeiro estabelecimento
            $uf = $estabs[0]['co_estado_gestor'] ?? '';

            // Profissionais distintos neste municipio (via carga_horaria)
            $profissionaisDistintos = [];
            $horasAmbulatoriais = 0;
            $horasHospitalares = 0;
            foreach ($unidadesMunicipio as $unidade) {
                if (isset($cargaByUnidade[$unidade])) {
                    foreach ($cargaByUnidade[$unidade] as $ch) {
                        $profSus = $ch['co_profissional_sus'] ?? '';
                        if ($profSus !== '') {
                            $profissionaisDistintos[$profSus] = true;
                        }
                        $horasAmbulatoriais += (int) ($ch['qt_carga_horaria_ambulatorial'] ?? 0);
                        $horasHospitalares += (int) ($ch['qt_carga_hor_hosp_sus'] ?? 0);
                    }
                }
            }

            // Equipes ESF (tp_equipe IN ('70','76')) vs outras
            $totalEquipesEsf = 0;
            $totalEquipesOutras = 0;
            foreach ($unidadesMunicipio as $unidade) {
                if (isset($equipesByUnidade[$unidade])) {
                    foreach ($equipesByUnidade[$unidade] as $eq) {
                        $tpEquipe = $eq['tp_equipe'] ?? '';
                        if (in_array($tpEquipe, ['70', '76'], true)) {
                            $totalEquipesEsf++;
                        } else {
                            $totalEquipesOutras++;
                        }
                    }
                }
            }

            // Equipamentos SUS
            $totalEquipamentosSus = 0;
            foreach ($unidadesMunicipio as $unidade) {
                if (isset($equipByUnidade[$unidade])) {
                    foreach ($equipByUnidade[$unidade] as $ee) {
                        $totalEquipamentosSus += (int) ($ee['qt_sus'] ?? 0);
                    }
                }
            }

            // Classificacao por tipo de estabelecimento
            $qtdUbs = 0;
            $qtdHospitais = 0;
            $qtdUpas = 0;
            $qtdOutros = 0;
            foreach ($estabs as $estab) {
                $tipo = $estab['co_tipo_estabelecimento'] ?? '';
                if (in_array($tipo, ['001', '002'], true)) {
                    $qtdUbs++;
                } elseif (in_array($tipo, ['005', '007'], true)) {
                    $qtdHospitais++;
                } elseif (in_array($tipo, ['009', '020'], true)) {
                    $qtdUpas++;
                } else {
                    $qtdOutros++;
                }
            }

            // Nome do municipio: usamos o codigo como fallback (sem acesso a tabela de dimensao)
            $nomeMunicipio = (string) $coMunicipio;

            $row = [
                'co_municipio'              => (string) $coMunicipio,
                'nome_municipio'            => $nomeMunicipio,
                'uf'                        => $uf,
                'total_estabelecimentos'    => $totalEstab,
                'total_profissionais'       => count($profissionaisDistintos),
                'total_equipes_esf'         => $totalEquipesEsf,
                'total_equipes_outras'      => $totalEquipesOutras,
                'total_equipamentos_sus'    => $totalEquipamentosSus,
                'qtd_ubs'                   => $qtdUbs,
                'qtd_hospitais'             => $qtdHospitais,
                'qtd_upas'                  => $qtdUpas,
                'qtd_outros'                => $qtdOutros,
                'horas_ambulatoriais_total' => $horasAmbulatoriais,
                'horas_hospitalares_total'  => $horasHospitalares,
            ];

            // Grava no filesystem gold
            $this->writer->writeGold('municipal_health', $row);

            // Grava no PostgreSQL
            $stmt->execute([
                ':co_municipio'              => $row['co_municipio'],
                ':nome_municipio'            => $row['nome_municipio'],
                ':uf'                        => $row['uf'],
                ':total_estabelecimentos'    => $row['total_estabelecimentos'],
                ':total_profissionais'       => $row['total_profissionais'],
                ':total_equipes_esf'         => $row['total_equipes_esf'],
                ':total_equipes_outras'      => $row['total_equipes_outras'],
                ':total_equipamentos_sus'    => $row['total_equipamentos_sus'],
                ':qtd_ubs'                   => $row['qtd_ubs'],
                ':qtd_hospitais'             => $row['qtd_hospitais'],
                ':qtd_upas'                  => $row['qtd_upas'],
                ':qtd_outros'                => $row['qtd_outros'],
                ':horas_ambulatoriais_total' => $row['horas_ambulatoriais_total'],
                ':horas_hospitalares_total'  => $row['horas_hospitalares_total'],
            ]);

            $count++;
        }

        echo "[Gold] municipal_health: {$count} registros processados\n";
        return $count;
    }

    /**
     * Agregacao: perfil de forca de trabalho por profissional.
     * Cruza profissionais com carga_horaria e equipe_profissionais.
     */
    private function processWorkforce(
        array $profissionais,
        array $cargaHoraria,
        array $equipeProfissionais
    ): int {
        if (empty($profissionais)) return 0;

        // Indexa carga_horaria por co_profissional_sus
        $cargaByProf = [];
        foreach ($cargaHoraria as $ch) {
            $profSus = $ch['co_profissional_sus'] ?? '';
            if ($profSus !== '') {
                $cargaByProf[$profSus][] = $ch;
            }
        }

        // Indexa equipe_profissionais por co_profissional_sus
        $equipeByProf = [];
        foreach ($equipeProfissionais as $ep) {
            $profSus = $ep['co_profissional_sus'] ?? '';
            if ($profSus !== '') {
                $equipeByProf[$profSus][] = $ep;
            }
        }

        $count = 0;

        $stmt = $this->postgres->prepare("
            INSERT INTO gold_workforce
                (co_profissional_sus, no_profissional, cbo_codigo, cbo_descricao,
                 conselho_classe, total_vinculos,
                 horas_ambulatorial, horas_hospitalar, horas_outros,
                 qtd_estabelecimentos, qtd_equipes, dt_processamento)
            VALUES
                (:co_profissional_sus, :no_profissional, :cbo_codigo, :cbo_descricao,
                 :conselho_classe, :total_vinculos,
                 :horas_ambulatorial, :horas_hospitalar, :horas_outros,
                 :qtd_estabelecimentos, :qtd_equipes, NOW())
            ON CONFLICT (co_profissional_sus) DO UPDATE SET
                no_profissional = EXCLUDED.no_profissional,
                cbo_codigo = EXCLUDED.cbo_codigo,
                cbo_descricao = EXCLUDED.cbo_descricao,
                conselho_classe = EXCLUDED.conselho_classe,
                total_vinculos = EXCLUDED.total_vinculos,
                horas_ambulatorial = EXCLUDED.horas_ambulatorial,
                horas_hospitalar = EXCLUDED.horas_hospitalar,
                horas_outros = EXCLUDED.horas_outros,
                qtd_estabelecimentos = EXCLUDED.qtd_estabelecimentos,
                qtd_equipes = EXCLUDED.qtd_equipes,
                dt_processamento = NOW()
        ");

        foreach ($profissionais as $prof) {
            $coProf = $prof['co_profissional_sus'] ?? '';
            if ($coProf === '') continue;

            $vinculos = $cargaByProf[$coProf] ?? [];
            $equipeProfs = $equipeByProf[$coProf] ?? [];

            $totalVinculos = count($vinculos);
            $horasAmbulatorial = 0;
            $horasHospitalar = 0;
            $horasOutros = 0;
            $unidadesDistintas = [];
            $cboCodigo = '';
            $conselhoClasse = '';

            foreach ($vinculos as $v) {
                $horasAmbulatorial += (int) ($v['qt_carga_horaria_ambulatorial'] ?? 0);
                $horasHospitalar += (int) ($v['qt_carga_hor_hosp_sus'] ?? 0);
                $horasOutros += (int) ($v['qt_carga_horaria_outros'] ?? 0);

                $unidade = $v['co_unidade'] ?? '';
                if ($unidade !== '') {
                    $unidadesDistintas[$unidade] = true;
                }

                // Pega CBO e conselho do primeiro vinculo encontrado
                if ($cboCodigo === '' && !empty($v['co_cbo'])) {
                    $cboCodigo = $v['co_cbo'];
                }
                if ($conselhoClasse === '' && !empty($v['co_conselho_classe'])) {
                    $conselhoClasse = $v['co_conselho_classe'];
                }
            }

            $row = [
                'co_profissional_sus' => $coProf,
                'no_profissional'     => $prof['no_profissional'] ?? '',
                'cbo_codigo'          => $cboCodigo,
                'cbo_descricao'       => $cboCodigo, // Sem acesso a tabela de dimensao, usa o codigo
                'conselho_classe'     => $conselhoClasse,
                'total_vinculos'      => $totalVinculos,
                'horas_ambulatorial'  => $horasAmbulatorial,
                'horas_hospitalar'    => $horasHospitalar,
                'horas_outros'        => $horasOutros,
                'qtd_estabelecimentos' => count($unidadesDistintas),
                'qtd_equipes'         => count($equipeProfs),
            ];

            // Grava no filesystem gold
            $this->writer->writeGold('workforce', $row);

            // Grava no PostgreSQL
            $stmt->execute([
                ':co_profissional_sus' => $row['co_profissional_sus'],
                ':no_profissional'     => $row['no_profissional'],
                ':cbo_codigo'          => $row['cbo_codigo'],
                ':cbo_descricao'       => $row['cbo_descricao'],
                ':conselho_classe'     => $row['conselho_classe'],
                ':total_vinculos'      => $row['total_vinculos'],
                ':horas_ambulatorial'  => $row['horas_ambulatorial'],
                ':horas_hospitalar'    => $row['horas_hospitalar'],
                ':horas_outros'        => $row['horas_outros'],
                ':qtd_estabelecimentos' => $row['qtd_estabelecimentos'],
                ':qtd_equipes'         => $row['qtd_equipes'],
            ]);

            $count++;
        }

        echo "[Gold] workforce: {$count} registros processados\n";
        return $count;
    }

    /**
     * Normaliza um valor para insercao em coluna numerica (DECIMAL) do PostgreSQL.
     * String vazia, null ou nao-numerico viram null; caso contrario retorna o numero.
     */
    private static function numOrNull($value): ?float
    {
        if ($value === null || $value === '' || !is_numeric($value)) {
            return null;
        }
        return (float) $value;
    }

    /**
     * Normaliza um valor para insercao em coluna DATE do PostgreSQL.
     * String vazia ou null viram null.
     */
    private static function dateOrNull($value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }
        return (string) $value;
    }
}
