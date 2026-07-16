<?php

namespace TCC\Pipeline;

use Exception;

class LoadGenerator
{
    private(set) array $counts = [
        'insert_estabelecimento' => 0,
        'update_estabelecimento' => 0,
        'insert_vinculo' => 0,
        'update_vinculo' => 0,
        'manage_equipe' => 0,
        'errors' => 0,
    ];
    private \PDO $pdo;
    private ScenarioConfig $scenarioConfig;

    public function __construct(
        array $config,
        private ScenarioConfig $scenario
    ) {
        $this->pdo = DatabaseConnection::get('mysql', $config['mysql']);
        $this->scenarioConfig = $this->scenario;
    }

    /**
     * Insere um novo estabelecimento de saude no MySQL.
     * Gera co_unidade combinando codigo do municipio + 7 digitos aleatorios.
     */
    public function insertEstabelecimento(): void
    {
        try {
            $municipio = $this->scenarioConfig->municipios[array_rand($this->scenarioConfig->municipios)];
            $coMunicipio = $municipio[0];
            $nomeMunicipio = $municipio[1];
            $uf = $municipio[2];

            $coUnidade = $coMunicipio . str_pad((string) random_int(0, 9999999), 7, '0', STR_PAD_LEFT);
            $coCnes = str_pad((string) random_int(0, 9999999), 7, '0', STR_PAD_LEFT);

            $tipoEstab = $this->scenarioConfig->tiposEstabelecimento[array_rand($this->scenarioConfig->tiposEstabelecimento)];
            $natureza = $this->scenarioConfig->naturezasJuridicas[array_rand($this->scenarioConfig->naturezasJuridicas)];
            $gestao = $this->scenarioConfig->gestoes[array_rand($this->scenarioConfig->gestoes)];
            $turno = $this->scenarioConfig->turnos[array_rand($this->scenarioConfig->turnos)];
            $nomeFantasia = $this->scenarioConfig->nomesFantasia[array_rand($this->scenarioConfig->nomesFantasia)];

            $razaoSocial = 'SECRETARIA MUNICIPAL DE SAUDE DE ' . $nomeMunicipio;
            $logradouro = 'RUA ' . strtoupper($this->scenarioConfig->nomesProfissionais[array_rand($this->scenarioConfig->nomesProfissionais)]);
            $nuEndereco = (string) random_int(1, 9999);
            $bairro = 'CENTRO';
            $coCep = str_pad((string) random_int(10000000, 99999999), 8, '0', STR_PAD_LEFT);
            $telefone = '(' . substr($coMunicipio, 0, 2) . ') ' . random_int(30000000, 39999999);
            $email = strtolower(str_replace(' ', '', $nomeFantasia)) . '@saude.' . strtolower($uf) . '.gov.br';
            $latitude = round(-3.0 - (mt_rand() / mt_getrandmax()) * 30, 6);
            $longitude = round(-35.0 - (mt_rand() / mt_getrandmax()) * 20, 6);

            $stmt = $this->pdo->prepare("
                INSERT INTO estabelecimentos
                    (co_unidade, co_cnes, no_razao_social, no_fantasia, no_logradouro,
                     nu_endereco, no_bairro, co_cep, nu_telefone, no_email,
                     co_turno_atendimento, co_estado_gestor, co_municipio_gestor,
                     nu_latitude, nu_longitude, co_natureza_jur, st_conexao_internet,
                     co_tipo_unidade, tp_gestao, co_tipo_estabelecimento, co_atividade_principal)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ");
            $stmt->execute([
                $coUnidade, $coCnes, $razaoSocial, $nomeFantasia, $logradouro,
                $nuEndereco, $bairro, $coCep, $telefone, $email,
                $turno, $uf, $coMunicipio,
                (string) $latitude, (string) $longitude, $natureza, '1',
                $tipoEstab, $gestao, $tipoEstab, '01',
            ]);

            $this->counts['insert_estabelecimento']++;
        } catch (Exception $e) {
            $this->counts['errors']++;
            throw $e;
        }
    }

    /**
     * Atualiza dados de um estabelecimento existente (telefone, email, coordenadas).
     */
    public function updateEstabelecimento(): void
    {
        try {
            $estab = $this->pdo->query("SELECT co_unidade, nu_latitude, nu_longitude FROM estabelecimentos ORDER BY RAND() LIMIT 1")->fetch();
            if (!$estab) return;

            // Decide aleatoriamente o que atualizar
            $choice = random_int(1, 3);
            if ($choice === 1) {
                // Atualiza telefone
                $novoTelefone = '(' . random_int(11, 99) . ') ' . random_int(30000000, 39999999);
                $stmt = $this->pdo->prepare("UPDATE estabelecimentos SET nu_telefone = ? WHERE co_unidade = ?");
                $stmt->execute([$novoTelefone, $estab['co_unidade']]);
            } elseif ($choice === 2) {
                // Atualiza email
                $novoEmail = 'contato' . random_int(1, 999) . '@saude.gov.br';
                $stmt = $this->pdo->prepare("UPDATE estabelecimentos SET no_email = ? WHERE co_unidade = ?");
                $stmt->execute([$novoEmail, $estab['co_unidade']]);
            } else {
                // Atualiza coordenadas com leve variacao
                $lat = (float) ($estab['nu_latitude'] ?? -15.0);
                $lng = (float) ($estab['nu_longitude'] ?? -47.0);
                $novaLat = round($lat + (mt_rand() / mt_getrandmax() - 0.5) * 0.01, 6);
                $novaLng = round($lng + (mt_rand() / mt_getrandmax() - 0.5) * 0.01, 6);
                $stmt = $this->pdo->prepare("UPDATE estabelecimentos SET nu_latitude = ?, nu_longitude = ? WHERE co_unidade = ?");
                $stmt->execute([(string) $novaLat, (string) $novaLng, $estab['co_unidade']]);
            }

            $this->counts['update_estabelecimento']++;
        } catch (Exception $e) {
            $this->counts['errors']++;
            throw $e;
        }
    }

    /**
     * Insere um novo vinculo profissional-estabelecimento (carga_horaria).
     * Se o vinculo ja existir (UK: co_unidade, co_profissional_sus, co_cbo),
     * atualiza as horas via ON DUPLICATE KEY UPDATE.
     */
    public function insertVinculo(): void
    {
        try {
            // Pega um profissional aleatorio
            $prof = $this->pdo->query("SELECT co_profissional_sus FROM profissionais ORDER BY RAND() LIMIT 1")->fetch();
            if (!$prof) return;

            // Pega um estabelecimento aleatorio
            $estab = $this->pdo->query("SELECT co_unidade FROM estabelecimentos ORDER BY RAND() LIMIT 1")->fetch();
            if (!$estab) return;

            $cbo = $this->scenarioConfig->cboCodes[array_rand($this->scenarioConfig->cboCodes)];
            $conselho = $this->scenarioConfig->conselhosClasse[array_rand($this->scenarioConfig->conselhosClasse)];
            $horasAmb = random_int(10, 40);
            $horasHosp = random_int(0, 24);
            $horasOutros = random_int(0, 10);
            $nuRegistro = str_pad((string) random_int(1000, 999999), 6, '0', STR_PAD_LEFT);

            $stmt = $this->pdo->prepare("
                INSERT INTO carga_horaria
                    (co_unidade, co_profissional_sus, co_cbo, tp_sus_nao_sus, ind_vinculacao,
                     qt_carga_horaria_ambulatorial, co_conselho_classe, nu_registro,
                     qt_carga_horaria_outros, qt_carga_hor_hosp_sus)
                VALUES
                    (?, ?, ?, '01', '01', ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    qt_carga_horaria_ambulatorial = VALUES(qt_carga_horaria_ambulatorial),
                    qt_carga_hor_hosp_sus = VALUES(qt_carga_hor_hosp_sus),
                    qt_carga_horaria_outros = VALUES(qt_carga_horaria_outros)
            ");
            $stmt->execute([
                $estab['co_unidade'],
                $prof['co_profissional_sus'],
                $cbo,
                $horasAmb,
                $conselho,
                $nuRegistro,
                $horasOutros,
                $horasHosp,
            ]);

            $this->counts['insert_vinculo']++;
        } catch (Exception $e) {
            $this->counts['errors']++;
            throw $e;
        }
    }

    /**
     * Atualiza horas de carga horaria de um vinculo existente.
     */
    public function updateVinculo(): void
    {
        try {
            $ch = $this->pdo->query("SELECT id, qt_carga_horaria_ambulatorial, qt_carga_hor_hosp_sus FROM carga_horaria ORDER BY RAND() LIMIT 1")->fetch();
            if (!$ch) return;

            $novasHorasAmb = max(0, (int) $ch['qt_carga_horaria_ambulatorial'] + random_int(-5, 10));
            $novasHorasHosp = max(0, (int) $ch['qt_carga_hor_hosp_sus'] + random_int(-3, 6));

            $stmt = $this->pdo->prepare("
                UPDATE carga_horaria
                SET qt_carga_horaria_ambulatorial = ?, qt_carga_hor_hosp_sus = ?
                WHERE id = ?
            ");
            $stmt->execute([$novasHorasAmb, $novasHorasHosp, $ch['id']]);

            $this->counts['update_vinculo']++;
        } catch (Exception $e) {
            $this->counts['errors']++;
            throw $e;
        }
    }

    /**
     * Gerencia equipes: 50% de chance de inserir nova equipe,
     * 50% de chance de vincular profissional a equipe existente.
     */
    public function manageEquipe(): void
    {
        try {
            if (random_int(0, 1) === 0) {
                $this->insertEquipe();
            } else {
                $this->addProfissionalToEquipe();
            }
            $this->counts['manage_equipe']++;
        } catch (Exception $e) {
            $this->counts['errors']++;
            throw $e;
        }
    }

    /**
     * Insere uma nova equipe de saude.
     */
    private function insertEquipe(): void
    {
        $municipio = $this->scenarioConfig->municipios[array_rand($this->scenarioConfig->municipios)];
        $coMunicipio = $municipio[0];

        // Pega um estabelecimento deste municipio ou qualquer um
        $estab = $this->pdo->prepare("SELECT co_unidade FROM estabelecimentos WHERE co_municipio_gestor = ? ORDER BY RAND() LIMIT 1");
        $estab->execute([$coMunicipio]);
        $estab = $estab->fetch();

        if (!$estab) {
            $estab = $this->pdo->query("SELECT co_unidade FROM estabelecimentos ORDER BY RAND() LIMIT 1")->fetch();
            if (!$estab) return;
        }

        $tpEquipe = $this->scenarioConfig->tiposEquipe[array_rand($this->scenarioConfig->tiposEquipe)];
        $coArea = str_pad((string) random_int(1, 9999), 4, '0', STR_PAD_LEFT);
        $seqEquipe = str_pad((string) random_int(1, 999), 8, '0', STR_PAD_LEFT);
        $noReferencia = $this->scenarioConfig->nomesFantasia[array_rand($this->scenarioConfig->nomesFantasia)] . ' EQUIPE';
        $coEquipe = str_pad((string) random_int(1, 9999999), 10, '0', STR_PAD_LEFT);

        $stmt = $this->pdo->prepare("
            INSERT IGNORE INTO equipes
                (co_municipio, co_area, seq_equipe, co_unidade, tp_equipe,
                 no_referencia, dt_ativacao, co_equipe)
            VALUES
                (?, ?, ?, ?, ?, ?, NOW(), ?)
        ");
        $stmt->execute([
            $coMunicipio, $coArea, $seqEquipe, $estab['co_unidade'],
            $tpEquipe, $noReferencia, $coEquipe,
        ]);
    }

    /**
     * Vincula um profissional a uma equipe existente.
     */
    private function addProfissionalToEquipe(): void
    {
        $equipe = $this->pdo->query("SELECT id, co_municipio, co_area, seq_equipe, co_unidade FROM equipes ORDER BY RAND() LIMIT 1")->fetch();
        if (!$equipe) {
            // Sem equipes, insere uma nova
            $this->insertEquipe();
            return;
        }

        $prof = $this->pdo->query("SELECT co_profissional_sus FROM profissionais ORDER BY RAND() LIMIT 1")->fetch();
        if (!$prof) return;

        $cbo = $this->scenarioConfig->cboCodes[array_rand($this->scenarioConfig->cboCodes)];

        $stmt = $this->pdo->prepare("
            INSERT IGNORE INTO equipe_profissionais
                (co_municipio, co_area, seq_equipe, co_profissional_sus,
                 co_unidade, co_cbo, tp_sus_nao_sus, ind_vinculacao,
                 dt_entrada, st_equipeminima)
            VALUES
                (?, ?, ?, ?, ?, ?, '01', '01', NOW(), '1')
        ");
        $stmt->execute([
            $equipe['co_municipio'],
            $equipe['co_area'],
            $equipe['seq_equipe'],
            $prof['co_profissional_sus'],
            $equipe['co_unidade'],
            $cbo,
        ]);
    }
}
