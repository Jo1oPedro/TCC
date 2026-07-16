<?php

namespace TCC\Pipeline;

class ScenarioConfig
{
    // Municipio data: array of [code, name, uf_sigla]
    private(set) array $municipios = [
        ['355030', 'SAO PAULO', 'SP'], ['330455', 'RIO DE JANEIRO', 'RJ'],
        ['310620', 'BELO HORIZONTE', 'MG'], ['410690', 'CURITIBA', 'PR'],
        ['431490', 'PORTO ALEGRE', 'RS'], ['292740', 'SALVADOR', 'BA'],
        ['261160', 'RECIFE', 'PE'], ['230440', 'FORTALEZA', 'CE'],
        ['530010', 'BRASILIA', 'DF'], ['316990', 'JUIZ DE FORA', 'MG'],
    ];

    private(set) array $tiposEstabelecimento = ['001', '002', '004', '005', '007', '036', '039'];

    private(set) array $tiposEquipe = ['70', '71', '72', '74', '76', '24', '34'];

    private(set) array $naturezasJuridicas = ['1031', '1244', '2062', '3069', '3999'];

    private(set) array $gestoes = ['01', '02', '04', '06'];

    private(set) array $turnos = ['03', '04', '06'];

    private(set) array $cboCodes = [
        '225125', '225142', '225170', '225130', '223505', '223565',
        '223810', '226305', '223905', '322205', '322245', '251510',
    ];

    private(set) array $conselhosClasse = ['05', '07', '08', '09', '11', '12'];

    private(set) array $nomesFantasia = [
        'UBS CENTRO', 'UBS JARDIM AMERICA', 'UBS VILA NOVA', 'HOSPITAL MUNICIPAL',
        'UPA 24H', 'CENTRO DE SAUDE', 'CLINICA DA FAMILIA', 'POSTO DE SAUDE CENTRAL',
        'UBS SAO JOSE', 'UBS SANTA MARIA', 'HOSPITAL REGIONAL', 'POLICLINICA CENTRAL',
        'UBS BOA VISTA', 'CAPS ADULTO', 'ESF COMUNIDADE FELIZ', 'UBS NOVA ESPERANCA',
    ];

    private(set) array $nomesProfissionais = [
        'MARIA SILVA', 'JOAO SANTOS', 'ANA OLIVEIRA', 'PEDRO SOUZA',
        'CARLA FERREIRA', 'LUCAS PEREIRA', 'JULIANA COSTA', 'MARCOS ALMEIDA',
        'PATRICIA LIMA', 'RAFAEL RIBEIRO', 'FERNANDA GOMES', 'BRUNO MARTINS',
        'CAMILA RODRIGUES', 'THIAGO NASCIMENTO', 'AMANDA BARBOSA', 'DIEGO CARDOSO',
    ];

    // Cenarios ordenados por taxa nominal crescente (op/s).
    // 'low', 'mixed', 'moderate' e 'burst' sao mantidos por compatibilidade com
    // os experimentos anteriores; 'rate25', 'rate40', 'rate60' e 'rate80' foram
    // adicionados para cobrir a regiao de transicao entre regime sustentavel
    // (~10 op/s) e saturacao (~100 op/s), conforme indicado na avaliacao
    // experimental do TCC.
    //
    // interval_ms = 1000 / taxa_nominal (op/s)
    private(set) array $scenarios = [
        'low' => [           // 1 op/s
            'interval_ms' => 1000,
            'nominal_ops' => 1,
            'operations' => [
                'insert_estabelecimento' => 40,
                'update_estabelecimento' => 30,
                'insert_vinculo' => 20,
                'update_vinculo' => 10,
            ],
        ],
        'mixed' => [         // 5 op/s
            'interval_ms' => 200,
            'nominal_ops' => 5,
            'operations' => [
                'insert_estabelecimento' => 10,
                'update_estabelecimento' => 20,
                'insert_vinculo' => 20,
                'update_vinculo' => 25,
                'manage_equipe' => 25,
            ],
        ],
        'moderate' => [      // 10 op/s
            'interval_ms' => 100,
            'nominal_ops' => 10,
            'operations' => [
                'insert_estabelecimento' => 15,
                'update_estabelecimento' => 15,
                'insert_vinculo' => 30,
                'update_vinculo' => 20,
                'manage_equipe' => 20,
            ],
        ],
        'rate25' => [        // 25 op/s — entre regime sustentavel e saturacao
            'interval_ms' => 40,
            'nominal_ops' => 25,
            'operations' => [
                'insert_estabelecimento' => 15,
                'update_estabelecimento' => 15,
                'insert_vinculo' => 30,
                'update_vinculo' => 20,
                'manage_equipe' => 20,
            ],
        ],
        'rate40' => [        // 40 op/s — proximo do joelho da curva
            'interval_ms' => 25,
            'nominal_ops' => 40,
            'operations' => [
                'insert_estabelecimento' => 15,
                'update_estabelecimento' => 15,
                'insert_vinculo' => 30,
                'update_vinculo' => 20,
                'manage_equipe' => 20,
            ],
        ],
        'rate60' => [        // 60 op/s — regime saturado leve
            'interval_ms' => 17,
            'nominal_ops' => 60,
            'operations' => [
                'insert_estabelecimento' => 20,
                'update_estabelecimento' => 10,
                'insert_vinculo' => 35,
                'update_vinculo' => 15,
                'manage_equipe' => 20,
            ],
        ],
        'rate80' => [        // 80 op/s — regime saturado
            'interval_ms' => 13,
            'nominal_ops' => 80,
            'operations' => [
                'insert_estabelecimento' => 25,
                'update_estabelecimento' => 10,
                'insert_vinculo' => 35,
                'update_vinculo' => 15,
                'manage_equipe' => 15,
            ],
        ],
        'burst' => [         // 100 op/s — limite superior
            'interval_ms' => 10,
            'nominal_ops' => 100,
            'operations' => [
                'insert_estabelecimento' => 25,
                'update_estabelecimento' => 10,
                'insert_vinculo' => 35,
                'update_vinculo' => 15,
                'manage_equipe' => 15,
            ],
        ],
    ];

    public function getScenario(string $scenario): array
    {
        if (!isset($this->scenarios[$scenario])) {
            echo "Cenario desconhecido: {$scenario}\n";
            echo "Cenarios disponiveis: " . implode(', ', array_keys($this->scenarios)) . "\n";
            exit(1);
        }
        return $this->scenarios[$scenario];
    }
}
