import gc
from models.AutomacaoWMS_CSW import RecarregaFilaTag, controle, AvaliacaoTags
from flask import Blueprint, jsonify, request
from functools import wraps

AtualizaFilaTags_routes = Blueprint('AtualizaFilaTags', __name__)

def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

## Funcao de Automacao 3 : Buscando a atualizacao das tag's em situacao gerada, disponibiliza os dados da situacao de tags em aberta : Duracao media de x Segundos
@AtualizaFilaTags_routes.route('/api/AtualizarTagsOFF', methods=['GET'])
@token_required
def atualizatagoff():
    # Obtém os dados do corpo da requisição (JSON)
    IntervaloAutomacao = request.args.get('IntervaloAutomacao',15)
    empresa = request.args.get('empresa','1')
    try:
        rotina = 'AtualizarTagsOFF'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, rotina)
        limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos, convertido para segundos)

        if tempo > limite:
            controle.InserindoStatus(rotina, client_ip, datainicio)
            RecarregaFilaTag.SalvarTagsNoBancoPostgre(rotina, client_ip, datainicio, empresa)
            controle.salvarStatus(rotina, client_ip, datainicio)
            gc.collect()
            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})

        else:
            gc.collect()
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO DA FILA TAGS OFF EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })

    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        return jsonify({"error": "O servidor foi reiniciado devido a um erro.", "status": False})
@AtualizaFilaTags_routes.route('/api/AtualizarTagsEstoque', methods=['GET'])
@token_required
def AtualizarTagsEstoque():
    # Obtém os dados do corpo da requisição (JSON)
    IntervaloAutomacao = request.args.get('IntervaloAutomacao', 15)
    empresa = request.args.get('empresa', '1')
    n_epc_atualizar = int(request.args.get('n_epc_atualizar',50))

    rotina = 'AtualizarTagsEstoque'
    client_ip = 'automacao'
    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacao(datainicio, rotina)
    limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos, convertido para segundos)
    print(datainicio)

    if tempo > limite:
            controle.InserindoStatus(rotina, client_ip, datainicio)
            RecarregaFilaTag.FilaTags(rotina, datainicio, empresa, n_epc_atualizar)
            RecarregaFilaTag.avaliacaoFila(rotina)

            controle.salvarStatus(rotina, client_ip, datainicio)
            gc.collect()
            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})

    else:
            gc.collect()
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO DA FILA TAGS  ESTOQUE EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })
@AtualizaFilaTags_routes.route('/api/LimpezaTagsSaidaForaWMS', methods=['GET'])
@token_required
def LimpezaTagsSaidaForaWMS():
        # Obtém os dados do corpo da requisição (JSON)
        IntervaloAutomacao = request.args.get('IntervaloAutomacao', 5)
        empresa = request.args.get('empresa', '1')

        # coloque o código que você deseja executar continuamente aqui
        rotina = 'LimpezaTagsSaidaForaWMS'
        client_ip = 'automacao'
        datainicio = controle.obterHoraAtual()
        tempo = controle.TempoUltimaAtualizacao(datainicio, 'LimpezaTagsSaidaForaWMS')
        limite = int(IntervaloAutomacao) * 60  # (limite de 60 minutos , convertido para segundos)

        if tempo > limite:
            controle.InserindoStatus(rotina, client_ip, datainicio)
            print('\nETAPA LimpezaTagsSaidaForaWMS- Inicio')
            AvaliacaoTags.avaliacaoFila(rotina, datainicio, empresa)
            controle.salvarStatus(rotina, 'automacao', datainicio)
            print('ETAPA LimpezaTagsSaidaForaWMS- Fim')
            gc.collect()
            return jsonify({"Mensagem": "Atualizado com sucesso", "status": True})


        else:
            return jsonify({
                "Mensagem": f" EXISTE UMA ATUALIZACAO {rotina}  EM MENOS DE {IntervaloAutomacao} MINUTOS",
                "status": False
            })
