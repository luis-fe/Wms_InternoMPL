from models import pedidosModel, imprimirEtiquetaModel, PedidosClass
from models.Pedidos import FilaPedidos_model
from flask import Blueprint, jsonify, request
from functools import wraps
import pandas as pd
from  models.configuracoes import empresaConfigurada

pedidos_routes = Blueprint('pedidos', __name__)


def token_required(f): # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

# Nessa Api é listado os pedidos em aberto
@pedidos_routes.route('/api/FilaPedidos', methods=['GET'])
@token_required
def get_FilaPedidos():
    empresa = empresaConfigurada.EmpresaEscolhida()
    Pedidos = FilaPedidos_model.FilaPedidos(empresa)
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    pedidosModel.obtendoAsultimasDevolucoes(empresa)

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@pedidos_routes.route('/api/FilaPedidosUsuario', methods=['GET'])
@token_required
def get_FilaPedidosUsuario():
    empresa = empresaConfigurada.EmpresaEscolhida()
    codUsuario = request.args.get('codUsuario')
    Pedidos = FilaPedidos_model.FilaAtribuidaUsuario(codUsuario, empresa)
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)
@pedidos_routes.route('/api/DetalharPedido', methods=['GET'])
@token_required
def get_DetalharPedido():
    # Obtém os dados do corpo da requisição (JSON)
    codPedido = request.args.get('codPedido')

    Endereco_det = pedidosModel.DetalhaPedido(codPedido)
    Endereco_det = pd.DataFrame(Endereco_det)

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@pedidos_routes.route('/api/FilaPedidosClassificacao', methods=['GET'])
@token_required
def get_FilaPedidosClassificacao():
    coluna = request.args.get('coluna','01-CodPedido')
    tipo = request.args.get('tipo','desc')
    empresa = empresaConfigurada.EmpresaEscolhida()

    Pedidos = FilaPedidos_model.ClassificarFila(coluna, tipo, empresa)
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@pedidos_routes.route('/api/AtribuirPedidos', methods=['POST'])
@token_required
def get_AtribuirPedidos():
    try:
        # Obtém os dados do corpo da requisição (JSON)
        datas = request.get_json()
        codUsuario = datas['codUsuario']
        data = datas['data']
        pedidos = datas['pedidos']

        Endereco_det = pedidosModel.AtribuirPedido(codUsuario, pedidos, data)
        Endereco_det = pd.DataFrame(Endereco_det)

        # Obtém os nomes das colunas
        column_names = Endereco_det.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        end_data = []
        for index, row in Endereco_det.iterrows():
            end_dict = {}
            for column_name in column_names:
                end_dict[column_name] = row[column_name]
            end_data.append(end_dict)
        return jsonify(end_data)
    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500


@pedidos_routes.route('/api/IndicadorDistribuicao', methods=['GET'])
@token_required
def IndicadorDistribuicao():


    Endereco_det = pedidosModel.AtribuicaoDiaria()


    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@pedidos_routes.route('/api/ConsultaPedidoViaTag', methods=['GET'])
@token_required
def get_ConsultaPedidoViaTag():
    codBarras = request.args.get('codBarras')
    TagReposicao = pedidosModel.InformacaoPedidoViaTag(codBarras)

    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@pedidos_routes.route('/api/imprimirEtiqueta', methods=['POST'])
@token_required
def imprimirEtiqueta():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    pedido = datas['pedido']
    pedido = pedido.strip()
    print(pedido)
    codcliente, cliente, separador, transportadora, agrupamento, prioridade,obs = pedidosModel.InformacaoImpresao(pedido)

    print(type(codcliente))  # Correção aqui

    codcliente = codcliente.replace('.0','')
    if obs == '-':
        obs =''

    TagReposicao = imprimirEtiquetaModel.criar_pdf(f'impressao.pdf', cliente, codcliente, pedido, transportadora, separador,agrupamento,prioridade, obs)
    imprimirEtiquetaModel.imprimir_pdf(f'impressao.pdf')

    return jsonify({'message': f'Imprimido o pedido {pedido} com sucesso', 'status':True})

@pedidos_routes.route('/api/Prioriza', methods=['PUT'])
@token_required
def Prioriza():
    try:
        # Obtém os dados do corpo da requisição (JSON)
        datas = request.get_json()
        pedidos = datas['pedidos']

        Endereco_det = pedidosModel.PrioridadePedido(pedidos)
        if Endereco_det == True:
            return jsonify({'message': f'pedidos priorizados com sucesso', 'status': True}), 200
        else:
            return jsonify({'message': f'pedidos nao encontrados', 'status': False}), 200


    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500


@pedidos_routes.route('/api/LimparPedido', methods=['PUT'])
@token_required
def put_limparPedido():
        # Obtém os dados do corpo da requisição (JSON)
        datas = request.get_json()
        pedidos = datas['pedidos']

        Endereco_det = pedidosModel.limparPedido(pedidos)
        if Endereco_det == True:
            return jsonify({'message': f'pedidos limpados com sucesso', 'status': True}), 200
        else:
            return jsonify({'message': f'pedidos nao encontrados', 'status': False}), 200


@pedidos_routes.route('/api/agrupar_pedido', methods=['get'])
@token_required
def get_agrupar_pedido():
        # Obtém os dados do corpo da requisição (JSON)
        pedidos = PedidosClass.Pedido('1').agrupar_pedidos()

        Endereco_det = pedidosModel.limparPedido(pedidos)
        if Endereco_det == True:
            return jsonify({'message': f'pedidos agrupados com sucesso', 'status': True}), 200
        else:
            return jsonify({'message': f'pedidos nao encontrados', 'status': False}), 200

