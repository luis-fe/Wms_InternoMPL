import models.G
from flask import Blueprint, jsonify, request
from functools import wraps

CarrinhoOFF_routes = Blueprint('CarrinhoOFF_routes', __name__)


def token_required(f):
    # TOKEN FIXO PARA ACESSO AO CONTEUDO
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@CarrinhoOFF_routes.route('/api/consultarCarrinhos', methods=['GET'])
@token_required
def get_consultarCarrinhos():
    # Obtém os dados do corpo da requisição (JSON)

    empresa = request.args.get('empresa','1')

    consulta = CarrinhoOFF.Carrinho('',empresa).consultarCarrinhos()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


@CarrinhoOFF_routes.route('/api/imprimirCodCarrinho', methods=['POST'])
@token_required
def post_imprimirCodCarrinho():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    NCarrinho = datas['NCarrinho']
    empresa = datas['empresa']

    pdf = CarrinhoOFF.Carrinho(NCarrinho,empresa)
    pdf.gerarEtiquetaCarrinho(f'carrinho.pdf')
    pdf.imprimir_pdfCarrinho(f'carrinho.pdf')

    return jsonify({'message': f'Imprimido o carrinho  com sucesso', 'status':True})


@CarrinhoOFF_routes.route('/api/NovoCarrinho', methods=['POST'])
@token_required
def post_NovoCarrinho():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    NCarrinho = datas['NCarrinho']
    empresa = datas['empresa']

    carrinho = CarrinhoOFF.Carrinho(NCarrinho,empresa)

    consulta = carrinho.cadastrarCarrinho()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


@CarrinhoOFF_routes.route('/api/ExcluirCarrinho', methods=['DELETE'])
@token_required
def post_ExcluirCarrinhoo():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    NCarrinho = datas['NCarrinho']
    empresa = datas['empresa']

    carrinho = CarrinhoOFF.Carrinho(NCarrinho,empresa)

    consulta = carrinho.excluirCarrinho()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in consulta.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


