######
# Nesse arquivo é fornecido a Api das operacoes envolvendo o login e cadastro de usuarios do WMS
from flask import Blueprint, jsonify, request
from functools import wraps
from models.configuracoes import empresaConfigurada
from  models import UsuarioClassWms

usuarios_routes = Blueprint('usuarios', __name__) # Esse é o nome atribuido para o conjunto de rotas envolvendo usuario


# TOKEN FIXO PARA ACESSO AO CONTEUDO
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


# URL para Informar todos os usuarios cadastrados no WMS
@usuarios_routes.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    consulta = UsuarioClassWms.Usuario().getUsuarios()
    # Obtém os nomes das colunas
    column_names = consulta.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    consulta_data = []
    for index, row in consulta.iterrows():
        consulta_dict = {}
        for column_name in column_names:
            consulta_dict[column_name] = row[column_name]
        consulta_data.append(consulta_dict)
    return jsonify(consulta_data)

@usuarios_routes.route('/api/UsuarioSenhaRestricao', methods=['GET'])
@token_required
def get_usuariosRestricao():
    usuarios = UsuarioClassWms.Usuario().PesquisarSenha()

    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome ', 'senha']

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)

    return jsonify(usuarios_data)
@usuarios_routes.route('/api/Usuarios/<int:codigo>', methods=['POST'])
@token_required
def update_usuario(codigo):
    '''Api para atualizar informacoes do usuario'''

    data = request.get_json()
    codigo_ant, nome_ant, funcao_ant, situacao_ant , empresa_ant, perfil_ant, login_ant = UsuarioClassWms.Usuario(codigo).consultaUsuario()
    if 'funcao' in data:
        nova_funcao = data['funcao']
    else:
        nova_funcao = funcao_ant
    if 'nome' in data:
        nome_novo = data['nome']
    else:
        nome_novo = nome_ant
    if 'situacao' in data:
        situacao_novo = data['situacao']
    else:
        situacao_novo = situacao_ant

    if 'login' in data:
        login = data['login']
    else:
        login = login_ant

    if 'perfil' in data:
        perfil = data['perfil']
    else:
        perfil = perfil_ant

    UsuarioClassWms.Usuario(codigo,login,nome_novo,situacao_novo,nova_funcao,'',perfil).atualizarInformUsuario()

    return jsonify({'message': f'Dados do Usuário {codigo} - {nome_novo} atualizado com sucesso'})


@usuarios_routes.route('/api/Usuarios', methods=['PUT'])
@token_required
def criar_usuario():
    '''Api para inserir um novo usuario no WMS '''

    novo_usuario = request.get_json()
    # Extraia os valores dos campos do novo usuário
    codigo = novo_usuario.get('codigo')
    funcao = novo_usuario.get('funcao')
    nome = novo_usuario.get('nome')
    senha = novo_usuario.get('senha')
    situacao = novo_usuario.get('situacao','ATIVO')
    perfil = novo_usuario.get('perfil')
    login = novo_usuario.get('login',codigo)

    emp = empresaConfigurada.EmpresaEscolhida()
    empresa = novo_usuario.get('empresa',emp)

    # Instanciando o objeto usuario
    usuario = UsuarioClassWms.Usuario(codigo, login,nome,situacao,funcao,senha,perfil)

    a_codigo, b_nome, c_funcao, d_situacao, e_empresa, f_perfil, g_login = usuario.consultaUsuario()

    if a_codigo != 0:
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} ja existe'}), 201
    else:

        usuario.inserirUsuario()
        # Retorne uma resposta indicando o sucesso da operação
        return jsonify({'message': f'Novo usuário:{usuario.codigo}- {nome} criado com sucesso'}), 200


# Rota com parametros para check do Usuario e Senha
@usuarios_routes.route('/api/UsuarioSenha', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    usuario = UsuarioClassWms.Usuario(codigo,'','','','',senha)
    result = usuario.consultaUsuarioSenha()


    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        codigo, nome, funcao, situacao, empresa1, perfil, login = usuario.consultaUsuario()

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao,
                "empresa":empresa1,
                "perfil":perfil,
                "login":login
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
                        "message": f'Usuário ou senha não existe'}), 401

@usuarios_routes.route('/api/incluirPerfilUsuario', methods=['POST'])
@token_required
def post_incluirPerfilUsuario():

        # Obtenha os dados do corpo da requisição
        novo_Tela = request.get_json()
        # Extraia os valores dos campos do novo usuário
        codUsuario = novo_Tela.get('codUsuario')
        nomePerfil = novo_Tela.get('nomePerfil')

        consulta = UsuarioClassWms.Usuario(codUsuario).inserirPerfilUsuario(nomePerfil)
        # Obtém os nomes das colunas
        column_names = consulta.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = []
        for index, row in consulta.iterrows():
            consulta_dict = {}
            for column_name in column_names:
                consulta_dict[column_name] = row[column_name]
            consulta_data.append(consulta_dict)
        return jsonify(consulta_data)


@usuarios_routes.route('/api/ArrayincluirPerfilUsuario', methods=['POST'])
@token_required
def post_ArrayincluirPerfilUsuario():

        # Obtenha os dados do corpo da requisição
        novo_Tela = request.get_json()
        # Extraia os valores dos campos do novo usuário
        ArraycodUsuario = novo_Tela.get('ArraycodUsuario')
        ArraynomePerfil = novo_Tela.get('ArraynomePerfil')

        consulta = UsuarioClassWms.Usuario().inserirArrayPefilUsuario(ArraycodUsuario,ArraynomePerfil)
        # Obtém os nomes das colunas
        column_names = consulta.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = []
        for index, row in consulta.iterrows():
            consulta_dict = {}
            for column_name in column_names:
                consulta_dict[column_name] = row[column_name]
            consulta_data.append(consulta_dict)
        return jsonify(consulta_data)


@usuarios_routes.route('/api/rotasAutorizadasUsuarios', methods=['GET'])
@token_required
def get_rotasAutorizadasUsuarios():


        consulta = UsuarioClassWms.Usuario().rotasAutorizadasUsuarios()
        # Obtém os nomes das colunas
        column_names = consulta.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = []
        for index, row in consulta.iterrows():
            consulta_dict = {}
            for column_name in column_names:
                consulta_dict[column_name] = row[column_name]
            consulta_data.append(consulta_dict)
        return jsonify(consulta_data)


@usuarios_routes.route('/api/rotasAutorizadasPORUsuario', methods=['GET'])
@token_required
def get_rotasAutorizadasPORUsuario():
        # Obtém o código do usuário e a senha dos parâmetros da URL
        codigo = request.args.get('codigo')

        consulta = UsuarioClassWms.Usuario(codigo).rotasAutorizadasPORUsuario()
        # Obtém os nomes das colunas
        column_names = consulta.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        consulta_data = []
        for index, row in consulta.iterrows():
            consulta_dict = {}
            for column_name in column_names:
                consulta_dict[column_name] = row[column_name]
            consulta_data.append(consulta_dict)
        return jsonify(consulta_data)