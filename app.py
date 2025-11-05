'''
####     ESSE É O  Arquivo Principal do sistema, onde é o primeiro processo a ser executado.
##### O WMS utiliza o frameWork Flask para controlar e disponibilizar serviços de Api's para o seu fronEnd.
'''
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import os
from functools import wraps # Pacote que ajuda a criar o token das Api's
from models.Dashboards import  Relatorios, ReposicaoSku
from dotenv import load_dotenv, dotenv_values

from models.configuracoes import empresaConfigurada
from routes import routes_blueprint

app = Flask(__name__) ## Aqui é criado essa funcao para iniciar o Projeto
load_dotenv('/home/grupompl/WMS_Teste/ambiente.env')

PORTA_APLICATION = os.getenv('PORTA_APLICATION')
print(PORTA_APLICATION)
port = int(os.environ.get('PORT', PORTA_APLICATION)) # A porta escolhida para rodar a Aplicacao é a 5000.

#Aqui registo todas as rotas , url's DO PROJETO, para acessar bastar ir na pasta "routes",
#duvidas o contato (62)99351-42-49 ou acessar a documentacao do projeto em:
app.register_blueprint(routes_blueprint)

CORS(app) # O Cors é uma funcao que permite ao FrontEnd realizar solicitacoes de API, com seguranca ausentando da necessidade do certificado
# Https e sim do Http.

# Decorator para verificar o token fixo: Aqui é cadastrado o token fixo para utilizar as apis
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@app.route('/api/ApontarTagReduzido', methods=['POST'])
@token_required
def get_ApontarTagReduzido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()

    codusuario = datas['codUsuario']
    dataHora = datas['dataHora']
    endereco = datas['endereço']
    codbarra = datas['codbarras']
    Prosseguir = datas.get('Prosseguir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo
    natureza = datas['natureza','5']
    empresa = datas['empresa','1']


    Endereco_det = ReposicaoSku.ApontarTagReduzido(codbarra, endereco, codusuario, 'dataHora', Prosseguir, natureza, empresa)

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


@app.route('/api/RelatorioEndereços', methods=['GET'])
def get_RelatorioEndereços():
    # Obtém os dados do corpo da requisição (JSON)

    Endereco_det = Relatorios.relatorioEndereços()

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

@app.route('/api/RelatorioFila', methods=['GET'])
def get_RelatorioFila():
    # Obtém os dados do corpo da requisição (JSON)
    Endereco_det = Relatorios.relatorioFila()

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

# Defina o diretório onde as imagens serão armazenadas
UPLOAD_FOLDER = 'imagens_chamado'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/api/get_image/<string:idchamado>', methods=['GET'])
def get_image(idchamado):
    directory = f'imagens_chamado/{idchamado}'

    # Verifique se o diretório existe
    if os.path.exists(directory):
        # Listar os arquivos dentro do diretório
        files = os.listdir(directory)

        # Verificar se há arquivos no diretório
        if files:
            # Escolher o primeiro arquivo da lista (você pode ajustar a lógica de escolha conforme necessário)
            filename = files[0]

            # Servir o arquivo escolhido
            return send_from_directory(directory, filename)

    # Se o diretório não existe ou não contém arequivos, retornar uma resposta adequada
    return jsonify({"Mensagem":"Arquivo não encontrado"}), 404


### Aqui iniciando o main do projeto WMS:

if __name__ == '__main__':
    api_key = os.getenv('CAMINHO')  # Troque por 'API_KEY' ou outro nome se necessário

    print(api_key)  # Exibe o valor da API Key
    app.run(host='0.0.0.0', port=port) # A porta foi atribuida na variavel port
