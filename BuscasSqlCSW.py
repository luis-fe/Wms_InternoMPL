######## ARQUIVO.py UTILIZADO PARA CATALOGAR OS CODIGOS SQL DE BUSCA NO CSW: ##########################

#Elaborado por : Luis Fernando Gonçalves de Lima Machado




#1 - SQL BUSCANDO AS ORDEM DE PRODUCAO EM ABERTO - velocidade media da consulta : 0,850 s (otima)

def OP_Aberto():


    OP_emAberto = 'SELECT (select pri.descricao  FROM tcp.PrioridadeOP pri WHERE pri.Empresa = 1 and o.codPrioridadeOP = pri.codPrioridadeOP ) as prioridade, dataInicio as startOP, codProduto  , numeroOP , codTipoOP , codFaseAtual as codFase , codSeqRoteiroAtual as seqAtual, ' \
                  'codPrioridadeOP, codLote , codEmpresa, (SELECT f.nome from tcp.FasesProducao f WHERE f.codempresa = 1 and f.codfase = o.codFaseAtual) as nomeFase, ' \
                  '(select e.descricao from tcp.Engenharia e WHERE e.codempresa = o.codEmpresa and e.codengenharia = o.codProduto) as descricao' \
                  ' FROM tco.OrdemProd o '\
                   ' WHERE o.codEmpresa = 1 and o.situacao = 3'

    return OP_emAberto



#2- SQL BUSCANDO AS " DATA/HORA DE MOVIMENTACAO DAS ORDEM DE PRODUCAO EM ABERTO " - velocidade media: 4,500 s (regular)

def DataMov(AREA):

    if AREA == 'PRODUCAO':
        DataMov = 'SELECT numeroOP, dataMov as data_entrada , horaMov , seqRoteiro, (seqRoteiro + 1) as seqAtual FROM tco.MovimentacaoOPFase mf '\
            ' WHERE  numeroOP in (SELECT o.numeroOP from  tco.OrdemProd o' \
            ' having o.codEmpresa = 1 and o.situacao = 3 and o.codtipoop <> 13) and mf.codempresa = 1 order by codlote desc'
    else:
        DataMov = 'SELECT numeroOP, dataMov as data_entrada , horaMov , seqRoteiro, (seqRoteiro + 1) as seqAtual FROM tco.MovimentacaoOPFase mf '\
            ' WHERE  numeroOP in (SELECT o.numeroOP from  tco.OrdemProd o' \
            ' having o.codEmpresa = 1 and o.situacao = 3 and o.codtipoop = 13) and mf.codempresa = 1 order by codlote desc'

    return DataMov

#3- SQL BUSCAR OS TIPO's DE OP DO CSW
def TipoOP():

    TipoOP = 'SELECT t.codTipo as codTipoOP, t.nome as nomeTipoOp  FROM tcp.TipoOP t WHERE t.Empresa = 1'

    return TipoOP


#4- Sql Buscando Pedidos Bloqueados NO CREDITO tempo 0,100 ms (otimo)
def BloqueiosCredito():

    BloqueiosCredito = "SELECT codPedido, 'BqCredito' as situacao  FROM Cre.PedidoCreditoBloq WHERE Empresa = 1 and situacao = 1 "

    return BloqueiosCredito

#5- Sql Buscando Pedidos Bloqueados NO COMERCIAL tempo 0,050 ms (otimo)
def bloqueioComerical():
    bloqueioComerical = 'SELECT codPedido, situacaoBloq as situacao from ped.PedidoBloqComl c WHERE codEmpresa = 1 and situacaoBloq = 1 '

    return bloqueioComerical

#6- SQL CAPA DOS PEDIDOS: Velocidade media : 1,5 s (ótimo - para o intervalo de 1 ano de pedidos)
def CapaPedido (iniVenda, finalVenda, tiponota):

    CapaPedido = "SELECT dataEmissao, codPedido, "\
    "(select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli, "\
    " codTipoNota, dataPrevFat, codCliente, codRepresentante, descricaoCondVenda, vlrPedido as vlrSaldo,qtdPecasFaturadas "\
    " FROM Ped.Pedido p"\
    " where codEmpresa = 1 and  dataEmissao >= '" + iniVenda + "' and dataEmissao <= '" + finalVenda + "' and codTipoNota in (" + tiponota + ")"\
    " order by codPedido desc "

    return CapaPedido


#7- SQL DE PEDIDOS NO NIVEL SKU - Velocidade Media 5 s para dados de 1 ano (regular)
def pedidosNivelSKU (iniVenda, finalVenda, tiponota):
    pedidosNivelSKU = 'select codPedido, codProduto as reduzido, qtdeCancelada, qtdeFaturada, qtdePedida '\
                        'from ped.PedidoItemGrade  p where codEmpresa = 1 and p.codPedido in '\
                        "(select p.codPedido FROM Ped.Pedido p where codEmpresa = 1 and dataEmissao >= '" + iniVenda + "' and dataEmissao <= '" + finalVenda + ")"

    return pedidosNivelSKU
#8- SQL DE BUSCA DE TERCEIRIZADOS POR OP E FASE - Velocidade Média: 0,700 s

def OPporTecerceirizado():
    OpTercerizados = 'SELECT CONVERT(VARCHAR(10), R.codOP) AS numeroOP, R.codFase as codFase, R.codFac,'\
  ' (SELECT nome  FROM tcg.Faccionista  f WHERE f.empresa = 1 and f.codfaccionista = r.codfac) as nome'\
 ' FROM TCT.RemessaOPsDistribuicao R'\
' INNER JOIN tco.OrdemProd op on'\
    ' op.codempresa = r.empresa and op.numeroop = CONVERT(VARCHAR(10), R.codOP)'\
    ' WHERE R.Empresa = 1 and op.situacao = 3 and r.situac = 2'

    return OpTercerizados

#9- SQL DEPARA DA ENGENHARIA PAI X FILHO: velocidade Média : 0,20 segundos

def DeParaFilhoPaiCategoria():

    dePara = "SELECT e.codEngenharia as codProduto,"\
     " (SELECT ep.descricao from tcp.Engenharia ep WHERE ep.codempresa = 1 and ep.codengenharia like '%-0' and '01'||SUBSTRING(e.codEngenharia, 3,9) = ep.codEngenharia) as descricaoPai"\
" FROM tcp.Engenharia e"\
" WHERE e.codEmpresa = 1 and e.codEngenharia like '6%' and e.codEngenharia like '%-0' and e.codEngenharia not like '65%'"

    return dePara

#10- SQL DE BUSCA DAS REQUISICOES DAS OPS : velocidade Média : 1,20 segundos

def RequisicoesOPs():

    requisicoes = ' SELECT numero,numOPConfec as numeroOP ,  seqRoteiro as fase, sitBaixa, codNatEstoque  ' \
                  ' FROM tcq.Requisicao r WHERE r.codEmpresa = 1 and ' \
                  ' r.numOPConfec in (SELECT op.numeroop from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3)'

    return requisicoes

#11- SQL DE BUSCA DAS PARTES DAS OPS : velocidade Média : 0,35 segundos (OTIMO)

def LocalizarPartesOP():

    partes = "SELECT p.codlote as numero, codopconjunto as numeroOP , '425' as fase, op.situacao as sitBaixa, codOPParte as codNatEstoque,"\
             " (SELECT e.descricao from tcp.Engenharia e WHERE e.codempresa = 1 and e.codengenharia = op.codProduto) as nomeParte"\
             " FROM tco.RelacaoOPsConjuntoPartes p"\
             " inner join tco.OrdemProd op on op.codEmpresa = p.Empresa and op.numeroOP = p.codOPParte "\
             " WHERE codopconjunto in (SELECT op.numeroop from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3 and op.codfaseatual = 426 )"

    return partes

#12- SQL DE BUSCA DAs cores : velocidade Média : 0,07 segundos (OTIMO)
def CoresVariantesCSW():
    cores = "SELECT codigoCor as sortimentosCores , descricao  FROM Ppcpt_Gen_Ttg.TabGenClasseCor c "\
                        "WHERE c.codEmpresa = 1 "
    return cores

#13- SQL DE BUSCA DAs transportadoras cadastras no csw : velocidade Média : 0,200 segundos (OTIMO)
def tranportadora(empresa):
    tranportadora = 'SELECT  t.cidade , t.siglaEstado as estado, f.fantasia as transportadora  FROM Asgo_Trb.TransPreferencia t'\
        ' join cad.Transportador  f on  f.codigo  = t.Transportador  '\
        ' WHERE t.Empresa = '+empresa

    return tranportadora
#14- SQL DE BUSCA DAS TAG'S DISPONIVEIS PARA A PRODUCAO : velocidade Média : 31,4 segundos (lenta)
def TagDisponiveis(emp):

    tagsDisponivel = 'SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'\
                    ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'\
    " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"\
    ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'\
    ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = ' + emp + ' and '\
    ' p.numeroOP in ( SELECT numeroOP  FROM tco.OrdemProd o WHERE codEmpresa = ' + emp + ' and codFaseAtual in (210, 320, 56, 432, 441, 452, 423, 433, 452, 437 ) and situacao = 3) '

    return tagsDisponivel
#15- SQL DE BUSCA DAS MOVIMENTACOES ENTRE DATAS , TESTE COM 1 ANO : velocidade 9 segundos (REGULAR)
def MovimentacoesOps():
        dados = 'SELECT codFase, mf.numeroOP, dataMov as data_entrada, horaMov, mf.seqRoteiro, (mf.seqRoteiro + 1) as seqAtual FROM'\
                ' tco.MovimentacaoOPFase mf'\
                ' WHERE mf.codempresa = 1 and dataBaixa <= CURRENT_TIMESTAMP AND  '\
                " dataBaixa > DATEADD('day', -365, CURRENT_TIMESTAMP)"
        return dados

#16- SQL DE BUSCA DAS MOVIMENTACOES ENTRE DATAS no dia : velocidade 0,33 segundos (otimo)
def MovimentacoesOpsNodia():
        dados = 'SELECT codFase, mf.numeroOP, dataMov as data_entrada, horaMov, mf.seqRoteiro, (mf.seqRoteiro + 1) as seqAtual FROM'\
                ' tco.MovimentacaoOPFase mf'\
                ' WHERE mf.codempresa = 1 and dataBaixa <= CURRENT_TIMESTAMP AND  '\
                " dataBaixa > DATEADD('day', -1, CURRENT_TIMESTAMP)"
        return dados
#17- SQL DE BUSCA DAS QUALIDADES DAS TAGS ENTRE DATAS no dia : velocidade 2,00 segundos (REGULAR)
def TagsSegundaQualidadePeriodo(datainicial, datafinal):

        detalhado =   f"""
        SELECT 
            codBarrasTag , 
            codReduzido , 
            codNaturezaAtual , 
            numeroOP , 
            CONVERT(VARCHAR(6), numeroOP) as OPpai,
            motivo2Qualidade  
        FROM 
            tcr.TagBarrasProduto t
        WHERE 
            t.codEmpresa = 1 
            and t.numeroOP in
                (
                SELECT 
                    op.numeroop 
                from 
                    tco.MovimentacaoOPFase op 
                WHERE 
                    op.codempresa = 1 and op.codfase in (429, 441, 449)
                    and op.datamov >= '{datainicial}' 
                    and op.datamov <= '{datafinal}' 
                ) 
        and motivo2Qualidade > 0 and situacao <> 1
        """

        return detalhado

#18- SQL DE BUSCA DAo cadastro de motivos : velocidade 0,09 segundos (otimo)

def Motivos():
    motivos = """
    SELECT 
        codMotivo as motivo2Qualidade , 
        nome, 
        codOrigem,
        (SELECT o.nome from tcp.OrgSegQualidade o WHERE o.empresa = 1 and o.codorigem = m.codorigem) as nomeOrigem
    FROM 
        tcp.Mot2Qualidade m 
    WHERE 
        m.Empresa = 1 
        """

    return motivos

#19- Sql Obter as OPs Baixadas no Periodo: velocidade 0,70 segundos (otimo)

def OpsBaixadas(datainicial, datafinal):
    opsBaixadas = """
            SELECT 
                M.dataLcto , 
                m.numDocto, 
                m.qtdMovto, 
                codNatureza1, 
                m.codItem 
            FROM est.Movimento m
            WHERE 
                codEmpresa = 1 and m.dataLcto >= '"""+ datainicial +"""'and m.dataLcto <= '"""+datafinal+"""'
                and operacao1 = '+' and numDocto like 'OP%'
                AND codNatureza1 IN (5,7)
                """

    return opsBaixadas

#20- Sql Obter as OPs Baixadas por faccionista no Periodo: velocidade 1,70 segundos (otimo)

def OpsBaixadasFaccionista(datainicial, datafinal):
    opBaixadas = f"""  
                SELECT 
                    CONVERT(VARCHAR(10), R.codOP) AS numeroOP2, 
                    CONVERT(VARCHAR(6), R.codOP) AS OPpai, 
                    R.codFase as codFase, 
                    R.codFac,
                    (SELECT fase.nome FROM tcp.FasesProducao fase WHERE fase.codempresa = 1 and fase.codfase = R.codFase) as nomeFase,
                    (SELECT nome  FROM tcg.Faccionista  f WHERE f.empresa = 1 and f.codfaccionista = r.codfac) as nomeFaccicionista
                FROM 
                    TCT.RemessaOPsDistribuicao R
                INNER JOIN tco.OrdemProd op on
                    op.codempresa = r.empresa and op.numeroop = CONVERT(VARCHAR(10), R.codOP)
                WHERE 
                    R.Empresa = 1 
                    and r.situac = 2 
                    and CONVERT(VARCHAR(6), R.codOP) in
                    (
                        SELECT 
                            SUBSTRING(m.numDocto, 11,6) 
                        FROM 
                            est.Movimento m      
                        WHERE 
                            codEmpresa = 1 
                            and m.dataLcto >= DATEADD('{datainicial}', -40, CURRENT_TIMESTAMP)"
                            and m.dataLcto <= DATEADD('{datafinal}', +20, CURRENT_TIMESTAMP)"
                            and operacao1 = '+' and numDocto like 'OP%'
                            AND codNatureza1 IN (5,7)
                    )  
                    and tiprem = 1 
                    and r.codfase = 429
                """

    return opBaixadas

#21- Sql Obter os itens substitutos dos ultimos 100 dias a nivel de op : velocidade 2,50 segundos (otimo)

def RegistroSubstituto():
    registro = "SELECT s.codRequisicao , r.numOPConfec as numeroOP ," \
               " (SELECT op.codProduto from tco.OrdemProd op WHERE op.codempresa = 1 and op.numeroop = r.numOPConfec ) as codProduto," \
               " r.dtBaixa, s.codItemPrincipal, ri.nomeMaterial, s.codMaterial as subst,"\
                " (select ri2.nomeMaterial from tcq.RequisicaoItem ri2 where s.codEmpresa = ri2.codEmpresa and s.codRequisicao = ri2.codRequisicao  and ri2.codMaterial = s.codMaterial)"\
                " as nomeSub"\
                " FROM TCQ.Requisicao R"\
                " inner join tcq.RequisicaoItemSubst s on s.codEmpresa = r.codEmpresa and s.codRequisicao = r.numero"\
                " left join tcq.RequisicaoItem ri on s.codEmpresa = ri.codEmpresa and s.codRequisicao = ri.codRequisicao  and ri.codMaterial = s.codItemPrincipal "\
                " WHERE R.codEmpresa = 1 and r.dtBaixa  > DATEADD('day', -100, CURRENT_TIMESTAMP)"
    return registro

#22- Sql Obter o compontente de cadas sku nas engenharias , relativo as 10Mil primeiras OP : velocidade 36 segundos (lento)

def ComponentesPrincipaisEngenharia():
    consulta = 'SELECT c.CodComponente , c.codSortimento, c.codProduto  FROM tcp.ComponentesVariaveis c' \
     'WHERE c.codEmpresa = 1 and c.codProduto in ('\
' SELECT top 10000 op.codproduto from tco.OrdemProd op WHERE op.codempresa = 1 '\
  ' order by numeroOP desc) and c.CodComponente in ('\
                ' SELECT s.codItemPrincipal from tcq.RequisicaoItemSubst s WHERE s.codempresa = 1'\
                " ) and c.codproduto like '01%'"
    return consulta

#23 - Sql Busca tags de uma determinadaOP

def SqlBuscaTags(emp, codbarras):
    consulta = 'SELECT p.codBarrasTag as codbarrastag , p.codReduzido as codreduzido, p.codEngenharia as engenharia,'\
    ' (select i.nome from cgi.Item i WHERE i.codigo = p.codReduzido) as descricao, situacao, codNaturezaAtual as natureza, codEmpresa as codempresa,'\
    " (select s.corbase||'-'||s.nomecorbase  from tcp.SortimentosProduto s WHERE s.codempresa = 1 and s.codproduto = p.codEngenharia and s.codsortimento = p.codSortimento)"\
    ' as cor, (select t.descricao from tcp.Tamanhos t WHERE t.codempresa = 1 and t.sequencia = p.seqTamanho ) as tamanho, p.numeroOP as numeroop'\
    ' from Tcr.TagBarrasProduto p WHERE p.codEmpresa = ' + emp + ' and situacao in (0, 9) and codbarrastag = '+codbarras

    return consulta


#24 - Sql Busca das tag indenizadas velocidade : 0,59s (boa)

def TagsIndenizadas():
    consulta = 'SELECT top 100000 i.codOP as numeroOP , i.codFornecedor as fornecedorIdenizado , i.vlrUnitario, it.codBarras  FROM tct.Indenizacoes i'\
' left JOIN TCT.IndenizacoesTags IT ON IT.codEmpresa = I.codEmpresa AND I.codOP = IT.codOP '\
' WHERE i.codEmpresa = 1 '\
' order by i.codOP desc '

    return consulta

#25 Sql buscar movimentacao de fases especificas entre datas -  velocidade: 1,6s

def MovFase(arrayFases, datainicio, dataFim):

    consulta = 'SELECT m.numeroOP , m.codfase, datamov ' \
                ' FROM tco.MovimentacaoOPFase m WHERE m.codempresa = 1' \
               ' AND m.codFase IN ('+arrayFases+') ' \
               " AND datamov BETWEEN '"+datainicio+"' AND "+"'"+dataFim+"'"


    return consulta

def OPsEstampariaFilhas():

    consulta = 'SELECT case when dados.OPpai is null then dados.numeroOP else dados.OPpai end OPpai, dados.codfase  FROM ( '\
      ' SELECT m.numeroOP, m.codfase, datamov,'\
        ' (SELECT p.codOPConjunto  FROM  tco.RelacaoOPsConjuntoPartes p WHERE p.empresa = 1 and p.codOPParte = m.numeroop  ) as OPpai'\
        ' FROM tco.MovimentacaoOPFase m WHERE m.codempresa = 1 '\
' AND m.codFase IN (74, 435) '\
" AND datamov BETWEEN '2024-01-01' AND '2024-03-27') as dados"

    return consulta


def OrigensCsw():

    consulta = """
    SELECT DISTINCT nome FROM tcp.OrgSegQualidade o
WHERE o.Empresa = 1
    """

    return consulta

