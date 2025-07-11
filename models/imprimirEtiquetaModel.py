import cups
import os
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics import barcode
import qrcode
import math
import ConexaoPostgreMPL
import pandas as pd
from reportlab.lib.pagesizes import A4
from datetime import datetime


def criar_pdf(saida_pdf, titulo, cliente, pedido, transportadora, separador, agrupamento, prioridade, obs = ''):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))

    # Criar um arquivo temporário para salvar o QR code
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        c.setFont("Helvetica-Bold", 9)
        title = titulo
        c.drawString(0.3 * cm, 1.5 * cm, title)


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(cliente)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 5.2 * cm, 0.43 * cm, width=1.45 * cm, height= 1.30 * cm)

        #barcode_value = cliente  # Substitua pelo valor do código de barras desejado
        #barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, humanReadable=True)
        # Desenhar o código de barras diretamente no canvas
        #barcode_code128.drawOn(c, 3.0 * cm, 0.3 * cm)

        c.setFont("Helvetica", 9)
        c.drawString(0.3 * cm, 1.1 * cm, "Nº Cliente:")
        c.drawString(0.3 * cm, 0.8 * cm, "Nº Pedido:")
        c.setFont("Helvetica-Bold", 14)
        c.drawString(0.3 * cm, 0.47 * cm, obs)

        c.setFont("Helvetica-Bold", 8)
        c.drawString(0.3 * cm, 0.1 * cm, separador+' '+prioridade)

        c.drawString(2.0 * cm, 1.1 * cm, cliente)
        c.drawString(2.0 * cm, 0.8 * cm, pedido)
        c.setFont("Helvetica-Bold", 5)
        c.drawString(2.5 * cm, 0.1 * cm, agrupamento)

        c.save()

def imprimir_pdf(pdf_file):
    conn = cups.Connection()
    #printers = conn.getPrinters()
    #printer_name = list(printers.keys())[0]
    printer_name = "ZM400" # Aqui teremos que criar uma funcao para imprimir as etiquetas de cianorte
    job_id = conn.printFile(printer_name,pdf_file,"Etiqueta",{'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100','Orientation':'3'})
    print(f"ID {job_id} enviado para impressão")

def EtiquetaPrateleira(saida_pdf,endereco, rua,modulo,posicao, natureza):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        title = rua
        c.setFont("Helvetica-Bold", 23)
        c.drawString(0.3 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(0.5 * cm, 1.5 * cm, 'Rua.')

        title = modulo
        c.setFont("Helvetica-Bold", 23)
        c.drawString(1.8 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(1.7 * cm, 1.5 * cm, 'Quadra.')

        title = posicao
        c.setFont("Helvetica-Bold", 23)
        c.drawString(3.3 * cm, 0.75 * cm, title)

        c.setFont("Helvetica-Bold", 9)
        c.drawString(3.2 * cm, 1.5 * cm, 'Posicao.')

        c.setFont("Helvetica-Bold", 7)
        c.drawString(5.2 * cm, 0.15 * cm, 'Natureza:')

        c.setFont("Helvetica-Bold", 7)
        c.drawString(6.4 * cm, 0.15 * cm, natureza)


        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(endereco)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 5.2 * cm, 0.43 * cm, width=1.45 * cm, height= 1.30 * cm)

        c.setFont("Helvetica-Bold", 9)
        barcode_value = endereco  # Substitua pelo valor do código de barras desejado
        barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, barWidth=0.75 ,humanReadable=False)
        # Desenhar o código de barras diretamente no canvas
        barcode_code128.drawOn(c,0.07 * cm, 0.1 * cm)

        c.save()

def EtiquetaPrateleira2(c, endereco, rua, modulo, posicao, natureza, x, y):
    label_width = 7.5 * cm
    label_height = 1.8 * cm

    # Título centralizado
    c.setFont("Helvetica-Bold", 23)
    c.drawString(x + 0.3 * cm, y + 0.75 * cm, rua)

    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 0.5 * cm, y + 1.5 * cm, 'Rua.')

    c.setFont("Helvetica-Bold", 23)
    c.drawString(x + 1.8 * cm, y + 0.75 * cm, modulo)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 1.7 * cm, y + 1.5 * cm, 'Quadra.')

    c.setFont("Helvetica-Bold", 23)
    c.drawString(x + 3.3 * cm, y + 0.75 * cm, posicao)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 3.2 * cm, y + 1.5 * cm, 'Posicao.')

    c.setFont("Helvetica-Bold", 7)
    c.drawString(x + 5.2 * cm, y + 0.15 * cm, 'Natureza:')
    c.drawString(x + 6.4 * cm, y + 0.15 * cm, natureza)

    # QR Code temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(endereco)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(temp_qr_file.name)
        c.drawImage(temp_qr_file.name, x + 5.2 * cm, y + 0.43 * cm, width=1.45 * cm, height=1.30 * cm)
        os.unlink(temp_qr_file.name)

    # Código de barras
    barcode_value = endereco
    barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, barWidth=0.75, humanReadable=False)
    barcode_code128.drawOn(c, x + 0.07 * cm, y + 0.1 * cm)

def gerar_etiquetas_pdf(saida_pdf, lista_etiquetas):
    c = canvas.Canvas(saida_pdf, pagesize=A4)
    page_width, page_height = A4

    label_height = 1.8 * cm
    margem_topo = 1 * cm
    margem_lateral = 1 * cm
    espaco_vertical = 0.5 * cm

    y = page_height - label_height - margem_topo

    for etiqueta in lista_etiquetas:
        endereco, rua, modulo, posicao, natureza = etiqueta
        EtiquetaPrateleira2(c, endereco, rua, modulo, posicao, natureza, x=margem_lateral, y=y)
        y -= (label_height + espaco_vertical)

        if y < margem_topo:
            c.showPage()
            y = page_height - label_height - margem_topo

    c.save()


def gerar_pdf_e_retornar_url(lista_etiquetas):
    nome_arquivo = f"etiquetas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    caminho_static = "/home/grupompl/Wms_InternoMPL/static"
    caminho_pdf = os.path.join(caminho_static, nome_arquivo)

    # Gera o PDF no caminho certo
    gerar_etiquetas_pdf(caminho_pdf, lista_etiquetas)

    # Retorna a URL para o front-end abrir
    url_pdf = f"http://10.162.0.191:5000/static/{nome_arquivo}"
    return url_pdf


def ImprimirSeqCaixa(saida_pdf,codigo1, codigo2 ='0', codigo3='0'):
    # Configurações das etiquetas e colunas
    label_width = 7.5 * cm
    label_height = 1.8 * cm
    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)


        #qrcode 1
        qr = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
        qr.add_data(codigo1)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 0.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

        c.setFont("Helvetica-Bold", 5)
        c.drawString(0.3 * cm, 0.2 * cm, 'NºCx:')

        c.setFont("Helvetica-Bold", 5)
        c.drawString(0.9 * cm, 0.2 * cm, '' + codigo1)

        # qrcode 2:

        if codigo2 == '0' :
            print('sem seq')
        else:
            with tempfile.NamedTemporaryFile(delete=False, suffix="2.png") as temp_qr_file2:
                qr_filename2 = temp_qr_file2.name

                qr2 = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr2.add_data(codigo2)
                qr2.make(fit=True)
                qr_img2 = qr2.make_image(fill_color="black", back_color="white")
                qr_img2.save(qr_filename2)
                c.drawImage(qr_filename2, 2.8 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

                c.setFont("Helvetica-Bold", 5)
                c.drawString(2.8 * cm, 0.2 * cm, 'NºCx:')

                c.setFont("Helvetica-Bold", 5)
                c.drawString(3.4 * cm, 0.2 * cm, '' + codigo2)

        if codigo3 == '0' :
            print('sem seq')
        else:
            with tempfile.NamedTemporaryFile(delete=False, suffix="3.png") as temp_qr_file3:
                qr_filename3 = temp_qr_file3.name

                qr3 = qrcode.QRCode(version=1, box_size=int(1.72 * cm), border=0)
                qr3.add_data(codigo3)
                qr3.make(fit=True)
                qr_img3 = qr3.make_image(fill_color="black", back_color="white")
                qr_img3.save(qr_filename3)
                c.drawImage(qr_filename3, 5.3 * cm, 0.43 * cm, width=1.45 * cm, height=1.30 * cm)

                c.setFont("Helvetica-Bold", 5)
                c.drawString(5.3 * cm, 0.2 * cm, 'NºCx:')

                c.setFont("Helvetica-Bold", 5)
                c.drawString(5.8 * cm, 0.2 * cm, '' + codigo3)

        c.save()
def QuantidadeImprimir(quantidade, usuario = '', salvaEtiqueta = False):
    quantidade = int(quantidade)
    n_impressoes = math.ceil(quantidade / 3)

    conn = ConexaoPostgreMPL.conexao()
    inicial = pd.read_sql('select sc.codigo::INTEGER from "off".seq_caixa sc order by codigo desc ',conn)

    inicial = inicial['codigo'][0]
    inicial2 = int(inicial)

    for i in range(n_impressoes):
        codigo1 = inicial2+1
        codigo2=inicial2+2
        codigo3 = inicial2+3
        inicial2 = codigo3

        codigo1 = ''+str(codigo1)
        codigo2 = ''+str(codigo2)
        codigo3 = ''+str(codigo3)

        if salvaEtiqueta == False:
            nometeste = 'caixa_'+str('')+".pdf"
        else:
            nometeste = 'caixa_' + str(i) + ".pdf"

        ImprimirSeqCaixa(nometeste,codigo1,codigo2,codigo3)

        insert = 'insert into "off".seq_caixa (codigo, usuario) values ( %s, %s )'
        cursor = conn.cursor()
        cursor.execute(insert,(codigo1,usuario,))
        conn.commit()

        cursor.execute(insert,(codigo2,usuario,))
        conn.commit()
        cursor.execute(insert,(codigo3,usuario,))
        conn.commit()

        cursor.close()
        imprimir_pdf(nometeste)

    conn.close()


