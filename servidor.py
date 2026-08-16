#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Servidor 24h do Double Blaze IA.
- Usa apenas biblioteca padrão do Python.
- Não executa apostas.
- Monitora uma fonte JSON opcional, analisa resultados e publica sinais.
- Pode enviar notificação por ntfy quando NTFY_TOPIC estiver configurado.

Variáveis de ambiente:
PORTA=8787
RESULTADOS_URL=https://exemplo.com/resultados.json
INTERVALO_SEGUNDOS=10
NTFY_SERVER=https://ntfy.sh
NTFY_TOPIC=seu_topico_privado
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode, urljoin
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import os
import threading
import time
import re
from html import unescape

BASE = Path(__file__).resolve().parent
BANCO = BASE / "banco_servidor.json"
CONFIG = BASE / "configuracao_servidor.json"

LOCK = threading.Lock()
INICIO_SERVIDOR_EPOCH = time.time()

ESTADO = {
    "rodadas": [],
    "ultima_atualizacao": "",
    "ultimo_sinal": {
        "valido": False
    },
    "ultimo_id_feed": "",
    "fonte_online": False,
    "ultima_consulta_fonte": "",
    "ultima_rodada_fonte": "",
    "ultimo_erro_fonte": "",
    "total_importadas": 0,
    "historico_sinais": [],
    "ultima_notificacao_epoch": 0.0
}

CONFIG_PADRAO = {
    "sinal_minimo": 0.60,
    "amostras_minimas": 20,
    "modo_adaptativo": True,
    "limites_testados": [0.50, 0.55, 0.60, 0.65, 0.70, 0.75],
    "amostras_testadas": [10, 20, 30, 50, 75, 100],
    "janela_recente": 100,
    "janela_longa": 1000,
    "resultados_url": "",
    "modo_fonte": "json",
    "intervalo_segundos": 10,
    "ntfy_server": "https://ntfy.sh",
    "ntfy_topic": "",
    "geracao_automatica": True,
    "intervalo_notificacao_minutos": 5,
    "concordancia_minima": 2,
    "estabilidade_minima": 50.0
}



def chave_acesso_configurada():
    return os.getenv("DOUBLE_IA_API_KEY", "").strip()


def chave_autorizada(headers):
    chave_esperada = chave_acesso_configurada()

    if not chave_esperada:
        return True

    recebida = str(headers.get("X-Double-IA-Key", "")).strip()
    return recebida == chave_esperada


def registrar_sinal(sinal):
    with LOCK:
        historico = ESTADO.setdefault("historico_sinais", [])
        historico.append(dict(sinal))

        if len(historico) > 1000:
            del historico[:-1000]

        salvar_json(BANCO, ESTADO)


def pode_notificar_agora():
    cfg = carregar_config()
    minutos = int(cfg.get("intervalo_notificacao_minutos", 5))

    if minutos <= 0:
        return True

    agora = time.time()

    with LOCK:
        ultima = float(ESTADO.get("ultima_notificacao_epoch", 0.0))

    return agora - ultima >= minutos * 60


def registrar_notificacao_enviada():
    with LOCK:
        ESTADO["ultima_notificacao_epoch"] = time.time()
        salvar_json(BANCO, ESTADO)


def agora_brasilia():
    tz = timezone(timedelta(hours=-3))
    return datetime.now(tz).strftime("%d/%m/%Y %H:%M:%S")


def carregar_json(path, default):
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                obj = json.load(f)
            return obj
    except Exception:
        pass
    return default


def salvar_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def carregar_estado():
    global ESTADO
    data = carregar_json(BANCO, ESTADO)
    if isinstance(data, dict):
        ESTADO = data


def carregar_config():
    cfg = carregar_json(CONFIG, CONFIG_PADRAO)
    if not isinstance(cfg, dict):
        return dict(CONFIG_PADRAO)
    merged = dict(CONFIG_PADRAO)
    merged.update(cfg)
    return merged


def normalizar_cor(valor):
    s = str(valor).strip().lower()

    if s in ("r", "red", "vermelho"):
        return "R"
    if s in ("b", "black", "preto"):
        return "B"
    if s in ("w", "white", "branco"):
        return "W"

    try:
        n = int(s)
        if n == 0:
            return "W"
        if 1 <= n <= 7:
            return "R"
        if 8 <= n <= 14:
            return "B"
    except Exception:
        pass

    return ""



def html_para_texto(html):
    texto = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    texto = re.sub(r"(?is)<style.*?>.*?</style>", " ", texto)
    texto = re.sub(r"(?s)<[^>]+>", " ", texto)
    texto = unescape(texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def extrair_bestblaze_html(html):
    """
    Extrai pares numero + data/hora do histórico público do BestBlaze.
    O parser não depende de classes CSS específicas; usa o conteúdo textual.
    """
    texto = html_para_texto(html)

    # Número do Double seguido de data/hora.
    padrao = re.compile(
        r"(?<!\d)(0|[1-9]|1[0-4])\s+"
        r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"
    )

    rodadas = []
    vistos = set()

    for numero_texto, data_hora in padrao.findall(texto):
        numero = int(numero_texto)
        identificador = "%s-%02d" % (
            data_hora.replace("/", "").replace(" ", "-").replace(":", ""),
            numero
        )

        if identificador in vistos:
            continue

        vistos.add(identificador)
        rodadas.append({
            "id": identificador,
            "numero": numero,
            "data_hora": data_hora
        })

    # Ordena cronologicamente para preservar sequência.
    def chave(item):
        try:
            return datetime.strptime(item["data_hora"], "%d/%m/%Y %H:%M:%S")
        except Exception:
            return datetime.min

    rodadas.sort(key=chave)
    return rodadas



def extrair_bestblaze_brancos_html(html):
    """
    Extrai os brancos da página pública /doubleBrancosDia.

    Nessa página, cada timestamp listado representa diretamente uma rodada
    branca. O número é armazenado como 0 apenas como metadado.
    """
    texto = html_para_texto(html)
    datas = re.findall(
        r"\b\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\b",
        texto
    )

    rodadas = []
    vistos = set()

    for data_hora in datas:
        try:
            momento = datetime.strptime(data_hora, "%d/%m/%Y %H:%M:%S")
        except Exception:
            continue

        identificador = "%s-%02d" % (
            momento.strftime("%Y%m%d-%H%M%S"),
            0
        )

        if identificador in vistos:
            continue

        vistos.add(identificador)
        rodadas.append({
            "id": identificador,
            "numero": 0,
            "cor": "W",
            "data_hora": data_hora,
            "origem": "bestblaze_brancos"
        })

    rodadas.sort(
        key=lambda item: datetime.strptime(
            item["data_hora"],
            "%d/%m/%Y %H:%M:%S"
        )
    )
    return rodadas


def mesclar_rodadas_por_horario(*listas):
    """
    Mescla listas de rodadas sem duplicar o mesmo timestamp.
    Se um timestamp constar na página de brancos, o branco tem prioridade.
    """
    mapa = {}

    for lista in listas:
        for item in lista or []:
            if not isinstance(item, dict):
                continue

            horario = str(item.get("data_hora", "")).strip()
            if not horario:
                continue

            anterior = mapa.get(horario)

            # Branco confirmado pela página específica tem prioridade.
            if anterior is None or str(item.get("cor", "")) == "W":
                mapa[horario] = item

    def chave(item):
        try:
            return datetime.strptime(
                str(item.get("data_hora", "")),
                "%d/%m/%Y %H:%M:%S"
            )
        except Exception:
            return datetime.min

    return sorted(mapa.values(), key=chave)


def buscar_html_publico(url):
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 DoubleBlazeIA/1.0",
            "Accept": "text/html,application/xhtml+xml"
        }
    )
    return urlopen(req, timeout=20).read().decode("utf-8", errors="replace")

def extrair_lista_feed(obj):
    if isinstance(obj, list):
        return obj

    if isinstance(obj, dict):
        for chave in ("resultados", "results", "rodadas", "data"):
            valor = obj.get(chave)
            if isinstance(valor, list):
                return valor

    return []


def item_feed_para_rodada(item):
    if isinstance(item, str) or isinstance(item, int):
        cor = normalizar_cor(item)
        if not cor:
            return None
        return {
            "id": "",
            "cor": cor,
            "data_hora": agora_brasilia()
        }

    if not isinstance(item, dict):
        return None

    bruto = item.get("cor", item.get("resultado", item.get("numero", "")))
    cor = normalizar_cor(bruto)

    if not cor:
        return None

    identificador = str(
        item.get("id", item.get("rodada", item.get("roll_id", item.get("uuid", ""))))
    )

    horario = str(
        item.get(
            "data_hora",
            item.get("datahora", item.get("horario", item.get("created_at", agora_brasilia())))
        )
    )

    return {
        "id": identificador,
        "cor": cor,
        "data_hora": horario
    }


def cores(rodadas):
    out = []
    for item in rodadas:
        cor = str(item.get("cor", ""))
        if cor in ("R", "B", "W"):
            out.append(cor)
    return out


def probabilidades_janela(data, tamanho):
    contagem = {"R": 1.0, "B": 1.0, "W": 1.0}
    inicio = max(0, len(data) - tamanho)

    for cor in data[inicio:]:
        contagem[cor] += 1.0

    total = contagem["R"] + contagem["B"] + contagem["W"]
    return {k: contagem[k] / total for k in contagem}


def melhor_cor(probs):
    return max(("R", "B", "W"), key=lambda c: probs.get(c, 0.0))


def probabilidade_ensemble(data, cfg):
    if not data:
        return {"R": 0.0, "B": 0.0, "W": 0.0}

    longa = probabilidades_janela(data, int(cfg["janela_longa"]))
    recente = probabilidades_janela(data, int(cfg["janela_recente"]))

    return {
        c: 0.50 * longa[c] + 0.50 * recente[c]
        for c in ("R", "B", "W")
    }


def contar_amostras_ultimo_padrao(data, tamanho=3):
    if len(data) <= tamanho:
        return 0

    padrao = data[-tamanho:]
    total = 0

    for i in range(0, len(data) - tamanho):
        if data[i:i+tamanho] == padrao:
            total += 1

    return total


def avaliar_configuracao_walk_forward(data, limite, amostras_minimas):
    if len(data) < 60:
        return {
            "entradas": 0,
            "acertos": 0,
            "taxa": 0.0
        }

    inicio = max(30, len(data) - 1000)
    entradas = 0
    acertos = 0

    for i in range(inicio, len(data)):
        prefixo = data[:i]
        cfg = carregar_config()
        probs = probabilidade_ensemble(prefixo, cfg)
        escolha = melhor_cor(probs)
        amostras = contar_amostras_ultimo_padrao(prefixo)

        if probs[escolha] < limite:
            continue
        if amostras < amostras_minimas:
            continue

        entradas += 1
        if data[i] == escolha:
            acertos += 1

    taxa = (100.0 * acertos / entradas) if entradas else 0.0
    return {
        "entradas": entradas,
        "acertos": acertos,
        "taxa": taxa
    }


def melhor_configuracao(data, cfg):
    if not cfg.get("modo_adaptativo", True):
        return {
            "limite": float(cfg["sinal_minimo"]),
            "amostras": int(cfg["amostras_minimas"]),
            "taxa": 0.0,
            "entradas": 0
        }

    melhor = None

    for limite in cfg["limites_testados"]:
        for amostras in cfg["amostras_testadas"]:
            r = avaliar_configuracao_walk_forward(data, float(limite), int(amostras))

            if r["entradas"] < 10:
                continue

            # Penaliza amostras muito pequenas.
            score = r["taxa"] + min(10.0, r["entradas"] / 20.0)

            candidato = {
                "limite": float(limite),
                "amostras": int(amostras),
                "taxa": float(r["taxa"]),
                "entradas": int(r["entradas"]),
                "score": float(score)
            }

            if melhor is None or candidato["score"] > melhor["score"]:
                melhor = candidato

    if melhor is None:
        return {
            "limite": float(cfg["sinal_minimo"]),
            "amostras": int(cfg["amostras_minimas"]),
            "taxa": 0.0,
            "entradas": 0,
            "score": 0.0
        }

    return melhor



def probabilidade_modelo_padrao(data, tamanho=3):
    if len(data) < tamanho + 1:
        return None

    padrao = data[-tamanho:]
    contagem = {"R": 0, "B": 0, "W": 0}
    ocorrencias = 0

    for i in range(0, len(data) - tamanho):
        if data[i:i+tamanho] != padrao:
            continue

        proximo_indice = i + tamanho

        if proximo_indice >= len(data):
            continue

        proxima = data[proximo_indice]

        if proxima in contagem:
            contagem[proxima] += 1
            ocorrencias += 1

    if ocorrencias < 5:
        return None

    return {
        cor: contagem[cor] / ocorrencias
        for cor in ("R", "B", "W")
    }


def concordancia_modelos(data, cfg):
    if not data:
        return {
            "acordos": 0,
            "total_modelos": 0,
            "escolha": ""
        }

    longa = probabilidades_janela(data, int(cfg["janela_longa"]))
    recente = probabilidades_janela(data, int(cfg["janela_recente"]))
    padrao = probabilidade_modelo_padrao(data, 3)
    ensemble = probabilidade_ensemble(data, cfg)

    escolha_final = melhor_cor(ensemble)
    escolhas = [
        melhor_cor(longa),
        melhor_cor(recente)
    ]

    if padrao is not None:
        escolhas.append(melhor_cor(padrao))

    acordos = sum(1 for escolha in escolhas if escolha == escolha_final)

    return {
        "acordos": acordos,
        "total_modelos": len(escolhas),
        "escolha": escolha_final
    }



def estabilidade_configuracao_servidor():
    with LOCK:
        sinais = list(ESTADO.get("historico_sinais", []))

    validos = [s for s in sinais if isinstance(s, dict) and s.get("valido") is not None]

    if not validos:
        return 0.0

    def taxa(janela):
        bloco = validos[-janela:]
        avaliados = 0
        acertos = 0

        for item in bloco:
            resultado_real = item.get("resultado_real")
            cor = item.get("cor")

            if resultado_real not in ("R", "B", "W"):
                continue
            if cor not in ("R", "B", "W"):
                continue

            avaliados += 1
            if resultado_real == cor:
                acertos += 1

        if avaliados == 0:
            return None

        return 100.0 * acertos / avaliados

    taxas = []

    for janela in (20, 50, 100):
        valor = taxa(janela)
        if valor is not None:
            taxas.append(valor)

    if not taxas:
        return 100.0

    media = sum(taxas) / len(taxas)
    dispersao = max(taxas) - min(taxas) if len(taxas) > 1 else 0.0
    return max(0.0, min(100.0, media - dispersao))


def calcular_sinal():
    with LOCK:
        data = cores(ESTADO["rodadas"])

    cfg = carregar_config()

    if len(data) < 30:
        return {"valido": False}

    melhor_cfg = melhor_configuracao(data, cfg)
    probs = probabilidade_ensemble(data, cfg)
    escolha = melhor_cor(probs)
    amostras = contar_amostras_ultimo_padrao(data)

    detalhes_concordancia = concordancia_modelos(data, cfg)
    total_modelos = int(detalhes_concordancia["total_modelos"])
    concordancia_minima = min(
        int(cfg.get("concordancia_minima", 2)),
        total_modelos
    )
    concordancia_ok = int(detalhes_concordancia["acordos"]) >= concordancia_minima

    estabilidade_atual = estabilidade_configuracao_servidor()
    estabilidade_minima = float(cfg.get("estabilidade_minima", 50.0))
    estabilidade_ok = estabilidade_atual >= estabilidade_minima

    valido = (
        probs[escolha] >= float(melhor_cfg["limite"])
        and amostras >= int(melhor_cfg["amostras"])
        and concordancia_ok
        and estabilidade_ok
    )

    return {
        "valido": bool(valido),
        "cor": escolha,
        "probabilidade": float(probs[escolha]),
        "amostras": int(amostras),
        "configuracao": "adaptativa %.0f%% / %d amostras" % (
            float(melhor_cfg["limite"]) * 100.0,
            int(melhor_cfg["amostras"])
        ),
        "taxa_historica_configuracao": float(melhor_cfg.get("taxa", 0.0)),
        "entradas_avaliadas": int(melhor_cfg.get("entradas", 0)),
        "concordancia_modelos": int(detalhes_concordancia["acordos"]),
        "total_modelos": total_modelos,
        "concordancia_minima": concordancia_minima,
        "estabilidade_atual": float(estabilidade_atual),
        "estabilidade_minima": float(estabilidade_minima),
        "data_hora_brasilia": agora_brasilia()
    }


def enviar_ntfy(sinal):
    cfg = carregar_config()
    topico = str(cfg.get("ntfy_topic", "")).strip() or os.getenv("NTFY_TOPIC", "").strip()
    if not topico:
        return False

    servidor = (
        str(cfg.get("ntfy_server", "")).strip()
        or os.getenv("NTFY_SERVER", "https://ntfy.sh").strip()
    ).rstrip("/")
    url = servidor + "/" + topico

    mensagem = (
        "Double IA: %s | %.2f%% | %d amostras | %s"
        % (
            sinal.get("cor", "-"),
            float(sinal.get("probabilidade", 0.0)) * 100.0,
            int(sinal.get("amostras", 0)),
            sinal.get("configuracao", "")
        )
    )

    try:
        req = Request(
            url,
            data=mensagem.encode("utf-8"),
            method="POST",
            headers={
                "Title": "Novo sinal estatístico",
                "Priority": "high",
                "Tags": "chart_with_upwards_trend"
            }
        )
        urlopen(req, timeout=10).read()
        return True
    except Exception as exc:
        print("Falha ao enviar notificação:", exc)
        return False


def atualizar_sinal_e_notificar():
    sinal = calcular_sinal()
    registrar_sinal(sinal)

    with LOCK:
        anterior = ESTADO.get("ultimo_sinal", {"valido": False})
        ESTADO["ultimo_sinal"] = sinal
        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)

    virou_novo_sinal = (
        sinal.get("valido", False)
        and (
            not anterior.get("valido", False)
            or anterior.get("cor") != sinal.get("cor")
            or anterior.get("data_hora_brasilia") != sinal.get("data_hora_brasilia")
        )
    )

    cfg = carregar_config()
    geracao_automatica = bool(cfg.get("geracao_automatica", True))

    if virou_novo_sinal and geracao_automatica and pode_notificar_agora():
        if enviar_ntfy(sinal):
            registrar_notificacao_enviada()


def adicionar_rodada(rodada):
    if not rodada:
        return False

    with LOCK:
        rodadas = ESTADO["rodadas"]

        identificador = rodada.get("id", "")
        if identificador:
            for existente in rodadas[-5000:]:
                if str(existente.get("id", "")) == identificador:
                    return False

        rodadas.append(rodada)

        if len(rodadas) > 50000:
            del rodadas[:-50000]

        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)

    atualizar_sinal_e_notificar()
    return True




def extrair_bestblaze_historico_html(html):
    """
    Parser histórico BestBlaze baseado na estrutura pública verificada:

      NUMERO
      DD/MM/AAAA HH:MM:SS

    Quando um timestamp aparece sem número imediatamente antes, ele é tratado
    como branco (W/0).
    """
    texto = html_para_texto(html)

    timestamp_re = re.compile(
        r"\b\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}\b"
    )

    par_re = re.compile(
        r"(?<!\d)(0|[1-9]|1[0-4])\s+"
        r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"
    )

    pares = {}
    for numero_texto, data_hora in par_re.findall(texto):
        try:
            numero = int(numero_texto)
        except Exception:
            continue
        pares[data_hora] = numero

    todos_horarios = []
    vistos_horarios = set()

    for data_hora in timestamp_re.findall(texto):
        if data_hora in vistos_horarios:
            continue
        vistos_horarios.add(data_hora)
        todos_horarios.append(data_hora)

    rodadas = []

    for data_hora in todos_horarios:
        try:
            momento = datetime.strptime(data_hora, "%d/%m/%Y %H:%M:%S")
        except Exception:
            continue

        if data_hora in pares:
            numero = int(pares[data_hora])
            cor = normalizar_cor(numero)
        else:
            numero = 0
            cor = "W"

        identificador = "%s-%02d" % (
            momento.strftime("%Y%m%d-%H%M%S"),
            numero
        )

        rodadas.append({
            "id": identificador,
            "numero": numero,
            "cor": cor,
            "data_hora": data_hora,
            "origem": "historico"
        })

    rodadas.sort(
        key=lambda item: datetime.strptime(
            item["data_hora"],
            "%d/%m/%Y %H:%M:%S"
        )
    )
    return rodadas


def detectar_formulario_periodo_bestblaze(html, base_url):
    forms = re.findall(
        r"(?is)<form\b([^>]*)>(.*?)</form>",
        html
    )

    for attrs, corpo in forms:
        inputs = re.findall(r"(?is)<input\b([^>]*)>", corpo)
        campos_data = []

        for attrs_input in inputs:
            tipo_m = re.search(
                r"(?i)\btype\s*=\s*[\"']?([^\"'\s>]+)",
                attrs_input
            )
            nome_m = re.search(
                r"(?i)\bname\s*=\s*[\"']?([^\"'\s>]+)",
                attrs_input
            )

            if not nome_m:
                continue

            tipo = tipo_m.group(1).lower() if tipo_m else "text"
            nome = nome_m.group(1)

            if tipo in ("date", "datetime-local"):
                campos_data.append((nome, tipo))

        if len(campos_data) < 2:
            continue

        method_m = re.search(
            r"(?i)\bmethod\s*=\s*[\"']?([^\"'\s>]+)",
            attrs
        )
        action_m = re.search(
            r"(?i)\baction\s*=\s*[\"']?([^\"'\s>]+)",
            attrs
        )

        metodo = method_m.group(1).upper() if method_m else "GET"
        action = action_m.group(1) if action_m else base_url

        return {
            "method": metodo,
            "action": urljoin(base_url, action),
            "campo_inicial": campos_data[0][0],
            "tipo_inicial": campos_data[0][1],
            "campo_final": campos_data[1][0],
            "tipo_final": campos_data[1][1]
        }

    return None


def buscar_periodo_bestblaze(data_inicial, data_final):
    base_url = "https://bestblaze.com.br/doubleRodadas"
    pagina_inicial = buscar_html_publico(base_url)
    form = detectar_formulario_periodo_bestblaze(
        pagina_inicial,
        base_url
    )

    if not form:
        raise RuntimeError(
            "Não foi possível identificar automaticamente o formulário de período da BestBlaze"
        )

    def formatar_data(dt, tipo):
        if tipo == "datetime-local":
            return dt.strftime("%Y-%m-%dT%H:%M")
        return dt.strftime("%Y-%m-%d")

    payload = {
        form["campo_inicial"]: formatar_data(
            data_inicial,
            form["tipo_inicial"]
        ),
        form["campo_final"]: formatar_data(
            data_final,
            form["tipo_final"]
        )
    }

    encoded = urlencode(payload)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 15) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0 Mobile Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        "Cache-Control": "no-cache"
    }

    if form["method"] == "POST":
        req = Request(
            form["action"],
            data=encoded.encode("utf-8"),
            headers={
                **headers,
                "Content-Type": "application/x-www-form-urlencoded"
            }
        )
        html = urlopen(req, timeout=35).read().decode(
            "utf-8",
            errors="replace"
        )
    else:
        separador = "&" if "?" in form["action"] else "?"
        url = form["action"] + separador + encoded
        req = Request(url, headers=headers)
        html = urlopen(req, timeout=35).read().decode(
            "utf-8",
            errors="replace"
        )

    return html, form, payload


def importar_1000_bestblaze(meta=1000):
    meta = max(100, min(int(meta), 5000))
    agora = datetime.now(timezone(timedelta(hours=-3)))

    total_adicionadas = 0
    dias_consultados = []
    ultimo_form = None
    ultimo_payload = None

    for deslocamento in range(1, 8):
        alvo = agora - timedelta(days=deslocamento)
        inicio = alvo.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = alvo.replace(hour=23, minute=59, second=59, microsecond=0)

        html, form, payload = buscar_periodo_bestblaze(inicio, fim)
        ultimo_form = form
        ultimo_payload = payload

        rodadas = extrair_bestblaze_historico_html(html)

        resultado = adicionar_rodadas_em_lote(rodadas)
        adicionadas = int(resultado.get("adicionadas", 0))
        total_adicionadas += adicionadas

        dias_consultados.append({
            "data": alvo.strftime("%d/%m/%Y"),
            "recebidas": len(rodadas),
            "adicionadas": adicionadas,
            "duplicadas": int(resultado.get("duplicadas", 0))
        })

        with LOCK:
            total_banco = len(ESTADO.get("rodadas", []))

        if total_banco >= meta:
            break

    with LOCK:
        total_banco = len(ESTADO.get("rodadas", []))

    return {
        "ok": total_banco >= meta,
        "meta": meta,
        "total_banco": total_banco,
        "total_adicionadas_nesta_importacao": total_adicionadas,
        "dias_consultados": dias_consultados,
        "formulario_detectado": ultimo_form,
        "payload_usado": ultimo_payload,
        "cores": resumo_cores_historico(meta),
        "sequencias": sequencias_cores(meta)
    }


def adicionar_rodadas_em_lote(rodadas_novas):
    """
    Importação em lote sem disparar análise/notificação a cada registro.
    Ao final, recalcula o sinal uma única vez.
    """
    if not isinstance(rodadas_novas, list):
        return {
            "recebidas": 0,
            "adicionadas": 0,
            "duplicadas": 0
        }

    adicionadas = 0
    duplicadas = 0

    with LOCK:
        existentes = set(
            str(item.get("id", ""))
            for item in ESTADO.get("rodadas", [])
            if isinstance(item, dict) and str(item.get("id", ""))
        )
        horarios_existentes = set(
            str(item.get("data_hora", "")).strip()
            for item in ESTADO.get("rodadas", [])
            if isinstance(item, dict) and str(item.get("data_hora", "")).strip()
        )

        banco = ESTADO.setdefault("rodadas", [])

        for rodada in rodadas_novas:
            if not isinstance(rodada, dict):
                continue

            identificador = str(rodada.get("id", ""))

            horario = str(rodada.get("data_hora", "")).strip()

            if (
                (identificador and identificador in existentes)
                or (horario and horario in horarios_existentes)
            ):
                duplicadas += 1
                continue

            cor = str(rodada.get("cor", ""))
            if cor not in ("R", "B", "W"):
                continue

            banco.append(rodada)
            adicionadas += 1

            if identificador:
                existentes.add(identificador)
            if horario:
                horarios_existentes.add(horario)

        def chave_banco(item):
            try:
                return datetime.strptime(
                    str(item.get("data_hora", "")),
                    "%d/%m/%Y %H:%M:%S"
                )
            except Exception:
                return datetime.min

        banco.sort(key=chave_banco)

        if len(banco) > 50000:
            del banco[:-50000]

        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)

    if adicionadas > 0:
        atualizar_sinal_e_notificar()

    return {
        "recebidas": len(rodadas_novas),
        "adicionadas": adicionadas,
        "duplicadas": duplicadas
    }


def importar_historico_bestblaze(url=None):
    """
    Importa histórico da página pública sem alterar o coletor ao vivo.
    Se url não for informada, usa a página pública de histórico.
    """
    if not url:
        url = "https://bestblaze.com.br/doubleRodadas"

    html = buscar_html_publico(url)
    rodadas_normais = extrair_bestblaze_historico_html(html)

    rodadas_brancas = []
    try:
        html_brancos = buscar_html_publico(
            "https://bestblaze.com.br/doubleBrancosDia"
        )
        rodadas_brancas = extrair_bestblaze_brancos_html(html_brancos)
    except Exception as exc_brancos:
        print("Aviso na importação de brancos:", exc_brancos)

    rodadas = mesclar_rodadas_por_horario(
        rodadas_normais,
        rodadas_brancas
    )

    if not rodadas:
        raise RuntimeError(
            "Histórico respondeu, mas nenhuma rodada confiável foi reconhecida"
        )

    resultado = adicionar_rodadas_em_lote(rodadas)
    resultado["url"] = url
    resultado["total_banco"] = len(ESTADO.get("rodadas", []))
    return resultado


def resumo_cores_historico(limite=1000):
    """
    Estatísticas focadas em cores; números ficam apenas como metadado.
    """
    with LOCK:
        bloco = list(ESTADO.get("rodadas", []))[-max(1, int(limite)):]

    contagem = {"R": 0, "B": 0, "W": 0}

    for item in bloco:
        if not isinstance(item, dict):
            continue
        cor = str(item.get("cor", ""))
        if cor in contagem:
            contagem[cor] += 1

    total = sum(contagem.values())

    def pct(valor):
        return (100.0 * valor / total) if total else 0.0

    return {
        "total": total,
        "vermelho": {
            "quantidade": contagem["R"],
            "percentual": pct(contagem["R"])
        },
        "preto": {
            "quantidade": contagem["B"],
            "percentual": pct(contagem["B"])
        },
        "branco": {
            "quantidade": contagem["W"],
            "percentual": pct(contagem["W"])
        }
    }


def sequencias_cores(limite=1000):
    with LOCK:
        dados = [
            str(item.get("cor", ""))
            for item in ESTADO.get("rodadas", [])[-max(1, int(limite)):]
            if isinstance(item, dict)
        ]

    dados = [c for c in dados if c in ("R", "B", "W")]

    if not dados:
        return {
            "maior_vermelho": 0,
            "maior_preto": 0,
            "maior_branco": 0,
            "alternancias": 0
        }

    max_seq = {"R": 0, "B": 0, "W": 0}
    atual_cor = None
    atual_tam = 0
    alternancias = 0

    anterior = None
    for cor in dados:
        if anterior is not None and cor != anterior:
            alternancias += 1
        anterior = cor

        if cor == atual_cor:
            atual_tam += 1
        else:
            atual_cor = cor
            atual_tam = 1

        if atual_tam > max_seq[cor]:
            max_seq[cor] = atual_tam

    return {
        "maior_vermelho": max_seq["R"],
        "maior_preto": max_seq["B"],
        "maior_branco": max_seq["W"],
        "alternancias": alternancias
    }


def backtest_cor_simples(cor="R", limite=1000):
    """
    Backtest básico e transparente: mede quantas vezes a cor escolhida apareceu
    no histórico analisado. Não promete previsão futura.
    """
    cor = str(cor).upper()
    if cor not in ("R", "B", "W"):
        raise ValueError("cor inválida")

    with LOCK:
        dados = [
            str(item.get("cor", ""))
            for item in ESTADO.get("rodadas", [])[-max(1, int(limite)):]
            if isinstance(item, dict)
        ]

    dados = [c for c in dados if c in ("R", "B", "W")]
    total = len(dados)
    acertos = sum(1 for c in dados if c == cor)
    taxa = (100.0 * acertos / total) if total else 0.0

    return {
        "cor": cor,
        "total": total,
        "ocorrencias": acertos,
        "taxa_historica": taxa
    }



def buscar_feed():
    cfg = carregar_config()
    url = str(cfg.get("resultados_url", "")).strip() or os.getenv("RESULTADOS_URL", "").strip()
    modo_fonte = str(cfg.get("modo_fonte", "json")).strip()

    with LOCK:
        ESTADO["ultima_consulta_fonte"] = agora_brasilia()

    if not url:
        with LOCK:
            ESTADO["fonte_online"] = False
            ESTADO["ultimo_erro_fonte"] = "Fonte não configurada"
            salvar_json(BANCO, ESTADO)
        return 0

    try:
        if modo_fonte == "bestblaze_html":
            raw_html = buscar_html_publico(url)
            items_normais = extrair_bestblaze_html(raw_html)

            # Complementa a fonte principal com a página dedicada aos brancos.
            # Se a página de brancos falhar, o coletor principal continua vivo.
            items_brancos = []
            try:
                html_brancos = buscar_html_publico(
                    "https://bestblaze.com.br/doubleBrancosDia"
                )
                items_brancos = extrair_bestblaze_brancos_html(html_brancos)
            except Exception as exc_brancos:
                print("Aviso ao consultar brancos:", exc_brancos)

            items = mesclar_rodadas_por_horario(
                items_normais,
                items_brancos
            )
        else:
            req = Request(url, headers={"User-Agent": "DoubleBlazeIA/1.0"})
            raw = urlopen(req, timeout=15).read().decode("utf-8")
            obj = json.loads(raw)
            items = extrair_lista_feed(obj)

        adicionadas = 0
        ultima_rodada = ""

        for item in items:
            rodada = item_feed_para_rodada(item)

            if rodada:
                ultima_rodada = str(rodada.get("data_hora", ""))

            if adicionar_rodada(rodada):
                adicionadas += 1

        with LOCK:
            ESTADO["fonte_online"] = True
            ESTADO["ultimo_erro_fonte"] = ""
            if ultima_rodada:
                ESTADO["ultima_rodada_fonte"] = ultima_rodada
            ESTADO["total_importadas"] = int(ESTADO.get("total_importadas", 0)) + adicionadas
            salvar_json(BANCO, ESTADO)

        return adicionadas

    except Exception as exc:
        with LOCK:
            ESTADO["fonte_online"] = False
            ESTADO["ultimo_erro_fonte"] = str(exc)
            salvar_json(BANCO, ESTADO)

        print("Erro ao consultar fonte:", exc)
        return 0


def worker_feed():
    while True:
        cfg = carregar_config()
        intervalo = int(cfg.get("intervalo_segundos", os.getenv("INTERVALO_SEGUNDOS", "10")))

        try:
            adicionadas = buscar_feed()
            if adicionadas:
                print("Rodadas novas:", adicionadas)
        except Exception as exc:
            print("Erro no monitor:", exc)

        time.sleep(max(5, intervalo))


class Handler(BaseHTTPRequestHandler):

    def autorizado(self):
        return chave_autorizada(self.headers)

    def exigir_autorizacao(self):
        if self.autorizado():
            return True

        self.enviar_json(401, {
            "erro": "não autorizado"
        })
        return False

    def enviar_json(self, codigo, obj):
        corpo = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(corpo)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(corpo)

    def ler_json(self):
        tamanho = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(tamanho) if tamanho else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_GET(self):
        if not self.exigir_autorizacao():
            return

        # V32: permite acionar a importacao historica diretamente pelo navegador.
        # Mantem a rota POST original e nao altera o coletor ao vivo.
        if self.path == "/importar-historico":
            try:
                resultado = importar_historico_bestblaze()
                self.enviar_json(200, {
                    "ok": True,
                    "resultado": resultado,
                    "cores": resumo_cores_historico(1000),
                    "sequencias": sequencias_cores(1000)
                })
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path.startswith("/importar-1000"):
            meta = 1000
            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("meta="):
                            meta = int(parte.split("=", 1)[1])
            except Exception:
                meta = 1000

            try:
                resultado = importar_1000_bestblaze(meta)
                self.enviar_json(
                    200 if resultado.get("ok") else 206,
                    resultado
                )
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path == "/diagnostico-brancos":
            try:
                html_brancos = buscar_html_publico(
                    "https://bestblaze.com.br/doubleBrancosDia"
                )
                brancos = extrair_bestblaze_brancos_html(html_brancos)

                with LOCK:
                    brancos_banco = sum(
                        1
                        for item in ESTADO.get("rodadas", [])
                        if isinstance(item, dict)
                        and str(item.get("cor", "")) == "W"
                    )

                self.enviar_json(200, {
                    "ok": True,
                    "brancos_encontrados_na_fonte": len(brancos),
                    "brancos_no_banco": brancos_banco,
                    "primeiro_branco_fonte": (
                        brancos[0].get("data_hora", "") if brancos else ""
                    ),
                    "ultimo_branco_fonte": (
                        brancos[-1].get("data_hora", "") if brancos else ""
                    )
                })
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path == "/diagnostico-periodo":
            try:
                base_url = "https://bestblaze.com.br/doubleRodadas"
                html = buscar_html_publico(base_url)
                form = detectar_formulario_periodo_bestblaze(
                    html,
                    base_url
                )
                self.enviar_json(200, {
                    "ok": bool(form),
                    "formulario": form
                })
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path.startswith("/analise-cores"):
            limite = 1000
            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                limite = 1000

            self.enviar_json(200, resumo_cores_historico(limite))
            return

        if self.path.startswith("/sequencias-cores"):
            limite = 1000
            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                limite = 1000

            self.enviar_json(200, sequencias_cores(limite))
            return

        if self.path.startswith("/backtest-cor"):
            cor = "R"
            limite = 1000

            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("cor="):
                            cor = parte.split("=", 1)[1].upper()
                        elif parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                pass

            try:
                self.enviar_json(200, backtest_cor_simples(cor, limite))
            except Exception as exc:
                self.enviar_json(400, {"erro": str(exc)})
            return

        if self.path == "/diagnostico":
            cfg = carregar_config()

            with LOCK:
                sinais_registrados = len(ESTADO.get("historico_sinais", []))
                rodadas = len(ESTADO.get("rodadas", []))
                fonte_online = bool(ESTADO.get("fonte_online", False))
                ultima_rodada = str(ESTADO.get("ultima_rodada_fonte", ""))
                ultima_atualizacao = str(ESTADO.get("ultima_atualizacao", ""))

            fonte_url = (
                str(cfg.get("resultados_url", "")).strip()
                or os.getenv("RESULTADOS_URL", "").strip()
            )

            ntfy_topic = (
                str(cfg.get("ntfy_topic", "")).strip()
                or os.getenv("NTFY_TOPIC", "").strip()
            )

            self.enviar_json(200, {
                "servidor_online": True,
                "fonte_configurada": bool(fonte_url),
                "fonte_online": fonte_online,
                "notificacao_configurada": bool(ntfy_topic),
                "geracao_automatica": bool(cfg.get("geracao_automatica", True)),
                "modo_adaptativo": bool(cfg.get("modo_adaptativo", True)),
                "autenticacao_ativada": bool(chave_acesso_configurada()),
                "rodadas": rodadas,
                "sinais_registrados": sinais_registrados,
                "tempo_ativo_segundos": int(time.time() - INICIO_SERVIDOR_EPOCH),
                "ultima_rodada": ultima_rodada,
                "ultima_atualizacao": ultima_atualizacao
            })
            return

        if self.path.startswith("/historico-sinais"):
            limite = 50

            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                limite = 50

            limite = max(1, min(limite, 500))

            with LOCK:
                sinais = ESTADO.get("historico_sinais", [])[-limite:]

            self.enviar_json(200, {
                "sinais": sinais,
                "quantidade": len(sinais)
            })
            return

        if self.path.startswith("/historico"):
            limite = 1000

            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                limite = 1000

            limite = max(1, min(limite, 5000))

            with LOCK:
                rodadas = ESTADO["rodadas"][-limite:]
                self.enviar_json(200, {
                    "rodadas": rodadas,
                    "quantidade": len(rodadas),
                    "ultima_atualizacao": ESTADO.get("ultima_atualizacao", "")
                })
            return

        if self.path == "/fonte-status":
            cfg = carregar_config()

            with LOCK:
                self.enviar_json(200, {
                    "online": bool(ESTADO.get("fonte_online", False)),
                    "configurada": bool(
                        str(cfg.get("resultados_url", "")).strip()
                        or os.getenv("RESULTADOS_URL", "").strip()
                    ),
                    "modo_fonte": str(cfg.get("modo_fonte", "json")),
                    "ultima_consulta": str(ESTADO.get("ultima_consulta_fonte", "")),
                    "ultima_rodada": str(ESTADO.get("ultima_rodada_fonte", "")),
                    "ultimo_erro": str(ESTADO.get("ultimo_erro_fonte", "")),
                    "total_importadas": int(ESTADO.get("total_importadas", 0))
                })
            return

        if self.path == "/status":
            with LOCK:
                self.enviar_json(200, {
                    "online": True,
                    "rodadas": len(ESTADO["rodadas"]),
                    "ultima_atualizacao": ESTADO.get("ultima_atualizacao", ""),
                    "feed_configurado": bool(
                        str(carregar_config().get("resultados_url", "")).strip()
                        or os.getenv("RESULTADOS_URL", "").strip()
                    ),
                    "modo_fonte": str(carregar_config().get("modo_fonte", "json")),
                    "fonte_online": bool(ESTADO.get("fonte_online", False)),
                    "ultima_rodada_fonte": str(ESTADO.get("ultima_rodada_fonte", ""))
                })
            return

        if self.path == "/sinal":
            self.enviar_json(200, calcular_sinal())
            return

        if self.path == "/configuracao":
            self.enviar_json(200, carregar_config())
            return

        self.enviar_json(404, {"erro": "rota não encontrada"})

    def do_POST(self):
        if not self.exigir_autorizacao():
            return

        if self.path == "/importar-historico":
            try:
                resultado = importar_historico_bestblaze()
                self.enviar_json(200, {
                    "ok": True,
                    "resultado": resultado,
                    "cores": resumo_cores_historico(1000),
                    "sequencias": sequencias_cores(1000)
                })
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path == "/atualizar-agora":
            novas = buscar_feed()
            self.enviar_json(200, {
                "ok": True,
                "novas_rodadas": int(novas),
                "data_hora_brasilia": agora_brasilia()
            })
            return

        if self.path == "/rodada":
            obj = self.ler_json()
            rodada = item_feed_para_rodada(obj)
            if rodada is None:
                self.enviar_json(400, {"erro": "rodada inválida"})
                return

            nova = adicionar_rodada(rodada)
            self.enviar_json(200, {"ok": True, "nova": nova})
            return

        if self.path == "/teste-notificacao":
            sinal_teste = {
                "valido": True,
                "cor": "B",
                "probabilidade": 0.65,
                "amostras": 100,
                "configuracao": "teste do servidor 24h",
                "data_hora_brasilia": agora_brasilia()
            }
            enviado = enviar_ntfy(sinal_teste)
            self.enviar_json(200, {
                "ok": bool(enviado),
                "mensagem": "notificação enviada" if enviado else "notificação não configurada ou falhou"
            })
            return

        if self.path == "/configuracao":
            obj = self.ler_json()
            if not isinstance(obj, dict):
                self.enviar_json(400, {"erro": "configuração inválida"})
                return

            cfg = carregar_config()
            permitidas = {
                "sinal_minimo",
                "amostras_minimas",
                "modo_adaptativo",
                "limites_testados",
                "amostras_testadas",
                "janela_recente",
                "janela_longa",
                "resultados_url",
                "modo_fonte",
                "intervalo_segundos",
                "ntfy_server",
                "ntfy_topic",
                "geracao_automatica",
                "intervalo_notificacao_minutos",
                "concordancia_minima",
                "estabilidade_minima"
            }

            for chave, valor in obj.items():
                if chave in permitidas:
                    cfg[chave] = valor

            salvar_json(CONFIG, cfg)
            atualizar_sinal_e_notificar()
            self.enviar_json(200, {"ok": True, "configuracao": cfg})
            return

        self.enviar_json(404, {"erro": "rota não encontrada"})

    def log_message(self, format, *args):
        print("[%s] %s" % (agora_brasilia(), format % args))


def main():
    carregar_estado()

    if not CONFIG.exists():
        salvar_json(CONFIG, CONFIG_PADRAO)

    porta = int(os.getenv("PORT", os.getenv("PORTA", "8787")))

    thread = threading.Thread(target=worker_feed, daemon=True)
    thread.start()

    servidor = ThreadingHTTPServer(("0.0.0.0", porta), Handler)
    print("Servidor 24h iniciado na porta", porta)
    print("Horário de Brasília:", agora_brasilia())
    servidor.serve_forever()


if __name__ == "__main__":
    main()
