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
    Extrai rodadas da página pública BestBlaze /doubleRodadasDia.

    Formato atual observado:
      DD/MM/AAAA HH:MM:SS
      NUMERO

    Se houver horário sem número logo depois, a entrada é ignorada.
    """
    texto = html_para_texto(html)

    padrao = re.compile(
        r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})"
        r"\s+"
        r"(0|[1-9]|1[0-4])(?=\s|$)"
    )

    rodadas = []
    vistos = set()

    for data_hora, numero_texto in padrao.findall(texto):
        try:
            numero = int(numero_texto)
            momento = datetime.strptime(data_hora, "%d/%m/%Y %H:%M:%S")
        except Exception:
            continue

        identificador = "%s-%02d" % (
            momento.strftime("%Y%m%d-%H%M%S"),
            numero
        )

        if identificador in vistos:
            continue

        vistos.add(identificador)
        rodadas.append({
            "id": identificador,
            "numero": numero,
            "cor": normalizar_cor(numero),
            "data_hora": data_hora
        })

    def chave(item):
        try:
            return datetime.strptime(item["data_hora"], "%d/%m/%Y %H:%M:%S")
        except Exception:
            return datetime.min

    rodadas.sort(key=chave)
    return rodadas

def buscar_html_publico(url):
    urls = [url]

    # Fallback entre www e sem www.
    if "://www.bestblaze.com.br/" in url:
        urls.append(url.replace("://www.bestblaze.com.br/", "://bestblaze.com.br/"))
    elif "://bestblaze.com.br/" in url:
        urls.append(url.replace("://bestblaze.com.br/", "://www.bestblaze.com.br/"))

    ultimo_erro = None

    for tentativa in urls:
        try:
            req = Request(
                tentativa,
                headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Mobile Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                }
            )
            resposta = urlopen(req, timeout=25)
            status = getattr(resposta, "status", 200)

            if status < 200 or status >= 300:
                raise RuntimeError("HTTP %s ao consultar BestBlaze" % status)

            corpo = resposta.read().decode("utf-8", errors="replace")

            if not corpo.strip():
                raise RuntimeError("BestBlaze respondeu conteúdo vazio")

            return corpo

        except Exception as exc:
            ultimo_erro = exc

    raise RuntimeError("Falha ao consultar BestBlaze: %s" % ultimo_erro)

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
            url = "https://www.bestblaze.com.br/doubleRodadasDia"
            raw_html = buscar_html_publico(url)
            items = extrair_bestblaze_html(raw_html)

            if not items:
                raise RuntimeError("BestBlaze respondeu, mas nenhuma rodada foi reconhecida no HTML")
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

        if self.path == "/":
            self.enviar_json(200, {
                "online": True,
                "servico": "Double Blaze IA 24h",
                "rotas": ["/status", "/diagnostico", "/fonte-status", "/historico", "/sinal"]
            })
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
