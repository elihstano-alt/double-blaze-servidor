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
DATABASE_URL=postgresql://... (Supabase)
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from urllib.error import URLError, HTTPError
from http.cookiejar import CookieJar
from urllib.parse import urlencode, urljoin, urlparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import os
import threading
import time
import re
import hashlib
import bisect
import numpy as np
from numba import njit
from html import unescape

try:
    import psycopg
except Exception:
    psycopg = None

try:
    import websocket
except Exception:
    websocket = None

BASE = Path(__file__).resolve().parent
BANCO = BASE / "banco_servidor.json"
CONFIG = BASE / "configuracao_servidor.json"

LOCK = threading.Lock()
INICIO_SERVIDOR_EPOCH = time.time()
ULTIMO_HEARTBEAT = {"tentativa": "", "sucesso": "", "erro": "", "url": ""}

LIMITE_HISTORICO = 30000

# V58.0: início da base recente auditada manualmente a partir do JSON completo.
CORTE_BASE_AUDITADA = "24/08/2026 00:00:03"
META_CORTE_BASE = "v58_base_auditada_20260824"

ANALISE_EVENT = threading.Event()
ANALISE_PENDENTE_LOCK = threading.Lock()
ANALISE_PENDENTE_RODADA = None

LIMITE_AVALIACOES_SHADOW = 500
LIMITE_ENTRADAS_REGISTRADAS = 1000
LIMITE_SEM_RESULTADO_SEGUNDOS = 75.0

LIMITE_LACUNA_HISTORICO_SEGUNDOS = max(45, int(os.getenv("LIMITE_LACUNA_HISTORICO_SEGUNDOS", "45")))

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
    "ultima_notificacao_epoch": 0.0,
    "postgres_online": False,
    "ultimo_erro_postgres": "",
    "ultima_sincronizacao_postgres": "",
    "coletor_ciclos_sem_novas": 0,
    "coletor_fallback_ultimo": "",
    "coletor_fallback_adicionadas": 0,
    "coletor_ultimo_modo": "",
    "coletor_ultimo_erro": "",
    "ws_online": False,
    "ws_ultimo_evento": "",
    "ws_ultima_rodada": "",
    "ws_ultimo_erro": "",
    "ws_eventos_recebidos": 0,
    "ws_rodadas_adicionadas": 0,
    "ws_latencia_segundos": None,
    "ws_latencia_media_segundos": None,
    "ws_latencias_recentes": [],
    "ws_mensagens_raw": 0,
    "ws_ultimo_raw": "",
    "ws_endpoint_atual": "",
    "ws_assinaturas_enviadas": 0,
    "ws_handshake_recebido": False,
    "ws_duplicadas": 0,
    "ws_fora_de_ordem": 0,
    "ws_suspeitas_gap": 0,
    "ws_ultimo_id_rolling": "",
    "ws_ultimo_recebido_epoch": 0.0,
    "ws_ultimo_source_epoch": 0.0,
    "ws_processamento_ms": None,
    "ws_ultimo_recebido_brasilia": "",
    "ws_eventos_rolling_recebidos": 0,
    "ws_conectado_epoch": 0.0,
    "ws_ultimo_raw_epoch": 0.0,
    "sinal_recalculado_inicio": False,
    "sinal_erro_inicio": "",
    "sinal_base_rodada_id": "",
    "sinal_base_data_hora": "",
    "sinal_gerado_em": "",
    "sinal_calculo_ms": None,
    "sinal_seq": 0,
    "analise_em_andamento": False,
    "analise_pendente_id": "",
    "analise_iniciada_em": "",
    "shadow_previsao_pendente": {},
    "shadow_avaliacoes": [],
    "shadow_total_avaliado": 0,
    "shadow_ultima_avaliacao": "",
    "entradas_registradas": [],
    "entrada_pendente": {},
    "contador_win": 0,
    "contador_lose": 0,
    "contador_sem_gale": 0,
    "contador_gale_1": 0,
    "captura_interrompida": False,
    "captura_interrompida_desde": "",
    "ultima_verificacao_continuidade": "",
    "demo_contas": {},
    "bootstrap_1500": {"status": "aguardando", "tentativa": "", "sucesso": "", "erro": "", "quantidade": 0}
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


def database_url():
    return os.getenv("DATABASE_URL", "").strip()


def postgres_configurado():
    return bool(database_url())


def postgres_driver_ok():
    return psycopg is not None


def conectar_postgres():
    if not postgres_configurado():
        raise RuntimeError("DATABASE_URL não configurada")

    if psycopg is None:
        raise RuntimeError(
            "driver psycopg não instalado; confira requirements.txt"
        )

    # Supabase Transaction Pooler não suporta prepared statements.
    return psycopg.connect(
        database_url(),
        connect_timeout=5,
        prepare_threshold=None,
        options="-c statement_timeout=8000 -c lock_timeout=3000"
    )


def postgres_inicializar():
    if not postgres_configurado():
        return False

    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS double_rodadas (
                        id TEXT PRIMARY KEY,
                        data_hora TEXT NOT NULL UNIQUE,
                        momento TIMESTAMP WITHOUT TIME ZONE,
                        numero INTEGER,
                        cor CHAR(1) NOT NULL
                            CHECK (cor IN ('R','B','W')),
                        origem TEXT,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS
                    idx_double_rodadas_momento
                    ON double_rodadas (momento)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS double_demo_sessions (
                        demo_id TEXT PRIMARY KEY,
                        payload JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS double_meta (
                        chave TEXT PRIMARY KEY,
                        valor JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

        with LOCK:
            ESTADO["postgres_online"] = True
            ESTADO["ultimo_erro_postgres"] = ""
        return True

    except Exception as exc:
        with LOCK:
            ESTADO["postgres_online"] = False
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return False


def _momento_postgres(data_hora):
    try:
        return datetime.strptime(
            str(data_hora),
            "%d/%m/%Y %H:%M:%S"
        )
    except Exception:
        return None


def postgres_salvar_rodadas(rodadas):
    if not rodadas:
        return {
            "ok": True,
            "recebidas": 0,
            "gravadas_ou_existentes": 0
        }

    if not postgres_configurado():
        return {
            "ok": False,
            "erro": "DATABASE_URL não configurada",
            "recebidas": len(rodadas)
        }

    try:
        linhas = []

        for item in rodadas:
            if not isinstance(item, dict):
                continue

            identificador = str(item.get("id", "")).strip()
            data_hora = str(item.get("data_hora", "")).strip()
            cor = str(item.get("cor", "")).strip().upper()

            if not identificador or not data_hora:
                continue
            if cor not in ("R", "B", "W"):
                continue

            numero = item.get("numero")
            try:
                numero = int(numero) if numero is not None else None
            except Exception:
                numero = None

            linhas.append((
                identificador,
                data_hora,
                _momento_postgres(data_hora),
                numero,
                cor,
                str(item.get("origem", "")),
                json.dumps(item, ensure_ascii=False)
            ))

        if not linhas:
            return {
                "ok": True,
                "recebidas": len(rodadas),
                "gravadas_ou_existentes": 0
            }

        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO double_rodadas (
                        id,
                        data_hora,
                        momento,
                        numero,
                        cor,
                        origem,
                        payload
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT DO NOTHING
                """, linhas)

        limpeza = postgres_limitar_historico(
            LIMITE_HISTORICO
        )

        with LOCK:
            ESTADO["postgres_online"] = True
            ESTADO["ultimo_erro_postgres"] = ""
            ESTADO["ultima_sincronizacao_postgres"] = agora_brasilia()

        return {
            "ok": True,
            "recebidas": len(rodadas),
            "gravadas_ou_existentes": len(linhas),
            "limite_historico": LIMITE_HISTORICO,
            "removidas_antigas": int(
                limpeza.get("removidas", 0)
            )
        }

    except Exception as exc:
        with LOCK:
            ESTADO["postgres_online"] = False
            ESTADO["ultimo_erro_postgres"] = str(exc)

        return {
            "ok": False,
            "erro": str(exc),
            "recebidas": len(rodadas)
        }


def postgres_limitar_historico(limite=LIMITE_HISTORICO):
    limite = max(1, int(limite))

    if not postgres_configurado():
        return {
            "ok": False,
            "removidas": 0,
            "erro": "DATABASE_URL não configurada"
        }

    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM double_rodadas
                    WHERE id IN (
                        SELECT id
                        FROM double_rodadas
                        ORDER BY
                            momento DESC NULLS LAST,
                            created_at DESC
                        OFFSET %s
                    )
                """, (limite,))
                removidas = max(0, int(cur.rowcount or 0))

        return {
            "ok": True,
            "removidas": removidas,
            "limite": limite
        }

    except Exception as exc:
        return {
            "ok": False,
            "removidas": 0,
            "erro": str(exc)
        }


def postgres_carregar_rodadas(limite=50000):
    limite = max(1, min(int(limite), 50000))

    if not postgres_configurado():
        return []

    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT payload, created_at
                    FROM double_rodadas
                    ORDER BY momento DESC NULLS LAST, created_at DESC
                    LIMIT %s
                """, (limite,))
                linhas = cur.fetchall()

        rodadas = []

        for linha in linhas:
            payload = linha[0]
            created_at = linha[1]

            if isinstance(payload, dict):
                item = payload
            elif isinstance(payload, str):
                try:
                    item = json.loads(payload)
                except Exception:
                    continue
            else:
                continue

            if (
                isinstance(item, dict)
                and item.get("cor") in ("R", "B", "W")
            ):
                item = dict(item)

                if (
                    str(item.get("origem", "")) == "blaze_websocket"
                    and not item.get("recebido_epoch")
                    and created_at is not None
                ):
                    try:
                        item["recebido_epoch"] = created_at.timestamp()
                        item["recebido_em_brasilia"] = (
                            created_at.astimezone(
                                timezone(timedelta(hours=-3))
                            ).strftime("%d/%m/%Y %H:%M:%S")
                        )
                    except Exception:
                        pass

                rodadas.append(item)

        rodadas = ordenar_rodadas_canonicas(rodadas)

        with LOCK:
            ESTADO["postgres_online"] = True
            ESTADO["ultimo_erro_postgres"] = ""

        return rodadas

    except Exception as exc:
        with LOCK:
            ESTADO["postgres_online"] = False
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return []



def postgres_meta_obter(chave, default=None):
    if not postgres_configurado():
        return default
    try:
        postgres_inicializar()
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT valor FROM double_meta WHERE chave=%s", (str(chave),))
                row = cur.fetchone()
                return row[0] if row else default
    except Exception as exc:
        with LOCK:
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return default


def postgres_meta_definir(chave, valor):
    if not postgres_configurado():
        return False
    try:
        postgres_inicializar()
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO double_meta (chave, valor, updated_at)
                       VALUES (%s, %s::jsonb, NOW())
                       ON CONFLICT (chave) DO UPDATE SET valor=EXCLUDED.valor, updated_at=NOW()""",
                    (str(chave), json.dumps(valor, ensure_ascii=False))
                )
        return True
    except Exception as exc:
        with LOCK:
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return False


def bootstrap_permanente_inicializado():
    meta = postgres_meta_obter("bootstrap_1500_inicializado", None)
    if isinstance(meta, dict) and bool(meta.get("concluido", False)):
        return meta
    return None

def marcar_bootstrap_permanente(quantidade, motivo):
    payload = {
        "concluido": True,
        "quantidade_no_marco": int(quantidade),
        "marcado_em": agora_brasilia(),
        "motivo": str(motivo),
        "versao": "V54.8"
    }
    postgres_meta_definir("bootstrap_1500_inicializado", payload)
    return payload


def postgres_manter_desde(data_hora_corte):
    """Apaga do Postgres somente rodadas anteriores ao corte confirmado."""
    if not postgres_configurado():
        return {"ok": False, "removidas": 0, "erro": "DATABASE_URL não configurada"}

    momento_corte = _momento_postgres(data_hora_corte)
    if momento_corte is None:
        return {"ok": False, "removidas": 0, "erro": "corte inválido"}

    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM double_rodadas
                    WHERE momento < %s
                    """,
                    (momento_corte,)
                )
                removidas = max(0, int(cur.rowcount or 0))

        return {
            "ok": True,
            "removidas": removidas,
            "corte": data_hora_corte
        }
    except Exception as exc:
        return {
            "ok": False,
            "removidas": 0,
            "erro": str(exc),
            "corte": data_hora_corte
        }


def aplicar_corte_base_auditada_uma_vez():
    """
    Mantém somente a base real recente já auditada:
    24/08/2026 00:00:03 em diante.

    Não cria nenhuma rodada. Não altera número/cor/horário.
    Executa uma única vez por banco via double_meta.
    """
    meta = postgres_meta_obter(META_CORTE_BASE, None) if postgres_configurado() else None

    if isinstance(meta, dict) and bool(meta.get("concluido", False)):
        return meta

    corte_epoch = _epoch_brasilia(CORTE_BASE_AUDITADA)
    if corte_epoch <= 0:
        raise RuntimeError("CORTE_BASE_AUDITADA inválido")

    with LOCK:
        antes = len(ESTADO.get("rodadas", []))
        mantidas = []
        vistos_horario = set()

        for item in ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", []))):
            ep = momento_efetivo_epoch(item)
            if ep < corte_epoch:
                continue

            horario = str(item.get("data_hora", "")).strip()
            if horario and horario in vistos_horario:
                # Mesmo timestamp = mesma rodada; mantém só uma.
                continue

            if horario:
                vistos_horario.add(horario)

            mantidas.append(dict(item))

        ESTADO["rodadas"] = ordenar_rodadas_canonicas(mantidas)
        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)
        depois = len(ESTADO["rodadas"])

    pg = {"ok": False, "removidas": 0}
    if postgres_configurado():
        pg = postgres_manter_desde(CORTE_BASE_AUDITADA)

        # Regrava as mantidas para garantir persistência, sem duplicar.
        with LOCK:
            snapshot = list(ESTADO.get("rodadas", []))
        postgres_salvar_rodadas(snapshot)

        meta_payload = {
            "concluido": True,
            "corte": CORTE_BASE_AUDITADA,
            "antes_memoria": antes,
            "depois_memoria": depois,
            "removidas_postgres": int(pg.get("removidas", 0)),
            "executado_em": agora_brasilia(),
            "versao": "V58.0"
        }
        postgres_meta_definir(META_CORTE_BASE, meta_payload)
        return meta_payload

    return {
        "concluido": True,
        "corte": CORTE_BASE_AUDITADA,
        "antes_memoria": antes,
        "depois_memoria": depois,
        "executado_em": agora_brasilia(),
        "versao": "V58.0"
    }


def postgres_demo_salvar(demo_id, sessao):
    if not postgres_configurado():
        return False
    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO double_demo_sessions (demo_id, payload, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (demo_id)
                    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()
                """, (
                    str(demo_id),
                    json.dumps(sessao, ensure_ascii=False)
                ))
        return True
    except Exception as exc:
        with LOCK:
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return False


def postgres_demo_carregar_todos():
    if not postgres_configurado():
        return {}
    try:
        postgres_inicializar()
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT demo_id, payload FROM double_demo_sessions")
                linhas = cur.fetchall()
        out = {}
        for demo_id, payload in linhas:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    continue
            if isinstance(payload, dict):
                out[str(demo_id)] = payload
        return out
    except Exception as exc:
        with LOCK:
            ESTADO["ultimo_erro_postgres"] = str(exc)
        return {}


def postgres_demo_excluir(demo_id):
    if not postgres_configurado():
        return False
    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM double_demo_sessions WHERE demo_id = %s",
                    (str(demo_id),)
                )
        return True
    except Exception:
        return False


def postgres_status():
    resultado = {
        "ok": False,
        "configurado": postgres_configurado(),
        "driver_psycopg": postgres_driver_ok(),
        "online": False,
        "total_rodadas_postgres": 0,
        "total_rodadas_memoria": 0,
        "ultima_sincronizacao": "",
        "limite_historico": LIMITE_HISTORICO,
        "erro": ""
    }

    with LOCK:
        resultado["total_rodadas_memoria"] = len(
            ESTADO.get("rodadas", [])
        )
        resultado["ultima_sincronizacao"] = str(
            ESTADO.get(
                "ultima_sincronizacao_postgres",
                ""
            )
        )

    if not resultado["configurado"]:
        resultado["erro"] = "DATABASE_URL não configurada"
        return resultado

    if not resultado["driver_psycopg"]:
        resultado["erro"] = "psycopg não instalado"
        return resultado

    try:
        if not postgres_inicializar():
            with LOCK:
                resultado["erro"] = str(
                    ESTADO.get("ultimo_erro_postgres", "")
                )
            return resultado

        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM double_rodadas"
                )
                total = int(cur.fetchone()[0])

        resultado["ok"] = True
        resultado["online"] = True
        resultado["total_rodadas_postgres"] = total
        return resultado

    except Exception as exc:
        resultado["erro"] = str(exc)
        return resultado


def sincronizar_memoria_postgres():
    with LOCK:
        snapshot = list(ESTADO.get("rodadas", []))

    resultado = postgres_salvar_rodadas(snapshot)

    if resultado.get("ok"):
        resultado["total_memoria"] = len(snapshot)

    return resultado


def _epoch_brasilia(data_hora):
    try:
        dt = datetime.strptime(
            str(data_hora),
            "%d/%m/%Y %H:%M:%S"
        )
        return dt.replace(
            tzinfo=timezone(timedelta(hours=-3))
        ).timestamp()
    except Exception:
        return 0.0


def momento_efetivo_epoch(item):
    """Momento canônico da rodada.

    V56.6: a sequência histórica deve SEMPRE usar o horário real da rodada
    (data_hora). recebido_epoch é apenas telemetria de chegada do WebSocket e
    pode mudar após importações/redeploys; usá-lo para ordenar/validar fazia
    milhares de rodadas antigas parecerem uma lacuna única.
    """
    if not isinstance(item, dict):
        return 0.0

    momento_real = _epoch_brasilia(item.get("data_hora", ""))
    if momento_real > 0:
        return momento_real

    # Fallback somente para registros antigos sem data_hora parseável.
    try:
        return float(item.get("recebido_epoch", 0.0))
    except Exception:
        return 0.0


def ordenar_rodadas_canonicas(rodadas):
    validas = [
        item for item in rodadas
        if isinstance(item, dict)
        and item.get("cor") in ("R", "B", "W")
    ]

    validas.sort(
        key=lambda item: (
            momento_efetivo_epoch(item),
            str(item.get("id", ""))
        )
    )

    return validas[-LIMITE_HISTORICO:]


def diagnosticar_continuidade_historico(rodadas=None, limite_gap=None):
    """Separa o histórico em blocos contínuos sem atravessar lacunas temporais."""
    if limite_gap is None:
        limite_gap = LIMITE_LACUNA_HISTORICO_SEGUNDOS
    limite_gap = max(45, int(limite_gap))

    if rodadas is None:
        with LOCK:
            rodadas = list(ESTADO.get("rodadas", []))

    base = ordenar_rodadas_canonicas(list(rodadas or []))
    if not base:
        return {
            "total_bruto": 0,
            "total_validado": 0,
            "lacunas": 0,
            "limite_gap_segundos": limite_gap,
            "segmento_atual": [],
            "segmentos": []
        }

    segmentos = []
    atual = [base[0]]
    lacunas = []

    for anterior, corrente in zip(base, base[1:]):
        a = momento_efetivo_epoch(anterior)
        b = momento_efetivo_epoch(corrente)
        delta = b - a if a > 0 and b > 0 else 0.0

        if delta > float(limite_gap):
            segmentos.append(atual)
            lacunas.append({
                "de": str(anterior.get("data_hora", "")),
                "ate": str(corrente.get("data_hora", "")),
                "segundos": round(delta, 3)
            })
            atual = [corrente]
        else:
            atual.append(corrente)

    if atual:
        segmentos.append(atual)

    segmento_atual = segmentos[-1] if segmentos else []
    resumo_segmentos = []
    for bloco in segmentos[-20:]:
        resumo_segmentos.append({
            "quantidade": len(bloco),
            "inicio": str(bloco[0].get("data_hora", "")) if bloco else "",
            "fim": str(bloco[-1].get("data_hora", "")) if bloco else ""
        })

    return {
        "total_bruto": len(base),
        "total_validado": len(segmento_atual),
        "lacunas": len(lacunas),
        "limite_gap_segundos": limite_gap,
        "segmento_atual": segmento_atual,
        "segmentos": resumo_segmentos,
        "ultimas_lacunas": lacunas[-20:]
    }


def historico_validado(limite=30000):
    diag = diagnosticar_continuidade_historico()
    limite = max(1, min(int(limite), 30000))
    rodadas = list(diag.get("segmento_atual", []))[-limite:]
    return {
        "rodadas": rodadas,
        "quantidade": len(rodadas),
        "total_bruto": int(diag.get("total_bruto", 0)),
        "total_validado": int(diag.get("total_validado", 0)),
        "lacunas_detectadas": int(diag.get("lacunas", 0)),
        "limite_gap_segundos": int(diag.get("limite_gap_segundos", LIMITE_LACUNA_HISTORICO_SEGUNDOS)),
        "segmentos": diag.get("segmentos", []),
        "ultimas_lacunas": diag.get("ultimas_lacunas", []),
        "modo": "segmento_continuo_mais_recente",
        "ultima_atualizacao": ESTADO.get("ultima_atualizacao", "")
    }


def websocket_tempo_real_saudavel(max_sem_rolling=90.0):
    agora = time.time()

    with LOCK:
        online = bool(ESTADO.get("ws_online", False))
        handshake = bool(
            ESTADO.get("ws_handshake_recebido", False)
        )
        conectado = float(
            ESTADO.get("ws_conectado_epoch", 0.0)
        )
        ultimo_rolling = float(
            ESTADO.get("ws_ultimo_recebido_epoch", 0.0)
        )

    if not online or not handshake:
        return False

    if ultimo_rolling <= 0:
        return conectado > 0 and agora - conectado <= max_sem_rolling

    return agora - ultimo_rolling <= max_sem_rolling


def recalcular_sinal_inicial():
    try:
        with LOCK:
            base = ordenar_rodadas_canonicas(
                list(ESTADO.get("rodadas", []))
            )
            ultima = dict(base[-1]) if base else {}

        atualizar_sinal_e_notificar(ultima)

        with LOCK:
            ESTADO["sinal_recalculado_inicio"] = True
            ESTADO["sinal_erro_inicio"] = ""

    except Exception as exc:
        with LOCK:
            ESTADO["sinal_recalculado_inicio"] = False
            ESTADO["sinal_erro_inicio"] = str(exc)


def carregar_estado():
    """
    V58.10 — restauração NÃO DESTRUTIVA.

    Regra de segurança:
    - PostgreSQL é a fonte persistente principal quando DATABASE_URL existe.
    - Um deploy/restart nunca transforma falha temporária do PostgreSQL em
      "histórico vazio".
    - A memória/local e o PostgreSQL são MESCLADOS por identidade/horário;
      nenhuma base válida é substituída por uma lista vazia.
    - Esta função não apaga rodadas.
    """
    global ESTADO

    data = carregar_json(BANCO, ESTADO)
    if isinstance(data, dict):
        ESTADO = data

    ESTADO.setdefault("rodadas", [])
    ESTADO.setdefault("demo_contas", {})

    local_rodadas = ordenar_rodadas_canonicas(
        list(ESTADO.get("rodadas", []))
    )

    if postgres_configurado():
        try:
            postgres_inicializar()
            rodadas_pg = postgres_carregar_rodadas(LIMITE_HISTORICO)

            # Nunca substitui uma base existente por [].
            if rodadas_pg:
                mapa = {}
                for item in list(rodadas_pg) + list(local_rodadas):
                    if not isinstance(item, dict):
                        continue
                    rid = str(item.get("id", "")).strip()
                    horario = str(item.get("data_hora", "")).strip()
                    chave = ("id:" + rid) if rid else ("hora:" + horario)
                    if chave in ("id:", "hora:"):
                        continue
                    # O registro persistido no PostgreSQL vem primeiro.
                    if chave not in mapa:
                        mapa[chave] = dict(item)

                mescladas = ordenar_rodadas_canonicas(list(mapa.values()))
                if len(mescladas) > LIMITE_HISTORICO:
                    mescladas = mescladas[-LIMITE_HISTORICO:]

                with LOCK:
                    ESTADO["rodadas"] = mescladas
                    ESTADO["ultima_atualizacao"] = agora_brasilia()
                    salvar_json(BANCO, ESTADO)
            elif local_rodadas:
                # PostgreSQL vazio/indisponível não zera o histórico local.
                with LOCK:
                    ESTADO["rodadas"] = local_rodadas

            demos_pg = postgres_demo_carregar_todos()
            if demos_pg:
                with LOCK:
                    ESTADO["demo_contas"] = demos_pg

        except Exception as exc:
            # Boot continua com a última base local conhecida.
            with LOCK:
                ESTADO["rodadas"] = local_rodadas
                ESTADO["postgres_online"] = False
                ESTADO["ultimo_erro_postgres"] = str(exc)
    else:
        with LOCK:
            ESTADO["rodadas"] = local_rodadas


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
        base_ordenada = ordenar_rodadas_canonicas(
            list(ESTADO["rodadas"])
        )
        data = cores(base_ordenada)

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




def limite_atraso_fonte_servidor():
    try:
        return max(
            0.1,
            float(
                os.getenv(
                    "MAX_ATRASO_FONTE_SERVIDOR_SEGUNDOS",
                    "2.0"
                )
            )
        )
    except Exception:
        return 2.0


def limite_idade_sinal():
    try:
        return max(
            0.5,
            float(
                os.getenv(
                    "MAX_IDADE_SINAL_SEGUNDOS",
                    "5.0"
                )
            )
        )
    except Exception:
        return 5.0



def _streak_atual(data):
    if not data:
        return "", 0

    cor = data[-1]
    tamanho = 1

    for item in reversed(data[:-1]):
        if item != cor:
            break
        tamanho += 1

    return cor, tamanho


def _freq(data, janela):
    bloco = data[-max(1, int(janela)):]
    total = len(bloco)

    if total == 0:
        return {"R": 0.0, "B": 0.0, "W": 0.0}

    return {
        cor: sum(1 for x in bloco if x == cor) / total
        for cor in ("R", "B", "W")
    }


def _transicoes_1(data, origem):
    cont = {"R": 0, "B": 0, "W": 0}
    total = 0

    for i in range(len(data) - 1):
        if data[i] != origem:
            continue

        proxima = data[i + 1]

        if proxima in cont:
            cont[proxima] += 1
            total += 1

    if total == 0:
        return None

    return {
        cor: cont[cor] / total
        for cor in ("R", "B", "W")
    }


def _padrao_seguinte(data, tamanho=2):
    tamanho = max(1, int(tamanho))

    if len(data) < tamanho + 2:
        return None

    padrao = data[-tamanho:]
    cont = {"R": 0, "B": 0, "W": 0}
    total = 0

    for i in range(len(data) - tamanho):
        if data[i:i + tamanho] != padrao:
            continue

        idx = i + tamanho

        if idx >= len(data):
            continue

        cor = data[idx]

        if cor in cont:
            cont[cor] += 1
            total += 1

    if total < 4:
        return None

    return {
        "probs": {
            cor: cont[cor] / total
            for cor in ("R", "B", "W")
        },
        "amostras": total,
        "padrao": "".join(padrao)
    }


def detectar_regime_estrategico(data):
    """
    Detector puramente estatístico.
    'coleta' é apenas um apelido visual para regime anômalo;
    não afirma manipulação da plataforma.
    """
    if len(data) < 20:
        return {
            "regime": "indefinido",
            "apelido": "dados insuficientes",
            "score_anomalia": 0.0,
            "sequencia_cor": "",
            "sequencia_tamanho": 0,
            "concentracao_20": 0.0,
            "persistencia_20": 0.0
        }

    cor_seq, tam_seq = _streak_atual(data)
    f20 = _freq(data, 20)

    concentracao = max(
        f20.get("R", 0.0),
        f20.get("B", 0.0)
    )

    bloco = data[-20:]
    pares = max(1, len(bloco) - 1)
    iguais = sum(
        1
        for i in range(len(bloco) - 1)
        if bloco[i] == bloco[i + 1]
    )
    persistencia = iguais / pares

    score = 0.0

    # Sequência longa pesa progressivamente.
    if cor_seq in ("R", "B"):
        score += min(45.0, max(0, tam_seq - 2) * 9.0)

    # Concentração fora do equilíbrio recente.
    score += max(
        0.0,
        (concentracao - 0.55) * 100.0 * 1.3
    )

    # Persistência de cores adjacentes.
    score += max(
        0.0,
        (persistencia - 0.50) * 70.0
    )

    score = max(0.0, min(100.0, score))

    if score >= 70.0:
        regime = "anomalo"
        apelido = "modo coleta (rótulo estatístico)"
    elif score >= 40.0:
        regime = "instavel"
        apelido = "instável"
    else:
        regime = "normal"
        apelido = "normal"

    return {
        "regime": regime,
        "apelido": apelido,
        "score_anomalia": round(score, 2),
        "sequencia_cor": cor_seq,
        "sequencia_tamanho": int(tam_seq),
        "concentracao_20": round(concentracao, 4),
        "persistencia_20": round(persistencia, 4),
        "frequencias_20": f20
    }


def catalogo_estrategias():
    return [
        {
            "id": "seguir_sequencia",
            "nome": "Seguir sequência",
            "familia": "continuacao",
            "descricao": "Segue R/B quando há repetição recente."
        },
        {
            "id": "reverter_sequencia",
            "nome": "Reverter sequência",
            "familia": "reversao",
            "descricao": "Procura quebra após sequência R/B."
        },
        {
            "id": "transicao_1",
            "nome": "Transição de 1 cor",
            "familia": "transicao",
            "descricao": "Observa o que historicamente veio após a cor atual."
        },
        {
            "id": "padrao_2",
            "nome": "Padrão de 2 cores",
            "familia": "padrao",
            "descricao": "Compara o último padrão de 2 cores com ocorrências anteriores."
        },
        {
            "id": "vies_recente",
            "nome": "Viés recente",
            "familia": "tendencia",
            "descricao": "Segue predominância R/B da janela recente."
        },
        {
            "id": "reversao_desequilibrio",
            "nome": "Reversão de desequilíbrio",
            "familia": "reversao",
            "descricao": "Busca retorno após forte concentração de uma cor."
        }
    ]


def avaliar_estrategia(data, estrategia_id):
    if len(data) < 10:
        return {
            "id": estrategia_id,
            "voto": "",
            "confianca": 0.0,
            "motivo": "dados insuficientes"
        }

    cor_seq, tam_seq = _streak_atual(data)

    if estrategia_id == "seguir_sequencia":
        if cor_seq in ("R", "B") and tam_seq >= 3:
            return {
                "id": estrategia_id,
                "voto": cor_seq,
                "confianca": min(
                    0.82,
                    0.52 + 0.055 * tam_seq
                ),
                "motivo": "%d %s seguidos" % (
                    tam_seq,
                    cor_seq
                )
            }

    elif estrategia_id == "reverter_sequencia":
        if cor_seq in ("R", "B") and tam_seq >= 3:
            voto = "B" if cor_seq == "R" else "R"

            return {
                "id": estrategia_id,
                "voto": voto,
                "confianca": min(
                    0.74,
                    0.50 + 0.04 * tam_seq
                ),
                "motivo": "reversão após %d %s" % (
                    tam_seq,
                    cor_seq
                )
            }

    elif estrategia_id == "transicao_1":
        origem = data[-1]
        probs = _transicoes_1(data[-1000:], origem)

        if probs:
            voto = melhor_cor(probs)
            conf = float(probs.get(voto, 0.0))

            if conf >= 0.42:
                return {
                    "id": estrategia_id,
                    "voto": voto,
                    "confianca": conf,
                    "motivo": "transição após %s: %.1f%%" % (
                        origem,
                        conf * 100.0
                    )
                }

    elif estrategia_id == "padrao_2":
        pad = _padrao_seguinte(
            data[-1000:],
            2
        )

        if pad:
            voto = melhor_cor(
                pad["probs"]
            )
            conf = float(
                pad["probs"].get(voto, 0.0)
            )

            if conf >= 0.42:
                return {
                    "id": estrategia_id,
                    "voto": voto,
                    "confianca": conf,
                    "motivo": "padrão %s (%d amostras): %.1f%%" % (
                        pad["padrao"],
                        pad["amostras"],
                        conf * 100.0
                    )
                }

    elif estrategia_id == "vies_recente":
        f = _freq(data, 30)
        voto = "R" if f["R"] >= f["B"] else "B"
        conf = max(f["R"], f["B"])

        if conf >= 0.50:
            return {
                "id": estrategia_id,
                "voto": voto,
                "confianca": conf,
                "motivo": "janela 30: R %.1f%% / B %.1f%%" % (
                    f["R"] * 100.0,
                    f["B"] * 100.0
                )
            }

    elif estrategia_id == "reversao_desequilibrio":
        f = _freq(data, 20)

        if f["R"] >= 0.65:
            return {
                "id": estrategia_id,
                "voto": "B",
                "confianca": min(0.78, f["R"]),
                "motivo": "R concentrado em %.1f%% da janela 20" % (
                    f["R"] * 100.0
                )
            }

        if f["B"] >= 0.65:
            return {
                "id": estrategia_id,
                "voto": "R",
                "confianca": min(0.78, f["B"]),
                "motivo": "B concentrado em %.1f%% da janela 20" % (
                    f["B"] * 100.0
                )
            }

    return {
        "id": estrategia_id,
        "voto": "",
        "confianca": 0.0,
        "motivo": "sem condição"
    }


def motor_estrategias(data=None):
    if data is None:
        with LOCK:
            base = ordenar_rodadas_canonicas(
                list(ESTADO.get("rodadas", []))
            )
        data = cores(base)

    data = list(data or [])

    regime = detectar_regime_estrategico(
        data
    )

    catalogo = {
        item["id"]: item
        for item in catalogo_estrategias()
    }

    votos = []

    for estrategia_id in catalogo:
        resultado = avaliar_estrategia(
            data,
            estrategia_id
        )

        meta = catalogo[estrategia_id]
        resultado["nome"] = meta["nome"]
        resultado["familia"] = meta["familia"]

        # Em regime anômalo, continuidade recebe mais peso.
        peso_regime = 1.0

        if regime["regime"] == "anomalo":
            if meta["familia"] == "continuacao":
                peso_regime = 1.35
            elif meta["familia"] == "reversao":
                peso_regime = 0.75

        resultado["peso_regime"] = peso_regime

        if resultado.get("voto") in ("R", "B", "W"):
            resultado["peso_voto"] = (
                float(resultado.get("confianca", 0.0))
                * peso_regime
            )
        else:
            resultado["peso_voto"] = 0.0

        votos.append(resultado)

    placar = {"R": 0.0, "B": 0.0, "W": 0.0}

    for item in votos:
        voto = item.get("voto")

        if voto in placar:
            placar[voto] += float(
                item.get("peso_voto", 0.0)
            )

    total_peso = sum(placar.values())

    if total_peso > 0:
        candidato = max(
            placar,
            key=lambda c: placar[c]
        )
        confianca_consenso = (
            placar[candidato] / total_peso
        )
    else:
        candidato = ""
        confianca_consenso = 0.0

    ativos = [
        item
        for item in votos
        if item.get("voto")
    ]

    return {
        "ok": True,
        "modo": "shadow",
        "regime": regime,
        "candidato": candidato,
        "confianca_consenso": round(
            confianca_consenso,
            4
        ),
        "placar": {
            k: round(v, 4)
            for k, v in placar.items()
        },
        "estrategias_ativas": len(ativos),
        "estrategias_total": len(votos),
        "votos": votos,
        "nota": (
            "Shadow mode: este motor ainda não substitui "
            "o sinal principal."
        )
    }




def _entrada_id(base_id, cor_sinal, gerado_em):
    texto = "%s|%s|%s" % (str(base_id), str(cor_sinal), str(gerado_em))
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()[:20]


def registrar_entrada_sinal(sinal, rodada_base):
    if not isinstance(sinal, dict) or not bool(sinal.get("valido", False)):
        return None

    cor_sinal = str(sinal.get("cor", "")).upper()
    if cor_sinal not in ("R", "B"):
        return None

    base_id = str((rodada_base or {}).get("id", ""))
    gerado_em = str(sinal.get("gerado_em_brasilia", "") or agora_brasilia())

    entrada = {
        "id": _entrada_id(base_id, cor_sinal, gerado_em),
        "status": "PENDENTE",
        "gale": "PENDENTE",
        "estrategia_id": str(sinal.get("motor", "motor_principal")),
        "cor_sinal": cor_sinal,
        "rodada_base_id": base_id,
        "rodada_base_data_hora": str((rodada_base or {}).get("data_hora", "")),
        "rodada_resultado_id": "",
        "rodada_resultado_data_hora": "",
        "rodada_gale1_id": "",
        "rodada_gale1_data_hora": "",
        "horario_sinal": gerado_em,
        "horario_fechamento": "",
        "probabilidade": float(sinal.get("probabilidade", 0.0) or 0.0),
        "resultado_1": "",
        "resultado_2": ""
    }

    with LOCK:
        atual = dict(ESTADO.get("entrada_pendente", {}) or {})
        if atual and atual.get("status") == "PENDENTE":
            return atual

        historico = list(ESTADO.get("entradas_registradas", []) or [])
        historico.append(entrada)
        ESTADO["entradas_registradas"] = historico[-LIMITE_ENTRADAS_REGISTRADAS:]
        ESTADO["entrada_pendente"] = entrada
        salvar_json(BANCO, ESTADO)

    return entrada


def fechar_entrada_com_rodada(rodada_real):
    cor_real = _cor_real_rodada(rodada_real)
    if cor_real not in ("R", "B", "W"):
        return None

    with LOCK:
        entrada = dict(ESTADO.get("entrada_pendente", {}) or {})

    if not entrada or entrada.get("status") != "PENDENTE":
        return None

    rodada_id = str((rodada_real or {}).get("id", ""))
    if rodada_id and rodada_id == str(entrada.get("rodada_base_id", "")):
        return None

    cor_sinal = str(entrada.get("cor_sinal", ""))

    if not entrada.get("rodada_resultado_id"):
        entrada["rodada_resultado_id"] = rodada_id
        entrada["rodada_resultado_data_hora"] = str((rodada_real or {}).get("data_hora", ""))
        entrada["resultado_1"] = cor_real

        if cor_real == cor_sinal:
            entrada["status"] = "WIN"
            entrada["gale"] = "SEM_GALE"
            entrada["horario_fechamento"] = agora_brasilia()
        else:
            entrada["gale"] = "AGUARDANDO_GALE_1"
    else:
        if rodada_id and rodada_id == str(entrada.get("rodada_resultado_id", "")):
            return None

        entrada["rodada_gale1_id"] = rodada_id
        entrada["rodada_gale1_data_hora"] = str((rodada_real or {}).get("data_hora", ""))
        entrada["resultado_2"] = cor_real
        entrada["gale"] = "GALE_1"
        entrada["horario_fechamento"] = agora_brasilia()
        entrada["status"] = "WIN" if cor_real == cor_sinal else "LOSE"

    with LOCK:
        historico = list(ESTADO.get("entradas_registradas", []) or [])
        for i in range(len(historico) - 1, -1, -1):
            if str(historico[i].get("id", "")) == str(entrada.get("id", "")):
                historico[i] = dict(entrada)
                break

        ESTADO["entradas_registradas"] = historico

        if entrada.get("status") in ("WIN", "LOSE"):
            ESTADO["entrada_pendente"] = {}
            if entrada["status"] == "WIN":
                ESTADO["contador_win"] = int(ESTADO.get("contador_win", 0)) + 1
            else:
                ESTADO["contador_lose"] = int(ESTADO.get("contador_lose", 0)) + 1

            if entrada.get("gale") == "SEM_GALE":
                ESTADO["contador_sem_gale"] = int(ESTADO.get("contador_sem_gale", 0)) + 1
            elif entrada.get("gale") == "GALE_1":
                ESTADO["contador_gale_1"] = int(ESTADO.get("contador_gale_1", 0)) + 1
        else:
            ESTADO["entrada_pendente"] = dict(entrada)

        salvar_json(BANCO, ESTADO)

    return entrada


def status_entradas():
    with LOCK:
        historico = list(ESTADO.get("entradas_registradas", []) or [])
        pendente = dict(ESTADO.get("entrada_pendente", {}) or {})
        wins = int(ESTADO.get("contador_win", 0))
        loses = int(ESTADO.get("contador_lose", 0))
        sem_gale = int(ESTADO.get("contador_sem_gale", 0))
        gale1 = int(ESTADO.get("contador_gale_1", 0))

    total = wins + loses
    return {
        "ok": True,
        "wins": wins,
        "loses": loses,
        "taxa_win": round((100.0 * wins / total) if total else 0.0, 2),
        "wins_sem_gale": sem_gale,
        "entradas_com_gale_1": gale1,
        "entrada_pendente": pendente,
        "historico_total": len(historico),
        "ultimas_50": list(reversed(historico[-50:]))
    }


def verificar_continuidade_captura():
    agora = time.time()
    with LOCK:
        ultimo = float(ESTADO.get("ws_ultimo_recebido_epoch", 0.0) or 0.0)
        online = bool(ESTADO.get("ws_online", False))

    idade = agora - ultimo if ultimo > 0 else None
    interrompida = (not online or idade is None or idade > LIMITE_SEM_RESULTADO_SEGUNDOS)

    with LOCK:
        anterior = bool(ESTADO.get("captura_interrompida", False))
        ESTADO["captura_interrompida"] = interrompida
        ESTADO["ultima_verificacao_continuidade"] = agora_brasilia()

        if interrompida and not anterior:
            ESTADO["captura_interrompida_desde"] = agora_brasilia()
        elif not interrompida:
            ESTADO["captura_interrompida_desde"] = ""

    return {
        "ok": not interrompida,
        "interrompida": interrompida,
        "idade_ultimo_resultado_segundos": round(idade, 3) if idade is not None else None,
        "limite_segundos": LIMITE_SEM_RESULTADO_SEGUNDOS,
        "ws_online": online
    }


def worker_continuidade():
    while True:
        try:
            verificar_continuidade_captura()
        except Exception:
            pass
        time.sleep(5)


def _cor_real_rodada(rodada):
    cor = str((rodada or {}).get("cor", "")).upper()

    if cor in ("R", "B", "W"):
        return cor

    try:
        numero = int((rodada or {}).get("numero"))
    except Exception:
        return ""

    return cor_numero(numero)


def _estatistica_shadow(avaliacoes, filtro_estrategia=None, regime=None):
    registros = []

    for item in avaliacoes:
        if not isinstance(item, dict):
            continue

        if regime and str(item.get("regime", "")) != regime:
            continue

        if filtro_estrategia:
            for voto in item.get("votos", []) or []:
                if (
                    str(voto.get("id", "")) == filtro_estrategia
                    and voto.get("voto") in ("R", "B")
                ):
                    registros.append({
                        "acertou": bool(voto.get("acertou", False)),
                        "voto": voto.get("voto"),
                        "real": item.get("resultado_real")
                    })
        else:
            if item.get("candidato") in ("R", "B"):
                registros.append({
                    "acertou": bool(item.get("acertou", False)),
                    "voto": item.get("candidato"),
                    "real": item.get("resultado_real")
                })

    entradas = len(registros)
    acertos = sum(1 for x in registros if x["acertou"])
    erros = entradas - acertos
    taxa = (100.0 * acertos / entradas) if entradas else 0.0

    saldo = 0.0
    pico = 0.0
    max_dd = 0.0

    for x in registros:
        saldo += 1.0 if x["acertou"] else -1.0
        pico = max(pico, saldo)
        max_dd = max(max_dd, pico - saldo)

    return {
        "entradas": entradas,
        "acertos": acertos,
        "erros": erros,
        "taxa_acerto": round(taxa, 2),
        "resultado_unidades": round(saldo, 2),
        "max_drawdown_unidades": round(max_dd, 2)
    }


def avaliar_shadow_com_resultado(rodada_real):
    """
    Fecha a previsão pendente usando SOMENTE a rodada seguinte recebida.
    Depois a previsão é removida da pendência.
    """
    cor_real = _cor_real_rodada(rodada_real)

    if cor_real not in ("R", "B", "W"):
        return None

    with LOCK:
        pendente = dict(
            ESTADO.get("shadow_previsao_pendente", {}) or {}
        )

    if not pendente:
        return None

    base_id = str(pendente.get("base_rodada_id", ""))
    rodada_id = str((rodada_real or {}).get("id", ""))

    # Nunca avalia contra a mesma rodada usada como base.
    if base_id and rodada_id and base_id == rodada_id:
        return None

    candidato = str(pendente.get("candidato", ""))
    votos_resultado = []

    for voto in pendente.get("votos", []) or []:
        if not isinstance(voto, dict):
            continue

        palpite = str(voto.get("voto", ""))

        if palpite not in ("R", "B"):
            continue

        copia = dict(voto)
        copia["acertou"] = palpite == cor_real
        votos_resultado.append(copia)

    avaliacao = {
        "base_rodada_id": base_id,
        "base_data_hora": str(
            pendente.get("base_data_hora", "")
        ),
        "gerado_em": str(
            pendente.get("gerado_em", "")
        ),
        "regime": str(
            pendente.get("regime", "")
        ),
        "score_anomalia": float(
            pendente.get("score_anomalia", 0.0) or 0.0
        ),
        "candidato": candidato,
        "consenso": float(
            pendente.get("consenso", 0.0) or 0.0
        ),
        "resultado_rodada_id": rodada_id,
        "resultado_data_hora": str(
            (rodada_real or {}).get("data_hora", "")
        ),
        "resultado_real": cor_real,
        "acertou": (
            candidato in ("R", "B")
            and candidato == cor_real
        ),
        "votos": votos_resultado,
        "avaliado_em": agora_brasilia()
    }

    with LOCK:
        historico = list(
            ESTADO.get("shadow_avaliacoes", []) or []
        )
        historico.append(avaliacao)
        historico = historico[-LIMITE_AVALIACOES_SHADOW:]

        ESTADO["shadow_avaliacoes"] = historico
        ESTADO["shadow_total_avaliado"] = int(
            ESTADO.get("shadow_total_avaliado", 0)
        ) + 1
        ESTADO["shadow_ultima_avaliacao"] = agora_brasilia()
        ESTADO["shadow_previsao_pendente"] = {}

        salvar_json(BANCO, ESTADO)

    return avaliacao


def criar_previsao_shadow(rodada_base):
    """
    Cria a previsão que será julgada somente pela próxima rodada real.
    Não executa aposta e não interfere no sinal principal.
    """
    with LOCK:
        base = ordenar_rodadas_canonicas(
            list(ESTADO.get("rodadas", []))
        )

    motor = motor_estrategias(
        cores(base)
    )

    votos = []

    for item in motor.get("votos", []) or []:
        voto = str(item.get("voto", ""))

        if voto not in ("R", "B"):
            continue

        votos.append({
            "id": str(item.get("id", "")),
            "nome": str(item.get("nome", "")),
            "familia": str(item.get("familia", "")),
            "voto": voto,
            "confianca": float(
                item.get("confianca", 0.0) or 0.0
            ),
            "peso_regime": float(
                item.get("peso_regime", 1.0) or 1.0
            )
        })

    regime = motor.get("regime", {}) or {}

    previsao = {
        "base_rodada_id": str(
            (rodada_base or {}).get("id", "")
        ),
        "base_data_hora": str(
            (rodada_base or {}).get("data_hora", "")
        ),
        "gerado_em": agora_brasilia(),
        "regime": str(
            regime.get("regime", "")
        ),
        "score_anomalia": float(
            regime.get("score_anomalia", 0.0) or 0.0
        ),
        "candidato": str(
            motor.get("candidato", "")
        ),
        "consenso": float(
            motor.get("confianca_consenso", 0.0) or 0.0
        ),
        "estrategias_ativas": int(
            motor.get("estrategias_ativas", 0)
        ),
        "votos": votos
    }

    with LOCK:
        ESTADO["shadow_previsao_pendente"] = previsao
        salvar_json(BANCO, ESTADO)

    return previsao


def atualizar_avaliador_shadow(rodada_real):
    """
    Ordem correta:
    1) fecha a previsão anterior com o resultado recém-chegado;
    2) cria uma nova previsão para a PRÓXIMA rodada.
    """
    avaliacao = avaliar_shadow_com_resultado(
        rodada_real
    )
    previsao = criar_previsao_shadow(
        rodada_real
    )

    return {
        "avaliacao_fechada": avaliacao,
        "nova_previsao": previsao
    }


def status_avaliador_shadow():
    with LOCK:
        historico = list(
            ESTADO.get("shadow_avaliacoes", []) or []
        )
        pendente = dict(
            ESTADO.get("shadow_previsao_pendente", {}) or {}
        )
        total = int(
            ESTADO.get("shadow_total_avaliado", 0)
        )
        ultima = str(
            ESTADO.get("shadow_ultima_avaliacao", "")
        )

    por_estrategia = []

    for meta in catalogo_estrategias():
        geral = _estatistica_shadow(
            historico,
            filtro_estrategia=meta["id"]
        )

        recente = _estatistica_shadow(
            historico[-50:],
            filtro_estrategia=meta["id"]
        )

        por_regime = {
            r: _estatistica_shadow(
                historico,
                filtro_estrategia=meta["id"],
                regime=r
            )
            for r in (
                "normal",
                "instavel",
                "anomalo"
            )
        }

        score = (
            geral["resultado_unidades"]
            - 0.35 * geral["max_drawdown_unidades"]
            + min(8.0, geral["entradas"] / 20.0)
        )

        por_estrategia.append({
            "id": meta["id"],
            "nome": meta["nome"],
            "familia": meta["familia"],
            "geral": geral,
            "ultimas_50": recente,
            "por_regime": por_regime,
            "score_shadow": round(score, 3)
        })

    por_estrategia.sort(
        key=lambda x: x["score_shadow"],
        reverse=True
    )

    candidato_geral = _estatistica_shadow(
        historico
    )
    candidato_50 = _estatistica_shadow(
        historico[-50:]
    )

    return {
        "ok": True,
        "modo": "shadow_live",
        "previsao_pendente": pendente,
        "total_avaliado_sessao_persistido": total,
        "ultima_avaliacao": ultima,
        "historico_guardado": len(historico),
        "limite_historico": LIMITE_AVALIACOES_SHADOW,
        "candidato_ensemble_geral": candidato_geral,
        "candidato_ensemble_ultimas_50": candidato_50,
        "ranking_estrategias": por_estrategia,
        "ultimas_20_avaliacoes": list(
            reversed(historico[-20:])
        ),
        "nota": (
            "Avaliação shadow: previsão é registrada antes "
            "e julgada somente pela rodada seguinte."
        )
    }


def backtest_estrategia(
    estrategia_id,
    limite=1000
):
    limite = max(
        100,
        min(int(limite), 30000)
    )

    with LOCK:
        base = ordenar_rodadas_canonicas(
            list(ESTADO.get("rodadas", []))
        )

    data = cores(base[-limite:])

    entradas = 0
    acertos = 0
    erros = 0
    lucro_unidades = 0.0
    max_drawdown = 0.0
    pico = 0.0
    saldo = 0.0

    for i in range(30, len(data)):
        prefixo = data[:i]
        real = data[i]

        r = avaliar_estrategia(
            prefixo,
            estrategia_id
        )
        voto = r.get("voto", "")

        if voto not in ("R", "B"):
            continue

        entradas += 1

        if voto == real:
            acertos += 1
            saldo += 1.0
        else:
            erros += 1
            saldo -= 1.0

        lucro_unidades = saldo
        pico = max(pico, saldo)
        dd = pico - saldo
        max_drawdown = max(
            max_drawdown,
            dd
        )

    taxa = (
        100.0 * acertos / entradas
        if entradas
        else 0.0
    )

    return {
        "estrategia_id": estrategia_id,
        "rodadas_avaliadas": len(data),
        "entradas": entradas,
        "acertos": acertos,
        "erros": erros,
        "taxa_acerto": round(taxa, 2),
        "resultado_unidades": round(
            lucro_unidades,
            2
        ),
        "max_drawdown_unidades": round(
            max_drawdown,
            2
        ),
        "assuncao_simulacao": (
            "+1 unidade por acerto e -1 por erro; "
            "não representa retorno financeiro garantido."
        )
    }


def laboratorio_estrategias(limite=1000):
    """
    Primeira versão do laboratório automático.
    Compara estratégias existentes por resultado simulado e drawdown.
    Futuras versões podem gerar parâmetros novos.
    """
    resultados = []

    for item in catalogo_estrategias():
        r = backtest_estrategia(
            item["id"],
            limite
        )
        r["nome"] = item["nome"]
        r["familia"] = item["familia"]

        # Score penaliza baixo número de entradas e drawdown.
        r["score_laboratorio"] = round(
            r["resultado_unidades"]
            - 0.35 * r["max_drawdown_unidades"]
            + min(10.0, r["entradas"] / 50.0),
            3
        )

        resultados.append(r)

    resultados.sort(
        key=lambda x: x["score_laboratorio"],
        reverse=True
    )

    return {
        "ok": True,
        "limite": limite,
        "ranking": resultados,
        "nota": (
            "Ranking exploratório. Estratégia não deve ser promovida "
            "automaticamente só por lucro passado."
        )
    }


def calcular_sinal_tempo_real():
    """
    Caminho rápido para a próxima rodada AO VIVO.

    Diferença para calcular_sinal():
    - NÃO executa busca adaptativa walk-forward completa a cada rodada.
    - Usa os limites já configurados no servidor.
    - Mantém ensemble, padrão, concordância e estabilidade.
    - Trabalha no máximo com a janela longa configurada.

    Isso evita bloquear o sinal por dezenas de segundos/minutos.
    """
    with LOCK:
        base_ordenada = ordenar_rodadas_canonicas(
            list(ESTADO.get("rodadas", []))
        )

    cfg = carregar_config()

    janela_longa = max(
        30,
        min(
            int(cfg.get("janela_longa", 1000)),
            LIMITE_HISTORICO
        )
    )

    if len(base_ordenada) > janela_longa:
        base_ordenada = base_ordenada[-janela_longa:]

    data = cores(base_ordenada)

    if len(data) < 30:
        return {"valido": False}

    probs = probabilidade_ensemble(data, cfg)
    escolha = melhor_cor(probs)
    amostras = contar_amostras_ultimo_padrao(data)

    detalhes_concordancia = concordancia_modelos(data, cfg)
    total_modelos = int(
        detalhes_concordancia.get("total_modelos", 0)
    )

    concordancia_minima = min(
        int(cfg.get("concordancia_minima", 2)),
        total_modelos
    ) if total_modelos > 0 else 0

    concordancia_ok = (
        int(detalhes_concordancia.get("acordos", 0))
        >= concordancia_minima
    )

    estabilidade_atual = estabilidade_configuracao_servidor()
    estabilidade_minima = float(
        cfg.get("estabilidade_minima", 50.0)
    )
    estabilidade_ok = (
        estabilidade_atual >= estabilidade_minima
    )

    limite = float(cfg.get("sinal_minimo", 0.60))
    amostras_minimas = int(
        cfg.get("amostras_minimas", 20)
    )

    prob_atual = float(probs.get(escolha, 0.0))

    filtros = {
        "probabilidade_ok": prob_atual >= limite,
        "amostras_ok": amostras >= amostras_minimas,
        "concordancia_ok": bool(concordancia_ok),
        "estabilidade_ok": bool(estabilidade_ok)
    }

    motivos_rejeicao = []

    if not filtros["probabilidade_ok"]:
        motivos_rejeicao.append(
            "probabilidade %.1f%% < mínimo %.1f%%"
            % (prob_atual * 100.0, limite * 100.0)
        )

    if not filtros["amostras_ok"]:
        motivos_rejeicao.append(
            "amostras %d < mínimo %d"
            % (amostras, amostras_minimas)
        )

    if not filtros["concordancia_ok"]:
        motivos_rejeicao.append(
            "concordância %d/%d abaixo do mínimo %d"
            % (
                int(detalhes_concordancia.get("acordos", 0)),
                total_modelos,
                concordancia_minima
            )
        )

    if not filtros["estabilidade_ok"]:
        motivos_rejeicao.append(
            "estabilidade %.1f%% < mínimo %.1f%%"
            % (estabilidade_atual, estabilidade_minima)
        )

    valido = all(filtros.values())

    return {
        "valido": bool(valido),
        "cor": escolha,
        "probabilidade": prob_atual,
        "amostras": int(amostras),
        "configuracao": (
            "tempo-real %.0f%% / %d amostras"
            % (limite * 100.0, amostras_minimas)
        ),
        "taxa_historica_configuracao": 0.0,
        "entradas_avaliadas": 0,
        "concordancia_modelos": int(
            detalhes_concordancia.get("acordos", 0)
        ),
        "total_modelos": total_modelos,
        "concordancia_minima": concordancia_minima,
        "estabilidade_atual": float(
            estabilidade_atual
        ),
        "estabilidade_minima": float(
            estabilidade_minima
        ),
        "data_hora_brasilia": agora_brasilia(),
        "motor": "tempo_real_rapido",
        "filtros": filtros,
        "motivos_rejeicao": motivos_rejeicao
    }


def atualizar_sinal_tempo_real(rodada_base=None):
    """
    Atualiza somente o sinal ao vivo usando o caminho rápido.
    Preserva a mesma estrutura/metadados da V53.2.
    """
    inicio = time.perf_counter()
    sinal = calcular_sinal_tempo_real()
    calculo_ms = (
        time.perf_counter() - inicio
    ) * 1000.0

    rodada_base = (
        rodada_base
        if isinstance(rodada_base, dict)
        else {}
    )

    base_id = str(
        rodada_base.get("id", "")
    )
    base_data_hora = str(
        rodada_base.get("data_hora", "")
    )
    gerado_em = agora_brasilia()

    sinal = dict(
        sinal or {"valido": False}
    )

    recebido_epoch = 0.0
    try:
        recebido_epoch = float(
            rodada_base.get("recebido_epoch", 0.0) or 0.0
        )
    except Exception:
        recebido_epoch = 0.0

    fonte_epoch = 0.0
    try:
        fonte_dt = _parse_iso_utc_para_datetime(
            rodada_base.get("timestamp_fonte", "")
        )
        fonte_epoch = (
            fonte_dt.timestamp()
            if fonte_dt is not None
            else 0.0
        )
    except Exception:
        fonte_epoch = 0.0

    atraso_fonte_servidor = None
    if recebido_epoch > 0 and fonte_epoch > 0:
        atraso_fonte_servidor = max(
            0.0,
            recebido_epoch - fonte_epoch
        )

    idade_total_sinal = None
    if recebido_epoch > 0:
        idade_total_sinal = max(
            0.0,
            time.time() - recebido_epoch
        )

    max_atraso = limite_atraso_fonte_servidor()
    max_idade = limite_idade_sinal()

    frescor_ok = True
    motivos_latencia = []

    if (
        atraso_fonte_servidor is not None
        and atraso_fonte_servidor > max_atraso
    ):
        frescor_ok = False
        motivos_latencia.append(
            "atraso fonte→servidor %.3fs > limite %.3fs"
            % (
                atraso_fonte_servidor,
                max_atraso
            )
        )

    if (
        idade_total_sinal is not None
        and idade_total_sinal > max_idade
    ):
        frescor_ok = False
        motivos_latencia.append(
            "sinal ficou velho: %.3fs > limite %.3fs"
            % (
                idade_total_sinal,
                max_idade
            )
        )

    if not frescor_ok:
        sinal["valido"] = False

    motivos_existentes = list(
        sinal.get("motivos_rejeicao", []) or []
    )
    motivos_existentes.extend(motivos_latencia)

    sinal["motivos_rejeicao"] = motivos_existentes
    sinal["frescor_ok"] = bool(frescor_ok)
    sinal["atraso_fonte_servidor_segundos"] = (
        round(atraso_fonte_servidor, 3)
        if atraso_fonte_servidor is not None
        else None
    )
    sinal["idade_total_sinal_segundos"] = (
        round(idade_total_sinal, 3)
        if idade_total_sinal is not None
        else None
    )
    sinal["limite_atraso_fonte_servidor_segundos"] = max_atraso
    sinal["limite_idade_sinal_segundos"] = max_idade

    sinal["base_rodada_id"] = base_id
    sinal["base_data_hora"] = base_data_hora
    sinal["gerado_em_brasilia"] = gerado_em
    sinal["calculo_ms"] = round(
        calculo_ms,
        3
    )

    registrar_sinal(sinal)

    with LOCK:
        anterior = dict(
            ESTADO.get(
                "ultimo_sinal",
                {"valido": False}
            )
        )

        ESTADO["ultimo_sinal"] = sinal
        ESTADO["sinal_base_rodada_id"] = base_id
        ESTADO["sinal_base_data_hora"] = base_data_hora
        ESTADO["sinal_gerado_em"] = gerado_em
        ESTADO["sinal_calculo_ms"] = round(
            calculo_ms,
            3
        )
        ESTADO["sinal_seq"] = int(
            ESTADO.get("sinal_seq", 0)
        ) + 1
        ESTADO["ultima_atualizacao"] = gerado_em
        salvar_json(BANCO, ESTADO)

    virou_novo_sinal = (
        sinal.get("valido", False)
        and (
            not anterior.get("valido", False)
            or anterior.get("cor") != sinal.get("cor")
            or anterior.get(
                "base_rodada_id"
            ) != base_id
        )
    )

    cfg = carregar_config()

    if (
        virou_novo_sinal
        and bool(
            cfg.get(
                "geracao_automatica",
                True
            )
        )
        and pode_notificar_agora()
    ):
        if enviar_ntfy(sinal):
            registrar_notificacao_enviada()


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


def atualizar_sinal_e_notificar(rodada_base=None):
    """
    Calcula o sinal depois de uma rodada nova e grava exatamente
    QUAL rodada serviu de base. Não altera o WebSocket.
    """
    inicio = time.perf_counter()
    sinal = calcular_sinal()
    calculo_ms = (time.perf_counter() - inicio) * 1000.0

    rodada_base = rodada_base if isinstance(rodada_base, dict) else {}

    base_id = str(rodada_base.get("id", ""))
    base_data_hora = str(rodada_base.get("data_hora", ""))
    gerado_em = agora_brasilia()

    sinal = dict(sinal or {"valido": False})
    sinal["base_rodada_id"] = base_id
    sinal["base_data_hora"] = base_data_hora
    sinal["gerado_em_brasilia"] = gerado_em
    sinal["calculo_ms"] = round(calculo_ms, 3)

    registrar_sinal(sinal)

    with LOCK:
        anterior = dict(
            ESTADO.get("ultimo_sinal", {"valido": False})
        )

        ESTADO["ultimo_sinal"] = sinal
        ESTADO["sinal_base_rodada_id"] = base_id
        ESTADO["sinal_base_data_hora"] = base_data_hora
        ESTADO["sinal_gerado_em"] = gerado_em
        ESTADO["sinal_calculo_ms"] = round(calculo_ms, 3)
        ESTADO["sinal_seq"] = int(
            ESTADO.get("sinal_seq", 0)
        ) + 1
        ESTADO["ultima_atualizacao"] = gerado_em
        salvar_json(BANCO, ESTADO)

    virou_novo_sinal = (
        sinal.get("valido", False)
        and (
            not anterior.get("valido", False)
            or anterior.get("cor") != sinal.get("cor")
            or anterior.get("base_rodada_id") != base_id
        )
    )

    cfg = carregar_config()
    geracao_automatica = bool(
        cfg.get("geracao_automatica", True)
    )

    if (
        virou_novo_sinal
        and geracao_automatica
        and pode_notificar_agora()
    ):
        if enviar_ntfy(sinal):
            registrar_notificacao_enviada()



def solicitar_recalculo_sinal(rodada_base):
    global ANALISE_PENDENTE_RODADA

    rodada_base = dict(rodada_base or {})

    with ANALISE_PENDENTE_LOCK:
        ANALISE_PENDENTE_RODADA = rodada_base

    with LOCK:
        ESTADO["analise_pendente_id"] = str(
            rodada_base.get("id", "")
        )

    ANALISE_EVENT.set()


def worker_analise_sinal():
    global ANALISE_PENDENTE_RODADA

    while True:
        ANALISE_EVENT.wait()
        ANALISE_EVENT.clear()

        with ANALISE_PENDENTE_LOCK:
            rodada_base = (
                dict(ANALISE_PENDENTE_RODADA)
                if isinstance(ANALISE_PENDENTE_RODADA, dict)
                else {}
            )
            ANALISE_PENDENTE_RODADA = None

        if not rodada_base:
            continue

        with LOCK:
            ESTADO["analise_em_andamento"] = True
            ESTADO["analise_iniciada_em"] = agora_brasilia()

        try:
            fechar_entrada_com_rodada(rodada_base)
            atualizar_sinal_tempo_real(rodada_base)
            atualizar_avaliador_shadow(rodada_base)

            with LOCK:
                sinal_atual = dict(ESTADO.get("ultimo_sinal", {}) or {})

            registrar_entrada_sinal(sinal_atual, rodada_base)
        except Exception as exc:
            with LOCK:
                ESTADO["sinal_erro_inicio"] = str(exc)
        finally:
            with LOCK:
                ESTADO["analise_em_andamento"] = False



def _demo_cor_nome(cor):
    return {"R": "VERMELHO", "B": "PRETO", "W": "BRANCO"}.get(str(cor), "")


def _demo_padrao_bate(data, pattern):
    if not isinstance(pattern, list) or not pattern:
        return False
    if len(data) < len(pattern):
        return False
    return list(data[-len(pattern):]) == [str(x) for x in pattern]


def _demo_sinal_estrategia(strategy, previous_colors):
    if not isinstance(strategy, dict):
        return ""

    strategy_type = str(strategy.get("type", "single"))

    if strategy_type == "white":
        data = [str(x) for x in (previous_colors or [])]
        # Distância desde o último branco, medida exatamente como no app:
        # 0 significa que a rodada imediatamente anterior foi branca.
        gap = -1
        for pos in range(len(data) - 1, -1, -1):
            if data[pos] == "W":
                gap = len(data) - 1 - pos
                break
        if gap < 0:
            return ""

        min_gap = int(strategy.get("min_gap", 0))
        max_gap = int(strategy.get("max_gap", 999999))
        if gap < min_gap or gap > max_gap:
            return ""

        plen = int(strategy.get("pattern_len", 0))
        pattern = strategy.get("pattern", [])
        if plen > 0:
            if not isinstance(pattern, list) or len(pattern) != plen:
                return ""
            if len(data) < plen:
                return ""
            tail = data[-plen:]
            if any(c not in ("R", "B") for c in tail):
                return ""
            if tail != [str(x) for x in pattern]:
                return ""
        return "W"

    if strategy_type == "ensemble":
        components = strategy.get("components", [])
        if not isinstance(components, list):
            return ""
        votes = {"R": 0, "B": 0}
        for comp in components:
            if not isinstance(comp, dict):
                continue
            pattern = comp.get("pattern", [])
            if _demo_padrao_bate(previous_colors, pattern):
                target = str(comp.get("entry", "")).upper()
                if target in votes:
                    votes[target] += 1
        if votes["R"] > votes["B"]:
            return "R"
        if votes["B"] > votes["R"]:
            return "B"
        return ""

    pattern = strategy.get("pattern", [])
    if _demo_padrao_bate(previous_colors, pattern):
        target = str(strategy.get("entry", "")).upper()
        return target if target in ("R", "B", "W") else ""
    return ""


def _demo_risk_reason(session):
    strategy = dict(session.get("strategy", {}) or {})
    bank = float(session.get("bank", 0.0))
    peak = float(session.get("peak_bank", bank))
    initial = float(session.get("initial_bank", bank))
    recent = list(session.get("recent_results", []) or [])

    if bool(strategy.get("risk_stop_win_enabled", False)):
        alvo = float(strategy.get("risk_stop_win_value", 0.0))
        if alvo > 0 and bank >= initial + alvo:
            return "STOP WIN • LUCRO R$ %.2f" % (bank - initial)

    if bool(strategy.get("risk_trailing_enabled", False)) and peak > 0:
        pct = float(strategy.get("risk_trailing_pct", 15.0))
        queda = 100.0 * (peak - bank) / peak
        if queda >= pct:
            return "TRAILING STOP %.2f%%" % queda

    if bool(strategy.get("risk_capital_enabled", False)):
        piso = float(strategy.get("risk_capital_floor", initial))
        if bank <= piso:
            return "PROTEÇÃO DE CAPITAL"

    if bool(strategy.get("risk_performance_enabled", False)):
        janela = max(1, int(strategy.get("risk_performance_window", 20)))
        minimo = float(strategy.get("risk_performance_min_rate", 50.0))
        if len(recent) >= janela:
            bloco = recent[-janela:]
            taxa = 100.0 * sum(1 for x in bloco if bool(x)) / janela
            if taxa < minimo:
                return "STOP POR DESEMPENHO %.2f%%" % taxa

    return ""


def _demo_operacao(session, rodada, kind, target, stake, gale, reason=""):
    ops = list(session.get("operations", []) or [])
    original = str(session.get("last_original_target", ""))
    executed = str(session.get("last_executed_target", target))
    contrary = bool(session.get("last_contrary_applied", False))
    ops.insert(0, {
        "data_hora": str((rodada or {}).get("data_hora", agora_brasilia())),
        "rodada_id": str((rodada or {}).get("id", "")),
        "tipo": str(kind),
        "alvo": str(target),
        "resultado": str((rodada or {}).get("cor", "")),
        "stake": round(float(stake), 2),
        "gale": int(gale),
        "banca": round(float(session.get("bank", 0.0)), 2),
        "motivo": str(reason),
        # V58.4: auditoria inequívoca da inversão em cada aposta.
        "sinal_original": original,
        "sinal_executado": executed,
        "sinal_contrario_ativo": contrary
    })
    session["operations"] = ops[:500]


def _demo_processar_sessao(session, rodada, previous_colors):
    if not isinstance(session, dict) or not bool(session.get("active", False)):
        return session

    strategy = dict(session.get("strategy", {}) or {})
    if not strategy:
        return session

    rodada_id = str((rodada or {}).get("id", ""))
    rodada_hora = str((rodada or {}).get("data_hora", ""))

    # V58.8: a mesma rodada oficial nunca pode ser processada duas vezes.
    processed_keys = list(session.get("processed_round_keys", []) or [])
    round_key = rodada_id if rodada_id else rodada_hora
    if round_key and round_key in processed_keys:
        return session
    if rodada_id and rodada_id == str(session.get("last_round_id", "")):
        return session

    is_white = str(strategy.get("type", "single")) == "white"
    if is_white:
        max_attempts = max(1, min(20, int(strategy.get("attempts", 6))))
        gale_max = max_attempts - 1
        gale_mode = 0
    else:
        gale_max = max(0, min(11, int(strategy.get("gale", 0))))
        gale_mode = int(strategy.get("gale_mode", 0))
    white_enabled = bool(session.get("white_enabled", False))
    signal = _demo_sinal_estrategia(strategy, previous_colors)
    cycle_active = bool(session.get("cycle_active", False))
    current_gale = int(session.get("current_gale", 0))
    cycle_target = str(session.get("cycle_target", ""))

    should_bet = False
    target = ""

    # Desligar BRANCO cancela uma sequência branca pendente sem registrar LOSS
    # e impede novas entradas até o usuário ligar novamente.
    if is_white and not white_enabled:
        cycle_active = False
        current_gale = 0
        cycle_target = ""
        signal = ""

    if not cycle_active:
        if signal:
            cycle_active = True
            current_gale = 0
            cycle_target = signal
            should_bet = True
            target = signal
    else:
        if gale_mode == 0:
            should_bet = True
            target = cycle_target
        elif signal:
            should_bet = True
            target = signal
            cycle_target = signal

    if should_bet:
        # V57.8: auditoria explícita da aposta.
        original_target = target
        contrary_applied = bool(session.get("contrary_signal", False)) and target in ("R", "B")
        if contrary_applied:
            target = "B" if target == "R" else "R"
        session["last_original_target"] = original_target
        session["last_executed_target"] = target
        session["last_contrary_applied"] = contrary_applied
        session["last_bet_gale"] = int(current_gale)

        entry_value = float(session.get("entry_value", strategy.get("stake", 10.0)))
        # Branco: valor fixo em todas as tentativas. Estratégias normais: Gale 2x.
        stake = entry_value if is_white else entry_value * (2.0 ** current_gale)
        bank = float(session.get("bank", 0.0))

        if bank < stake:
            session["losses"] = int(session.get("losses", 0)) + 1
            session["stop_reason"] = (
                "BANCA INSUFICIENTE PARA G%d • precisava R$ %.2f"
                % (current_gale, stake)
            )
            session["active"] = False
            session["cycle_active"] = False
            _demo_operacao(
                session, rodada, "STOP", target, stake, current_gale,
                session["stop_reason"]
            )
        else:
            bank -= stake
            session["bank"] = bank
            session["min_bank"] = min(
                float(session.get("min_bank", bank)), bank
            )
            peak = float(session.get("peak_bank", bank))
            session["max_dd"] = max(
                float(session.get("max_dd", 0.0)),
                peak - bank
            )

            actual = str((rodada or {}).get("cor", ""))
            if actual == target:
                multiplicador = float(strategy.get("payout", 14.0)) if is_white else 2.0
                bank += stake * multiplicador
                session["bank"] = bank
                session["peak_bank"] = max(
                    float(session.get("peak_bank", bank)), bank
                )
                session["wins"] = int(session.get("wins", 0)) + 1
                gw = list(session.get("gale_wins", []) or [])
                while len(gw) <= gale_max:
                    gw.append(0)
                if current_gale < len(gw):
                    gw[current_gale] = int(gw[current_gale]) + 1
                session["gale_wins"] = gw
                recent = list(session.get("recent_results", []) or [])
                recent.append(True)
                session["recent_results"] = recent[-500:]
                _demo_operacao(
                    session, rodada, "WIN", target, stake, current_gale
                )
                cycle_active = False
                current_gale = 0
                cycle_target = ""
            else:
                _demo_operacao(
                    session, rodada, "LOSS_TENTATIVA",
                    target, stake, current_gale
                )
                if current_gale >= gale_max:
                    session["losses"] = int(session.get("losses", 0)) + 1
                    recent = list(session.get("recent_results", []) or [])
                    recent.append(False)
                    session["recent_results"] = recent[-500:]
                    cycle_active = False
                    current_gale = 0
                    cycle_target = ""
                else:
                    current_gale += 1

            session["cycle_active"] = cycle_active
            session["current_gale"] = current_gale
            session["cycle_target"] = cycle_target

            reason = _demo_risk_reason(session)
            if reason:
                session["stop_reason"] = reason
                session["active"] = False
                _demo_operacao(
                    session, rodada, "STOP", target, 0.0,
                    current_gale, reason
                )

    session["last_round_id"] = rodada_id
    session["last_round_data_hora"] = str((rodada or {}).get("data_hora", ""))
    if round_key:
        processed_keys.append(round_key)
        session["processed_round_keys"] = processed_keys[-5000:]
    session["processed_rounds"] = int(session.get("processed_rounds", 0)) + 1
    session["updated_at"] = agora_brasilia()
    return session


def processar_demos_com_rodada(rodada):
    if not isinstance(rodada, dict):
        return

    with LOCK:
        base = ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", [])))
        demos = dict(ESTADO.get("demo_contas", {}) or {})

    if not demos:
        return

    rodada_id = str(rodada.get("id", ""))
    idx = -1
    for i in range(len(base) - 1, -1, -1):
        if rodada_id and str(base[i].get("id", "")) == rodada_id:
            idx = i
            break
    if idx < 0:
        idx = len(base) - 1

    previous_colors = cores(base[:idx])
    alteradas = {}

    for demo_id, sessao in demos.items():
        if not isinstance(sessao, dict) or not sessao.get("active"):
            continue
        nova = _demo_processar_sessao(
            dict(sessao), rodada, previous_colors
        )
        alteradas[str(demo_id)] = nova

    if not alteradas:
        return

    with LOCK:
        contas = ESTADO.setdefault("demo_contas", {})
        for demo_id, sessao in alteradas.items():
            contas[demo_id] = sessao
        salvar_json(BANCO, ESTADO)

    if postgres_configurado():
        for demo_id, sessao in alteradas.items():
            postgres_demo_salvar(demo_id, sessao)


def _demo_inicio_pendente(base, sessao):
    """
    Retorna o primeiro índice ainda não processado.
    Prioridade:
    1) ID exato da última rodada da Demo;
    2) horário da última rodada da Demo;
    3) se não há referência, não retroage antes da criação da sessão.

    O ponto crítico da V58.3 era: se o ID mudasse após deploy/bootstrap,
    o código assumia start=len(base) e pulava todas as rodadas pendentes.
    """
    if not base:
        return 0

    last_id = str(sessao.get("last_round_id", "")).strip()
    if last_id:
        for i, item in enumerate(base):
            if str(item.get("id", "")).strip() == last_id:
                return i + 1

    last_time = str(sessao.get("last_round_data_hora", "")).strip()
    last_epoch = _epoch_brasilia(last_time) if last_time else 0.0
    if last_epoch > 0:
        for i, item in enumerate(base):
            ep = momento_efetivo_epoch(item)
            if ep > last_epoch + 0.001:
                return i
        return len(base)

    # Sessão sem referência anterior: começa da rodada atual, sem inventar passado.
    return len(base)


def demo_recuperar_pendentes():
    # V58.8: DESATIVADO propositalmente.
    # A Demo é estritamente ao vivo a partir da ativação da estratégia.
    # Não retroprocessa rodadas antigas após deploy/restart/reconexão.
    return {"demos": 0, "processadas": 0, "modo": "somente_tempo_real"}


def worker_demo_sincronia_24h():
    # V58.8: sem replay histórico. Mantido apenas para compatibilidade.
    while True:
        time.sleep(60)


def demo_iniciar(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload inválido")

    demo_id = str(payload.get("demo_id", "principal")).strip() or "principal"
    strategy = payload.get("strategy", {})
    if not isinstance(strategy, dict) or not strategy:
        raise ValueError("strategy inválida")

    initial_bank = max(0.01, float(payload.get("initial_bank", 1000.0)))
    entry_value = max(0.01, float(payload.get(
        "entry_value", strategy.get("stake", 10.0)
    )))
    if str(strategy.get("type", "single")) == "white":
        gale_max = max(1, min(20, int(strategy.get("attempts", 6)))) - 1
    else:
        gale_max = max(0, min(11, int(strategy.get("gale", 0))))

    with LOCK:
        base = ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", [])))
        last = dict(base[-1]) if base else {}

    sessao = {
        "demo_id": demo_id,
        "active": True,
        "white_enabled": bool(payload.get("white_enabled", False)),
        "contrary_signal": bool(payload.get("contrary_signal", False)),
        "strategy": dict(strategy),
        "initial_bank": initial_bank,
        "entry_value": entry_value,
        "bank": initial_bank,
        "peak_bank": initial_bank,
        "min_bank": initial_bank,
        "max_dd": 0.0,
        "wins": 0,
        "losses": 0,
        "gale_wins": [0 for _ in range(gale_max + 1)],
        "current_gale": 0,
        "cycle_active": False,
        "cycle_target": "",
        "last_original_target": "",
        "last_executed_target": "",
        "last_contrary_applied": False,
        "last_bet_gale": 0,
        "last_round_id": str(last.get("id", "")),
        "last_round_data_hora": str(last.get("data_hora", "")),
        "processed_rounds": 0,
        "processed_round_keys": [],
        "activation_round_id": str(last.get("id", "")),
        "activation_round_data_hora": str(last.get("data_hora", "")),
        "operations": [],
        "recent_results": [],
        "stop_reason": "",
        "started_at": agora_brasilia(),
        "updated_at": agora_brasilia()
    }

    # A banca da estratégia passa a ser a banca real da sessão Demo,
    # para Stop Win e proteções usarem a referência correta.
    sessao["strategy"]["initial_bank"] = initial_bank
    sessao["strategy"]["stake"] = entry_value

    with LOCK:
        ESTADO.setdefault("demo_contas", {})[demo_id] = sessao
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_salvar(demo_id, sessao)
    return sessao



def _demo_watchdog_recuperar_sequencial(sessao, base):
    """
    V58.6 — watchdog conservador.
    Não inventa rodadas e não pula lacunas: apenas detecta atraso entre a
    última rodada processada pela sessão e a base real já armazenada.
    Retorna a fatia REAL pendente, em ordem cronológica, para o mesmo motor
    de processamento da Demo consumir uma a uma.
    """
    if not isinstance(sessao, dict) or not isinstance(base, list) or not base:
        return []

    last_id = str(sessao.get("last_round_id") or "").strip()
    last_dt = str(sessao.get("last_round_data_hora") or "").strip()

    # Sessão nova: não retroprocessar histórico inteiro.
    if not last_id and not last_dt:
        return []

    idx = -1
    for i in range(len(base) - 1, -1, -1):
        r = base[i] if isinstance(base[i], dict) else {}
        rid = str(r.get("id") or "").strip()
        rdt = str(r.get("data_hora") or r.get("dataHora") or "").strip()
        if (last_id and rid == last_id) or (not last_id and last_dt and rdt == last_dt):
            idx = i
            break

    # Se a âncora sumiu da janela/base, não adivinhar posição.
    # Segurança: nada é processado artificialmente.
    if idx < 0:
        sessao["watchdog_anchor_missing"] = True
        return []

    sessao["watchdog_anchor_missing"] = False
    pendentes = base[idx + 1:]
    return [r for r in pendentes if isinstance(r, dict)]

def demo_status(demo_id="principal"):
    with LOCK:
        sessao = dict(
            ESTADO.get("demo_contas", {}).get(str(demo_id), {}) or {}
        )
    if not sessao:
        return {"ok": True, "existe": False, "demo_id": str(demo_id)}

    total = int(sessao.get("wins", 0)) + int(sessao.get("losses", 0))
    taxa = (
        100.0 * int(sessao.get("wins", 0)) / total
        if total else 0.0
    )
    with LOCK:
        base = ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", [])))
    server_last = dict(base[-1]) if base else {}
    # V58.8: Demo não possui fila histórica. Só recebe rodadas novas.
    pending = 0

    out = dict(sessao)
    out.update({
        "ok": True,
        "existe": True,
        "taxa_acerto": round(taxa, 2),
        "profit": round(
            float(sessao.get("bank", 0.0))
            - float(sessao.get("initial_bank", 0.0)),
            2
        ),
        "server_latest_round_id": str(server_last.get("id", "")),
        "server_latest_round_time": str(server_last.get("data_hora", "")),
        "pending_rounds": int(pending),
        "demo_synced": bool(pending == 0),
        "watchdog_v586": True,
        "watchdog_anchor_missing": bool(sessao.get("watchdog_anchor_missing", False))
    })
    return out


def demo_alterar_estrategia(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload inválido")
    demo_id = str(payload.get("demo_id", "principal")).strip() or "principal"
    strategy = payload.get("strategy", {})
    if not isinstance(strategy, dict) or not strategy:
        raise ValueError("strategy inválida")
    with LOCK:
        sessao = dict(ESTADO.get("demo_contas", {}).get(demo_id, {}) or {})
        if not sessao:
            raise ValueError("demo não encontrada")
        # V58.8: estratégia nova começa EXATAMENTE AGORA.
        # Nenhuma rodada anterior à ativação pode virar entrada.
        base = ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", [])))
        last = dict(base[-1]) if base else {}

        sessao["strategy"] = dict(strategy)
        sessao["strategy"]["initial_bank"] = float(sessao.get("initial_bank", 1000.0))
        sessao["strategy"]["stake"] = float(sessao.get("entry_value", 10.0))
        sessao["last_round_id"] = str(last.get("id", ""))
        sessao["last_round_data_hora"] = str(last.get("data_hora", ""))
        sessao["activation_round_id"] = str(last.get("id", ""))
        sessao["activation_round_data_hora"] = str(last.get("data_hora", ""))
        sessao["processed_round_keys"] = []
        # Troca limpa: não herda Gale/tentativa da estratégia anterior.
        sessao["cycle_active"] = False
        sessao["current_gale"] = 0
        sessao["cycle_target"] = ""
        sessao["updated_at"] = agora_brasilia()
        ESTADO.setdefault("demo_contas", {})[demo_id] = sessao
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_salvar(demo_id, sessao)
    return demo_status(demo_id)


def demo_branco_ativo(demo_id="principal", enabled=True):
    with LOCK:
        sessao = dict(ESTADO.get("demo_contas", {}).get(str(demo_id), {}) or {})
        if not sessao:
            raise ValueError("demo não encontrada")
        sessao["white_enabled"] = bool(enabled)
        if not enabled and str((sessao.get("strategy") or {}).get("type", "")) == "white":
            sessao["cycle_active"] = False
            sessao["current_gale"] = 0
            sessao["cycle_target"] = ""
        sessao["updated_at"] = agora_brasilia()
        ESTADO.setdefault("demo_contas", {})[str(demo_id)] = sessao
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_salvar(demo_id, sessao)
    return demo_status(demo_id)


def demo_sinal_contrario(demo_id="principal", enabled=True):
    with LOCK:
        sessao = dict(ESTADO.get("demo_contas", {}).get(str(demo_id), {}) or {})
        if not sessao:
            raise ValueError("demo não encontrada")
        # Não reinicia o ciclo. Enquanto ligado, a inversão é aplicada
        # em TODA aposta executada, inclusive G0/G1/G2... do ciclo atual.
        sessao["contrary_signal"] = bool(enabled)
        sessao["updated_at"] = agora_brasilia()
        ESTADO.setdefault("demo_contas", {})[str(demo_id)] = sessao
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_salvar(demo_id, sessao)
    return demo_status(demo_id)


def demo_parar(demo_id="principal"):
    with LOCK:
        sessao = dict(
            ESTADO.get("demo_contas", {}).get(str(demo_id), {}) or {}
        )
        if not sessao:
            return {"ok": False, "erro": "demo não encontrada"}
        sessao["active"] = False
        sessao["updated_at"] = agora_brasilia()
        ESTADO["demo_contas"][str(demo_id)] = sessao
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_salvar(demo_id, sessao)
    return demo_status(demo_id)


def demo_resetar(demo_id="principal"):
    with LOCK:
        ESTADO.setdefault("demo_contas", {}).pop(str(demo_id), None)
        salvar_json(BANCO, ESTADO)
    if postgres_configurado():
        postgres_demo_excluir(demo_id)
    return {"ok": True, "demo_id": str(demo_id), "resetada": True}


def adicionar_rodada(rodada):
    if not rodada:
        return False

    with LOCK:
        rodadas = ESTADO["rodadas"]

        identificador = str(rodada.get("id", "")).strip()
        horario = str(rodada.get("data_hora", "")).strip()

        # V58.0: deduplica pela identidade OU pelo horário real.
        # Evita a mesma rodada entrar duas vezes com IDs de formatos diferentes.
        for existente in rodadas[-30000:]:
            if identificador and str(existente.get("id", "")).strip() == identificador:
                return False
            if horario and str(existente.get("data_hora", "")).strip() == horario:
                return False

        rodadas.append(rodada)
        rodadas[:] = ordenar_rodadas_canonicas(rodadas)

        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)

    if postgres_configurado():
        postgres_salvar_rodadas([rodada])

    # Processa todas as contas Demo no servidor, mesmo sem app conectado.
    try:
        processar_demos_com_rodada(rodada)
    except Exception as exc:
        print("Erro ao processar Demo 24h:", exc, flush=True)

    if str(rodada.get("origem", "")) == "blaze_websocket":
        solicitar_recalculo_sinal(rodada)
    else:
        atualizar_sinal_e_notificar(rodada)

    return True




def extrair_bestblaze_historico_html(html):
    """
    Parser robusto do histórico público BestBlaze.

    A página apresenta a sequência:
      número (quando não é branco)
      data/hora

    Quando aparece data/hora sem um número 0..14 imediatamente antes,
    a rodada é branca (W). Textos como títulos, totais e contadores são
    ignorados.
    """
    texto = html_para_texto(html)

    token_re = re.compile(
        r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}"
        r"|"
        r"(?<!\d)(?:0|[1-9]|1[0-4])(?!\d)"
    )

    tokens = token_re.findall(texto)
    rodadas = []
    vistos = set()
    numero_pendente = None

    for token in tokens:
        if re.fullmatch(r"0|[1-9]|1[0-4]", token):
            numero_pendente = int(token)
            continue

        # Timestamp.
        data_hora = token

        try:
            momento = datetime.strptime(
                data_hora,
                "%d/%m/%Y %H:%M:%S"
            )
        except Exception:
            numero_pendente = None
            continue

        if data_hora in vistos:
            numero_pendente = None
            continue

        vistos.add(data_hora)

        if numero_pendente is None:
            numero = 0
            cor = "W"
        else:
            numero = numero_pendente
            cor = normalizar_cor(numero)

        numero_pendente = None

        rodadas.append({
            "id": "%s-%02d" % (
                momento.strftime("%Y%m%d-%H%M%S"),
                numero
            ),
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


def _atributo_html(tag, nome, padrao=""):
    """Extrai atributo HTML; se não existir, retorna o valor padrão informado."""
    nome_esc = re.escape(str(nome))
    padrao_texto = "(?:^|\\s)" + nome_esc + "\\s*=\\s*(?:\"([^\"]*)\"|'([^']*)'|([^\\s>]+))"
    achado = re.search(padrao_texto, tag or "", re.IGNORECASE)
    if not achado:
        return padrao
    for valor in achado.groups():
        if valor is not None:
            return unescape(valor)
    return padrao


def _selecionar_valor_select(corpo_select):
    """
    Retorna a opção selecionada; se não houver selected, usa a primeira option
    com value. Isso replica o comportamento normal do navegador.
    """
    opcoes = re.findall(
        r"(?is)<option\b([^>]*)>(.*?)</option>",
        corpo_select or ""
    )

    primeira = ""

    for attrs, _texto in opcoes:
        valor = _atributo_html(attrs, "value", "")

        if primeira == "" and valor != "":
            primeira = valor

        if re.search(r"(?i)(?:^|\s)selected(?:\s|=|$)", attrs or ""):
            return valor

    return primeira


def detectar_formulario_periodo_bestblaze(html, base_url):
    """
    Detecta o formulário real e TODOS os controles relevantes:
    hidden, select, radio/checkbox marcado e botão de envio.

    Isso evita adivinhar o valor de tipoFiltro.
    """
    forms = re.findall(r"(?is)<form\b([^>]*)>(.*?)</form>", html)

    for attrs, corpo in forms:
        campos_data = []
        defaults = {}
        nomes_controles = []

        # Inputs.
        for attrs_input in re.findall(r"(?is)<input\b([^>]*)>", corpo):
            tipo = _atributo_html(attrs_input, "type", "text").lower()
            nome = _atributo_html(attrs_input, "name", "")
            valor = _atributo_html(attrs_input, "value", "")

            if not nome:
                continue

            nomes_controles.append(nome)

            if tipo in ("date", "datetime-local"):
                campos_data.append((nome, tipo))
                continue

            if tipo in ("radio", "checkbox"):
                marcado = bool(
                    re.search(
                        r"(?i)(?:^|\s)checked(?:\s|=|$)",
                        attrs_input or ""
                    )
                )
                if marcado:
                    defaults[nome] = valor
                continue

            if tipo in ("hidden", "text", "number"):
                defaults[nome] = valor
                continue

            # Submit input: inclui apenas se ele carrega um nome/valor útil.
            if tipo == "submit" and nome and valor:
                defaults.setdefault(nome, valor)

        # Selects: preserva o valor que o navegador enviaria.
        for attrs_select, corpo_select in re.findall(
            r"(?is)<select\b([^>]*)>(.*?)</select>",
            corpo
        ):
            nome = _atributo_html(attrs_select, "name", "")
            if not nome:
                continue

            nomes_controles.append(nome)
            defaults[nome] = _selecionar_valor_select(corpo_select)

        # Buttons: muitos sites colocam tipoFiltro no botão clicado.
        botoes = []
        for attrs_button, texto_button in re.findall(
            r"(?is)<button\b([^>]*)>(.*?)</button>",
            corpo
        ):
            tipo = _atributo_html(attrs_button, "type", "submit").lower()
            nome = _atributo_html(attrs_button, "name", "")
            valor = _atributo_html(attrs_button, "value", "")
            texto_limpo = re.sub(r"<[^>]+>", " ", texto_button or "")
            texto_limpo = re.sub(r"\s+", " ", unescape(texto_limpo)).strip()

            if tipo == "submit":
                botoes.append({
                    "name": nome,
                    "value": valor,
                    "texto": texto_limpo
                })

        if len(campos_data) < 2:
            continue

        metodo = _atributo_html(attrs, "method", "GET").upper()
        action = _atributo_html(attrs, "action", base_url)

        # Se tipoFiltro ainda não tiver valor, procura no botão submit.
        if not str(defaults.get("tipoFiltro", "")).strip():
            for botao in botoes:
                if (
                    botao.get("name") == "tipoFiltro"
                    and str(botao.get("value", "")).strip()
                ):
                    defaults["tipoFiltro"] = botao["value"]
                    break

        return {
            "method": metodo,
            "action": urljoin(base_url, action),
            "campo_inicial": campos_data[0][0],
            "tipo_inicial": campos_data[0][1],
            "campo_final": campos_data[1][0],
            "tipo_final": campos_data[1][1],
            "defaults": defaults,
            "nomes_controles": sorted(set(nomes_controles)),
            "botoes": botoes
        }

    return None


def _normalizar_url_bestblaze(url):
    """Normaliza o host BestBlaze sem desativar a validação SSL."""
    valor = str(url or "").strip()
    valor = re.sub(
        r"(?i)^(https?://)www\.bestblaze\.com\.br(?=/|$)",
        r"\1bestblaze.com.br",
        valor
    )
    valor = re.sub(
        r"(?i)^//www\.bestblaze\.com\.br(?=/|$)",
        "//bestblaze.com.br",
        valor
    )
    return valor


def _headers_bestblaze(referer=""):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 15) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0 Mobile Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1"
    }

    if referer:
        headers["Referer"] = referer

    return headers


def criar_sessao_bestblaze():
    jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    return opener, jar


def abrir_com_sessao_bestblaze(opener, url, data=None, referer=""):
    headers = _headers_bestblaze(referer)

    if data is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        parsed = urlparse(url)
        headers["Origin"] = "%s://%s" % (parsed.scheme, parsed.netloc)

    req = Request(
        url,
        data=data,
        headers=headers,
        method="POST" if data is not None else "GET"
    )

    resposta = opener.open(req, timeout=40)
    status = getattr(resposta, "status", 200)
    corpo = resposta.read().decode("utf-8", errors="replace")

    if status < 200 or status >= 400:
        raise RuntimeError("HTTP %s ao consultar período" % status)

    return corpo, status


def buscar_periodo_bestblaze(data_inicial, data_final):
    """
    Fluxo de navegador:
    GET inicial -> cookies/CSRF -> detecta todos os campos -> POST na mesma sessão.
    """
    base_url = "https://bestblaze.com.br/doubleRodadas"
    opener, jar = criar_sessao_bestblaze()

    pagina_inicial, status_get = abrir_com_sessao_bestblaze(
        opener,
        base_url
    )

    form = detectar_formulario_periodo_bestblaze(
        pagina_inicial,
        base_url
    )

    if not form:
        raise RuntimeError(
            "Formulário de período não identificado no HTML atual"
        )

    def formatar_data(dt, tipo):
        if tipo == "datetime-local":
            return dt.strftime("%Y-%m-%dT%H:%M")
        return dt.strftime("%Y-%m-%d")

    payload = dict(form.get("defaults", {}))

    payload[form["campo_inicial"]] = formatar_data(
        data_inicial,
        form["tipo_inicial"]
    )
    payload[form["campo_final"]] = formatar_data(
        data_final,
        form["tipo_final"]
    )

    # Campos vazios de filtro secundário normalmente devem continuar vazios.
    # Só removemos chaves None; strings vazias são preservadas como no browser.
    payload = {
        str(k): "" if v is None else str(v)
        for k, v in payload.items()
        if str(k).strip()
    }

    encoded_text = urlencode(payload)
    encoded = encoded_text.encode("utf-8")

    if form["method"] == "POST":
        html, status_post = abrir_com_sessao_bestblaze(
            opener,
            form["action"],
            data=encoded,
            referer=base_url
        )
        url_final = form["action"]
    else:
        separador = "&" if "?" in form["action"] else "?"
        url_final = form["action"] + separador + encoded_text
        html, status_post = abrir_com_sessao_bestblaze(
            opener,
            url_final,
            referer=base_url
        )

    cookies = [
        {
            "nome": cookie.name,
            "dominio": cookie.domain,
            "seguro": bool(cookie.secure)
        }
        for cookie in jar
    ]

    diagnostico = {
        "status_get": status_get,
        "status_envio": status_post,
        "metodo": form["method"],
        "action": form["action"],
        "campo_inicial": form["campo_inicial"],
        "campo_final": form["campo_final"],
        "tipo_filtro_enviado": payload.get("tipoFiltro", ""),
        "controles_detectados": form.get("nomes_controles", []),
        "botoes_detectados": form.get("botoes", []),
        "cookies_recebidos": cookies,
        "url_final": url_final
    }

    return html, form, payload, diagnostico

def diagnosticar_sessao_periodo_bestblaze():
    """
    Diagnóstico sem efetuar importação grande.
    Valida cookie/token e consulta o dia atual.
    """
    agora = datetime.now(timezone(timedelta(hours=-3)))
    inicio = agora.replace(hour=0, minute=0, second=0, microsecond=0)
    fim = agora

    html, form, payload, diag = buscar_periodo_bestblaze(
        inicio,
        fim
    )

    rodadas = extrair_bestblaze_historico_html(html)

    texto_resposta = html_para_texto(html)

    return {
        "ok": True,
        "rodadas_reconhecidas": len(rodadas),
        "brancos_reconhecidos": sum(
            1 for item in rodadas if item.get("cor") == "W"
        ),
        "tamanho_html": len(html),
        "tem_texto_total_rodadas": "Total de rodadas" in texto_resposta,
        "amostra_resposta": texto_resposta[:500],
        "sessao": diag,
        "campos_enviados": sorted(list(payload.keys())),
        "tipo_filtro_enviado": payload.get("tipoFiltro", ""),
        "tem_rodadas_no_periodo": "Rodadas no período" in texto_resposta,
        "tem_total_de_rodadas": "Total de rodadas" in texto_resposta
    }


def importar_1000_bestblaze(meta=1000, max_dias=7):
    """
    Importa vários dias anteriores até atingir a meta.

    Preserva o fluxo V40 já validado:
    sessão + cookies + CSRF + formulário + tipoFiltro.

    Cada dia é consultado separadamente para reduzir risco de timeout e
    permitir diagnóstico preciso por período.
    """
    meta = max(100, min(int(meta), 30000))
    max_dias = max(1, min(int(max_dias), 30))

    agora = datetime.now(timezone(timedelta(hours=-3)))

    with LOCK:
        total_inicial = len(ESTADO.get("rodadas", []))

    if total_inicial >= meta:
        return {
            "ok": True,
            "meta": meta,
            "meta_efetiva": meta,
            "max_dias": max_dias,
            "total_inicial": total_inicial,
            "total_banco": total_inicial,
            "total_adicionadas_nesta_importacao": 0,
            "faltam_para_meta": 0,
            "dias_consultados": [],
            "mensagem": "Meta já atingida.",
            "cores": resumo_cores_historico(meta),
            "sequencias": sequencias_cores(meta)
        }

    total_adicionadas = 0
    dias_consultados = []
    ultima_sessao = None

    for deslocamento in range(1, max_dias + 1):
        alvo = agora - timedelta(days=deslocamento)
        inicio = alvo.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        fim = alvo.replace(
            hour=23, minute=59, second=59, microsecond=0
        )

        try:
            html, form, payload, diag = buscar_periodo_bestblaze(
                inicio,
                fim
            )
            ultima_sessao = diag

            rodadas = extrair_bestblaze_historico_html(html)

            if not rodadas:
                dias_consultados.append({
                    "data": alvo.strftime("%d/%m/%Y"),
                    "ok": False,
                    "recebidas": 0,
                    "adicionadas": 0,
                    "duplicadas": 0,
                    "status_http": int(diag.get("status_envio", 0)),
                    "tipo_filtro": str(
                        diag.get("tipo_filtro_enviado", "")
                    ),
                    "erro": (
                        "HTTP 200, mas nenhuma rodada foi reconhecida"
                    )
                })
                continue

            resultado = adicionar_rodadas_em_lote(rodadas)

            adicionadas = int(resultado.get("adicionadas", 0))
            duplicadas = int(resultado.get("duplicadas", 0))
            total_adicionadas += adicionadas

            with LOCK:
                total_banco = len(ESTADO.get("rodadas", []))

            dias_consultados.append({
                "data": alvo.strftime("%d/%m/%Y"),
                "ok": True,
                "recebidas": len(rodadas),
                "adicionadas": adicionadas,
                "duplicadas": duplicadas,
                "brancos_recebidos": sum(
                    1
                    for item in rodadas
                    if isinstance(item, dict)
                    and item.get("cor") == "W"
                ),
                "status_http": int(diag.get("status_envio", 0)),
                "tipo_filtro": str(
                    diag.get("tipo_filtro_enviado", "")
                ),
                "total_banco_apos_dia": total_banco
            })

            if total_banco >= meta:
                break

        except Exception as exc:
            dias_consultados.append({
                "data": alvo.strftime("%d/%m/%Y"),
                "ok": False,
                "recebidas": 0,
                "adicionadas": 0,
                "duplicadas": 0,
                "erro": str(exc)
            })

    with LOCK:
        total_banco = len(ESTADO.get("rodadas", []))

    return {
        "ok": total_banco >= meta,
        "meta": meta,
        "meta_efetiva": meta,
        "max_dias": max_dias,
        "total_inicial": total_inicial,
        "total_banco": total_banco,
        "total_adicionadas_nesta_importacao": total_adicionadas,
        "faltam_para_meta": max(0, meta - total_banco),
        "dias_consultados": dias_consultados,
        "ultima_sessao": ultima_sessao,
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
    novas_adicionadas = []

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
            novas_adicionadas.append(dict(rodada))
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

        if len(banco) > LIMITE_HISTORICO:
            del banco[:-LIMITE_HISTORICO]

        ESTADO["ultima_atualizacao"] = agora_brasilia()
        salvar_json(BANCO, ESTADO)

    if adicionadas > 0:
        if postgres_configurado():
            postgres_salvar_rodadas(novas_adicionadas)

        # V58.4: rodadas recuperadas em lote também alimentam a Demo.
        # Antes, somente adicionar_rodada() processava Demo; fallback/bootstrap
        # podiam atualizar o histórico sem atualizar a sessão Demo.
        for rodada_demo in ordenar_rodadas_canonicas(novas_adicionadas):
            try:
                processar_demos_com_rodada(rodada_demo)
            except Exception as exc:
                print("V58.4 erro Demo em lote:", repr(exc), flush=True)

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



def reparar_historico_publico_atual():
    """Tenta preencher lacunas usando o histórico público disponível no momento."""
    antes = diagnosticar_continuidade_historico()
    resultado = importar_historico_bestblaze()
    depois = diagnosticar_continuidade_historico()
    return {
        "ok": True,
        "importacao": resultado,
        "antes": {
            "total_bruto": antes.get("total_bruto", 0),
            "total_validado": antes.get("total_validado", 0),
            "lacunas": antes.get("lacunas", 0)
        },
        "depois": {
            "total_bruto": depois.get("total_bruto", 0),
            "total_validado": depois.get("total_validado", 0),
            "lacunas": depois.get("lacunas", 0)
        }
    }


def worker_reparo_inicial():
    # Dá tempo para o serviço e o banco terminarem de subir antes da consulta pública.
    time.sleep(20)
    try:
        diag = diagnosticar_continuidade_historico()
        if int(diag.get("lacunas", 0)) > 0:
            rep = reparar_historico_publico_atual()
            print(
                "Reparo histórico inicial:",
                rep.get("antes", {}), "->", rep.get("depois", {}),
                flush=True
            )
    except Exception as exc:
        print("Reparo histórico inicial falhou:", exc, flush=True)



def bootstrap_1500_mais_recentes(meta=1500, max_dias=3):
    """
    Monta uma base inicial com as rodadas públicas mais recentes, sem emendar
    trechos separados por lacunas. Só substitui o histórico atual se existir
    um segmento contínuo com pelo menos `meta` rodadas.

    Fluxo:
      1) consulta HOJE e dias anteriores, do mais recente para trás;
      2) junta com as rodadas LIVE já recebidas durante a importação;
      3) deduplica por horário/id;
      4) exige continuidade com gap máximo de 45 s;
      5) guarda exatamente as `meta` rodadas mais recentes do segmento;
      6) dali em diante a janela cresce com LIVE até 30000 e desliza.
    """
    meta = max(100, min(int(meta), 30000))
    max_dias = max(1, min(int(max_dias), 7))
    agora = datetime.now(timezone(timedelta(hours=-3)))

    # Proteção V54.7: nunca reduz uma base que já esteja validada e contínua.
    # Em deploys do Render, o JSON local pode reiniciar, mas o PostgreSQL restaura
    # as rodadas. Portanto a decisão de refazer o bootstrap deve olhar a própria
    # base carregada, e não apenas o campo bootstrap_1500.
    with LOCK:
        base_existente = ordenar_rodadas_canonicas(list(ESTADO.get("rodadas", [])))

    diag_existente = diagnosticar_continuidade_historico(base_existente, limite_gap=45)
    total_existente = len(base_existente)
    validado_existente = int(diag_existente.get("total_validado", 0))
    lacunas_existentes = int(diag_existente.get("lacunas", 0))

    if (
        total_existente >= meta
        and lacunas_existentes == 0
        and validado_existente == total_existente
    ):
        with LOCK:
            ESTADO["bootstrap_1500"] = {
                "status": "preservado",
                "tentativa": agora_brasilia(),
                "sucesso": agora_brasilia(),
                "erro": "",
                "quantidade": total_existente,
                "meta": meta,
                "motivo": "base_existente_continua_nao_reduzida"
            }
            salvar_json(BANCO, ESTADO)
        return {
            "ok": True,
            "preservado": True,
            "quantidade": total_existente,
            "meta": meta,
            "mensagem": "Base existente já é contínua; bootstrap não foi refeito para evitar redução."
        }

    with LOCK:
        ESTADO["bootstrap_1500"] = {
            "status": "importando",
            "tentativa": agora_brasilia(),
            "sucesso": "",
            "erro": "",
            "quantidade": 0,
            "meta": meta
        }
        live_snapshot = list(ESTADO.get("rodadas", []))
        salvar_json(BANCO, ESTADO)

    coletadas = []
    dias = []

    # Começa em HOJE para encaixar a cauda pública com as próximas rodadas LIVE.
    for deslocamento in range(0, max_dias):
        alvo = agora - timedelta(days=deslocamento)
        inicio = alvo.replace(hour=0, minute=0, second=0, microsecond=0)
        fim = agora if deslocamento == 0 else alvo.replace(hour=23, minute=59, second=59, microsecond=0)
        try:
            html, form, payload, diag = buscar_periodo_bestblaze(inicio, fim)
            rodadas = extrair_bestblaze_historico_html(html)
            for r in rodadas:
                if isinstance(r, dict):
                    item = dict(r)
                    item["origem"] = "bootstrap_publico_1500"
                    coletadas.append(item)
            dias.append({
                "data": alvo.strftime("%d/%m/%Y"),
                "recebidas": len(rodadas),
                "status_http": int(diag.get("status_envio", 0)),
                "ok": bool(rodadas)
            })
        except Exception as exc:
            dias.append({"data": alvo.strftime("%d/%m/%Y"), "recebidas": 0, "ok": False, "erro": str(exc)})

    # Captura novamente o LIVE porque novas rodadas podem ter chegado durante as consultas.
    with LOCK:
        live_agora = list(ESTADO.get("rodadas", []))

    candidatos = coletadas + live_snapshot + live_agora
    unicos = {}
    for r in candidatos:
        if not isinstance(r, dict) or str(r.get("cor", "")) not in ("R", "B", "W"):
            continue
        horario = str(r.get("data_hora", "")).strip()
        ident = str(r.get("id", "")).strip()
        chave = horario or ident
        if not chave:
            continue
        existente = unicos.get(chave)
        # Em duplicata, prefere LIVE por ter metadados do WebSocket.
        if existente is None or str(r.get("origem", "")) == "blaze_websocket":
            unicos[chave] = dict(r)

    base = ordenar_rodadas_canonicas(list(unicos.values()))
    diag = diagnosticar_continuidade_historico(base, limite_gap=45)
    segmento = list(diag.get("segmento_atual", []))

    if len(segmento) < meta:
        erro = "Segmento contínuo insuficiente: %d/%d" % (len(segmento), meta)
        with LOCK:
            ESTADO["bootstrap_1500"] = {
                "status": "falhou_sem_substituir",
                "tentativa": agora_brasilia(),
                "sucesso": "",
                "erro": erro,
                "quantidade": len(segmento),
                "meta": meta,
                "dias": dias,
                "lacunas_detectadas": int(diag.get("lacunas", 0))
            }
            salvar_json(BANCO, ESTADO)
        return {"ok": False, "erro": erro, "quantidade_continua": len(segmento), "meta": meta, "dias": dias}

    nova_base = segmento[-meta:]
    # A última rodada da base precisa ser recente para não criar um buraco antes do LIVE futuro.
    ultimo_epoch = momento_efetivo_epoch(nova_base[-1]) if nova_base else 0.0
    idade = max(0.0, time.time() - ultimo_epoch) if ultimo_epoch > 0 else 999999.0
    # Se houver rodadas LIVE mais novas, elas já estão no segmento; 120 s dá margem de deploy/rede.
    if idade > 120.0:
        erro = "Cauda pública não está recente o bastante para encaixar no LIVE: %.1f s" % idade
        with LOCK:
            ESTADO["bootstrap_1500"] = {
                "status": "falhou_sem_substituir",
                "tentativa": agora_brasilia(),
                "sucesso": "",
                "erro": erro,
                "quantidade": len(nova_base),
                "meta": meta,
                "dias": dias
            }
            salvar_json(BANCO, ESTADO)
        return {"ok": False, "erro": erro, "quantidade_continua": len(nova_base), "meta": meta, "idade_ultima_segundos": round(idade, 1)}

    # Commit atômico: só troca quando a candidata passou em todas as validações.
    with LOCK:
        ESTADO["rodadas"] = ordenar_rodadas_canonicas(nova_base)
        ESTADO["ultima_atualizacao"] = agora_brasilia()
        ESTADO["bootstrap_1500"] = {
            "status": "pronto",
            "tentativa": agora_brasilia(),
            "sucesso": agora_brasilia(),
            "erro": "",
            "quantidade": len(nova_base),
            "meta": meta,
            "dias": dias,
            "inicio": str(nova_base[0].get("data_hora", "")),
            "fim": str(nova_base[-1].get("data_hora", "")),
            "idade_ultima_segundos": round(idade, 1),
            "limite_gap_segundos": 45
        }
        salvar_json(BANCO, ESTADO)

    if postgres_configurado():
        # Persiste as 1500; o limitador mantém a janela máxima de 30000.
        postgres_salvar_rodadas(nova_base)
        postgres_limitar_historico(LIMITE_HISTORICO)
        marcar_bootstrap_permanente(len(nova_base), "bootstrap_publico_concluido")

    atualizar_sinal_e_notificar()
    return {
        "ok": True,
        "quantidade": len(nova_base),
        "meta": meta,
        "inicio": str(nova_base[0].get("data_hora", "")),
        "fim": str(nova_base[-1].get("data_hora", "")),
        "idade_ultima_segundos": round(idade, 1),
        "dias": dias,
        "mensagem": "Base inicial contínua criada; próximas rodadas LIVE completam a janela progressivamente até 30000/30000."
    }


def worker_bootstrap_1500():
    # V58.10: bootstrap histórico automático DESATIVADO.
    # Em produção o servidor somente restaura a base persistida e acrescenta
    # novas rodadas reais. Deploy não reconstrói nem reseta o histórico.
    with LOCK:
        ESTADO["bootstrap_1500"] = {
            "status": "desativado_base_persistente",
            "tentativa": agora_brasilia(),
            "sucesso": "",
            "erro": "",
            "quantidade": len(ESTADO.get("rodadas", [])),
            "motivo": "V58.10_sem_reset_em_deploy"
        }
    return


def _cores_recentes(limite=1000):
    limite = max(1, min(int(limite), 30000))
    with LOCK:
        bloco = list(ESTADO.get("rodadas", []))[-limite:]
    return [
        str(item.get("cor", ""))
        for item in bloco
        if isinstance(item, dict)
        and str(item.get("cor", "")) in ("R", "B", "W")
    ]


def analise_janelas():
    resultado = {}
    for janela in (10, 20, 50, 100, 500, 1000):
        cores = _cores_recentes(janela)
        total = len(cores)
        cont = {c: cores.count(c) for c in ("R", "B", "W")}
        resultado[str(janela)] = {
            "total": total,
            "R": {"qtd": cont["R"], "pct": (100.0*cont["R"]/total) if total else 0.0},
            "B": {"qtd": cont["B"], "pct": (100.0*cont["B"]/total) if total else 0.0},
            "W": {"qtd": cont["W"], "pct": (100.0*cont["W"]/total) if total else 0.0},
        }
    return resultado


def analise_transicoes(limite=1000):
    cores = _cores_recentes(limite)
    matriz = {a: {b: 0 for b in ("R","B","W")} for a in ("R","B","W")}
    for a, b in zip(cores, cores[1:]):
        matriz[a][b] += 1
    probs = {}
    for a in ("R","B","W"):
        total = sum(matriz[a].values())
        probs[a] = {
            b: {"qtd": matriz[a][b], "pct": (100.0*matriz[a][b]/total) if total else 0.0}
            for b in ("R","B","W")
        }
    return {"total_cores": len(cores), "matriz": matriz, "probabilidades": probs}


def analise_padroes(limite=1000, tamanho=3, top=20):
    tamanho = max(2, min(int(tamanho), 5))
    top = max(1, min(int(top), 100))
    cores = _cores_recentes(limite)
    contagem = {}
    for i in range(max(0, len(cores)-tamanho+1)):
        p = "".join(cores[i:i+tamanho])
        contagem[p] = contagem.get(p, 0) + 1
    total = sum(contagem.values())
    itens = sorted(contagem.items(), key=lambda kv: (-kv[1], kv[0]))[:top]
    return {
        "tamanho": tamanho,
        "total_padroes": total,
        "top": [
            {"padrao": p, "qtd": q, "pct": (100.0*q/total) if total else 0.0}
            for p, q in itens
        ]
    }


def analise_brancos(limite=1000):
    cores = _cores_recentes(limite)
    indices = [i for i, c in enumerate(cores) if c == "W"]
    intervalos = [max(0, b-a-1) for a, b in zip(indices, indices[1:])]
    desde_ultimo = (len(cores)-1-indices[-1]) if indices else len(cores)
    return {
        "total_cores": len(cores),
        "brancos": len(indices),
        "media_intervalo": (sum(intervalos)/len(intervalos)) if intervalos else 0.0,
        "menor_intervalo": min(intervalos) if intervalos else 0,
        "maior_intervalo": max(intervalos) if intervalos else 0,
        "rodadas_desde_ultimo_branco": desde_ultimo,
        "ultimos_intervalos": intervalos[-100:]
    }


def analise_sequencias_detalhada(limite=1000):
    cores = _cores_recentes(limite)
    maiores = {"R":0,"B":0,"W":0}
    if not cores:
        return {"total_cores":0,"maiores":maiores,"sequencia_atual":{"cor":"","tamanho":0},"alternancias":0,"taxa_alternancia_pct":0.0}
    atual = cores[0]
    tam = 0
    alt = 0
    ant = None
    for c in cores:
        if ant is not None and c != ant:
            alt += 1
        ant = c
        if c == atual:
            tam += 1
        else:
            maiores[atual] = max(maiores[atual], tam)
            atual, tam = c, 1
    maiores[atual] = max(maiores[atual], tam)
    fim = cores[-1]
    fim_tam = 0
    for c in reversed(cores):
        if c == fim:
            fim_tam += 1
        else:
            break
    return {
        "total_cores": len(cores),
        "maiores": maiores,
        "sequencia_atual": {"cor": fim, "tamanho": fim_tam},
        "alternancias": alt,
        "taxa_alternancia_pct": (100.0*alt/(len(cores)-1)) if len(cores)>1 else 0.0
    }


def backtest_regra_transicao(limite=1000, origem="R", apostar="B"):
    origem = str(origem).upper()
    apostar = str(apostar).upper()
    if origem not in ("R","B","W") or apostar not in ("R","B","W"):
        raise ValueError("cor inválida")
    cores = _cores_recentes(limite)
    entradas=acertos=erros=seq_a=seq_e=max_a=max_e=0
    saldo=pico=dd=0.0
    ganho = 13.0 if apostar == "W" else 1.0
    for i in range(len(cores)-1):
        if cores[i] != origem:
            continue
        entradas += 1
        if cores[i+1] == apostar:
            acertos += 1; seq_a += 1; seq_e = 0; max_a = max(max_a, seq_a); saldo += ganho
        else:
            erros += 1; seq_e += 1; seq_a = 0; max_e = max(max_e, seq_e); saldo -= 1.0
        pico = max(pico, saldo)
        dd = max(dd, pico-saldo)
    return {
        "regra":{"quando_aparecer":origem,"apostar_na_proxima":apostar},
        "entradas":entradas,"acertos":acertos,"erros":erros,
        "taxa_acerto_pct":(100.0*acertos/entradas) if entradas else 0.0,
        "maior_sequencia_acertos":max_a,"maior_sequencia_erros":max_e,
        "saldo_simulado_unidades":saldo,"drawdown_max_unidades":dd
    }


def backtest_padroes(limite=1000, padrao="RB", apostar="R"):
    padrao = str(padrao).upper().strip()
    apostar = str(apostar).upper().strip()
    if not 1 <= len(padrao) <= 5 or any(c not in ("R","B","W") for c in padrao):
        raise ValueError("padrao inválido")
    if apostar not in ("R","B","W"):
        raise ValueError("cor inválida")
    cores = _cores_recentes(limite)
    n = len(padrao)
    entradas=acertos=erros=0
    for i in range(len(cores)-n):
        if "".join(cores[i:i+n]) == padrao:
            entradas += 1
            if cores[i+n] == apostar: acertos += 1
            else: erros += 1
    return {
        "padrao":padrao,"apostar_na_proxima":apostar,
        "entradas":entradas,"acertos":acertos,"erros":erros,
        "taxa_acerto_pct":(100.0*acertos/entradas) if entradas else 0.0
    }


def painel_analise(limite=1000):
    return {
        "limite": min(max(1,int(limite)),30000),
        "janelas": analise_janelas(),
        "sequencias": analise_sequencias_detalhada(limite),
        "transicoes": analise_transicoes(limite),
        "brancos": analise_brancos(limite),
        "padroes_2": analise_padroes(limite,2,10),
        "padroes_3": analise_padroes(limite,3,10),
        "padroes_4": analise_padroes(limite,4,10),
        "padroes_5": analise_padroes(limite,5,10)
    }


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



def websocket_disponivel():
    return websocket is not None


def _parse_iso_utc_para_datetime(valor):
    if not valor:
        return None

    s = str(valor).strip()

    formatos = (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z"
    )

    for fmt in formatos:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue

    return None


def _registrar_latencia_ws(payload):
    candidatos = (
        payload.get("updated_at"),
        payload.get("created_at"),
        payload.get("rolled_at")
    )

    origem = None

    for valor in candidatos:
        origem = _parse_iso_utc_para_datetime(valor)
        if origem is not None:
            break

    if origem is None:
        return None

    agora_utc = datetime.now(timezone.utc)
    latencia = max(
        0.0,
        (agora_utc - origem).total_seconds()
    )

    with LOCK:
        historico = ESTADO.setdefault(
            "ws_latencias_recentes",
            []
        )
        historico.append(latencia)

        if len(historico) > 100:
            del historico[:-100]

        ESTADO["ws_latencia_segundos"] = latencia
        ESTADO["ws_latencia_media_segundos"] = (
            sum(historico) / len(historico)
        )

    return latencia


def _cor_ws_para_rbW(valor):
    try:
        n = int(valor)
    except Exception:
        return ""

    # Implementações públicas da Blaze usam:
    # 0 branco, 1 vermelho, 2 preto.
    if n == 0:
        return "W"
    if n == 1:
        return "R"
    if n == 2:
        return "B"

    return ""


def _rodada_payload_ws(payload):
    if not isinstance(payload, dict):
        return None

    status = str(
        payload.get("status", "")
    ).lower()

    if status != "rolling":
        return None

    cor = _cor_ws_para_rbW(
        payload.get("color")
    )

    if not cor:
        return None

    try:
        numero = int(
            payload.get("roll")
        )
    except Exception:
        numero = None

    stamp = (
        payload.get("updated_at")
        or payload.get("created_at")
        or agora_brasilia()
    )

    dt_utc = _parse_iso_utc_para_datetime(stamp)

    if dt_utc is not None:
        brasilia = timezone(timedelta(hours=-3))
        data_hora = dt_utc.astimezone(
            brasilia
        ).strftime("%d/%m/%Y %H:%M:%S")
    else:
        data_hora = agora_brasilia()

    identificador = str(
        payload.get("id")
        or payload.get("uuid")
        or (
            data_hora.replace("/", "")
            .replace(" ", "-")
            .replace(":", "")
            + "-%s" % (
                numero if numero is not None else cor
            )
        )
    )

    return {
        "id": identificador,
        "numero": numero,
        "cor": cor,
        "data_hora": data_hora,
        "origem": "blaze_websocket"
    }


def processar_mensagem_ws(msg):
    inicio_proc = time.perf_counter()
    recebido_epoch = time.time()

    if not isinstance(msg, str):
        return {"evento": False, "adicionada": False}

    match = re.match(r'^\d+\["data",\s*({.*})\]$', msg)
    if not match:
        return {"evento": False, "adicionada": False}

    try:
        envelope = json.loads(match.group(1))
    except Exception as exc:
        return {
            "evento": False,
            "adicionada": False,
            "erro": "JSON inválido: %s" % exc
        }

    if not isinstance(envelope, dict):
        return {"evento": False, "adicionada": False}

    evento_id = str(envelope.get("id", ""))
    payload = envelope.get("payload")

    if evento_id != "double.tick":
        return {
            "evento": False,
            "adicionada": False,
            "id": evento_id
        }

    if not isinstance(payload, dict):
        return {
            "evento": True,
            "adicionada": False,
            "erro": "payload ausente"
        }

    with LOCK:
        ESTADO["ws_eventos_recebidos"] = int(
            ESTADO.get("ws_eventos_recebidos", 0)
        ) + 1
        ESTADO["ws_ultimo_evento"] = agora_brasilia()

    rodada = _rodada_payload_ws(payload)

    if rodada is None:
        return {
            "evento": True,
            "adicionada": False,
            "status": str(payload.get("status", ""))
        }

    # A partir daqui é um resultado final (rolling).
    # O relógio confiável é o instante de chegada ao nosso servidor.
    rodada["recebido_epoch"] = recebido_epoch
    rodada["recebido_em_brasilia"] = datetime.fromtimestamp(
        recebido_epoch,
        timezone(timedelta(hours=-3))
    ).strftime("%d/%m/%Y %H:%M:%S")
    rodada["timestamp_fonte"] = (
        payload.get("updated_at")
        or payload.get("created_at")
        or payload.get("rolled_at")
        or ""
    )

    rodada_id = str(rodada.get("id", ""))
    source_dt = (
        _parse_iso_utc_para_datetime(payload.get("updated_at"))
        or _parse_iso_utc_para_datetime(payload.get("created_at"))
        or _parse_iso_utc_para_datetime(payload.get("rolled_at"))
    )
    source_epoch = source_dt.timestamp() if source_dt else 0.0

    with LOCK:
        ESTADO["ws_eventos_rolling_recebidos"] = int(
            ESTADO.get("ws_eventos_rolling_recebidos", 0)
        ) + 1

        ultimo_id = str(
            ESTADO.get("ws_ultimo_id_rolling", "")
        )
        ultimo_recv = float(
            ESTADO.get("ws_ultimo_recebido_epoch", 0.0)
        )
        ultimo_source = float(
            ESTADO.get("ws_ultimo_source_epoch", 0.0)
        )

        if rodada_id and rodada_id == ultimo_id:
            ESTADO["ws_duplicadas"] = int(
                ESTADO.get("ws_duplicadas", 0)
            ) + 1

        if (
            source_epoch > 0
            and ultimo_source > 0
            and source_epoch < ultimo_source
        ):
            ESTADO["ws_fora_de_ordem"] = int(
                ESTADO.get("ws_fora_de_ordem", 0)
            ) + 1

        # Não afirma "rodada perdida": apenas marca uma lacuna temporal suspeita.
        if (
            ultimo_recv > 0
            and recebido_epoch - ultimo_recv > 60.0
        ):
            ESTADO["ws_suspeitas_gap"] = int(
                ESTADO.get("ws_suspeitas_gap", 0)
            ) + 1

        ESTADO["ws_ultimo_id_rolling"] = rodada_id
        ESTADO["ws_ultimo_recebido_epoch"] = recebido_epoch
        ESTADO["ws_ultimo_recebido_brasilia"] = agora_brasilia()

        if source_epoch > 0:
            ESTADO["ws_ultimo_source_epoch"] = source_epoch

    adicionada = adicionar_rodada(rodada)

    processamento_ms = (
        time.perf_counter() - inicio_proc
    ) * 1000.0

    with LOCK:
        ESTADO["ws_processamento_ms"] = processamento_ms

        if adicionada:
            ESTADO["ws_rodadas_adicionadas"] = int(
                ESTADO.get("ws_rodadas_adicionadas", 0)
            ) + 1
            ESTADO["ws_ultima_rodada"] = str(
                rodada.get("data_hora", "")
            )
            ESTADO["coletor_ultimo_modo"] = "websocket"

    return {
        "evento": True,
        "adicionada": bool(adicionada),
        "rodada": rodada,
        "processamento_ms": processamento_ms
    }


def worker_websocket_double():
    """
    WebSocket Double com heartbeat Engine.IO textual.

    O servidor usa Engine.IO v3 sobre WebSocket. O keepalive é feito
    enviando a mensagem de TEXTO "2" periodicamente e respondendo
    "3" quando o servidor enviar "2".

    Não usamos WebSocket ping frame para o heartbeat da aplicação.
    """
    if websocket is None:
        with LOCK:
            ESTADO["ws_online"] = False
            ESTADO["ws_ultimo_erro"] = "websocket-client não instalado"
        return

    url = (
        "wss://api-gaming.blaze.bet.br"
        "/replication/?EIO=3&transport=websocket"
    )

    assinatura = (
        '420["cmd",{"id":"subscribe",'
        '"payload":{"room":"double_room_1"}}]'
    )

    while True:
        heartbeat_stop = threading.Event()

        try:
            with LOCK:
                ESTADO["ws_endpoint_atual"] = url
                ESTADO["ws_online"] = False
                ESTADO["ws_handshake_recebido"] = False
                ESTADO["ws_ultimo_erro"] = ""

            def heartbeat_texto(ws):
                while not heartbeat_stop.wait(10):
                    try:
                        if ws.sock and ws.sock.connected:
                            ws.send("2")
                    except Exception as exc:
                        with LOCK:
                            ESTADO["ws_ultimo_erro"] = (
                                "heartbeat texto: %s" % exc
                            )
                        break

            def on_open(ws):
                with LOCK:
                    ESTADO["ws_online"] = True
                    ESTADO["ws_conectado_epoch"] = time.time()
                    ESTADO["ws_ultimo_erro"] = ""
                    ESTADO["ws_assinaturas_enviadas"] = int(
                        ESTADO.get("ws_assinaturas_enviadas", 0)
                    ) + 1

                # Assina o Double.
                ws.send(assinatura)

                # Heartbeat Engine.IO: texto "2" a cada 10s.
                threading.Thread(
                    target=heartbeat_texto,
                    args=(ws,),
                    daemon=True
                ).start()

            def on_message(ws, msg):
                with LOCK:
                    ESTADO["ws_ultimo_raw_epoch"] = time.time()
                    ESTADO["ws_mensagens_raw"] = int(
                        ESTADO.get("ws_mensagens_raw", 0)
                    ) + 1

                    texto = (
                        msg
                        if isinstance(msg, str)
                        else repr(msg)
                    )
                    ESTADO["ws_ultimo_raw"] = texto[:500]

                # Handshake Engine.IO.
                if isinstance(msg, str) and msg.startswith("0"):
                    with LOCK:
                        ESTADO["ws_handshake_recebido"] = True
                    return

                # Ping Engine.IO recebido -> pong textual.
                if msg == "2":
                    try:
                        ws.send("3")
                    except Exception:
                        pass
                    return

                processar_mensagem_ws(msg)

            def on_error(ws, erro):
                heartbeat_stop.set()
                with LOCK:
                    ESTADO["ws_online"] = False
                    ESTADO["ws_ultimo_erro"] = str(erro)

            def on_close(ws, codigo, motivo):
                heartbeat_stop.set()
                with LOCK:
                    ESTADO["ws_online"] = False
                    ESTADO["ws_ultimo_erro"] = (
                        "fechado %s %s" % (codigo, motivo)
                    )

            app = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                header=[
                    (
                        "User-Agent: Mozilla/5.0 "
                        "(Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 "
                        "(KHTML, like Gecko) "
                        "Chrome/126.0 Safari/537.36"
                    )
                ]
            )

            # Sem ping_interval/ping_payload:
            # o keepalive da aplicação é Engine.IO textual.
            app.run_forever(
                origin="https://blaze.bet.br"
            )

        except Exception as exc:
            heartbeat_stop.set()
            with LOCK:
                ESTADO["ws_online"] = False
                ESTADO["ws_ultimo_erro"] = str(exc)

        heartbeat_stop.set()
        time.sleep(2)


def status_tempo_real():
    agora_epoch = time.time()

    with LOCK:
        todas = ordenar_rodadas_canonicas(
            list(ESTADO.get("rodadas", []))
        )

        live = [
            dict(item)
            for item in todas
            if str(item.get("origem", "")) == "blaze_websocket"
            and float(item.get("recebido_epoch", 0.0) or 0.0) > 0
        ]
        live.sort(
            key=lambda item: float(
                item.get("recebido_epoch", 0.0)
            )
        )

        recv_epoch = float(
            ESTADO.get("ws_ultimo_recebido_epoch", 0.0)
        )

        snapshot = {
            "ws_online": bool(ESTADO.get("ws_online", False)),
            "ultimo_recebido": str(
                ESTADO.get("ws_ultimo_recebido_brasilia", "")
            ),
            "processamento": ESTADO.get("ws_processamento_ms"),
            "eventos": int(ESTADO.get("ws_eventos_recebidos", 0)),
            "rolling": int(
                ESTADO.get("ws_eventos_rolling_recebidos", 0)
            ),
            "adicionadas": int(
                ESTADO.get("ws_rodadas_adicionadas", 0)
            ),
            "duplicadas": int(ESTADO.get("ws_duplicadas", 0)),
            "fora_ordem": int(
                ESTADO.get("ws_fora_de_ordem", 0)
            ),
            "lacunas": int(
                ESTADO.get("ws_suspeitas_gap", 0)
            ),
            "sinal": dict(
                ESTADO.get("ultimo_sinal", {"valido": False})
            ),
            "sinal_inicio": bool(
                ESTADO.get("sinal_recalculado_inicio", False)
            ),
            "sinal_erro": str(
                ESTADO.get("sinal_erro_inicio", "")
            ),
            "sinal_base_rodada_id": str(
                ESTADO.get("sinal_base_rodada_id", "")
            ),
            "sinal_base_data_hora": str(
                ESTADO.get("sinal_base_data_hora", "")
            ),
            "sinal_gerado_em": str(
                ESTADO.get("sinal_gerado_em", "")
            ),
            "sinal_calculo_ms": ESTADO.get("sinal_calculo_ms"),
            "sinal_seq": int(ESTADO.get("sinal_seq", 0)),
            "analise_em_andamento": bool(
                ESTADO.get("analise_em_andamento", False)
            ),
            "analise_pendente_id": str(
                ESTADO.get("analise_pendente_id", "")
            ),
            "analise_iniciada_em": str(
                ESTADO.get("analise_iniciada_em", "")
            )
        }

    saudavel = websocket_tempo_real_saudavel()

    motor_shadow = motor_estrategias(
        cores(todas)
    )
    avaliador_shadow = status_avaliador_shadow()
    entradas_status = status_entradas()
    continuidade_status = verificar_continuidade_captura()

    idade_s = (
        max(0.0, agora_epoch - recv_epoch)
        if recv_epoch > 0
        else None
    )

    ultimas_live = live[-20:]

    intervalo_ultimas_live_s = None
    atraso_fonte_servidor_s = None

    if ultimas_live:
        ultima_live = ultimas_live[-1]
        try:
            recebido_epoch_ultima = float(
                ultima_live.get("recebido_epoch", 0.0) or 0.0
            )
        except Exception:
            recebido_epoch_ultima = 0.0

        try:
            fonte_dt_ultima = _parse_iso_utc_para_datetime(
                ultima_live.get("timestamp_fonte", "")
            )
            fonte_epoch_ultima = (
                fonte_dt_ultima.timestamp()
                if fonte_dt_ultima
                else 0.0
            )
        except Exception:
            fonte_epoch_ultima = 0.0

        if recebido_epoch_ultima > 0 and fonte_epoch_ultima > 0:
            atraso_fonte_servidor_s = (
                recebido_epoch_ultima - fonte_epoch_ultima
            )

    if len(ultimas_live) >= 2:
        try:
            intervalo_ultimas_live_s = max(
                0.0,
                float(ultimas_live[-1].get("recebido_epoch", 0.0))
                - float(ultimas_live[-2].get("recebido_epoch", 0.0))
            )
        except Exception:
            intervalo_ultimas_live_s = None

    ultima_live_id = str(
        (ultimas_live[-1] if ultimas_live else {}).get("id", "")
    )
    sinal_sincronizado = bool(
        ultima_live_id
        and snapshot["sinal_base_rodada_id"] == ultima_live_id
    )

    return {
        "ok": True,
        "versao": "V53.9",
        "modo": "tempo_real_websocket",
        "ws_online": snapshot["ws_online"],
        "ws_saudavel": saudavel,
        "ultimo_recebido": snapshot["ultimo_recebido"],
        "idade_ultimo_resultado_segundos": idade_s,
        "processamento_ultimo_evento_ms": snapshot["processamento"],
        "eventos_double_tick": snapshot["eventos"],
        "resultados_rolling_recebidos": snapshot["rolling"],
        "rodadas_adicionadas_ws": snapshot["adicionadas"],
        "duplicadas_detectadas": snapshot["duplicadas"],
        "fora_de_ordem_detectadas": snapshot["fora_ordem"],
        "lacunas_maiores_60s": snapshot["lacunas"],
        "total_base": len(todas),
        "total_live_memoria": len(live),
        "limite_historico": LIMITE_HISTORICO,
        "sinal_recalculado_inicio": snapshot["sinal_inicio"],
        "sinal_erro_inicio": snapshot["sinal_erro"],
        "sinal_atual": snapshot["sinal"],
        "motor_estrategias_shadow": motor_shadow,
        "avaliador_shadow": avaliador_shadow,
        "entradas": entradas_status,
        "continuidade": continuidade_status,
        "sinal_sincronizado_com_ultima_rodada": sinal_sincronizado,
        "sinal_base_rodada_id": snapshot["sinal_base_rodada_id"],
        "sinal_base_data_hora": snapshot["sinal_base_data_hora"],
        "sinal_gerado_em": snapshot["sinal_gerado_em"],
        "sinal_calculo_ms": snapshot["sinal_calculo_ms"],
        "sinal_seq": snapshot["sinal_seq"],
        "analise_em_andamento": snapshot["analise_em_andamento"],
        "analise_pendente_id": snapshot["analise_pendente_id"],
        "analise_iniciada_em": snapshot["analise_iniciada_em"],
        "intervalo_ultimas_live_segundos": intervalo_ultimas_live_s,
        "atraso_fonte_servidor_segundos": atraso_fonte_servidor_s,
        "ultima_rodada_live": (
            ultimas_live[-1] if ultimas_live else None
        ),
        "ultimas_20_live": list(reversed(ultimas_live))
    }


def painel_tempo_real_html():
    return """<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Double — Tempo Real V53.9</title>
<style>
*{box-sizing:border-box}
body{font-family:Arial,sans-serif;background:#0d0f12;color:#f1f1f1;margin:0;padding:14px}
h1{font-size:25px;margin:8px 0 14px}
.card{background:#181b20;border:1px solid #292d34;border-radius:14px;padding:14px;margin-bottom:12px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.metric{background:#111419;border-radius:10px;padding:10px}
.label{font-size:12px;color:#9da5b1}
.value{font-size:18px;font-weight:700;margin-top:4px}
.good{color:#56db87}.bad{color:#ff7373}.warn{color:#ffd166}
.ball{display:inline-flex;align-items:center;justify-content:center;width:42px;height:42px;border-radius:50%;font-weight:800;margin-right:8px}
.R{background:#d83a4e}.B{background:#252a32}.W{background:#f3f3f3;color:#111}
.row{display:flex;align-items:center;border-bottom:1px solid #2a2e35;padding:9px 0}
.row:last-child{border-bottom:none}
.time{margin-left:auto;color:#aeb5bf;font-size:13px}
#signalMain{font-size:24px;font-weight:800}
.small{font-size:12px;color:#9da5b1;margin-top:6px}
</style>
</head>
<body>
<h1>Double — Tempo Real V53.9</h1>

<div class="card">
  <div id="health">Carregando...</div>
  <div class="grid" style="margin-top:10px">
    <div class="metric"><div class="label">Último resultado</div><div class="value" id="lastTime">-</div></div>
    <div class="metric"><div class="label">Idade do resultado</div><div class="value" id="age">-</div></div>
    <div class="metric"><div class="label">Pipeline servidor</div><div class="value" id="proc">-</div></div>
    <div class="metric"><div class="label">Base estatística</div><div class="value" id="base">-</div></div>
  </div>
</div>

<div class="card">
  <div class="label">PRÓXIMA RODADA — SINAL ESTATÍSTICO</div>
  <div id="signalMain">Aguardando cálculo...</div>
  <div class="small" id="signalDetail"></div>
</div>

<div class="card">
  <div class="label">QUALIDADE DA CAPTURA</div>
  <div class="small" id="quality"></div>
</div>

<div class="card">
  <div class="label">STRATEGY ENGINE V1 — SHADOW MODE</div>
  <div class="value" id="regime">-</div>
  <div class="small" id="strategySummary"></div>
  <div class="small" id="strategyVotes"></div>
</div>

<div class="card">
  <div class="label">AVALIADOR AUTOMÁTICO — SHADOW LIVE</div>
  <div class="value" id="shadowScore">Aguardando avaliações...</div>
  <div class="small" id="shadowPending"></div>
  <div class="small" id="shadowRanking"></div>
</div>

<div class="card">
  <div class="label">ENTRADAS — WIN / LOSE / GALE</div>
  <div class="value" id="entryScore">Sem entradas fechadas</div>
  <div class="small" id="entryPending"></div>
</div>

<div class="card">
  <div class="label">CONTINUIDADE 24H</div>
  <div class="value" id="continuity">Verificando...</div>
  <div class="small" id="continuityDetail"></div>
</div>

<div class="card">
  <b>Últimas rodadas recebidas AO VIVO</b>
  <div id="rounds" style="margin-top:8px"></div>
</div>

<div class="small">
Feed ao vivo e histórico ficam separados visualmente.
A base histórica continua sendo usada no modelo.
Sinais são probabilísticos e não garantem resultado.
</div>

<script>
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function nomeCor(c){return c==='R'?'VERMELHO':c==='B'?'PRETO':c==='W'?'BRANCO':'-';}
async function atualizar(){
  try{
    const r=await fetch('/tempo-real-json',{cache:'no-store'});
    const d=await r.json();

    const healthy=d.ws_online && d.ws_saudavel;
    document.getElementById('health').innerHTML =
      '<b class="'+(healthy?'good':'bad')+'">WebSocket: '+(healthy?'ONLINE / SAUDÁVEL':'ATENÇÃO')+'</b>';

    document.getElementById('lastTime').textContent=d.ultimo_recebido||'-';
    document.getElementById('age').textContent=
      d.idade_ultimo_resultado_segundos==null?'-':d.idade_ultimo_resultado_segundos.toFixed(1)+' s';
    document.getElementById('proc').textContent=
      d.processamento_ultimo_evento_ms==null?'-':d.processamento_ultimo_evento_ms.toFixed(1)+' ms';
    document.getElementById('base').textContent=d.total_base+' / '+d.limite_historico;

    const s=d.sinal_atual||{};

    if(!d.sinal_sincronizado_com_ultima_rodada){
      document.getElementById('signalMain').textContent='ATUALIZANDO SINAL...';
      document.getElementById('signalDetail').textContent=
        'Cálculo rápido da próxima rodada em segundo plano. A captura WebSocket continua livre para receber novos resultados.';
    }else if(s.valido){
      document.getElementById('signalMain').textContent=nomeCor(s.cor);
      document.getElementById('signalDetail').textContent=
        'SINAL PARA A PRÓXIMA RODADA | Base: '+(d.sinal_base_data_hora||'-')+
        ' | Gerado: '+(d.sinal_gerado_em||'-')+
        ' | Cálculo: '+(d.sinal_calculo_ms==null?'-':Number(d.sinal_calculo_ms).toFixed(0)+' ms')+
        ' | Probabilidade do modelo: '+((Number(s.probabilidade)||0)*100).toFixed(1)+'%'+
        ' | Amostras: '+(s.amostras??'-')+
        ' | Concordância: '+(s.concordancia_modelos??'-')+'/'+(s.total_modelos??'-');
    }else{
      document.getElementById('signalMain').textContent='SEM ENTRADA';
      const motivos=(s.motivos_rejeicao||[]);
      const frescorTxt = s.frescor_ok===false
        ? ' | BLOQUEADO POR LATÊNCIA'
        : '';
      document.getElementById('signalDetail').textContent=
        'ANÁLISE ATUALIZADA PARA A PRÓXIMA RODADA | Base: '+(d.sinal_base_data_hora||'-')+
        ' | Gerado: '+(d.sinal_gerado_em||'-')+
        ' | Candidato estatístico: '+nomeCor(s.cor)+
        frescorTxt+
        ' | Motivo(s): '+(motivos.length?motivos.join(' ; '):'filtros de segurança não aprovaram a entrada');
    }

    const intervaloLive = d.intervalo_ultimas_live_segundos==null
      ? '-'
      : Number(d.intervalo_ultimas_live_segundos).toFixed(1)+' s';

    const atrasoFonte = d.atraso_fonte_servidor_segundos==null
      ? '-'
      : Number(d.atraso_fonte_servidor_segundos).toFixed(3)+' s';

    document.getElementById('quality').textContent=
      'Intervalo entre as 2 últimas capturas: '+intervaloLive+
      ' | Atraso fonte→servidor: '+atrasoFonte+
      ' | Rolling recebidos: '+d.resultados_rolling_recebidos+
      ' | Adicionados: '+d.rodadas_adicionadas_ws+
      ' | Duplicadas: '+d.duplicadas_detectadas+
      ' | Fora de ordem: '+d.fora_de_ordem_detectadas+
      ' | Lacunas >60s: '+d.lacunas_maiores_60s;

    const motor=d.motor_estrategias_shadow||{};
    const reg=motor.regime||{};
    document.getElementById('regime').textContent=
      'Regime: '+String(reg.apelido||reg.regime||'-').toUpperCase()+
      ' | Anomalia: '+Number(reg.score_anomalia||0).toFixed(1)+'/100';

    document.getElementById('strategySummary').textContent=
      'Candidato shadow: '+nomeCor(motor.candidato)+
      ' | Consenso: '+(Number(motor.confianca_consenso||0)*100).toFixed(1)+'%'+
      ' | Estratégias votando: '+(motor.estrategias_ativas||0)+'/'+(motor.estrategias_total||0)+
      ' | Sequência: '+(reg.sequencia_tamanho||0)+' '+nomeCor(reg.sequencia_cor);

    const votos=(motor.votos||[]).filter(x=>x.voto);
    document.getElementById('strategyVotes').textContent=
      votos.length
      ? votos.map(x=>x.nome+': '+nomeCor(x.voto)+' '+(Number(x.confianca||0)*100).toFixed(1)+'%').join(' | ')
      : 'Nenhuma estratégia com condição ativa agora.';

    const av=d.avaliador_shadow||{};
    const geral=av.candidato_ensemble_geral||{};
    const ult50=av.candidato_ensemble_ultimas_50||{};
    document.getElementById('shadowScore').textContent=
      'Ensemble: '+(geral.acertos||0)+'/'+(geral.entradas||0)+
      ' ('+Number(geral.taxa_acerto||0).toFixed(1)+'%)'+
      ' | Últimas 50: '+Number(ult50.taxa_acerto||0).toFixed(1)+'%';

    const pend=av.previsao_pendente||{};
    document.getElementById('shadowPending').textContent=
      pend.candidato
      ? 'Próxima previsão pendente: '+nomeCor(pend.candidato)+
        ' | Consenso '+(Number(pend.consenso||0)*100).toFixed(1)+'%'+
        ' | Regime '+String(pend.regime||'-').toUpperCase()
      : 'Nenhuma previsão pendente ainda.';

    const rank=(av.ranking_estrategias||[]).slice(0,3);
    document.getElementById('shadowRanking').textContent=
      rank.length
      ? 'Top shadow: '+rank.map(x=>
          x.nome+' '+Number((x.geral||{}).taxa_acerto||0).toFixed(1)+'%'+
          ' ('+((x.geral||{}).entradas||0)+' entradas)'
        ).join(' | ')
      : 'Aguardando histórico shadow suficiente.';

    const ent=d.entradas||{};
    document.getElementById('entryScore').textContent=
      'WIN '+(ent.wins||0)+' | LOSE '+(ent.loses||0)+
      ' | Taxa '+Number(ent.taxa_win||0).toFixed(1)+'%'+
      ' | Sem Gale '+(ent.wins_sem_gale||0)+
      ' | Gale 1 '+(ent.entradas_com_gale_1||0);

    const ep=ent.entrada_pendente||{};
    document.getElementById('entryPending').textContent=
      ep.id ? 'Pendente: '+nomeCor(ep.cor_sinal)+' | Estado '+String(ep.gale||'PENDENTE')
      : 'Nenhuma entrada pendente.';

    const cont=d.continuidade||{};
    document.getElementById('continuity').textContent=
      cont.interrompida ? 'CAPTURA INTERROMPIDA' : 'CAPTURA CONTÍNUA';
    document.getElementById('continuity').className=
      cont.interrompida ? 'value bad' : 'value good';
    document.getElementById('continuityDetail').textContent=
      'Idade do último resultado: '+
      (cont.idade_ultimo_resultado_segundos==null?'-':Number(cont.idade_ultimo_resultado_segundos).toFixed(1)+' s')+
      ' | Limite de alerta: '+Number(cont.limite_segundos||0).toFixed(0)+' s';

    const rounds=d.ultimas_20_live||[];
    document.getElementById('rounds').innerHTML = rounds.length ? rounds.map(x=>
      '<div class="row"><span class="ball '+esc(x.cor)+'">'+esc(x.numero??nomeCor(x.cor)[0])+'</span>'+
      '<div><b>'+esc(nomeCor(x.cor))+'</b><div class="small">'+esc(x.id)+'</div></div>'+
      '<div class="time">Blaze: '+esc(x.data_hora||'-')+'<br>Servidor: '+esc(x.recebido_em_brasilia||'-')+'</div></div>'
    ).join('') : '<div class="small">Aguardando a primeira rodada ao vivo após este deploy...</div>';
  }catch(e){
    document.getElementById('health').innerHTML='<b class="bad">Erro ao atualizar: '+esc(e)+'</b>';
  }
}
atualizar();
setInterval(atualizar,750);
</script>
</body></html>"""




def diagnostico_websocket():
    with LOCK:
        return {
            "ok": True,
            "driver_websocket": websocket_disponivel(),
            "online": bool(
                ESTADO.get("ws_online", False)
            ),
            "ultimo_evento": str(
                ESTADO.get("ws_ultimo_evento", "")
            ),
            "ultima_rodada": str(
                ESTADO.get("ws_ultima_rodada", "")
            ),
            "ultimo_erro": str(
                ESTADO.get("ws_ultimo_erro", "")
            ),
            "mensagens_raw": int(
                ESTADO.get(
                    "ws_mensagens_raw",
                    0
                )
            ),
            "ultimo_raw": str(
                ESTADO.get("ws_ultimo_raw", "")
            ),
            "endpoint_atual": str(
                ESTADO.get("ws_endpoint_atual", "")
            ),
            "sala": "double_room_1",
            "pacote_assinatura": "420",
            "formato_evento": "data -> id=double.tick",
            "heartbeat": "Engine.IO texto 2 a cada 10s",
            "handshake_recebido": bool(
                ESTADO.get(
                    "ws_handshake_recebido",
                    False
                )
            ),
            "assinaturas_enviadas": int(
                ESTADO.get(
                    "ws_assinaturas_enviadas",
                    0
                )
            ),
            "eventos_recebidos": int(
                ESTADO.get(
                    "ws_eventos_recebidos",
                    0
                )
            ),
            "rodadas_adicionadas": int(
                ESTADO.get(
                    "ws_rodadas_adicionadas",
                    0
                )
            ),
            "nota_timestamp_fonte": (
                "updated_at da fonte não é tratado como latência; "
                "o relógio operacional é o recebimento no servidor"
            ),
            "processamento_ms": ESTADO.get(
                "ws_processamento_ms"
            ),
            "duplicadas": int(
                ESTADO.get("ws_duplicadas", 0)
            ),
            "fora_de_ordem": int(
                ESTADO.get("ws_fora_de_ordem", 0)
            ),
            "lacunas_maiores_60s": int(
                ESTADO.get("ws_suspeitas_gap", 0)
            ),
            "rolling_recebidos": int(
                ESTADO.get("ws_eventos_rolling_recebidos", 0)
            ),
            "ultimo_recebido_brasilia": str(
                ESTADO.get("ws_ultimo_recebido_brasilia", "")
            ),
            "total_memoria": len(
                ESTADO.get("rodadas", [])
            )
        }


def buscar_feed_fallback_bestblaze():
    """
    Fallback do coletor ao vivo.

    Usa o mesmo fluxo de sessão/cookies/CSRF/formulário já validado
    pela importação histórica. Consulta somente uma janela recente e
    processa apenas as últimas rodadas retornadas.

    Se o formulário do site aceitar apenas 'date', o servidor pode retornar
    o dia inteiro; nesse caso limitamos localmente às últimas 40 rodadas.
    """
    agora = datetime.now(
        timezone(timedelta(hours=-3))
    )
    inicio = agora - timedelta(minutes=20)
    fim = agora

    html, form, payload, diag = buscar_periodo_bestblaze(
        inicio,
        fim
    )

    rodadas = extrair_bestblaze_historico_html(html)

    # Segurança: nunca reprocessar uma página inteira no loop ao vivo.
    rodadas = rodadas[-40:]

    adicionadas = 0

    for rodada in rodadas:
        if adicionar_rodada(rodada):
            adicionadas += 1

    with LOCK:
        ESTADO["coletor_fallback_ultimo"] = agora_brasilia()
        ESTADO["coletor_fallback_adicionadas"] = adicionadas
        ESTADO["coletor_ultimo_modo"] = "fallback_bestblaze"
        ESTADO["coletor_ultimo_erro"] = ""

    return {
        "ok": True,
        "adicionadas": adicionadas,
        "reconhecidas": len(rodadas),
        "status_get": int(diag.get("status_get", 0)),
        "status_envio": int(diag.get("status_envio", 0)),
        "tipo_filtro": str(
            diag.get("tipo_filtro_enviado", "")
        )
    }


def diagnostico_coletor():
    cfg = carregar_config()

    with LOCK:
        estado = {
            "fonte_online": bool(
                ESTADO.get("fonte_online", False)
            ),
            "ultimo_erro_fonte": str(
                ESTADO.get("ultimo_erro_fonte", "")
            ),
            "ultima_consulta_fonte": str(
                ESTADO.get("ultima_consulta_fonte", "")
            ),
            "ultima_rodada_fonte": str(
                ESTADO.get("ultima_rodada_fonte", "")
            ),
            "ciclos_sem_novas": int(
                ESTADO.get("coletor_ciclos_sem_novas", 0)
            ),
            "fallback_ultimo": str(
                ESTADO.get("coletor_fallback_ultimo", "")
            ),
            "fallback_adicionadas": int(
                ESTADO.get("coletor_fallback_adicionadas", 0)
            ),
            "ultimo_modo": str(
                ESTADO.get("coletor_ultimo_modo", "")
            ),
            "ultimo_erro": str(
                ESTADO.get("coletor_ultimo_erro", "")
            ),
            "total_memoria": len(
                ESTADO.get("rodadas", [])
            )
        }

    return {
        "ok": True,
        "intervalo_segundos": int(
            cfg.get("intervalo_segundos", 10)
        ),
        "modo_fonte_configurado": str(
            cfg.get("modo_fonte", "")
        ),
        "resultados_url_configurada": bool(
            str(cfg.get("resultados_url", "")).strip()
            or os.getenv("RESULTADOS_URL", "").strip()
        ),
        "fallback_apos_ciclos": 3,
        "fallback_intervalo_minimo_segundos": 60,
        "estado": estado,
        "postgres": postgres_status()
    }


def buscar_feed():
    cfg = carregar_config()
    url = str(cfg.get("resultados_url", "")).strip() or os.getenv("RESULTADOS_URL", "").strip()
    if "bestblaze.com.br" in url.lower():
        url = _normalizar_url_bestblaze(url)
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
    ciclos_sem_novas = 0
    ultimo_fallback_epoch = 0.0

    while True:
        if websocket_tempo_real_saudavel():
            time.sleep(10)
            continue

        cfg = carregar_config()
        intervalo = int(
            cfg.get(
                "intervalo_segundos",
                os.getenv("INTERVALO_SEGUNDOS", "10")
            )
        )

        adicionadas = 0

        try:
            adicionadas = buscar_feed()

            if adicionadas > 0:
                ciclos_sem_novas = 0

                with LOCK:
                    ESTADO["coletor_ciclos_sem_novas"] = 0
                    ESTADO["coletor_ultimo_modo"] = "fonte_principal"
                    ESTADO["coletor_ultimo_erro"] = ""

                print(
                    "Rodadas novas pela fonte principal:",
                    adicionadas
                )
            else:
                ciclos_sem_novas += 1

                with LOCK:
                    ESTADO["coletor_ciclos_sem_novas"] = (
                        ciclos_sem_novas
                    )

        except Exception as exc:
            ciclos_sem_novas += 1

            with LOCK:
                ESTADO["coletor_ciclos_sem_novas"] = (
                    ciclos_sem_novas
                )
                ESTADO["coletor_ultimo_erro"] = str(exc)

            print("Erro no monitor principal:", exc)

        # Depois de 3 ciclos sem novas rodadas, usa fallback.
        # Nunca executa o fallback mais de uma vez por minuto.
        agora_epoch = time.time()

        if (
            ciclos_sem_novas >= 3
            and agora_epoch - ultimo_fallback_epoch >= 60
        ):
            try:
                resultado = buscar_feed_fallback_bestblaze()
                ultimo_fallback_epoch = agora_epoch

                novas_fallback = int(
                    resultado.get("adicionadas", 0)
                )

                if novas_fallback > 0:
                    ciclos_sem_novas = 0

                    with LOCK:
                        ESTADO["coletor_ciclos_sem_novas"] = 0

                    print(
                        "Rodadas novas pelo fallback:",
                        novas_fallback
                    )

            except Exception as exc:
                ultimo_fallback_epoch = agora_epoch

                with LOCK:
                    ESTADO["coletor_ultimo_erro"] = str(exc)

                print("Erro no fallback BestBlaze:", exc)

        time.sleep(max(5, intervalo))



def worker_reconciliacao_recente():
    """
    Rede de segurança V58.0.

    O WebSocket continua sendo a fonte rápida.
    A cada 45s, independente de o WS parecer saudável, consulta a janela
    recente do histórico público para recuperar alguma rodada que o WS tenha
    perdido durante reconexão, deploy ou oscilação.

    Não inventa rodadas: só chama o parser/fonte pública já existente.
    """
    # espera o boot estabilizar
    time.sleep(15)

    while True:
        try:
            resultado = buscar_feed_fallback_bestblaze()
            adicionadas = int(resultado.get("adicionadas", 0))

            if adicionadas > 0:
                print(
                    "V58.0 reconciliação recuperou",
                    adicionadas,
                    "rodada(s) real(is)",
                    flush=True
                )

            with LOCK:
                ESTADO["ultima_verificacao_continuidade"] = agora_brasilia()

        except Exception as exc:
            with LOCK:
                ESTADO["coletor_ultimo_erro"] = (
                    "reconciliacao_recente: %s" % str(exc)
                )
            print("V58.0 reconciliação erro:", exc, flush=True)

        time.sleep(45)


def heartbeat_public_url():
    """URL publica usada para gerar trafego HTTP de entrada periodico."""
    url = os.getenv("HEARTBEAT_URL", "").strip()
    if not url:
        url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not url:
        return ""
    return url.rstrip("/") + "/heartbeat"


def worker_heartbeat():
    """Faz uma requisicao HTTP pela URL publica a cada poucos minutos.

    O recurso e configuravel por HEARTBEAT_ATIVO e HEARTBEAT_INTERVALO_SEGUNDOS.
    Ele nao substitui a recuperacao de lacunas do historico; apenas cria trafego
    HTTP de entrada enquanto o processo estiver executando.
    """
    global ULTIMO_HEARTBEAT
    ativo = os.getenv("HEARTBEAT_ATIVO", "1").strip().lower() not in ("0", "false", "nao", "não", "off")
    if not ativo:
        print("Heartbeat: desativado por HEARTBEAT_ATIVO", flush=True)
        return
    try:
        intervalo = max(60, int(os.getenv("HEARTBEAT_INTERVALO_SEGUNDOS", "300")))
    except Exception:
        intervalo = 300

    # aguarda o HTTP server subir
    time.sleep(20)
    while True:
        url = heartbeat_public_url()
        ULTIMO_HEARTBEAT["url"] = url
        ULTIMO_HEARTBEAT["tentativa"] = agora_brasilia()
        if not url:
            ULTIMO_HEARTBEAT["erro"] = "HEARTBEAT_URL/RENDER_EXTERNAL_URL nao configurada"
            time.sleep(intervalo)
            continue
        try:
            req = Request(url, headers={"User-Agent": "DoubleBlazeIA-Heartbeat/1.0", "Cache-Control": "no-cache"})
            with urlopen(req, timeout=15) as resp:
                resp.read(256)
            ULTIMO_HEARTBEAT["sucesso"] = agora_brasilia()
            ULTIMO_HEARTBEAT["erro"] = ""
            print("Heartbeat OK:", ULTIMO_HEARTBEAT["sucesso"], flush=True)
        except Exception as exc:
            ULTIMO_HEARTBEAT["erro"] = str(exc)[:300]
            print("Heartbeat ERRO:", ULTIMO_HEARTBEAT["erro"], flush=True)
        time.sleep(intervalo)



# ================================================================
# V56.0 - OTIMIZADOR EXATO REMOTO
# Move o processamento pesado do Godot/Android para o servidor.
# O servidor mantém cache por histórico+filtros e executa em thread.
# ================================================================
OPTIMIZER_JOBS = {}
OPTIMIZER_JOBS_LOCK = threading.Lock()
OPTIMIZER_RESULT_CACHE = {}
OPTIMIZER_RANKING_CACHE = {}
OPTIMIZER_RANKING_CACHE_LOCK = threading.Lock()
OPTIMIZER_CACHE_LOCK = threading.Lock()
OPTIMIZER_CACHE_FILE = BASE / "optimizer_result_cache.json"


def _optimizer_load_cache():
    global OPTIMIZER_RESULT_CACHE
    try:
        if OPTIMIZER_CACHE_FILE.exists():
            raw = json.loads(OPTIMIZER_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                OPTIMIZER_RESULT_CACHE = raw
    except Exception as exc:
        print("Optimizer cache: não foi possível carregar:", exc)


def _optimizer_save_cache():
    try:
        with OPTIMIZER_CACHE_LOCK:
            # Mantém somente os 20 resultados mais recentes para não crescer sem limite.
            items = list(OPTIMIZER_RESULT_CACHE.items())[-20:]
            data = dict(items)
        OPTIMIZER_CACHE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8"
        )
    except Exception as exc:
        print("Optimizer cache: não foi possível salvar:", exc)


def _optimizer_colors():
    # V56.7: SOMENTE o otimizador usa o histórico completo já armazenado.
    # Não altera, apaga, repara ou grava nenhuma rodada.
    with LOCK:
        rows = ordenar_rodadas_canonicas(
            list(ESTADO.get("rodadas", []))
        )[-LIMITE_HISTORICO:]

    colors = []
    ids = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        c = str(r.get("cor", "")).upper()
        if c not in ("R", "B", "W"):
            continue
        colors.append(c)
        ids.append(str(r.get("id", "")) + ":" + c)
    return colors, ids


def _optimizer_occurrences(colors):
    n = len(colors)
    occ = {}
    # Máscara compatível com o Godot: bit 0 = primeiro símbolo do padrão.
    for plen in range(1, 13):
        buckets = [[] for _ in range(1 << plen)]
        for signal_index in range(plen, n):
            code = 0
            valid = True
            start = signal_index - plen
            for pos in range(plen):
                c = colors[start + pos]
                if c == "W":
                    valid = False
                    break
                if c == "B":
                    code |= (1 << pos)
            if valid:
                buckets[code].append(signal_index)
        for mask, positions in enumerate(buckets):
            occ[(plen, mask)] = positions
    return occ


def _optimizer_pattern_from_mask(plen, mask):
    return ["B" if ((mask >> pos) & 1) else "R" for pos in range(plen)]


def _optimizer_risk_stop(cfg, bank, peak, recent):
    if bool(cfg.get("risk_trailing_enabled", False)) and peak > 0:
        pct = float(cfg.get("risk_trailing_pct", 15.0))
        if 100.0 * (peak - bank) / peak >= pct:
            return True
    if bool(cfg.get("risk_capital_enabled", False)):
        if bank <= float(cfg.get("risk_capital_floor", cfg.get("initial_bank", 1000.0))):
            return True
    if bool(cfg.get("risk_performance_enabled", False)):
        window = max(1, int(cfg.get("risk_performance_window", 20)))
        if len(recent) >= window:
            block = recent[-window:]
            rate = 100.0 * sum(1 for x in block if x) / float(window)
            if rate < float(cfg.get("risk_performance_min_rate", 50.0)):
                return True
    return False


def _optimizer_simulate_mask(colors, signal_mask, targets, cfg, gale, mode):
    n = len(colors)
    stake_base = float(cfg.get("stake", 10.0))
    initial_bank = float(cfg.get("initial_bank", 1000.0))
    bank = initial_bank
    peak = bank
    min_bank = bank
    max_dd = 0.0
    peak_at_max_dd = bank
    bank_at_max_dd = bank
    entries = wins = losses = 0
    gale_wins = [0] * (gale + 1)
    max_gale_reached = 0
    largest_stake = 0.0
    max_cycle_exposure = 0.0
    bankrupt_stops = 0
    recent = []

    def place_bet(idx, target, g, exposure):
        nonlocal bank, peak, min_bank, max_dd, peak_at_max_dd, bank_at_max_dd
        nonlocal wins, losses, max_gale_reached, largest_stake, max_cycle_exposure, bankrupt_stops
        stake = stake_base * (2.0 ** g)
        if bank < stake:
            bankrupt_stops += 1
            losses += 1
            return False, True, exposure
        max_gale_reached = max(max_gale_reached, g)
        largest_stake = max(largest_stake, stake)
        exposure += stake
        max_cycle_exposure = max(max_cycle_exposure, exposure)
        bank -= stake
        min_bank = min(min_bank, bank)
        dd = peak - bank
        if dd > max_dd:
            max_dd = dd
            peak_at_max_dd = peak
            bank_at_max_dd = bank
        if 0 <= idx < n and colors[idx] == target:
            bank += stake * 2.0
            wins += 1
            gale_wins[g] += 1
            peak = max(peak, bank)
            return True, False, exposure
        return False, False, exposure

    # Tradicional / pular rodadas: sinal inicia o ciclo; Gales usam o mesmo alvo.
    if mode == 0 or 2 <= mode <= 10:
        skip_rounds = (mode - 1) if mode >= 2 else 0
        search_from = 0
        while search_from < n:
            idx = signal_mask.find(b'\x01', search_from)
            if idx < 0:
                break
            target_code = targets[idx]
            if target_code == 0:
                search_from = idx + 1
                continue
            target = "R" if target_code == 1 else "B"
            entries += 1
            exposure = 0.0
            bet_idx = idx
            cycle_done = False
            for g in range(gale + 1):
                won, broke, exposure = place_bet(bet_idx, target, g, exposure)
                if broke:
                    recent.append(False)
                    cycle_done = True
                    break
                if won:
                    recent.append(True)
                    cycle_done = True
                    break
                if g >= gale:
                    losses += 1
                    recent.append(False)
                    cycle_done = True
                    break
                bet_idx += (skip_rounds + 1) if mode >= 2 else 1
                if bet_idx >= n:
                    cycle_done = True
                    break
            if _optimizer_risk_stop(cfg, bank, peak, recent):
                break
            search_from = max(idx + 1, bet_idx + 1)

    # Próximo sinal / pular sinais: cada Gale espera outro sinal válido.
    else:
        skip_signals = 0 if mode == 1 else max(1, mode - 10)
        search_from = 0
        while search_from < n:
            idx = signal_mask.find(b'\x01', search_from)
            if idx < 0:
                break
            entries += 1
            exposure = 0.0
            current_idx = idx
            cycle_finished = False
            for g in range(gale + 1):
                target_code = targets[current_idx]
                target = "R" if target_code == 1 else "B"
                won, broke, exposure = place_bet(current_idx, target, g, exposure)
                if broke:
                    recent.append(False)
                    cycle_finished = True
                    break
                if won:
                    recent.append(True)
                    cycle_finished = True
                    break
                if g >= gale:
                    losses += 1
                    recent.append(False)
                    cycle_finished = True
                    break
                # Pula N sinais e executa no seguinte.
                nxt = current_idx + 1
                for _ in range(skip_signals + 1):
                    nxt = signal_mask.find(b'\x01', nxt)
                    if nxt < 0:
                        break
                    if _ < skip_signals:
                        nxt += 1
                if nxt < 0:
                    cycle_finished = True
                    current_idx = n
                    break
                current_idx = nxt
            if _optimizer_risk_stop(cfg, bank, peak, recent):
                break
            search_from = current_idx + 1 if current_idx < n else n

    rate = 100.0 * wins / entries if entries else 0.0
    dd_pct = 100.0 * max_dd / peak_at_max_dd if peak_at_max_dd > 0 else 0.0
    return {
        "entries": entries, "wins": wins, "losses": losses,
        "gale_wins": gale_wins, "profit": bank - initial_bank,
        "rate": rate, "bank": bank, "max_dd": max_dd,
        "max_gale_reached": max_gale_reached,
        "largest_stake": largest_stake,
        "max_cycle_exposure": max_cycle_exposure,
        "min_bank": min_bank, "bankrupt_stops": bankrupt_stops,
        "peak_bank": peak, "bank_at_max_dd": bank_at_max_dd,
        "peak_at_max_dd": peak_at_max_dd, "max_dd_pct": dd_pct,
        "broke_bank": bankrupt_stops > 0
    }



@njit(cache=True, fastmath=True, nogil=True)
def _optimizer_numba_core(colors, positions, targets, gale, mode, stake_base, initial_bank):
    n = colors.shape[0]
    m = positions.shape[0]
    bank = initial_bank
    peak = bank
    min_bank = bank
    max_dd = 0.0
    peak_at_max_dd = bank
    bank_at_max_dd = bank
    entries = wins = losses = bankrupt_stops = 0
    max_gale_reached = 0
    largest_stake = 0.0
    max_cycle_exposure = 0.0
    gale_wins = np.zeros(gale + 1, dtype=np.int64)

    p = 0
    while p < m:
        idx = int(positions[p])
        target = int(targets[idx])
        if target == 0:
            p += 1
            continue

        entries += 1
        exposure = 0.0
        cur = idx

        for g in range(gale + 1):
            stake = stake_base * (2.0 ** g)
            if bank < stake:
                bankrupt_stops += 1
                losses += 1
                break

            if g > max_gale_reached:
                max_gale_reached = g
            if stake > largest_stake:
                largest_stake = stake

            exposure += stake
            if exposure > max_cycle_exposure:
                max_cycle_exposure = exposure

            bank -= stake
            if bank < min_bank:
                min_bank = bank

            dd = peak - bank
            if dd > max_dd:
                max_dd = dd
                peak_at_max_dd = peak
                bank_at_max_dd = bank

            actual = colors[cur] if 0 <= cur < n else 0
            if actual == target:
                bank += stake * 2.0
                wins += 1
                gale_wins[g] += 1
                if bank > peak:
                    peak = bank
                break

            if g >= gale:
                losses += 1
                break

            if mode == 1 or mode >= 11:
                skip_signals = 0 if mode == 1 else max(1, mode - 10)
                next_p = p + skip_signals + 1
                if next_p >= m:
                    cur = n
                    break
                cur = int(positions[next_p])
            else:
                skip_rounds = (mode - 1) if mode >= 2 else 0
                cur += (skip_rounds + 1) if mode >= 2 else 1
                if cur >= n:
                    break

        if mode == 1 or mode >= 11:
            p += 1
        else:
            search_from = max(idx + 1, cur + 1)
            lo = p + 1
            hi = m
            while lo < hi:
                mid = (lo + hi) // 2
                if positions[mid] < search_from:
                    lo = mid + 1
                else:
                    hi = mid
            p = lo

    rate = 100.0 * wins / entries if entries else 0.0
    return (entries, wins, losses, gale_wins, bank - initial_bank, rate, bank,
            max_dd, max_gale_reached, largest_stake, max_cycle_exposure,
            min_bank, bankrupt_stops, peak, bank_at_max_dd, peak_at_max_dd)



@njit(cache=True, fastmath=True, nogil=True)
def _optimizer_sweep_chunk_numba(
    colors,
    signal_matrix,
    entry_codes,
    votes_r,
    votes_b,
    start_count,
    end_count,
    consensuses,
    gale,
    mode,
    stake_base,
    initial_bank,
    contrary,
    min_rate_enabled,
    min_rate,
    rate_mode,
    objective_mode,
    dd_enabled,
    dd_limit
):
    """
    V56.5: testa um bloco inteiro de quantidades x consensos dentro do JIT.
    Nenhuma configuração é amostrada ou descartada.
    votes_r/votes_b são carregados entre blocos, permitindo progresso na UI.
    """
    n = colors.shape[0]
    # V58.9 objective_mode:
    # 0 = maior lucro (maior profit)
    # 1 = maior prejuízo (menor profit)
    best_profit = 1.0e300 if objective_mode == 1 else -1.0e300
    best_dd = 1.0e300
    best_count = -1
    best_con = -1
    best_rate = 0.0
    best_entries = 0
    best_wins = 0
    best_losses = 0
    best_bank = initial_bank
    best_max_gale = 0
    best_largest_stake = 0.0
    best_exposure = 0.0
    best_min_bank = initial_bank
    best_bankrupt = 0
    best_peak = initial_bank
    best_bank_at_dd = initial_bank
    best_peak_at_dd = initial_bank
    best_gale_wins = np.zeros(gale + 1, dtype=np.int64)
    tested = 0

    targets = np.zeros(n, dtype=np.uint8)
    positions = np.empty(n, dtype=np.int32)

    for count0 in range(start_count, end_count):
        row = signal_matrix[count0]
        entry = int(entry_codes[count0])

        # Somente a estratégia recém-adicionada altera os votos.
        for i in range(n):
            if row[i] != 0:
                if entry == 1:
                    votes_r[i] += 1
                else:
                    votes_b[i] += 1

        count = count0 + 1

        for ci in range(consensuses.shape[0]):
            con = int(consensuses[ci])
            m = 0

            # Gera o consenso e a lista esparsa num único passe nativo.
            for i in range(n):
                vr = int(votes_r[i])
                vb = int(votes_b[i])
                active = vr + vb
                t = 0
                if active > 0 and vr != vb:
                    winner = vr if vr > vb else vb
                    # comparação inteira exata; empate já foi excluído
                    if winner * 100 >= active * con:
                        t = 1 if vr > vb else 2
                        if contrary:
                            t = 2 if t == 1 else 1
                targets[i] = t
                if t != 0:
                    positions[m] = i
                    m += 1

            vals = _optimizer_numba_core(
                colors,
                positions[:m],
                targets,
                gale,
                mode,
                stake_base,
                initial_bank
            )
            (entries, wins, losses, gale_wins, profit, rate, bank, max_dd,
             max_gale_reached, largest_stake, max_cycle_exposure, min_bank,
             bankrupt_stops, peak_bank, bank_at_max_dd, peak_at_max_dd) = vals

            tested += 1

            # V58.2: configuração sem entrada não é estratégia válida.
            # No modo ACERTO MÁXIMO, 0 entradas = 0% e R$0 poderia
            # vencer estratégias reais negativas. Isso agora é bloqueado.
            passes = entries > 0

            if min_rate_enabled:
                if rate_mode == 1:
                    if rate - 0.0001 > min_rate:
                        passes = False
                else:
                    if rate + 0.0001 < min_rate:
                        passes = False
            if dd_enabled and max_dd > dd_limit:
                passes = False

            better_financial = (
                profit < best_profit
                if objective_mode == 1
                else profit > best_profit
            )
            if passes and (
                better_financial or
                (abs(profit - best_profit) < 1e-9 and max_dd < best_dd)
            ):
                best_profit = profit
                best_dd = max_dd
                best_count = count
                best_con = con
                best_rate = rate
                best_entries = entries
                best_wins = wins
                best_losses = losses
                best_bank = bank
                best_max_gale = max_gale_reached
                best_largest_stake = largest_stake
                best_exposure = max_cycle_exposure
                best_min_bank = min_bank
                best_bankrupt = bankrupt_stops
                best_peak = peak_bank
                best_bank_at_dd = bank_at_max_dd
                best_peak_at_dd = peak_at_max_dd
                for gi in range(gale + 1):
                    best_gale_wins[gi] = gale_wins[gi]

    return (
        tested,
        best_profit, best_dd, best_count, best_con, best_rate,
        best_entries, best_wins, best_losses, best_bank,
        best_max_gale, best_largest_stake, best_exposure,
        best_min_bank, best_bankrupt, best_peak,
        best_bank_at_dd, best_peak_at_dd, best_gale_wins
    )


def _optimizer_simulate_numba(colors, signal_positions, targets, cfg, gale, mode):
    # Se algum stop dinâmico estiver ligado, preserva o simulador Python antigo.
    if (bool(cfg.get("risk_trailing_enabled", False)) or
        bool(cfg.get("risk_capital_enabled", False)) or
        bool(cfg.get("risk_performance_enabled", False))):
        return _optimizer_simulate_sparse(colors, signal_positions, targets, cfg, gale, mode)

    positions = np.asarray(signal_positions, dtype=np.int32)
    targets_np = np.asarray(targets, dtype=np.uint8)
    vals = _optimizer_numba_core(
        np.asarray(colors, dtype=np.uint8),
        positions, targets_np, int(gale), int(mode),
        float(cfg.get("stake", 10.0)),
        float(cfg.get("initial_bank", 1000.0))
    )
    (entries, wins, losses, gale_wins, profit, rate, bank, max_dd,
     max_gale_reached, largest_stake, max_cycle_exposure, min_bank,
     bankrupt_stops, peak_bank, bank_at_max_dd, peak_at_max_dd) = vals

    dd_pct = 100.0 * float(max_dd) / float(peak_at_max_dd) if float(peak_at_max_dd) > 0 else 0.0
    return {
        "entries": int(entries), "wins": int(wins), "losses": int(losses),
        "gale_wins": [int(x) for x in gale_wins.tolist()],
        "profit": float(profit), "rate": float(rate), "bank": float(bank),
        "max_dd": float(max_dd), "max_gale_reached": int(max_gale_reached),
        "largest_stake": float(largest_stake),
        "max_cycle_exposure": float(max_cycle_exposure),
        "min_bank": float(min_bank), "bankrupt_stops": int(bankrupt_stops),
        "peak_bank": float(peak_bank), "bank_at_max_dd": float(bank_at_max_dd),
        "peak_at_max_dd": float(peak_at_max_dd), "max_dd_pct": float(dd_pct),
        "broke_bank": int(bankrupt_stops) > 0
    }


def _optimizer_simulate_sparse(colors, signal_positions, targets, cfg, gale, mode):
    """
    Simulador EXATO com a mesma regra do _optimizer_simulate_mask,
    mas percorre apenas os índices que realmente têm sinal.
    Isso evita procurar byte a byte nas 30.000 rodadas a cada configuração.
    """
    n = len(colors)
    positions = signal_positions
    m = len(positions)
    stake_base = float(cfg.get("stake", 10.0))
    initial_bank = float(cfg.get("initial_bank", 1000.0))
    bank = initial_bank
    peak = bank
    min_bank = bank
    max_dd = 0.0
    peak_at_max_dd = bank
    bank_at_max_dd = bank
    entries = wins = losses = 0
    gale_wins = [0] * (gale + 1)
    max_gale_reached = 0
    largest_stake = 0.0
    max_cycle_exposure = 0.0
    bankrupt_stops = 0
    recent = []

    def place_bet(idx, target_code, g, exposure):
        nonlocal bank, peak, min_bank, max_dd, peak_at_max_dd, bank_at_max_dd
        nonlocal wins, losses, max_gale_reached, largest_stake, max_cycle_exposure, bankrupt_stops
        stake = stake_base * (2.0 ** g)
        if bank < stake:
            bankrupt_stops += 1
            losses += 1
            return False, True, exposure
        max_gale_reached = max(max_gale_reached, g)
        largest_stake = max(largest_stake, stake)
        exposure += stake
        max_cycle_exposure = max(max_cycle_exposure, exposure)
        bank -= stake
        min_bank = min(min_bank, bank)
        dd = peak - bank
        if dd > max_dd:
            max_dd = dd
            peak_at_max_dd = peak
            bank_at_max_dd = bank
        actual = colors[idx] if 0 <= idx < n else 0
        if actual == target_code:
            bank += stake * 2.0
            wins += 1
            gale_wins[g] += 1
            peak = max(peak, bank)
            return True, False, exposure
        return False, False, exposure

    if m == 0:
        return {
            "entries": 0, "wins": 0, "losses": 0, "gale_wins": gale_wins,
            "profit": 0.0, "rate": 0.0, "bank": bank, "max_dd": 0.0,
            "max_gale_reached": 0, "largest_stake": 0.0,
            "max_cycle_exposure": 0.0, "min_bank": bank,
            "bankrupt_stops": 0, "peak_bank": bank,
            "bank_at_max_dd": bank, "peak_at_max_dd": bank,
            "max_dd_pct": 0.0, "broke_bank": False
        }

    # Tradicional / pular rodadas.
    if mode == 0 or 2 <= mode <= 10:
        skip_rounds = (mode - 1) if mode >= 2 else 0
        p = 0
        while p < m:
            idx = int(positions[p])
            target_code = int(targets[idx])
            if target_code == 0:
                p += 1
                continue
            entries += 1
            exposure = 0.0
            bet_idx = idx
            for g in range(gale + 1):
                won, broke, exposure = place_bet(bet_idx, target_code, g, exposure)
                if broke:
                    recent.append(False)
                    break
                if won:
                    recent.append(True)
                    break
                if g >= gale:
                    losses += 1
                    recent.append(False)
                    break
                bet_idx += (skip_rounds + 1) if mode >= 2 else 1
                if bet_idx >= n:
                    break
            if _optimizer_risk_stop(cfg, bank, peak, recent):
                break
            search_from = max(idx + 1, bet_idx + 1)
            p = bisect.bisect_left(positions, search_from, p + 1)

    # Próximo sinal / pular sinais.
    else:
        skip_signals = 0 if mode == 1 else max(1, mode - 10)
        p = 0
        while p < m:
            entries += 1
            exposure = 0.0
            cur_p = p
            current_idx = int(positions[cur_p])
            for g in range(gale + 1):
                target_code = int(targets[current_idx])
                won, broke, exposure = place_bet(current_idx, target_code, g, exposure)
                if broke:
                    recent.append(False)
                    break
                if won:
                    recent.append(True)
                    break
                if g >= gale:
                    losses += 1
                    recent.append(False)
                    break
                cur_p += skip_signals + 1
                if cur_p >= m:
                    current_idx = n
                    break
                current_idx = int(positions[cur_p])
            if _optimizer_risk_stop(cfg, bank, peak, recent):
                break
            if current_idx >= n:
                p = m
            else:
                p = bisect.bisect_right(positions, current_idx, cur_p)

    rate = 100.0 * wins / entries if entries else 0.0
    dd_pct = 100.0 * max_dd / peak_at_max_dd if peak_at_max_dd > 0 else 0.0
    return {
        "entries": entries, "wins": wins, "losses": losses,
        "gale_wins": gale_wins, "profit": bank - initial_bank,
        "rate": rate, "bank": bank, "max_dd": max_dd,
        "max_gale_reached": max_gale_reached,
        "largest_stake": largest_stake,
        "max_cycle_exposure": max_cycle_exposure,
        "min_bank": min_bank, "bankrupt_stops": bankrupt_stops,
        "peak_bank": peak, "bank_at_max_dd": bank_at_max_dd,
        "peak_at_max_dd": peak_at_max_dd, "max_dd_pct": dd_pct,
        "broke_bank": bankrupt_stops > 0
    }


def _optimizer_constant_targets_np(n, positions, target_code):
    targets = np.zeros(n, dtype=np.uint8)
    if positions:
        targets[np.asarray(positions, dtype=np.int32)] = target_code
    return targets


def _optimizer_single_mask(n, positions, target_code):
    mask = bytearray(n)
    targets = bytearray(n)
    for idx in positions:
        mask[idx] = 1
        targets[idx] = target_code
    return mask, targets


def _optimizer_consensus_target(vr, vb, consensus):
    active = vr + vb
    if active <= 0 or vr == vb:
        return 0
    winner = vr if vr > vb else vb
    required = 50.0001 if consensus <= 50 else float(consensus)
    if 100.0 * winner / active + 0.0001 < required:
        return 0
    return 1 if vr > vb else 2


def _optimizer_strategy_dict(selected, gale, mode, consensus, cfg, contrary):
    comps = []
    for item in selected:
        entry = item["entry"]
        if contrary:
            entry = "B" if entry == "R" else "R"
        comps.append({
            "type": "single",
            "name": "AUTO • %s → %s" % (",".join(item["pattern"]), entry),
            "pattern": list(item["pattern"]), "entry": entry,
            "gale": gale, "gale_mode": mode,
            "stake": float(cfg.get("stake", 10.0)),
            "initial_bank": float(cfg.get("initial_bank", 1000.0))
        })
    if len(comps) == 1:
        st = dict(comps[0])
        st["name"] = "CONTRÁRIA • AUTO • 1 ESTRATÉGIA" if contrary else "AUTO • 1 ESTRATÉGIA"
        st["contrarian"] = bool(contrary)
    else:
        st = {
            "type": "ensemble",
            "name": ("CONTRÁRIA • " if contrary else "") + "AUTO • %d ESTRATÉGIAS" % len(comps),
            "components": comps, "gale": gale, "gale_mode": mode,
            "consensus_pct": consensus,
            "stake": float(cfg.get("stake", 10.0)),
            "initial_bank": float(cfg.get("initial_bank", 1000.0)),
            "contrarian": bool(contrary)
        }
    for key in (
        "risk_trailing_enabled", "risk_trailing_pct", "risk_capital_enabled",
        "risk_capital_floor", "risk_performance_enabled", "risk_performance_window",
        "risk_performance_min_rate"
    ):
        if key in cfg:
            st[key] = cfg[key]
    return st


def _optimizer_job_update(job_id, **changes):
    with OPTIMIZER_JOBS_LOCK:
        job = OPTIMIZER_JOBS.get(job_id)
        if isinstance(job, dict):
            job.update(changes)
            job["updated_at"] = agora_brasilia()



# ================================================================
# V56.4 - CACHE PERSISTENTE DE CONFIGURAÇÕES EXATAS NO POSTGRES
#
# O cache guarda o resultado bruto de CADA configuração já calculada.
# Filtros como acerto mínimo e drawdown NÃO entram na chave da simulação,
# então podem mudar sem refazer o backtest.
# ================================================================
OPTIMIZER_PG_CACHE_READY = False
OPTIMIZER_PG_CACHE_LOCK = threading.Lock()


def _optimizer_pg_ensure():
    global OPTIMIZER_PG_CACHE_READY
    if OPTIMIZER_PG_CACHE_READY or not postgres_configurado() or not postgres_driver_ok():
        return OPTIMIZER_PG_CACHE_READY
    with OPTIMIZER_PG_CACHE_LOCK:
        if OPTIMIZER_PG_CACHE_READY:
            return True
        try:
            with conectar_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS optimizer_exact_cache (
                            space_key TEXT NOT NULL,
                            gale INTEGER NOT NULL,
                            gale_mode INTEGER NOT NULL,
                            strategy_count INTEGER NOT NULL,
                            consensus INTEGER NOT NULL,
                            profit DOUBLE PRECISION NOT NULL,
                            rate DOUBLE PRECISION NOT NULL,
                            max_dd DOUBLE PRECISION NOT NULL,
                            entries INTEGER NOT NULL,
                            wins INTEGER NOT NULL,
                            losses INTEGER NOT NULL,
                            result_json JSONB,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (
                                space_key, gale, gale_mode,
                                strategy_count, consensus
                            )
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_optimizer_exact_scope
                        ON optimizer_exact_cache
                        (space_key, gale, gale_mode, strategy_count, consensus)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_optimizer_exact_best
                        ON optimizer_exact_cache
                        (space_key, profit DESC, max_dd ASC)
                    """)
                conn.commit()
            OPTIMIZER_PG_CACHE_READY = True
        except Exception as exc:
            print("Optimizer PG cache indisponível:", repr(exc), flush=True)
            OPTIMIZER_PG_CACHE_READY = False
    return OPTIMIZER_PG_CACHE_READY


def _optimizer_space_key(history_hash, cfg, contrary):
    # min_rate e limite de DD são filtros posteriores, portanto ficam FORA.
    raw = {
        "engine": "v56.4-exact-config-cache",
        "history": history_hash,
        "cfg": cfg,
        "contrarian": bool(contrary)
    }
    return hashlib.sha256(
        json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _optimizer_pg_cached_pairs(space_key, gale, mode, max_count, consensuses):
    if not _optimizer_pg_ensure():
        return {}
    try:
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_count, consensus,
                           profit, rate, max_dd,
                           entries, wins, losses, result_json
                    FROM optimizer_exact_cache
                    WHERE space_key = %s
                      AND gale = %s
                      AND gale_mode = %s
                      AND strategy_count BETWEEN 1 AND %s
                      AND consensus = ANY(%s)
                """, (
                    space_key, int(gale), int(mode),
                    int(max_count), [int(x) for x in consensuses]
                ))
                rows = cur.fetchall()
        result = {}
        for row in rows:
            key = (int(row[0]), int(row[1]))
            rj = row[8]
            if isinstance(rj, str):
                try:
                    rj = json.loads(rj)
                except Exception:
                    rj = {}
            result[key] = {
                "profit": float(row[2]),
                "rate": float(row[3]),
                "max_dd": float(row[4]),
                "entries": int(row[5]),
                "wins": int(row[6]),
                "losses": int(row[7]),
                "result": dict(rj or {})
            }
        return result
    except Exception as exc:
        print("Optimizer PG leitura falhou:", repr(exc), flush=True)
        return {}


def _optimizer_pg_insert_batch(space_key, gale, mode, batch):
    if not batch or not _optimizer_pg_ensure():
        return
    try:
        rows = []
        for item in batch:
            rows.append((
                space_key, int(gale), int(mode),
                int(item["count"]), int(item["consensus"]),
                float(item["profit"]), float(item["rate"]),
                float(item["max_dd"]), int(item["entries"]),
                int(item["wins"]), int(item["losses"]),
                json.dumps(item["result"], ensure_ascii=False, separators=(",", ":"))
            ))
        with conectar_postgres() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO optimizer_exact_cache (
                        space_key, gale, gale_mode, strategy_count, consensus,
                        profit, rate, max_dd, entries, wins, losses, result_json
                    )
                    VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
                    )
                    ON CONFLICT (
                        space_key, gale, gale_mode, strategy_count, consensus
                    ) DO NOTHING
                """, rows)
            conn.commit()
    except Exception as exc:
        print("Optimizer PG gravação falhou:", repr(exc), flush=True)


def _optimizer_cache_accepts(item, min_rate_enabled, min_rate, rate_mode, dd_enabled, dd_limit):
    if min_rate_enabled:
        rate = float(item.get("rate", 0.0))
        limite = float(min_rate)
        if str(rate_mode).lower() == "max":
            if rate - 0.0001 > limite:
                return False
        else:
            if rate + 0.0001 < limite:
                return False
    if dd_enabled and float(item.get("max_dd", 0.0)) > float(dd_limit):
        return False
    return True


def _optimizer_run_job(job_id, payload):
    started = time.time()
    try:
        colors_raw, history_ids = _optimizer_colors()
        n = len(colors_raw)
        if n < 20:
            raise RuntimeError("Histórico insuficiente para otimização")

        colors = np.fromiter(
            (1 if c == "R" else 2 if c == "B" else 0 for c in colors_raw),
            dtype=np.uint8, count=n
        )

        gale_req = int(payload.get("gale", -1))
        mode_req = int(payload.get("gale_mode", -1))
        max_count_req = max(1, min(3000, int(payload.get("max_count", 3000))))
        consensus_req = int(payload.get("consensus", -1))
        contrary = bool(payload.get("contrarian", False))
        cfg = dict(payload.get("config", {}) or {})
        min_rate_enabled = bool(payload.get("min_rate_enabled", False))
        min_rate = float(payload.get("min_rate", 0.0))
        rate_mode = str(payload.get("rate_mode", "min")).strip().lower()
        if rate_mode not in ("min", "max"):
            rate_mode = "min"
        rate_mode_code = 1 if rate_mode == "max" else 0

        objective = str(payload.get("objective", "profit")).strip().lower()
        if objective not in ("profit", "loss"):
            objective = "profit"
        objective_mode_code = 1 if objective == "loss" else 0

        dd_enabled = bool(payload.get("dd_enabled", False))
        dd_limit = float(payload.get("dd_limit", 0.0))

        gales = [gale_req] if gale_req >= 0 else list(range(12))
        modes = [mode_req] if mode_req >= 0 else list(range(20))
        consensuses = [consensus_req] if consensus_req >= 0 else [50,60,70,80,90,100]
        consensuses_np = np.asarray(consensuses, dtype=np.int16)

        hist_hash = hashlib.sha256("|".join(history_ids).encode("utf-8")).hexdigest()[:20]
        cache_payload = {
            "engine": "v58.9-profit-or-loss-objective-exact",
            "history": hist_hash,
            "gales": gales, "modes": modes,
            "max_count": max_count_req,
            "consensus": consensuses,
            "contrarian": contrary,
            "cfg": cfg,
            "min_rate_enabled": min_rate_enabled,
            "min_rate": min_rate,
            "rate_mode": rate_mode,
            "objective": objective,
            "dd_enabled": dd_enabled,
            "dd_limit": dd_limit
        }
        cache_key = hashlib.sha256(
            json.dumps(cache_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        # Resultado completo repetido = retorno imediato.
        with OPTIMIZER_CACHE_LOCK:
            cached = OPTIMIZER_RESULT_CACHE.get(cache_key)
        if isinstance(cached, dict):
            cached = dict(cached)
            cached["cached"] = True
            _optimizer_job_update(
                job_id, status="done", progress=1.0,
                phase="CACHE COMPLETO V56.5", result=cached
            )
            return

        total_configs = len(gales) * len(modes) * max_count_req * len(consensuses)
        _optimizer_job_update(
            job_id, status="running",
            phase="PREPARANDO MOTOR JIT V58.3",
            tested=0, total=total_configs, progress=0.0
        )

        # Warm-up pequeno para compilar o kernel antes da busca real.
        _wc = np.asarray([1,2,1,2,1,2], dtype=np.uint8)
        _sm = np.zeros((1, 6), dtype=np.uint8)
        _sm[0, 1] = 1
        _ec = np.asarray([1], dtype=np.uint8)
        _vr = np.zeros(6, dtype=np.uint16)
        _vb = np.zeros(6, dtype=np.uint16)
        _optimizer_sweep_chunk_numba(
            _wc, _sm, _ec, _vr, _vb,
            0, 1, np.asarray([50], dtype=np.int16),
            0, 0, 10.0, 1000.0, False,
            False, 0.0, 0, 0, False, 0.0
        )

        _optimizer_job_update(
            job_id, status="running",
            phase="MOTOR JIT V58.3 PRONTO",
            tested=0, total=total_configs, progress=0.0
        )

        # Ocorrências dos 1022 padrões calculadas uma única vez.
        occ = _optimizer_occurrences(colors_raw)
        # V57.8: universo solicitado = 3000 estratégias individuais.
        # São 1500 padrões canônicos x 2 entradas (R/B).
        # Preserva todos os padrões mais curtos primeiro e não usa sorteio.
        base = []
        for plen in range(1, 13):
            for mask in range(1 << plen):
                if len(base) >= 1500:
                    break
                base.append({
                    "plen": plen,
                    "mask": mask,
                    "pattern": _optimizer_pattern_from_mask(plen, mask),
                    "positions": occ[(plen, mask)]
                })
            if len(base) >= 1500:
                break

        best_profit = float("inf") if objective == "loss" else float("-inf")
        best_dd = float("inf")
        best_strategy = None
        best_result = None
        tested = 0

        for gale in gales:
            for mode in modes:
                _optimizer_job_update(
                    job_id,
                    phase="RANKING EXATO 3000 • G%d • M%d" % (gale, mode),
                    tested=tested, total=total_configs,
                    progress=min(0.999, tested / float(max(1, total_configs)))
                )

                # Ranking exato das 3000 estratégias.
                # Cacheado por histórico + gale + modo + configuração.
                ranking_key_raw = {
                    "history": hist_hash,
                    "gale": int(gale),
                    "mode": int(mode),
                    "stake": float(cfg.get("stake", 10.0)),
                    "initial_bank": float(cfg.get("initial_bank", 1000.0)),
                    "rate_mode": str(rate_mode),
                    "objective": str(objective)
                }
                ranking_key = hashlib.sha256(
                    json.dumps(
                        ranking_key_raw,
                        sort_keys=True,
                        separators=(",", ":")
                    ).encode()
                ).hexdigest()

                ranked = None
                with OPTIMIZER_RANKING_CACHE_LOCK:
                    cached_rank = OPTIMIZER_RANKING_CACHE.get(ranking_key)
                    if isinstance(cached_rank, list):
                        ranked = cached_rank

                if ranked is None:
                    ranked = []
                    for idx, cand0 in enumerate(base):
                        positions = np.asarray(cand0["positions"], dtype=np.int32)
                        targets = np.zeros(n, dtype=np.uint8)

                        for entry, target_code in (("R", 1), ("B", 2)):
                            targets.fill(0)
                            targets[positions] = target_code
                            r = _optimizer_simulate_numba(
                                colors, positions, targets, cfg, gale, mode
                            )
                            # V57.8: nenhuma das 3000 estratégias é removida
                            # por baixa quantidade de entradas.
                            ranked.append({
                                "plen": cand0["plen"],
                                "mask": cand0["mask"],
                                "pattern": cand0["pattern"],
                                "entry": entry,
                                "positions": cand0["positions"],
                                "profit": float(r.get("profit", float("-inf"))),
                                "rate": float(r.get("rate", 0.0))
                            })

                        if idx % 128 == 0:
                            _optimizer_job_update(
                                job_id,
                                phase="RANKING EXATO 3000 • %d/3000" % min(3000, idx * 2),
                                tested=tested, total=total_configs
                            )

                    # V58.3:
                    # O sweep testa prefixos do ranking. No modo tradicional
                    # (ACERTO MÍNIMO) faz sentido começar pelas mais lucrativas.
                    #
                    # No modo ACERTO MÁXIMO, porém, ordenar por lucro fazia os
                    # prefixos começarem justamente pelas estratégias de maior
                    # acerto/lucro. Resultado: ao pedir "até 20%", muitas vezes
                    # nenhuma configuração chegava a ser elegível.
                    #
                    # Para ACERTO MÁXIMO, começamos pelas estratégias de MENOR
                    # taxa de acerto. Depois o sweep ainda escolhe, entre todas
                    # as configurações que respeitam taxa <= limite, a de MAIOR
                    # resultado financeiro — mesmo que esse resultado seja
                    # negativo.
                    if objective == "loss":
                        # Gerador de prejuízo: começa pelas estratégias individuais
                        # de pior resultado financeiro, sem apagar os filtros de taxa.
                        ranked.sort(
                            key=lambda x: (
                                float(x.get("profit", 0.0)),
                                float(x.get("rate", 0.0))
                            )
                        )
                    elif rate_mode == "max":
                        ranked.sort(
                            key=lambda x: (
                                float(x.get("rate", 0.0)),
                                -float(x.get("profit", 0.0))
                            )
                        )
                    else:
                        ranked.sort(
                            key=lambda x: (x["profit"], x["rate"]),
                            reverse=True
                        )
                    # Guarda o ranking completo das 3000 desta base.
                    with OPTIMIZER_RANKING_CACHE_LOCK:
                        OPTIMIZER_RANKING_CACHE[ranking_key] = ranked
                        # Mantém somente os rankings mais recentes.
                        if len(OPTIMIZER_RANKING_CACHE) > 32:
                            oldest_key = next(iter(OPTIMIZER_RANKING_CACHE))
                            if oldest_key != ranking_key:
                                OPTIMIZER_RANKING_CACHE.pop(oldest_key, None)
                else:
                    _optimizer_job_update(
                        job_id,
                        phase="RANKING 3000 • CACHE REUTILIZADO",
                        tested=tested,
                        total=total_configs
                    )

                ranked = ranked[:max_count_req]
                max_count = len(ranked)
                if max_count <= 0:
                    continue

                # V57.8: com universo de 3000, o sweep deve percorrer todas
                # as quantidades 1..max_count. Não encerra antes por filtro interno.

                # Matriz compacta: 1 byte por estratégia x rodada.
                # 10000 x 30000 = ~300 MB e evita recriar sinais milhões de vezes.
                signal_matrix = np.zeros((max_count, n), dtype=np.uint8)
                entry_codes = np.zeros(max_count, dtype=np.uint8)
                for ri, cand in enumerate(ranked):
                    pos = np.asarray(cand["positions"], dtype=np.int32)
                    signal_matrix[ri, pos] = 1
                    entry_codes[ri] = 1 if cand["entry"] == "R" else 2

                votes_r = np.zeros(n, dtype=np.uint16)
                votes_b = np.zeros(n, dtype=np.uint16)

                # Blocos mantêm a UI atualizando, mas o trabalho interno fica 100% JIT.
                # V57.8: bloco maior apenas para reduzir overhead.
                # Busca continua EXATA: nenhuma configuração é pulada/amostrada.
                chunk = 30000
                for c0 in range(0, max_count, chunk):
                    c1 = min(max_count, c0 + chunk)
                    vals = _optimizer_sweep_chunk_numba(
                        colors,
                        signal_matrix,
                        entry_codes,
                        votes_r,
                        votes_b,
                        c0,
                        c1,
                        consensuses_np,
                        int(gale),
                        int(mode),
                        float(cfg.get("stake", 10.0)),
                        float(cfg.get("initial_bank", 1000.0)),
                        bool(contrary),
                        bool(min_rate_enabled),
                        float(min_rate),
                        int(rate_mode_code),
                        int(objective_mode_code),
                        bool(dd_enabled),
                        float(dd_limit)
                    )
                    (
                        tested_chunk,
                        chunk_profit, chunk_dd, chunk_count, chunk_con, chunk_rate,
                        chunk_entries, chunk_wins, chunk_losses, chunk_bank,
                        chunk_max_gale, chunk_largest_stake, chunk_exposure,
                        chunk_min_bank, chunk_bankrupt, chunk_peak,
                        chunk_bank_at_dd, chunk_peak_at_dd, chunk_gale_wins
                    ) = vals

                    tested += int(tested_chunk)

                    if int(chunk_count) > 0 and (
                        float(chunk_profit) > best_profit or
                        (
                            abs(float(chunk_profit) - best_profit) < 1e-9
                            and float(chunk_dd) < best_dd
                        )
                    ):
                        best_profit = float(chunk_profit)
                        best_dd = float(chunk_dd)
                        bc = int(chunk_count)
                        bcon = int(chunk_con)
                        selected = ranked[:bc]
                        best_strategy = _optimizer_strategy_dict(
                            selected, gale, mode, bcon, cfg, contrary
                        )
                        dd_pct = (
                            100.0 * float(chunk_dd) / float(chunk_peak_at_dd)
                            if float(chunk_peak_at_dd) > 0 else 0.0
                        )
                        best_result = {
                            "entries": int(chunk_entries),
                            "wins": int(chunk_wins),
                            "losses": int(chunk_losses),
                            "gale_wins": [int(x) for x in chunk_gale_wins.tolist()],
                            "profit": float(chunk_profit),
                            "rate": float(chunk_rate),
                            "bank": float(chunk_bank),
                            "max_dd": float(chunk_dd),
                            "max_gale_reached": int(chunk_max_gale),
                            "largest_stake": float(chunk_largest_stake),
                            "max_cycle_exposure": float(chunk_exposure),
                            "min_bank": float(chunk_min_bank),
                            "bankrupt_stops": int(chunk_bankrupt),
                            "peak_bank": float(chunk_peak),
                            "bank_at_max_dd": float(chunk_bank_at_dd),
                            "peak_at_max_dd": float(chunk_peak_at_dd),
                            "max_dd_pct": float(dd_pct),
                            "broke_bank": int(chunk_bankrupt) > 0
                        }

                    elapsed_now = max(0.000001, time.time() - started)
                    configs_per_second = float(tested) / elapsed_now
                    _optimizer_job_update(
                        job_id,
                        phase="MOTOR DENSE JIT V58.2",
                        tested=tested,
                        total=total_configs,
                        progress=min(0.999, tested / float(max(1, total_configs))),
                        configs_per_second=configs_per_second,
                        best_profit=(best_profit if best_profit != float("-inf") else 0.0),
                        count=c1,
                        gale=gale,
                        gale_mode=mode
                    )

        if best_strategy is None:
            result_payload = {
                "ok": False,
                "message": "Nenhuma configuração passou pelos filtros.",
                "tested": tested,
                "total": total_configs,
                "elapsed": time.time() - started,
                "configs_per_second": float(tested) / max(0.000001, time.time() - started),
                "history_count": n,
                "engine": "V57.8 DENSE JIT 10K EXACT FASTMATH"
            }
        else:
            result_payload = {
                "ok": True,
                "strategy": best_strategy,
                "result": best_result,
                "tested": tested,
                "total": total_configs,
                "elapsed": time.time() - started,
                "history_count": n,
                "history_hash": hist_hash,
                "rate_mode": rate_mode,
                "objective": objective,
                "rate_limit": float(min_rate) if min_rate_enabled else None,
                "cached": False,
                "engine": "V57.8 DENSE JIT 10K EXACT FASTMATH"
            }

            # Um único cache final, sem milhares de INSERTs no meio da busca.
            with OPTIMIZER_CACHE_LOCK:
                OPTIMIZER_RESULT_CACHE[cache_key] = result_payload
            _optimizer_save_cache()

        _optimizer_job_update(
            job_id,
            status="done",
            phase="CONCLUÍDO V58.3",
            progress=1.0,
            tested=tested,
            total=total_configs,
            result=result_payload
        )

    except Exception as exc:
        _optimizer_job_update(
            job_id, status="error",
            phase="ERRO V57.1",
            error=str(exc), progress=1.0
        )
        print("Optimizer remoto V57.1 erro:", repr(exc), flush=True)

def optimizer_start(payload):
    job_id = hashlib.sha1((str(time.time_ns()) + repr(payload)).encode()).hexdigest()[:16]
    with OPTIMIZER_JOBS_LOCK:
        OPTIMIZER_JOBS[job_id] = {
            "job_id": job_id, "status": "queued", "phase": "NA FILA", "progress": 0.0,
            "tested": 0, "total": 0, "created_at": agora_brasilia(), "updated_at": agora_brasilia()
        }
        # Limpa jobs antigos.
        if len(OPTIMIZER_JOBS) > 20:
            old = list(OPTIMIZER_JOBS.keys())[:-20]
            for k in old:
                OPTIMIZER_JOBS.pop(k, None)
    threading.Thread(target=_optimizer_run_job, args=(job_id, dict(payload or {})), daemon=True, name="optimizer-" + job_id).start()
    return {"ok": True, "job_id": job_id}


def optimizer_status(job_id):
    with OPTIMIZER_JOBS_LOCK:
        job = OPTIMIZER_JOBS.get(str(job_id))
        return dict(job) if isinstance(job, dict) else {"ok": False, "erro": "job não encontrado"}

_optimizer_load_cache()


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

    def enviar_html(self, codigo, html):
        corpo = html.encode("utf-8")
        self.send_response(codigo)
        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8"
        )
        self.send_header("Content-Length", str(len(corpo)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(corpo)

    def ler_json(self):
        tamanho = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(tamanho) if tamanho else b"{}"
        return json.loads(raw.decode("utf-8"))

    def do_GET(self):
        # Heartbeat publico e leve: usado apenas como health/keepalive.
        if self.path.split("?", 1)[0] == "/heartbeat":
            self.enviar_json(200, {
                "ok": True,
                "servico": "double-blaze-ia",
                "agora_brasilia": agora_brasilia(),
                "uptime_segundos": round(time.time() - INICIO_SERVIDOR_EPOCH, 1),
                "heartbeat": dict(ULTIMO_HEARTBEAT)
            })
            return

        if not self.exigir_autorizacao():
            return

        if self.path.startswith("/optimizer/status"):
            try:
                parsed = urlparse(self.path)
                qs = {}
                for pair in parsed.query.split("&"):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        qs[k] = v
                self.enviar_json(200, optimizer_status(qs.get("job_id", "")))
            except Exception as exc:
                self.enviar_json(500, {"ok": False, "erro": str(exc)})
            return

        if self.path.startswith("/demo/status"):
            demo_id = "principal"
            if "?" in self.path:
                for parte in self.path.split("?", 1)[1].split("&"):
                    k, sep, v = parte.partition("=")
                    if sep and k == "demo_id":
                        demo_id = v or "principal"
            self.enviar_json(200, demo_status(demo_id))
            return

        if self.path.startswith("/demo/historico"):
            demo_id = "principal"
            limite = 200
            if "?" in self.path:
                for parte in self.path.split("?", 1)[1].split("&"):
                    k, sep, v = parte.partition("=")
                    if not sep:
                        continue
                    if k == "demo_id":
                        demo_id = v or "principal"
                    elif k == "limite":
                        try:
                            limite = max(1, min(500, int(v)))
                        except Exception:
                            limite = 200
            status = demo_status(demo_id)
            if status.get("existe"):
                ops = list(status.get("operations", []) or [])[:limite]
                self.enviar_json(200, {
                    "ok": True,
                    "demo_id": demo_id,
                    "operations": ops,
                    "quantidade": len(ops)
                })
            else:
                self.enviar_json(200, {
                    "ok": True,
                    "demo_id": demo_id,
                    "operations": [],
                    "quantidade": 0
                })
            return

        # V32: permite acionar a importacao historica diretamente pelo navegador.
        # Mantem a rota POST original e nao altera o coletor ao vivo.
        if self.path == "/reparar-historico":
            try:
                self.enviar_json(200, reparar_historico_publico_atual())
            except Exception as exc:
                self.enviar_json(500, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/bootstrap-1500":
            try:
                self.enviar_json(200, bootstrap_1500_mais_recentes(1500, 3))
            except Exception as exc:
                self.enviar_json(500, {"ok": False, "erro": str(exc)})
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

        if self.path.startswith("/importar-1000"):
            meta = 1000
            max_dias = 7

            caminho, _, query = self.path.partition("?")

            for parte in query.split("&"):
                chave, separador, valor = parte.partition("=")

                if not separador:
                    continue

                if chave == "meta":
                    try:
                        meta = int(valor)
                    except (TypeError, ValueError):
                        meta = 1000

                elif chave == "max_dias":
                    try:
                        max_dias = int(valor)
                    except (TypeError, ValueError):
                        max_dias = 7

            try:
                resultado = importar_1000_bestblaze(
                    meta=meta,
                    max_dias=max_dias
                )
                resultado["meta_solicitada_na_url"] = meta
                resultado["max_dias_solicitado_na_url"] = max_dias
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

        if self.path == "/saude-v36":
            cfg = carregar_config()
            with LOCK:
                banco = list(ESTADO.get("rodadas", []))
                fonte_online = bool(ESTADO.get("fonte_online", False))
                ultima_atualizacao = str(
                    ESTADO.get("ultima_atualizacao", "")
                )

            self.enviar_json(200, {
                "ok": True,
                "versao": "V53",
                "fonte_online": fonte_online,
                "rodadas": len(banco),
                "vermelhos": sum(
                    1 for x in banco
                    if isinstance(x, dict) and x.get("cor") == "R"
                ),
                "pretos": sum(
                    1 for x in banco
                    if isinstance(x, dict) and x.get("cor") == "B"
                ),
                "brancos": sum(
                    1 for x in banco
                    if isinstance(x, dict) and x.get("cor") == "W"
                ),
                "ultima_atualizacao": ultima_atualizacao,
                "modo_fonte": str(cfg.get("modo_fonte", "json"))
            })
            return

        if self.path == "/status-1000":
            with LOCK:
                banco = list(ESTADO.get("rodadas", []))

            total = len(banco)

            self.enviar_json(200, {
                "ok": True,
                "meta": 1000,
                "total_banco": total,
                "faltam_para_1000": max(0, 1000 - total),
                "meta_atingida": total >= 1000,
                "vermelhos": sum(
                    1
                    for item in banco
                    if isinstance(item, dict)
                    and item.get("cor") == "R"
                ),
                "pretos": sum(
                    1
                    for item in banco
                    if isinstance(item, dict)
                    and item.get("cor") == "B"
                ),
                "brancos": sum(
                    1
                    for item in banco
                    if isinstance(item, dict)
                    and item.get("cor") == "W"
                )
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
                resultado = diagnosticar_sessao_periodo_bestblaze()
                self.enviar_json(200, resultado)
            except HTTPError as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": "HTTP %s" % getattr(exc, "code", "?"),
                    "detalhe": str(exc)
                })
            except Exception as exc:
                self.enviar_json(500, {
                    "ok": False,
                    "erro": str(exc)
                })
            return

        if self.path == "/entradas":
            self.enviar_json(200, status_entradas())
            return

        if self.path == "/continuidade":
            self.enviar_json(200, verificar_continuidade_captura())
            return

        if self.path == "/avaliador-estrategias":
            self.enviar_json(
                200,
                status_avaliador_shadow()
            )
            return

        if self.path == "/estrategias":
            self.enviar_json(
                200,
                {
                    "ok": True,
                    "estrategias": catalogo_estrategias()
                }
            )
            return

        if self.path == "/motor-estrategias":
            self.enviar_json(
                200,
                motor_estrategias()
            )
            return

        if self.path.startswith("/backtest-estrategia"):
            estrategia_id = ""
            limite = 1000

            if "?" in self.path:
                query = self.path.split("?", 1)[1]

                for parte in query.split("&"):
                    chave, sep, valor = parte.partition("=")

                    if not sep:
                        continue

                    if chave == "id":
                        estrategia_id = valor
                    elif chave == "limite":
                        try:
                            limite = int(valor)
                        except Exception:
                            limite = 1000

            ids_validos = {
                x["id"]
                for x in catalogo_estrategias()
            }

            if estrategia_id not in ids_validos:
                self.enviar_json(
                    400,
                    {
                        "ok": False,
                        "erro": "estrategia inválida",
                        "ids_validos": sorted(ids_validos)
                    }
                )
                return

            self.enviar_json(
                200,
                backtest_estrategia(
                    estrategia_id,
                    limite
                )
            )
            return

        if self.path.startswith("/laboratorio-estrategias"):
            limite = 1000

            if "?" in self.path:
                query = self.path.split("?", 1)[1]

                for parte in query.split("&"):
                    if parte.startswith("limite="):
                        try:
                            limite = int(
                                parte.split("=", 1)[1]
                            )
                        except Exception:
                            limite = 1000

            self.enviar_json(
                200,
                laboratorio_estrategias(limite)
            )
            return

        if self.path == "/tempo-real":
            self.enviar_html(
                200,
                painel_tempo_real_html()
            )
            return

        if self.path == "/tempo-real-json":
            self.enviar_json(
                200,
                status_tempo_real()
            )
            return

        if self.path == "/diagnostico-websocket":
            self.enviar_json(
                200,
                diagnostico_websocket()
            )
            return

        if self.path == "/diagnostico-coletor":
            self.enviar_json(
                200,
                diagnostico_coletor()
            )
            return

        if self.path == "/coletar-agora":
            try:
                resultado = buscar_feed_fallback_bestblaze()
                self.enviar_json(200, resultado)
            except Exception as exc:
                self.enviar_json(
                    500,
                    {
                        "ok": False,
                        "erro": str(exc)
                    }
                )
            return

        if self.path == "/status-postgres":
            self.enviar_json(
                200,
                postgres_status()
            )
            return

        if self.path == "/limitar-postgres-5000":
            resultado = postgres_limitar_historico(
                LIMITE_HISTORICO
            )
            self.enviar_json(
                200 if resultado.get("ok") else 500,
                resultado
            )
            return

        if self.path == "/sincronizar-postgres":
            resultado = sincronizar_memoria_postgres()
            self.enviar_json(
                200 if resultado.get("ok") else 500,
                resultado
            )
            return

        if self.path.startswith("/painel-analise"):
            limite = 1000
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    if parte.startswith("limite="):
                        try: limite = int(parte.split("=",1)[1])
                        except Exception: pass
            self.enviar_json(200, painel_analise(limite))
            return

        if self.path == "/analise-janelas":
            self.enviar_json(200, analise_janelas())
            return

        if self.path.startswith("/analise-transicoes"):
            limite = 1000
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    if parte.startswith("limite="):
                        try: limite = int(parte.split("=",1)[1])
                        except Exception: pass
            self.enviar_json(200, analise_transicoes(limite))
            return

        if self.path.startswith("/analise-brancos"):
            limite = 1000
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    if parte.startswith("limite="):
                        try: limite = int(parte.split("=",1)[1])
                        except Exception: pass
            self.enviar_json(200, analise_brancos(limite))
            return

        if self.path.startswith("/analise-padroes"):
            limite, tamanho, top = 1000, 3, 20
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    chave, sep, valor = parte.partition("=")
                    if not sep: continue
                    try:
                        if chave == "limite": limite = int(valor)
                        elif chave == "tamanho": tamanho = int(valor)
                        elif chave == "top": top = int(valor)
                    except Exception: pass
            self.enviar_json(200, analise_padroes(limite,tamanho,top))
            return

        if self.path.startswith("/backtest-transicao"):
            limite, origem, apostar = 1000, "R", "B"
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    chave, sep, valor = parte.partition("=")
                    if not sep: continue
                    if chave == "origem": origem = valor.upper()
                    elif chave == "apostar": apostar = valor.upper()
                    elif chave == "limite":
                        try: limite = int(valor)
                        except Exception: pass
            try:
                self.enviar_json(200, backtest_regra_transicao(limite,origem,apostar))
            except Exception as exc:
                self.enviar_json(400, {"erro":str(exc)})
            return

        if self.path.startswith("/backtest-padrao"):
            limite, padrao, apostar = 1000, "RB", "R"
            if "?" in self.path:
                for parte in self.path.split("?",1)[1].split("&"):
                    chave, sep, valor = parte.partition("=")
                    if not sep: continue
                    if chave == "padrao": padrao = valor.upper()
                    elif chave == "apostar": apostar = valor.upper()
                    elif chave == "limite":
                        try: limite = int(valor)
                        except Exception: pass
            try:
                self.enviar_json(200, backtest_padroes(limite,padrao,apostar))
            except Exception as exc:
                self.enviar_json(400, {"erro":str(exc)})
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

        if self.path == "/bootstrap-1500-status":
            with LOCK:
                boot = dict(ESTADO.get("bootstrap_1500", {}) or {})
            self.enviar_json(200, boot)
            return

        if self.path.startswith("/historico-validado"):
            limite = 30000
            try:
                if "?" in self.path:
                    query = self.path.split("?", 1)[1]
                    for parte in query.split("&"):
                        if parte.startswith("limite="):
                            limite = int(parte.split("=", 1)[1])
            except Exception:
                limite = 30000

            self.enviar_json(200, historico_validado(limite))
            return

        if self.path == "/continuidade-historico":
            diag = diagnosticar_continuidade_historico()
            diag.pop("segmento_atual", None)
            self.enviar_json(200, diag)
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

            limite = max(1, min(limite, 30000))

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

        if self.path == "/optimizer/start":
            try:
                payload = self.ler_json()
                self.enviar_json(202, optimizer_start(payload))
            except Exception as exc:
                self.enviar_json(500, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/iniciar":
            try:
                obj = self.ler_json()
                sessao = demo_iniciar(obj)
                self.enviar_json(200, {
                    "ok": True,
                    "sessao": demo_status(sessao.get("demo_id", "principal"))
                })
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/alterar-estrategia":
            try:
                obj = self.ler_json()
                self.enviar_json(200, demo_alterar_estrategia(obj))
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/branco-ativo":
            try:
                obj = self.ler_json()
                demo_id = str(obj.get("demo_id", "principal")) if isinstance(obj, dict) else "principal"
                enabled = bool(obj.get("enabled", True)) if isinstance(obj, dict) else True
                self.enviar_json(200, demo_branco_ativo(demo_id, enabled))
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/sinal-contrario":
            try:
                obj = self.ler_json()
                demo_id = str(obj.get("demo_id", "principal")) if isinstance(obj, dict) else "principal"
                enabled = bool(obj.get("enabled", False)) if isinstance(obj, dict) else False
                self.enviar_json(200, demo_sinal_contrario(demo_id, enabled))
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/parar":
            try:
                obj = self.ler_json()
                demo_id = str(obj.get("demo_id", "principal")) if isinstance(obj, dict) else "principal"
                self.enviar_json(200, demo_parar(demo_id))
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
            return

        if self.path == "/demo/resetar":
            try:
                obj = self.ler_json()
                demo_id = str(obj.get("demo_id", "principal")) if isinstance(obj, dict) else "principal"
                self.enviar_json(200, demo_resetar(demo_id))
            except Exception as exc:
                self.enviar_json(400, {"ok": False, "erro": str(exc)})
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



def _optimizer_warmup_background():
    """V57.8: compila os kernels Numba logo após o boot, fora do primeiro clique do usuário."""
    try:
        t0 = time.time()
        _wc = np.asarray([1, 2, 1, 2, 1, 2], dtype=np.uint8)
        _sm = np.zeros((1, 6), dtype=np.uint8)
        _sm[0, 1] = 1
        _ec = np.asarray([1], dtype=np.uint8)
        _vr = np.zeros(6, dtype=np.uint16)
        _vb = np.zeros(6, dtype=np.uint16)
        _optimizer_sweep_chunk_numba(
            _wc, _sm, _ec, _vr, _vb,
            0, 1, np.asarray([50], dtype=np.int16),
            0, 0, 10.0, 1000.0, False,
            False, 0.0, False, 0.0
        )
        print(
            "V58.9 optimizer JIT pre-aquecido em %.3fs" % (time.time() - t0),
            flush=True
        )
    except Exception as exc:
        print("V58.9 warmup optimizer falhou:", repr(exc), flush=True)


def main():
    print("V58.10 HISTORICO PERSISTENTE SEM RESET + OBJETIVO PREJUIZO: iniciando servidor...", flush=True)
    carregar_estado()

    # V58.10: NUNCA executa corte/deleção de histórico durante boot/deploy.
    # O corte V58.0 era uma migração antiga e não pertence ao ciclo normal
    # de inicialização de um servidor que precisa preservar histórico.
    print(
        "V58.10 histórico restaurado sem corte destrutivo:",
        len(ESTADO.get("rodadas", [])),
        flush=True
    )

    print("V54 Demo24h: estado carregado", flush=True)

    try:
        demo_recuperar_pendentes()
        print("V54 Demo24h: demos sincronizadas", flush=True)
    except Exception as exc:
        print("V54 Demo24h: erro ao recuperar demos:", exc, flush=True)

    if not CONFIG.exists():
        salvar_json(CONFIG, CONFIG_PADRAO)

    porta = int(os.getenv("PORT", os.getenv("PORTA", "8787")))

    thread_sinal = threading.Thread(
        target=recalcular_sinal_inicial,
        daemon=True
    )
    thread_sinal.start()

    thread_continuidade = threading.Thread(
        target=worker_continuidade,
        daemon=True
    )
    thread_continuidade.start()

    thread_analise = threading.Thread(
        target=worker_analise_sinal,
        daemon=True
    )
    thread_analise.start()

    thread = threading.Thread(
        target=worker_feed,
        daemon=True
    )
    thread.start()

    thread_ws = threading.Thread(
        target=worker_websocket_double,
        daemon=True
    )
    thread_ws.start()

    # V58.0: segunda camada independente do WebSocket.
    thread_reconciliacao = threading.Thread(
        target=worker_reconciliacao_recente,
        daemon=True
    )
    thread_reconciliacao.start()

    # V58.4: Demo 24h se auto-sincroniza mesmo após deploy/restart.
    thread_demo_sync = threading.Thread(
        target=worker_demo_sincronia_24h,
        daemon=True,
        name="demo-sync-24h"
    )
    thread_demo_sync.start()

    thread_reparo = threading.Thread(
        target=worker_reparo_inicial,
        daemon=True
    )
    thread_reparo.start()

    thread_bootstrap = threading.Thread(
        target=worker_bootstrap_1500,
        daemon=True
    )
    thread_bootstrap.start()

    thread_heartbeat = threading.Thread(
        target=worker_heartbeat,
        daemon=True
    )
    thread_heartbeat.start()

    # V57.8: o custo de compilação JIT acontece em background no boot,
    # não quando o usuário toca em ENCONTRAR A MELHOR.
    thread_optimizer_warmup = threading.Thread(
        target=_optimizer_warmup_background,
        daemon=True
    )
    thread_optimizer_warmup.start()

    servidor = ThreadingHTTPServer(("0.0.0.0", porta), Handler)
    print("Servidor 24h iniciado na porta", porta, flush=True)
    print("Horário de Brasília:", agora_brasilia(), flush=True)
    servidor.serve_forever()


if __name__ == "__main__":
    main()
