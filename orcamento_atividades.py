# ===============================================================
# Importador ATIVIDADES_POR_PONTO_BASE
# - Lê a lista de fontes em BD_Config!A3:A (IDs ou URLs)
# - Copia A:J (linha 2+) da aba ATIVIDADES_POR_PONTO de cada fonte
# - Concatena e cola em ATIVIDADES_POR_PONTO_BASE!A2
# - Converte colunas A e G para número
# - Relatório de linhas por fonte e total colado
# - Distribui o orçamento de cada Unidade (coluna J) para a aba
#   BD_Orçamento da planilha da Unidade + carimbo em Prog_TPM!E1/G1
#   (substitui os Apps Scripts copiarAtividadesParaOrcamentoFiltrado)
# ===============================================================

import os
import sys
import re
import time
import random
import socket
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ===================== CONFIG =====================

SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "credenciais.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Planilha de DESTINO e abas
DEST_SPREADSHEET_ID = "1Ipp454Clq0lKik8G5LjMMmV-8eA0R6if4FGG555K1j8"
DEST_SHEET_NAME      = "ATIVIDADES_POR_PONTO_BASE"
CONFIG_SHEET_NAME    = "BD_Config"              # onde estão as fontes
CONFIG_RANGE         = "A3:A"                   # lista de IDs/URLs das fontes

# Aba de origem (mesmo nome em todas as fontes)
SOURCE_SHEET_NAME    = "ATIVIDADES_POR_PONTO"

START_ROW_DEST       = 2   # começa a colar na linha 2
NUM_COLS             = 10  # A:J
WRITE_CHUNK_ROWS     = 20000

# Status HTTP transientes que valem retry (rate-limit / erros de servidor)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 6
BASE_DELAY = 2.0  # segundos; cresce exponencial

# Distribuição por Unidade: filtra a coluna J da BASE e cola A:J (com
# cabeçalho) em BD_Orçamento!A1 da planilha da Unidade.
UNIDADE_SHEET_NAME = "BD_Orçamento"
PROG_TPM_SHEET     = "Prog_TPM"   # E1 = momento da cópia, G1 = carimbo da BASE (L2)
# Gabarito das Unidades: B = nome, C = ID da planilha, E = valores da
# coluna J aceitos (mais de um separado por vírgula). Linha 3 em diante.
GABARITO_SPREADSHEET_ID = "1kMJedysNlxxPU2PtCwICHlBbZVL4YpvyHSsR7Xl71Ig"
GABARITO_RANGE          = "BD_Planilhas!B3:E"

# ===============================================================


def execute_with_retry(request, what="operação"):
    """Executa request.execute() com backoff exponencial + jitter.
    Retenta em erros HTTP transientes (429/5xx) e falhas de rede.
    Re-lança o erro se esgotar as tentativas ou se for erro não-transiente.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return request.execute()
        except HttpError as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            try:
                status = int(status)
            except (TypeError, ValueError):
                status = None
            if status in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                delay = BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(
                    f"⏳ {what}: HTTP {status}, retry {attempt}/{MAX_RETRIES} "
                    f"em {delay:.1f}s..."
                )
                time.sleep(delay)
                continue
            raise
        except (socket.timeout, ConnectionError, TimeoutError, OSError) as e:
            if attempt < MAX_RETRIES:
                delay = BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(
                    f"⏳ {what}: rede ({type(e).__name__}), retry "
                    f"{attempt}/{MAX_RETRIES} em {delay:.1f}s..."
                )
                time.sleep(delay)
                continue
            raise


def get_service_and_email():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_FILE}")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    svc = build("sheets", "v4", credentials=creds)
    return svc, creds.service_account_email


def get_sheet_props(svc, spreadsheet_id, sheet_name):
    """Propriedades (sheetId, gridProperties) da aba, ou None se não existir.
    Pede só sheets.properties: sem o filtro a API devolve a planilha inteira."""
    meta = execute_with_retry(
        svc.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields="sheets.properties"
        ),
        f"ler metadados de {sheet_name}",
    )
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return props
    return None


def ensure_dest_sheet_exists(svc, spreadsheet_id, sheet_name):
    if get_sheet_props(svc, spreadsheet_id, sheet_name):
        return
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    execute_with_retry(
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body),
        "criar aba de destino",
    )


def ensure_dest_grid_size(svc, spreadsheet_id, sheet_name, min_rows, min_cols):
    """
    Garante que a aba de destino tenha pelo menos min_rows linhas e min_cols colunas.
    Se necessário, atualiza gridProperties.rowCount / columnCount via batchUpdate.
    Retorna o rowCount final da aba (usado para limpar o resíduo).
    """
    target_sheet = get_sheet_props(svc, spreadsheet_id, sheet_name)
    if not target_sheet:
        raise RuntimeError(f"aba '{sheet_name}' não encontrada")

    sheet_id = target_sheet["sheetId"]
    grid = target_sheet.get("gridProperties", {})
    current_rows = grid.get("rowCount", 1000)
    current_cols = grid.get("columnCount", 26)

    new_grid = {}
    fields_list = []

    if current_rows < min_rows:
        new_grid["rowCount"] = min_rows
        fields_list.append("gridProperties.rowCount")

    if current_cols < min_cols:
        new_grid["columnCount"] = min_cols
        fields_list.append("gridProperties.columnCount")

    if not new_grid:
        return current_rows

    fields_str = ",".join(fields_list)
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": new_grid,
                    },
                    "fields": fields_str,
                }
            }
        ]
    }
    execute_with_retry(
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body),
        "ajustar tamanho da grade",
    )
    return max(current_rows, min_rows)


def col_index_to_letter(n):
    """1 -> A, 26 -> Z, 27 -> AA ... (suporta além de 26 colunas)."""
    letters = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters


def pad_row_to_n_cols(row, n):
    if len(row) < n:
        return row + [""] * (n - len(row))
    elif len(row) > n:
        return row[:n]
    return row


def limpar_numero(valor):
    """Converte 'texto numérico' -> float (remove ', R$, espaços, apóstrofo)."""
    if isinstance(valor, (int, float)):
        return valor
    if not isinstance(valor, str):
        return ""
    v = valor.strip().replace("'", "").replace(" ", "")
    v = re.sub(r"(?i)r\$", "", v)  # remove R$ em qualquer caixa
    v = v.replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return ""


def tratar_colunas_numericas(rows):
    """Aplica limpeza nas colunas A (0) e G (6)."""
    for r in rows:
        if len(r) > 0:
            r[0] = limpar_numero(r[0])
        if len(r) > 6:
            r[6] = limpar_numero(r[6])
    return rows


def read_values(svc, spreadsheet_id, rng):
    resp = execute_with_retry(
        svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            majorDimension="ROWS",
        ),
        f"ler {rng}",
    )
    return resp.get("values", [])


def extract_spreadsheet_id(text):
    """Aceita ID puro ou URL; retorna o ID ou None se inválido."""
    if not text:
        return None
    text = text.strip()
    # URL padrão: .../spreadsheets/d/<ID>/...
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", text)
    if m:
        return m.group(1)
    # ID 'cru': letras, números, - e _
    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", text):
        return text
    return None


def get_source_ids_from_config(svc):
    """Lê BD_Config!A3:A e devolve lista de IDs de planilhas válidos (sem vazios)."""
    raw = read_values(svc, DEST_SPREADSHEET_ID, f"{CONFIG_SHEET_NAME}!{CONFIG_RANGE}")
    ids = []
    for row in raw:
        cell = row[0].strip() if row and len(row) > 0 else ""
        if not cell:
            continue
        sid = extract_spreadsheet_id(cell)
        if sid:
            ids.append(sid)

    # remove duplicatas mantendo ordem
    seen = set()
    uniq = []
    for sid in ids:
        if sid not in seen:
            uniq.append(sid)
            seen.add(sid)
    return uniq


def read_source_block(svc, spreadsheet_id, sheet_name):
    """Lê A2:J da origem e aplica tratamento numérico."""
    rng = f"{sheet_name}!A2:J"
    values = read_values(svc, spreadsheet_id, rng)
    rows = [pad_row_to_n_cols(r, NUM_COLS) for r in values]
    return tratar_colunas_numericas(rows)


def mascara_ponto(val_b):
    """Coluna K: 'B-' + coluna B ('B-0' quando o projeto antes do '_' tem 6 dígitos)."""
    if val_b in ("", None):
        return ""
    val_b = str(val_b)
    digits_only = re.sub(r"\D", "", val_b.split("_", 1)[0])
    prefix = "B-0" if len(digits_only) == 6 else "B-"
    return prefix + val_b


_T0 = time.monotonic()


def log_tempo(etapa):
    """Marca no log o tempo decorrido desde o início do script."""
    print(f"⏲️  [{time.monotonic() - _T0:6.1f}s] {etapa}")


# ===============================================================
# LIMPAR DESTINO
# ===============================================================
def clear_dest_range(svc, spreadsheet_id, sheet_name, start_row, end_row=None, col_end="K"):
    # limpa de A{start_row} até col_end. Com end_row, delimita (evita estourar a grade).
    end = end_row if end_row is not None else ""
    rng = f"{sheet_name}!A{start_row}:{col_end}{end}"
    execute_with_retry(
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=rng,
            body={},
        ),
        f"limpar {rng}",
    )


def write_values_in_chunks(
    svc,
    spreadsheet_id,
    sheet_name,
    start_row,
    data,
    chunk_rows,
    num_cols,
):
    total = len(data)
    written = 0
    col_end = col_index_to_letter(num_cols)
    while written < total:
        take = min(chunk_rows, total - written)
        chunk = data[written : written + take]
        start = start_row + written
        end = start + take - 1
        rng = f"{sheet_name}!A{start}:{col_end}{end}"
        execute_with_retry(
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=rng,
                valueInputOption="USER_ENTERED",
                body={"values": chunk},
            ),
            f"gravar {rng}",
        )
        written += take
    return written


def count_pasted_rows(svc, spreadsheet_id, sheet_name, start_row, expected_rows):
    """Conta linhas coladas olhando colunas A:B.
    Uma linha conta se A OU B tiver conteúdo. Col A pode virar "" pela
    limpeza numérica; col B (chave) não sofre limpeza — usar as duas evita
    o falso 'undercount' que aparecia ao contar só a coluna A.
    """
    end_row = start_row + max(expected_rows - 1, 0)
    if end_row < start_row:
        return 0
    rng = f"{sheet_name}!A{start_row}:B{end_row}"
    vals = read_values(svc, spreadsheet_id, rng)
    return sum(
        1 for r in vals if any(c not in ("", None) for c in r[:2])
    )


def copiar_unidade(svc, nome, spreadsheet_id, criterios, header, all_rows, carimbo_base):
    """Cola em BD_Orçamento!A1 o cabeçalho + linhas da BASE cuja coluna J
    está em `criterios`. Grava antes de limpar (destino nunca fica vazio),
    depois limpa o resíduo A:J abaixo. Colunas K+ do destino não são tocadas.
    Retorna o nº de linhas coladas (sem cabeçalho); 0 = nada feito.
    """
    rows = [r for r in all_rows if str(r[9]).strip().upper() in criterios]
    if not rows:
        print(f"⚠️  {nome}: nenhuma linha na BASE para {sorted(criterios)}. Destino mantido.")
        return 0

    data = [header] + rows
    row_count = ensure_dest_grid_size(
        svc, spreadsheet_id, UNIDADE_SHEET_NAME, len(data), NUM_COLS
    )
    write_values_in_chunks(
        svc, spreadsheet_id, UNIDADE_SHEET_NAME, 1, data, WRITE_CHUNK_ROWS, NUM_COLS
    )

    first_residual = len(data) + 1
    if row_count >= first_residual:
        clear_dest_range(
            svc, spreadsheet_id, UNIDADE_SHEET_NAME, first_residual, row_count, col_end="J"
        )

    pasted = count_pasted_rows(svc, spreadsheet_id, UNIDADE_SHEET_NAME, 1, len(data))
    if pasted != len(data):
        raise RuntimeError(f"conferência: esperado {len(data)} linha(s), colado {pasted}")

    now_brt = datetime.now(timezone.utc) - timedelta(hours=3)
    execute_with_retry(
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": [
                    {"range": f"{PROG_TPM_SHEET}!E1",
                     "values": [[now_brt.strftime("%d/%m/%Y %H:%M:%S")]]},
                    {"range": f"{PROG_TPM_SHEET}!G1", "values": [[carimbo_base]]},
                ],
            },
        ),
        f"{nome}: carimbo Prog_TPM",
    )
    return len(rows)


def get_destinos_unidade(svc):
    """Lê o gabarito BD_Planilhas e devolve [(nome, id, {valores J})]."""
    destinos = []
    for row in read_values(svc, GABARITO_SPREADSHEET_ID, GABARITO_RANGE):
        row = pad_row_to_n_cols(row, 4)
        nome = row[0].strip()
        sid = extract_spreadsheet_id(row[1])
        criterios = {v.strip().upper() for v in row[3].split(",") if v.strip()}
        if not (sid or criterios):
            continue  # linha vazia
        if not sid or not criterios:
            print(f"⚠️  Gabarito: linha '{nome}' sem ID ou sem valores em E — ignorada.")
            continue
        destinos.append((nome or sid, sid, criterios))
    return destinos


def distribuir_unidades(svc, all_rows, carimbo_base):
    """Copia o orçamento de cada Unidade. Uma Unidade com erro não impede
    as outras. Retorna a lista de Unidades que falharam."""
    print("\n=== DISTRIBUIÇÃO POR UNIDADE ===")
    header = read_values(svc, DEST_SPREADSHEET_ID, f"{DEST_SHEET_NAME}!A1:J1")
    header = pad_row_to_n_cols(header[0] if header else [], NUM_COLS)

    # Linha sem Unidade não vai para planilha nenhuma. Costuma ser o
    # ARRAYFORMULA da coluna I/J da fonte quebrado (#REF!) por dado colado.
    sem_unidade = sum(1 for r in all_rows if not str(r[9]).strip())
    if sem_unidade:
        print(f"⚠️  {sem_unidade} linha(s) da BASE sem Unidade (coluna J vazia) — ficam fora.")

    try:
        destinos = get_destinos_unidade(svc)
    except Exception as e:
        print(f"❌ Erro ao ler o gabarito {GABARITO_RANGE}: {e}")
        return ["gabarito"]
    if not destinos:
        print(f"❌ Nenhuma Unidade válida em {GABARITO_RANGE}.")
        return ["gabarito"]
    print(f"📋 Gabarito: {len(destinos)} Unidade(s).")

    falhas = []
    for nome, sid, criterios in destinos:
        try:
            n = copiar_unidade(svc, nome, sid, criterios, header, all_rows, carimbo_base)
            if n:
                print(f"✅ {nome}: {n} linha(s) em {UNIDADE_SHEET_NAME}.")
        except Exception as e:
            falhas.append(nome)
            print(f"❌ {nome} ({sid}): {e}")
        log_tempo(f"Unidade {nome}")
    return falhas


def falhar(msg):
    """Encerra com exit 1: o run do Actions fica vermelho em vez de verde."""
    print(msg)
    log_tempo("fim (com erro)")
    sys.exit(1)


def main():
    print("🔄 Iniciando importação baseado em BD_Config!A3:A ...\n")

    try:
        svc, sa_email = get_service_and_email()
    except FileNotFoundError as e:
        falhar(f"❌ {e}\n   Coloque 'credenciais.json' na mesma pasta do script.")

    print(f"👤 Service Account: {sa_email}")
    print("   ➜ Garanta acesso às fontes listadas na BD_Config e ao destino.\n")

    # Garante que a aba de destino existe
    try:
        ensure_dest_sheet_exists(svc, DEST_SPREADSHEET_ID, DEST_SHEET_NAME)
    except HttpError as e:
        falhar(f"❌ Erro ao acessar destino: {e}")

    # Lê fontes da BD_Config
    source_ids = get_source_ids_from_config(svc)
    if not source_ids:
        falhar("❌ Nenhuma fonte encontrada em BD_Config!A3:A (IDs/URLs).")

    print(f"📚 Fontes encontradas em BD_Config: {len(source_ids)}")
    for i, sid in enumerate(source_ids, start=1):
        print(f"   - Fonte #{i}: {sid}")
    print()

    # Lê todas as fontes e empilha
    all_rows = []
    report_lines = []
    fontes_com_erro = 0
    for i, fid in enumerate(source_ids, start=1):
        try:
            rows = read_source_block(svc, fid, SOURCE_SHEET_NAME)
            report_lines.append(f"Fonte #{i}: {len(rows)} linha(s).")
            all_rows.extend(rows)
        except HttpError as e:
            fontes_com_erro += 1
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
            print(f"⚠️  Origem #{i} inacessível (ID: {fid}). Compartilhe com {sa_email}.")
        except Exception as e:
            fontes_com_erro += 1
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
    log_tempo("leitura das fontes")

    total_expected = len(all_rows)
    report_lines.append(f"\nTotal esperado: {total_expected} linha(s).")

    # BASE incompleta não sobrescreve a anterior: o carteira_ponto_a_ponto e
    # as Unidades leem dela. Melhor ficar com a BASE da última execução boa.
    if fontes_com_erro:
        print("\n".join(report_lines))
        falhar(f"\n❌ {fontes_com_erro} fonte(s) com erro — BASE NÃO atualizada.")
    if total_expected == 0:
        print("\n".join(report_lines))
        falhar("\n❌ Fontes sem nenhuma linha — BASE NÃO atualizada.")

    # A:J da fonte + K (máscara+ponto) calculada aqui mesmo, numa gravação só
    base_rows = [r + [mascara_ponto(r[1])] for r in all_rows]
    base_cols = NUM_COLS + 1  # A:K

    # Garante que a grade da aba tenha linhas/colunas suficientes (A:K)
    min_rows = START_ROW_DEST + total_expected - 1
    row_count = ensure_dest_grid_size(
        svc,
        DEST_SPREADSHEET_ID,
        DEST_SHEET_NAME,
        min_rows,
        base_cols,
    )

    # Grava ANTES de limpar: o destino nunca fica vazio.
    # Se a gravação cair no meio, sobra mistura de dado novo + antigo
    # (sem buraco em branco). Só depois removemos o resíduo abaixo.
    print(f"📤 Colando {total_expected} linha(s) em {DEST_SHEET_NAME} (A:K)...")
    write_values_in_chunks(
        svc,
        DEST_SPREADSHEET_ID,
        DEST_SHEET_NAME,
        START_ROW_DEST,
        base_rows,
        WRITE_CHUNK_ROWS,
        base_cols,
    )
    del base_rows
    log_tempo("colagem da BASE")

    # Limpa só as linhas antigas que sobraram abaixo do novo dado.
    first_residual = START_ROW_DEST + total_expected
    if row_count >= first_residual:
        print(
            f"🧹 Limpando resíduo (linhas {first_residual}–{row_count}, A:K)..."
        )
        clear_dest_range(
            svc,
            DEST_SPREADSHEET_ID,
            DEST_SHEET_NAME,
            first_residual,
            row_count,
        )
    else:
        print("🧹 Sem resíduo abaixo do novo dado.")

    # Checagem final
    pasted_count = count_pasted_rows(
        svc, DEST_SPREADSHEET_ID, DEST_SHEET_NAME, START_ROW_DEST, total_expected
    )
    log_tempo("conferência da BASE")

    report_lines.append(
        f"Total efetivamente colado (A/B): {pasted_count} linha(s)."
    )
    ok = pasted_count == total_expected

    print("\n=== RELATÓRIO DE IMPORTAÇÃO ===")
    print("\n".join(report_lines))
    print("\n✅ OK - Tudo conferido!" if ok else "\n⚠️ Diferença detectada.")

    # ===============================================================
    # === TIMESTAMP EM L2 DA ABA ATIVIDADES_POR_PONTO_BASE ==========
    # ===============================================================
    now_brt = datetime.now(timezone.utc) - timedelta(hours=3)
    timestamp = now_brt.strftime("%d/%m/%Y %H:%M:%S")

    try:
        execute_with_retry(
            svc.spreadsheets().values().update(
                spreadsheetId=DEST_SPREADSHEET_ID,
                range=f"{DEST_SHEET_NAME}!L2",
                valueInputOption="USER_ENTERED",
                body={"values": [[timestamp]]},
            ),
            "gravar timestamp L2",
        )
        print(f"⏱️ Timestamp gravado em {DEST_SHEET_NAME}!L2 (BRT): {timestamp}")
    except Exception as e:
        falhar(f"❌ Erro ao gravar timestamp em L2: {e}")

    # ===============================================================
    # === DISTRIBUIÇÃO DO ORÇAMENTO POR UNIDADE =====================
    # ===============================================================
    # Só distribui uma BASE íntegra: colagem divergente propagaria
    # orçamento incompleto para as Unidades.
    if not ok:
        falhar("\n❌ Distribuição por Unidade NÃO executada (conferência da BASE divergente).")

    falhas = distribuir_unidades(svc, all_rows, timestamp)
    if falhas:
        falhar(f"\n❌ Unidades com falha: {', '.join(falhas)}")
    print("\n✅ Distribuição por Unidade concluída.")
    log_tempo("fim")


if __name__ == "__main__":
    main()
