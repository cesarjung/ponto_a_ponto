# ===============================================================
# Importador ATIVIDADES_POR_PONTO_BASE
# - Lê a lista de fontes em BD_Config!A3:A (IDs ou URLs)
# - Copia A:J (linha 2+) da aba ATIVIDADES_POR_PONTO de cada fonte
# - Concatena e cola em ATIVIDADES_POR_PONTO_BASE!A2
# - Converte colunas A e G para número
# - Relatório de linhas por fonte e total colado
# ===============================================================

import os
import re
from datetime import datetime, timedelta
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

# ===============================================================


def get_service_and_email():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_FILE}")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    svc = build("sheets", "v4", credentials=creds)
    return svc, creds.service_account_email


def ensure_dest_sheet_exists(svc, spreadsheet_id, sheet_name):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == sheet_name:
            return
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def ensure_dest_grid_size(svc, spreadsheet_id, sheet_name, min_rows, min_cols):
    """
    Garante que a aba de destino tenha pelo menos min_rows linhas e min_cols colunas.
    Se necessário, atualiza gridProperties.rowCount / columnCount via batchUpdate.
    """
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    target_sheet = None
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            target_sheet = props
            break

    if not target_sheet:
        # já deveria existir, mas por segurança
        return

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
        return

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
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body,
    ).execute()


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
    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        majorDimension="ROWS",
    ).execute()
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


def clear_dest_range(svc, spreadsheet_id, sheet_name, start_row, num_cols):
    col_end = chr(ord("A") + num_cols - 1)
    rng = f"{sheet_name}!A{start_row}:{col_end}"
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=rng,
        body={},
    ).execute()


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
    col_end = chr(ord("A") + num_cols - 1)
    while written < total:
        take = min(chunk_rows, total - written)
        chunk = data[written : written + take]
        start = start_row + written
        end = start + take - 1
        rng = f"{sheet_name}!A{start}:{col_end}{end}"
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body={"values": chunk},
        ).execute()
        written += take
    return written


def count_nonempty_rows_in_col_a(
    svc, spreadsheet_id, sheet_name, start_row, expected_rows
):
    end_row = start_row + max(expected_rows - 1, 0)
    if end_row < start_row:
        return 0
    rng = f"{sheet_name}!A{start_row}:A{end_row}"
    vals = read_values(svc, spreadsheet_id, rng)
    return sum(1 for r in vals if len(r) > 0 and r[0] not in ("", None))


def main():
    print("🔄 Iniciando importação baseado em BD_Config!A3:A ...\n")

    try:
        svc, sa_email = get_service_and_email()
    except FileNotFoundError as e:
        print("❌", e)
        print("   Coloque 'credenciais.json' na mesma pasta do script.")
        return

    print(f"👤 Service Account: {sa_email}")
    print("   ➜ Garanta acesso às fontes listadas na BD_Config e ao destino.\n")

    # Garante que a aba de destino existe
    try:
        ensure_dest_sheet_exists(svc, DEST_SPREADSHEET_ID, DEST_SHEET_NAME)
    except HttpError as e:
        print("❌ Erro ao acessar destino:", e)
        return

    # Lê fontes da BD_Config
    source_ids = get_source_ids_from_config(svc)
    if not source_ids:
        print("❌ Nenhuma fonte encontrada em BD_Config!A3:A (IDs/URLs).")
        return

    print(f"📚 Fontes encontradas em BD_Config: {len(source_ids)}")
    for i, sid in enumerate(source_ids, start=1):
        print(f"   - Fonte #{i}: {sid}")
    print()

    # Lê todas as fontes e empilha
    all_rows = []
    report_lines = []
    for i, fid in enumerate(source_ids, start=1):
        try:
            rows = read_source_block(svc, fid, SOURCE_SHEET_NAME)
            report_lines.append(f"Fonte #{i}: {len(rows)} linha(s).")
            all_rows.extend(rows)
        except HttpError as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")
            print(f"⚠️  Origem #{i} inacessível (ID: {fid}). Compartilhe com {sa_email}.")
        except Exception as e:
            report_lines.append(f"Fonte #{i}: ERRO -> {e}")

    total_expected = len(all_rows)
    report_lines.append(f"\nTotal esperado: {total_expected} linha(s).")

    if total_expected == 0:
        print("\n".join(report_lines))
        print("\nNada para colar.")
        return

    # Garante que a grade da aba tenha linhas/colunas suficientes (A:J)
    min_rows = START_ROW_DEST + total_expected - 1
    ensure_dest_grid_size(
        svc,
        DEST_SPREADSHEET_ID,
        DEST_SHEET_NAME,
        min_rows,
        NUM_COLS,
    )

    # Limpa destino e cola
    print("🧹 Limpando destino A2:J...")
    clear_dest_range(
        svc,
        DEST_SPREADSHEET_ID,
        DEST_SHEET_NAME,
        START_ROW_DEST,
        NUM_COLS,
    )

    print(f"📤 Colando {total_expected} linha(s) em {DEST_SHEET_NAME}...")
    write_values_in_chunks(
        svc,
        DEST_SPREADSHEET_ID,
        DEST_SHEET_NAME,
        START_ROW_DEST,
        all_rows,
        WRITE_CHUNK_ROWS,
        NUM_COLS,
    )

    # Checagem final
    pasted_count = count_nonempty_rows_in_col_a(
        svc, DEST_SPREADSHEET_ID, DEST_SHEET_NAME, START_ROW_DEST, total_expected
    )

    report_lines.append(
        f"Total efetivamente colado (coluna A): {pasted_count} linha(s)."
    )
    ok = pasted_count == total_expected

    print("\n=== RELATÓRIO DE IMPORTAÇÃO ===")
    print("\n".join(report_lines))
    print("\n✅ OK - Tudo conferido!" if ok else "\n⚠️ Diferença detectada.")

    # ===============================================================
    # === COLUNA K: GERAR CÓDIGO A PARTIR DA COLUNA B ===============
    # ===============================================================
    try:
        if pasted_count > 0:
            start_row = START_ROW_DEST
            end_row = START_ROW_DEST + pasted_count - 1

            # garante até a coluna K
            ensure_dest_grid_size(
                svc,
                DEST_SPREADSHEET_ID,
                DEST_SHEET_NAME,
                min_rows=end_row,
                min_cols=11,
            )

            rng_b = f"{DEST_SHEET_NAME}!B{start_row}:B{end_row}"
            vals_b = read_values(svc, DEST_SPREADSHEET_ID, rng_b)

            new_k_values = []
            for i in range(pasted_count):
                val_b = ""
                if i < len(vals_b) and vals_b[i]:
                    val_b = vals_b[i][0]

                if val_b in ("", None):
                    new_k_values.append([""])
                    continue

                if not isinstance(val_b, str):
                    val_b = str(val_b)

                before_underscore = val_b.split("_", 1)[0]
                digits_only = re.sub(r"\D", "", before_underscore)

                if len(digits_only) == 6:
                    prefix = "B-0"
                elif len(digits_only) == 7:
                    prefix = "B-"
                else:
                    prefix = "B-"  # fallback

                k_val = prefix + val_b
                new_k_values.append([k_val])

            rng_k = f"{DEST_SHEET_NAME}!K{start_row}:K{end_row}"
            svc.spreadsheets().values().update(
                spreadsheetId=DEST_SPREADSHEET_ID,
                range=rng_k,
                valueInputOption="USER_ENTERED",
                body={"values": new_k_values},
            ).execute()
            print(f"🔤 Coluna K preenchida para {pasted_count} linha(s).")
    except Exception as e:
        print("⚠️ Erro ao atualizar coluna K:", e)

    # ===============================================================
    # === TIMESTAMP EM L2 DA ABA ATIVIDADES_POR_PONTO_BASE ==========
    # ===============================================================
    now_brt = datetime.utcnow() - timedelta(hours=3)
    timestamp = now_brt.strftime("%d/%m/%Y %H:%M:%S")

    try:
        svc.spreadsheets().values().update(
            spreadsheetId=DEST_SPREADSHEET_ID,
            range=f"{DEST_SHEET_NAME}!L2",
            valueInputOption="USER_ENTERED",
            body={"values": [[timestamp]]},
        ).execute()
        print(f"⏱️ Timestamp gravado em {DEST_SHEET_NAME}!L2 (BRT): {timestamp}")
    except Exception as e:
        print("⚠️ Erro ao gravar timestamp em L2:", e)


if __name__ == "__main__":
    main()
