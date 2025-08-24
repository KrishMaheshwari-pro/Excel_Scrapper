from google.colab import files
import os, shutil, re
import pandas as pd
from zipfile import ZipFile
from collections import Counter, defaultdict

#folders to exclude or skip during extraction due to some error in formating in the excel
EXCLUDE_FOLDERS = {"geojit", "agreya", "invesq", "moneybee", "moneylife", "spark"}
EXCLUDE_FOLDERS_NORM = {f.lower() for f in EXCLUDE_FOLDERS}
SKIPPED_FOLDERS_ACTUALLY = set()
YEAR = 2025    # change year
TARGET = 5       # the day or month to filter on ( month number )
HEADER_SCAN_ROWS = 6     

print("Upload: 1) ZIP of Excels, 2) baseline CSV, 3+) mapping XLSX(s)")
uploaded = files.upload()
zip_file    = next(f for f in uploaded if f.lower().endswith(".zip"))
csv_file    = next(f for f in uploaded if f.lower().endswith(".csv"))
mapping_files = [
    f for f in uploaded
    if f.lower().endswith((".xls","xlsx")) and f not in (zip_file, csv_file)
]
if not mapping_files:
    raise FileNotFoundError("Please upload at least one mapping .xlsx")

mapping_dfs = []
for mf in mapping_files:
    dfm = pd.read_excel(mf)
    dfm.columns = dfm.columns.str.strip()
    if {"Folder Name","Scheme Name","Scheme Code"}.issubset(dfm.columns):
        mapping_dfs.append(dfm[["Folder Name","Scheme Name","Scheme Code"]])
if not mapping_dfs:
    raise ValueError("No mapping sheets contained required columns")
mapping_df = pd.concat(mapping_dfs, ignore_index=True)
mapping_df["_norm_folder"] = (
    mapping_df["Folder Name"].astype(str)
      .str.strip().str.strip("-–— ").str.lower()
)
def normalize_cell(x):
    s = str(x).replace("\xa0"," ")
    return re.sub(r"\s+"," ",s).strip().lower()
mapping_df["_norm_name"] = mapping_df["Scheme Name"].astype(str).apply(normalize_cell)

extract_dir = "/content/extracted_excels"
if os.path.exists(extract_dir):
    shutil.rmtree(extract_dir)
os.makedirs(extract_dir, exist_ok=True)
with ZipFile(zip_file, "r") as zf:
    zf.extractall(extract_dir)

actual_folders = {
    d.strip().strip("-–— ").lower()
    for d in os.listdir(extract_dir)
    if os.path.isdir(os.path.join(extract_dir, d))
}

csv_df = pd.read_csv(csv_file)
skip_kw = {"benchmark","bse","nse","nifty","s&p","rolling","s &"}

def is_date(x):
    try:
        pd.to_datetime(str(x), errors="raise")
        return True
    except:
        return False

blocks_meta = []

for root, _, files_list in os.walk(extract_dir):
    top_folder = os.path.relpath(root, extract_dir).split(os.sep)[0]
    top_norm   = top_folder.strip().strip("-–— ").lower()
    if top_norm in EXCLUDE_FOLDERS_NORM:
        SKIPPED_FOLDERS_ACTUALLY.add(top_folder)
        continue

    for fn in files_list:
        if not fn.lower().endswith((".xls","xlsx","xlsm")):
            continue
        path = os.path.join(root, fn)
        try:
            xls = pd.ExcelFile(path)
        except:
            continue

        for sheet in xls.sheet_names:
            try:
                df = xls.parse(sheet, header=None)
            except:
                continue
            df.dropna(how="all", axis=0, inplace=True)
            df.dropna(how="all", axis=1, inplace=True)

            bm = {
                c for c in range(min(20, df.shape[1]))
                for r in range(min(20, df.shape[0]))
                if any(kw in str(df.iat[r,c]).lower() for kw in skip_kw)
            }
            if bm:
                df.drop(columns=[df.columns[c] for c in sorted(bm)],
                        inplace=True, errors="ignore")

            hdrs = [
                i for i in range(min(50, df.shape[0]))
                if (df.iloc[i].astype(str).str.lower().str.strip().eq("date").any() and
                    df.iloc[i].astype(str).str.lower().str.strip().eq("nav").any())
            ]
            for hdr in hdrs:
                vals = df.iloc[hdr].astype(str).str.lower().tolist()
                sub  = df.iloc[hdr+1:].reset_index(drop=True)
                sub.columns = vals
                bad = [h for h in vals if any(kw in h for kw in skip_kw)]
                if bad:
                    sub.drop(columns=bad, inplace=True, errors="ignore")

                d_idxs = [i for i,h in enumerate(vals) if "date" in h]
                n_idxs = [i for i,h in enumerate(vals) if "nav"  in h]

                for di in d_idxs:
                    col    = sub.iloc[:,di]
                    parsed = pd.to_datetime(col, dayfirst=True, errors="coerce")
                    swap_mask = parsed.dt.day.eq(TARGET) & ~parsed.dt.month.eq(TARGET) & parsed.notna()
                    if swap_mask.any():
                        p = parsed[swap_mask]
                        parsed.loc[swap_mask] = pd.to_datetime({
                            "year":  p.dt.year,
                            "month": p.dt.day,
                            "day":   p.dt.month
                        })
                    mask = ((parsed.dt.day.eq(TARGET) | parsed.dt.month.eq(TARGET)) &
                            parsed.dt.year.eq(YEAR))
                    if not mask.any():
                        continue
                    raw_clean = parsed.dt.strftime("%d-%m-%Y")

                    ni = next((n for n in n_idxs if n>di), None)
                    if ni is None:
                        continue
                    navs_raw = sub.iloc[:,ni].astype(str)

                    pos = list(mask[mask].index)
                    runs, run = [], [pos[0]]
                    for idx in pos[1:]:
                        if idx == run[-1] + 1:
                            run.append(idx)
                        else:
                            runs.append(run); run=[idx]
                    runs.append(run)
                    good_runs = [r for r in runs if len(r)>=2]
                    good_pos  = [i for r in good_runs for i in r] if good_runs else [pos[-1]]

                    clean_dates = pd.Series(raw_clean.values).reindex(good_pos).reset_index(drop=True)
                    nav_vals    = pd.Series(navs_raw.values).reindex(good_pos).reset_index(drop=True)

                    if di+1 < sub.shape[1]:
                        alt_vals = sub.iloc[good_pos, di+1].astype(str).reset_index(drop=True)
                        num_mask = pd.to_numeric(nav_vals, errors="coerce").notna()
                        nav_vals = nav_vals.where(num_mask, alt_vals)

                    nav_vals = nav_vals.replace(r'^\s*nan\s*$', pd.NA, regex=True)
                    orig_len = len(nav_vals)
                    keep = nav_vals.notna() & nav_vals.astype(str).str.strip().astype(bool)
                    if keep.sum() < orig_len:
                        continue

                    clean_dates = clean_dates[keep].reset_index(drop=True)
                    nav_vals    = nav_vals[keep].reset_index(drop=True)
                    blk = pd.DataFrame({"Date": clean_dates, "NAV": nav_vals})
                    if not blk.empty:
                        blocks_meta.append((blk, top_folder, path, sheet, hdr, di))

            if not hdrs:
                for dc in range(df.shape[1]):
                    date_rows = [r for r in range(min(100, df.shape[0])) if is_date(df.iat[r,dc])]
                    if not date_rows:
                        continue
                    header_row = date_rows[0] - 1

                    if header_row>=0 and "date" in str(df.iat[header_row,dc]).lower():
                        nc = dc+1
                        if nc>=df.shape[1] or "nav" not in str(df.iat[header_row,nc]).lower():
                            continue
                    else:
                        header_nav_cols = {
                            j for i in range(min(HEADER_SCAN_ROWS, df.shape[0]))
                            for j in range(df.shape[1])
                            if "nav" in str(df.iat[i,j]).lower()
                        }
                        cands = sorted(j for j in header_nav_cols if j>dc)
                        if not cands:
                            continue
                        nc = cands[0]

                    col    = df.iloc[:,dc]
                    parsed = pd.to_datetime(col, dayfirst=True, errors="coerce")
                    swap_mask = parsed.dt.day.eq(TARGET) & ~parsed.dt.month.eq(TARGET) & parsed.notna()
                    if swap_mask.any():
                        p = parsed[swap_mask]
                        parsed.loc[swap_mask] = pd.to_datetime({
                            "year":  p.dt.year,
                            "month": p.dt.day,
                            "day":   p.dt.month
                        })
                    mask = ((parsed.dt.day.eq(TARGET) | parsed.dt.month.eq(TARGET)) &
                            parsed.dt.year.eq(YEAR))
                    if not mask.any():
                        continue
                    raw_clean = parsed.dt.strftime("%d-%m-%Y")

                    pos = list(mask[mask].index)
                    runs, run = [], [pos[0]]
                    for idx in pos[1:]:
                        if idx == run[-1] + 1:
                            run.append(idx)
                        else:
                            runs.append(run); run=[idx]
                    runs.append(run)
                    good_runs = [r for r in runs if len(r)>=2]
                    good_pos  = [i for r in good_runs for i in r] if good_runs else [pos[-1]]
                    good_pos = [i for i in good_pos if i < len(parsed)]

                    clean_dates = pd.Series(raw_clean.values).reindex(good_pos).reset_index(drop=True)
                    nav_vals    = pd.Series(df.iloc[good_pos, nc].astype(str).values).reset_index(drop=True)

                    if dc+1 < df.shape[1]:
                        alt_vals = df.iloc[good_pos, dc+1].astype(str).reset_index(drop=True)
                        num_mask = pd.to_numeric(nav_vals, errors="coerce").notna()
                        nav_vals = nav_vals.where(num_mask, alt_vals)

                    nav_vals = nav_vals.replace(r'^\s*nan\s*$', pd.NA, regex=True)
                    orig_len = len(nav_vals)
                    keep = nav_vals.notna() & nav_vals.astype(str).str.strip().astype(bool)
                    if keep.sum() < orig_len:
                        continue

                    clean_dates = clean_dates[keep].reset_index(drop=True)
                    nav_vals    = nav_vals[keep].reset_index(drop=True)
                    blk = pd.DataFrame({"Date": clean_dates, "NAV": nav_vals})
                    if not blk.empty:
                        blocks_meta.append((blk, top_folder, path, sheet, None, None))

sheet_counts = Counter((path, sheet) for _, _, path, sheet, _, _ in blocks_meta)

all_blocks, all_src, block_paths, block_folders, block_folder_norms = [], [], [], [], []
for blk, folder, path, sheet, hdr, di in blocks_meta:
    top_norm = folder.strip().strip("-–— ").lower()
    cands    = mapping_df[mapping_df["_norm_folder"] == top_norm].copy()
    patterns = [(re.compile(rf"(?<!\w){re.escape(nm)}(?!\w)"), sc)
                for nm, sc in zip(cands["_norm_name"], cands["Scheme Code"])]
    code = None
    wb   = pd.ExcelFile(path)
    sh   = wb.parse(sheet, header=None).applymap(normalize_cell)

    if sheet_counts[(path, sheet)] > 1 and hdr is not None and di is not None:
        hdr_vals = sh.iloc[hdr].tolist()
        nav_idxs = [i for i,h in enumerate(hdr_vals) if "nav" in h]
        nav_col  = next((n for n in nav_idxs if n>di), nav_idxs[0] if nav_idxs else None)
        for r in range(hdr-1,-1,-1):
            for col in (di, nav_col):
                txt = sh.iat[r,col] if col is not None else ""
                if txt and not any(k in txt for k in ("date","nav")):
                    for pat,sc in patterns:
                        if pat.search(txt):
                            code=sc; break
                    if code: break
            if code: break
        if code is None:
            nm = normalize_cell(os.path.basename(path))
            code = next((sc for n,nm2,sc in zip(cands["_norm_name"], cands["_norm_name"], cands["Scheme Code"]) if nm2 in nm), None)
        if code is None:
            sn = normalize_cell(sheet)
            code = next((sc for nm2,sc in zip(cands["_norm_name"], cands["Scheme Code"]) if nm2 in sn), None)
        code = code or top_norm

    else:
        # single-block logic
        if hdr is not None and di is not None:
            for r in range(hdr-1,-1,-1):
                txt = sh.iat[r,di]
                for pat,sc in patterns:
                    if pat.search(txt):
                        code=sc; break
                if code: break
        if not code and hdr and hdr>0:
            line = " ".join(sh.iloc[hdr-1].dropna().tolist())
            for pat,sc in patterns:
                if pat.search(line):
                    code=sc; break
        if not code:
            flat = sh.values.flatten()
            for pat,sc in patterns:
                if any(pat.search(str(x)) for x in flat):
                    code=sc; break
        if not code:
            for oth in wb.sheet_names:
                if oth==sheet: continue
                block = wb.parse(oth,header=None).iloc[:6].applymap(normalize_cell).values.flatten()
                for pat,sc in patterns:
                    if any(pat.search(str(x)) for x in block):
                        code=sc; break
                if code: break
        if not code:
            nm = normalize_cell(os.path.basename(path))
            code = next((sc for nm2,sc in zip(cands["_norm_name"], cands["Scheme Code"]) if nm2 in nm), None)
        if not code:
            sn = normalize_cell(sheet)
            code = next((sc for nm2,sc in zip(cands["_norm_name"], cands["Scheme Code"]) if nm2 in sn), None)
        if not code and len(cands)==1:
            code = cands["Scheme Code"].iat[0]
        code = code or top_norm

    all_blocks.append(blk)
    all_src.append(code)
    block_paths.append(path)
    block_folders.append(folder)
    block_folder_norms.append(top_norm)

file_to_idxs = defaultdict(list)
for i,p in enumerate(block_paths):
    file_to_idxs[p].append(i)
for p,idxs in file_to_idxs.items():
    if len(idxs)>1 and any(all_src[i]==block_folder_norms[i] for i in idxs):
        for i in idxs:
            all_src[i]=block_folder_norms[i]

final_blocks, final_src, final_folders = [], [], []
for blk,sc,fld in zip(all_blocks, all_src, block_folder_norms):
    key = set(zip(blk["Date"], blk["NAV"]))
    duplicate = False
    for j,ebb in enumerate(final_blocks):
        ek = set(zip(ebb["Date"], ebb["NAV"]))
        if key.issubset(ek):
            duplicate = True
            break
        if ek.issubset(key):
            final_blocks.pop(j)
            final_src.pop(j)
            final_folders.pop(j)
            break
    if not duplicate:
        final_blocks.append(blk)
        final_src.append(sc)
        final_folders.append(fld)

bc = Counter(final_src)
rc = Counter()
for blk,sc in zip(final_blocks, final_src):
    rc[sc] += len(blk)
for i,sc in enumerate(final_src):
    if bc[sc]>1 or rc[sc]>31:
        final_src[i] = final_folders[i]

for blk,sc in zip(final_blocks, final_src):
    out = blk.copy()
    out.insert(0, "SchemeCode", sc)
    csv_df = pd.concat([csv_df, out[["SchemeCode","Date","NAV"]]], ignore_index=True)
csv_df.to_csv("final_revised_output.csv", index=False)
files.download("final_revised_output.csv")

used = set(final_src) & set(mapping_df["Scheme Code"])
unmatched = mapping_df.loc[~mapping_df["Scheme Code"].isin(used),
                           ["Scheme Code","Scheme Name","Folder Name"]].copy()
pos1 = unmatched.columns.get_loc("Folder Name") + 1
unmatched.insert(pos1, "", "")
unmatched["Excluded Folder"] = unmatched["Folder Name"].where(
    unmatched["Folder Name"].str.strip().str.strip("-–— ").str.lower()
      .isin({f.lower() for f in SKIPPED_FOLDERS_ACTUALLY}), ""
)
pos2 = unmatched.columns.get_loc("Excluded Folder") + 1
unmatched.insert(pos2, " ", "")
unmatched["Folder Not Available"] = unmatched["Folder Name"].where(
    ~unmatched["Folder Name"].str.strip().str.strip("-–— ").str.lower()
      .isin(actual_folders), ""
)
unmatched.to_csv("unmatched_schemes.csv", index=False)
files.download("unmatched_schemes.csv")
