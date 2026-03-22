import csv
from pathlib import Path

path = Path('history/market_history.csv')
rows = []
with path.open('r', encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for r in reader:
        rows.append(r)

updates = {
    'KXHIGHNY-26MAR19-B45.5': {'actual_outcome_yes': '0', 'actual_high_temp_f': '43.5'},
    'KXHIGHNY-26MAR19-B43.5': {'actual_outcome_yes': '1', 'actual_high_temp_f': '43.5'},
    'KXHIGHAUS-26MAR19-B85.5': {'actual_outcome_yes': '1', 'actual_high_temp_f': '85.5'},
    'KXHIGHMIA-26MAR19-B77.5': {'actual_outcome_yes': '1', 'actual_high_temp_f': '77.5'},
    'KXHIGHLAX-26MAR19-T83': {'actual_outcome_yes': '0', 'actual_high_temp_f': '83.3'},
    'KXHIGHLAX-26MAR19-B83.5': {'actual_outcome_yes': '1', 'actual_high_temp_f': '83.3'},
}

changed = 0
for r in rows:
    mt = r.get('market_ticker')
    if mt in updates:
        for k, v in updates[mt].items():
            if r.get(k) != v:
                r[k] = v
                changed += 1

with path.open('w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print('rows updated', len([r for r in rows if r.get('market_ticker') in updates]))
print('fields changed', changed)

for mt in updates:
    row = next((r for r in rows if r.get('market_ticker') == mt), None)
    if row:
        print(mt, row.get('actual_outcome_yes'), row.get('actual_high_temp_f'))
